package database

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/hdt3213/rdb/crc64jones"
	"github.com/hdt3213/rdb/lzf"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// Redis FUNCTION DUMP is an RDB-like stream: [0xF5][RDB string library code]…
// followed by a 2-byte RDB version and an 8-byte CRC64-Jones footer.
// This file implements the wire format so dump/restore can interoperate with
// Redis instead of using the private GODISFN1 envelope.
const (
	maxFunctionDumpRDBVersion = 16
	maxFunctionDumpStringSize = 512 << 20

	rdbLen6Bit  = 0
	rdbLen14Bit = 1
	rdbLen32Bit = 2
	rdbLenEnc   = 3

	rdbLen32BitFlag = 0x80
	rdbLen64BitFlag = 0x81
	rdbEncInt8      = 0
	rdbEncInt16     = 1
	rdbEncInt32     = 2
	rdbEncLZF       = 3
)

var errBadFunctionDumpFooter = fmt.Errorf("DUMP payload version or checksum are wrong")

type functionLibraryDump struct {
	Name string
	Code string
}

// encodeFunctionDump writes a Redis-compatible FUNCTION DUMP payload.
// Library order is the caller's order; sorting happens at the command layer.
func encodeFunctionDump(libs []functionLibraryDump) []byte {
	var body []byte
	for _, lib := range libs {
		if lib.Code == "" {
			continue
		}
		body = append(body, rdbOpcodeFunction2)
		body = appendRDBString(body, []byte(lib.Code))
	}
	return appendDumpFooter(body)
}

// decodeFunctionDump parses a Redis-compatible FUNCTION DUMP payload.
func decodeFunctionDump(payload []byte) ([]functionLibraryDump, error) {
	if len(payload) < 10 || !verifyFunctionDumpFooter(payload) {
		return nil, errBadFunctionDumpFooter
	}
	version := binary.LittleEndian.Uint16(payload[len(payload)-10 : len(payload)-8])
	if version == 0 || version > maxFunctionDumpRDBVersion {
		return nil, errBadFunctionDumpFooter
	}

	body := payload[:len(payload)-10]
	libs := make([]functionLibraryDump, 0, 1)
	for off := 0; off < len(body); {
		opcode := body[off]
		off++
		switch opcode {
		case rdbOpcodeFunction2:
		case rdbOpcodeFunctionPreGA:
			return nil, fmt.Errorf("Pre-GA function format not supported")
		default:
			return nil, fmt.Errorf("given type is not a function")
		}

		code, next, err := readRDBString(body, off)
		if err != nil {
			return nil, err
		}
		off = next

		name, errReply := parseFunctionShebang(string(code))
		if errReply != nil {
			return nil, fmt.Errorf("%s", functionMetadataError(errReply))
		}
		libs = append(libs, functionLibraryDump{Name: name, Code: string(code)})
	}
	return libs, nil
}

// looksLikeFunctionDump reports whether payload is a plausible official
// FUNCTION DUMP (including the empty dump, which has no library record).
func looksLikeFunctionDump(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	if payload[0] == rdbOpcodeFunction2 || payload[0] == rdbOpcodeFunctionPreGA {
		return true
	}
	if len(payload) < 10 {
		return false
	}
	if !verifyFunctionDumpFooter(payload) {
		return false
	}
	version := binary.LittleEndian.Uint16(payload[len(payload)-10 : len(payload)-8])
	return version >= 1 && version <= maxFunctionDumpRDBVersion
}

func verifyFunctionDumpFooter(payload []byte) bool {
	h := crc64jones.New()
	_, _ = h.Write(payload[:len(payload)-8])
	return h.Sum64() == binary.LittleEndian.Uint64(payload[len(payload)-8:])
}

func functionMetadataError(r redis.Reply) string {
	msg := strings.TrimSpace(string(r.ToBytes()))
	msg = strings.TrimPrefix(msg, "-")
	msg = strings.TrimSpace(msg)
	return strings.TrimPrefix(msg, "ERR ")
}

func appendRDBString(dst, val []byte) []byte {
	n := len(val)
	switch {
	case n < 1<<6:
		dst = append(dst, byte(n))
	case n < 1<<14:
		dst = append(dst, byte(n>>8)|0x40, byte(n))
	default:
		dst = append(dst, rdbLen32BitFlag, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
	return append(dst, val...)
}

func readRDBString(data []byte, off int) ([]byte, int, error) {
	if off >= len(data) {
		return nil, off, fmt.Errorf("unexpected end of FUNCTION DUMP payload")
	}
	b := data[off]
	off++
	switch b >> 6 {
	case rdbLen6Bit:
		return readRDBStringBody(data, off, int(b&0x3f))
	case rdbLen14Bit:
		if off >= len(data) {
			return nil, off, fmt.Errorf("unexpected end of FUNCTION DUMP length")
		}
		n := int(b&0x3f)<<8 | int(data[off])
		return readRDBStringBody(data, off+1, n)
	case rdbLen32Bit:
		switch b {
		case rdbLen32BitFlag:
			if off+4 > len(data) {
				return nil, off, fmt.Errorf("unexpected end of FUNCTION DUMP length")
			}
			n := int(binary.BigEndian.Uint32(data[off:]))
			return readRDBStringBody(data, off+4, n)
		case rdbLen64BitFlag:
			if off+8 > len(data) {
				return nil, off, fmt.Errorf("unexpected end of FUNCTION DUMP length")
			}
			n64 := binary.BigEndian.Uint64(data[off:])
			off += 8
			if n64 > maxFunctionDumpStringSize || n64 > uint64(len(data)-off) {
				return nil, off, fmt.Errorf("FUNCTION DUMP string length out of range")
			}
			n := int(n64)
			return data[off : off+n], off + n, nil
		default:
			return nil, off, fmt.Errorf("invalid RDB string length encoding")
		}
	case rdbLenEnc:
		return readRDBEncodedString(data, off, b&0x3f)
	default:
		return nil, off, fmt.Errorf("invalid RDB string encoding")
	}
}

func readRDBStringBody(data []byte, off, n int) ([]byte, int, error) {
	if n < 0 || n > maxFunctionDumpStringSize || n > len(data)-off {
		return nil, off, fmt.Errorf("FUNCTION DUMP string length out of range")
	}
	return data[off : off+n], off + n, nil
}

func readRDBEncodedString(data []byte, off int, enc byte) ([]byte, int, error) {
	switch enc {
	case rdbEncInt8:
		if off+1 > len(data) {
			return nil, off, fmt.Errorf("unexpected end of FUNCTION DUMP integer")
		}
		v := int64(int8(data[off]))
		return []byte(strconv.FormatInt(v, 10)), off + 1, nil
	case rdbEncInt16:
		if off+2 > len(data) {
			return nil, off, fmt.Errorf("unexpected end of FUNCTION DUMP integer")
		}
		v := int64(int16(binary.LittleEndian.Uint16(data[off:])))
		return []byte(strconv.FormatInt(v, 10)), off + 2, nil
	case rdbEncInt32:
		if off+4 > len(data) {
			return nil, off, fmt.Errorf("unexpected end of FUNCTION DUMP integer")
		}
		v := int64(int32(binary.LittleEndian.Uint32(data[off:])))
		return []byte(strconv.FormatInt(v, 10)), off + 4, nil
	case rdbEncLZF:
		compressedLen, next, err := readRDBRawLength(data, off)
		if err != nil {
			return nil, off, err
		}
		uncompressedLen, next, err := readRDBRawLength(data, next)
		if err != nil {
			return nil, off, err
		}
		if compressedLen < 0 || compressedLen > len(data)-next {
			return nil, off, fmt.Errorf("FUNCTION DUMP LZF length out of range")
		}
		if uncompressedLen < 0 || uncompressedLen > maxFunctionDumpStringSize {
			return nil, off, fmt.Errorf("FUNCTION DUMP LZF output length out of range")
		}
		compressed := data[next : next+compressedLen]
		out, err := decompressFunctionLZF(compressed, uncompressedLen)
		if err != nil {
			return nil, off, fmt.Errorf("invalid LZF payload in FUNCTION DUMP")
		}
		return out, next + compressedLen, nil
	default:
		return nil, off, fmt.Errorf("unsupported RDB string encoding")
	}
}

func decompressFunctionLZF(compressed []byte, outLen int) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			out = nil
			err = fmt.Errorf("corrupt LZF data")
		}
	}()
	return lzf.Decompress(compressed, len(compressed), outLen)
}

func readRDBRawLength(data []byte, off int) (int, int, error) {
	if off >= len(data) {
		return 0, off, fmt.Errorf("unexpected end of FUNCTION DUMP length")
	}
	b := data[off]
	off++
	switch b >> 6 {
	case rdbLen6Bit:
		return int(b & 0x3f), off, nil
	case rdbLen14Bit:
		if off >= len(data) {
			return 0, off, fmt.Errorf("unexpected end of FUNCTION DUMP length")
		}
		return int(b&0x3f)<<8 | int(data[off]), off + 1, nil
	case rdbLen32Bit:
		switch b {
		case rdbLen32BitFlag:
			if off+4 > len(data) {
				return 0, off, fmt.Errorf("unexpected end of FUNCTION DUMP length")
			}
			n := binary.BigEndian.Uint32(data[off:])
			if n > maxFunctionDumpStringSize {
				return 0, off, fmt.Errorf("FUNCTION DUMP length out of range")
			}
			return int(n), off + 4, nil
		case rdbLen64BitFlag:
			if off+8 > len(data) {
				return 0, off, fmt.Errorf("unexpected end of FUNCTION DUMP length")
			}
			n := binary.BigEndian.Uint64(data[off:])
			if n > maxFunctionDumpStringSize {
				return 0, off, fmt.Errorf("FUNCTION DUMP length out of range")
			}
			return int(n), off + 8, nil
		default:
			return 0, off, fmt.Errorf("invalid RDB length encoding")
		}
	default:
		return 0, off, fmt.Errorf("invalid RDB length encoding")
	}
}

func makeFunctionDumpError(err error) redis.Reply {
	msg := err.Error()
	if strings.HasPrefix(msg, "ERR ") {
		return protocol.MakeErrReply(msg)
	}
	return protocol.MakeErrReply("ERR " + msg)
}
