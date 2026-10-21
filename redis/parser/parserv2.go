package parser

import (
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"
)

// ParseFrame parses a single complete RESP command frame from a byte slice without
// mutating or consuming bytes when the frame is incomplete (TCP half-packet safe).
// Returns:
//   - cmdLine: parsed command arguments (copied so caller may discard the buffer)
//   - consumed: number of bytes consumed from buf when complete == true
//   - complete: true when a full frame was parsed; false when more bytes are needed
//   - err: non-nil on malformed RESP syntax
func ParseFrame(buf []byte) (cmdLine [][]byte, consumed int, complete bool, err error) {
	if len(buf) == 0 {
		return nil, 0, false, nil
	}
	if buf[0] != '*' {
		idx := bytes.IndexByte(buf, '\n')
		if idx < 0 {
			return nil, 0, false, nil
		}
		line := buf[:idx]
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		consumed = idx + 1
		if len(line) == 0 {
			return nil, consumed, true, nil
		}
		parts := strings.Fields(string(line))
		result := make([][]byte, len(parts))
		for i, s := range parts {
			result[i] = []byte(s)
		}
		return result, consumed, true, nil
	}

	idx := bytes.IndexByte(buf, '\n')
	if idx < 0 {
		return nil, 0, false, nil
	}
	if idx < 2 || buf[idx-1] != '\r' {
		return nil, 0, false, errors.New("protocol error: illegal array header")
	}
	count, convErr := strconv.Atoi(string(buf[1 : idx-1]))
	if convErr != nil {
		return nil, 0, false, errors.New("protocol error: illegal array header")
	}
	pos := idx + 1
	if count <= 0 {
		return nil, pos, true, nil
	}
	if int64(count) > maxArrayElements {
		return nil, 0, false, errors.New("protocol error: array too long")
	}

	result := make([][]byte, count)
	for i := 0; i < count; i++ {
		if pos >= len(buf) {
			return nil, 0, false, nil
		}
		if buf[pos] != '$' {
			return nil, 0, false, errors.New("protocol error: expected '$', got '" + string(buf[pos:pos+1]) + "'")
		}
		relIdx := bytes.IndexByte(buf[pos:], '\n')
		if relIdx < 0 {
			return nil, 0, false, nil
		}
		lineEnd := pos + relIdx
		if relIdx < 2 || buf[lineEnd-1] != '\r' {
			return nil, 0, false, errors.New("protocol error: illegal bulk string header")
		}
		strLen, lenErr := strconv.Atoi(string(buf[pos+1 : lineEnd-1]))
		if lenErr != nil || strLen < -1 {
			return nil, 0, false, errors.New("protocol error: illegal bulk string length")
		}
		pos = lineEnd + 1
		if strLen == -1 {
			result[i] = nil
			continue
		}
		if int64(strLen) > maxBulkStringLen() {
			return nil, 0, false, errors.New("protocol error: bulk string too long")
		}
		if pos+strLen+2 > len(buf) {
			return nil, 0, false, nil
		}
		if buf[pos+strLen] != '\r' || buf[pos+strLen+1] != '\n' {
			return nil, 0, false, errors.New("protocol error: bad bulk string CRLF")
		}
		arg := make([]byte, strLen)
		copy(arg, buf[pos:pos+strLen])
		result[i] = arg
		pos += strLen + 2
	}
	return result, pos, true, nil
}

func ParseV2(r io.Reader) ([][]byte, error) {
	// 读取起始字符 '*'
	buf := make([]byte, 1)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}
	if buf[0] != '*' {
		// try text protocol
		buf2, err := readLine(r)
		if err != nil {
			return nil, err
		}
		buf = append(buf, buf2...)
		line := strings.Split(string(buf), " ")
		result := make([][]byte, len(line))
		for i, s := range line {
			result[i] = []byte(s)
		}
		return result, nil
	}

	// 读取参数数量
	count, err := readInteger(r)
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, nil
	}
	if int64(count) > maxArrayElements {
		return nil, errors.New("array too long")
	}

	// 读取每个参数
	result := make([][]byte, count)
	for i := 0; i < count; i++ {
		// 读取类型前缀
		_, err := io.ReadFull(r, buf)
		if err != nil {
			return nil, err
		}

		switch buf[0] {
		case '$': // Bulk String
			strLen, err := readInteger(r)
			if err != nil {
				return nil, err
			}
			if strLen < 0 {
				result[i] = nil // Null Bulk String
				continue
			}
			if int64(strLen) > maxBulkStringLen() {
				return nil, errors.New("bulk string too long")
			}

			data := make([]byte, strLen+2)
			_, err = io.ReadFull(r, data)
			if err != nil {
				return nil, err
			}
			result[i] = data[:strLen]

		// case '+', ':': // Simple String or Integer
		// 	simpleStr, err := readLine(r)
		// 	if err != nil {
		// 		return nil, err
		// 	}
		// 	result[i] = []byte(simpleStr)

		// case '-': // Error
		// 	errStr, err := readLine(r)
		// 	if err != nil {
		// 		return nil, err
		// 	}
		// 	result[i] = []byte(errStr)

		default:
			return nil, errors.New("unsupported RESP type")
		}
	}

	return result, nil
}

func readInteger(r io.Reader) (int, error) {
	line, err := readLine(r)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(string(line))
}

func readLine(r io.Reader) ([]byte, error) {
	var line []byte
	buf := make([]byte, 1)

	for {
		_, err := io.ReadFull(r, buf)
		if err != nil {
			return nil, err
		}

		switch buf[0] {
		case '\r':
			_, err := io.ReadFull(r, buf)
			if err != nil {
				return nil, errors.New("unexpected EOF after \\r")
			}
			if buf[0] != '\n' {
				return nil, errors.New("expected \\n after \\r")
			}
			return line, nil
		default:
			line = append(line, buf[0])
		}
	}
}
