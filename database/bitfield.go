package database

import (
	"strconv"
	"strings"

	"github.com/linkerlin/godis/datastruct/bitmap"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

const (
	overflowWrap = iota
	overflowSat
	overflowFail
)

// execBitField BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset increment]
// [OVERFLOW WRAP|SAT|FAIL]
func execBitField(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'bitfield' command")
	}
	key := string(args[0])
	bs, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bs == nil {
		bs = []byte{}
	}
	// Work on a copy so FAIL can abort without partial writes from earlier ops in same command —
	// Redis applies ops sequentially; FAIL only skips the failing INCRBY and keeps prior changes.
	bm := bitmap.FromBytes(append([]byte(nil), bs...))
	overflow := overflowWrap
	var replies []redis.Reply
	modified := false

	i := 1
	for i < len(args) {
		op := strings.ToUpper(string(args[i]))
		switch op {
		case "OVERFLOW":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			mode := strings.ToUpper(string(args[i+1]))
			switch mode {
			case "WRAP":
				overflow = overflowWrap
			case "SAT":
				overflow = overflowSat
			case "FAIL":
				overflow = overflowFail
			default:
				return protocol.MakeErrReply("ERR Invalid OVERFLOW type specified")
			}
			i += 2
		case "GET":
			if i+2 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			bitSize, signed, err := parseBitfieldType(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply(err.Error())
			}
			offset, err := parseBitfieldOffset(string(args[i+2]), bitSize)
			if err != nil {
				return protocol.MakeErrReply(err.Error())
			}
			val := getBitfieldBits(bm, offset, bitSize, signed)
			replies = append(replies, protocol.MakeIntReply(val))
			i += 3
		case "SET":
			if i+3 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			bitSize, signed, err := parseBitfieldType(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply(err.Error())
			}
			offset, err := parseBitfieldOffset(string(args[i+2]), bitSize)
			if err != nil {
				return protocol.MakeErrReply(err.Error())
			}
			setVal, err := strconv.ParseInt(string(args[i+3]), 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			old := getBitfieldBits(bm, offset, bitSize, signed)
			setBitfieldBits(bm, offset, bitSize, setVal)
			modified = true
			replies = append(replies, protocol.MakeIntReply(old))
			i += 4
		case "INCRBY":
			if i+3 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			bitSize, signed, err := parseBitfieldType(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply(err.Error())
			}
			offset, err := parseBitfieldOffset(string(args[i+2]), bitSize)
			if err != nil {
				return protocol.MakeErrReply(err.Error())
			}
			incr, err := strconv.ParseInt(string(args[i+3]), 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			cur := getBitfieldBits(bm, offset, bitSize, signed)
			next, ok := bitfieldIncr(cur, incr, bitSize, signed, overflow)
			if !ok {
				replies = append(replies, protocol.MakeNullBulkReply())
				i += 4
				continue
			}
			setBitfieldBits(bm, offset, bitSize, next)
			modified = true
			replies = append(replies, protocol.MakeIntReply(next))
			i += 4
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	if modified {
		db.PutEntity(key, &database.DataEntity{Data: bm.ToBytes()})
		db.addAof(utils.ToCmdLine3("bitfield", args...))
	}
	return protocol.MakeMultiRawReply(replies)
}

func parseBitfieldType(t string) (bits int, signed bool, err error) {
	if len(t) < 2 {
		return 0, false, errBitfieldType
	}
	switch t[0] {
	case 'i', 'I':
		signed = true
	case 'u', 'U':
		signed = false
	default:
		return 0, false, errBitfieldType
	}
	n, e := strconv.Atoi(t[1:])
	if e != nil || n < 1 {
		return 0, false, errBitfieldType
	}
	if signed && n > 64 {
		return 0, false, errBitfieldType
	}
	if !signed && n > 63 {
		return 0, false, errBitfieldType
	}
	return n, signed, nil
}

var errBitfieldType = &bitfieldTypeError{}

type bitfieldTypeError struct{}

func (e *bitfieldTypeError) Error() string {
	return "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."
}

func parseBitfieldOffset(s string, bitSize int) (int64, error) {
	if len(s) > 0 && s[0] == '#' {
		idx, err := strconv.ParseInt(s[1:], 10, 64)
		if err != nil || idx < 0 {
			return 0, errBitfieldOffset
		}
		return idx * int64(bitSize), nil
	}
	off, err := strconv.ParseInt(s, 10, 64)
	if err != nil || off < 0 {
		return 0, errBitfieldOffset
	}
	return off, nil
}

var errBitfieldOffset = &bitfieldOffsetError{}

type bitfieldOffsetError struct{}

func (e *bitfieldOffsetError) Error() string {
	return "ERR bit offset is not an integer or out of range"
}

func getBitfieldBits(bm *bitmap.BitMap, offset int64, bits int, signed bool) int64 {
	var val uint64
	for i := 0; i < bits; i++ {
		val <<= 1
		if bm.GetBit(offset+int64(i)) > 0 {
			val |= 1
		}
	}
	if signed {
		signBit := uint64(1) << (bits - 1)
		if val&signBit != 0 {
			// sign extend
			mask := (uint64(1) << bits) - 1
			return int64(val | ^mask)
		}
	}
	return int64(val)
}

func setBitfieldBits(bm *bitmap.BitMap, offset int64, bits int, value int64) {
	u := uint64(value)
	for i := 0; i < bits; i++ {
		bit := byte(0)
		if u&(1<<uint(bits-1-i)) != 0 {
			bit = 1
		}
		bm.SetBit(offset+int64(i), bit)
	}
}

func bitfieldIncr(cur, incr int64, bits int, signed bool, overflow int) (int64, bool) {
	var min, max int64
	if signed {
		max = (1 << (bits - 1)) - 1
		min = -(1 << (bits - 1))
	} else {
		min = 0
		if bits == 63 {
			max = (1 << 63) - 1
		} else {
			max = (1 << bits) - 1
		}
	}
	next := cur + incr
	switch overflow {
	case overflowFail:
		if next > max || next < min {
			return 0, false
		}
		return next, true
	case overflowSat:
		if next > max {
			return max, true
		}
		if next < min {
			return min, true
		}
		return next, true
	default: // WRAP
		rangeSize := max - min + 1
		if rangeSize <= 0 {
			return next, true
		}
		// Bring into [min, max] with wrap
		mod := ((next-min)%rangeSize + rangeSize) % rangeSize
		return min + mod, true
	}
}

func init() {
	registerCommand("BitField", execBitField, writeFirstKey, rollbackFirstKey, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("BitField_Ro", execBitFieldRo, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
}

// execBitFieldRo is BITFIELD_RO — only GET subcommands are allowed.
func execBitFieldRo(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'bitfield_ro' command")
	}
	for i := 1; i < len(args); {
		op := strings.ToUpper(string(args[i]))
		switch op {
		case "GET":
			if i+2 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			i += 3
		case "SET", "INCRBY", "OVERFLOW":
			return protocol.MakeErrReply("ERR BITFIELD_RO only supports the GET subcommand")
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}
	return execBitField(db, args)
}
