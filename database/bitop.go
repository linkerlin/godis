package database

import (
	"strings"

	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// execBitOp BITOP <operation> destkey key [key ...]
// operations: AND OR XOR NOT DIFF DIFF1 ANDOR ONE
func execBitOp(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'bitop' command")
	}
	op := strings.ToUpper(string(args[0]))
	dest := string(args[1])
	srcKeys := make([]string, len(args)-2)
	for i := 2; i < len(args); i++ {
		srcKeys[i-2] = string(args[i])
	}

	switch op {
	case "NOT":
		if len(srcKeys) != 1 {
			return protocol.MakeErrReply("ERR BITOP NOT must be called with a single source key.")
		}
	case "AND", "OR", "XOR", "ONE":
		if len(srcKeys) < 1 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'bitop' command")
		}
	case "DIFF", "DIFF1", "ANDOR":
		if len(srcKeys) < 2 {
			return protocol.MakeErrReply("ERR BITOP " + op + " must be called with at least two source keys.")
		}
	default:
		return protocol.MakeErrReply("ERR syntax error")
	}

	srcs := make([][]byte, len(srcKeys))
	maxlen := 0
	for i, k := range srcKeys {
		bs, errReply := db.getAsString(k)
		if errReply != nil {
			return errReply
		}
		if bs == nil {
			bs = []byte{}
		}
		srcs[i] = bs
		if len(bs) > maxlen {
			maxlen = len(bs)
		}
	}

	result := make([]byte, maxlen)
	switch op {
	case "AND":
		if maxlen == 0 {
			break
		}
		copy(result, padBytes(srcs[0], maxlen))
		for i := 1; i < len(srcs); i++ {
			s := padBytes(srcs[i], maxlen)
			for j := 0; j < maxlen; j++ {
				result[j] &= s[j]
			}
		}
	case "OR":
		for i := 0; i < len(srcs); i++ {
			s := padBytes(srcs[i], maxlen)
			for j := 0; j < maxlen; j++ {
				result[j] |= s[j]
			}
		}
	case "XOR":
		for i := 0; i < len(srcs); i++ {
			s := padBytes(srcs[i], maxlen)
			for j := 0; j < maxlen; j++ {
				result[j] ^= s[j]
			}
		}
	case "NOT":
		s := padBytes(srcs[0], maxlen)
		for j := 0; j < maxlen; j++ {
			result[j] = ^s[j]
		}
	case "DIFF":
		// X & ~(Y1|Y2|...)
		x := padBytes(srcs[0], maxlen)
		orY := make([]byte, maxlen)
		for i := 1; i < len(srcs); i++ {
			s := padBytes(srcs[i], maxlen)
			for j := 0; j < maxlen; j++ {
				orY[j] |= s[j]
			}
		}
		for j := 0; j < maxlen; j++ {
			result[j] = x[j] & ^orY[j]
		}
	case "DIFF1":
		// ~X & (Y1|Y2|...)
		x := padBytes(srcs[0], maxlen)
		orY := make([]byte, maxlen)
		for i := 1; i < len(srcs); i++ {
			s := padBytes(srcs[i], maxlen)
			for j := 0; j < maxlen; j++ {
				orY[j] |= s[j]
			}
		}
		for j := 0; j < maxlen; j++ {
			result[j] = ^x[j] & orY[j]
		}
	case "ANDOR":
		// X & (Y1|Y2|...)
		x := padBytes(srcs[0], maxlen)
		orY := make([]byte, maxlen)
		for i := 1; i < len(srcs); i++ {
			s := padBytes(srcs[i], maxlen)
			for j := 0; j < maxlen; j++ {
				orY[j] |= s[j]
			}
		}
		for j := 0; j < maxlen; j++ {
			result[j] = x[j] & orY[j]
		}
	case "ONE":
		// bit set in exactly one source (raw byte bit order: bit0 = LSB)
		for j := 0; j < maxlen; j++ {
			var byteOut byte
			for bit := 0; bit < 8; bit++ {
				mask := byte(1 << bit)
				cnt := 0
				for i := 0; i < len(srcs); i++ {
					s := padBytes(srcs[i], maxlen)
					if s[j]&mask != 0 {
						cnt++
					}
				}
				if cnt == 1 {
					byteOut |= mask
				}
			}
			result[j] = byteOut
		}
	}

	// Trim trailing zeros? Redis keeps full length of longest input for AND/OR/XOR/NOT
	if maxlen == 0 {
		db.Remove(dest)
	} else {
		db.PutEntity(dest, &database.DataEntity{Data: result})
	}
	db.addAof(utils.ToCmdLine3("bitop", args...))
	return protocol.MakeIntReply(int64(maxlen))
}

func padBytes(b []byte, n int) []byte {
	if len(b) >= n {
		return b[:n]
	}
	out := make([]byte, n)
	copy(out, b)
	return out
}

func prepareBitOp(args [][]byte) ([]string, []string) {
	// dest + all sources are written/read; lock all as write for simplicity
	if len(args) < 3 {
		return nil, nil
	}
	keys := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys = append(keys, string(args[i]))
	}
	return keys, nil
}

func init() {
	registerCommand("BitOp", execBitOp, prepareBitOp, rollbackFirstKey, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 2, -1, 1)
}
