package database

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/linkerlin/godis/aof"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/lib/validate"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/zeebo/xxh3"
)

// valueDigest returns the Redis 8.4+ hash digest of a string value: the
// 64-bit XXH3 hash formatted as a lowercase, zero-padded 16-char hex string.
func valueDigest(value []byte) string {
	return fmt.Sprintf("%016x", xxh3.Hash(value))
}

// execDigest DIGEST key (Redis 8.4)
func execDigest(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'digest' command")
	}
	key := string(args[0])
	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes == nil {
		return &protocol.NullBulkReply{}
	}
	return protocol.MakeBulkReply([]byte(valueDigest(bytes)))
}

// execDelEx DELEX key [IFEQ v | IFNE v | IFDEQ digest | IFDNE digest] (Redis 8.4)
// Without an option it deletes regardless of type, like DEL on one key.
// With an option the key must hold a string; otherwise WRONGTYPE.
func execDelEx(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 || len(args) > 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'delex' command")
	}
	key := string(args[0])
	if len(args) == 1 {
		if _, ok := db.GetEntity(key); !ok {
			return protocol.MakeIntReply(0)
		}
		db.Remove(key)
		db.addAof(utils.ToCmdLine3("delex", args...))
		return protocol.MakeIntReply(1)
	}
	if len(args) != 3 {
		return protocol.MakeSyntaxErrReply()
	}

	entity, exists := db.GetEntity(key)
	if !exists || entity == nil {
		return protocol.MakeIntReply(0)
	}
	cur, isStr := entity.Data.([]byte)
	if !isStr {
		return &protocol.WrongTypeErrReply{}
	}

	operand := string(args[2])
	var condOK bool
	switch strings.ToUpper(string(args[1])) {
	case "IFEQ":
		condOK = string(cur) == operand
	case "IFNE":
		condOK = string(cur) != operand
	case "IFDEQ":
		condOK = valueDigest(cur) == strings.ToLower(operand)
	case "IFDNE":
		condOK = valueDigest(cur) != strings.ToLower(operand)
	default:
		return protocol.MakeSyntaxErrReply()
	}
	if !condOK {
		return protocol.MakeIntReply(0)
	}
	db.Remove(key)
	db.addAof(utils.ToCmdLine3("delex", args...))
	return protocol.MakeIntReply(1)
}

// execMSetEX MSETEX numkeys key value [key value ...] [NX | XX]
// [EX seconds | PX milliseconds | EXAT unix-time-seconds |
//
//	PXAT unix-time-milliseconds | KEEPTTL]  (Redis 8.4)
//
// All-or-nothing: NX requires none of the keys to exist, XX requires all of
// them to exist. Returns 1 when all keys were set, 0 otherwise.
func execMSetEX(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'msetex' command")
	}
	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	if numKeys <= 0 {
		return protocol.MakeErrReply("ERR numkeys should be greater than 0")
	}
	if len(args) < 1+2*numKeys {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'msetex' command")
	}
	pairs := args[1 : 1+2*numKeys]
	for i := 1; i < len(pairs); i += 2 {
		if err := validate.ValidateValue(pairs[i]); err != nil {
			return protocol.MakeErrReply("ERR value too large")
		}
	}

	condition := ""
	keepTTL := false
	ttl := int64(0)       // relative ms
	absExpire := int64(0) // unix ms
	// opts records the raw expiration tokens for the absolute-AOF replay.
	opts := [][]byte{}

	rest := args[1+2*numKeys:]
	for i := 0; i < len(rest); i++ {
		arg := strings.ToUpper(string(rest[i]))
		switch arg {
		case "NX", "XX":
			if condition != "" {
				return protocol.MakeSyntaxErrReply()
			}
			condition = arg
		case "KEEPTTL":
			if keepTTL || ttl != 0 || absExpire != 0 {
				return protocol.MakeSyntaxErrReply()
			}
			keepTTL = true
		case "EX", "PX", "EXAT", "PXAT":
			if i+1 >= len(rest) || keepTTL || ttl != 0 || absExpire != 0 {
				return protocol.MakeSyntaxErrReply()
			}
			n, perr := strconv.ParseInt(string(rest[i+1]), 10, 64)
			if perr != nil || n <= 0 {
				return protocol.MakeErrReply("ERR invalid expire time in 'msetex' command")
			}
			switch arg {
			case "EX":
				ttl = n * 1000
			case "PX":
				ttl = n
			case "EXAT":
				absExpire = n * 1000
			case "PXAT":
				absExpire = n
			}
			opts = [][]byte{rest[i], rest[i+1]}
			i++
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	for i := 0; i < numKeys; i++ {
		key := string(pairs[2*i])
		_, exists := db.GetEntity(key)
		if condition == "NX" && exists {
			return protocol.MakeIntReply(0)
		}
		if condition == "XX" && !exists {
			return protocol.MakeIntReply(0)
		}
	}

	for i := 0; i < numKeys; i++ {
		key := string(pairs[2*i])
		db.PutEntity(key, &database.DataEntity{Data: pairs[2*i+1]})
		removeKeyFromIndex(db, key)
		switch {
		case absExpire != 0:
			db.Expire(key, time.Unix(0, absExpire*int64(time.Millisecond)))
		case ttl != 0:
			db.Expire(key, time.Now().Add(time.Duration(ttl)*time.Millisecond))
		case !keepTTL:
			db.Persist(key)
		}
	}

	// AOF: the base line carries NX/XX; relative expirations are replayed as
	// absolute PEXPIREAT commands (deterministic on reload).
	base := make(CmdLine, 0, 2+2*numKeys+len(opts))
	base = append(base, []byte("msetex"), args[0])
	base = append(base, pairs...)
	if condition != "" {
		base = append(base, []byte(condition))
	}
	if absExpire != 0 {
		base = append(base, opts...)
	}
	db.addAof(base)
	if ttl != 0 {
		for i := 0; i < numKeys; i++ {
			key := string(pairs[2*i])
			raw, ok := db.ttlMap.GetWithLock(key)
			if !ok {
				continue
			}
			if expireTime, ok := raw.(time.Time); ok {
				db.addAof(aof.MakeExpireCmd(key, expireTime).Args)
			}
		}
	}
	return protocol.MakeIntReply(1)
}

func prepareMSetEX(args [][]byte) ([]string, []string) {
	if len(args) < 1 {
		return nil, nil
	}
	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil || numKeys < 1 || len(args) < 1+2*numKeys {
		return nil, nil
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[1+2*i])
	}
	return keys, nil
}

func init() {
	registerCommand("Digest", execDigest, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("DelEx", execDelEx, writeFirstKey, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("MSetEX", execMSetEX, prepareMSetEX, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, -1, 2)
}
