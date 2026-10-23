package database

import (
	"fmt"
	"math"
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

// execIncrEx INCREX key [BYFLOAT inc | BYINT inc] [LBOUND lo] [UBOUND hi]
// [SATURATE] [EX s | PX ms | EXAT s | PXAT ms | PERSIST] [ENX]  (Redis 8.8)
// Atomic bounded increment. Replies [new_value, applied_increment]; when the
// result would leave the bounds the operation is skipped and the reply is
// [current_value, 0] (or saturated to the bound with SATURATE).
func execIncrEx(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	floatMode := false
	floatInc := 0.0
	intInc := int64(1)
	saturate := false
	var loSet, hiSet bool
	var loF, hiF float64
	var loI, hiI int64
	ttl := int64(0)       // relative ms
	absExpire := int64(0) // unix ms
	persist := false
	enx := false

	for i := 1; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "BYFLOAT", "BYINT":
			if floatMode || i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			if arg == "BYFLOAT" {
				f, err := strconv.ParseFloat(string(args[i+1]), 64)
				if err != nil || math.IsNaN(f) {
					return protocol.MakeErrReply("ERR value is not a valid float")
				}
				floatMode = true
				floatInc = f
			} else {
				n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				intInc = n
			}
			i++
		case "LBOUND", "UBOUND":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			if floatMode {
				f, err := strconv.ParseFloat(string(args[i+1]), 64)
				if err != nil || math.IsNaN(f) {
					return protocol.MakeErrReply("ERR value is not a valid float")
				}
				if arg == "LBOUND" {
					loF, loSet = f, true
				} else {
					hiF, hiSet = f, true
				}
			} else {
				n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				if arg == "LBOUND" {
					loI, loSet = n, true
				} else {
					hiI, hiSet = n, true
				}
			}
			i++
		case "SATURATE":
			saturate = true
		case "EX", "PX", "EXAT", "PXAT":
			if ttl != 0 || absExpire != 0 || persist || i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || n <= 0 {
				return protocol.MakeErrReply("ERR invalid expire time in 'increx' command")
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
			i++
		case "PERSIST":
			if ttl != 0 || absExpire != 0 || persist {
				return protocol.MakeSyntaxErrReply()
			}
			persist = true
		case "ENX":
			if persist {
				return protocol.MakeSyntaxErrReply()
			}
			enx = true
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}
	if enx && ttl == 0 && absExpire == 0 {
		return protocol.MakeSyntaxErrReply()
	}
	if loSet && hiSet && ((floatMode && loF > hiF) || (!floatMode && loI > hiI)) {
		return protocol.MakeErrReply("ERR LBOUND can't be greater than UBOUND")
	}

	entity, exists := db.GetEntity(key)
	curBytes := []byte(nil)
	if exists && entity != nil {
		b, isStr := entity.Data.([]byte)
		if !isStr {
			return &protocol.WrongTypeErrReply{}
		}
		curBytes = b
	}

	if floatMode {
		return incrExFloat(db, key, curBytes, floatInc, loSet, hiSet, loF, hiF,
			saturate, ttl, absExpire, persist, enx)
	}
	return incrExInt(db, key, curBytes, intInc, loSet, hiSet, loI, hiI,
		saturate, ttl, absExpire, persist, enx)
}

func incrExInt(db *DB, key string, curBytes []byte, inc int64, loSet, hiSet bool, lo, hi int64,
	saturate bool, ttl, absExpire int64, persist, enx bool) redis.Reply {
	cur := int64(0)
	if curBytes != nil {
		n, err := strconv.ParseInt(string(curBytes), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		cur = n
	}
	lbound, ubound := int64(math.MinInt64), int64(math.MaxInt64)
	if loSet {
		lbound = lo
	}
	if hiSet {
		ubound = hi
	}

	// Overflow-safe bounds check without wrapping.
	below := false // result would fall under lbound
	above := false // result would exceed ubound
	if inc > 0 {
		above = cur > ubound-inc
	} else if inc < 0 {
		below = cur < lbound-inc
	}
	if below || above {
		if !saturate {
			return protocol.MakeMultiRawReply([]redis.Reply{
				protocol.MakeIntReply(cur), protocol.MakeIntReply(0)})
		}
		target := ubound
		if below {
			target = lbound
		}
		delta := target - cur
		return applyIncrExInt(db, key, cur, target, delta, ttl, absExpire, persist, enx)
	}
	return applyIncrExInt(db, key, cur, cur+inc, inc, ttl, absExpire, persist, enx)
}

func applyIncrExInt(db *DB, key string, cur, newVal, delta int64,
	ttl, absExpire int64, persist, enx bool) redis.Reply {
	db.PutEntity(key, &database.DataEntity{Data: []byte(strconv.FormatInt(newVal, 10))})
	applyIncrExTTL(db, key, ttl, absExpire, persist, enx)
	// AOF: replay the actually-applied delta (deterministic given the same
	// prior state), then the absolute expiry effect.
	db.addAof(CmdLine{[]byte("increx"), []byte(key), []byte("BYINT"),
		[]byte(strconv.FormatInt(delta, 10))})
	writeIncrExTTLAof(db, key, ttl, absExpire, persist, enx)
	return protocol.MakeMultiRawReply([]redis.Reply{
		protocol.MakeIntReply(newVal), protocol.MakeIntReply(delta)})
}

func incrExFloat(db *DB, key string, curBytes []byte, inc float64, loSet, hiSet bool, lo, hi float64,
	saturate bool, ttl, absExpire int64, persist, enx bool) redis.Reply {
	cur := 0.0
	if curBytes != nil {
		f, err := strconv.ParseFloat(string(curBytes), 64)
		if err != nil || math.IsNaN(f) {
			return protocol.MakeErrReply("ERR value is not a valid float")
		}
		cur = f
	}
	lbound, ubound := -math.MaxFloat64, math.MaxFloat64
	if loSet {
		lbound = lo
	}
	if hiSet {
		ubound = hi
	}

	result := cur + inc
	if math.IsNaN(result) || math.IsInf(result, 0) {
		return protocol.MakeErrReply("ERR increment would produce NaN or Infinity")
	}
	if result < lbound || result > ubound {
		if !saturate {
			curRep := curBytes
			if curRep == nil {
				curRep = []byte("0")
			}
			return protocol.MakeMultiRawReply([]redis.Reply{
				protocol.MakeBulkReply(curRep), protocol.MakeBulkReply([]byte("0"))})
		}
		target := ubound
		if result < lbound {
			target = lbound
		}
		delta := target - cur
		return applyIncrExFloat(db, key, target, delta, ttl, absExpire, persist, enx)
	}
	return applyIncrExFloat(db, key, result, inc, ttl, absExpire, persist, enx)
}

func applyIncrExFloat(db *DB, key string, newVal, delta float64,
	ttl, absExpire int64, persist, enx bool) redis.Reply {
	valStr := strconv.FormatFloat(newVal, 'f', -1, 64)
	db.PutEntity(key, &database.DataEntity{Data: []byte(valStr)})
	applyIncrExTTL(db, key, ttl, absExpire, persist, enx)
	db.addAof(CmdLine{[]byte("increx"), []byte(key), []byte("BYFLOAT"),
		[]byte(strconv.FormatFloat(delta, 'f', -1, 64))})
	writeIncrExTTLAof(db, key, ttl, absExpire, persist, enx)
	return protocol.MakeMultiRawReply([]redis.Reply{
		protocol.MakeBulkReply([]byte(valStr)),
		protocol.MakeBulkReply([]byte(strconv.FormatFloat(delta, 'f', -1, 64)))})
}

// applyIncrExTTL applies the expiration effect to the key.
func applyIncrExTTL(db *DB, key string, ttl, absExpire int64, persist, enx bool) {
	_, hasTTL := db.ttlMap.GetWithLock(key)
	if enx && hasTTL {
		return
	}
	switch {
	case absExpire != 0:
		db.Expire(key, time.Unix(0, absExpire*int64(time.Millisecond)))
	case ttl != 0:
		db.Expire(key, time.Now().Add(time.Duration(ttl)*time.Millisecond))
	case persist:
		db.Persist(key)
	}
}

// writeIncrExTTLAof records the expiration EFFECT (absolute timestamps), so
// replay reproduces the same TTL decisions.
func writeIncrExTTLAof(db *DB, key string, ttl, absExpire int64, persist, enx bool) {
	_, hasTTL := db.ttlMap.GetWithLock(key)
	if enx && hasTTL {
		return // TTL left unchanged
	}
	switch {
	case absExpire != 0:
		db.addAof(aof.MakeExpireCmd(key, time.Unix(0, absExpire*int64(time.Millisecond))).Args)
	case ttl != 0:
		if raw, ok := db.ttlMap.GetWithLock(key); ok {
			if expireTime, ok := raw.(time.Time); ok {
				db.addAof(aof.MakeExpireCmd(key, expireTime).Args)
			}
		}
	case persist:
		db.addAof(utils.ToCmdLine3("persist", []byte(key)))
	}
}

func init() {
	registerCommand("IncrEx", execIncrEx, writeFirstKey, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
}
