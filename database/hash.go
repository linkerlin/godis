package database

import (
	"math"
	"strconv"
	"strings"

	Dict "github.com/linkerlin/godis/datastruct/dict"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

func (db *DB) getAsDict(key string) (Dict.Dict, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	dict, ok := entity.Data.(Dict.Dict)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	if ed, ok := dict.(*Dict.ExpireDict); ok {
		db.wireExpireDict(key, ed)
	}
	return dict, nil
}

func (db *DB) getOrInitDict(key string) (dict Dict.Dict, inited bool, errReply protocol.ErrorReply) {
	dict, errReply = db.getAsDict(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if dict == nil {
		dict = Dict.MakeSimple()
		db.PutEntity(key, &database.DataEntity{
			Data: dict,
		})
		inited = true
	}
	return dict, inited, nil
}

// execHSet sets one or more fields in a hash table.
// Usage: HSET key field value [field value ...]
// Returns the number of fields that were added (not updated).
func execHSet(db *DB, args [][]byte) redis.Reply {
	// args layout: key, then one or more field/value pairs.
	if len(args) < 3 || len(args)%2 != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hset' command")
	}
	key := string(args[0])
	for i := 1; i < len(args); i++ {
		if reply := validateBulkBytes(args[i]); reply != nil {
			return reply
		}
	}

	// get or init entity
	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	added := 0
	for i := 1; i < len(args); i += 2 {
		field := string(args[i])
		value := args[i+1]
		if dict.Put(field, value) > 0 {
			added++
		}
	}
	db.addAof(utils.ToCmdLine3("hset", args...))
	notifyKeyspaceEvent(db, "hset", key)
	reindexHash(db, key)
	return protocol.MakeIntReply(int64(added))
}

func undoHSet(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	fields := make([]string, 0, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		fields = append(fields, string(args[i]))
	}
	return rollbackHashFields(db, key, fields...)
}

// execHSetNX sets field in hash table only if field not exists
func execHSetNX(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	field := string(args[1])
	value := args[2]
	if reply := validateBulkBytes(args[1]); reply != nil {
		return reply
	}
	if reply := validateBulkBytes(value); reply != nil {
		return reply
	}

	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	result := dict.PutIfAbsent(field, value)
	if result > 0 {
		db.addAof(utils.ToCmdLine3("hsetnx", args...))
		reindexHash(db, key)
	}
	return protocol.MakeIntReply(int64(result))
}

// execHGet gets field value of hash table
func execHGet(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	if reply := validateBulkBytes(args[1]); reply != nil {
		return reply
	}
	field := string(args[1])

	// get entity
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return &protocol.NullBulkReply{}
	}

	raw, exists := dict.Get(field)
	if !exists {
		return &protocol.NullBulkReply{}
	}
	value, _ := raw.([]byte)
	return protocol.MakeBulkReply(value)
}

// execHExists checks if a hash field exists
func execHExists(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	if reply := validateBulkBytes(args[1]); reply != nil {
		return reply
	}
	field := string(args[1])

	// get entity
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeIntReply(0)
	}

	_, exists := dict.Get(field)
	if exists {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// execHDel deletes a hash field
func execHDel(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = string(v)
	}
	if reply := validateBulkBytesSlice(fieldArgs); reply != nil {
		return reply
	}

	// get entity
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeIntReply(0)
	}

	removedKey := false
	deleted := 0
	for _, field := range fields {
		_, result := dict.Remove(field)
		deleted += result
	}
	if dict.Len() == 0 {
		db.Remove(key)
		removedKey = true
	}
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("hdel", args...))
		notifyKeyspaceEvent(db, "hdel", key)
		if removedKey {
			removeHashFromIndex(db, key)
		} else {
			reindexHash(db, key)
		}
	}

	return protocol.MakeIntReply(int64(deleted))
}

func undoHDel(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = string(v)
	}
	return rollbackHashFields(db, key, fields...)
}

// execHLen gets number of fields in hash table
func execHLen(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(int64(dict.Len()))
}

// execHStrlen Returns the string length of the value associated with field in the hash stored at key.
// If the key or the field do not exist, 0 is returned.
func execHStrlen(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	if reply := validateBulkBytes(args[1]); reply != nil {
		return reply
	}
	field := string(args[1])

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeIntReply(0)
	}

	raw, exists := dict.Get(field)
	if exists {
		value, _ := raw.([]byte)
		return protocol.MakeIntReply(int64(len(value)))
	}
	return protocol.MakeIntReply(0)
}

// execHMSet sets multi fields in hash table
func execHMSet(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args)%2 != 1 {
		return protocol.MakeSyntaxErrReply()
	}
	key := string(args[0])
	size := (len(args) - 1) / 2
	fields := make([]string, size)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[2*i+1])
		values[i] = args[2*i+2]
	}
	for i := 0; i < size; i++ {
		if reply := validateBulkBytes(args[2*i+1]); reply != nil {
			return reply
		}
		if reply := validateBulkBytes(args[2*i+2]); reply != nil {
			return reply
		}
	}

	// get or init entity
	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	// put data
	for i, field := range fields {
		value := values[i]
		dict.Put(field, value)
	}
	db.addAof(utils.ToCmdLine3("hmset", args...))
	reindexHash(db, key)
	return &protocol.OkReply{}
}

func undoHMSet(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	size := (len(args) - 1) / 2
	fields := make([]string, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[2*i+1])
	}
	return rollbackHashFields(db, key, fields...)
}

// execHMGet gets multi fields in hash table
func execHMGet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	if reply := validateBulkBytesSlice(args[1:]); reply != nil {
		return reply
	}
	size := len(args) - 1
	fields := make([]string, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[i+1])
	}

	// get entity
	result := make([][]byte, size)
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeMultiBulkReply(result)
	}

	for i, field := range fields {
		value, ok := dict.Get(field)
		if !ok {
			result[i] = nil
		} else {
			bytes, _ := value.([]byte)
			result[i] = bytes
		}
	}
	return protocol.MakeMultiBulkReply(result)
}

// execHKeys gets all field names in hash table
func execHKeys(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	fields := make([][]byte, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		fields[i] = []byte(key)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(fields[:i])
}

// execHVals gets all field value in hash table
func execHVals(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	// get entity
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	values := make([][]byte, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		values[i], _ = val.([]byte)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(values[:i])
}

// execHGetAll gets all key-value entries in hash table.
// Returns MapReply so RESP3 connections get % maps while RESP2 still sees a flat array.
func execHGetAll(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	// get entity
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	m := protocol.MakeMapReply()
	if dict == nil {
		return m
	}
	dict.ForEach(func(field string, val interface{}) bool {
		bytes, _ := val.([]byte)
		m.Put(field, protocol.MakeBulkReply(bytes))
		return true
	})
	return m
}

// execHIncrBy increments the integer value of a hash field by the given number
func execHIncrBy(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	rawDelta := string(args[2])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	value, exists := dict.Get(field)
	if !exists {
		dict.Put(field, args[2])
		db.addAof(utils.ToCmdLine3("hincrby", args...))
	notifyKeyspaceEvent(db, "hincrby", key)
		reindexHash(db, key)
		return protocol.MakeIntReply(delta)
	}
	val, err := strconv.ParseInt(string(value.([]byte)), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR hash value is not an integer")
	}
	if wouldIntOverflow(val, delta) {
		return protocol.MakeErrReply("ERR increment or decrement would overflow")
	}
	val += delta
	bytes := []byte(strconv.FormatInt(val, 10))
	putHashValuePreservingTTL(dict, field, bytes)
	db.addAof(utils.ToCmdLine3("hincrby", args...))
	notifyKeyspaceEvent(db, "hincrby", key)
	reindexHash(db, key)
	return protocol.MakeIntReply(val)
}

func undoHIncr(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	field := string(args[1])
	return rollbackHashFields(db, key, field)
}

// execHIncrByFloat increments the float value of a hash field by the given number
func execHIncrByFloat(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	rawDelta := string(args[2])
	delta, err := strconv.ParseFloat(rawDelta, 64)
	if err != nil || math.IsNaN(delta) {
		return protocol.MakeErrReply("ERR value is not a valid float")
	}
	if math.IsInf(delta, 0) {
		return protocol.MakeErrReply("ERR value is NaN or Infinity")
	}

	// get or init entity
	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	value, exists := dict.Get(field)
	if !exists {
		dict.Put(field, args[2])
		db.addAof(utils.ToCmdLine3("hincrbyfloat", args...))
		notifyKeyspaceEvent(db, "hincrby", key)
		reindexHash(db, key)
		return protocol.MakeDoubleReply(delta)
	}
	val, err := strconv.ParseFloat(string(value.([]byte)), 64)
	if err != nil || math.IsNaN(val) {
		return protocol.MakeErrReply("ERR hash value is not a float")
	}
	result := val + delta
	if math.IsNaN(result) || math.IsInf(result, 0) {
		return protocol.MakeErrReply("ERR increment would produce NaN or Infinity")
	}
	resultBytes := []byte(strconv.FormatFloat(result, 'f', -1, 64))
	putHashValuePreservingTTL(dict, field, resultBytes)
	db.addAof(utils.ToCmdLine3("hincrbyfloat", args...))
	notifyKeyspaceEvent(db, "hincrby", key)
	reindexHash(db, key)
	return protocol.MakeDoubleReply(result)
}

// putHashValuePreservingTTL updates a hash field value without clearing an
// existing field-level TTL. It is used by HINCRBY/HINCRBYFLOAT which should
// preserve expiration.
func putHashValuePreservingTTL(d Dict.Dict, field string, value []byte) {
	if ed, ok := d.(*Dict.ExpireDict); ok {
		_, remaining, exists := ed.GetWithExpire(field)
		if !exists {
			ed.Put(field, value)
			return
		}
		ed.SetWithTTL(field, value, remaining)
		return
	}
	d.Put(field, value)
}

func wouldIntOverflow(a, b int64) bool {
	if b > 0 && a > math.MaxInt64-b {
		return true
	}
	if b < 0 && a < math.MinInt64-b {
		return true
	}
	return false
}

// execHRandField return a random field(or field-value) from the hash value stored at key.
func execHRandField(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	countSpecified := false
	count := 1
	withvalues := false

	if len(args) > 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hrandfield' command")
	}

	if len(args) >= 2 {
		countSpecified = true
		count64, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		count = int(count64)
	}
	if len(args) == 3 {
		if strings.ToLower(string(args[2])) == "withvalues" {
			withvalues = true
		} else {
			return protocol.MakeSyntaxErrReply()
		}
	}

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil || dict.Len() == 0 {
		if !countSpecified {
			return &protocol.NullBulkReply{}
		}
		return &protocol.EmptyMultiBulkReply{}
	}

	if !countSpecified {
		fields := dict.RandomDistinctKeys(1)
		if len(fields) == 0 {
			return &protocol.NullBulkReply{}
		}
		return protocol.MakeBulkReply([]byte(fields[0]))
	}

	if count > 0 {
		fields := dict.RandomDistinctKeys(count)
		Numfield := len(fields)
		if !withvalues {
			result := make([][]byte, Numfield)
			for i, v := range fields {
				result[i] = []byte(v)
			}
			return protocol.MakeMultiBulkReply(result)
		}
		// Positive count WITHVALUES → Map in RESP3 (distinct fields; Redis forbids dups here).
		m := protocol.MakeMapReply()
		for _, v := range fields {
			raw, _ := dict.Get(v)
			bytes, _ := raw.([]byte)
			m.Put(v, protocol.MakeBulkReply(bytes))
		}
		return m
	} else if count < 0 {
		fields := dict.RandomKeys(-count)
		Numfield := len(fields)
		if !withvalues {
			result := make([][]byte, Numfield)
			for i, v := range fields {
				result[i] = []byte(v)
			}
			return protocol.MakeMultiBulkReply(result)
		}
		// Negative count may repeat fields — stay flat array (cannot be a Map).
		result := make([][]byte, 2*Numfield)
		for i, v := range fields {
			result[2*i] = []byte(v)
			raw, _ := dict.Get(v)
			result[2*i+1] = raw.([]byte)
		}
		return protocol.MakeMultiBulkReply(result)
	}

	return &protocol.EmptyMultiBulkReply{}
}

func execHScan(db *DB, args [][]byte) redis.Reply {
	var count int = 10
	var pattern string = "*"
	if len(args) > 2 {
		for i := 2; i < len(args); i++ {
			arg := strings.ToLower(string(args[i]))
			if arg == "count" {
				if i+1 >= len(args) {
					return &protocol.SyntaxErrReply{}
				}
				count0, err := strconv.Atoi(string(args[i+1]))
				if err != nil {
					return &protocol.SyntaxErrReply{}
				}
				count = count0
				i++
			} else if arg == "match" {
				if i+1 >= len(args) {
					return &protocol.SyntaxErrReply{}
				}
				pattern = string(args[i+1])
				i++
			} else {
				return &protocol.SyntaxErrReply{}
			}
		}
	}
	if len(args) < 2 {
		return &protocol.SyntaxErrReply{}
	}
	key := string(args[0])
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return emptyScanReply()
	}
	cursor, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR invalid cursor")
	}

	keysReply, nextCursor := dict.DictScan(cursor, count, pattern)
	if nextCursor < 0 {
		return protocol.MakeErrReply("ERR invalid argument")
	}

	return protocol.MakeMultiRawReply([]redis.Reply{
		protocol.MakeBulkReply([]byte(strconv.FormatInt(int64(nextCursor), 10))),
		protocol.MakeMultiBulkReply(keysReply),
	})
}

// emptyScanReply is Redis SCAN/HSCAN reply for a missing key: ["0", []].
func emptyScanReply() redis.Reply {
	return protocol.MakeMultiRawReply([]redis.Reply{
		protocol.MakeBulkReply([]byte("0")),
		protocol.MakeMultiBulkReply([][]byte{}),
	})
}

func init() {
	registerCommand("HSet", execHSet, writeFirstKey, undoHSet, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HSetNX", execHSetNX, writeFirstKey, undoHSet, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HGet", execHGet, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HExists", execHExists, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HDel", execHDel, writeFirstKey, undoHDel, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("HLen", execHLen, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HStrlen", execHStrlen, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HMSet", execHMSet, writeFirstKey, undoHMSet, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HMGet", execHMGet, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HKeys", execHKeys, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, 1, 1)
	registerCommand("HVals", execHVals, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, 1, 1)
	registerCommand("HGetAll", execHGetAll, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagRandom}, 1, 1, 1)
	registerCommand("HIncrBy", execHIncrBy, writeFirstKey, undoHIncr, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HIncrByFloat", execHIncrByFloat, writeFirstKey, undoHIncr, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HRandField", execHRandField, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagRandom, redisFlagReadonly}, 1, 1, 1)
	registerCommand("HScan", execHScan, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, 1, 1)
}
