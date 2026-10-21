package database

// Redis 8.8 Array preview subset (R5-19): ARSET/ARMSET/ARGET/ARMGET/
// ARGETRANGE/ARLEN/ARCOUNT/ARDEL/ARDELRANGE. TYPE reports "array";
// persistence goes through the Godis opaque envelope. ARINSERT/ARRING/
// AROP/ARGREP/ARINFO etc. wait until this subset is stable (official preview).

import (
	"strconv"

	"github.com/linkerlin/godis/datastruct/array"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

func (db *DB) getAsArray(key string) (*array.Array, redis.Reply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	arr, ok := entity.Data.(*array.Array)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return arr, nil
}

func (db *DB) getOrInitArray(key string) (*array.Array, bool, redis.Reply) {
	arr, errReply := db.getAsArray(key)
	if errReply != nil {
		return nil, false, errReply
	}
	if arr != nil {
		return arr, false, nil
	}
	arr = array.New()
	db.PutEntity(key, &database.DataEntity{Data: arr})
	return arr, true, nil
}

// execARSet ARSET key index value [value ...] — contiguous writes,
// returns the number of new (previously empty) slots filled.
func execARSet(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'arset' command")
	}
	index, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil || index < 0 || index+int64(len(args)-3) > array.MaxIndex {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	arr, _, errReply := db.getOrInitArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	fresh := 0
	for i, v := range args[2:] {
		fresh += arr.Set(index+int64(i), v)
	}
	db.addAof(utils.ToCmdLine3("arset", args...))
	return protocol.MakeIntReply(int64(fresh))
}

// execARMSet ARMSET key index value [index value ...] — sparse pairs,
// returns the number of new slots filled.
func execARMSet(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'armset' command")
	}
	type pair struct {
		idx int64
		val []byte
	}
	pairs := make([]pair, 0, (len(args)-1)/2)
	for i := 1; i+1 < len(args); i += 2 {
		idx, err := strconv.ParseInt(string(args[i]), 10, 64)
		if err != nil || idx < 0 || idx > array.MaxIndex {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		pairs = append(pairs, pair{idx, args[i+1]})
	}
	arr, _, errReply := db.getOrInitArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	fresh := 0
	for _, p := range pairs {
		fresh += arr.Set(p.idx, p.val)
	}
	db.addAof(utils.ToCmdLine3("armset", args...))
	return protocol.MakeIntReply(int64(fresh))
}

// execARGet ARGET key index — bulk or nil for missing key/index/empty slot.
func execARGet(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'arget' command")
	}
	index, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if arr == nil {
		return &protocol.NullBulkReply{}
	}
	if v, ok := arr.Get(index); ok {
		return protocol.MakeBulkReply(v)
	}
	return &protocol.NullBulkReply{}
}

// execARMGet ARMGET key index [index ...] — nil for unset indices.
func execARMGet(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'armget' command")
	}
	indices := make([]int64, 0, len(args)-1)
	for _, a := range args[1:] {
		idx, err := strconv.ParseInt(string(a), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		indices = append(indices, idx)
	}
	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	result := make([]redis.Reply, len(indices))
	for i, idx := range indices {
		if arr != nil {
			if v, ok := arr.Get(idx); ok {
				result[i] = protocol.MakeBulkReply(v)
				continue
			}
		}
		result[i] = &protocol.NullBulkReply{}
	}
	return protocol.MakeMultiRawReply(result)
}

// execARGetRange ARGETRANGE key start end — inclusive; start>end reverses.
func execARGetRange(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'argetrange' command")
	}
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	end, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if arr == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}
	slots := arr.Range(start, end)
	result := make([]redis.Reply, len(slots))
	for i, s := range slots {
		if s == nil {
			result[i] = &protocol.NullBulkReply{}
		} else {
			result[i] = protocol.MakeBulkReply(s)
		}
	}
	return protocol.MakeMultiRawReply(result)
}

// execARLen ARLEN key — max index + 1, 0 when missing.
func execARLen(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'arlen' command")
	}
	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if arr == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(arr.Len())
}

// execARCount ARCOUNT key — number of non-empty slots, 0 when missing.
func execARCount(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'arcount' command")
	}
	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if arr == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(arr.Count())
}

// execARDel ARDEL key index [index ...] — count of non-empty slots deleted.
func execARDel(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ardel' command")
	}
	indices := make([]int64, 0, len(args)-1)
	for _, a := range args[1:] {
		idx, err := strconv.ParseInt(string(a), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		indices = append(indices, idx)
	}
	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if arr == nil {
		return protocol.MakeIntReply(0)
	}
	deleted := arr.Del(indices...)
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("ardel", args...))
	}
	return protocol.MakeIntReply(deleted)
}

// execARDelRange ARDELRANGE key start end [start end ...] — inclusive
// ranges, start>end normalized, overlaps counted once.
func execARDelRange(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ardelrange' command")
	}
	ranges := make([][2]int64, 0, (len(args)-1)/2)
	for i := 1; i+1 < len(args); i += 2 {
		lo, err := strconv.ParseInt(string(args[i]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		hi, err := strconv.ParseInt(string(args[i+1]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		ranges = append(ranges, [2]int64{lo, hi})
	}
	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if arr == nil {
		return protocol.MakeIntReply(0)
	}
	deleted := arr.DelRanges(ranges)
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("ardelrange", args...))
	}
	return protocol.MakeIntReply(deleted)
}

func init() {
	registerCommand("ARSet", execARSet, writeFirstKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("ARMSet", execARMSet, writeFirstKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("ARGet", execARGet, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ARMGet", execARMGet, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ARGetRange", execARGetRange, readFirstKey, nil, 4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ARLen", execARLen, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ARCount", execARCount, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ARDel", execARDel, writeFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("ARDelRange", execARDelRange, writeFirstKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
}
