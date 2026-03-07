package database

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/hdt3213/godis/datastruct/probabilistic"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
)

// === Bloom Filter Commands ===

// execBFReserve creates a new Bloom filter
// BF.RESERVE key error_rate capacity [EXPANSION expansion] [NONSCALING]
func execBFReserve(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'bf.reserve' command")
	}
	
	key := string(args[0])
	
	errorRate, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR Error rate must be a double")
	}
	
	capacity, err := strconv.ParseUint(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR Capacity must be an integer")
	}
	
	// Check if key exists
	_, exists := db.GetEntity(key)
	if exists {
		return protocol.MakeErrReply("ERR key already exists")
	}
	
	bf := probabilistic.NewBloomFilter(uint(capacity), errorRate)
	db.PutEntity(key, &database.DataEntity{Data: bf})
	
	db.addAof(prependCmd("bf.reserve", args))
	return protocol.MakeOkReply()
}

// execBFAdd adds an element to a Bloom filter
// BF.ADD key item
func execBFAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'bf.add' command")
	}
	
	key := string(args[0])
	item := args[1]
	
	entity, exists := db.GetEntity(key)
	var bf *probabilistic.BloomFilter
	
	if !exists {
		// Auto-create with defaults
		bf = probabilistic.NewBloomFilter(1000, 0.001)
		db.PutEntity(key, &database.DataEntity{Data: bf})
	} else {
		var ok bool
		bf, ok = entity.Data.(*probabilistic.BloomFilter)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
	}
	
	bf.Add(item)
	
	db.addAof(prependCmd("bf.add", args))
	return protocol.MakeIntReply(1)
}

// execBFMAdd adds multiple elements
// BF.MADD key item [item ...]
func execBFMAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'bf.madd' command")
	}
	
	key := string(args[0])
	
	entity, exists := db.GetEntity(key)
	var bf *probabilistic.BloomFilter
	
	if !exists {
		bf = probabilistic.NewBloomFilter(1000, 0.001)
		db.PutEntity(key, &database.DataEntity{Data: bf})
	} else {
		var ok bool
		bf, ok = entity.Data.(*probabilistic.BloomFilter)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
	}
	
	for i := 1; i < len(args); i++ {
		bf.Add(args[i])
	}
	
	db.addAof(prependCmd("bf.madd", args))
	
	// Return array of 1s (all added)
	results := make([][]byte, len(args)-1)
	for i := range results {
		results[i] = []byte("1")
	}
	return protocol.MakeMultiBulkReply(results)
}

// execBFExists checks if an element might exist
// BF.EXISTS key item
func execBFExists(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'bf.exists' command")
	}
	
	key := string(args[0])
	item := args[1]
	
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}
	
	bf, ok := entity.Data.(*probabilistic.BloomFilter)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	if bf.Exists(item) {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// execBFInfo returns Bloom filter info
// BF.INFO key
func execBFInfo(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'bf.info' command")
	}
	
	key := string(args[0])
	
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}
	
	bf, ok := entity.Data.(*probabilistic.BloomFilter)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	info := bf.Info()
	
	var reply [][]byte
	for k, v := range info {
		reply = append(reply, []byte(k))
		reply = append(reply, []byte(fmt.Sprintf("%v", v)))
	}
	
	return protocol.MakeMultiBulkReply(reply)
}

// === Cuckoo Filter Commands ===

// execCFReserve creates a new Cuckoo filter
// CF.RESERVE key capacity [BUCKETSIZE bucketsize] [MAXITERATIONS maxiterations] [EXPANSION expansion]
func execCFReserve(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cf.reserve' command")
	}
	
	key := string(args[0])
	
	capacity, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR Capacity must be an integer")
	}
	
	_, exists := db.GetEntity(key)
	if exists {
		return protocol.MakeErrReply("ERR key already exists")
	}
	
	cf := probabilistic.NewCuckooFilter(uint(capacity))
	db.PutEntity(key, &database.DataEntity{Data: cf})
	
	db.addAof(prependCmd("cf.reserve", args))
	return protocol.MakeOkReply()
}

// execCFAdd adds an element to a Cuckoo filter
// CF.ADD key item
func execCFAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cf.add' command")
	}
	
	key := string(args[0])
	item := args[1]
	
	entity, exists := db.GetEntity(key)
	var cf *probabilistic.CuckooFilter
	
	if !exists {
		cf = probabilistic.NewCuckooFilter(1000)
		db.PutEntity(key, &database.DataEntity{Data: cf})
	} else {
		var ok bool
		cf, ok = entity.Data.(*probabilistic.CuckooFilter)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
	}
	
	if err := cf.Add(item); err != nil {
		return protocol.MakeErrReply("ERR filter is full")
	}
	
	db.addAof(prependCmd("cf.add", args))
	return protocol.MakeIntReply(1)
}

// execCFAddNX adds an element only if it doesn't exist
// CF.ADDNX key item
func execCFAddNX(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cf.addnx' command")
	}
	
	key := string(args[0])
	item := args[1]
	
	entity, exists := db.GetEntity(key)
	var cf *probabilistic.CuckooFilter
	
	if !exists {
		cf = probabilistic.NewCuckooFilter(1000)
		db.PutEntity(key, &database.DataEntity{Data: cf})
	} else {
		var ok bool
		cf, ok = entity.Data.(*probabilistic.CuckooFilter)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
	}
	
	// Check if exists first
	if cf.Exists(item) {
		return protocol.MakeIntReply(0)
	}
	
	if err := cf.Add(item); err != nil {
		return protocol.MakeErrReply("ERR filter is full")
	}
	
	db.addAof(prependCmd("cf.addnx", args))
	return protocol.MakeIntReply(1)
}

// execCFExists checks if an element exists in Cuckoo filter
// CF.EXISTS key item
func execCFExists(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cf.exists' command")
	}
	
	key := string(args[0])
	item := args[1]
	
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}
	
	cf, ok := entity.Data.(*probabilistic.CuckooFilter)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	if cf.Exists(item) {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// execCFDel deletes an element from Cuckoo filter
// CF.DEL key item
func execCFDel(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cf.del' command")
	}
	
	key := string(args[0])
	item := args[1]
	
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}
	
	cf, ok := entity.Data.(*probabilistic.CuckooFilter)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	if cf.Delete(item) {
		db.addAof(prependCmd("cf.del", args))
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// execCFCount returns the count of possible occurrences
// CF.COUNT key item
func execCFCount(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cf.count' command")
	}
	
	key := string(args[0])
	item := args[1]
	
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}
	
	cf, ok := entity.Data.(*probabilistic.CuckooFilter)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	// Cuckoo filter can return 0 or 1
	if cf.Exists(item) {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// === Count-Min Sketch Commands ===

// execCMSInitByDim creates a CMS with specified dimensions
// CMS.INITBYDIM key width depth
func execCMSInitByDim(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cms.initbydim' command")
	}
	
	key := string(args[0])
	
	width, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR Width must be an integer")
	}
	
	depth, err := strconv.ParseUint(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR Depth must be an integer")
	}
	
	_, exists := db.GetEntity(key)
	if exists {
		return protocol.MakeErrReply("ERR key already exists")
	}
	
	cms := probabilistic.NewCountMinSketch(uint(width), uint(depth))
	db.PutEntity(key, &database.DataEntity{Data: cms})
	
	db.addAof(prependCmd("cms.initbydim", args))
	return protocol.MakeOkReply()
}

// execCMSIncrBy increments item counts
// CMS.INCRBY key item increment [item increment ...]
func execCMSIncrBy(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cms.incrby' command")
	}
	
	key := string(args[0])
	
	entity, exists := db.GetEntity(key)
	var cms *probabilistic.CountMinSketch
	
	if !exists {
		// Auto-create with default dimensions
		cms = probabilistic.NewCountMinSketchFromError(0.001, 0.99)
		db.PutEntity(key, &database.DataEntity{Data: cms})
	} else {
		var ok bool
		cms, ok = entity.Data.(*probabilistic.CountMinSketch)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
	}
	
	for i := 1; i < len(args); i += 2 {
		item := args[i]
		increment, err := strconv.ParseUint(string(args[i+1]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR Increment must be an integer")
		}
		cms.IncrBy(item, increment)
	}
	
	db.addAof(prependCmd("cms.incrby", args))
	return protocol.MakeOkReply()
}

// execCMSQuery queries item counts
// CMS.QUERY key item [item ...]
func execCMSQuery(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cms.query' command")
	}
	
	key := string(args[0])
	
	entity, exists := db.GetEntity(key)
	if !exists {
		// Return 0s for all items
		results := make([][]byte, len(args)-1)
		for i := range results {
			results[i] = []byte("0")
		}
		return protocol.MakeMultiBulkReply(results)
	}
	
	cms, ok := entity.Data.(*probabilistic.CountMinSketch)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	results := make([][]byte, len(args)-1)
	for i := 1; i < len(args); i++ {
		count := cms.Query(args[i])
		results[i-1] = []byte(strconv.FormatUint(count, 10))
	}
	
	return protocol.MakeMultiBulkReply(results)
}

// === Top-K Commands ===

// execTopKReserve creates a new Top-K structure
// TOPK.RESERVE key k [width depth decay]
func execTopKReserve(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'topk.reserve' command")
	}
	
	key := string(args[0])
	
	k, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR K must be an integer")
	}
	
	_, exists := db.GetEntity(key)
	if exists {
		return protocol.MakeErrReply("ERR key already exists")
	}
	
	topk := probabilistic.NewTopK(k)
	db.PutEntity(key, &database.DataEntity{Data: topk})
	
	db.addAof(prependCmd("topk.reserve", args))
	return protocol.MakeOkReply()
}

// execTopKAdd adds items to Top-K
// TOPK.ADD key item [item ...]
func execTopKAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'topk.add' command")
	}
	
	key := string(args[0])
	
	entity, exists := db.GetEntity(key)
	var topk *probabilistic.TopK
	
	if !exists {
		// Auto-create with k=10
		topk = probabilistic.NewTopK(10)
		db.PutEntity(key, &database.DataEntity{Data: topk})
	} else {
		var ok bool
		topk, ok = entity.Data.(*probabilistic.TopK)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
	}
	
	// Return items that were dropped (if any)
	var dropped [][]byte
	
	for i := 1; i < len(args); i++ {
		item := topk.Add(args[i])
		// Check if this item caused another to be dropped
		// Simplified: just add the item
		_ = item
	}
	
	db.addAof(prependCmd("topk.add", args))
	return protocol.MakeMultiBulkReply(dropped)
}

// execTopKQuery queries if items are in Top-K
// TOPK.QUERY key item [item ...]
func execTopKQuery(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'topk.query' command")
	}
	
	key := string(args[0])
	
	entity, exists := db.GetEntity(key)
	if !exists {
		// Return 0s
		results := make([][]byte, len(args)-1)
		for i := range results {
			results[i] = []byte("0")
		}
		return protocol.MakeMultiBulkReply(results)
	}
	
	topk, ok := entity.Data.(*probabilistic.TopK)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	results := make([][]byte, len(args)-1)
	for i := 1; i < len(args); i++ {
		_, _, found := topk.Query(args[i])
		if found {
			results[i-1] = []byte("1")
		} else {
			results[i-1] = []byte("0")
		}
	}
	
	return protocol.MakeMultiBulkReply(results)
}

// execTopKList returns the Top-K list
// TOPK.LIST key [WITHCOUNT]
func execTopKList(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 || len(args) > 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'topk.list' command")
	}
	
	key := string(args[0])
	withCount := false
	
	if len(args) == 2 && strings.ToUpper(string(args[1])) == "WITHCOUNT" {
		withCount = true
	}
	
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeEmptyMultiBulkReply()
	}
	
	topk, ok := entity.Data.(*probabilistic.TopK)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	items := topk.List()
	
	var reply [][]byte
	for _, item := range items {
		reply = append(reply, []byte(item.Item))
		if withCount {
			reply = append(reply, []byte(strconv.FormatUint(item.Count, 10)))
		}
	}
	
	return protocol.MakeMultiBulkReply(reply)
}

func init() {
	// Bloom Filter
	registerCommand("BF.Reserve", execBFReserve, nil, nil, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 0, 0, 0)
	registerCommand("BF.Add", execBFAdd, nil, nil, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 0, 0, 0)
	registerCommand("BF.MAdd", execBFMAdd, nil, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 0, 0, 0)
	registerCommand("BF.Exists", execBFExists, nil, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 0, 0, 0)
	registerCommand("BF.Info", execBFInfo, nil, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
	
	// Cuckoo Filter
	registerCommand("CF.Reserve", execCFReserve, nil, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 0, 0, 0)
	registerCommand("CF.Add", execCFAdd, nil, nil, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 0, 0, 0)
	registerCommand("CF.AddNX", execCFAddNX, nil, nil, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 0, 0, 0)
	registerCommand("CF.Exists", execCFExists, nil, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 0, 0, 0)
	registerCommand("CF.Del", execCFDel, nil, nil, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("CF.Count", execCFCount, nil, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 0, 0, 0)
	
	// Count-Min Sketch
	registerCommand("CMS.InitByDim", execCMSInitByDim, nil, nil, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 0, 0, 0)
	registerCommand("CMS.IncrBy", execCMSIncrBy, nil, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("CMS.Query", execCMSQuery, nil, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 0, 0, 0)
	
	// Top-K
	registerCommand("TopK.Reserve", execTopKReserve, nil, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 0, 0, 0)
	registerCommand("TopK.Add", execTopKAdd, nil, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("TopK.Query", execTopKQuery, nil, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 0, 0, 0)
	registerCommand("TopK.List", execTopKList, nil, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
}
