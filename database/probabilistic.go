package database

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/linkerlin/godis/datastruct/probabilistic"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
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

	expansion := uint(2)
	nonScaling := false
	for i := 3; i < len(args); {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "EXPANSION":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			v, err := strconv.ParseUint(string(args[i+1]), 10, 64)
			if err != nil || v == 0 {
				return protocol.MakeErrReply("ERR EXPANSION must be a positive integer")
			}
			expansion = uint(v)
			i += 2
		case "NONSCALING":
			nonScaling = true
			i++
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	_, exists := db.GetEntity(key)
	if exists {
		return protocol.MakeErrReply("ERR key already exists")
	}

	bf := probabilistic.NewBloomFilter(uint(capacity), errorRate)
	bf.SetExpansion(expansion)
	bf.SetNonScaling(nonScaling)
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

	added := bf.Add(item)

	db.addAof(prependCmd("bf.add", args))
	if added {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
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

	results := make([][]byte, len(args)-1)
	for i := 1; i < len(args); i++ {
		if bf.Add(args[i]) {
			results[i-1] = []byte("1")
		} else {
			results[i-1] = []byte("0")
		}
	}

	db.addAof(prependCmd("bf.madd", args))
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

// execBFInsert inserts items, optionally creating the filter
// BF.INSERT key [CAPACITY cap] [ERROR error] [EXPANSION exp] [NOCREATE] [NONSCALING] ITEMS item [item ...]
func execBFInsert(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'bf.insert' command")
	}
	key := string(args[0])
	capacity := uint(1000)
	errorRate := 0.001
	noCreate := false
	i := 1
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "CAPACITY":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			c, err := strconv.ParseUint(string(args[i+1]), 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR Capacity must be an integer")
			}
			capacity = uint(c)
			i += 2
		case "ERROR":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			e, err := strconv.ParseFloat(string(args[i+1]), 64)
			if err != nil {
				return protocol.MakeErrReply("ERR Error must be a double")
			}
			errorRate = e
			i += 2
		case "EXPANSION":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			i += 2 // accepted, unused
		case "NOCREATE":
			noCreate = true
			i++
		case "NONSCALING":
			i++ // accepted, unused
		case "ITEMS":
			i++
			goto items
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}
items:
	if i >= len(args) {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'bf.insert' command")
	}
	entity, exists := db.GetEntity(key)
	var bf *probabilistic.BloomFilter
	if !exists {
		if noCreate {
			return protocol.MakeErrReply("ERR not found")
		}
		bf = probabilistic.NewBloomFilter(capacity, errorRate)
		db.PutEntity(key, &database.DataEntity{Data: bf})
	} else {
		var ok bool
		bf, ok = entity.Data.(*probabilistic.BloomFilter)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
	}
	results := make([][]byte, 0, len(args)-i)
	for ; i < len(args); i++ {
		if bf.Add(args[i]) {
			results = append(results, []byte("1"))
		} else {
			results = append(results, []byte("0"))
		}
	}
	db.addAof(prependCmd("bf.insert", args))
	return protocol.MakeMultiBulkReply(results)
}

// execBFMExists checks multiple elements
// BF.MEXISTS key item [item ...]
func execBFMExists(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'bf.mexists' command")
	}
	key := string(args[0])
	results := make([][]byte, len(args)-1)
	entity, exists := db.GetEntity(key)
	if !exists {
		for i := range results {
			results[i] = []byte("0")
		}
		return protocol.MakeMultiBulkReply(results)
	}
	bf, ok := entity.Data.(*probabilistic.BloomFilter)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	for i := 1; i < len(args); i++ {
		if bf.Exists(args[i]) {
			results[i-1] = []byte("1")
		} else {
			results[i-1] = []byte("0")
		}
	}
	return protocol.MakeMultiBulkReply(results)
}

// execBFCard returns approximate number of items added
// BF.CARD key
func execBFCard(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'bf.card' command")
	}
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		return protocol.MakeIntReply(0)
	}
	bf, ok := entity.Data.(*probabilistic.BloomFilter)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	info := bf.Info()
	count, _ := info["count"].(uint)
	return protocol.MakeIntReply(int64(count))
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
	return mapReplyFromInfo(info)
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

	bucketSize := uint(4)
	maxIter := uint(500)
	expansion := uint(1)
	for i := 2; i < len(args); {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "BUCKETSIZE":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			v, err := strconv.ParseUint(string(args[i+1]), 10, 64)
			if err != nil || v == 0 {
				return protocol.MakeErrReply("ERR BUCKETSIZE must be a positive integer")
			}
			bucketSize = uint(v)
			i += 2
		case "MAXITERATIONS":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			v, err := strconv.ParseUint(string(args[i+1]), 10, 64)
			if err != nil || v == 0 {
				return protocol.MakeErrReply("ERR MAXITERATIONS must be a positive integer")
			}
			maxIter = uint(v)
			i += 2
		case "EXPANSION":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			v, err := strconv.ParseUint(string(args[i+1]), 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR EXPANSION must be an integer")
			}
			expansion = uint(v)
			i += 2
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	_, exists := db.GetEntity(key)
	if exists {
		return protocol.MakeErrReply("ERR key already exists")
	}

	cf := probabilistic.NewCuckooFilterOpts(uint(capacity), bucketSize, maxIter)
	cf.SetExpansion(expansion)
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

	// Cuckoo filter may store duplicates; count fingerprint occurrences
	return protocol.MakeIntReply(int64(cf.CountItem(item)))
}

// execCFMExists checks multiple items
// CF.MEXISTS key item [item ...]
func execCFMExists(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cf.mexists' command")
	}
	results := make([][]byte, len(args)-1)
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		for i := range results {
			results[i] = []byte("0")
		}
		return protocol.MakeMultiBulkReply(results)
	}
	cf, ok := entity.Data.(*probabilistic.CuckooFilter)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	for i := 1; i < len(args); i++ {
		if cf.Exists(args[i]) {
			results[i-1] = []byte("1")
		} else {
			results[i-1] = []byte("0")
		}
	}
	return protocol.MakeMultiBulkReply(results)
}

// execCFInsert inserts items, optionally creating the filter
// CF.INSERT key [CAPACITY cap] [NOCREATE] ITEMS item [item ...]
func execCFInsert(db *DB, args [][]byte) redis.Reply {
	return execCFInsertInternal(db, args, false)
}

// execCFInsertNX inserts items only if not present
// CF.INSERTNX key [CAPACITY cap] [NOCREATE] ITEMS item [item ...]
func execCFInsertNX(db *DB, args [][]byte) redis.Reply {
	return execCFInsertInternal(db, args, true)
}

func execCFInsertInternal(db *DB, args [][]byte, nx bool) redis.Reply {
	cmd := "cf.insert"
	if nx {
		cmd = "cf.insertnx"
	}
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for '" + cmd + "' command")
	}
	key := string(args[0])
	capacity := uint(1000)
	noCreate := false
	i := 1
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "CAPACITY":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			c, err := strconv.ParseUint(string(args[i+1]), 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR Capacity must be an integer")
			}
			capacity = uint(c)
			i += 2
		case "NOCREATE":
			noCreate = true
			i++
		case "ITEMS":
			i++
			goto items
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}
items:
	if i >= len(args) {
		return protocol.MakeErrReply("ERR wrong number of arguments for '" + cmd + "' command")
	}
	entity, exists := db.GetEntity(key)
	var cf *probabilistic.CuckooFilter
	if !exists {
		if noCreate {
			return protocol.MakeErrReply("ERR not found")
		}
		cf = probabilistic.NewCuckooFilter(capacity)
		db.PutEntity(key, &database.DataEntity{Data: cf})
	} else {
		var ok bool
		cf, ok = entity.Data.(*probabilistic.CuckooFilter)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
	}
	results := make([][]byte, 0, len(args)-i)
	for ; i < len(args); i++ {
		item := args[i]
		if nx && cf.Exists(item) {
			results = append(results, []byte("0"))
			continue
		}
		if err := cf.Add(item); err != nil {
			return protocol.MakeErrReply("ERR filter is full")
		}
		results = append(results, []byte("1"))
	}
	db.addAof(prependCmd(cmd, args))
	return protocol.MakeMultiBulkReply(results)
}

// execCFInfo returns Cuckoo filter info as Map (RESP3 %) / flat array (RESP2).
// CF.INFO key
func execCFInfo(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cf.info' command")
	}
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}
	cf, ok := entity.Data.(*probabilistic.CuckooFilter)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	return mapReplyFromInfo(cf.Info())
}

// execCFCompact is a no-op compatibility stub (Redis may rearrange buckets)
// CF.COMPACT key
func execCFCompact(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cf.compact' command")
	}
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		return protocol.MakeErrReply("ERR not found")
	}
	if _, ok := entity.Data.(*probabilistic.CuckooFilter); !ok {
		return &protocol.WrongTypeErrReply{}
	}
	return protocol.MakeOkReply()
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

	results := make([][]byte, 0, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		item := args[i]
		increment, err := strconv.ParseUint(string(args[i+1]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR Increment must be an integer")
		}
		cms.IncrBy(item, increment)
		results = append(results, []byte(strconv.FormatUint(cms.Query(item), 10)))
	}

	db.addAof(prependCmd("cms.incrby", args))
	return protocol.MakeMultiBulkReply(results)
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

// execCMSInitByProb creates a CMS from error rate and probability
// CMS.INITBYPROB key error probability
func execCMSInitByProb(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cms.initbyprob' command")
	}
	key := string(args[0])
	errRate, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil || errRate <= 0 || errRate >= 1 {
		return protocol.MakeErrReply("ERR invalid error rate")
	}
	prob, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil || prob <= 0 || prob >= 1 {
		return protocol.MakeErrReply("ERR invalid probability")
	}
	if _, exists := db.GetEntity(key); exists {
		return protocol.MakeErrReply("ERR key already exists")
	}
	cms := probabilistic.NewCountMinSketchFromError(errRate, prob)
	db.PutEntity(key, &database.DataEntity{Data: cms})
	db.addAof(prependCmd("cms.initbyprob", args))
	return protocol.MakeOkReply()
}

// execCMSMerge merges source sketches into dest
// CMS.MERGE dest numKeys key [key ...] [WEIGHTS weight [weight ...]]
func execCMSMerge(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cms.merge' command")
	}
	destKey := string(args[0])
	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil || numKeys < 1 {
		return protocol.MakeErrReply("ERR numKeys must be a positive integer")
	}
	if len(args) < 2+numKeys {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cms.merge' command")
	}
	srcKeys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		srcKeys[i] = string(args[2+i])
	}
	weights := make([]uint64, numKeys)
	for i := range weights {
		weights[i] = 1
	}
	rest := args[2+numKeys:]
	if len(rest) > 0 {
		if strings.ToUpper(string(rest[0])) != "WEIGHTS" || len(rest) != 1+numKeys {
			return protocol.MakeErrReply("ERR syntax error")
		}
		for i := 0; i < numKeys; i++ {
			w, err := strconv.ParseUint(string(rest[1+i]), 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR weight must be an integer")
			}
			weights[i] = w
		}
	}

	destEntity, exists := db.GetEntity(destKey)
	var dest *probabilistic.CountMinSketch
	if !exists {
		// create with first source dimensions
		first, ok := db.GetEntity(srcKeys[0])
		if !ok {
			return protocol.MakeErrReply("ERR key does not exist")
		}
		src, ok := first.Data.(*probabilistic.CountMinSketch)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
		info := src.Info()
		dest = probabilistic.NewCountMinSketch(info["width"].(uint), info["depth"].(uint))
		db.PutEntity(destKey, &database.DataEntity{Data: dest})
	} else {
		var ok bool
		dest, ok = destEntity.Data.(*probabilistic.CountMinSketch)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
	}

	for i, sk := range srcKeys {
		entity, ok := db.GetEntity(sk)
		if !ok {
			return protocol.MakeErrReply("ERR key does not exist")
		}
		src, ok := entity.Data.(*probabilistic.CountMinSketch)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
		if err := dest.MergeWeighted(src, weights[i]); err != nil {
			return protocol.MakeErrReply("ERR " + err.Error())
		}
	}
	db.addAof(prependCmd("cms.merge", args))
	return protocol.MakeOkReply()
}

// execCMSInfo returns CMS dimensions
// CMS.INFO key
func execCMSInfo(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cms.info' command")
	}
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}
	cms, ok := entity.Data.(*probabilistic.CountMinSketch)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	info := cms.Info()
	m := protocol.MakeMapReply()
	m.Put("width", infoScalarReply(info["width"]))
	m.Put("depth", infoScalarReply(info["depth"]))
	m.Put("count", infoScalarReply(info["count"]))
	return m
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

	width, depth := 8, 7
	decay := 0.9
	if len(args) >= 5 {
		width, err = strconv.Atoi(string(args[2]))
		if err != nil || width <= 0 {
			return protocol.MakeErrReply("ERR width must be a positive integer")
		}
		depth, err = strconv.Atoi(string(args[3]))
		if err != nil || depth <= 0 {
			return protocol.MakeErrReply("ERR depth must be a positive integer")
		}
		decay, err = strconv.ParseFloat(string(args[4]), 64)
		if err != nil || decay <= 0 || decay > 1 {
			return protocol.MakeErrReply("ERR decay must be a float in (0,1]")
		}
	} else if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'topk.reserve' command")
	}

	_, exists := db.GetEntity(key)
	if exists {
		return protocol.MakeErrReply("ERR key already exists")
	}

	topk := probabilistic.NewTopKOpts(k, width, depth, decay)
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

	// Return items that were dropped (null if none)
	dropped := make([][]byte, len(args)-1)
	for i := 1; i < len(args); i++ {
		if name, ok := topk.Add(args[i]); ok {
			dropped[i-1] = []byte(name)
		} else {
			dropped[i-1] = nil // RESP null bulk
		}
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

// execTopKCount returns counts for items (0 if not in structure)
// TOPK.COUNT key item [item ...]
func execTopKCount(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'topk.count' command")
	}
	results := make([][]byte, len(args)-1)
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		for i := range results {
			results[i] = []byte("0")
		}
		return protocol.MakeMultiBulkReply(results)
	}
	topk, ok := entity.Data.(*probabilistic.TopK)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	for i := 1; i < len(args); i++ {
		count, _, found := topk.Query(args[i])
		if !found {
			results[i-1] = []byte("0")
		} else {
			results[i-1] = []byte(strconv.FormatUint(count, 10))
		}
	}
	return protocol.MakeMultiBulkReply(results)
}

// execTopKInfo returns Top-K info
// TOPK.INFO key
func execTopKInfo(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'topk.info' command")
	}
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}
	topk, ok := entity.Data.(*probabilistic.TopK)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	info := topk.Info()
	return mapReplyFromInfo(info)
}

// execTopKIncrBy increments item counts (via repeated Add)
// TOPK.INCRBY key item increment [item increment ...]
func execTopKIncrBy(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'topk.incrby' command")
	}
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	var topk *probabilistic.TopK
	if !exists {
		topk = probabilistic.NewTopK(10)
		db.PutEntity(key, &database.DataEntity{Data: topk})
	} else {
		var ok bool
		topk, ok = entity.Data.(*probabilistic.TopK)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
	}
	dropped := make([][]byte, 0, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		inc, err := strconv.ParseInt(string(args[i+1]), 10, 64)
		if err != nil || inc <= 0 {
			return protocol.MakeErrReply("ERR increment must be a positive integer")
		}
		var lastDropped string
		var hadDrop bool
		for j := int64(0); j < inc; j++ {
			if name, ok := topk.Add(args[i]); ok {
				lastDropped = name
				hadDrop = true
			}
		}
		if hadDrop {
			dropped = append(dropped, []byte(lastDropped))
		} else {
			dropped = append(dropped, nil)
		}
	}
	db.addAof(prependCmd("topk.incrby", args))
	return protocol.MakeMultiBulkReply(dropped)
}

func prepareCMSMerge(args [][]byte) ([]string, []string) {
	if len(args) < 3 {
		return nil, nil
	}
	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil || numKeys < 1 || len(args) < 2+numKeys {
		return []string{string(args[0])}, nil
	}
	keys := make([]string, 0, 1+numKeys)
	keys = append(keys, string(args[0]))
	for i := 0; i < numKeys; i++ {
		keys = append(keys, string(args[2+i]))
	}
	return keys, nil
}

// mapReplyFromInfo builds a RESP3 Map from *INFO-style key/value data.
func mapReplyFromInfo(info map[string]interface{}) *protocol.MapReply {
	m := protocol.MakeMapReply()
	for k, v := range info {
		m.Put(k, infoScalarReply(v))
	}
	return m
}

func infoScalarReply(v interface{}) redis.Reply {
	switch val := v.(type) {
	case float64:
		return protocol.MakeDoubleReply(val)
	case float32:
		return protocol.MakeDoubleReply(float64(val))
	case int:
		return protocol.MakeIntReply(int64(val))
	case int32:
		return protocol.MakeIntReply(int64(val))
	case int64:
		return protocol.MakeIntReply(val)
	case uint:
		return protocol.MakeIntReply(int64(val))
	case uint32:
		return protocol.MakeIntReply(int64(val))
	case uint64:
		return protocol.MakeIntReply(int64(val))
	case bool:
		return protocol.MakeBooleanReply(val)
	case string:
		return protocol.MakeBulkReply([]byte(val))
	default:
		return protocol.MakeBulkReply([]byte(fmt.Sprintf("%v", v)))
	}
}

func init() {
	// Bloom Filter
	registerCommand("BF.Reserve", execBFReserve, writeFirstKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("BF.Add", execBFAdd, writeFirstKey, nil, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("BF.MAdd", execBFMAdd, writeFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("BF.Exists", execBFExists, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("BF.Insert", execBFInsert, writeFirstKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("BF.MExists", execBFMExists, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("BF.Card", execBFCard, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("BF.Info", execBFInfo, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)

	// Cuckoo Filter
	registerCommand("CF.Reserve", execCFReserve, writeFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("CF.Add", execCFAdd, writeFirstKey, nil, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("CF.AddNX", execCFAddNX, writeFirstKey, nil, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("CF.Exists", execCFExists, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("CF.Del", execCFDel, writeFirstKey, nil, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("CF.Count", execCFCount, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("CF.MExists", execCFMExists, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("CF.Insert", execCFInsert, writeFirstKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("CF.InsertNX", execCFInsertNX, writeFirstKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("CF.Compact", execCFCompact, writeFirstKey, nil, 2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("CF.Info", execCFInfo, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)

	// Count-Min Sketch
	registerCommand("CMS.InitByDim", execCMSInitByDim, writeFirstKey, nil, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("CMS.InitByProb", execCMSInitByProb, writeFirstKey, nil, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("CMS.IncrBy", execCMSIncrBy, writeFirstKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("CMS.Query", execCMSQuery, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("CMS.Merge", execCMSMerge, prepareCMSMerge, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, -1, 1)
	registerCommand("CMS.Info", execCMSInfo, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)

	// Top-K
	registerCommand("TopK.Reserve", execTopKReserve, writeFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("TopK.Add", execTopKAdd, writeFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("TopK.IncrBy", execTopKIncrBy, writeFirstKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("TopK.Query", execTopKQuery, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("TopK.Count", execTopKCount, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("TopK.List", execTopKList, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("TopK.Info", execTopKInfo, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
