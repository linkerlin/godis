package database

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/linkerlin/godis/datastruct/probabilistic"
	database2 "github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// execTDigestCreate creates a new T-Digest
// TDIGEST.CREATE key [COMPRESSION compression]
func execTDigestCreate(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'tdigest.create' command")
	}

	key := string(args[0])

	compression := 100.0
	if len(args) >= 3 && strings.ToUpper(string(args[1])) == "COMPRESSION" {
		compression, _ = strconv.ParseFloat(string(args[2]), 64)
	}

	_, exists := db.GetEntity(key)
	if exists {
		return protocol.MakeErrReply("ERR key already exists")
	}

	td := probabilistic.NewTDigest(compression)
	db.PutEntity(key, &database2.DataEntity{Data: td})

	db.addAof(prependCmd("tdigest.create", args))
	return protocol.MakeOkReply()
}

// execTDigestAdd adds values to a T-Digest
// TDIGEST.ADD key value [value ...]
func execTDigestAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'tdigest.add' command")
	}

	key := string(args[0])

	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}

	td, ok := entity.Data.(*probabilistic.TDigest)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	for i := 1; i < len(args); i++ {
		value, err := strconv.ParseFloat(string(args[i]), 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not a valid float")
		}
		td.Add(value, 1.0)
	}

	db.addAof(prependCmd("tdigest.add", args))
	return protocol.MakeOkReply()
}

// execTDigestQuantile returns quantiles
// TDIGEST.QUANTILE key quantile [quantile ...]
func execTDigestQuantile(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'tdigest.quantile' command")
	}

	key := string(args[0])

	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeEmptyMultiBulkReply()
	}

	td, ok := entity.Data.(*probabilistic.TDigest)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	var results [][]byte
	for i := 1; i < len(args); i++ {
		q, err := strconv.ParseFloat(string(args[i]), 64)
		if err != nil {
			results = append(results, []byte("nan"))
			continue
		}

		value := td.Quantile(q)
		results = append(results, []byte(strconv.FormatFloat(value, 'f', -1, 64)))
	}

	return protocol.MakeMultiBulkReply(results)
}

// execTDigestCDF returns CDF values
// TDIGEST.CDF key value [value ...]
func execTDigestCDF(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'tdigest.cdf' command")
	}

	key := string(args[0])

	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeEmptyMultiBulkReply()
	}

	td, ok := entity.Data.(*probabilistic.TDigest)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	var results [][]byte
	for i := 1; i < len(args); i++ {
		v, err := strconv.ParseFloat(string(args[i]), 64)
		if err != nil {
			results = append(results, []byte("nan"))
			continue
		}

		cdf := td.CDF(v)
		results = append(results, []byte(strconv.FormatFloat(cdf, 'f', -1, 64)))
	}

	return protocol.MakeMultiBulkReply(results)
}

// execTDigestInfo returns T-Digest info
// TDIGEST.INFO key
func execTDigestInfo(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'tdigest.info' command")
	}

	key := string(args[0])

	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}

	td, ok := entity.Data.(*probabilistic.TDigest)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	info := td.Info()

	var reply [][]byte
	for k, v := range info {
		reply = append(reply, []byte(k))
		switch val := v.(type) {
		case float64:
			reply = append(reply, []byte(strconv.FormatFloat(val, 'f', -1, 64)))
		case int:
			reply = append(reply, []byte(strconv.Itoa(val)))
		case int64:
			reply = append(reply, []byte(strconv.FormatInt(val, 10)))
		case uint64:
			reply = append(reply, []byte(strconv.FormatUint(val, 10)))
		default:
			reply = append(reply, []byte(fmt.Sprintf("%v", v)))
		}
	}

	return protocol.MakeMultiBulkReply(reply)
}

// execTDigestMin returns the minimum observed value
// TDIGEST.MIN key
func execTDigestMin(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'tdigest.min' command")
	}
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}
	td, ok := entity.Data.(*probabilistic.TDigest)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	return protocol.MakeBulkReply([]byte(strconv.FormatFloat(td.Min(), 'f', -1, 64)))
}

// execTDigestMax returns the maximum observed value
// TDIGEST.MAX key
func execTDigestMax(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'tdigest.max' command")
	}
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}
	td, ok := entity.Data.(*probabilistic.TDigest)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	return protocol.MakeBulkReply([]byte(strconv.FormatFloat(td.Max(), 'f', -1, 64)))
}

// execTDigestReset clears a T-Digest
// TDIGEST.RESET key
func execTDigestReset(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'tdigest.reset' command")
	}
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}
	td, ok := entity.Data.(*probabilistic.TDigest)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	td.Reset()
	db.addAof(prependCmd("tdigest.reset", args))
	return protocol.MakeOkReply()
}

// execTDigestRank returns ranks for values
// TDIGEST.RANK key value [value ...]
func execTDigestRank(db *DB, args [][]byte) redis.Reply {
	return execTDigestRankInternal(db, args, false)
}

// execTDigestRevRank returns reverse ranks for values
// TDIGEST.REVRANK key value [value ...]
func execTDigestRevRank(db *DB, args [][]byte) redis.Reply {
	return execTDigestRankInternal(db, args, true)
}

func execTDigestRankInternal(db *DB, args [][]byte, rev bool) redis.Reply {
	cmd := "tdigest.rank"
	if rev {
		cmd = "tdigest.revrank"
	}
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for '" + cmd + "' command")
	}
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}
	td, ok := entity.Data.(*probabilistic.TDigest)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	results := make([][]byte, len(args)-1)
	for i := 1; i < len(args); i++ {
		v, err := strconv.ParseFloat(string(args[i]), 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not a valid float")
		}
		var rank int64
		if rev {
			rank = td.RevRank(v)
		} else {
			rank = td.Rank(v)
		}
		results[i-1] = []byte(strconv.FormatInt(rank, 10))
	}
	return protocol.MakeMultiBulkReply(results)
}

// execTDigestByRank returns values at ranks
// TDIGEST.BYRANK key rank [rank ...]
func execTDigestByRank(db *DB, args [][]byte) redis.Reply {
	return execTDigestByRankInternal(db, args, false)
}

// execTDigestByRevRank returns values at reverse ranks
// TDIGEST.BYREVRANK key rank [rank ...]
func execTDigestByRevRank(db *DB, args [][]byte) redis.Reply {
	return execTDigestByRankInternal(db, args, true)
}

func execTDigestByRankInternal(db *DB, args [][]byte, rev bool) redis.Reply {
	cmd := "tdigest.byrank"
	if rev {
		cmd = "tdigest.byrevrank"
	}
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for '" + cmd + "' command")
	}
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}
	td, ok := entity.Data.(*probabilistic.TDigest)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	results := make([][]byte, len(args)-1)
	for i := 1; i < len(args); i++ {
		rank, err := strconv.ParseInt(string(args[i]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		var v float64
		if rev {
			v = td.ByRevRank(rank)
		} else {
			v = td.ByRank(rank)
		}
		results[i-1] = []byte(strconv.FormatFloat(v, 'f', -1, 64))
	}
	return protocol.MakeMultiBulkReply(results)
}

// execTDigestTrimmedMean TDIGEST.TRIMMED_MEAN key low_cut high_cut
func execTDigestTrimmedMean(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'tdigest.trimmed_mean' command")
	}
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}
	td, ok := entity.Data.(*probabilistic.TDigest)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	low, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not a valid float")
	}
	high, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not a valid float")
	}
	v := td.TrimmedMean(low, high)
	return protocol.MakeBulkReply([]byte(strconv.FormatFloat(v, 'f', -1, 64)))
}

// execTDigestMerge TDIGEST.MERGE dest numkeys source [source ...] [COMPRESSION c] [OVERRIDE]
func execTDigestMerge(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'tdigest.merge' command")
	}
	dest := string(args[0])
	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil || numKeys < 1 {
		return protocol.MakeErrReply("ERR numkeys must be a positive integer")
	}
	if len(args) < 2+numKeys {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'tdigest.merge' command")
	}
	override := false
	compression := 0.0
	for i := 2 + numKeys; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "OVERRIDE":
			override = true
		case "COMPRESSION":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			compression, err = strconv.ParseFloat(string(args[i+1]), 64)
			if err != nil {
				return protocol.MakeErrReply("ERR value is not a valid float")
			}
			i++
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	sources := make([]*probabilistic.TDigest, 0, numKeys)
	for i := 0; i < numKeys; i++ {
		entity, exists := db.GetEntity(string(args[2+i]))
		if !exists {
			return protocol.MakeErrReply("ERR key does not exist")
		}
		td, ok := entity.Data.(*probabilistic.TDigest)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
		sources = append(sources, td)
	}

	var destTD *probabilistic.TDigest
	if entity, exists := db.GetEntity(dest); exists {
		if !override {
			return protocol.MakeErrReply("ERR key already exists")
		}
		var ok bool
		destTD, ok = entity.Data.(*probabilistic.TDigest)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
		destTD.Reset()
	} else {
		c := 100.0
		if compression > 0 {
			c = compression
		} else if len(sources) > 0 {
			c = sources[0].Info()["compression"].(float64)
		}
		destTD = probabilistic.NewTDigest(c)
	}
	for _, src := range sources {
		destTD.MergeFrom(src)
	}
	db.PutEntity(dest, &database2.DataEntity{Data: destTD})
	db.addAof(prependCmd("tdigest.merge", args))
	return protocol.MakeOkReply()
}

func init() {
	registerCommand("TDigest.Create", execTDigestCreate, writeFirstKey, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("TDigest.Add", execTDigestAdd, writeFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("TDigest.Quantile", execTDigestQuantile, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("TDigest.CDF", execTDigestCDF, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("TDigest.Min", execTDigestMin, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("TDigest.Max", execTDigestMax, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("TDigest.Reset", execTDigestReset, writeFirstKey, nil, 2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("TDigest.Rank", execTDigestRank, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("TDigest.RevRank", execTDigestRevRank, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("TDigest.ByRank", execTDigestByRank, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("TDigest.ByRevRank", execTDigestByRevRank, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("TDigest.Trimmed_Mean", execTDigestTrimmedMean, readFirstKey, nil, 4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("TDigest.Merge", execTDigestMerge, prepareTDigestMerge, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, -1, 1)
	registerCommand("TDigest.Info", execTDigestInfo, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}

func prepareTDigestMerge(args [][]byte) ([]string, []string) {
	if len(args) < 3 {
		return nil, nil
	}
	writeKeys := []string{string(args[0])}
	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil || numKeys < 1 {
		return writeKeys, nil
	}
	readKeys := make([]string, 0, numKeys)
	for i := 0; i < numKeys && 2+i < len(args); i++ {
		readKeys = append(readKeys, string(args[2+i]))
	}
	return writeKeys, readKeys
}
