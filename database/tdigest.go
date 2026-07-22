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
	registerCommand("TDigest.Info", execTDigestInfo, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
