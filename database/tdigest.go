package database

import (
	"strconv"
	"strings"

	"github.com/hdt3213/godis/datastruct/probabilistic"
	database2 "github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
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
		reply = append(reply, []byte(strconv.FormatFloat(v.(float64), 'f', -1, 64)))
	}
	
	return protocol.MakeMultiBulkReply(reply)
}

func init() {
	registerCommand("TDigest.Create", execTDigestCreate, nil, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 0, 0, 0)
	registerCommand("TDigest.Add", execTDigestAdd, nil, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("TDigest.Quantile", execTDigestQuantile, nil, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
	registerCommand("TDigest.CDF", execTDigestCDF, nil, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
	registerCommand("TDigest.Info", execTDigestInfo, nil, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
}
