package database

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/linkerlin/godis/datastruct/timeseries"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// execTSCreate creates a new time series
// TS.CREATE key [RETENTION retention] [ENCODING compression] [CHUNK_SIZE size] [DUPLICATE_POLICY policy] [LABELS label value ...]
func execTSCreate(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ts.create' command")
	}

	key := string(args[0])

	// Default options
	retention := time.Duration(0) // Unlimited
	chunkSize := 0
	labels := make(map[string]string)
	dupPolicy := timeseries.DupBlock

	// Parse options
	for i := 1; i < len(args); {
		arg := strings.ToUpper(string(args[i]))

		switch arg {
		case "RETENTION":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			retentionMs, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR Retention must be an integer")
			}
			retention = time.Duration(retentionMs) * time.Millisecond
			i += 2

		case "LABELS":
			i++
			for i+1 < len(args) {
				// Check if next arg is a keyword
				nextArg := strings.ToUpper(string(args[i]))
				if nextArg == "RETENTION" || nextArg == "CHUNK_SIZE" ||
					nextArg == "ENCODING" || nextArg == "DUPLICATE_POLICY" {
					break
				}
				label := string(args[i])
				value := string(args[i+1])
				labels[label] = value
				i += 2
			}

		case "CHUNK_SIZE":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			size, err := strconv.Atoi(string(args[i+1]))
			if err != nil || size <= 0 {
				return protocol.MakeErrReply("ERR CHUNK_SIZE must be a positive integer")
			}
			chunkSize = size
			i += 2

		case "ENCODING":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			enc := strings.ToUpper(string(args[i+1]))
			if enc != "COMPRESSED" && enc != "UNCOMPRESSED" {
				return protocol.MakeErrReply("ERR unknown ENCODING type")
			}
			i += 2

		case "DUPLICATE_POLICY":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			pol, err := timeseries.ParseDuplicatePolicy(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("ERR Unknown DUPLICATE_POLICY '" + string(args[i+1]) + "'")
			}
			dupPolicy = pol
			i += 2

		default:
			i++
		}
	}

	// Check if key exists
	_, exists := db.GetEntity(key)
	if exists {
		return protocol.MakeErrReply("ERR key already exists")
	}

	// Create time series
	ts := timeseries.NewTimeSeries(key, retention)
	ts.DuplicatePolicy = dupPolicy
	if chunkSize > 0 {
		ts.ChunkSize = chunkSize
	}
	for k, v := range labels {
		ts.AddLabel(k, v)
	}

	db.PutEntity(key, &database.DataEntity{Data: ts})

	db.addAof(prependCmd("ts.create", args))
	return protocol.MakeOkReply()
}

// execTSAdd adds a sample to a time series
// TS.ADD key timestamp value [RETENTION retention] [ENCODING compression] [CHUNK_SIZE size] [ON_DUPLICATE policy] [LABELS label value ...]
func execTSAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ts.add' command")
	}

	key := string(args[0])

	// Parse timestamp
	var timestamp int64
	timestampStr := string(args[1])
	if strings.ToUpper(timestampStr) == "*" {
		timestamp = time.Now().UnixMilli()
	} else {
		var err error
		timestamp, err = strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR Timestamp must be an integer or *")
		}
	}

	// Parse value
	value, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR Value must be a double")
	}

	// Parse optional trailing options.
	onDup := (*timeseries.DuplicatePolicy)(nil)
	for i := 3; i < len(args); {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "ON_DUPLICATE", "DUPLICATE_POLICY":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			pol, err := timeseries.ParseDuplicatePolicy(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("ERR Unknown DUPLICATE_POLICY '" + string(args[i+1]) + "'")
			}
			onDup = &pol
			i += 2
		case "RETENTION", "ENCODING", "CHUNK_SIZE":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			i += 2
		case "LABELS":
			i++
			for i+1 < len(args) {
				next := strings.ToUpper(string(args[i]))
				if next == "RETENTION" || next == "ENCODING" || next == "CHUNK_SIZE" ||
					next == "ON_DUPLICATE" || next == "DUPLICATE_POLICY" {
					break
				}
				i += 2
			}
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	// Get or create time series
	entity, exists := db.GetEntity(key)
	var ts *timeseries.TimeSeries

	if !exists {
		// Auto-create if doesn't exist
		ts = timeseries.NewTimeSeries(key, 0)
		db.PutEntity(key, &database.DataEntity{Data: ts})
	} else {
		var ok bool
		ts, ok = entity.Data.(*timeseries.TimeSeries)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
	}

	// Add sample
	var tsTimestamp int64
	if onDup != nil {
		tsTimestamp, err = ts.AddWithPolicy(timestamp, value, *onDup)
	} else {
		tsTimestamp, err = ts.Add(timestamp, value)
	}
	if err != nil {
		if err == timeseries.ErrTimestampTooOld {
			return protocol.MakeErrReply("ERR Timestamp is older than retention")
		}
		if err == timeseries.ErrDuplicateTimestamp {
			return protocol.MakeErrReply("ERR TSDB: Error at TEMPADD, update is not supported when DUPLICATE_POLICY is set to BLOCK policy.")
		}
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}

	db.addAof(prependCmd("ts.add", args))
	return protocol.MakeIntReply(tsTimestamp)
}

// execTSMAdd adds multiple samples across keys
// TS.MADD key timestamp value [key timestamp value ...]
func execTSMAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 || len(args)%3 != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ts.madd' command")
	}
	results := make([][]byte, 0, len(args)/3)
	for i := 0; i < len(args); i += 3 {
		key := string(args[i])
		var timestamp int64
		timestampStr := string(args[i+1])
		if strings.ToUpper(timestampStr) == "*" {
			timestamp = time.Now().UnixMilli()
		} else {
			var err error
			timestamp, err = strconv.ParseInt(timestampStr, 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR Timestamp must be an integer or *")
			}
		}
		value, err := strconv.ParseFloat(string(args[i+2]), 64)
		if err != nil {
			return protocol.MakeErrReply("ERR Value must be a double")
		}
		entity, exists := db.GetEntity(key)
		var ts *timeseries.TimeSeries
		if !exists {
			ts = timeseries.NewTimeSeries(key, 0)
			db.PutEntity(key, &database.DataEntity{Data: ts})
		} else {
			var ok bool
			ts, ok = entity.Data.(*timeseries.TimeSeries)
			if !ok {
				return &protocol.WrongTypeErrReply{}
			}
		}
		tsTimestamp, err := ts.Add(timestamp, value)
		if err != nil {
			if err == timeseries.ErrTimestampTooOld {
				return protocol.MakeErrReply("ERR Timestamp is older than retention")
			}
			return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
		}
		results = append(results, []byte(strconv.FormatInt(tsTimestamp, 10)))
	}
	db.addAof(prependCmd("ts.madd", args))
	return protocol.MakeMultiBulkReply(results)
}

// execTSGet gets the last sample
// TS.GET key
func execTSGet(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ts.get' command")
	}

	key := string(args[0])

	entity, exists := db.GetEntity(key)
	if !exists {
		return &protocol.NullBulkReply{}
	}

	ts, ok := entity.Data.(*timeseries.TimeSeries)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	sample, ok := ts.GetLast()
	if !ok {
		return &protocol.NullBulkReply{}
	}

	// Return [timestamp, value]
	result := [][]byte{
		[]byte(strconv.FormatInt(sample.Timestamp, 10)),
		[]byte(strconv.FormatFloat(sample.Value, 'f', -1, 64)),
	}

	return protocol.MakeMultiBulkReply(result)
}

// execTSRange queries a range
// TS.RANGE key fromTimestamp toTimestamp [COUNT count] [AGGREGATION aggregationType timeBucket]
func execTSRange(db *DB, args [][]byte) redis.Reply {
	return execTSRangeInternal(db, args, false)
}

// execTSRevRange queries a range in reverse
// TS.REVRANGE key fromTimestamp toTimestamp [COUNT count] [AGGREGATION aggregationType timeBucket]
func execTSRevRange(db *DB, args [][]byte) redis.Reply {
	return execTSRangeInternal(db, args, true)
}

func execTSRangeInternal(db *DB, args [][]byte, reverse bool) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ts.range' command")
	}

	key := string(args[0])

	// Parse from timestamp
	var from int64
	fromStr := string(args[1])
	if strings.ToUpper(fromStr) == "-" {
		from = 0
	} else {
		var err error
		from, err = strconv.ParseInt(fromStr, 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR fromTimestamp must be an integer or -")
		}
	}

	// Parse to timestamp
	var to int64
	toStr := string(args[2])
	if strings.ToUpper(toStr) == "+" {
		to = time.Now().UnixMilli() + 1000000000 // Far future
	} else {
		var err error
		to, err = strconv.ParseInt(toStr, 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR toTimestamp must be an integer or +")
		}
	}

	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeEmptyMultiBulkReply()
	}

	ts, ok := entity.Data.(*timeseries.TimeSeries)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	// Parse options
	count := -1
	var aggType timeseries.AggregationType
	var bucketSize time.Duration
	useAggregation := false

	for i := 3; i < len(args); {
		arg := strings.ToUpper(string(args[i]))

		switch arg {
		case "COUNT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			var err error
			count, err = strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("ERR Count must be an integer")
			}
			i += 2

		case "AGGREGATION":
			if i+2 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			aggStr := string(args[i+1])
			bucketMs, err := strconv.ParseInt(string(args[i+2]), 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR Time bucket must be an integer")
			}

			aggType, err = timeseries.ParseAggregationType(aggStr)
			if err != nil {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Unknown aggregation type '%s'", aggStr))
			}

			bucketSize = time.Duration(bucketMs) * time.Millisecond
			useAggregation = true
			i += 3

		default:
			i++
		}
	}

	// Get samples
	var samples []timeseries.Sample
	if useAggregation {
		samples = ts.RangeWithAggregation(from, to, bucketSize, aggType)
	} else {
		samples = ts.Range(from, to)
	}

	// Apply count limit
	if count > 0 && len(samples) > count {
		if reverse {
			samples = samples[len(samples)-count:]
		} else {
			samples = samples[:count]
		}
	}

	// Reverse if needed
	if reverse {
		for i, j := 0, len(samples)-1; i < j; i, j = i+1, j-1 {
			samples[i], samples[j] = samples[j], samples[i]
		}
	}

	// Build reply. RedisTimeSeries returns an array of [timestamp, value] pairs
	// where timestamp is an integer and value is a bulk number; each pair must be
	// a nested array (not flattened), otherwise clients like go-redis fail to
	// parse ("can't parse int reply").
	replies := make([]redis.Reply, 0, len(samples))
	for _, s := range samples {
		pair := []redis.Reply{
			protocol.MakeIntReply(s.Timestamp),
			protocol.MakeBulkReply([]byte(strconv.FormatFloat(s.Value, 'f', -1, 64))),
		}
		replies = append(replies, protocol.MakeMultiRawReply(pair))
	}

	return protocol.MakeMultiRawReply(replies)
}

// execTSInfo returns time series info
// TS.INFO key
func execTSInfo(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ts.info' command")
	}

	key := string(args[0])

	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}

	ts, ok := entity.Data.(*timeseries.TimeSeries)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	info := ts.Info()

	// Format as flat array
	var reply [][]byte
	for k, v := range info {
		reply = append(reply, []byte(k))
		reply = append(reply, []byte(fmt.Sprintf("%v", v)))
	}

	// Add labels
	labels := ts.GetLabels()
	if len(labels) > 0 {
		reply = append(reply, []byte("labels"))
		var labelPairs [][]byte
		for k, v := range labels {
			labelPairs = append(labelPairs, []byte(k))
			labelPairs = append(labelPairs, []byte(v))
		}
		reply = append(reply, protocol.MakeMultiBulkReply(labelPairs).ToBytes())
	}

	return protocol.MakeMultiBulkReply(reply)
}

// execTSDel deletes samples in a range
// TS.DEL key fromTimestamp toTimestamp
func execTSDel(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ts.del' command")
	}

	key := string(args[0])

	from, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR fromTimestamp must be an integer")
	}

	to, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR toTimestamp must be an integer")
	}

	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	ts, ok := entity.Data.(*timeseries.TimeSeries)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	deleted := ts.Del(from, to)

	if deleted > 0 {
		db.addAof(prependCmd("ts.del", args))
	}

	return protocol.MakeIntReply(int64(deleted))
}

// execTSIncrBy increments the latest value
// TS.INCRBY key value [TIMESTAMP timestamp] [RETENTION retention] [LABELS label value ...]
func execTSIncrBy(db *DB, args [][]byte) redis.Reply {
	return execTSIncrDecr(db, args, true)
}

// execTSDecrBy decrements the latest value
// TS.DECRBY key value [TIMESTAMP timestamp] [RETENTION retention] [LABELS label value ...]
func execTSDecrBy(db *DB, args [][]byte) redis.Reply {
	return execTSIncrDecr(db, args, false)
}

func execTSIncrDecr(db *DB, args [][]byte, isIncr bool) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments")
	}

	key := string(args[0])

	delta, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR Value must be a double")
	}

	if !isIncr {
		delta = -delta
	}

	// Parse timestamp
	timestamp := time.Now().UnixMilli()
	for i := 2; i < len(args); i += 2 {
		if i+1 >= len(args) {
			break
		}
		if strings.ToUpper(string(args[i])) == "TIMESTAMP" {
			ts, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err == nil {
				timestamp = ts
			}
		}
	}

	// Get or create time series
	entity, exists := db.GetEntity(key)
	var ts *timeseries.TimeSeries

	if !exists {
		ts = timeseries.NewTimeSeries(key, 0)
		db.PutEntity(key, &database.DataEntity{Data: ts})
	} else {
		var ok bool
		ts, ok = entity.Data.(*timeseries.TimeSeries)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
	}

	// Get last value and increment
	lastSample, hasLast := ts.GetLast()
	var newValue float64
	if hasLast {
		newValue = lastSample.Value + delta
	} else {
		newValue = delta
	}

	// Add new sample
	tsTimestamp, err := ts.Add(timestamp, newValue)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}

	aofCmd := "ts.decrby"
	if isIncr {
		aofCmd = "ts.incrby"
	}
	db.addAof(prependCmd(aofCmd, args))
	return protocol.MakeIntReply(tsTimestamp)
}

// Helper functions

func prependCmd(cmd string, args [][]byte) [][]byte {
	parts := strings.Split(cmd, " ")
	result := make([][]byte, 0, len(parts)+len(args))
	for _, p := range parts {
		result = append(result, []byte(p))
	}
	result = append(result, args...)
	return result
}

func init() {
	registerCommand("TS.Create", execTSCreate, writeFirstKey, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("TS.Add", execTSAdd, writeFirstKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("TS.MAdd", execTSMAdd, prepareTSMAdd, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, -1, 3)
	registerCommand("TS.Get", execTSGet, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("TS.Range", execTSRange, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("TS.RevRange", execTSRevRange, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("TS.Info", execTSInfo, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("TS.Del", execTSDel, writeFirstKey, nil, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("TS.IncrBy", execTSIncrBy, writeFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("TS.DecrBy", execTSDecrBy, writeFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("TS.Alter", execTSAlter, writeFirstKey, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("TS.CreateRule", execTSCreateRule, prepareWriteFirstTwoKeys, nil, 6, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 2, 1)
	registerCommand("TS.DeleteRule", execTSDeleteRule, writeFirstKey, nil, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
}

// execTSAlter modifies the labels or retention of an existing time series
// TS.ALTER key [RETENTION retention] [LABELS label value ...]
func execTSAlter(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ts.alter' command")
	}

	key := string(args[0])

	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}

	ts, ok := entity.Data.(*timeseries.TimeSeries)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	// Parse options
	for i := 1; i < len(args); {
		arg := strings.ToUpper(string(args[i]))

		switch arg {
		case "RETENTION":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			retentionMs, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR Retention must be an integer")
			}
			ts.SetRetention(time.Duration(retentionMs) * time.Millisecond)
			i += 2

		case "LABELS":
			i++
			newLabels := make(map[string]string)
			for i+1 < len(args) {
				nextArg := strings.ToUpper(string(args[i]))
				if nextArg == "RETENTION" {
					break
				}
				label := string(args[i])
				value := string(args[i+1])
				newLabels[label] = value
				i += 2
			}
			ts.SetLabels(newLabels)

		default:
			i++
		}
	}

	db.addAof(prependCmd("ts.alter", args))
	return protocol.MakeOkReply()
}

// execTSCreateRule creates a compaction rule
// TS.CREATERULE sourceKey destKey AGGREGATION aggregationType timeBucket
func execTSCreateRule(db *DB, args [][]byte) redis.Reply {
	if len(args) != 5 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ts.createrule' command")
	}

	sourceKey := string(args[0])
	destKey := string(args[1])

	if strings.ToUpper(string(args[2])) != "AGGREGATION" {
		return protocol.MakeSyntaxErrReply()
	}

	aggType, err := timeseries.ParseAggregationType(string(args[3]))
	if err != nil {
		return protocol.MakeErrReply("ERR Invalid aggregation type")
	}

	timeBucketMs, err := strconv.ParseInt(string(args[4]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR Time bucket must be an integer")
	}

	// Get source time series
	entity, exists := db.GetEntity(sourceKey)
	if !exists {
		return protocol.MakeErrReply("ERR source key does not exist")
	}

	sourceTS, ok := entity.Data.(*timeseries.TimeSeries)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	// Create or get destination time series
	destEntity, destExists := db.GetEntity(destKey)
	var destTS *timeseries.TimeSeries

	if !destExists {
		// Create destination time series with same retention as source
		destTS = timeseries.NewTimeSeries(destKey, sourceTS.GetRetention())
		db.PutEntity(destKey, &database.DataEntity{Data: destTS})
	} else {
		if _, ok := destEntity.Data.(*timeseries.TimeSeries); !ok {
			return &protocol.WrongTypeErrReply{}
		}
	}

	// Create downsample rule
	rule := timeseries.DownsampleRule{
		TimeBucket:  time.Duration(timeBucketMs) * time.Millisecond,
		Aggregation: aggType,
		Destination: destKey,
	}

	sourceTS.AddDownsampleRule(rule)

	db.addAof(prependCmd("ts.createrule", args))
	return protocol.MakeOkReply()
}

// execTSDeleteRule deletes a compaction rule
// TS.DELETERULE sourceKey destKey
func execTSDeleteRule(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ts.deleterule' command")
	}

	sourceKey := string(args[0])
	destKey := string(args[1])

	entity, exists := db.GetEntity(sourceKey)
	if !exists {
		return protocol.MakeErrReply("ERR source key does not exist")
	}

	ts, ok := entity.Data.(*timeseries.TimeSeries)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	ts.RemoveDownsampleRule(destKey)

	db.addAof(prependCmd("ts.deleterule", args))
	return protocol.MakeOkReply()
}
