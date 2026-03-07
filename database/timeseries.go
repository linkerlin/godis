package database

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hdt3213/godis/datastruct/timeseries"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
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
	labels := make(map[string]string)
	
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
				if nextArg == "RETENTION" || nextArg == "CHUNK_SIZE" {
					break
				}
				label := string(args[i])
				value := string(args[i+1])
				labels[label] = value
				i += 2
			}
			
		case "CHUNK_SIZE", "ENCODING", "DUPLICATE_POLICY":
			// Skip for now
			if i+1 < len(args) {
				i += 2
			} else {
				i++
			}
			
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
	tsTimestamp, err := ts.Add(timestamp, value)
	if err != nil {
		if err == timeseries.ErrTimestampTooOld {
			return protocol.MakeErrReply("ERR Timestamp is older than retention")
		}
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}
	
	db.addAof(prependCmd("ts.add", args))
	return protocol.MakeIntReply(tsTimestamp)
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
	
	// Build reply
	var result [][]byte
	for _, s := range samples {
		pair := [][]byte{
			[]byte(strconv.FormatInt(s.Timestamp, 10)),
			[]byte(strconv.FormatFloat(s.Value, 'f', -1, 64)),
		}
		result = append(result, protocol.MakeMultiBulkReply(pair).ToBytes())
	}
	
	return protocol.MakeMultiBulkReply(result)
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
	
	db.addAof(prependCmd("ts.incrby", args))
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
	registerCommand("TS.Create", execTSCreate, nil, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 0, 0, 0)
	registerCommand("TS.Add", execTSAdd, nil, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 0, 0, 0)
	registerCommand("TS.Get", execTSGet, nil, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 0, 0, 0)
	registerCommand("TS.Range", execTSRange, nil, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
	registerCommand("TS.RevRange", execTSRevRange, nil, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
	registerCommand("TS.Info", execTSInfo, nil, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 0, 0, 0)
	registerCommand("TS.Del", execTSDel, nil, nil, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("TS.IncrBy", execTSIncrBy, nil, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 0, 0, 0)
	registerCommand("TS.DecrBy", execTSDecrBy, nil, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 0, 0, 0)
}
