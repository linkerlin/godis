// Multi-series support for Time Series
package database

import (
	"strconv"
	"strings"
	"time"

	"github.com/hdt3213/godis/datastruct/timeseries"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
)

// execTSMRANGE queries multiple time series by filters
// TS.MRANGE fromTimestamp toTimestamp [COUNT count] [AGGREGATION aggregator bucketDuration]
//     [WITHLABELS] [FILTER filter...]
func execTSMRange(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ts.mrange' command")
	}
	
	// Parse timestamps
	var from, to int64
	fromStr := string(args[0])
	if fromStr == "-" {
		from = 0
	} else {
		from, _ = strconv.ParseInt(fromStr, 10, 64)
	}
	
	toStr := string(args[1])
	if toStr == "+" {
		to = time.Now().UnixMilli() + 1000000000
	} else {
		to, _ = strconv.ParseInt(toStr, 10, 64)
	}
	
	// Parse options and filters
	count := -1
	withLabels := false
	var filters []string
	var aggType timeseries.AggregationType
	var bucketSize time.Duration
	useAggregation := false
	
	for i := 2; i < len(args); {
		arg := strings.ToUpper(string(args[i]))
		
		switch arg {
		case "COUNT":
			if i+1 < len(args) {
				count, _ = strconv.Atoi(string(args[i+1]))
				i += 2
			} else {
				i++
			}
		case "AGGREGATION":
			if i+2 < len(args) {
				aggStr := string(args[i+1])
				bucketMs, _ := strconv.ParseInt(string(args[i+2]), 10, 64)
				aggType, _ = timeseries.ParseAggregationType(aggStr)
				bucketSize = time.Duration(bucketMs) * time.Millisecond
				useAggregation = true
				i += 3
			} else {
				i++
			}
		case "WITHLABELS":
			withLabels = true
			i++
		case "FILTER":
			i++
			for i < len(args) {
				nextArg := strings.ToUpper(string(args[i]))
				if nextArg == "COUNT" || nextArg == "AGGREGATION" {
					break
				}
				filters = append(filters, string(args[i]))
				i++
			}
		default:
			i++
		}
	}
	
	// Find all matching series
	var results [][]byte
	
	// Iterate all keys (simplified - in production would use index)
	db.data.ForEach(func(key string, val interface{}) bool {
		ts, ok := val.(*timeseries.TimeSeries)
		if !ok {
			return true
		}
		
		// Check filters
		if !matchFilters(ts, filters) {
			return true
		}
		
		// Get data
		var samples []timeseries.Sample
		if useAggregation {
			samples = ts.RangeWithAggregation(from, to, bucketSize, aggType)
		} else {
			samples = ts.Range(from, to)
		}
		
		if count > 0 && len(samples) > count {
			samples = samples[:count]
		}
		
		// Build result for this series
		var seriesResult [][]byte
		seriesResult = append(seriesResult, []byte(key))
		
		if withLabels {
			labels := ts.GetLabels()
			var labelPairs [][]byte
			for k, v := range labels {
				labelPairs = append(labelPairs, []byte(k))
				labelPairs = append(labelPairs, []byte(v))
			}
			seriesResult = append(seriesResult, protocol.MakeMultiBulkReply(labelPairs).ToBytes())
		}
		
		// Add samples
		for _, s := range samples {
			pair := [][]byte{
				[]byte(strconv.FormatInt(s.Timestamp, 10)),
				[]byte(strconv.FormatFloat(s.Value, 'f', -1, 64)),
			}
			seriesResult = append(seriesResult, protocol.MakeMultiBulkReply(pair).ToBytes())
		}
		
		results = append(results, protocol.MakeMultiBulkReply(seriesResult).ToBytes())
		return true
	})
	
	return protocol.MakeMultiBulkReply(results)
}

// execTSMGet gets the last sample from multiple time series
// TS.MGET [WITHLABELS] FILTER filter...
func execTSMGet(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ts.mget' command")
	}
	
	withLabels := false
	var filters []string
	
	for i := 0; i < len(args); {
		arg := strings.ToUpper(string(args[i]))
		
		switch arg {
		case "WITHLABELS":
			withLabels = true
			i++
		case "FILTER":
			i++
			for i < len(args) {
				filters = append(filters, string(args[i]))
				i++
			}
		default:
			i++
		}
	}
	
	if len(filters) == 0 {
		return protocol.MakeErrReply("ERR Missing FILTER clause")
	}
	
	// Find all matching series
	var results [][]byte
	
	db.data.ForEach(func(key string, val interface{}) bool {
		ts, ok := val.(*timeseries.TimeSeries)
		if !ok {
			return true
		}
		
		if !matchFilters(ts, filters) {
			return true
		}
		
		// Get last sample
		sample, hasSample := ts.GetLast()
		if !hasSample {
			return true
		}
		
		var seriesResult [][]byte
		seriesResult = append(seriesResult, []byte(key))
		
		if withLabels {
			labels := ts.GetLabels()
			var labelPairs [][]byte
			for k, v := range labels {
				labelPairs = append(labelPairs, []byte(k))
				labelPairs = append(labelPairs, []byte(v))
			}
			seriesResult = append(seriesResult, protocol.MakeMultiBulkReply(labelPairs).ToBytes())
		}
		
		// Add timestamp and value
		seriesResult = append(seriesResult, []byte(strconv.FormatInt(sample.Timestamp, 10)))
		seriesResult = append(seriesResult, []byte(strconv.FormatFloat(sample.Value, 'f', -1, 64)))
		
		results = append(results, protocol.MakeMultiBulkReply(seriesResult).ToBytes())
		return true
	})
	
	return protocol.MakeMultiBulkReply(results)
}

// execTSQueryIndex returns all time series keys matching filters
// TS.QUERYINDEX filter...
func execTSQueryIndex(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ts.queryindex' command")
	}
	
	filters := make([]string, len(args))
	for i, arg := range args {
		filters[i] = string(arg)
	}
	
	var keys [][]byte
	
	db.data.ForEach(func(key string, val interface{}) bool {
		ts, ok := val.(*timeseries.TimeSeries)
		if !ok {
			return true
		}
		
		if matchFilters(ts, filters) {
			keys = append(keys, []byte(key))
		}
		return true
	})
	
	return protocol.MakeMultiBulkReply(keys)
}

// matchFilters checks if a time series matches the filter conditions
// Filters: label=value, label!=value, label= (exists), label!= (not exists)
func matchFilters(ts *timeseries.TimeSeries, filters []string) bool {
	labels := ts.GetLabels()
	
	for _, filter := range filters {
		parts := strings.SplitN(filter, "=", 2)
		if len(parts) != 2 {
			continue
		}
		
		label := parts[0]
		value := parts[1]
		
		// Check not equal
		if strings.HasSuffix(label, "!") {
			label = strings.TrimSuffix(label, "!")
			if labels[label] == value {
				return false
			}
		} else {
			// Check equal
			if labels[label] != value {
				return false
			}
		}
	}
	
	return true
}

func init() {
	registerCommand("TS.MRange", execTSMRange, nil, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
	registerCommand("TS.MGet", execTSMGet, nil, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
	registerCommand("TS.QueryIndex", execTSQueryIndex, nil, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 0, 0, 0)
}


