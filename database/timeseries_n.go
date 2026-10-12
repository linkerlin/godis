package database

// Redis 8.6–8.10 Time Series additions: comma-separated aggregator lists
// (8.8), TS.NRANGE / TS.NREVRANGE (8.10) and TS.QUERYLABELS (8.10).

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/linkerlin/godis/datastruct/timeseries"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// parseAggList parses "avg" or "avg,max,countNaN" into aggregator types.
func parseAggList(s string) ([]timeseries.AggregationType, redis.Reply) {
	parts := strings.Split(s, ",")
	aggs := make([]timeseries.AggregationType, 0, len(parts))
	for _, p := range parts {
		agg, err := timeseries.ParseAggregationType(p)
		if err != nil {
			return nil, protocol.MakeErrReply("ERR Unknown aggregation type '" + p + "'")
		}
		aggs = append(aggs, agg)
	}
	return aggs, nil
}

// formatTSValue renders a sample/aggregated value, NaN included.
func formatTSValue(v float64) []byte {
	return []byte(strconv.FormatFloat(v, 'f', -1, 64))
}

// tsNRangeInternal implements TS.NRANGE / TS.NREVRANGE: an outer join of the
// listed series grouped by timestamp (raw pivot), or per-key aggregated
// buckets with a flat value list per timestamp. Missing values are NaN.
//
// Supported: COUNT, AGGREGATION (single aggregator or comma list per key,
// shared bucketDuration). LATEST / FILTER_BY_TS / FILTER_BY_VALUE / ALIGN /
// BUCKETTIMESTAMP / EMPTY are rejected explicitly (not silently ignored).
func tsNRangeInternal(db *DB, args [][]byte, reverse bool) redis.Reply {
	cmd := "ts.nrange"
	if reverse {
		cmd = "ts.nrevrange"
	}
	if len(args) < 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for '" + cmd + "' command")
	}
	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil || numKeys <= 0 {
		return protocol.MakeErrReply("ERR TSDB: wrong number of keys")
	}
	if len(args) < 1+numKeys+2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for '" + cmd + "' command")
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[1+i])
	}
	fromStr, toStr := string(args[1+numKeys]), string(args[2+numKeys])

	rest := args[3+numKeys:]
	count := -1
	var aggLists [][]timeseries.AggregationType // per key
	bucketSize := time.Duration(0)
	for i := 0; i < len(rest); {
		arg := strings.ToUpper(string(rest[i]))
		switch arg {
		case "LATEST", "FILTER_BY_TS", "FILTER_BY_VALUE", "ALIGN", "BUCKETTIMESTAMP", "EMPTY":
			return protocol.MakeErrReply("ERR TSDB: " + arg + " is not supported by this build")
		case "COUNT":
			if i+1 >= len(rest) {
				return protocol.MakeSyntaxErrReply()
			}
			count, err = strconv.Atoi(string(rest[i+1]))
			if err != nil || count < 0 {
				return protocol.MakeErrReply("ERR TSDB: wrong COUNT")
			}
			i += 2
		case "AGGREGATION":
			// AGGREGATION agg-per-key [agg-per-key ...] bucketDuration
			j := i + 1
			var lists [][]timeseries.AggregationType
			for j < len(rest) {
				// Each per-key argument is one aggregator or a comma list;
				// the bucket duration is the first non-aggregator argument.
				l, rerr := parseAggList(string(rest[j]))
				if rerr != nil {
					break
				}
				lists = append(lists, l)
				j++
			}
			if len(lists) != numKeys || j >= len(rest) {
				return protocol.MakeErrReply("ERR TSDB: wrong number of aggregation arguments")
			}
			bucketMs, perr := strconv.ParseInt(string(rest[j]), 10, 64)
			if perr != nil || bucketMs <= 0 {
				return protocol.MakeErrReply("ERR TSDB: wrong bucketDuration")
			}
			aggLists = lists
			bucketSize = time.Duration(bucketMs) * time.Millisecond
			i = j + 1
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	// Load series (order and duplicates preserved).
	seriesList := make([]*timeseries.TimeSeries, 0, numKeys)
	for _, key := range keys {
		entity, exists := db.GetEntity(key)
		if !exists {
			return protocol.MakeErrReply("ERR TSDB: the key does not exist")
		}
		ts, ok := entity.Data.(*timeseries.TimeSeries)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
		seriesList = append(seriesList, ts)
	}

	from, fromErr := parseTSBound(fromStr, true)
	to, toErr := parseTSBound(toStr, false)
	if fromErr != nil || toErr != nil {
		return protocol.MakeSyntaxErrReply()
	}

	// Per key: timestamp -> values (one per aggregator, in listed order).
	colsPerKey := make([]int, numKeys)
	if aggLists != nil {
		for k, l := range aggLists {
			colsPerKey[k] = len(l)
		}
	} else {
		for k := range colsPerKey {
			colsPerKey[k] = 1
		}
	}
	perKey := make([]map[int64][]float64, numKeys)
	allTS := map[int64]struct{}{}
	for k, ts := range seriesList {
		m := make(map[int64][]float64)
		if aggLists != nil {
			for _, agg := range aggLists[k] {
				for _, s := range ts.RangeWithAggregation(from, to, bucketSize, agg) {
					m[s.Timestamp] = append(m[s.Timestamp], s.Value)
				}
			}
		} else {
			for _, s := range ts.Range(from, to) {
				m[s.Timestamp] = append(m[s.Timestamp], s.Value)
			}
		}
		perKey[k] = m
		for t := range m {
			allTS[t] = struct{}{}
		}
	}

	order := make([]int64, 0, len(allTS))
	for t := range allTS {
		order = append(order, t)
	}
	sort.Slice(order, func(a, b int) bool {
		if reverse {
			return order[a] > order[b]
		}
		return order[a] < order[b]
	})

	replies := make([]redis.Reply, 0, len(order))
	for _, t := range order {
		vals := make([]redis.Reply, 0, len(keys))
		for k := 0; k < numKeys; k++ {
			cols := colsPerKey[k]
			if got, ok := perKey[k][t]; ok && len(got) == cols {
				for _, v := range got {
					vals = append(vals, protocol.MakeBulkReply(formatTSValue(v)))
				}
			} else {
				for i := 0; i < cols; i++ {
					vals = append(vals, protocol.MakeBulkReply(formatTSValue(math.NaN())))
				}
			}
		}
		if count > 0 && len(replies) >= count {
			break
		}
		replies = append(replies, protocol.MakeMultiRawReply([]redis.Reply{
			protocol.MakeIntReply(t),
			protocol.MakeMultiRawReply(vals),
		}))
	}
	return protocol.MakeMultiRawReply(replies)
}

// parseTSBound parses "-" / "+" / integer bounds ("-" → 0, "+" → far future).
func parseTSBound(s string, isFrom bool) (int64, error) {
	if s == "-" {
		return 0, nil
	}
	if s == "+" {
		return time.Now().UnixMilli() + 1000000000, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

// execTSNRange TS.NRANGE numkeys key [key ...] from to [options] (8.10)
func execTSNRange(db *DB, args [][]byte) redis.Reply {
	return tsNRangeInternal(db, args, false)
}

// execTSNRevRange TS.NREVRANGE numkeys key [key ...] from to [options] (8.10)
func execTSNRevRange(db *DB, args [][]byte) redis.Reply {
	return tsNRangeInternal(db, args, true)
}

// execTSQueryLabels TS.QUERYLABELS <LABELS | VALUES label> [FILTER expr...] (8.10)
func execTSQueryLabels(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ts.querylabels' command")
	}
	valuesMode := false
	labelName := ""
	switch strings.ToUpper(string(args[0])) {
	case "LABELS":
	case "VALUES":
		valuesMode = true
		if len(args) < 2 {
			return protocol.MakeSyntaxErrReply()
		}
		labelName = string(args[1])
		args = args[1:]
	default:
		return protocol.MakeSyntaxErrReply()
	}
	args = args[1:]

	var filters []string
	for i := 0; i < len(args); {
		if strings.EqualFold(string(args[i]), "FILTER") {
			i++
			for i < len(args) {
				filters = append(filters, string(args[i]))
				i++
			}
			break
		}
		i++
	}

	seen := map[string]struct{}{}
	db.data.ForEach(func(key string, val interface{}) bool {
		ts, ok := entityAsTimeSeries(val)
		if !ok || !matchFilters(ts, filters) {
			return true
		}
		if valuesMode {
			if v, ok := ts.Labels[labelName]; ok {
				seen[v] = struct{}{}
			}
		} else {
			for k := range ts.Labels {
				seen[k] = struct{}{}
			}
		}
		return true
	})

	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	sort.Strings(out)

	replies := make([]redis.Reply, len(out))
	for i, s := range out {
		replies[i] = protocol.MakeBulkReply([]byte(s))
	}
	return protocol.MakeSetReply(replies)
}

func init() {
	registerCommand("TS.NRange", execTSNRange, prepareTSNRange, nil, -5, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, -1, 1)
	registerCommand("TS.NRevRange", execTSNRevRange, prepareTSNRange, nil, -5, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, -1, 1)
	registerCommand("TS.QueryLabels", execTSQueryLabels, prepareNoKeys, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
}

func prepareTSNRange(args [][]byte) ([]string, []string) {
	if len(args) < 1 {
		return nil, nil
	}
	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil || numKeys < 1 || len(args) < 1+numKeys {
		return nil, nil
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[1+i])
	}
	return keys, nil
}
