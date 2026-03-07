package database

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/hdt3213/godis/datastruct/vector"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

// execVSAdd adds a vector to a vector set
// VS.ADD key id vector [METADATA k1 v1 ...]
func execVSAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'vs.add' command")
	}
	
	key := string(args[0])
	id := string(args[1])
	
	// Parse vector from string
	vecStr := string(args[2])
	vec, err := parseVectorString(vecStr)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid vector format: %v", err))
	}
	
	// Parse metadata if provided
	metadata := make(map[string]string)
	for i := 3; i < len(args); {
		if strings.ToUpper(string(args[i])) == "METADATA" && i+2 < len(args) {
			i++
			for i+1 < len(args) {
				// Check for next keyword
				argUpper := strings.ToUpper(string(args[i]))
				if argUpper == "METADATA" {
					break
				}
				metadata[string(args[i])] = string(args[i+1])
				i += 2
			}
		} else {
			i++
		}
	}
	
	// Get or create vector set
	vs, errReply := db.getOrInitVectorSet(key)
	if errReply != nil {
		return errReply
	}
	
	// Add vector
	isNew := vs.Add(id, vec, metadata)
	
	// Add to AOF
	db.addAof(utils.ToCmdLine3("vs.add", args...))
	
	if isNew {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// execVSGet retrieves a vector by ID
// VS.GET key id
func execVSGet(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'vs.get' command")
	}
	
	key := string(args[0])
	id := string(args[1])
	
	// Get vector set
	entity, exists := db.GetEntity(key)
	if !exists {
		return &protocol.NullBulkReply{}
	}
	
	vs, ok := entity.Data.(*vector.VectorSet)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	item, ok := vs.Get(id)
	if !ok {
		return &protocol.NullBulkReply{}
	}
	
	// Format response: vector as array of strings
	vecData := item.Vector.ToFloat64()
	result := make([][]byte, len(vecData))
	for i, v := range vecData {
		result[i] = []byte(fmt.Sprintf("%.6f", v))
	}
	
	return protocol.MakeMultiBulkReply(result)
}

// execVSDel deletes vectors from a vector set
// VS.DEL key id [id ...]
func execVSDel(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'vs.del' command")
	}
	
	key := string(args[0])
	
	// Get vector set
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}
	
	vs, ok := entity.Data.(*vector.VectorSet)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	// Delete vectors
	deleted := 0
	for i := 1; i < len(args); i++ {
		id := string(args[i])
		if vs.Delete(id) {
			deleted++
		}
	}
	
	// Clean up empty set
	if vs.Len() == 0 {
		db.Remove(key)
	}
	
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("vs.del", args...))
	}
	
	return protocol.MakeIntReply(int64(deleted))
}

// execVSSearch searches for similar vectors
// VS.SEARCH key [K k] [METRIC COSINE|EUCLIDEAN|DOT] vector
func execVSSearch(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'vs.search' command")
	}
	
	key := string(args[0])
	
	// Get vector set
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeEmptyMultiBulkReply()
	}
	
	vs, ok := entity.Data.(*vector.VectorSet)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	// Parse options
	k := 10 // default k
	metric := vector.CosineSimilarity
	var queryVec *vector.Vector
	
	i := 1
	for i < len(args) {
		arg := strings.ToUpper(string(args[i]))
		
		switch arg {
		case "K":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			kVal, err := strconv.Atoi(string(args[i+1]))
			if err != nil || kVal <= 0 {
				return protocol.MakeErrReply("ERR K must be a positive integer")
			}
			k = kVal
			i += 2
			
		case "METRIC":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			metricStr := strings.ToUpper(string(args[i+1]))
			switch metricStr {
			case "COSINE":
				metric = vector.CosineSimilarity
			case "EUCLIDEAN":
				metric = vector.EuclideanDistance
			case "DOT":
				metric = vector.DotProduct
			default:
				return protocol.MakeErrReply("ERR Invalid metric, must be COSINE, EUCLIDEAN, or DOT")
			}
			i += 2
			
		default:
			// Assume it's the query vector
			vecStr := string(args[i])
			var err error
			queryVec, err = parseVectorString(vecStr)
			if err != nil {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid vector format: %v", err))
			}
			i++
		}
	}
	
	if queryVec == nil {
		return protocol.MakeErrReply("ERR Missing query vector")
	}
	
	// Perform search
	results := vs.SearchWithMetric(queryVec, k, metric)
	
	// Format response
	return formatSearchResults(results)
}

// execVSQuery searches for similar vectors using an existing ID
// VS.QUERY key id [K k] [METRIC COSINE|EUCLIDEAN|DOT]
func execVSQuery(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'vs.query' command")
	}
	
	key := string(args[0])
	queryID := string(args[1])
	
	// Get vector set
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeEmptyMultiBulkReply()
	}
	
	vs, ok := entity.Data.(*vector.VectorSet)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	// Get query vector
	queryItem, ok := vs.Get(queryID)
	if !ok {
		return protocol.MakeEmptyMultiBulkReply()
	}
	
	// Parse options
	k := 10 // default k
	metric := vector.CosineSimilarity
	
	for i := 2; i < len(args); {
		arg := strings.ToUpper(string(args[i]))
		
		switch arg {
		case "K":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			kVal, err := strconv.Atoi(string(args[i+1]))
			if err != nil || kVal <= 0 {
				return protocol.MakeErrReply("ERR K must be a positive integer")
			}
			k = kVal
			i += 2
			
		case "METRIC":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			metricStr := strings.ToUpper(string(args[i+1]))
			switch metricStr {
			case "COSINE":
				metric = vector.CosineSimilarity
			case "EUCLIDEAN":
				metric = vector.EuclideanDistance
			case "DOT":
				metric = vector.DotProduct
			default:
				return protocol.MakeErrReply("ERR Invalid metric, must be COSINE, EUCLIDEAN, or DOT")
			}
			i += 2
			
		default:
			i++
		}
	}
	
	// Perform search (exclude the query ID itself)
	allResults := vs.SearchWithMetric(queryItem.Vector, k+1, metric)
	
	// Filter out the query ID
	var results []*vector.SearchResult
	for _, r := range allResults {
		if r.ID != queryID {
			results = append(results, r)
		}
		if len(results) >= k {
			break
		}
	}
	
	return formatSearchResults(results)
}

// execVSRange searches for vectors within a range
// VS.RANGE key metric threshold vector
func execVSRange(db *DB, args [][]byte) redis.Reply {
	if len(args) < 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'vs.range' command")
	}
	
	key := string(args[0])
	
	// Get vector set
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeEmptyMultiBulkReply()
	}
	
	vs, ok := entity.Data.(*vector.VectorSet)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	// Parse metric
	metricStr := strings.ToUpper(string(args[1]))
	var metric vector.SearchMetric
	switch metricStr {
	case "COSINE":
		metric = vector.CosineSimilarity
	case "EUCLIDEAN":
		metric = vector.EuclideanDistance
	case "DOT":
		metric = vector.DotProduct
	default:
		return protocol.MakeErrReply("ERR Invalid metric, must be COSINE, EUCLIDEAN, or DOT")
	}
	
	// Parse threshold
	threshold, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR Invalid threshold")
	}
	
	// Parse query vector
	vecStr := string(args[3])
	queryVec, err := parseVectorString(vecStr)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid vector format: %v", err))
	}
	
	// Perform range search
	results := vs.RangeSearch(queryVec, float32(threshold), metric)
	
	return formatSearchResults(results)
}

// execVSLen returns the number of vectors in a set
// VS.LEN key
func execVSLen(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'vs.len' command")
	}
	
	key := string(args[0])
	
	// Get vector set
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}
	
	vs, ok := entity.Data.(*vector.VectorSet)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	return protocol.MakeIntReply(int64(vs.Len()))
}

// execVSCard is alias for VSLEN
// VS.CARD key
func execVSCard(db *DB, args [][]byte) redis.Reply {
	return execVSLen(db, args)
}

// Helper functions

func (db *DB) getOrInitVectorSet(key string) (*vector.VectorSet, redis.Reply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		vs := vector.NewVectorSet()
		db.PutEntity(key, &database.DataEntity{Data: vs})
		return vs, nil
	}
	
	vs, ok := entity.Data.(*vector.VectorSet)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	
	return vs, nil
}

func parseVectorString(s string) (*vector.Vector, error) {
	// Parse format: "[f1,f2,f3,...]" or "f1 f2 f3 ..."
	
	// Try bracket format [a,b,c]
	if len(s) > 2 && s[0] == '[' && s[len(s)-1] == ']' {
		s = s[1 : len(s)-1]
	}
	
	// Parse comma or space separated values
	var values []float64
	
	// Use simpler parsing - split by comma or space
	var parts []string
	if strings.Contains(s, ",") {
		parts = strings.Split(s, ",")
	} else {
		parts = strings.Fields(s)
	}
	
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		val, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float value: %s", part)
		}
		values = append(values, val)
	}
	
	if len(values) == 0 {
		return nil, fmt.Errorf("empty vector")
	}
	
	return vector.NewVectorFromFloat64(values), nil
}

func formatSearchResults(results []*vector.SearchResult) redis.Reply {
	if len(results) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	
	// Format: [[id, score, vector], ...]
	var reply [][]byte
	for _, r := range results {
		reply = append(reply, []byte(r.ID))
		reply = append(reply, []byte(fmt.Sprintf("%.6f", r.Score)))
		
		vecData := r.Vector.ToFloat64()
		vecStrs := make([][]byte, len(vecData))
		for i, v := range vecData {
			vecStrs[i] = []byte(fmt.Sprintf("%.6f", v))
		}
		reply = append(reply, protocol.MakeMultiBulkReply(vecStrs).ToBytes())
	}
	
	return protocol.MakeMultiBulkReply(reply)
}

// prepareVSKey prepares keys for vector commands
func prepareVSKey(args [][]byte) ([]string, []string) {
	return []string{string(args[0])}, nil
}

func init() {
	registerCommand("VSAdd", execVSAdd, prepareVSKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("VSGet", execVSGet, prepareVSKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("VSDel", execVSDel, prepareVSKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("VSSearch", execVSSearch, prepareVSKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("VSQuery", execVSQuery, prepareVSKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("VSRange", execVSRange, prepareVSKey, nil, 5, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("VSLen", execVSLen, prepareVSKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("VSCard", execVSCard, prepareVSKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
}
