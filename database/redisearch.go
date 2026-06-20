package database

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/linkerlin/godis/datastruct/redisearch"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// Global search engines registry
var searchEngines = make(map[string]*redisearch.RediSearchEngine)
var searchEnginesMu = &struct{ sync.RWMutex }{}

// execFTCreate creates a new search index
// FT.CREATE index [ON HASH | JSON] [PREFIX count prefix ...] SCHEMA field [TEXT [NOSTEM] | NUMERIC | TAG | GEO] [SORTABLE] [NOINDEX] ...
func execFTCreate(db *DB, args [][]byte) redis.Reply {
	if currentSearchBackend().Name() == backendSQLite {
		return sqliteFTCreate(db, args)
	}
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.create' command")
	}

	indexName := string(args[0])
	if reply := validateBulkBytes(args[0]); reply != nil {
		return reply
	}

	// Parse options
	var prefix []string
	schemaStart := 1

	for i := 1; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))

		switch arg {
		case "ON":
			// ON HASH|JSON - skip for now
			i++
		case "PREFIX":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("ERR Invalid prefix count")
			}
			i += 2
			for j := 0; j < count && i < len(args); j++ {
				if reply := validateBulkBytes(args[i]); reply != nil {
					return reply
				}
				prefix = append(prefix, string(args[i]))
				i++
			}
			i--
		case "SCHEMA":
			schemaStart = i + 1
			i = len(args) // Break out
		}
	}

	if schemaStart >= len(args) {
		return protocol.MakeErrReply("ERR No schema specified")
	}

	// Parse schema
	var fields []*redisearch.Field
	i := schemaStart
	for i < len(args) {
		fieldName := string(args[i])
		if reply := validateBulkBytes(args[i]); reply != nil {
			return reply
		}
		i++

		if i >= len(args) {
			return protocol.MakeErrReply(fmt.Sprintf("ERR No type specified for field '%s'", fieldName))
		}

		fieldType := strings.ToUpper(string(args[i]))
		field := &redisearch.Field{
			Name:     fieldName,
			Weight:   1.0,
			Stemming: true,
		}

		switch fieldType {
		case "TEXT":
			field.Type = redisearch.FieldTypeText
		case "NUMERIC":
			field.Type = redisearch.FieldTypeNumeric
		case "TAG":
			field.Type = redisearch.FieldTypeTag
		case "GEO":
			field.Type = redisearch.FieldTypeGeo
		case "VECTOR":
			field.Type = redisearch.FieldTypeVector
		default:
			return protocol.MakeErrReply(fmt.Sprintf("ERR Unknown field type '%s'", fieldType))
		}
		i++

		// Parse field options until the next field name (token before a type keyword).
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))

			switch opt {
			case "SORTABLE":
				field.Sortable = true
				i++
			case "NOINDEX":
				field.NoIndex = true
				i++
			case "NOSTEM":
				field.Stemming = false
				i++
			case "WEIGHT":
				if i+1 >= len(args) {
					return protocol.MakeSyntaxErrReply()
				}
				if reply := validateBulkBytes(args[i+1]); reply != nil {
					return reply
				}
				weight, err := strconv.ParseFloat(string(args[i+1]), 64)
				if err != nil {
					return protocol.MakeErrReply("ERR Invalid weight")
				}
				field.Weight = weight
				i += 2
			default:
				if i+1 < len(args) && isFTFieldType(string(args[i+1])) {
					goto nextField
				}
				i++
			}
		}

	nextField:
		fields = append(fields, field)
	}

	// Create engine
	config := &redisearch.EngineConfig{
		Name: indexName,
	}

	engine := redisearch.NewRediSearchEngine(config)
	if err := engine.CreateIndex(fields); err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}

	// Store engine
	searchEnginesMu.Lock()
	searchEngines[indexName] = engine
	searchEnginesMu.Unlock()

	// Also store in DB for persistence tracking
	db.PutEntity(indexName, &database.DataEntity{Data: engine})

	db.addAof(utils.ToCmdLine3("ft.create", args...))
	return protocol.MakeOkReply()
}

// execFTDropIndex drops an index
// FT.DROPINDEX index [DD]
func execFTDropIndex(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 || len(args) > 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.dropindex' command")
	}

	indexName := string(args[0])
	deleteDocs := false

	if len(args) == 2 && strings.ToUpper(string(args[1])) == "DD" {
		deleteDocs = true
	}

	searchEnginesMu.Lock()
	engine, ok := searchEngines[indexName]
	searchEnginesMu.Unlock()

	if !ok {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Index '%s' does not exist", indexName))
	}

	if err := engine.DropIndex(deleteDocs); err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}

	searchEnginesMu.Lock()
	delete(searchEngines, indexName)
	searchEnginesMu.Unlock()

	db.Remove(indexName)

	db.addAof(utils.ToCmdLine3("ft.dropindex", args...))
	return protocol.MakeOkReply()
}

// execFTAdd adds a document to an index
// FT.ADD index doc_id [SCORE score] [NOSAVE] [PAYLOAD payload] [LANGUAGE lang] FIELDS field value [field value ...]
func execFTAdd(db *DB, args [][]byte) redis.Reply {
	if currentSearchBackend().Name() == backendSQLite {
		return sqliteFTAdd(db, args)
	}
	if len(args) < 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.add' command")
	}

	indexName := string(args[0])
	docID := string(args[1])
	if reply := validateBulkBytes(args[0]); reply != nil {
		return reply
	}
	if reply := validateBulkBytes(args[1]); reply != nil {
		return reply
	}

	searchEnginesMu.RLock()
	engine, ok := searchEngines[indexName]
	searchEnginesMu.RUnlock()

	if !ok {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Index '%s' does not exist", indexName))
	}

	// Parse options
	score := 1.0
	nosave := false
	var payload []byte
	language := ""
	fieldsStart := 2

	for i := 2; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))

		switch arg {
		case "SCORE":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			var err error
			score, err = strconv.ParseFloat(string(args[i+1]), 64)
			if err != nil {
				return protocol.MakeErrReply("ERR Invalid score")
			}
			i++
		case "NOSAVE":
			nosave = true
		case "PAYLOAD":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			if reply := validateBulkBytes(args[i+1]); reply != nil {
				return reply
			}
			payload = args[i+1]
			i++
		case "LANGUAGE":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			if reply := validateBulkBytes(args[i+1]); reply != nil {
				return reply
			}
			language = string(args[i+1])
			i++
		case "FIELDS":
			fieldsStart = i + 1
			i = len(args) // Break out
		default:
			// Assume it's the start of fields
			fieldsStart = i
			i = len(args)
		}
	}

	// Parse fields
	if fieldsStart >= len(args) || (len(args)-fieldsStart)%2 != 0 {
		return protocol.MakeErrReply("ERR Fields must be specified as field-value pairs")
	}

	fields := make(map[string]interface{})
	for i := fieldsStart; i < len(args); i += 2 {
		if reply := validateBulkBytes(args[i]); reply != nil {
			return reply
		}
		if reply := validateBulkBytes(args[i+1]); reply != nil {
			return reply
		}
		fieldName := string(args[i])
		fieldValue := string(args[i+1])
		fields[fieldName] = fieldValue
	}

	// Add document
	if err := engine.AddDocument(docID, fields, score, payload); err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}

	_ = nosave
	_ = language

	db.addAof(utils.ToCmdLine3("ft.add", args...))
	return protocol.MakeOkReply()
}

// execFTDel deletes a document from an index
// FT.DEL index doc_id [DD]
func execFTDel(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 || len(args) > 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.del' command")
	}

	indexName := string(args[0])
	docID := string(args[1])

	searchEnginesMu.RLock()
	engine, ok := searchEngines[indexName]
	searchEnginesMu.RUnlock()

	if !ok {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Index '%s' does not exist", indexName))
	}

	deleted := engine.DeleteDocument(docID)

	if deleted {
		db.addAof(utils.ToCmdLine3("ft.del", args...))
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// execFTSearch searches the index
// FT.SEARCH index query [NOCONTENT] [VERBATIM] [NOSTOPWORDS] [WITHSCORES] [WITHPAYLOADS] [WITHSORTKEYS]
//
//	[FILTER numeric_field min max [FILTER numeric_field min max ...]]
//	[GEOFILTER geo_field lon lat radius m|km|mi|ft [GEOFILTER geo_field lon lat radius m|km|mi|ft ...]]
//	[INKEYS count key [key ...]]
//	[RETURN count field [field ...]]
//	[SORTBY field [ASC|DESC]]
//	[LIMIT offset num]
func execFTSearch(db *DB, args [][]byte) redis.Reply {
	if currentSearchBackend().Name() == backendSQLite {
		return sqliteFTSearch(db, args)
	}
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.search' command")
	}

	indexName := string(args[0])
	query := string(args[1])

	searchEnginesMu.RLock()
	engine, ok := searchEngines[indexName]
	searchEnginesMu.RUnlock()

	if !ok {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Index '%s' does not exist", indexName))
	}

	// Parse options
	opts := &redisearch.SearchOptions{}
	noContent := false
	withScores := false
	withPayloads := false
	returnFields := []string{}

	for i := 2; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))

		switch arg {
		case "NOCONTENT":
			noContent = true
		case "WITHSCORES":
			withScores = true
			opts.WithScores = true
		case "WITHPAYLOADS":
			withPayloads = true
			opts.WithPayloads = true
		case "LIMIT":
			if i+2 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			offset, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("ERR Invalid offset")
			}
			limit, err := strconv.Atoi(string(args[i+2]))
			if err != nil {
				return protocol.MakeErrReply("ERR Invalid limit")
			}
			opts.Offset = offset
			opts.Limit = limit
			i += 2
		case "SORTBY":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			opts.SortBy = string(args[i+1])
			i++
			if i+1 < len(args) {
				next := strings.ToUpper(string(args[i+1]))
				if next == "ASC" {
					opts.SortDesc = false
					i++
				} else if next == "DESC" {
					opts.SortDesc = true
					i++
				}
			}
		case "RETURN":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("ERR Invalid return count")
			}
			i += 2
			for j := 0; j < count && i < len(args); j++ {
				nextArg := strings.ToUpper(string(args[i]))
				if nextArg == "LIMIT" || nextArg == "SORTBY" || nextArg == "GEOFILTER" {
					i--
					break
				}
				returnFields = append(returnFields, string(args[i]))
				i++
			}
			i--
		case "GEOFILTER":
			// GEOFILTER geo_field lon lat radius m|km|mi|ft
			if i+5 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			geoField := string(args[i+1])
			lon, err := strconv.ParseFloat(string(args[i+2]), 64)
			if err != nil {
				return protocol.MakeErrReply("ERR Invalid longitude")
			}
			lat, err := strconv.ParseFloat(string(args[i+3]), 64)
			if err != nil {
				return protocol.MakeErrReply("ERR Invalid latitude")
			}
			radius, err := strconv.ParseFloat(string(args[i+4]), 64)
			if err != nil {
				return protocol.MakeErrReply("ERR Invalid radius")
			}
			unit := strings.ToLower(string(args[i+5]))
			if unit != "m" && unit != "km" && unit != "mi" && unit != "ft" {
				return protocol.MakeErrReply("ERR Invalid unit")
			}
			opts.GeoFilter = &redisearch.GeoFilterOptions{
				Field:  geoField,
				Lon:    lon,
				Lat:    lat,
				Radius: radius,
				Unit:   unit,
			}
			i += 5
		}
	}

	// Search
	results, err := engine.Search(query, opts)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}

	// Build response
	var reply [][]byte
	reply = append(reply, []byte(strconv.Itoa(results.Total)))

	for _, result := range results.Results {
		reply = append(reply, []byte(result.Document.ID))

		if withScores {
			reply = append(reply, []byte(fmt.Sprintf("%.6f", result.Score)))
		}

		if withPayloads && result.Document.Payload != nil {
			reply = append(reply, result.Document.Payload)
		}

		if !noContent {
			// Return fields
			var fields [][]byte

			if len(returnFields) > 0 {
				for _, field := range returnFields {
					if val, ok := result.Fields[field]; ok {
						fields = append(fields, []byte(field))
						fields = append(fields, []byte(fmt.Sprintf("%v", val)))
					}
				}
			} else {
				// Return all fields
				for k, v := range result.Fields {
					fields = append(fields, []byte(k))
					fields = append(fields, []byte(fmt.Sprintf("%v", v)))
				}
			}

			reply = append(reply, protocol.MakeMultiBulkReply(fields).ToBytes())

			// Add highlights if requested
			if opts.Highlight && len(result.Highlights) > 0 {
				var highlights [][]byte
				for field, value := range result.Highlights {
					highlights = append(highlights, []byte(field))
					highlights = append(highlights, []byte(value))
				}
				if len(highlights) > 0 {
					reply = append(reply, []byte("highlight"))
					reply = append(reply, protocol.MakeMultiBulkReply(highlights).ToBytes())
				}
			}
		}
	}

	return protocol.MakeMultiBulkReply(reply)
}

// execFTAggregate performs an aggregation query
// FT.AGGREGATE index query [VERBATIM] [LOAD count field [field ...]]
//
//	[GROUPBY nargs property [property ...] [REDUCE func nargs arg [arg ...] [AS name]] ...]
//	[SORTBY nargs property [ASC|DESC] ... [MAX num]]
//	[LIMIT offset num]
func execFTAggregate(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.aggregate' command")
	}

	indexName := string(args[0])
	query := string(args[1])

	searchEnginesMu.RLock()
	engine, ok := searchEngines[indexName]
	searchEnginesMu.RUnlock()

	if !ok {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Index '%s' does not exist", indexName))
	}

	// Parse aggregation request
	req := &redisearch.AggregationRequest{
		Query:  query,
		Offset: 0,
		Limit:  10,
	}

	for i := 2; i < len(args); {
		arg := strings.ToUpper(string(args[i]))

		switch arg {
		case "LOAD":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("ERR Invalid load count")
			}
			i += 2
			for j := 0; j < count && i < len(args); j++ {
				nextArg := strings.ToUpper(string(args[i]))
				if nextArg == "GROUPBY" || nextArg == "SORTBY" || nextArg == "LIMIT" {
					break
				}
				req.Load = append(req.Load, string(args[i]))
				i++
			}
			continue

		case "GROUPBY":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			nargs, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("ERR Invalid groupby nargs")
			}
			i += 2

			// Get properties (support multiple fields)
			for j := 0; j < nargs && i < len(args); j++ {
				nextArg := strings.ToUpper(string(args[i]))
				if nextArg == "REDUCE" || nextArg == "HAVING" {
					break
				}
				req.GroupBy = append(req.GroupBy, string(args[i]))
				i++
			}

			// Parse HAVING clause if present
			if i < len(args) && strings.ToUpper(string(args[i])) == "HAVING" {
				if i+3 >= len(args) {
					return protocol.MakeSyntaxErrReply()
				}
				i++ // Skip HAVING

				havingLeft := string(args[i])
				i++

				havingOp := string(args[i])
				i++

				// Parse the right side value
				havingRight := string(args[i])
				i++

				// Try to parse as number
				var havingValue interface{} = havingRight
				if num, err := strconv.ParseFloat(havingRight, 64); err == nil {
					havingValue = num
				}

				req.Having = &redisearch.HavingClause{
					Left:     havingLeft,
					Operator: havingOp,
					Right:    havingValue,
				}
			}

			// Get REDUCE clauses
			for i < len(args) && strings.ToUpper(string(args[i])) == "REDUCE" {
				if i+2 >= len(args) {
					return protocol.MakeSyntaxErrReply()
				}

				funcName := string(args[i+1])
				rargs, err := strconv.Atoi(string(args[i+2]))
				if err != nil {
					return protocol.MakeErrReply("ERR Invalid reduce nargs")
				}

				reducer := redisearch.Reducer{
					Function: strings.ToUpper(funcName),
				}

				i += 3
				for j := 0; j < rargs && i < len(args); j++ {
					nextArg := strings.ToUpper(string(args[i]))
					if nextArg == "AS" || nextArg == "REDUCE" || nextArg == "SORTBY" || nextArg == "LIMIT" {
						break
					}
					reducer.Field = string(args[i])
					i++
				}

				// Check for AS
				if i < len(args) && strings.ToUpper(string(args[i])) == "AS" {
					if i+1 >= len(args) {
						return protocol.MakeSyntaxErrReply()
					}
					reducer.As = string(args[i+1])
					i += 2
				}

				req.Reduce = append(req.Reduce, reducer)
			}
			continue

		case "SORTBY":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			nargs, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("ERR Invalid sortby nargs")
			}
			i += 2

			if nargs > 0 && i < len(args) {
				req.SortBy = string(args[i])
				i++

				if i < len(args) {
					dir := strings.ToUpper(string(args[i]))
					if dir == "ASC" {
						req.SortDesc = false
						i++
					} else if dir == "DESC" {
						req.SortDesc = true
						i++
					}
				}
			}
			continue

		case "LIMIT":
			if i+2 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			offset, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("ERR Invalid offset")
			}
			limit, err := strconv.Atoi(string(args[i+2]))
			if err != nil {
				return protocol.MakeErrReply("ERR Invalid limit")
			}
			req.Offset = offset
			req.Limit = limit
			i += 3
			continue

		case "FILTER":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			// FILTER expression (e.g., "@field > 10")
			req.Filter = string(args[i+1])
			i += 2
			continue

		case "HAVING":
			// HAVING is parsed within GROUPBY block
			// If we see it here, skip it as it's already handled
			i++
			// Skip the condition (3 args: field op value)
			if i+2 < len(args) {
				i += 3
			}
			continue
		}

		i++
	}

	// Execute aggregation
	result, err := engine.Aggregate(req)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}

	// Build response
	var reply [][]byte
	reply = append(reply, []byte(strconv.Itoa(result.Total)))

	for _, group := range result.Groups {
		var fields [][]byte

		fields = append(fields, []byte(fmt.Sprintf("%v", group.By)))

		for k, v := range group.Fields {
			fields = append(fields, []byte(k))
			fields = append(fields, []byte(fmt.Sprintf("%v", v)))
		}

		reply = append(reply, protocol.MakeMultiBulkReply(fields).ToBytes())
	}

	return protocol.MakeMultiBulkReply(reply)
}

// execFTInfo returns information about an index
// FT.INFO index
func execFTInfo(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.info' command")
	}

	indexName := string(args[0])

	searchEnginesMu.RLock()
	engine, ok := searchEngines[indexName]
	searchEnginesMu.RUnlock()

	if !ok {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Index '%s' does not exist", indexName))
	}

	info := engine.Info()

	// Convert to flat array
	var reply [][]byte

	for k, v := range info {
		reply = append(reply, []byte(k))
		reply = append(reply, []byte(fmt.Sprintf("%v", v)))
	}

	return protocol.MakeMultiBulkReply(reply)
}

// execFTList lists all indexes
// FT._LIST
func execFTList(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft._list' command")
	}

	searchEnginesMu.RLock()
	defer searchEnginesMu.RUnlock()

	var indexes [][]byte
	for name := range searchEngines {
		indexes = append(indexes, []byte(name))
	}

	return protocol.MakeMultiBulkReply(indexes)
}

// execFTSugAdd adds a suggestion string
// FT.SUGADD key string score [INCR] [PAYLOAD payload]
func execFTSugAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.sugadd' command")
	}

	key := string(args[0])
	str := string(args[1])

	score, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR Invalid score")
	}

	// Parse options
	incr := false
	payload := ""

	for i := 3; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "INCR":
			incr = true
		case "PAYLOAD":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			payload = string(args[i+1])
			i++
		}
	}

	// Get or create autocomplete for this key
	searchEnginesMu.Lock()
	engine, exists := searchEngines[key]
	if !exists {
		// Create new engine for autocomplete
		engine = redisearch.NewRediSearchEngine(&redisearch.EngineConfig{
			Name: key,
		})
		searchEngines[key] = engine
	}
	searchEnginesMu.Unlock()

	// Add suggestion
	engine.AddSuggestion(str, score, payload, incr)

	db.addAof(utils.ToCmdLine3("ft.sugadd", args...))
	return protocol.MakeIntReply(1)
}

// execFTSugGet gets suggestion strings
// FT.SUGGET key prefix [FUZZY] [MAX num] [WITHSCORES] [WITHPAYLOADS]
func execFTSugGet(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.sugget' command")
	}

	key := string(args[0])
	prefix := string(args[1])

	searchEnginesMu.RLock()
	engine, ok := searchEngines[key]
	searchEnginesMu.RUnlock()

	if !ok || engine == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	max := 5
	withScores := false
	withPayloads := false
	fuzzy := false

	for i := 2; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))

		switch arg {
		case "MAX":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			m, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("ERR Invalid max")
			}
			max = m
			i++
		case "WITHSCORES":
			withScores = true
		case "WITHPAYLOADS":
			withPayloads = true
		case "FUZZY":
			fuzzy = true
		}
	}

	suggestions := engine.Suggest(prefix, max, fuzzy)

	var reply [][]byte
	for _, sug := range suggestions {
		reply = append(reply, []byte(sug.Term))
		if withScores {
			reply = append(reply, []byte(strconv.FormatFloat(sug.Score, 'f', -1, 64)))
		}
		if withPayloads {
			reply = append(reply, []byte(sug.Payload))
		}
	}

	return protocol.MakeMultiBulkReply(reply)
}

// execFTSugDel deletes a suggestion
// FT.SUGDEL key string
func execFTSugDel(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.sugdel' command")
	}

	key := string(args[0])
	str := string(args[1])

	searchEnginesMu.RLock()
	engine, ok := searchEngines[key]
	searchEnginesMu.RUnlock()

	if !ok || engine == nil {
		return protocol.MakeIntReply(0)
	}

	// Note: Need to add Del method to Autocomplete
	// For now, just return success
	_ = str

	db.addAof(utils.ToCmdLine3("ft.sugdel", args...))
	return protocol.MakeIntReply(1)
}

// execFTSugLen gets the number of suggestions
// FT.SUGLEN key
func execFTSugLen(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.suglen' command")
	}

	key := string(args[0])

	searchEnginesMu.RLock()
	engine, ok := searchEngines[key]
	searchEnginesMu.RUnlock()

	if !ok || engine == nil {
		return protocol.MakeIntReply(0)
	}

	// Get count from autocomplete
	count := 0
	if engine != nil {
		// This would need a method to get count from autocomplete
		// For now return 0
	}

	return protocol.MakeIntReply(int64(count))
}

func isFTFieldType(token string) bool {
	switch strings.ToUpper(token) {
	case "TEXT", "NUMERIC", "TAG", "GEO", "VECTOR":
		return true
	default:
		return false
	}
}

func init() {
	registerCommand("FT.Create", execFTCreate, nil, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("FT.DropIndex", execFTDropIndex, nil, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("FT.Add", execFTAdd, nil, nil, -5, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("FT.Del", execFTDel, nil, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("FT.Search", execFTSearch, nil, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
	registerCommand("FT.Aggregate", execFTAggregate, nil, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
	registerCommand("FT.Info", execFTInfo, nil, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
	registerCommand("FT._List", execFTList, nil, nil, 1, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
	registerCommand("FT.SugAdd", execFTSugAdd, nil, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("FT.SugGet", execFTSugGet, nil, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
	registerCommand("FT.SugDel", execFTSugDel, nil, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("FT.SugLen", execFTSugLen, nil, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
}
