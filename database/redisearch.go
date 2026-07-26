package database

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	godisjson "github.com/linkerlin/godis/datastruct/json"
	"github.com/linkerlin/godis/datastruct/redisearch"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// Global search engines registry
var searchEngines = make(map[string]*redisearch.RediSearchEngine)
var searchEnginesMu = &struct{ sync.RWMutex }{}

// indexMeta tracks each FT index's key prefixes and schema so that hash keys
// written via HSET/HMSet can be auto-indexed, mirroring RediSearch ON HASH.
type indexMeta struct {
	prefixes []string // empty entry = match all keys (PREFIX *)
	schema   []*redisearch.Field
	onType   string // "HASH" (default) or "JSON"
}

var (
	searchIndexMeta   = make(map[string]*indexMeta)
	searchIndexMetaMu sync.RWMutex
)

// indexMatchesKey reports whether key matches any of the index prefixes.
// An empty prefix list, or a "*" / "" entry, matches every key.
func indexMatchesKey(prefixes []string, key string) bool {
	if len(prefixes) == 0 {
		return true
	}
	for _, p := range prefixes {
		if p == "" || p == "*" || strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

// reindexHash re-indexes a hash key into every FT index whose prefix matches.
// Best-effort: called after HSET/HMSet/HSetNX mutate a hash so that hash-based
// documents (RediSearch ON HASH) stay searchable. Errors are ignored to keep
// the hash write path from failing.
// ponytail: synchronous re-index per HSET; batch/offload if HSET throughput matters.
func reindexHash(db *DB, key string) {
	dict, errReply := db.getAsDict(key)
	if errReply != nil || dict == nil {
		return
	}
	searchIndexMetaMu.RLock()
	metas := make(map[string]*indexMeta, len(searchIndexMeta))
	for name, meta := range searchIndexMeta {
		metas[name] = meta
	}
	searchIndexMetaMu.RUnlock()
	if len(metas) == 0 {
		return
	}

	searchEnginesMu.RLock()
	engines := make(map[string]*redisearch.RediSearchEngine, len(metas))
	for name := range metas {
		if e := searchEngines[name]; e != nil {
			engines[name] = e
		}
	}
	searchEnginesMu.RUnlock()

	for name, meta := range metas {
		if meta.onType == "JSON" {
			continue // ON JSON indexes are not fed by HSET
		}
		if !indexMatchesKey(meta.prefixes, key) {
			continue
		}
		engine := engines[name]
		if engine == nil {
			continue
		}
		fields := make(map[string]interface{}, len(meta.schema))
		for _, f := range meta.schema {
			raw, ok := dict.Get(f.Name)
			if !ok {
				continue
			}
			if b, ok := raw.([]byte); ok {
				fields[f.Name] = string(b)
			} else {
				fields[f.Name] = raw
			}
		}
		engine.DeleteDocument(key)
		_ = engine.AddDocument(key, fields, 1.0, nil)
	}
}

// removeHashFromIndex removes a hash key from every FT index whose prefix
// matches. Called on DEL/UNLINK (and HDel of the last field) so that deleted
// hash documents stop appearing in searches. Best-effort: errors ignored.
func removeHashFromIndex(db *DB, key string) {
	_ = db
	searchIndexMetaMu.RLock()
	names := make([]string, 0, len(searchIndexMeta))
	for name, meta := range searchIndexMeta {
		if meta.onType == "JSON" {
			continue
		}
		if indexMatchesKey(meta.prefixes, key) {
			names = append(names, name)
		}
	}
	searchIndexMetaMu.RUnlock()
	if len(names) == 0 {
		return
	}
	searchEnginesMu.RLock()
	engines := make([]*redisearch.RediSearchEngine, 0, len(names))
	for _, name := range names {
		if e := searchEngines[name]; e != nil {
			engines = append(engines, e)
		}
	}
	searchEnginesMu.RUnlock()
	for _, engine := range engines {
		engine.DeleteDocument(key)
	}
}

// reindexJSON indexes a JSON key into matching FT indexes (ON JSON).
func reindexJSON(db *DB, key string) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return
	}
	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return
	}
	searchIndexMetaMu.RLock()
	metas := make(map[string]*indexMeta, len(searchIndexMeta))
	for name, meta := range searchIndexMeta {
		metas[name] = meta
	}
	searchIndexMetaMu.RUnlock()
	if len(metas) == 0 {
		return
	}
	searchEnginesMu.RLock()
	engines := make(map[string]*redisearch.RediSearchEngine, len(metas))
	for name := range metas {
		if e := searchEngines[name]; e != nil {
			engines[name] = e
		}
	}
	searchEnginesMu.RUnlock()

	for name, meta := range metas {
		if meta.onType != "JSON" {
			continue
		}
		if !indexMatchesKey(meta.prefixes, key) {
			continue
		}
		engine := engines[name]
		if engine == nil {
			continue
		}
		fields := make(map[string]interface{}, len(meta.schema))
		for _, f := range meta.schema {
			path := f.Path
			if path == "" {
				path = f.Name
			}
			if !strings.HasPrefix(path, "$") {
				path = "$." + path
			}
			val, err := jv.Get(path)
			if err != nil || val == nil {
				continue
			}
			fields[f.Name] = val
		}
		engine.DeleteDocument(key)
		_ = engine.AddDocument(key, fields, 1.0, nil)
	}
}

// removeJSONFromIndex removes a JSON key from ON JSON indexes.
func removeJSONFromIndex(db *DB, key string) {
	_ = db
	searchIndexMetaMu.RLock()
	names := make([]string, 0, len(searchIndexMeta))
	for name, meta := range searchIndexMeta {
		if meta.onType != "JSON" {
			continue
		}
		if indexMatchesKey(meta.prefixes, key) {
			names = append(names, name)
		}
	}
	searchIndexMetaMu.RUnlock()
	if len(names) == 0 {
		return
	}
	searchEnginesMu.RLock()
	engines := make([]*redisearch.RediSearchEngine, 0, len(names))
	for _, name := range names {
		if e := searchEngines[name]; e != nil {
			engines = append(engines, e)
		}
	}
	searchEnginesMu.RUnlock()
	for _, engine := range engines {
		engine.DeleteDocument(key)
	}
}

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
	onType := "HASH" // Redis default
	schemaStart := 1

	for i := 1; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))

		switch arg {
		case "ON":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			t := strings.ToUpper(string(args[i+1]))
			if t != "HASH" && t != "JSON" {
				return protocol.MakeErrReply("ERR Wrong type specified for ON. Expected HASH or JSON.")
			}
			onType = t
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

	fields, errReply := parseFTSchemaFields(args[schemaStart:])
	if errReply != nil {
		return errReply
	}
	if len(fields) == 0 {
		return protocol.MakeErrReply("ERR No schema specified")
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

	// Store prefix + schema meta for hash auto-indexing (RediSearch ON HASH).
	meta := &indexMeta{prefixes: prefix, schema: fields, onType: onType}
	searchIndexMetaMu.Lock()
	searchIndexMeta[indexName] = meta
	searchIndexMetaMu.Unlock()

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

	indexName := resolveSearchIndex(string(args[0]))
	deleteDocs := false

	if len(args) == 2 && strings.ToUpper(string(args[1])) == "DD" {
		deleteDocs = true
	}

	searchEnginesMu.Lock()
	engine, ok := searchEngines[indexName]
	searchEnginesMu.Unlock()

	if !ok {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Index '%s' does not exist", string(args[0])))
	}

	if err := engine.DropIndex(deleteDocs); err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}

	searchEnginesMu.Lock()
	delete(searchEngines, indexName)
	searchEnginesMu.Unlock()

	searchIndexMetaMu.Lock()
	delete(searchIndexMeta, indexName)
	searchIndexMetaMu.Unlock()

	clearAliasesForIndex(indexName)

	db.Remove(indexName)
	dropSynDB(indexName)

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

	indexName := resolveSearchIndex(string(args[0]))
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

	_ = language // accepted for wire compat; tokenizer is language-agnostic today
	if !nosave {
		db.addAof(utils.ToCmdLine3("ft.add", args...))
	}
	return protocol.MakeOkReply()
}

// execFTDel deletes a document from an index
// FT.DEL index doc_id [DD]
func execFTDel(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 || len(args) > 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.del' command")
	}

	indexName := resolveSearchIndex(string(args[0]))
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

	indexName := resolveSearchIndex(string(args[0]))
	query := string(args[1])

	searchEnginesMu.RLock()
	engine, ok := searchEngines[indexName]
	searchEnginesMu.RUnlock()

	if !ok {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Index '%s' does not exist", indexName))
	}

	// Parse options (Redis default LIMIT 0 10 when omitted)
	opts := &redisearch.SearchOptions{Limit: 10}
	noContent := false
	withScores := false
	withPayloads := false
	withSortKeys := false
	type returnFieldSpec struct {
		source string
		name   string // reply key (AS alias or source)
	}
	returnFields := []returnFieldSpec{}
	returnSpecified := false
	dialectSpecified := false
	var inKeys map[string]struct{}

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
		case "VERBATIM":
			opts.Verbatim = true
		case "NOSTOPWORDS":
			opts.NoStopWords = true
		case "WITHSORTKEYS":
			withSortKeys = true
			opts.WithSortKeys = true
		case "WITHCURSOR":
			return protocol.MakeErrReply("ERR WITHCURSOR is not supported")
		case "DIALECT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			d, err := strconv.Atoi(string(args[i+1]))
			if err != nil || !validFTDialect(d) {
				return protocol.MakeErrReply("ERR Invalid DIALECT value")
			}
			dialectSpecified = true
			i++ // dialect recorded for validation; query engine is fixed
		case "SLOP":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			slop, err := strconv.Atoi(string(args[i+1]))
			if err != nil || slop < 0 {
				return protocol.MakeErrReply("ERR Invalid SLOP value")
			}
			opts.Slop = slop
			i++
		case "INORDER":
			opts.InOrder = true
		case "TIMEOUT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			ms, err := strconv.Atoi(string(args[i+1]))
			if err != nil || ms < 0 {
				return protocol.MakeErrReply("ERR Invalid TIMEOUT value")
			}
			opts.TimeoutMs = ms
			i++ // accept; cancellation not wired
		case "INFIELDS":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil || count < 0 {
				return protocol.MakeErrReply("ERR Invalid INFIELDS count")
			}
			i += 2
			for j := 0; j < count; j++ {
				if i >= len(args) {
					return protocol.MakeSyntaxErrReply()
				}
				opts.InFields = append(opts.InFields, string(args[i]))
				i++
			}
			i--
		case "SUMMARIZE":
			opts.Summarize = true
			if opts.SummarizeLen == 0 {
				opts.SummarizeLen = 20
			}
			for i+1 < len(args) {
				next := strings.ToUpper(string(args[i+1]))
				if next == "FIELDS" {
					if i+2 >= len(args) {
						return protocol.MakeSyntaxErrReply()
					}
					n, err := strconv.Atoi(string(args[i+2]))
					if err != nil || n < 0 {
						return protocol.MakeErrReply("ERR Invalid SUMMARIZE FIELDS count")
					}
					last := i + 2 + n
					if last >= len(args) {
						return protocol.MakeSyntaxErrReply()
					}
					for j := i + 3; j <= last; j++ {
						opts.SummarizeFields = append(opts.SummarizeFields, string(args[j]))
					}
					i = last
					continue
				}
				if next == "FRAGS" || next == "LEN" {
					if i+2 >= len(args) {
						return protocol.MakeSyntaxErrReply()
					}
					v, err := strconv.Atoi(string(args[i+2]))
					if err != nil || v < 0 {
						return protocol.MakeErrReply("ERR Invalid SUMMARIZE " + next + " value")
					}
					if next == "LEN" {
						opts.SummarizeLen = v
					}
					// FRAGS accepted but unused (single-fragment truncate)
					i += 2
					continue
				}
				if next == "SEPARATOR" {
					if i+2 >= len(args) {
						return protocol.MakeSyntaxErrReply()
					}
					i += 2 // accept separator; unused in minimal truncate
					continue
				}
				break
			}
		case "INKEYS":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil || count < 0 {
				return protocol.MakeErrReply("ERR Invalid INKEYS count")
			}
			i += 2
			inKeys = make(map[string]struct{}, count)
			for j := 0; j < count; j++ {
				if i >= len(args) {
					return protocol.MakeSyntaxErrReply()
				}
				inKeys[string(args[i])] = struct{}{}
				i++
			}
			i--
		case "HIGHLIGHT":
			opts.Highlight = true
			if opts.HighlightOpenTag == "" {
				opts.HighlightOpenTag = "<b>"
			}
			if opts.HighlightCloseTag == "" {
				opts.HighlightCloseTag = "</b>"
			}
			for i+1 < len(args) {
				next := strings.ToUpper(string(args[i+1]))
				if next == "FIELDS" {
					if i+2 >= len(args) {
						return protocol.MakeSyntaxErrReply()
					}
					n, err := strconv.Atoi(string(args[i+2]))
					if err != nil || n < 0 {
						return protocol.MakeErrReply("ERR Invalid HIGHLIGHT FIELDS count")
					}
					last := i + 2 + n
					if last >= len(args) {
						return protocol.MakeSyntaxErrReply()
					}
					for j := i + 3; j <= last; j++ {
						opts.HighlightFields = append(opts.HighlightFields, string(args[j]))
					}
					i = last
					continue
				}
				if next == "TAGS" {
					if i+3 >= len(args) {
						return protocol.MakeSyntaxErrReply()
					}
					opts.HighlightOpenTag = string(args[i+2])
					opts.HighlightCloseTag = string(args[i+3])
					i += 3
					continue
				}
				break
			}
		case "FILTER":
			if i+3 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			minV, err1 := strconv.ParseFloat(string(args[i+2]), 64)
			maxV, err2 := strconv.ParseFloat(string(args[i+3]), 64)
			if err1 != nil || err2 != nil {
				return protocol.MakeErrReply("ERR Invalid filter range")
			}
			opts.Filters = append(opts.Filters, redisearch.FieldFilter{
				Field: string(args[i+1]),
				Min:   minV,
				Max:   maxV,
			})
			i += 3
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
			returnSpecified = true
			i += 2
			for j := 0; j < count && i < len(args); j++ {
				nextArg := strings.ToUpper(string(args[i]))
				if nextArg == "LIMIT" || nextArg == "SORTBY" || nextArg == "GEOFILTER" ||
					nextArg == "WITHCURSOR" || nextArg == "HIGHLIGHT" || nextArg == "SUMMARIZE" {
					i--
					break
				}
				src := string(args[i])
				name := src
				i++
				if i < len(args) && strings.EqualFold(string(args[i]), "AS") {
					if i+1 >= len(args) {
						return protocol.MakeSyntaxErrReply()
					}
					name = string(args[i+1])
					i += 2
				}
				returnFields = append(returnFields, returnFieldSpec{source: src, name: name})
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
				Field:  strings.TrimPrefix(geoField, "@"),
				Lon:    lon,
				Lat:    lat,
				Radius: radius,
				Unit:   unit,
			}
			i += 5
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	// Apply FT.CONFIG defaults / caps (MAXSEARCHRESULTS, TIMEOUT, DEFAULT_DIALECT).
	if !dialectSpecified {
		d := getFTConfigInt("DEFAULT_DIALECT")
		if d == 0 {
			d = 1
		}
		if !validFTDialect(d) {
			return protocol.MakeErrReply("ERR Invalid DIALECT value")
		}
	}
	if opts.TimeoutMs == 0 {
		if t := getFTConfigInt("TIMEOUT"); t > 0 {
			opts.TimeoutMs = t
		}
	}
	if max := getFTConfigInt("MAXSEARCHRESULTS"); max > 0 && opts.Limit > max {
		opts.Limit = max
	}

	// Search
	results, err := engine.Search(query, opts)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}

	if len(inKeys) > 0 {
		filtered := make([]*redisearch.SearchResult, 0, len(results.Results))
		for _, result := range results.Results {
			if result.Document == nil {
				continue
			}
			if _, ok := inKeys[result.Document.ID]; ok {
				filtered = append(filtered, result)
			}
		}
		results.Results = filtered
		results.Total = len(filtered)
	}

	// Build response in RediSearch wire format:
	//   [total(int), docId, [field,val,...], docId, [field,val,...], ...]
	// The total must be an integer (not a bulk string) and each document's
	// fields must be a nested array, otherwise clients like go-redis reject the
	// reply ("invalid total results format").
	replies := make([]redis.Reply, 0, 1+3*len(results.Results))
	replies = append(replies, protocol.MakeIntReply(int64(results.Total)))

	for _, result := range results.Results {
		replies = append(replies, protocol.MakeBulkReply([]byte(result.Document.ID)))

		if withScores {
			replies = append(replies, protocol.MakeBulkReply([]byte(fmt.Sprintf("%.6f", result.Score))))
		}

		if withPayloads && result.Document.Payload != nil {
			replies = append(replies, protocol.MakeBulkReply(result.Document.Payload))
		}

		if withSortKeys {
			sk := ""
			if opts.SortBy != "" {
				if val, ok := result.Fields[opts.SortBy]; ok {
					sk = fmt.Sprintf("%v", val)
				} else if result.Document != nil && result.Document.Fields != nil {
					if val, ok := result.Document.Fields[opts.SortBy]; ok {
						sk = fmt.Sprintf("%v", val)
					}
				}
			}
			replies = append(replies, protocol.MakeBulkReply([]byte(sk)))
		}

		if !noContent {
			// Return fields
			var fields [][]byte

			formatVal := func(field string, val interface{}) string {
				s := fmt.Sprintf("%v", val)
				if opts.Summarize {
					if len(opts.SummarizeFields) > 0 {
						ok := false
						for _, f := range opts.SummarizeFields {
							if f == field {
								ok = true
								break
							}
						}
						if !ok {
							return s
						}
					}
					return summarizeFTText(s, opts.SummarizeLen)
				}
				return s
			}

			if returnSpecified {
				for _, rf := range returnFields {
					if val, ok := result.Fields[rf.source]; ok {
						fields = append(fields, []byte(rf.name))
						fields = append(fields, []byte(formatVal(rf.source, val)))
					}
				}
			} else {
				// Return all fields when RETURN omitted
				for k, v := range result.Fields {
					fields = append(fields, []byte(k))
					fields = append(fields, []byte(formatVal(k, v)))
				}
			}

			replies = append(replies, protocol.MakeMultiBulkReply(fields))

			// Add highlights if requested
			if opts.Highlight && len(result.Highlights) > 0 {
				var highlights [][]byte
				for field, value := range result.Highlights {
					highlights = append(highlights, []byte(field))
					highlights = append(highlights, []byte(value))
				}
				if len(highlights) > 0 {
					replies = append(replies, protocol.MakeBulkReply([]byte("highlight")))
					replies = append(replies, protocol.MakeMultiBulkReply(highlights))
				}
			}
		}
	}

	return protocol.MakeMultiRawReply(replies)
}

// summarizeFTText truncates field text for FT.SEARCH SUMMARIZE (minimal single fragment).
func summarizeFTText(s string, maxLen int) string {
	if maxLen <= 0 {
		maxLen = 20
	}
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen]) + "..."
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

	indexName := resolveSearchIndex(string(args[0]))
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
		case "VERBATIM":
			req.Verbatim = true
			i++
			continue

		case "WITHCURSOR":
			return protocol.MakeErrReply("ERR WITHCURSOR is not supported")

		case "TIMEOUT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			ms, err := strconv.Atoi(string(args[i+1]))
			if err != nil || ms < 0 {
				return protocol.MakeErrReply("ERR Invalid TIMEOUT value")
			}
			req.TimeoutMs = ms
			i += 2
			continue

		case "APPLY":
			return protocol.MakeErrReply("ERR APPLY is not supported")

		case "LOAD":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			if string(args[i+1]) == "*" {
				req.LoadAll = true
				i += 2
				continue
			}
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("ERR Invalid load count")
			}
			i += 2
			for j := 0; j < count && i < len(args); j++ {
				nextArg := strings.ToUpper(string(args[i]))
				if nextArg == "GROUPBY" || nextArg == "SORTBY" || nextArg == "LIMIT" ||
					nextArg == "APPLY" || nextArg == "FILTER" || nextArg == "LOAD" {
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

		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	if max := getFTConfigInt("MAXSEARCHRESULTS"); max > 0 && req.Limit > max {
		req.Limit = max
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

	indexName := resolveSearchIndex(string(args[0]))

	searchEnginesMu.RLock()
	engine, ok := searchEngines[indexName]
	searchEnginesMu.RUnlock()

	if !ok {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Index '%s' does not exist", string(args[0])))
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

	if !engine.DelSuggestion(str) {
		return protocol.MakeIntReply(0)
	}

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

	return protocol.MakeIntReply(int64(engine.SuggestionCount()))
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
	registerCommand("FT.Create", execFTCreate, writeFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("FT.DropIndex", execFTDropIndex, writeFirstKey, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("FT.Add", execFTAdd, writeFirstKey, nil, -5, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("FT.Del", execFTDel, writeFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("FT.Search", execFTSearch, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("FT.Aggregate", execFTAggregate, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("FT.Info", execFTInfo, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("FT._List", execFTList, prepareNoKeys, nil, 1, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
	registerCommand("FT.SugAdd", execFTSugAdd, writeFirstKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("FT.SugGet", execFTSugGet, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("FT.SugDel", execFTSugDel, writeFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("FT.SugLen", execFTSugLen, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
