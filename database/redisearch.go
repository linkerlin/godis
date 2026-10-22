package database

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	Dict "github.com/linkerlin/godis/datastruct/dict"
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
	// Index-level options mirrored onto the engine for FT.INFO / later wiring.
	noOffsets     bool
	noFields      bool
	noFreqs       bool
	noHL          bool
	maxTextFields bool
	temporary     int
	filterExpr    string
	indexAll      string
	indexMissing  bool
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

// reindexKey re-indexes a key into every matching FT index regardless of the
// key's type (HASH or JSON). The type-specific reindex paths are no-ops when
// the key is the wrong type, so calling both is safe. Use after any mutation
// that might change a key's indexed content but not its identity (HINCRBY,
// RENAME destination, JSON.ARRAPPEND, TTL changes that affect FILTER/score).
func reindexKey(db *DB, key string) {
	reindexHash(db, key)
	reindexJSON(db, key)
}

// removeKeyFromIndex removes a key from every FT index regardless of type.
// Use when a key is deleted outright (RENAME source, COPY overwrite of dest,
// RESTORE replacing a different-typed value).
func removeKeyFromIndex(db *DB, key string) {
	removeHashFromIndex(db, key)
	removeJSONFromIndex(db, key)
}

// backfillIndexFromExistingKeys scans the whole keyspace and indexes every
// already-existing key that matches the new index's prefixes/type, mirroring
// RediSearch's synchronous initial index build. Called from FT.CREATE unless
// SKIPINITIALSCAN was given.
//
// FT.CREATE holds a write lock on indexName (via writeFirstKey) for the
// duration of execFTCreate, so the scan must skip re-locking that key's shard
// to avoid deadlocking against the lock the current goroutine already holds.
func backfillIndexFromExistingKeys(db *DB, indexName string, engine *redisearch.RediSearchEngine, meta *indexMeta) {
	db.ForEachSkippingLockedKeys([]string{indexName}, func(key string, entity *database.DataEntity, _ *time.Time) bool {
		if entity == nil || !indexMatchesKey(meta.prefixes, key) {
			return true
		}
		switch meta.onType {
		case "JSON":
			jv, ok := entity.Data.(*godisjson.JSONValue)
			if !ok {
				return true
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
		default:
			dict, ok := entity.Data.(Dict.Dict)
			if !ok {
				return true
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
		return true
	})
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
	schemaStart := -1
	skipInitialScan := false
	var stopWords []string
	hasStopWords := false
	// Index-level option scratch (FT.CREATE 8.x).
	var noOffsets, noFields, noFreqs, noHL, maxTextFields bool
	var temporary int
	var hasTemporary bool
	var filterExpr string
	var indexAll string

	for i := 1; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))

		switch arg {
		case "ON":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			t := strings.ToUpper(string(args[i+1]))
			if t != "HASH" && t != "JSON" {
				// Redis 8.x QE: SEARCH_ADD_ARGS Invalid rule type: <TOKEN>
				return protocol.MakeErrReply("SEARCH_ADD_ARGS Invalid rule type: " + t)
			}
			onType = t
			i++
		case "PREFIX":
			// Redis 8.x: missing / non-int / negative → SEARCH_PARSE_ARGS (like STOPWORDS).
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for PREFIX: Expected an argument, but none provided")
			}
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for PREFIX: Could not convert argument to expected type")
			}
			if count < 0 {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for PREFIX: Value is outside acceptable bounds")
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
		case "SKIPINITIALSCAN":
			skipInitialScan = true
		case "STOPWORDS":
			// Redis 8.x: missing / non-int / negative have distinct SEARCH_PARSE_ARGS.
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for STOPWORDS: Expected an argument, but none provided")
			}
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for STOPWORDS: Could not convert argument to expected type")
			}
			if count < 0 {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for STOPWORDS: Value is outside acceptable bounds")
			}
			i += 2
			stopWords = make([]string, 0, count)
			for j := 0; j < count && i < len(args); j++ {
				if reply := validateBulkBytes(args[i]); reply != nil {
					return reply
				}
				stopWords = append(stopWords, string(args[i]))
				i++
			}
			hasStopWords = true
			i--
		case "NOOFFSETS":
			noOffsets = true
		case "NOHL":
			noHL = true
		case "NOFIELDS":
			noFields = true
		case "NOFREQS":
			noFreqs = true
		case "MAXTEXTFIELDS":
			maxTextFields = true
		case "TEMPORARY":
			// Redis 8.x: accepts any integer (incl. negative); 0 → expire immediately.
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for TEMPORARY: Expected an argument, but none provided")
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for TEMPORARY: Could not convert argument to expected type")
			}
			temporary = n
			hasTemporary = true
			i++
		case "FILTER":
			// FILTER uses the aggregation expression language; evaluated in
			// AddDocument against document fields + @__key (Redis 8.x).
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for FILTER: Expected an argument, but none provided")
			}
			filterExpr = string(args[i+1])
			i++
		case "INDEXALL":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for INDEXALL: Expected an argument, but none provided")
			}
			v := strings.ToUpper(string(args[i+1]))
			if v != "ENABLE" && v != "DISABLE" {
				return protocol.MakeErrReply("SEARCH_ADD_ARGS Invalid argument for `INDEXALL`, use ENABLE/DISABLE")
			}
			indexAll = v
			i++
		case "LANGUAGE":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for LANGUAGE: Expected an argument, but none provided")
			}
			if !redisearch.IsKnownSearchLanguage(string(args[i+1])) {
				return protocol.MakeErrReply("SEARCH_ADD_ARGS Invalid language")
			}
			i++
		case "SCORE":
			// Redis 8.x default document score ∈ [0,1] or NaN; others → Invalid score.
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for SCORE: Expected an argument, but none provided")
			}
			sc, err := strconv.ParseFloat(string(args[i+1]), 64)
			if err != nil || math.IsInf(sc, 0) || (!math.IsNaN(sc) && (sc < 0 || sc > 1)) {
				return protocol.MakeErrReply("SEARCH_ADD_ARGS Invalid score")
			}
			i++
		case "LANGUAGE_FIELD", "SCORE_FIELD", "PAYLOAD_FIELD":
			// Accepted for syntax parity; field name consumed, value unused here.
			// Redis 8.x: missing → SEARCH_PARSE_ARGS Bad arguments for <OPT>: …
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for " + arg + ": Expected an argument, but none provided")
			}
			i++
		case "SCHEMA":
			schemaStart = i + 1
			i = len(args) // Break out
		}
	}

	if schemaStart < 0 {
		return protocol.MakeErrReply("SEARCH_PARSE_ARGS No schema found")
	}
	if schemaStart >= len(args) {
		// SCHEMA present but no field tokens — Redis 8.x distinct from "No schema found".
		return protocol.MakeErrReply("SEARCH_PARSE_ARGS Fields arguments are missing")
	}

	fields, errReply := parseFTSchemaFields(args[schemaStart:])
	if errReply != nil {
		return errReply
	}
	if len(fields) == 0 {
		return protocol.MakeErrReply("SEARCH_PARSE_ARGS Fields arguments are missing")
	}

	// Redis: TEMPORARY 0 → CREATE OK but index is gone immediately (INFO not found).
	if hasTemporary && temporary == 0 {
		db.addAof(utils.ToCmdLine3("ft.create", args...))
		return protocol.MakeOkReply()
	}

	// Create engine
	config := &redisearch.EngineConfig{
		Name:         indexName,
		StopWords:    stopWords,
		HasStopWords: hasStopWords,
		NoOffsets:    noOffsets,
		NoFields:     noFields,
		NoFreqs:      noFreqs,
		NoHL:         noHL,
		MaxTextFields: maxTextFields,
		Temporary:    temporary,
		Filter:       filterExpr,
		IndexAll:     indexAll,
		IndexMissing: indexAll == "ENABLE",
	}

	engine := redisearch.NewRediSearchEngine(config)
	if err := engine.CreateIndex(fields); err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}
	engine.SetCreateArgs(args)

	// Store engine
	searchEnginesMu.Lock()
	searchEngines[indexName] = engine
	searchEnginesMu.Unlock()

	// Store prefix + schema meta for hash auto-indexing (RediSearch ON HASH).
	meta := &indexMeta{
		prefixes:      prefix,
		schema:        fields,
		onType:        onType,
		noOffsets:     noOffsets,
		noFields:      noFields,
		noFreqs:       noFreqs,
		noHL:          noHL,
		maxTextFields: maxTextFields,
		temporary:     temporary,
		filterExpr:    filterExpr,
		indexAll:      indexAll,
		indexMissing:  indexAll == "ENABLE",
	}
	searchIndexMetaMu.Lock()
	searchIndexMeta[indexName] = meta
	searchIndexMetaMu.Unlock()

	// Also store in DB for persistence tracking
	db.PutEntity(indexName, &database.DataEntity{Data: engine})

	// RediSearch indexes existing matching keys synchronously unless told not to.
	if !skipInitialScan {
		backfillIndexFromExistingKeys(db, indexName, engine, meta)
	}

	db.addAof(utils.ToCmdLine3("ft.create", args...))
	return protocol.MakeOkReply()
}

// execFTDropIndex drops an index
// FT.DROPINDEX index [DD]
func execFTDropIndex(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.DROPINDEX' command")
	}

	indexName := resolveSearchIndex(string(args[0]))
	searchEnginesMu.Lock()
	engine, ok := searchEngines[indexName]
	searchEnginesMu.Unlock()
	if !ok {
		return searchIndexNotFoundReply(string(args[0]))
	}

	deleteDocs := false
	if len(args) >= 2 {
		if strings.EqualFold(string(args[1]), "DD") {
			deleteDocs = true
			if len(args) > 2 {
				return protocol.MakeErrReply("SEARCH_ARG_UNRECOGNIZED Unknown argument")
			}
		} else {
			return protocol.MakeErrReply("SEARCH_ARG_UNRECOGNIZED Unknown argument")
		}
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
		return protocol.MakeErrReply(fmt.Sprintf("SEARCH_INDEX_NOT_FOUND Index not found: %s", indexName))
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
				return protocol.MakeErrReply("ERR invalid score")
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
			if !redisearch.IsKnownSearchLanguage(language) {
				return protocol.MakeErrReply("Invalid language")
			}
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
		return protocol.MakeErrReply(fmt.Sprintf("SEARCH_INDEX_NOT_FOUND Index not found: %s", indexName))
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
func parseFTCursorCountArg(args [][]byte, idx int) (count int, errMsg string) {
	if idx >= len(args) {
		return 0, "Bad arguments for COUNT: Expected an argument, but none provided"
	}
	n, err := strconv.Atoi(string(args[idx]))
	if err != nil {
		return 0, "Bad arguments for COUNT: Could not convert argument to expected type"
	}
	if n <= 0 {
		return 0, "Bad arguments for COUNT: Value is outside acceptable bounds"
	}
	return n, ""
}

// parseFTLimitPair parses LIMIT offset num at args[i]==LIMIT (Redis 8.x).
// Missing args → "LIMIT requires two arguments"; non-numeric or negative →
// "LIMIT needs two numeric arguments". Messages use SEARCH_PARSE_ARGS (no ERR).
func parseFTLimitPair(args [][]byte, i int) (offset, limit int, errMsg string) {
	if i+2 >= len(args) {
		return 0, 0, "SEARCH_PARSE_ARGS LIMIT requires two arguments"
	}
	off, err1 := strconv.Atoi(string(args[i+1]))
	lim, err2 := strconv.Atoi(string(args[i+2]))
	if err1 != nil || err2 != nil || off < 0 || lim < 0 {
		return 0, 0, "SEARCH_PARSE_ARGS LIMIT needs two numeric arguments"
	}
	return off, lim, ""
}

// validateFTReducer mirrors Redis 8.x FT.AGGREGATE REDUCE arity / name errors
// (no ERR prefix). COLLECT is Godis 8.8+ support and is accepted.
func validateFTReducer(funcName string, r redisearch.Reducer) string {
	fn := strings.ToUpper(r.Function)
	switch fn {
	case "COUNT":
		if len(r.Args) != 0 {
			return "Count accepts 0 values only"
		}
	case "SUM", "MIN", "MAX", "AVG", "STDDEV", "TOLIST", "COUNT_DISTINCT", "COUNT_DISTINCTISH":
		if len(r.Args) < 1 {
			return "Missing arguments for " + fn
		}
	case "QUANTILE":
		if len(r.Args) < 1 {
			return "Missing arguments for QUANTILE"
		}
		if len(r.Args) < 2 {
			return "Bad arguments for QUANTILE: Could not convert argument to expected type"
		}
		q, err := strconv.ParseFloat(r.Args[1], 64)
		if err != nil {
			return "Bad arguments for QUANTILE: Could not convert argument to expected type"
		}
		if q < 0 || q > 1 {
			return "Percentage must be between 0.0 and 1.0"
		}
	case "FIRST_VALUE":
		if len(r.Args) < 1 {
			return "Missing arguments for FIRST_VALUE"
		}
		if len(r.Args) == 1 {
			return ""
		}
		if !strings.EqualFold(r.Args[1], "BY") {
			return fmt.Sprintf("Unknown argument `%s` at position 1 for FIRST_VALUE", r.Args[1])
		}
		if len(r.Args) < 3 {
			return "Missing arguments for FIRST_VALUE"
		}
		if len(r.Args) >= 4 {
			dir := strings.ToUpper(r.Args[3])
			if dir != "ASC" && dir != "DESC" {
				return fmt.Sprintf("Unknown argument `%s` at position 3 for FIRST_VALUE", r.Args[3])
			}
		}
		if len(r.Args) > 4 {
			return fmt.Sprintf("Unknown argument `%s` at position 4 for FIRST_VALUE", r.Args[4])
		}
	case "RANDOM_SAMPLE":
		if len(r.Args) < 1 {
			return "Missing arguments for RANDOM_SAMPLE"
		}
		if len(r.Args) < 2 {
			return "Bad arguments for <sample size>: Expected an argument, but none provided"
		}
		size, err := strconv.Atoi(r.Args[1])
		if err != nil {
			return "Bad arguments for <sample size>: Could not convert argument to expected type"
		}
		if size < 0 {
			return "Bad arguments for <sample size>: Value is outside acceptable bounds"
		}
	case "COLLECT":
		// Redis 8.8+; keep accepting on Godis.
	default:
		return "No such reducer: " + funcName
	}
	return ""
}

// aggRowBytes encodes one aggregation result row in the wire format used by
// execFTAggregate: an optional leading GROUPBY key (skipped when the row came
// from a passthrough, non-grouped document) followed by field/value pairs.
// COLLECT reducer results ([]redisearch.CollectEntry) are serialized as nested
// arrays rather than Go %v map printing.
func aggRowBytes(group *redisearch.Group) []byte {
	var fields [][]byte
	if group.By != nil {
		fields = append(fields, []byte(fmt.Sprintf("%v", group.By)))
	}
	for k, v := range group.Fields {
		fields = append(fields, []byte(k))
		fields = append(fields, collectValueBytes(v))
	}
	return protocol.MakeMultiBulkReply(fields).ToBytes()
}

// collectValueBytes renders a GROUPBY field value for the wire. []CollectEntry
// becomes a nested array of k/v maps; []string (matched_terms / multi-value)
// and []interface{} (REDUCE TOLIST) become nested bulk arrays — not Go %v.
func collectValueBytes(v interface{}) []byte {
	switch entries := v.(type) {
	case []redisearch.CollectEntry:
		elems := make([][]byte, 0, len(entries))
		for _, e := range entries {
			var kv [][]byte
			for k, fv := range e.Fields {
				kv = append(kv, []byte(k))
				kv = append(kv, []byte(fmt.Sprintf("%v", fv)))
			}
			elems = append(elems, protocol.MakeMultiBulkReply(kv).ToBytes())
		}
		return protocol.MakeMultiBulkReply(elems).ToBytes()
	case []string:
		elems := make([][]byte, len(entries))
		for i, s := range entries {
			elems[i] = []byte(s)
		}
		return protocol.MakeMultiBulkReply(elems).ToBytes()
	case []interface{}:
		elems := make([][]byte, len(entries))
		for i, x := range entries {
			if x == nil {
				elems[i] = nil // MultiBulk nil → $-1
				continue
			}
			elems[i] = []byte(fmt.Sprintf("%v", x))
		}
		return protocol.MakeMultiBulkReply(elems).ToBytes()
	default:
		if v == nil {
			return nil // Redis Null APPLY field → $-1 (not empty $0)
		}
		switch x := v.(type) {
		case float64:
			if math.IsNaN(x) {
				return []byte("nan")
			}
			return []byte(strconv.FormatFloat(x, 'f', -1, 64))
		case bool:
			// Redis APPLY bools (exists/startswith/…) wire as "1"/"0", not "true"/"false".
			if x {
				return []byte("1")
			}
			return []byte("0")
		default:
			return []byte(fmt.Sprintf("%v", v))
		}
	}
}

// ftCursorEntry holds the not-yet-delivered page of an FT.AGGREGATE WITHCURSOR
// result, keyed by an opaque cursor id.
type ftCursorEntry struct {
	indexName  string
	total      int
	rows       [][]byte
	lastAccess time.Time
	maxIdle    time.Duration // FROM WITHCURSOR MAXIDLE; 0 → ftCursorIdleTimeout
}

const ftCursorIdleTimeout = time.Minute

// Redis Query Engine caps MAXIDLE at 300000 ms (default when omitted is 5 min).
const ftCursorMaxIdleCapMs = 300000

var (
	ftCursorMu      sync.Mutex
	ftCursorStore   = make(map[uint64]*ftCursorEntry)
	ftCursorCounter uint64
)

// ftSweepExpiredCursorsLocked drops cursors idle past their MAXIDLE (or the
// default). Caller must hold ftCursorMu.
func ftSweepExpiredCursorsLocked() {
	if len(ftCursorStore) == 0 {
		return
	}
	now := time.Now()
	for id, entry := range ftCursorStore {
		idle := entry.maxIdle
		if idle <= 0 {
			idle = ftCursorIdleTimeout
		}
		if now.Sub(entry.lastAccess) > idle {
			delete(ftCursorStore, id)
		}
	}
}

// ftBuildCursorPage slices off up to count rows to return immediately,
// storing the remainder (if any) under a freshly minted cursor id. Returns
// the Redis reply shape `[[total, row, row, ...], cursorID]`, with cursorID
// 0 once the result set is exhausted.
func ftBuildCursorPage(indexName string, total int, rows [][]byte, count int, maxIdle time.Duration) redis.Reply {
	if count <= 0 {
		count = 10
	}
	if maxIdle <= 0 {
		maxIdle = ftCursorIdleTimeout
	}
	pageRows := rows
	remaining := ([][]byte)(nil)
	if count < len(rows) {
		pageRows = rows[:count]
		remaining = rows[count:]
	}

	innerArgs := make([][]byte, 0, 1+len(pageRows))
	innerArgs = append(innerArgs, []byte(strconv.Itoa(total)))
	innerArgs = append(innerArgs, pageRows...)
	inner := protocol.MakeMultiBulkReply(innerArgs)

	var cursorID uint64
	ftCursorMu.Lock()
	ftSweepExpiredCursorsLocked()
	if len(remaining) > 0 {
		ftCursorCounter++
		cursorID = ftCursorCounter
		ftCursorStore[cursorID] = &ftCursorEntry{
			indexName:  indexName,
			total:      total,
			rows:       remaining,
			lastAccess: time.Now(),
			maxIdle:    maxIdle,
		}
	}
	ftCursorMu.Unlock()

	return protocol.MakeMultiRawReply([]redis.Reply{inner, protocol.MakeIntReply(int64(cursorID))})
}

// execFTCursor handles FT.CURSOR READ/DEL for paging through a stored
// FT.AGGREGATE WITHCURSOR result set.
// FT.CURSOR READ index cursor [COUNT n]
// FT.CURSOR DEL index cursor
func execFTCursor(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.cursor' command")
	}
	sub := strings.ToUpper(string(args[0]))
	indexName := resolveSearchIndex(string(args[1]))
	cursorID, err := strconv.ParseUint(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("Cursor not found, id: %s", args[2]))
	}

	switch sub {
	case "READ":
		// Redis 8.x: bare COUNT with no value → ignore (default page);
		// COUNT 0 → default; COUNT <0 → remaining rows; non-numeric →
		// Bad value for COUNT: `…` (distinct from WITHCURSOR COUNT bounds).
		count := 0
		readAll := false
		if len(args) >= 4 && strings.EqualFold(string(args[3]), "COUNT") {
			if len(args) >= 5 {
				n, err := strconv.Atoi(string(args[4]))
				if err != nil {
					return protocol.MakeErrReply(fmt.Sprintf("Bad value for COUNT: `%s`", args[4]))
				}
				if n < 0 {
					readAll = true
				} else {
					count = n
				}
			}
		}

		ftCursorMu.Lock()
		entry, ok := ftCursorStore[cursorID]
		if ok {
			delete(ftCursorStore, cursorID)
		}
		ftCursorMu.Unlock()

		if !ok || entry.indexName != indexName {
			return protocol.MakeErrReply(fmt.Sprintf("Cursor not found, id: %d", cursorID))
		}
		if readAll {
			count = len(entry.rows)
			if count == 0 {
				count = 1
			}
		} else if count <= 0 {
			count = 10
		}
		return ftBuildCursorPage(indexName, entry.total, entry.rows, count, entry.maxIdle)

	case "DEL":
		ftCursorMu.Lock()
		_, ok := ftCursorStore[cursorID]
		if ok {
			delete(ftCursorStore, cursorID)
		}
		ftCursorMu.Unlock()
		if !ok {
			return protocol.MakeErrReply("Cursor does not exist")
		}
		return protocol.MakeOkReply()

	default:
		return protocol.MakeErrReply("ERR unknown subcommand for 'ft.cursor'")
	}
}

// prepareFTCursor read-locks the index name (args[1]) so FT.CURSOR doesn't
// race with FT.DROPINDEX/FT.CREATE on the same index.
func prepareFTCursor(args [][]byte) ([]string, []string) {
	if len(args) < 2 {
		return nil, nil
	}
	return nil, []string{string(args[1])}
}

// execFTInfo returns information about an index
// FT.INFO index
func execFTInfo(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.INFO' command")
	}

	indexName := resolveSearchIndex(string(args[0]))

	searchEnginesMu.RLock()
	engine, ok := searchEngines[indexName]
	searchEnginesMu.RUnlock()

	if !ok {
		return protocol.MakeErrReply(fmt.Sprintf("SEARCH_INDEX_NOT_FOUND Index not found: %s", string(args[0])))
	}

	info := engine.Info()

	// Build a MapReply so RESP2 connections get the flat k/v array and RESP3
	// connections get a proper map. Nested values (attributes, index_definition)
	// are converted recursively so structure is preserved in both protocols.
	m := protocol.MakeMapReply()
	for k, v := range info {
		m.Put(k, interfaceToReply(v))
	}
	return m
}

// interfaceToReply converts an arbitrary Go value (as produced by engine.Info)
// into a redis.Reply, preserving structure: maps become MapReply, slices become
// arrays, scalars become bulk/int replies. Used by FT.INFO so both RESP2 and
// RESP3 clients see correctly nested output.
func interfaceToReply(v interface{}) redis.Reply {
	switch x := v.(type) {
	case nil:
		return protocol.MakeNullBulkReply()
	case bool:
		if x {
			return protocol.MakeIntReply(1)
		}
		return protocol.MakeIntReply(0)
	case int:
		return protocol.MakeIntReply(int64(x))
	case int64:
		return protocol.MakeIntReply(x)
	case float64:
		return protocol.MakeBulkReply([]byte(strconv.FormatFloat(x, 'f', -1, 64)))
	case string:
		return protocol.MakeBulkReply([]byte(x))
	case []byte:
		return protocol.MakeBulkReply(x)
	case map[string]interface{}:
		sub := protocol.MakeMapReply()
		for k, sv := range x {
			sub.Put(k, interfaceToReply(sv))
		}
		return sub
	case map[string]bool:
		sub := protocol.MakeMapReply()
		for k, sv := range x {
			sub.Put(k, interfaceToReply(sv))
		}
		return sub
	case []string:
		elems := make([]redis.Reply, 0, len(x))
		for _, s := range x {
			elems = append(elems, protocol.MakeBulkReply([]byte(s)))
		}
		return protocol.MakeMultiRawReply(elems)
	case []map[string]interface{}:
		elems := make([]redis.Reply, 0, len(x))
		for _, sm := range x {
			elems = append(elems, interfaceToReply(sm))
		}
		return protocol.MakeMultiRawReply(elems)
	case []interface{}:
		elems := make([]redis.Reply, 0, len(x))
		for _, sv := range x {
			elems = append(elems, interfaceToReply(sv))
		}
		return protocol.MakeMultiRawReply(elems)
	default:
		return protocol.MakeBulkReply([]byte(fmt.Sprintf("%v", v)))
	}
}

// execFTList lists all indexes
// FT._LIST
func execFTList(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'FT._LIST' command")
	}

	searchEnginesMu.RLock()
	defer searchEnginesMu.RUnlock()

	names := make([]string, 0, len(searchEngines))
	for name := range searchEngines {
		names = append(names, name)
	}
	sort.Strings(names)
	indexes := make([][]byte, len(names))
	for i, name := range names {
		indexes[i] = []byte(name)
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
	if err != nil || math.IsNaN(score) {
		return protocol.MakeErrReply("ERR invalid score")
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
	case "TEXT", "NUMERIC", "TAG", "GEO", "VECTOR", "GEOSHAPE":
		return true
	default:
		return false
	}
}

// ftUnrecognizedArg mirrors Redis 8.x SEARCH_ARG_UNRECOGNIZED for unknown
// FT.SEARCH / FT.AGGREGATE option tokens. pos is 1-based among option tokens
// (args[0]=index, args[1]=query → position = i-1).
func ftUnrecognizedArg(arg string, pos int) redis.Reply {
	return protocol.MakeErrReply(fmt.Sprintf(
		"SEARCH_ARG_UNRECOGNIZED Unknown argument `%s` at position %d for <main>", arg, pos,
	))
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
	registerCommand("FT.Cursor", execFTCursor, prepareFTCursor, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 2, 2, 1)
	registerCommand("FT.Info", execFTInfo, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("FT._List", execFTList, prepareNoKeys, nil, -1, flagReadOnly).
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
