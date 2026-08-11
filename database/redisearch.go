package database

import (
	"fmt"
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
	schemaStart := 1
	skipInitialScan := false
	var stopWords []string
	hasStopWords := false
	// Index-level option scratch (FT.CREATE 8.x).
	var noOffsets, noFields, noFreqs, noHL, maxTextFields bool
	var temporary int
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
		case "SKIPINITIALSCAN":
			skipInitialScan = true
		case "STOPWORDS":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil || count < 0 {
				return protocol.MakeErrReply("ERR Invalid stopwords count")
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
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil || n <= 0 {
				return protocol.MakeErrReply("ERR Invalid TEMPORARY value")
			}
			temporary = n
			i++
		case "FILTER":
			// ponytail: FILTER expression is stored but not yet evaluated; needs
			// an aggregation-style expression evaluator against @__key and doc
			// fields. Until then it is accepted for syntax parity.
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			filterExpr = string(args[i+1])
			i++
		case "INDEXALL":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			v := strings.ToUpper(string(args[i+1]))
			if v != "ENABLE" && v != "DISABLE" {
				return protocol.MakeErrReply("ERR Invalid INDEXALL value, expected ENABLE or DISABLE")
			}
			indexAll = v
			i++
		case "LANGUAGE", "LANGUAGE_FIELD", "SCORE", "SCORE_FIELD", "PAYLOAD_FIELD":
			// Accepted for syntax parity; the few that are wired (SCORE_FIELD,
			// PAYLOAD_FIELD, LANGUAGE default) flow through EngineConfig below.
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			i++
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
	opts.MinPrefix = getFTConfigInt("MINPREFIX")
	opts.MaxExpansions = getFTConfigInt("MAXEXPANSIONS")
	dialect := getFTConfigInt("DEFAULT_DIALECT")
	if dialect == 0 {
		dialect = 1
	}
	opts.Dialect = dialect
	noContent := false
	withScores := false
	withPayloads := false
	withSortKeys := false
	withCursor := false
	cursorCount := 10
	type returnFieldSpec struct {
		source string
		name   string // reply key (AS alias or source)
	}
	returnFields := []returnFieldSpec{}
	returnSpecified := false
	dialectSpecified := false
	var inKeys map[string]struct{}
	var params map[string][]byte // PARAMS name -> raw value (often a vector blob)

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
			withCursor = true
			if i+1 < len(args) && strings.EqualFold(string(args[i+1]), "COUNT") {
				if i+2 >= len(args) {
					return protocol.MakeSyntaxErrReply()
				}
				n, err := strconv.Atoi(string(args[i+2]))
				if err != nil || n <= 0 {
					return protocol.MakeErrReply("ERR Invalid COUNT value")
				}
				cursorCount = n
				i += 2
			}
		case "DIALECT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			d, err := strconv.Atoi(string(args[i+1]))
			if err != nil || !validFTDialect(d) {
				return protocol.MakeErrReply("ERR Invalid DIALECT value")
			}
			dialect = d
			dialectSpecified = true
			opts.Dialect = d
			i++
		case "SCORER":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			opts.Scorer = strings.ToUpper(string(args[i+1]))
			i++
		case "PARAMS":
			// PARAMS count name value [name value ...] — count is the total
			// number of attribute tokens (2 × number of named parameters).
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil || count < 0 || count%2 != 0 {
				return protocol.MakeErrReply("ERR Invalid PARAMS count")
			}
			i += 2
			if params == nil {
				params = make(map[string][]byte, count/2)
			}
			for j := 0; j < count; j += 2 {
				if i+1 >= len(args) {
					return protocol.MakeSyntaxErrReply()
				}
				name := string(args[i])
				params[name] = args[i+1]
				i += 2
			}
			i--
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
		dialect = getFTConfigInt("DEFAULT_DIALECT")
		if dialect == 0 {
			dialect = 1
		}
		if !validFTDialect(dialect) {
			return protocol.MakeErrReply("ERR Invalid DIALECT value")
		}
		opts.Dialect = dialect
	}
	// PARAMS (and any $-parameter substitution) requires DIALECT >= 2. Deferred
	// to here so the DIALECT option may appear in any order relative to PARAMS.
	if len(params) > 0 && dialect < 2 {
		return protocol.MakeErrReply("ERR PARAMS requires DIALECT 2 or higher")
	}
	opts.Params = params
	if opts.TimeoutMs == 0 {
		if t := getFTConfigInt("TIMEOUT"); t > 0 {
			opts.TimeoutMs = t
		}
	}
	if max := getFTConfigInt("MAXSEARCHRESULTS"); max > 0 && opts.Limit > max {
		opts.Limit = max
	}
	if withCursor && opts.Limit < cursorCount*10 {
		// Pull enough hits to page; COUNT controls page size, not total.
		if max := getFTConfigInt("MAXSEARCHRESULTS"); max > 0 {
			opts.Limit = max
		} else if opts.Limit < 1000 {
			opts.Limit = 1000
		}
	}

	// Expand query terms into their FT.SYNADD synonyms for this index.
	engine.SetSynonymExpander(func(term string) []string {
		return getSynonyms(indexName, term)
	})

	// Detect a vector KNN clause: "<base>=>[KNN K @field $param ...]".
	baseQuery, knnClause, kerr := redisearch.SplitKNNClause(query)
	if kerr != nil {
		return protocol.MakeErrReply("ERR " + kerr.Error())
	}

	var results *redisearch.SearchResults
	var err error
	if knnClause != nil {
		if dialect < 2 {
			return protocol.MakeErrReply("ERR Vector KNN queries require DIALECT 2 or higher")
		}
		blob, ok := params[strings.TrimPrefix(knnClause.Param, "$")]
		if !ok {
			return protocol.MakeErrReply(fmt.Sprintf("ERR Parameter '%s' was not found in PARAMS", knnClause.Param))
		}
		vi := engine.VectorIndex(knnClause.Field)
		if vi == nil {
			return protocol.MakeErrReply(fmt.Sprintf("ERR Vector field '%s' not found in index", knnClause.Field))
		}
		queryVec, derr := vi.DecodeVector(blob)
		if derr != nil {
			return protocol.MakeErrReply("ERR " + derr.Error())
		}
		results, err = engine.SearchKNN(baseQuery, opts, knnClause, queryVec)
		if err != nil {
			return ftTimeoutReply(err)
		}
	} else {
		// Search
		results, err = engine.Search(query, opts)
		if err != nil {
			return ftTimeoutReply(err)
		}
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

	var cursorRows [][]byte
	for _, result := range results.Results {
		if withCursor {
			// Pack one document as a single multi-bulk row for cursor paging:
			// [id, score?, fields...] flattened; COUNT is in documents.
			rowArgs := [][]byte{[]byte(result.Document.ID)}
			if withScores {
				rowArgs = append(rowArgs, []byte(fmt.Sprintf("%.6f", result.Score)))
			}
			if !noContent {
				var fields [][]byte
				if returnSpecified {
					for _, rf := range returnFields {
						if val, ok := result.Fields[rf.source]; ok {
							fields = append(fields, []byte(rf.name))
							fields = append(fields, []byte(fmt.Sprintf("%v", val)))
						}
					}
				} else {
					for k, v := range result.Fields {
						fields = append(fields, []byte(k))
						fields = append(fields, []byte(fmt.Sprintf("%v", v)))
					}
				}
				rowArgs = append(rowArgs, protocol.MakeMultiBulkReply(fields).ToBytes())
			}
			cursorRows = append(cursorRows, protocol.MakeMultiBulkReply(rowArgs).ToBytes())
			continue
		}

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

	if withCursor {
		return ftBuildCursorPage(indexName, results.Total, cursorRows, cursorCount)
	}
	// Wrap in FTSearchReply so RESP3 connections get the Redis 8.x map shape;
	// RESP2 connections see the unchanged positional array via ToBytes().
	var attrNames []string
	if returnSpecified {
		attrNames = make([]string, 0, len(returnFields))
		for _, rf := range returnFields {
			attrNames = append(attrNames, rf.name)
		}
	}
	return MakeFTSearchReply(protocol.MakeMultiRawReply(replies), int64(results.Total), withScores, withPayloads, withSortKeys, noContent, attrNames)
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
	req.MinPrefix = getFTConfigInt("MINPREFIX")
	req.MaxExpansions = getFTConfigInt("MAXEXPANSIONS")
	req.Dialect = getFTConfigInt("DEFAULT_DIALECT")
	if req.Dialect == 0 {
		req.Dialect = 1
	}
	withCursor := false
	cursorCount := 10
	sawGroupBy := false

	for i := 2; i < len(args); {
		arg := strings.ToUpper(string(args[i]))

		switch arg {
		case "VERBATIM":
			req.Verbatim = true
			i++
			continue

		case "DIALECT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			d, err := strconv.Atoi(string(args[i+1]))
			if err != nil || !validFTDialect(d) {
				return protocol.MakeErrReply("ERR Invalid DIALECT value")
			}
			req.Dialect = d
			i += 2
			continue

		case "WITHCURSOR":
			withCursor = true
			i++
			if i < len(args) && strings.EqualFold(string(args[i]), "COUNT") {
				if i+1 >= len(args) {
					return protocol.MakeSyntaxErrReply()
				}
				n, err := strconv.Atoi(string(args[i+1]))
				if err != nil || n <= 0 {
					return protocol.MakeErrReply("ERR Invalid COUNT value")
				}
				cursorCount = n
				i += 2
			}
			continue

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
			if i+3 >= len(args) || !strings.EqualFold(string(args[i+2]), "AS") {
				return protocol.MakeSyntaxErrReply()
			}
			req.Apply = append(req.Apply, redisearch.ApplyClause{
				Expr:     string(args[i+1]),
				As:       string(args[i+3]),
				PreGroup: !sawGroupBy,
			})
			i += 4
			continue

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
			sawGroupBy = true
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
			if strings.EqualFold(funcName, "COLLECT") {
				// REDUCE COLLECT's own args embed FIELDS/SORTBY/LIMIT keywords;
				// consume exactly rargs of them so they aren't mistaken for the
				// next pipeline step. Only "AS" (the reducer alias) terminates.
				for j := 0; j < rargs && i < len(args); j++ {
					if strings.EqualFold(string(args[i]), "AS") {
						break
					}
					reducer.Args = append(reducer.Args, string(args[i]))
					i++
				}
			} else {
				for j := 0; j < rargs && i < len(args); j++ {
					nextArg := strings.ToUpper(string(args[i]))
					if nextArg == "AS" || nextArg == "REDUCE" || nextArg == "SORTBY" || nextArg == "LIMIT" {
						break
					}
					reducer.Args = append(reducer.Args, string(args[i]))
					i++
				}
			}
			// Field is the first arg (if any) with the leading @ stripped.
			if len(reducer.Args) > 0 {
				reducer.Field = strings.TrimPrefix(reducer.Args[0], "@")
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

	// FT.AGGREGATE results are capped by search-max-aggregate-results (8.0
	// name), NOT MAXSEARCHRESULTS which applies to FT.SEARCH only.
	if max := getFTConfigInt("MAXAGGREGATERESULTS"); max > 0 && req.Limit > max {
		req.Limit = max
	}

	// Execute aggregation
	result, err := engine.Aggregate(req)
	if err != nil {
		if err == redisearch.ErrTimeout && strings.EqualFold(getFTConfigString("ON_TIMEOUT"), "RETURN") {
			return protocol.MakeMultiBulkReply([][]byte{[]byte("0")})
		}
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}

	if withCursor {
		rows := make([][]byte, 0, len(result.Groups))
		for _, group := range result.Groups {
			rows = append(rows, aggRowBytes(group))
		}
		return ftBuildCursorPage(indexName, result.Total, rows, cursorCount)
	}

	// Build response
	var reply [][]byte
	reply = append(reply, []byte(strconv.Itoa(result.Total)))

	for _, group := range result.Groups {
		reply = append(reply, aggRowBytes(group))
	}

	// Wrap for dual-form RESP2/RESP3 output (RESP3 gets the 8.x map shape).
	return MakeFTAggregateReply(protocol.MakeMultiBulkReply(reply), int64(result.Total))
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
// becomes a nested array of k/v maps; everything else keeps %v formatting.
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
	default:
		return []byte(fmt.Sprintf("%v", v))
	}
}

// ftCursorEntry holds the not-yet-delivered page of an FT.AGGREGATE WITHCURSOR
// result, keyed by an opaque cursor id.
type ftCursorEntry struct {
	indexName  string
	total      int
	rows       [][]byte
	lastAccess time.Time
}

const ftCursorIdleTimeout = time.Minute

var (
	ftCursorMu      sync.Mutex
	ftCursorStore   = make(map[uint64]*ftCursorEntry)
	ftCursorCounter uint64
)

// ftSweepExpiredCursorsLocked drops cursors idle for longer than
// ftCursorIdleTimeout. Caller must hold ftCursorMu.
func ftSweepExpiredCursorsLocked() {
	if len(ftCursorStore) == 0 {
		return
	}
	now := time.Now()
	for id, entry := range ftCursorStore {
		if now.Sub(entry.lastAccess) > ftCursorIdleTimeout {
			delete(ftCursorStore, id)
		}
	}
}

// ftBuildCursorPage slices off up to count rows to return immediately,
// storing the remainder (if any) under a freshly minted cursor id. Returns
// the Redis reply shape `[[total, row, row, ...], cursorID]`, with cursorID
// 0 once the result set is exhausted.
func ftBuildCursorPage(indexName string, total int, rows [][]byte, count int) redis.Reply {
	if count <= 0 {
		count = 10
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
		return protocol.MakeErrReply("ERR Cursor not found")
	}

	switch sub {
	case "READ":
		count := 0
		if len(args) >= 5 && strings.EqualFold(string(args[3]), "COUNT") {
			n, err := strconv.Atoi(string(args[4]))
			if err != nil || n <= 0 {
				return protocol.MakeErrReply("ERR Invalid COUNT value")
			}
			count = n
		}

		ftCursorMu.Lock()
		entry, ok := ftCursorStore[cursorID]
		if ok {
			delete(ftCursorStore, cursorID)
		}
		ftCursorMu.Unlock()

		if !ok || entry.indexName != indexName {
			return protocol.MakeErrReply("ERR Cursor not found")
		}
		if count <= 0 {
			count = 10
		}
		return ftBuildCursorPage(indexName, entry.total, entry.rows, count)

	case "DEL":
		ftCursorMu.Lock()
		_, ok := ftCursorStore[cursorID]
		if ok {
			delete(ftCursorStore, cursorID)
		}
		ftCursorMu.Unlock()
		if !ok {
			return protocol.MakeErrReply("ERR Cursor not found")
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
	case "TEXT", "NUMERIC", "TAG", "GEO", "VECTOR", "GEOSHAPE":
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
	registerCommand("FT.Cursor", execFTCursor, prepareFTCursor, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 2, 2, 1)
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
