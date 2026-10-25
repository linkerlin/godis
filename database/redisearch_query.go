// FT.SEARCH and FT.AGGREGATE executors plus their reply/scoring helpers.
package database

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/linkerlin/godis/datastruct/redisearch"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

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
		return protocol.MakeErrReply(fmt.Sprintf("SEARCH_INDEX_NOT_FOUND Index not found: %s", indexName))
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
	withoutCount := false
	withScores := false
	withPayloads := false
	withSortKeys := false
	withCursor := false
	cursorCount := 10
	cursorMaxIdle := ftCursorIdleTimeout
	type returnFieldSpec struct {
		source string
		name   string // reply key (AS alias or source)
	}
	returnFields := []returnFieldSpec{}
	returnSpecified := false
	dialectSpecified := false
	var inKeys map[string]struct{}
	inKeysFilter := false // true once INKEYS seen (count 0 → empty result set)
	var params map[string][]byte // PARAMS name -> raw value (often a vector blob)
	formatExpand := false

	for i := 2; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))

		switch arg {
		case "NOCONTENT":
			noContent = true
		case "WITHOUTCOUNT":
			// Redis: leading total = docs returned in this reply (not full hit count).
			withoutCount = true
		case "WITHCOUNT":
			// Redis accepts bare WITHCOUNT (accurate total is already the default).
		case "WITHSCORES":
			withScores = true
			opts.WithScores = true
		case "EXPLAINSCORE":
			// Requires WITHSCORES (validated after the option loop).
			opts.ExplainScore = true
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
		case "FORMAT":
			// Redis 8.x: STRING (default wire), JSON unsupported, EXPAND needs
			// DIALECT≥3 + RESP3 + ON JSON (validated after dialect resolve).
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_VALUE_BAD Need an argument for FORMAT")
			}
			fmtOpt := strings.ToUpper(string(args[i+1]))
			i++
			switch fmtOpt {
			case "STRING":
				// default RESP shape
			case "JSON":
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS FORMAT JSON is not supported")
			case "EXPAND":
				formatExpand = true
			default:
				return protocol.MakeErrReply(fmt.Sprintf("SEARCH_PARSE_ARGS FORMAT %s is not supported", fmtOpt))
			}
		case "WITHCURSOR":
			withCursor = true
			for i+1 < len(args) {
				opt := strings.ToUpper(string(args[i+1]))
				if opt == "COUNT" {
					n, errMsg := parseFTCursorCountArg(args, i+2)
					if errMsg != "" {
						return protocol.MakeErrReply(errMsg)
					}
					cursorCount = n
					i += 2
					continue
				}
				if opt == "MAXIDLE" {
					if i+2 >= len(args) {
						return protocol.MakeErrReply("Bad arguments for MAXIDLE: Expected an argument, but none provided")
					}
					ms, err := strconv.ParseInt(string(args[i+2]), 10, 64)
					if err != nil {
						return protocol.MakeErrReply("Bad arguments for MAXIDLE: Could not convert argument to expected type")
					}
					if ms <= 0 {
						return protocol.MakeErrReply("Bad arguments for MAXIDLE: Value is outside acceptable bounds")
					}
					if ms > ftCursorMaxIdleCapMs {
						ms = ftCursorMaxIdleCapMs
					}
					cursorMaxIdle = time.Duration(ms) * time.Millisecond
					i += 2
					continue
				}
				break
			}
		case "DIALECT":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Need an argument for DIALECT")
			}
			d, err := strconv.Atoi(string(args[i+1]))
			if err != nil || !validFTDialect(d) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS DIALECT requires a non negative integer >=1 and <= 4")
			}
			dialect = d
			dialectSpecified = true
			opts.Dialect = d
			i++
		case "SCORER":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("Bad arguments for SCORER: Expected an argument, but none provided")
			}
			scorerName := string(args[i+1])
			if !redisearch.IsKnownScorer(scorerName) {
				return protocol.MakeErrReply("No such scorer " + scorerName)
			}
			opts.Scorer = scorerName
			i++
		case "LANGUAGE":
			// Query-time stemmer language (Redis 8.x). Godis accepts known names
			// but still uses the English tokenizer path (subset; not full ICU).
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for LANGUAGE: Expected an argument, but none provided")
			}
			if !redisearch.IsKnownSearchLanguage(string(args[i+1])) {
				return protocol.MakeErrReply("SEARCH_QUERY_BAD No such language")
			}
			i++
		case "PAYLOAD":
			// Query payload for SCORER HAMMING (Redis 8.x).
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for PAYLOAD: Expected an argument, but none provided")
			}
			opts.Payload = args[i+1]
			i++
		case "BM25STD_TANH_FACTOR":
			// Per-query divisor for SCORER BM25STD.TANH (default 4).
			// Redis 8.x: integer ≥1 (message cites 1..10000; values >10000 still accepted).
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Need an argument for BM25STD_TANH_FACTOR")
			}
			raw := string(args[i+1])
			f, err := strconv.ParseFloat(raw, 64)
			if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || f < 1 || math.Trunc(f) != f {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS BM25STD_TANH_FACTOR must be between 1 and 10000 inclusive")
			}
			opts.BM25STDTanhFactor = f
			i++
		case "PARAMS":
			// PARAMS count name value [name value ...] — count is the total
			// number of attribute tokens (2 × number of named parameters).
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for PARAMS: Expected an argument, but none provided")
			}
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for PARAMS: Could not convert argument to expected type")
			}
			if count < 0 {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for PARAMS: Value is outside acceptable bounds")
			}
			// Redis rejects 0 and odd counts alike (PARAM VALUE pairs).
			if count == 0 || count%2 != 0 {
				return protocol.MakeErrReply("SEARCH_ADD_ARGS Parameters must be specified in PARAM VALUE pairs")
			}
			i += 2
			if params == nil {
				params = make(map[string][]byte, count/2)
			}
			for j := 0; j < count; j += 2 {
				if i+1 >= len(args) {
					return protocol.MakeErrReply("SEARCH_ADD_ARGS Parameters must be specified in PARAM VALUE pairs")
				}
				name := string(args[i])
				params[name] = args[i+1]
				i += 2
			}
			i--
		case "SLOP":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for SLOP: Expected an argument, but none provided")
			}
			slop, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				// Redis 8.x: non-integer → convert error (negative SLOP is accepted).
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for SLOP: Could not convert argument to expected type")
			}
			opts.Slop = slop
			i++
		case "INORDER":
			opts.InOrder = true
		case "EXPANDER":
			// Redis 8.x: missing → PARSE_ARGS; unknown names accepted (STEM/custom no-op here).
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for EXPANDER: Expected an argument, but none provided")
			}
			i++
		case "TIMEOUT":
			if i+1 >= len(args) {
				// Redis 8.x: SEARCH_PARSE_ARGS Need argument for TIMEOUT
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Need argument for TIMEOUT")
			}
			ms, err := strconv.Atoi(string(args[i+1]))
			if err != nil || ms < 0 {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS TIMEOUT requires a non negative integer")
			}
			opts.TimeoutMs = ms
			i++ // engine Search/KNN honor TimeoutMs soft deadline
		case "INFIELDS":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for INFIELDS: Expected an argument, but none provided")
			}
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for INFIELDS: Could not convert argument to expected type")
			}
			if count < 0 {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for INFIELDS: Value is outside acceptable bounds")
			}
			i += 2
			for j := 0; j < count; j++ {
				if i >= len(args) {
					return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for INFIELDS: Expected an argument, but none provided")
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
						return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for SUMMARIZE")
					}
					n, err := strconv.Atoi(string(args[i+2]))
					if err != nil || n < 0 {
						return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for SUMMARIZE")
					}
					last := i + 2 + n
					if last >= len(args) {
						return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for SUMMARIZE")
					}
					for j := i + 3; j <= last; j++ {
						opts.SummarizeFields = append(opts.SummarizeFields, string(args[j]))
					}
					i = last
					continue
				}
				if next == "FRAGS" || next == "LEN" {
					if i+2 >= len(args) {
						return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for SUMMARIZE")
					}
					v, err := strconv.Atoi(string(args[i+2]))
					if err != nil || v < 0 {
						return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for SUMMARIZE")
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
						return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for SUMMARIZE")
					}
					i += 2 // accept separator; unused in minimal truncate
					continue
				}
				break
			}
		case "INKEYS":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for INKEYS: Expected an argument, but none provided")
			}
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for INKEYS: Could not convert argument to expected type")
			}
			if count < 0 {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for INKEYS: Value is outside acceptable bounds")
			}
			i += 2
			inKeysFilter = true
			inKeys = make(map[string]struct{}, count)
			for j := 0; j < count; j++ {
				if i >= len(args) {
					return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for INKEYS: Expected an argument, but none provided")
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
						return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for HIGHLIGHT")
					}
					n, err := strconv.Atoi(string(args[i+2]))
					if err != nil || n < 0 {
						return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for HIGHLIGHT")
					}
					last := i + 2 + n
					if last >= len(args) {
						return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for HIGHLIGHT")
					}
					for j := i + 3; j <= last; j++ {
						opts.HighlightFields = append(opts.HighlightFields, string(args[j]))
					}
					i = last
					continue
				}
				if next == "TAGS" {
					if i+3 >= len(args) {
						return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for HIGHLIGHT")
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
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS FILTER requires 3 arguments")
			}
			minRaw := string(args[i+2])
			maxRaw := string(args[i+3])
			minV, err1 := strconv.ParseFloat(minRaw, 64)
			if err1 != nil {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad lower range: " + minRaw)
			}
			maxV, err2 := strconv.ParseFloat(maxRaw, 64)
			if err2 != nil {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad upper range: " + maxRaw)
			}
			opts.Filters = append(opts.Filters, redisearch.FieldFilter{
				Field: string(args[i+1]),
				Min:   minV,
				Max:   maxV,
			})
			i += 3
		case "LIMIT":
			offset, limit, errMsg := parseFTLimitPair(args, i)
			if errMsg != "" {
				return protocol.MakeErrReply(errMsg)
			}
			opts.Offset = offset
			opts.Limit = limit
			i += 2
		case "SORTBY":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("Bad SORTBY arguments")
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
			// WITHCOUNT: ask for accurate total (Godis already returns full hit
			// count unless WITHOUTCOUNT). Accept for Redis 8.x syntax parity.
			if i+1 < len(args) && strings.EqualFold(string(args[i+1]), "WITHCOUNT") {
				i++
			}
		case "RETURN":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for RETURN: Expected an argument, but none provided")
			}
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for RETURN: Could not convert argument to expected type")
			}
			if count < 0 {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for RETURN: Value is outside acceptable bounds")
			}
			returnSpecified = true
			i += 2
			for j := 0; j < count; j++ {
				if i >= len(args) {
					return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for RETURN: Expected an argument, but none provided")
				}
				src := string(args[i])
				name := src
				i++
				if i < len(args) && strings.EqualFold(string(args[i]), "AS") {
					if i+1 >= len(args) {
						return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for RETURN: Expected an argument, but none provided")
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
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS GEOFILTER requires 5 arguments")
			}
			geoField := string(args[i+1])
			lon, err := strconv.ParseFloat(string(args[i+2]), 64)
			if err != nil {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for <lon>: Could not convert argument to expected type")
			}
			lat, err := strconv.ParseFloat(string(args[i+3]), 64)
			if err != nil {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for <lat>: Could not convert argument to expected type")
			}
			radius, err := strconv.ParseFloat(string(args[i+4]), 64)
			if err != nil {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for <radius>: Could not convert argument to expected type")
			}
			// Redis 8.x: lon ∈ [-180,180], lat ∈ [-90,90] (not mercator clamp).
			if lon < -180 || lon > 180 || lat < -90 || lat > 90 {
				return protocol.MakeErrReply("SEARCH_SYNTAX Invalid GeoFilter lat/lon")
			}
			unitRaw := string(args[i+5])
			unit := strings.ToLower(unitRaw)
			if unit != "m" && unit != "km" && unit != "mi" && unit != "ft" {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Unknown distance unit " + unitRaw)
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
			// Redis 8.x: position is 1-based among option tokens (args[0]=index, args[1]=query).
			return ftUnrecognizedArg(string(args[i]), i-1)
		}
	}

	if opts.ExplainScore && !withScores {
		// Redis 8.x: SEARCH_PARSE_ARGS prefix (no ERR).
		return protocol.MakeErrReply("SEARCH_PARSE_ARGS EXPLAINSCORE must be accompanied with WITHSCORES")
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
	if formatExpand {
		if dialect < 3 {
			return protocol.MakeErrReply("SEARCH_LIMIT_OVER EXPAND format requires dialect 3 or greater")
		}
		resp3 := false
		if id := peekBlockingClientID(); id != 0 {
			if c := FindClientByID(id); c != nil && c.GetProtocolVersion() == 3 {
				resp3 = true
			}
		}
		if !resp3 {
			return protocol.MakeErrReply("SEARCH_VALUE_BAD EXPAND format is only supported with RESP3")
		}
		searchIndexMetaMu.RLock()
		meta := searchIndexMeta[indexName]
		onJSON := meta != nil && meta.onType == "JSON"
		searchIndexMetaMu.RUnlock()
		if !onJSON {
			return protocol.MakeErrReply("SEARCH_VALUE_BAD EXPAND format is only supported with JSON")
		}
		// JSON+RESP3+DIALECT≥3: accept; reply shape remains dual-form FTSearchReply
		// (not a full Redis EXPAND JSON tree — subset).
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

	if opts.SortBy != "" {
		sf := engine.SchemaField(opts.SortBy)
		if sf == nil {
			return protocol.MakeErrReply(fmt.Sprintf("Property `%s` not loaded nor in schema", opts.SortBy))
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
		pname := strings.TrimPrefix(knnClause.Param, "$")
		blob, ok := params[pname]
		if !ok {
			// Match RediSearch wording; name without leading $.
			return protocol.MakeErrReply(fmt.Sprintf("ERR No such parameter '%s'", pname))
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
			if !(err == redisearch.ErrTimeout && results != nil &&
				strings.EqualFold(getFTConfigString("ON_TIMEOUT"), "RETURN")) {
				return ftTimeoutReply(err)
			}
		}
	} else {
		// VECTOR_RANGE attr block (Redis 8.10): $YIELD_DISTANCE_AS always;
		// $EPSILON only on HNSW/SVS (validated at search); reject $EF_RUNTIME,
		// hybrid-only attrs, and unknown keys (do not silently ignore).
		// Text-clause attributes ($weight/$slop/…) use the same "=> {…}" shape
		// and must NOT be consumed here — leave them for ExpressionParser.
		q := query
		if strings.Contains(strings.ToUpper(q), "VECTOR_RANGE") {
			base, attrs, aerr := redisearch.StripTrailingAttrBlock(q)
			if aerr != nil {
				return protocol.MakeErrReply("ERR " + aerr.Error())
			}
			if attrs != nil {
				q = base
				for k, val := range attrs {
					switch k {
					case "YIELD_DISTANCE_AS":
						if val == "" {
							return protocol.MakeErrReply("ERR YIELD_DISTANCE_AS requires a field name")
						}
						opts.VectorRangeYield = val
					case "EPSILON":
						if val == "" {
							return protocol.MakeErrReply("ERR EPSILON requires a value")
						}
						eps, perr := strconv.ParseFloat(val, 64)
						if perr != nil || eps < 0 {
							return protocol.MakeErrReply("ERR Invalid EPSILON")
						}
						opts.VectorRangeEpsilon = eps
						opts.VectorRangeEpsilonSet = true
					case "EF_RUNTIME":
						// Redis rejects $EF_RUNTIME on VECTOR_RANGE (KNN-only hint).
						return protocol.MakeErrReply("ERR Invalid option EF_RUNTIME for VECTOR_RANGE")
					case "HYBRID_POLICY", "BATCH_SIZE":
						if val == "" {
							return protocol.MakeErrReply("ERR " + k + " requires a value")
						}
						return protocol.MakeErrReply("ERR hybrid query attributes were sent for a non-hybrid query")
					default:
						return protocol.MakeErrReply("ERR Invalid attribute " + k)
					}
				}
			}
		}
		results, err = engine.Search(q, opts)
		if err != nil {
			if !(err == redisearch.ErrTimeout && results != nil &&
				strings.EqualFold(getFTConfigString("ON_TIMEOUT"), "RETURN")) {
				return ftTimeoutReply(err)
			}
		}
	}

	if inKeysFilter {
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
	// WITHOUTCOUNT: leading int is the number of docs in this reply (Redis 8.x).
	replyTotal := int64(results.Total)
	if withoutCount {
		replyTotal = int64(len(results.Results))
	}
	replies := make([]redis.Reply, 0, 1+3*len(results.Results))
	replies = append(replies, protocol.MakeIntReply(replyTotal))

	// RETURN_STRICT (8.10): stop building the reply once the deadline passes.
	strictDeadline := ftStrictDeadline(opts.TimeoutMs)

	var cursorRows [][]byte
	for idx, result := range results.Results {
		if idx%256 == 255 && ftStrictCutoff(strictDeadline) {
			break
		}
		if withCursor {
			// Pack one document as a single multi-bulk row for cursor paging:
			// [id, score?, fields...] flattened; COUNT is in documents.
			rowArgs := [][]byte{[]byte(result.Document.ID)}
			if withScores {
				rowArgs = append(rowArgs, []byte(formatFTScore(result.Score)))
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
			scoreBulk := protocol.MakeBulkReply([]byte(formatFTScore(result.Score)))
			if opts.ExplainScore && result.ScoreExplain != nil {
				// Redis: score slot becomes [score, explain] when EXPLAINSCORE.
				replies = append(replies, protocol.MakeMultiRawReply([]redis.Reply{
					scoreBulk,
					encodeScoreExplain(result.ScoreExplain),
				}))
			} else {
				replies = append(replies, scoreBulk)
			}
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
			// Redis WITHSORTKEYS: numeric fields prefixed '#', text '$'; missing → empty.
			if sk != "" {
				prefix := "$"
				if sf := engine.SchemaField(opts.SortBy); sf != nil && sf.Type == redisearch.FieldTypeNumeric {
					prefix = "#"
				}
				sk = prefix + sk
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
		return ftBuildCursorPage(indexName, results.Total, cursorRows, cursorCount, cursorMaxIdle)
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
	return MakeFTSearchReply(protocol.MakeMultiRawReply(replies), replyTotal, withScores, withPayloads, withSortKeys, noContent, attrNames).
		setExplainScore(opts.ExplainScore)
}

// encodeScoreExplain maps a ScoreExplanation to Redis 8.x nested RESP
// (simple-string leaves). BM25 single-term vs multi-term shapes differ.
func encodeScoreExplain(ex *redisearch.ScoreExplanation) redis.Reply {
	if ex == nil {
		return protocol.MakeStatusReply("")
	}
	if ex.Leaf != "" {
		return protocol.MakeStatusReply(ex.Leaf)
	}
	header := protocol.MakeStatusReply(ex.Header)
	termReplies := make([]redis.Reply, len(ex.TermLines))
	for i, line := range ex.TermLines {
		termReplies[i] = protocol.MakeStatusReply(line)
	}
	terms := protocol.MakeMultiRawReply(termReplies)
	if ex.WeightLine != "" {
		// Multi-term: [header, [[weightLine, [terms...]]]]
		return protocol.MakeMultiRawReply([]redis.Reply{
			header,
			protocol.MakeMultiRawReply([]redis.Reply{
				protocol.MakeMultiRawReply([]redis.Reply{
					protocol.MakeStatusReply(ex.WeightLine),
					terms,
				}),
			}),
		})
	}
	// Single-term: [header, [termLine...]]
	return protocol.MakeMultiRawReply([]redis.Reply{header, terms})
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
		return protocol.MakeErrReply(fmt.Sprintf("SEARCH_INDEX_NOT_FOUND Index not found: %s", indexName))
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
	cursorMaxIdle := ftCursorIdleTimeout
	sawGroupBy := false
	sawApply := false

	for i := 2; i < len(args); {
		arg := strings.ToUpper(string(args[i]))

		switch arg {
		case "VERBATIM":
			req.Verbatim = true
			i++
			continue

		case "ADDSCORES":
			// Expose full-text score as @__score for SORTBY/APPLY/FILTER.
			req.AddScores = true
			i++
			continue

		case "FORMAT":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_VALUE_BAD Need an argument for FORMAT")
			}
			fmtOpt := strings.ToUpper(string(args[i+1]))
			switch fmtOpt {
			case "STRING":
				// default wire
			case "JSON":
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS FORMAT JSON is not supported")
			default:
				return protocol.MakeErrReply(fmt.Sprintf("SEARCH_PARSE_ARGS FORMAT %s is not supported", fmtOpt))
			}
			i += 2
			continue

		case "DIALECT":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Need an argument for DIALECT")
			}
			d, err := strconv.Atoi(string(args[i+1]))
			if err != nil || !validFTDialect(d) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS DIALECT requires a non negative integer >=1 and <= 4")
			}
			req.Dialect = d
			i += 2
			continue

		case "WITHCURSOR":
			withCursor = true
			i++
			for i < len(args) {
				opt := strings.ToUpper(string(args[i]))
				if opt != "COUNT" && opt != "MAXIDLE" {
					break
				}
				if opt == "COUNT" {
					n, errMsg := parseFTCursorCountArg(args, i+1)
					if errMsg != "" {
						return protocol.MakeErrReply(errMsg)
					}
					cursorCount = n
					i += 2
					continue
				}
				// MAXIDLE
				if i+1 >= len(args) {
					return protocol.MakeErrReply("Bad arguments for MAXIDLE: Expected an argument, but none provided")
				}
				ms, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("Bad arguments for MAXIDLE: Could not convert argument to expected type")
				}
				if ms <= 0 {
					return protocol.MakeErrReply("Bad arguments for MAXIDLE: Value is outside acceptable bounds")
				}
				if ms > ftCursorMaxIdleCapMs {
					ms = ftCursorMaxIdleCapMs
				}
				cursorMaxIdle = time.Duration(ms) * time.Millisecond
				i += 2
			}
			continue

		case "TIMEOUT":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Need argument for TIMEOUT")
			}
			ms, err := strconv.Atoi(string(args[i+1]))
			if err != nil || ms < 0 {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS TIMEOUT requires a non negative integer")
			}
			req.TimeoutMs = ms
			i += 2
			continue

		case "APPLY":
			// Redis 8.x: APPLY expr [AS name]; bare APPLY → PARSE_ARGS; AS without name → AS needs argument;
			// no AS → alias defaults to the expression text.
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for APPLY/FILTER: Expected an argument, but none provided")
			}
			expr := string(args[i+1])
			i += 2
			asName := expr
			if i < len(args) && strings.EqualFold(string(args[i]), "AS") {
				if i+1 >= len(args) {
					return protocol.MakeErrReply("SEARCH_PARSE_ARGS AS needs argument")
				}
				asName = string(args[i+1])
				i += 2
			}
			sawApply = true
			ac := &redisearch.ApplyClause{
				Expr:     expr,
				As:       asName,
				PreGroup: !sawGroupBy,
			}
			req.Apply = append(req.Apply, *ac)
			if !sawGroupBy {
				req.PreSteps = append(req.PreSteps, redisearch.AggStep{Apply: ac})
			} else {
				req.PostSteps = append(req.PostSteps, redisearch.AggStep{Apply: ac})
			}
			continue

		case "LOAD":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for LOAD: Expected an argument, but none provided")
			}
			if string(args[i+1]) == "*" {
				req.LoadAll = true
				i += 2
				continue
			}
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for LOAD: Expected number of fields or `*`")
			}
			if count < 0 {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for LOAD: Value is outside acceptable bounds")
			}
			i += 2
			consumed := 0
			for consumed < count {
				if i >= len(args) {
					return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for LOAD: Expected an argument, but none provided")
				}
				field := strings.TrimPrefix(string(args[i]), "@")
				i++
				consumed++
				as := ""
				if consumed < count && i < len(args) && strings.EqualFold(string(args[i]), "AS") {
					i++
					consumed++
					if consumed >= count || i >= len(args) {
						return protocol.MakeErrReply("LOAD path AS name - must be accompanied with NAME")
					}
					as = string(args[i])
					i++
					consumed++
				}
				req.Load = append(req.Load, redisearch.LoadSpec{Field: field, As: as})
			}
			continue

		case "GROUPBY":
			sawGroupBy = true
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for GROUPBY: Expected an argument, but none provided")
			}
			nargs, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for GROUPBY: Could not convert argument to expected type")
			}
			// Redis 8.x: negative nargs → same "Expected an argument" wording.
			if nargs < 0 {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for GROUPBY: Expected an argument, but none provided")
			}
			i += 2

			// Get properties (support multiple fields); require exact nargs.
			for j := 0; j < nargs; j++ {
				if i >= len(args) {
					return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for GROUPBY: Expected an argument, but none provided")
				}
				nextArg := strings.ToUpper(string(args[i]))
				if nextArg == "REDUCE" || nextArg == "HAVING" {
					return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for GROUPBY: Expected an argument, but none provided")
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
			if errMsg := validateFTReducer(funcName, reducer); errMsg != "" {
				return protocol.MakeErrReply(errMsg)
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
			// SORTBY nargs property [ASC|DESC] [property ASC|DESC ...] [MAX num]
			// Consume exactly nargs tokens (Redis); all properties drive sort.
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			nargs, err := strconv.Atoi(string(args[i+1]))
			if err != nil || nargs < 0 {
				return protocol.MakeErrReply("ERR Invalid sortby nargs")
			}
			i += 2
			end := i + nargs
			if end > len(args) {
				return protocol.MakeErrReply("Bad arguments for SORTBY: Expected an argument, but none provided")
			}
			var keys []redisearch.SortKey
			for i < end {
				field := string(args[i])
				i++
				desc := false
				if i < end {
					dir := strings.ToUpper(string(args[i]))
					if dir == "ASC" {
						i++
					} else if dir == "DESC" {
						desc = true
						i++
					}
				}
				keys = append(keys, redisearch.SortKey{Field: field, Desc: desc})
			}
			req.SortKeys = keys
			if i < len(args) && strings.EqualFold(string(args[i]), "MAX") {
				if i+1 >= len(args) {
					return protocol.MakeErrReply("Bad arguments for MAX: Could not convert argument to expected type")
				}
				maxN, err := strconv.Atoi(string(args[i+1]))
				if err != nil || maxN < 0 {
					return protocol.MakeErrReply("Bad arguments for MAX: Could not convert argument to expected type")
				}
				req.SortMax = maxN
				i += 2
			}
			continue

		case "LIMIT":
			offset, limit, errMsg := parseFTLimitPair(args, i)
			if errMsg != "" {
				return protocol.MakeErrReply(errMsg)
			}
			req.Offset = offset
			req.Limit = limit
			i += 3
			continue

		case "FILTER":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("SEARCH_PARSE_ARGS Bad arguments for APPLY/FILTER: Expected an argument, but none provided")
			}
			expr := string(args[i+1])
			// Keep legacy single-Filter fields for synthesizeSteps / older tests.
			req.Filter = expr
			if !sawApply {
				req.FilterBeforeApply = true
			}
			if !sawGroupBy {
				req.PreSteps = append(req.PreSteps, redisearch.AggStep{Filter: expr})
			} else {
				req.PostSteps = append(req.PostSteps, redisearch.AggStep{Filter: expr})
			}
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
			// Redis: REDUCE outside GROUPBY and other unknowns → SEARCH_ARG_UNRECOGNIZED.
			return ftUnrecognizedArg(string(args[i]), i-1)
		}
	}

	// FILTER-before-APPLY legacy flag only matters when steps were not built
	// (unit tests). Parser always fills PreSteps/PostSteps for FILTER/APPLY.
	if len(req.Apply) == 0 {
		req.FilterBeforeApply = false
	}

	// FT.AGGREGATE results are capped by search-max-aggregate-results (8.0
	// name), NOT MAXSEARCHRESULTS which applies to FT.SEARCH only.
	if max := getFTConfigInt("MAXAGGREGATERESULTS"); max > 0 && req.Limit > max {
		req.Limit = max
	}

	// Execute aggregation
	result, err := engine.Aggregate(req)
	if err != nil {
		if err == redisearch.ErrTimeout && result != nil &&
			strings.EqualFold(getFTConfigString("ON_TIMEOUT"), "RETURN") {
			// Soft RETURN: keep pipeline output built from docs fetched so far.
		} else if err == redisearch.ErrTimeout && strings.EqualFold(getFTConfigString("ON_TIMEOUT"), "RETURN") {
			return protocol.MakeMultiBulkReply([][]byte{[]byte("0")})
		} else if strings.HasPrefix(err.Error(), "SEARCH_") {
			return protocol.MakeErrReply(err.Error())
		} else {
			return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
		}
	}

	if withCursor {
		rows := make([][]byte, 0, len(result.Groups))
		strictDeadline := ftStrictDeadline(req.TimeoutMs)
		for idx, group := range result.Groups {
			if idx%256 == 255 && ftStrictCutoff(strictDeadline) {
				break
			}
			rows = append(rows, aggRowBytes(group))
		}
		return ftBuildCursorPage(indexName, result.Total, rows, cursorCount, cursorMaxIdle)
	}

	// Build response
	var reply [][]byte
	reply = append(reply, []byte(strconv.Itoa(result.Total)))

	strictDeadline := ftStrictDeadline(req.TimeoutMs)
	for idx, group := range result.Groups {
		if idx%256 == 255 && ftStrictCutoff(strictDeadline) {
			break
		}
		reply = append(reply, aggRowBytes(group))
	}

	// Wrap for dual-form RESP2/RESP3 output (RESP3 gets the 8.x map shape).
	return MakeFTAggregateReply(protocol.MakeMultiBulkReply(reply), int64(result.Total))
}

// parseFTCursorCountArg parses WITHCURSOR COUNT n (Redis 8.x Bad arguments for COUNT:…).
// idx is the index of the COUNT value token (may be past len(args)).
// formatFTScore renders scores at full shortest-roundtrip precision like
// real RediSearch (probed: 0.49917626886198924, not %.6f).
func formatFTScore(score float64) string {
	return strconv.FormatFloat(score, 'g', -1, 64)
}
