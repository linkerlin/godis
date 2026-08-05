package database

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/linkerlin/godis/datastruct/redisearch"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// execFTAlter FT.ALTER index SCHEMA ADD field type [options ...]
func execFTAlter(db *DB, args [][]byte) redis.Reply {
	if len(args) < 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.alter' command")
	}
	indexName := resolveSearchIndex(string(args[0]))
	if !strings.EqualFold(string(args[1]), "SCHEMA") || !strings.EqualFold(string(args[2]), "ADD") {
		return protocol.MakeSyntaxErrReply()
	}

	searchEnginesMu.RLock()
	engine, ok := searchEngines[indexName]
	searchEnginesMu.RUnlock()
	if !ok || engine == nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Index '%s' does not exist", string(args[0])))
	}

	fields, errReply := parseFTSchemaFields(args[3:])
	if errReply != nil {
		return errReply
	}
	if len(fields) == 0 {
		return protocol.MakeSyntaxErrReply()
	}

	if err := engine.AlterAddFields(fields); err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}

	searchIndexMetaMu.Lock()
	if meta := searchIndexMeta[indexName]; meta != nil {
		meta.schema = append(meta.schema, fields...)
	}
	searchIndexMetaMu.Unlock()

	db.addAof(utils.ToCmdLine3("ft.alter", args...))
	return protocol.MakeOkReply()
}

// execFTExplain FT.EXPLAIN index query [DIALECT n]
func execFTExplain(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.explain' command")
	}
	indexName := resolveSearchIndex(string(args[0]))
	query := string(args[1])
	for i := 2; i < len(args); i++ {
		if strings.EqualFold(string(args[i]), "DIALECT") {
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			if d, err := strconv.Atoi(string(args[i+1])); err != nil || !validFTDialect(d) {
				return protocol.MakeErrReply("ERR Invalid DIALECT value")
			}
			i++
			continue
		}
		return protocol.MakeSyntaxErrReply()
	}

	searchEnginesMu.RLock()
	_, ok := searchEngines[indexName]
	searchEnginesMu.RUnlock()
	if !ok {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Index '%s' does not exist", string(args[0])))
	}

	return protocol.MakeBulkReply([]byte(formatFTExplainPlan(query)))
}

// formatFTExplainPlan builds a shallow RediSearch-like explain tree.
func formatFTExplainPlan(query string) string {
	q := strings.TrimSpace(query)
	if q == "" {
		return "INTERSECT {\n}\n"
	}
	if strings.Contains(q, "|") {
		parts := strings.Split(q, "|")
		var b strings.Builder
		b.WriteString("UNION {\n")
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			b.WriteString("  INTERSECT {\n    ")
			b.WriteString(p)
			b.WriteString("\n  }\n")
		}
		b.WriteString("}\n")
		return b.String()
	}
	if strings.HasPrefix(q, "@") {
		return "INTERSECT {\n  " + q + "\n}\n"
	}
	terms := strings.Fields(q)
	if len(terms) > 1 && !strings.Contains(q, "\"") {
		var b strings.Builder
		b.WriteString("INTERSECT {\n")
		for _, t := range terms {
			b.WriteString("  ")
			b.WriteString(t)
			b.WriteByte('\n')
		}
		b.WriteString("}\n")
		return b.String()
	}
	return "INTERSECT {\n  " + q + "\n}\n"
}

// execFTExplainCLI FT.EXPLAINCLI index query — returns plan lines as array.
func execFTExplainCLI(db *DB, args [][]byte) redis.Reply {
	r := execFTExplain(db, args)
	if protocol.IsErrorReply(r) {
		return r
	}
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		return r
	}
	lines := strings.Split(strings.TrimRight(string(bulk.Arg), "\n"), "\n")
	out := make([][]byte, len(lines))
	for i, line := range lines {
		out[i] = []byte(line)
	}
	return protocol.MakeMultiBulkReply(out)
}

func parseFTSchemaFields(args [][]byte) ([]*redisearch.Field, redis.Reply) {
	var fields []*redisearch.Field
	i := 0
	for i < len(args) {
		if reply := validateBulkBytes(args[i]); reply != nil {
			return nil, reply
		}
		ident := string(args[i])
		i++

		jsonPath := ""
		if i < len(args) && strings.EqualFold(string(args[i]), "AS") {
			if i+1 >= len(args) {
				return nil, protocol.MakeSyntaxErrReply()
			}
			if reply := validateBulkBytes(args[i+1]); reply != nil {
				return nil, reply
			}
			jsonPath = ident
			ident = string(args[i+1])
			i += 2
		}

		if i >= len(args) {
			return nil, protocol.MakeErrReply(fmt.Sprintf("ERR No type specified for field '%s'", ident))
		}

		fieldType := strings.ToUpper(string(args[i]))
		field := &redisearch.Field{
			Name:     ident,
			Path:     jsonPath,
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
			// VECTOR is followed by: ALGO count [attr val ...]. Parse the whole
			// sub-block now; the generic option loop below must not re-enter it.
			cfg, consumed, err := redisearch.ParseVectorFieldConfig(args[i+1:])
			if err != nil {
				return nil, protocol.MakeErrReply("ERR " + err.Error())
			}
			field.VectorConfig = cfg
			i += consumed
		case "GEOSHAPE":
			field.Type = redisearch.FieldTypeGeoShape
			field.CoordinateSystem = "SPHERICAL" // Redis default
		default:
			return nil, protocol.MakeErrReply(fmt.Sprintf("ERR Unknown field type '%s'", fieldType))
		}
		i++
		for i < len(args) {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "SORTABLE":
				field.Sortable = true
				i++
				// SORTABLE may be followed by UNF (unnormalized sort column).
				// VECTOR / GEOSHAPE do not allow SORTABLE; NUMERIC/GEO ignore UNF
				// (UNF only meaningful for TEXT/TAG hash-stored values).
				if i < len(args) && strings.EqualFold(string(args[i]), "UNF") {
					if field.Type == redisearch.FieldTypeVector || field.Type == redisearch.FieldTypeGeoShape {
						return nil, protocol.MakeErrReply("ERR SORTABLE is not supported for VECTOR or GEOSHAPE fields")
					}
					field.SortableUNF = true
					i++
				}
			case "UNF":
				// Bare UNF without preceding SORTABLE is a syntax error.
				return nil, protocol.MakeSyntaxErrReply()
			case "NOINDEX":
				field.NoIndex = true
				i++
			case "NOSTEM":
				if field.Type != redisearch.FieldTypeText {
					return nil, protocol.MakeErrReply("ERR NOSTEM is supported only for TEXT fields")
				}
				field.Stemming = false
				i++
			case "SEPARATOR":
				if field.Type != redisearch.FieldTypeTag {
					return nil, protocol.MakeErrReply("ERR SEPARATOR is supported only for TAG fields")
				}
				if i+1 >= len(args) {
					return nil, protocol.MakeSyntaxErrReply()
				}
				field.Separator = string(args[i+1])
				i += 2
			case "CASESENSITIVE":
				if field.Type != redisearch.FieldTypeTag {
					return nil, protocol.MakeErrReply("ERR CASESENSITIVE is supported only for TAG fields")
				}
				field.CaseSensitive = true
				i++
			case "WEIGHT":
				if field.Type != redisearch.FieldTypeText {
					return nil, protocol.MakeErrReply("ERR WEIGHT is supported only for TEXT fields")
				}
				if i+1 >= len(args) {
					return nil, protocol.MakeSyntaxErrReply()
				}
				w, err := strconv.ParseFloat(string(args[i+1]), 64)
				if err != nil {
					return nil, protocol.MakeErrReply("ERR Invalid weight")
				}
				field.Weight = w
				i += 2
			case "PHONETIC":
				if field.Type != redisearch.FieldTypeText {
					return nil, protocol.MakeErrReply("ERR PHONETIC is supported only for TEXT fields")
				}
				if i+1 >= len(args) {
					return nil, protocol.MakeSyntaxErrReply()
				}
				matcher := strings.ToLower(string(args[i+1]))
				switch matcher {
				case "dm:en", "dm:fr", "dm:pt", "dm:es":
					field.Phonetic = matcher
				default:
					return nil, protocol.MakeErrReply(fmt.Sprintf("ERR Invalid PHONETIC matcher '%s'", string(args[i+1])))
				}
				i += 2
			case "INDEXMISSING":
				field.IndexMissing = true
				i++
			case "INDEXEMPTY":
				if field.Type != redisearch.FieldTypeText && field.Type != redisearch.FieldTypeTag {
					return nil, protocol.MakeErrReply("ERR INDEXEMPTY is supported only for TEXT and TAG fields")
				}
				field.IndexEmpty = true
				i++
			case "WITHSUFFIXTRIE":
				if field.Type != redisearch.FieldTypeText && field.Type != redisearch.FieldTypeTag {
					return nil, protocol.MakeErrReply("ERR WITHSUFFIXTRIE is supported only for TEXT and TAG fields")
				}
				field.WithSuffixTrie = true
				i++
			case "FLAT":
				if field.Type != redisearch.FieldTypeGeoShape {
					return nil, protocol.MakeErrReply("ERR FLAT is supported only for GEOSHAPE fields")
				}
				field.CoordinateSystem = "FLAT"
				i++
			case "SPHERICAL":
				if field.Type != redisearch.FieldTypeGeoShape {
					return nil, protocol.MakeErrReply("ERR SPHERICAL is supported only for GEOSHAPE fields")
				}
				field.CoordinateSystem = "SPHERICAL"
				i++
			default:
				if i+1 < len(args) && isFTFieldType(string(args[i+1])) {
					goto next
				}
				if strings.EqualFold(string(args[i]), "AS") {
					return nil, protocol.MakeSyntaxErrReply()
				}
				// Unknown token before next field — treat as next field name
				goto next
			}
		}
	next:
		fields = append(fields, field)
	}
	return fields, nil
}

func init() {
	registerCommand("FT.Alter", execFTAlter, writeFirstKey, nil, -5, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("FT.Explain", execFTExplain, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("FT.ExplainCli", execFTExplainCLI, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
