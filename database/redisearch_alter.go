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

// execFTExplain FT.EXPLAIN index query
func execFTExplain(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.explain' command")
	}
	indexName := resolveSearchIndex(string(args[0]))
	query := string(args[1])

	searchEnginesMu.RLock()
	_, ok := searchEngines[indexName]
	searchEnginesMu.RUnlock()
	if !ok {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Index '%s' does not exist", string(args[0])))
	}

	plan := "INTERSECT {\n  " + query + "\n}\n"
	return protocol.MakeBulkReply([]byte(plan))
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
			case "NOINDEX":
				field.NoIndex = true
				i++
			case "NOSTEM":
				field.Stemming = false
				i++
			case "SEPARATOR":
				if i+1 >= len(args) {
					return nil, protocol.MakeSyntaxErrReply()
				}
				field.Separator = string(args[i+1])
				i += 2
			case "WEIGHT":
				if i+1 >= len(args) {
					return nil, protocol.MakeSyntaxErrReply()
				}
				w, err := strconv.ParseFloat(string(args[i+1]), 64)
				if err != nil {
					return nil, protocol.MakeErrReply("ERR Invalid weight")
				}
				field.Weight = w
				i += 2
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
	registerCommand("FT.Explain", execFTExplain, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("FT.ExplainCli", execFTExplainCLI, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
