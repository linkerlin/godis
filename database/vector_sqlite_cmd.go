//go:build sqlite_backend

package database

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/linkerlin/godis/datastruct/vector"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

func sqliteVSAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'vs.add' command")
	}

	setKey := string(args[0])
	itemID := string(args[1])
	vec, err := parseVectorString(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid vector format: %v", err))
	}

	metadata := parseVSMetadata(args[3:])
	sqlDB, err := getSQLiteIndexDB()
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR sqlite index: %v", err))
	}

	isNew, err := sqliteVSAddVector(sqlDB, setKey, itemID, vec.ToFloat64(), metadata)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}

	db.addAof(utils.ToCmdLine3("vs.add", args...))
	if isNew {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

func sqliteVSSearch(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'vs.search' command")
	}

	setKey := string(args[0])
	k := 10
	var queryVec *vector.Vector

	for i := 1; i < len(args); {
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
			metric := strings.ToUpper(string(args[i+1]))
			if metric != "COSINE" && metric != "EUCLIDEAN" && metric != "DOT" {
				return protocol.MakeErrReply("ERR sqlite vector backend currently supports COSINE distance only")
			}
			i += 2
		default:
			var parseErr error
			queryVec, parseErr = parseVectorString(string(args[i]))
			if parseErr != nil {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid vector format: %v", parseErr))
			}
			i++
		}
	}
	if queryVec == nil {
		return protocol.MakeErrReply("ERR Missing query vector")
	}

	sqlDB, err := getSQLiteIndexDB()
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR sqlite index: %v", err))
	}
	hits, err := sqliteVSSearchVectors(sqlDB, setKey, queryVec.ToFloat64(), k)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}

	results := make([]*vector.SearchResult, 0, len(hits))
	for _, hit := range hits {
		results = append(results, &vector.SearchResult{
			ID:     hit.id,
			Vector: vector.NewVectorFromFloat64(hit.values),
			Score:  float32(hit.score),
		})
	}
	return formatSearchResults(results)
}

func parseVSMetadata(args [][]byte) map[string]string {
	metadata := make(map[string]string)
	for i := 0; i < len(args); {
		if strings.ToUpper(string(args[i])) == "METADATA" && i+2 < len(args) {
			i++
			for i+1 < len(args) {
				if strings.ToUpper(string(args[i])) == "METADATA" {
					break
				}
				metadata[string(args[i])] = string(args[i+1])
				i += 2
			}
		} else {
			i++
		}
	}
	return metadata
}

func sqliteVSDropIndex(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'vs.dropindex' command")
	}
	setKey := string(args[0])
	sqlDB, err := getSQLiteIndexDB()
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR sqlite index: %v", err))
	}
	deleted, err := sqliteVSDropIndexSet(sqlDB, setKey)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}
	// Also remove in-memory placeholder key if present.
	if entity, exists := db.GetEntity(setKey); exists {
		if _, ok := entity.Data.(*vector.VectorSet); ok {
			db.Remove(setKey)
		}
	}
	if deleted {
		db.addAof(utils.ToCmdLine3("vs.dropindex", args...))
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}
