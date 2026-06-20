//go:build sqlite_backend

package database

import (
	"fmt"
	"strconv"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

func sqliteFTCreate(db *DB, args [][]byte) redis.Reply {
	indexName, fields, errMsg := parseSQLiteTextSchema(args)
	if errMsg != "" {
		return protocol.MakeErrReply(errMsg)
	}
	if reply := validateBulkBytes(args[0]); reply != nil {
		return reply
	}

	sqlDB, err := getSQLiteIndexDB()
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR sqlite index: %v", err))
	}
	if err := sqliteFTCreateIndex(sqlDB, indexName, fields); err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}

	db.addAof(utils.ToCmdLine3("ft.create", args...))
	return protocol.MakeOkReply()
}

func sqliteFTAdd(db *DB, args [][]byte) redis.Reply {
	indexName, docID, fields, errMsg := parseSQLiteFTAddFields(args)
	if errMsg != "" {
		return protocol.MakeErrReply(errMsg)
	}
	if reply := validateBulkBytes(args[0]); reply != nil {
		return reply
	}
	if reply := validateBulkBytes(args[1]); reply != nil {
		return reply
	}

	sqlDB, err := getSQLiteIndexDB()
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR sqlite index: %v", err))
	}
	if err := sqliteFTAddDocument(sqlDB, indexName, docID, fields); err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}

	db.addAof(utils.ToCmdLine3("ft.add", args...))
	return protocol.MakeOkReply()
}

func sqliteFTSearch(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.search' command")
	}
	indexName := string(args[0])
	query := string(args[1])
	limit, offset, withScores, noContent := parseSQLiteFTSearchOptions(args)

	sqlDB, err := getSQLiteIndexDB()
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR sqlite index: %v", err))
	}
	hits, total, err := sqliteFTSearchDocs(sqlDB, indexName, query, limit, offset)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}

	var reply [][]byte
	reply = append(reply, []byte(strconv.Itoa(total)))
	for _, hit := range hits {
		reply = append(reply, []byte(hit.docID))
		if withScores {
			reply = append(reply, []byte(fmt.Sprintf("%.6f", hit.score)))
		}
		if !noContent {
			var fields [][]byte
			for k, v := range hit.fields {
				fields = append(fields, []byte(k), []byte(v))
			}
			reply = append(reply, protocol.MakeMultiBulkReply(fields).ToBytes())
		}
	}
	return protocol.MakeMultiBulkReply(reply)
}
