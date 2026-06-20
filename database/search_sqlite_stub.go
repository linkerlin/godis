//go:build !sqlite_backend

package database

import (
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

const errSQLiteSearchDisabled = "ERR sqlite search backend requires build tag sqlite_backend"

func sqliteFTCreate(_ *DB, _ [][]byte) redis.Reply {
	return protocol.MakeErrReply(errSQLiteSearchDisabled)
}

func sqliteFTAdd(_ *DB, _ [][]byte) redis.Reply {
	return protocol.MakeErrReply(errSQLiteSearchDisabled)
}

func sqliteFTSearch(_ *DB, _ [][]byte) redis.Reply {
	return protocol.MakeErrReply(errSQLiteSearchDisabled)
}
