//go:build !sqlite_backend

package database

import (
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

const errSQLiteVectorDisabled = "ERR sqlite vector backend requires build tag sqlite_backend"

func sqliteVSAdd(_ *DB, _ [][]byte) redis.Reply {
	return protocol.MakeErrReply(errSQLiteVectorDisabled)
}

func sqliteVSSearch(_ *DB, _ [][]byte) redis.Reply {
	return protocol.MakeErrReply(errSQLiteVectorDisabled)
}

func sqliteVSDropIndex(_ *DB, _ [][]byte) redis.Reply {
	return protocol.MakeErrReply(errSQLiteVectorDisabled)
}
