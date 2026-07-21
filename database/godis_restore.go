package database

import (
	"github.com/linkerlin/godis/aof"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// execGodisRestore restores a Godis opaque payload (stream/json/vector/timeseries).
// GODIS.RESTORE key payload
func execGodisRestore(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'godis.restore' command")
	}
	key := string(args[0])
	entity, ok := aof.DecodeOpaque(args[1])
	if !ok {
		return protocol.MakeErrReply("ERR invalid godis opaque payload")
	}
	db.PutEntity(key, entity)
	db.addAof(utils.ToCmdLine3("godis.restore", args...))
	return protocol.MakeOkReply()
}

func init() {
	registerCommand("Godis.Restore", execGodisRestore, writeFirstKey, rollbackFirstKey, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
}
