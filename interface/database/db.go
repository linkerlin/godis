package database

import (
	"time"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/rdb/core"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// DB is the interface for redis style storage engine
type DB interface {
	Exec(client redis.Connection, cmdLine [][]byte) redis.Reply
	AfterClientClose(c redis.Connection)
	Close()
}

// KeyEventCallback will be called back on key event, such as key inserted or deleted
// may be called concurrently
type KeyEventCallback func(dbIndex int, key string, entity *DataEntity)

// DBEngine is the embedding storage engine exposing more methods for complex application
type DBEngine interface {
	DB
	LoadRDB(dec *core.Decoder) error
	ExecWithLock(conn redis.Connection, cmdLine [][]byte) redis.Reply
	ExecMulti(conn redis.Connection, watching map[string]uint64, cmdLines []CmdLine) redis.Reply
	GetUndoLogs(dbIndex int, cmdLine [][]byte) ([]CmdLine, error)
	ForEach(dbIndex int, cb func(key string, data *DataEntity, expiration *time.Time) bool) error
	RWLocks(dbIndex int, writeKeys []string, readKeys []string) error
	RWUnLocks(dbIndex int, writeKeys []string, readKeys []string) error
	GetDBSize(dbIndex int) (int, int, error)
	GetEntity(dbIndex int, key string) (*DataEntity, bool, error)
	GetExpiration(dbIndex int, key string) (*time.Time, error)
	SetKeyInsertedCallback(cb KeyEventCallback)
	SetKeyDeletedCallback(cb KeyEventCallback)
}

// DataEntity stores data bound to a key, including a string, list, hash, set and so on
type DataEntity struct {
	Data interface{}
}
