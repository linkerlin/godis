package database

import (
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// execClusterStandalone answers CLUSTER * when running without the cluster
// package wired (standalone Server). Redis returns the same disabled ERR
// rather than "unknown command".
func execClusterStandalone(_ *DB, _ [][]byte) redis.Reply {
	return protocol.MakeErrReply("ERR This instance has cluster support disabled")
}

func init() {
	registerCommand("Cluster", execClusterStandalone, nil, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagAdmin}, 0, 0, 0)
}
