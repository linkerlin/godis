package database

import (
	"strings"
	"sync"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

var (
	shutdownHookMu sync.Mutex
	shutdownHook   func()
	shutdownOnce   sync.Once
)

// SetShutdownHook registers a callback invoked once on SHUTDOWN (e.g. close TCP handler).
func SetShutdownHook(fn func()) {
	shutdownHookMu.Lock()
	shutdownHook = fn
	shutdownHookMu.Unlock()
}

// execShutdown handles SHUTDOWN [NOSAVE|SAVE] [NOW] [FORCE] [ABORT]
func execShutdown(_ *Server, args [][]byte) redis.Reply {
	for _, a := range args {
		opt := strings.ToUpper(string(a))
		switch opt {
		case "NOSAVE", "SAVE", "NOW", "FORCE", "ABORT":
			// Accepted for Redis compatibility.
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}
	shutdownOnce.Do(func() {
		shutdownHookMu.Lock()
		fn := shutdownHook
		shutdownHookMu.Unlock()
		if fn != nil {
			go fn()
		}
	})
	return &protocol.NoReply{}
}

func init() {
	registerSpecialCommand("Shutdown", -1, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript, redisFlagLoading}, 0, 0, 0)
}
