package database

import (
	"fmt"
	"strings"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// execFTProfile FT.PROFILE index SEARCH|AGGREGATE [LIMITED] <query...>
// Minimal profile: run SEARCH/AGGREGATE and attach wall-clock timing.
func execFTProfile(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.profile' command")
	}
	index := args[0]
	kind := strings.ToUpper(string(args[1]))
	i := 2
	if i < len(args) && strings.EqualFold(string(args[i]), "LIMITED") {
		i++
	}
	if i >= len(args) {
		return protocol.MakeSyntaxErrReply()
	}
	rest := append([][]byte{index}, args[i:]...)
	start := time.Now()
	var result redis.Reply
	switch kind {
	case "SEARCH":
		result = execFTSearch(db, rest)
	case "AGGREGATE":
		result = execFTAggregate(db, rest)
	default:
		return protocol.MakeErrReply("ERR Unknown FT.PROFILE subcommand '" + string(args[1]) + "'")
	}
	ms := float64(time.Since(start).Microseconds()) / 1000.0
	profile := protocol.MakeMultiBulkReply([][]byte{
		[]byte("Total profile time (ms)"),
		[]byte(fmt.Sprintf("%.3f", ms)),
		[]byte("Profile type"),
		[]byte(kind),
	})
	return protocol.MakeMultiRawReply([]redis.Reply{result, profile})
}

func init() {
	registerCommand("FT.Profile", execFTProfile, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
