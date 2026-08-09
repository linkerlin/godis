package database

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// FTProfileReply: RESP2 [results, profile]; RESP3 Map{Results, Profile}.
type FTProfileReply struct {
	results redis.Reply
	profile redis.Reply
}

func (r *FTProfileReply) ToBytes() []byte {
	if r == nil {
		return []byte("*0\r\n")
	}
	return protocol.MakeMultiRawReply([]redis.Reply{r.results, r.profile}).ToBytes()
}

func (r *FTProfileReply) ToRESP3() []byte {
	m := protocol.MakeMapReply()
	m.Put("Results", r.results)
	m.Put("Profile", r.profile)
	return m.ToRESP3()
}

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
	totalMs := float64(time.Since(start).Microseconds()) / 1000.0

	// Count results from the reply for an honest metric. execFTSearch/
	// execFTAggregate bundle parse+execute, so a true parse/iterate split
	// requires refactoring those into separate phases (deferred to the
	// profiling/scoring overhaul). We report total wall clock + result count
	// instead of fabricating a 5/95 parse/iterate split.
	resultCount := profileCountResults(result)
	// ponytail: no per-iterator breakdown; instrument engine.Search/Aggregate
	// to return timing when detailed iterator profiles are needed.
	profile := protocol.MakeMapReply()
	profile.Put("Total profile time (ms)", protocol.MakeDoubleReply(totalMs))
	profile.Put("Result count", protocol.MakeIntReply(resultCount))
	profile.Put("Profile type", protocol.MakeBulkReply([]byte(kind)))
	return &FTProfileReply{results: result, profile: profile}
}

// profileCountResults extracts the result count from a SEARCH/AGGREGATE reply.
// FT.SEARCH/AGGREGATE replies are *MultiBulkReply whose Args[0] is the total
// count formatted as a decimal string. Returns -1 if the shape is unrecognized.
func profileCountResults(r redis.Reply) int64 {
	if r == nil {
		return -1
	}
	switch rep := r.(type) {
	case *FTSearchReply:
		return rep.total
	case *protocol.MultiRawReply:
		if len(rep.Replies) > 0 {
			return profileCountResults(rep.Replies[0])
		}
	case *protocol.MultiBulkReply:
		if len(rep.Args) > 0 {
			if n, err := strconv.ParseInt(string(rep.Args[0]), 10, 64); err == nil {
				return n
			}
		}
	}
	return -1
}

func init() {
	registerCommand("FT.Profile", execFTProfile, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}

// keep fmt for potential future profile string fields
var _ = fmt.Sprintf
