package database

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/linkerlin/godis/datastruct/redisearch"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

var (
	ftConfigMu sync.RWMutex
	ftConfig   = map[string]string{
		"TIMEOUT":          "0",
		"MAXSEARCHRESULTS": "10000",
		"DEFAULT_DIALECT":  "1",
		"ON_TIMEOUT":       "FAIL",
		"MINPREFIX":        "2",
		"MAXEXPANSIONS":    "200",
	}
)

// execFTConfig FT.CONFIG GET|SET ...
func execFTConfig(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.config' command")
	}
	sub := strings.ToUpper(string(args[0]))
	switch sub {
	case "GET":
		if len(args) != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.config|get' command")
		}
		pattern := string(args[1])
		ftConfigMu.RLock()
		defer ftConfigMu.RUnlock()
		var out [][]byte
		for k, v := range ftConfig {
			if pattern == "*" || strings.EqualFold(k, pattern) {
				out = append(out, []byte(k), []byte(v))
			}
		}
		return protocol.MakeMultiBulkReply(out)
	case "SET":
		if len(args) != 3 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.config|set' command")
		}
		key := strings.ToUpper(string(args[1]))
		val := string(args[2])
		ftConfigMu.Lock()
		defer ftConfigMu.Unlock()
		if _, ok := ftConfig[key]; !ok {
			return protocol.MakeErrReply(fmt.Sprintf("ERR Unknown option '%s'", string(args[1])))
		}
		if key == "TIMEOUT" || key == "MAXSEARCHRESULTS" || key == "MINPREFIX" || key == "MAXEXPANSIONS" {
			if _, err := strconv.Atoi(val); err != nil {
				return protocol.MakeErrReply("ERR Invalid value for option")
			}
		}
		if key == "DEFAULT_DIALECT" {
			d, err := strconv.Atoi(val)
			if err != nil || !validFTDialect(d) {
				return protocol.MakeErrReply("ERR Invalid value for option")
			}
		}
		if key == "ON_TIMEOUT" {
			u := strings.ToUpper(val)
			if u != "FAIL" && u != "RETURN" {
				return protocol.MakeErrReply("ERR Invalid value for option")
			}
			val = u
		}
		ftConfig[key] = val
		// Persist the setting so it survives AOF replay (DEFAULT_DIALECT,
		// MINPREFIX, MAXEXPANSIONS, TIMEOUT, etc.).
		db.addAof(utils.ToCmdLine3("ft.config", args...))
		return protocol.MakeOkReply()
	case "HELP":
		return protocol.MakeMultiBulkReply([][]byte{
			[]byte("FT.CONFIG GET <option|*>"),
			[]byte("FT.CONFIG SET <option> <value>"),
		})
	default:
		return protocol.MakeErrReply("ERR Unknown subcommand '" + string(args[0]) + "'")
	}
}

func getFTConfigInt(key string) int {
	ftConfigMu.RLock()
	defer ftConfigMu.RUnlock()
	n, _ := strconv.Atoi(ftConfig[key])
	return n
}

func getFTConfigString(key string) string {
	ftConfigMu.RLock()
	defer ftConfigMu.RUnlock()
	return ftConfig[key]
}

// ftTimeoutReply maps FT soft-timeout errors according to ON_TIMEOUT (FAIL|RETURN).
func ftTimeoutReply(err error) redis.Reply {
	if err == redisearch.ErrTimeout && strings.EqualFold(getFTConfigString("ON_TIMEOUT"), "RETURN") {
		return protocol.MakeMultiRawReply([]redis.Reply{protocol.MakeIntReply(0)})
	}
	return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
}

func validFTDialect(d int) bool {
	return d >= 1 && d <= 4
}

func init() {
	registerCommand("FT.Config", execFTConfig, prepareNoKeys, nil, -2, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin}, 0, 0, 0)
}
