package database

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

var (
	ftConfigMu sync.RWMutex
	ftConfig   = map[string]string{
		"TIMEOUT":          "0",
		"MAXSEARCHRESULTS": "10000",
		"DEFAULT_DIALECT":  "1",
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
		if key == "TIMEOUT" || key == "MAXSEARCHRESULTS" {
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
		ftConfig[key] = val
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

func validFTDialect(d int) bool {
	return d >= 1 && d <= 4
}

func init() {
	registerCommand("FT.Config", execFTConfig, prepareNoKeys, nil, -2, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin}, 0, 0, 0)
}
