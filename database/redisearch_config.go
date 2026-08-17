package database

import (
	"fmt"
	"sort"
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
		"TIMEOUT":             "0",
		"MAXSEARCHRESULTS":    "10000",
		"MAXAGGREGATERESULTS": "2147483648",
		"DEFAULT_DIALECT":     "1",
		"ON_TIMEOUT":          "FAIL",
		"MINPREFIX":           "2",
		"MAXEXPANSIONS":       "200",
	}
)

// ftKebabMap maps the Redis 8.0 kebab-case CONFIG keys (the search-* namespace
// that replaced FT.CONFIG in 8.0) to the internal ftConfig keys. Both directions
// are used so CONFIG GET search-* and CONFIG SET search-* interoperate with
// FT.CONFIG GET/SET on the same underlying values.
var ftKebabMap = map[string]string{
	"search-timeout":              "TIMEOUT",
	"search-on-timeout":           "ON_TIMEOUT",
	"search-max-search-results":   "MAXSEARCHRESULTS",
	"search-max-aggregate-results": "MAXAGGREGATERESULTS",
	"search-min-prefix":           "MINPREFIX",
	"search-max-expansions":       "MAXEXPANSIONS",
	"search-default-dialect":      "DEFAULT_DIALECT",
}

// searchKebabPairs returns the search-* config pairs (kebab key → value) for
// CONFIG GET, sourced from the live ftConfig. Sorted by key for stable output.
func searchKebabPairs() []configPair {
	ftConfigMu.RLock()
	defer ftConfigMu.RUnlock()
	out := make([]configPair, 0, len(ftKebabMap))
	for kebab, internal := range ftKebabMap {
		out = append(out, configPair{key: kebab, value: ftConfig[internal]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out
}

// setSearchKebab applies a CONFIG SET search-* <value> by routing it to the
// internal ftConfig key with the same validation FT.CONFIG SET uses. Returns
// false when the value is invalid (caller emits the error reply).
func setSearchKebab(kebab, value string) bool {
	internal, ok := ftKebabMap[kebab]
	if !ok {
		return false
	}
	ftConfigMu.Lock()
	defer ftConfigMu.Unlock()
	switch internal {
	case "TIMEOUT", "MAXSEARCHRESULTS", "MAXAGGREGATERESULTS", "MINPREFIX", "MAXEXPANSIONS":
		if _, err := strconv.Atoi(value); err != nil {
			return false
		}
	case "DEFAULT_DIALECT":
		d, err := strconv.Atoi(value)
		if err != nil || !validFTDialect(d) {
			return false
		}
	case "ON_TIMEOUT":
		u := strings.ToUpper(value)
		if u != "FAIL" && u != "RETURN" {
			return false
		}
		value = u
	}
	ftConfig[internal] = value
	return true
}

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
		m := protocol.MakeMapReply()
		for k, v := range ftConfig {
			if pattern == "*" || strings.EqualFold(k, pattern) {
				m.Put(k, protocol.MakeBulkReply([]byte(v)))
			}
		}
		return m
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
// FAIL → SEARCH_TIMEOUT… (no ERR prefix). RETURN with no partial → empty search shape.
func ftTimeoutReply(err error) redis.Reply {
	if err == redisearch.ErrTimeout && strings.EqualFold(getFTConfigString("ON_TIMEOUT"), "RETURN") {
		return protocol.MakeMultiRawReply([]redis.Reply{protocol.MakeIntReply(0)})
	}
	if err == redisearch.ErrTimeout || strings.HasPrefix(err.Error(), "SEARCH_") {
		return protocol.MakeErrReply(err.Error())
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
