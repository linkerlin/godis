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
	"search-timeout":               "TIMEOUT",
	"search-on-timeout":            "ON_TIMEOUT",
	"search-max-search-results":    "MAXSEARCHRESULTS",
	"search-max-aggregate-results": "MAXAGGREGATERESULTS",
	"search-min-prefix":            "MINPREFIX",
	"search-max-expansions":        "MAXEXPANSIONS",
	"search-default-dialect":       "DEFAULT_DIALECT",
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
	normalized, errReply := validateFTConfigValue(internal, value)
	if errReply != nil {
		return false
	}
	ftConfig[internal] = normalized
	return true
}

// validateFTConfigValue aligns FT.CONFIG SET validation with Redis 8.10 texts.
// Returns the normalized value to store, or an error reply.
func validateFTConfigValue(key, val string) (string, redis.Reply) {
	switch key {
	case "TIMEOUT":
		n, err := strconv.Atoi(val)
		if err != nil {
			return "", protocol.MakeErrReply("SEARCH_PARSE_ARGS Could not convert argument to expected type")
		}
		if n < 0 {
			return "", protocol.MakeErrReply("SEARCH_PARSE_ARGS Value is outside acceptable bounds")
		}
		return val, nil
	case "MAXSEARCHRESULTS", "MAXAGGREGATERESULTS":
		// Redis accepts negatives (unlimited semantics); only reject non-integers.
		if _, err := strconv.Atoi(val); err != nil {
			return "", protocol.MakeErrReply("SEARCH_PARSE_ARGS Could not convert argument to expected type")
		}
		return val, nil
	case "MINPREFIX", "MAXEXPANSIONS":
		n, err := strconv.Atoi(val)
		if err != nil {
			return "", protocol.MakeErrReply("SEARCH_PARSE_ARGS Could not convert argument to expected type")
		}
		if n < 1 {
			return "", protocol.MakeErrReply("SEARCH_PARSE_ARGS Value is outside acceptable bounds")
		}
		return val, nil
	case "DEFAULT_DIALECT":
		d, err := strconv.Atoi(val)
		if err != nil {
			return "", protocol.MakeErrReply("SEARCH_PARSE_ARGS Could not convert argument to expected type")
		}
		if d < 1 {
			return "", protocol.MakeErrReply("SEARCH_PARSE_ARGS Value is outside acceptable bounds")
		}
		if d > 4 {
			return "", protocol.MakeErrReply("SEARCH_VALUE_BAD Default dialect version cannot be higher than 4")
		}
		return val, nil
	case "ON_TIMEOUT":
		u := strings.ToUpper(val)
		switch u {
		case "FAIL", "RETURN", "RETURN-STRICT":
			return u, nil
		default:
			return "", protocol.MakeErrReply("SEARCH_VALUE_BAD Invalid ON_TIMEOUT value")
		}
	default:
		return val, nil
	}
}

// execFTConfig FT.CONFIG GET|SET ...
func execFTConfig(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.CONFIG' command")
	}
	sub := strings.ToUpper(string(args[0]))
	switch sub {
	case "GET":
		if len(args) < 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.CONFIG|GET' command")
		}
		// Redis accepts trailing args after the option (ignored).
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
		if len(args) < 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.CONFIG|SET' command")
		}
		if len(args) < 3 {
			return protocol.MakeErrReply("SEARCH_PARSE_ARGS Expected an argument, but none provided")
		}
		if len(args) > 3 {
			// Redis 8.10 bare EXCESSARGS (no ERR / SEARCH_ prefix).
			return protocol.MakeErrReply("EXCESSARGS")
		}
		key := strings.ToUpper(string(args[1]))
		val := string(args[2])
		ftConfigMu.Lock()
		defer ftConfigMu.Unlock()
		if _, ok := ftConfig[key]; !ok {
			return protocol.MakeErrReply("SEARCH_OPTION_INVALID Invalid option")
		}
		normalized, errReply := validateFTConfigValue(key, val)
		if errReply != nil {
			return errReply
		}
		ftConfig[key] = normalized
		// Persist the setting so it survives AOF replay (DEFAULT_DIALECT,
		// MINPREFIX, MAXEXPANSIONS, TIMEOUT, etc.).
		db.addAof(utils.ToCmdLine3("ft.config", args...))
		return protocol.MakeOkReply()
	case "HELP":
		// Redis 8.10: HELP arity -3 (needs ≥1 trailing token); body is empty.
		if len(args) < 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.CONFIG|HELP' command")
		}
		return protocol.MakeEmptyMultiBulkReply()
	default:
		return protocol.MakeErrReply(fmt.Sprintf("ERR unknown subcommand '%s'. Try FT.CONFIG HELP.", string(args[0])))
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

// ftTimeoutReply maps FT soft-timeout errors according to ON_TIMEOUT (FAIL|RETURN|RETURN-STRICT).
// FAIL → SEARCH_TIMEOUT… (no ERR prefix). RETURN / RETURN-STRICT with no partial → empty search shape.
func ftTimeoutReply(err error) redis.Reply {
	on := strings.ToUpper(getFTConfigString("ON_TIMEOUT"))
	if err == redisearch.ErrTimeout && (on == "RETURN" || on == "RETURN-STRICT") {
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
