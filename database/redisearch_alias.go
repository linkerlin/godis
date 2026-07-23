package database

import (
	"fmt"
	"sync"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// searchAliases maps alias name -> real index name.
var (
	searchAliases   = make(map[string]string)
	searchAliasesMu sync.RWMutex
)

func resolveSearchIndex(name string) string {
	searchAliasesMu.RLock()
	defer searchAliasesMu.RUnlock()
	if idx, ok := searchAliases[name]; ok {
		return idx
	}
	return name
}

func clearAliasesForIndex(indexName string) {
	searchAliasesMu.Lock()
	defer searchAliasesMu.Unlock()
	for alias, idx := range searchAliases {
		if idx == indexName {
			delete(searchAliases, alias)
		}
	}
}

// execFTAliasAdd FT.ALIASADD alias index
func execFTAliasAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.aliasadd' command")
	}
	alias := string(args[0])
	index := string(args[1])

	searchEnginesMu.RLock()
	_, ok := searchEngines[index]
	searchEnginesMu.RUnlock()
	if !ok {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Index '%s' does not exist", index))
	}

	searchAliasesMu.Lock()
	defer searchAliasesMu.Unlock()
	if _, exists := searchAliases[alias]; exists {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Alias '%s' already exists", alias))
	}
	searchEnginesMu.RLock()
	_, nameConflict := searchEngines[alias]
	searchEnginesMu.RUnlock()
	if nameConflict {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Alias '%s' conflicts with an existing index name", alias))
	}
	searchAliases[alias] = index
	db.addAof(utils.ToCmdLine3("ft.aliasadd", args...))
	return protocol.MakeOkReply()
}

// execFTAliasDel FT.ALIASDEL alias
func execFTAliasDel(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.aliasdel' command")
	}
	alias := string(args[0])
	searchAliasesMu.Lock()
	defer searchAliasesMu.Unlock()
	if _, ok := searchAliases[alias]; !ok {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Alias '%s' does not exist", alias))
	}
	delete(searchAliases, alias)
	db.addAof(utils.ToCmdLine3("ft.aliasdel", args...))
	return protocol.MakeOkReply()
}

// execFTAliasUpdate FT.ALIASUPDATE alias index
func execFTAliasUpdate(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.aliasupdate' command")
	}
	alias := string(args[0])
	index := string(args[1])

	searchEnginesMu.RLock()
	_, ok := searchEngines[index]
	searchEnginesMu.RUnlock()
	if !ok {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Index '%s' does not exist", index))
	}

	searchAliasesMu.Lock()
	defer searchAliasesMu.Unlock()
	searchAliases[alias] = index
	db.addAof(utils.ToCmdLine3("ft.aliasupdate", args...))
	return protocol.MakeOkReply()
}

// execFTTagVals FT.TAGVALS index fieldName
func execFTTagVals(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.tagvals' command")
	}
	indexName := resolveSearchIndex(string(args[0]))
	field := string(args[1])

	searchEnginesMu.RLock()
	engine, ok := searchEngines[indexName]
	searchEnginesMu.RUnlock()
	if !ok || engine == nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Index '%s' does not exist", string(args[0])))
	}

	tags := engine.TagVals(field)
	result := make([][]byte, len(tags))
	for i, t := range tags {
		result[i] = []byte(t)
	}
	return protocol.MakeMultiBulkReply(result)
}

func init() {
	registerCommand("FT.AliasAdd", execFTAliasAdd, prepareNoKeys, nil, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("FT.AliasDel", execFTAliasDel, prepareNoKeys, nil, 2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("FT.AliasUpdate", execFTAliasUpdate, prepareNoKeys, nil, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("FT.TagVals", execFTTagVals, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
