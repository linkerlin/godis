// RediSearch 同义词支持（按 index 隔离）
package database

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

type synonymGroups struct {
	groups map[string]map[string]bool // groupID -> set of terms
	terms  map[string]string          // term -> groupID
}

var (
	synonymDBs   = make(map[string]*synonymGroups)
	synonymDBMu  sync.RWMutex
	synGroupSeq  int64
)

func getOrCreateSynDB(index string) *synonymGroups {
	synonymDBMu.Lock()
	defer synonymDBMu.Unlock()
	if db, ok := synonymDBs[index]; ok {
		return db
	}
	db := &synonymGroups{
		groups: make(map[string]map[string]bool),
		terms:  make(map[string]string),
	}
	synonymDBs[index] = db
	return db
}

func getSynDB(index string) *synonymGroups {
	synonymDBMu.RLock()
	defer synonymDBMu.RUnlock()
	return synonymDBs[index]
}

func dropSynDB(index string) {
	synonymDBMu.Lock()
	defer synonymDBMu.Unlock()
	delete(synonymDBs, index)
}

// execFTSynAdd creates a new synonym group (deprecated in Redis; returns group id)
// FT.SYNADD index term [term ...]
func execFTSynAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.synadd' command")
	}
	sdb := getOrCreateSynDB(string(args[0]))
	id := atomic.AddInt64(&synGroupSeq, 1)
	groupID := strconv.FormatInt(id, 10)
	sdb.groups[groupID] = make(map[string]bool)
	for i := 1; i < len(args); i++ {
		term := string(args[i])
		sdb.groups[groupID][term] = true
		sdb.terms[term] = groupID
	}
	db.addAof(prependCmd("ft.synadd", args))
	return protocol.MakeIntReply(id)
}

// execFTSynUpdate 更新同义词组
// FT.SYNUPDATE index groupId [SKIPINITIALSCAN] term [term ...]
func execFTSynUpdate(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.synupdate' command")
	}
	sdb := getOrCreateSynDB(string(args[0]))
	groupID := string(args[1])

	startIdx := 2
	if strings.ToUpper(string(args[2])) == "SKIPINITIALSCAN" {
		startIdx = 3
	}
	if len(args) <= startIdx {
		return protocol.MakeErrReply("ERR wrong number of arguments")
	}

	// clear old term mappings for this group
	if old, ok := sdb.groups[groupID]; ok {
		for term := range old {
			if sdb.terms[term] == groupID {
				delete(sdb.terms, term)
			}
		}
	}
	sdb.groups[groupID] = make(map[string]bool)
	for i := startIdx; i < len(args); i++ {
		term := string(args[i])
		sdb.groups[groupID][term] = true
		sdb.terms[term] = groupID
	}

	db.addAof(prependCmd("ft.synupdate", args))
	return protocol.MakeOkReply()
}

// execFTSynDump dumps synonym terms → group ids.
// RESP2: flat field array (term, [groupIds], ...); RESP3: Map term→group-id array.
// FT.SYNDUMP index
func execFTSynDump(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.syndump' command")
	}
	sdb := getSynDB(string(args[0]))
	if sdb == nil {
		return protocol.MakeMapReply()
	}
	m := protocol.MakeMapReply()
	for term, groupID := range sdb.terms {
		m.Put(term, protocol.MakeMultiBulkReply([][]byte{[]byte(groupID)}))
	}
	return m
}

// getSynonyms 获取一个词在指定 index 下的同义词
func getSynonyms(index, term string) []string {
	sdb := getSynDB(index)
	if sdb == nil {
		return nil
	}
	groupID, ok := sdb.terms[term]
	if !ok {
		return nil
	}
	group, ok := sdb.groups[groupID]
	if !ok {
		return nil
	}
	var synonyms []string
	for t := range group {
		if t != term {
			synonyms = append(synonyms, t)
		}
	}
	return synonyms
}

func init() {
	registerCommand("FT.SynAdd", execFTSynAdd, writeFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("FT.SynUpdate", execFTSynUpdate, writeFirstKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("FT.SynDump", execFTSynDump, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
