// RediSearch 同义词支持
package database

import (
	"strings"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
)

// 全局同义词映射表
// groupID -> []terms
type synonymGroups struct {
	groups map[string]map[string]bool // groupID -> set of terms
	terms  map[string]string          // term -> groupID
}

var synonymDB = &synonymGroups{
	groups: make(map[string]map[string]bool),
	terms:  make(map[string]string),
}

// execFTSynUpdate 更新同义词组
// FT.SYNUPDATE index groupId [SKIPINITIALSCAN] term [term ...]
func execFTSynUpdate(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.synupdate' command")
	}
	
	indexName := string(args[0])
	groupID := string(args[1])
	
	_ = indexName
	
	// 查找SKIPINITIALSCAN选项
	startIdx := 2
	if strings.ToUpper(string(args[2])) == "SKIPINITIALSCAN" {
		startIdx = 3
	}
	
	if len(args) <= startIdx {
		return protocol.MakeErrReply("ERR wrong number of arguments")
	}
	
	// 创建或清空同义词组
	synonymDB.groups[groupID] = make(map[string]bool)
	
	// 添加同义词
	for i := startIdx; i < len(args); i++ {
		term := string(args[i])
		synonymDB.groups[groupID][term] = true
		synonymDB.terms[term] = groupID
	}
	
	db.addAof(prependCmd("ft.synupdate", args))
	return protocol.MakeOkReply()
}

// execFTSynDump 转储同义词组
// FT.SYNDUMP index
func execFTSynDump(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.syndump' command")
	}
	
	_ = string(args[0])
	
	// 返回所有同义词组
	var result [][]byte
	
	for groupID, terms := range synonymDB.groups {
		var groupData [][]byte
		groupData = append(groupData, []byte(groupID))
		
		for term := range terms {
			groupData = append(groupData, []byte(term))
		}
		
		result = append(result, protocol.MakeMultiBulkReply(groupData).ToBytes())
	}
	
	return protocol.MakeMultiBulkReply(result)
}

// getSynonyms 获取一个词的所有同义词
func getSynonyms(term string) []string {
	groupID, ok := synonymDB.terms[term]
	if !ok {
		return nil
	}
	
	group, ok := synonymDB.groups[groupID]
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
	registerCommand("FT.SynUpdate", execFTSynUpdate, nil, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("FT.SynDump", execFTSynDump, nil, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
}
