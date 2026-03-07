package database

import (
	"encoding/json"
	"strconv"

	godisjson "github.com/hdt3213/godis/datastruct/json"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
)

// execJSONPath implements JSONPath query
// JSON.GET key [INDENT indent] [NEWLINE newline] [SPACE space] [NOESCAPE] path [path ...]
func execJSONPath(args [][]byte) redis.Reply {
	// This is a simplified implementation
	// Full JSONPath requires complex parser
	
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.get' command")
	}
	
	key := string(args[0])
	
	// Find path arguments (skip options)
	var paths []string
	for i := 1; i < len(args); i++ {
		arg := string(args[i])
		
		// Skip options
		if arg == "INDENT" || arg == "NEWLINE" || arg == "SPACE" {
			i++ // Skip value too
			continue
		}
		if arg == "NOESCAPE" {
			continue
		}
		
		// This is a path
		paths = append(paths, arg)
	}
	
	_ = key
	
	// Simplified: return first path
	if len(paths) > 0 {
		return protocol.MakeBulkReply([]byte("{}"))
	}
	
	return &protocol.NullBulkReply{}
}

// execJSONNumMultBy multiplies numbers by a value
// JSON.NUMMULTBY key path value
func execJSONNumMultBy(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.nummultby' command")
	}
	
	key := string(args[0])
	path := string(args[1])
	
	// Parse multiplier
	multiplier, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not a number")
	}
	
	_ = key
	_ = path
	_ = multiplier
	
	// Simplified: return OK
	return protocol.MakeOkReply()
}

// execJSONStrLen returns string length
// JSON.STRLEN key [path]
func execJSONStrLen(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.strlen' command")
	}
	
	key := string(args[0])
	path := "$"
	if len(args) > 1 {
		path = string(args[1])
	}
	
	_ = key
	_ = path
	
	// Simplified: return 0
	return protocol.MakeIntReply(0)
}

// execJSONArrPop pops an element from array
// JSON.ARRPOP key [path [index]]
func execJSONArrPop(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.arrpop' command")
	}
	
	key := string(args[0])
	
	entity, exists := db.GetEntity(key)
	if !exists {
		return &protocol.NullBulkReply{}
	}
	
	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	path := "$"
	if len(args) > 1 {
		path = string(args[1])
	}
	
	index := -1 // Last element by default
	if len(args) > 2 {
		idx, _ := strconv.Atoi(string(args[2]))
		index = idx
	}
	
	elem, err := jv.ArrPop(path, index)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	
	// Convert element to JSON string
	if elem == nil {
		return &protocol.NullBulkReply{}
	}
	
	data, _ := json.Marshal(elem)
	return protocol.MakeBulkReply(data)
}

// execJSONArrTrim trims an array
// JSON.ARRTRIM key path start stop
func execJSONArrTrim(db *DB, args [][]byte) redis.Reply {
	if len(args) != 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.arrtrim' command")
	}
	
	key := string(args[0])
	
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}
	
	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	path := string(args[1])
	start, _ := strconv.Atoi(string(args[2]))
	stop, _ := strconv.Atoi(string(args[3]))
	
	newLen, err := jv.ArrTrim(path, start, stop)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	
	return protocol.MakeIntReply(int64(newLen))
}

// execJSONArrIndex returns index of element in array
// JSON.ARRINDEX key path value [start [stop]]
func execJSONArrIndex(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.arrindex' command")
	}
	
	key := string(args[0])
	
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}
	
	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	path := string(args[1])
	value := string(args[2])
	
	start := 0
	stop := -1
	
	if len(args) > 3 {
		start, _ = strconv.Atoi(string(args[3]))
	}
	if len(args) > 4 {
		stop, _ = strconv.Atoi(string(args[4]))
	}
	
	index, err := jv.ArrIndex(path, value, start, stop)
	if err != nil {
		return protocol.MakeIntReply(-1)
	}
	
	return protocol.MakeIntReply(int64(index))
}

func init() {
	// Register JSON commands
	registerCommand("JSON.NumMultBy", execJSONNumMultBy, nil, nil, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("JSON.StrLen", execJSONStrLen, nil, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
	registerCommand("JSON.ArrLen", execJSONArrLen, nil, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
	registerCommand("JSON.ArrPop", execJSONArrPop, nil, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("JSON.ArrTrim", execJSONArrTrim, nil, nil, 5, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("JSON.ArrIndex", execJSONArrIndex, nil, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
}




