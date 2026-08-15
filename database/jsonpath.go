package database

import (
	"encoding/json"
	"strconv"
	"strings"

	godisjson "github.com/linkerlin/godis/datastruct/json"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// execJSONNumMultBy multiplies numbers by a value
// JSON.NUMMULTBY key path value
func execJSONNumMultBy(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.nummultby' command")
	}

	key := string(args[0])
	path := string(args[1])

	multiplier, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not a number")
	}

	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeErrReply("ERR could not perform this operation on a key that doesn't exist")
	}

	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	newVal, err := jv.NumMultBy(path, multiplier)
	if err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}

	db.addAof(utils.ToCmdLine3("json.nummultby", args...))
	return wrapEnhancedJSONNumReply(path, newVal)
}

// execJSONStrLen returns string length
// JSON.STRLEN key [path]
func execJSONStrLen(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.strlen' command")
	}

	key := string(args[0])
	path := "$"
	explicitPath := false
	if len(args) > 1 {
		path = string(args[1])
		explicitPath = true
	}

	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeErrReply("ERR could not perform this operation on a key that doesn't exist")
	}

	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	length, err := jv.StrLen(path)
	if err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}
	return wrapEnhancedJSONIntReply(path, explicitPath, int64(length))
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
		return protocol.MakeErrReply("ERR could not perform this operation on a key that doesn't exist")
	}

	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	path := "$"
	explicitPath := false
	if len(args) > 1 {
		path = string(args[1])
		explicitPath = true
	}

	index := -1 // Last element by default
	if len(args) > 2 {
		idx, _ := strconv.Atoi(string(args[2]))
		index = idx
	}

	elem, err := jv.ArrPop(path, index)
	if err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}

	// Convert element to JSON string
	if elem == nil {
		return &protocol.NullBulkReply{}
	}

	if explicitPath && strings.HasPrefix(path, "$") {
		data, err := json.Marshal([]interface{}{elem})
		if err != nil {
			return protocol.MakeErrReply("ERR " + err.Error())
		}
		db.addAof(utils.ToCmdLine3("json.arrpop", args...))
		return protocol.MakeBulkReply(data)
	}

	data, _ := json.Marshal(elem)
	db.addAof(utils.ToCmdLine3("json.arrpop", args...))
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
		return protocol.MakeErrReply("ERR could not perform this operation on a key that doesn't exist")
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
		return protocol.MakeErrReply("ERR " + err.Error())
	}

	db.addAof(utils.ToCmdLine3("json.arrtrim", args...))
	return wrapEnhancedJSONIntReply(path, true, int64(newLen))
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
		return protocol.MakeErrReply("ERR Path does not exist")
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
		return wrapEnhancedJSONIntReply(path, true, -1)
	}

	return wrapEnhancedJSONIntReply(path, true, int64(index))
}

func init() {
	// Register JSON commands (ArrLen is registered in json.go with prepareJSONKey)
	registerCommand("JSON.NumMultBy", execJSONNumMultBy, prepareJSONKey, nil, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.StrLen", execJSONStrLen, prepareJSONKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("JSON.ArrPop", execJSONArrPop, prepareJSONKey, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.ArrTrim", execJSONArrTrim, prepareJSONKey, nil, 5, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.ArrIndex", execJSONArrIndex, prepareJSONKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
