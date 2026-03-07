package database

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	godisjson "github.com/hdt3213/godis/datastruct/json"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

// execJSONSet sets a JSON value at the specified path
// JSON.SET key path value [NX | XX]
func execJSONSet(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.set' command")
	}
	
	key := string(args[0])
	path := string(args[1])
	
	// Parse the JSON value
	var value interface{}
	if err := json.Unmarshal(args[2], &value); err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR invalid JSON: %v", err))
	}
	
	// Parse options
	nx := false
	xx := false
	for i := 3; i < len(args); i++ {
		op := strings.ToUpper(string(args[i]))
		switch op {
		case "NX":
			nx = true
		case "XX":
			xx = true
		}
	}
	
	if nx && xx {
		return protocol.MakeErrReply("ERR NX and XX are mutually exclusive")
	}
	
	// Get or create JSON value
	entity, exists := db.GetEntity(key)
	var jv *godisjson.JSONValue
	
	if !exists {
		if xx {
			// XX: only set if exists
			return &protocol.NullBulkReply{}
		}
		jv, _ = godisjson.NewJSONValueFromString("{}")
		db.PutEntity(key, &database.DataEntity{Data: jv})
	} else {
		var ok bool
		jv, ok = entity.Data.(*godisjson.JSONValue)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
	}
	
	// Set the value
	ok, err := jv.Set(path, value, nx, xx)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}
	
	if !ok {
		return &protocol.NullBulkReply{}
	}
	
	db.addAof(utils.ToCmdLine3("json.set", args...))
	return protocol.MakeOkReply()
}

// execJSONGet gets a JSON value at the specified path
// JSON.GET key [path ...]
func execJSONGet(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.get' command")
	}
	
	key := string(args[0])
	
	// Get JSON value
	entity, exists := db.GetEntity(key)
	if !exists {
		return &protocol.NullBulkReply{}
	}
	
	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	// Default path is root
	paths := []string{"$"}
	if len(args) > 1 {
		paths = make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			paths[i-1] = string(args[i])
		}
	}
	
	// Get values for all paths
	if len(paths) == 1 {
		val, err := jv.Get(paths[0])
		if err != nil {
			return &protocol.NullBulkReply{}
		}
		
		result, err := json.Marshal(val)
		if err != nil {
			return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
		}
		return protocol.MakeBulkReply(result)
	}
	
	// Multiple paths - return as object
	result := make(map[string]interface{})
	for _, path := range paths {
		val, err := jv.Get(path)
		if err == nil {
			result[path] = val
		}
	}
	
	data, err := json.Marshal(result)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}
	return protocol.MakeBulkReply(data)
}

// execJSONDel deletes JSON values at the specified path
// JSON.DEL key [path]
func execJSONDel(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 || len(args) > 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.del' command")
	}
	
	key := string(args[0])
	path := "$"
	if len(args) > 1 {
		path = string(args[1])
	}
	
	// Get JSON value
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}
	
	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	// Delete
	ok, err := jv.Del(path)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}
	
	// If root was deleted, remove the key
	if path == "$" {
		db.Remove(key)
	}
	
	if ok {
		db.addAof(utils.ToCmdLine3("json.del", args...))
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// execJSONType returns the type of JSON value at the specified path
// JSON.TYPE key [path]
func execJSONType(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 || len(args) > 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.type' command")
	}
	
	key := string(args[0])
	path := "$"
	if len(args) > 1 {
		path = string(args[1])
	}
	
	// Get JSON value
	entity, exists := db.GetEntity(key)
	if !exists {
		return &protocol.NullBulkReply{}
	}
	
	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	typ, err := jv.Type(path)
	if err != nil {
		return &protocol.NullBulkReply{}
	}
	
	return protocol.MakeBulkReply([]byte(typ))
}

// execJSONNumIncrBy increments a number at the specified path
// JSON.NUMINCRBY key path number
func execJSONNumIncrBy(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.numincrby' command")
	}
	
	key := string(args[0])
	path := string(args[1])
	
	// Parse increment
	increment, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR invalid number")
	}
	
	// Get JSON value
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}
	
	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	// Increment
	newVal, err := jv.NumIncrBy(path, increment)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}
	
	db.addAof(utils.ToCmdLine3("json.numincrby", args...))
	return protocol.MakeBulkReply([]byte(fmt.Sprintf("%g", newVal)))
}

// execJSONStrAppend appends a string to the value at the specified path
// JSON.STRAPPEND key [path] value
func execJSONStrAppend(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 || len(args) > 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.strappend' command")
	}
	
	key := string(args[0])
	var path string
	var value []byte
	
	if len(args) == 2 {
		path = "$"
		value = args[1]
	} else {
		path = string(args[1])
		value = args[2]
	}
	
	// Parse string value (remove quotes if present)
	strVal := string(value)
	if len(strVal) >= 2 && strVal[0] == '"' && strVal[len(strVal)-1] == '"' {
		strVal = strVal[1 : len(strVal)-1]
	}
	
	// Get JSON value
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}
	
	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	// Append
	newLen, err := jv.StrAppend(path, strVal)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}
	
	db.addAof(utils.ToCmdLine3("json.strappend", args...))
	return protocol.MakeIntReply(int64(newLen))
}

// execJSONArrAppend appends values to an array at the specified path
// JSON.ARRAPPEND key path value [value ...]
func execJSONArrAppend(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.arrappend' command")
	}
	
	key := string(args[0])
	path := string(args[1])
	
	// Parse values
	values := make([]interface{}, len(args)-2)
	for i := 2; i < len(args); i++ {
		var val interface{}
		if err := json.Unmarshal(args[i], &val); err != nil {
			// Try as string
			val = string(args[i])
		}
		values[i-2] = val
	}
	
	// Get JSON value
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}
	
	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	// Append
	newLen, err := jv.ArrAppend(path, values...)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}
	
	db.addAof(utils.ToCmdLine3("json.arrappend", args...))
	return protocol.MakeIntReply(int64(newLen))
}

// execJSONArrLen returns the length of an array at the specified path
// JSON.ARRLEN key [path]
func execJSONArrLen(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 || len(args) > 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.arrlen' command")
	}
	
	key := string(args[0])
	path := "$"
	if len(args) > 1 {
		path = string(args[1])
	}
	
	// Get JSON value
	entity, exists := db.GetEntity(key)
	if !exists {
		return &protocol.NullBulkReply{}
	}
	
	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	length, err := jv.ArrLen(path)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}
	
	return protocol.MakeIntReply(int64(length))
}

// execJSONObjKeys returns the keys of an object at the specified path
// JSON.OBJKEYS key [path]
func execJSONObjKeys(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 || len(args) > 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.objkeys' command")
	}
	
	key := string(args[0])
	path := "$"
	if len(args) > 1 {
		path = string(args[1])
	}
	
	// Get JSON value
	entity, exists := db.GetEntity(key)
	if !exists {
		return &protocol.NullBulkReply{}
	}
	
	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	keys, err := jv.ObjKeys(path)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}
	
	result := make([][]byte, len(keys))
	for i, k := range keys {
		result[i] = []byte(k)
	}
	return protocol.MakeMultiBulkReply(result)
}

// execJSONObjLen returns the number of keys in an object at the specified path
// JSON.OBJLEN key [path]
func execJSONObjLen(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 || len(args) > 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.objlen' command")
	}
	
	key := string(args[0])
	path := "$"
	if len(args) > 1 {
		path = string(args[1])
	}
	
	// Get JSON value
	entity, exists := db.GetEntity(key)
	if !exists {
		return &protocol.NullBulkReply{}
	}
	
	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	
	length, err := jv.ObjLen(path)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}
	
	return protocol.MakeIntReply(int64(length))
}

// Helper function for JSON command key preparation
func prepareJSONKey(args [][]byte) ([]string, []string) {
	return []string{string(args[0])}, nil
}

func init() {
	registerCommand("JSON.Set", execJSONSet, prepareJSONKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("JSON.Get", execJSONGet, prepareJSONKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("JSON.Del", execJSONDel, prepareJSONKey, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.Type", execJSONType, prepareJSONKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("JSON.NumIncrBy", execJSONNumIncrBy, prepareJSONKey, nil, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.StrAppend", execJSONStrAppend, prepareJSONKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.ArrAppend", execJSONArrAppend, prepareJSONKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.ArrLen", execJSONArrLen, prepareJSONKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("JSON.ObjKeys", execJSONObjKeys, prepareJSONKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("JSON.ObjLen", execJSONObjLen, prepareJSONKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}

