package database

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	godisjson "github.com/linkerlin/godis/datastruct/json"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// execJSONSet sets a JSON value at the specified path
// JSON.SET key path value [NX | XX]
func execJSONSet(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.set' command")
	}

	key := string(args[0])
	path := string(args[1])
	if reply := validateBulkBytes(args[1]); reply != nil {
		return reply
	}
	if reply := validateBulkBytes(args[2]); reply != nil {
		return reply
	}

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
// JSON.GET key [INDENT indent] [NEWLINE newline] [SPACE space] [NOESCAPE] [path ...]
func execJSONGet(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.get' command")
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

	opts, paths, errReply := parseJSONGetArgs(args[1:])
	if errReply != nil {
		return errReply
	}
	if len(paths) == 0 {
		paths = []string{"$"}
	}

	if len(paths) == 1 {
		val, err := jv.Get(paths[0])
		if err != nil {
			return &protocol.NullBulkReply{}
		}
		result, err := marshalJSONGet(val, opts)
		if err != nil {
			return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
		}
		return protocol.MakeBulkReply(result)
	}

	// Multiple paths — preserve argument order (not Go map iteration order).
	var b strings.Builder
	b.WriteByte('{')
	first := true
	for _, path := range paths {
		val, err := jv.Get(path)
		if err != nil {
			continue
		}
		if !first {
			b.WriteByte(',')
		}
		first = false
		keyBytes, err := marshalJSONRaw(path, opts.noEscape)
		if err != nil {
			return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
		}
		valBytes, err := marshalJSONRaw(val, opts.noEscape)
		if err != nil {
			return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
		}
		b.Write(keyBytes)
		b.WriteByte(':')
		b.Write(valBytes)
	}
	b.WriteByte('}')
	result := []byte(b.String())
	if opts.pretty {
		var err error
		result, err = formatJSONPretty(result, opts)
		if err != nil {
			return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
		}
	}
	return protocol.MakeBulkReply(result)
}

type jsonGetFormatOpts struct {
	indent   string
	newline  string
	space    string
	noEscape bool
	pretty   bool
}

func parseJSONGetArgs(args [][]byte) (jsonGetFormatOpts, []string, redis.Reply) {
	opts := jsonGetFormatOpts{newline: "\n", space: " "}
	var paths []string
	for i := 0; i < len(args); i++ {
		arg := string(args[i])
		upper := strings.ToUpper(arg)
		switch upper {
		case "INDENT":
			if i+1 >= len(args) {
				return opts, nil, protocol.MakeSyntaxErrReply()
			}
			opts.indent = string(args[i+1])
			opts.pretty = true
			i++
		case "NEWLINE":
			if i+1 >= len(args) {
				return opts, nil, protocol.MakeSyntaxErrReply()
			}
			opts.newline = string(args[i+1])
			opts.pretty = true
			i++
		case "SPACE":
			if i+1 >= len(args) {
				return opts, nil, protocol.MakeSyntaxErrReply()
			}
			opts.space = string(args[i+1])
			opts.pretty = true
			i++
		case "NOESCAPE":
			opts.noEscape = true
		default:
			paths = append(paths, arg)
		}
	}
	return opts, paths, nil
}

func marshalJSONRaw(v interface{}, noEscape bool) ([]byte, error) {
	if !noEscape {
		return json.Marshal(v)
	}
	var buf strings.Builder
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return []byte(strings.TrimSuffix(buf.String(), "\n")), nil
}

func marshalJSONGet(v interface{}, opts jsonGetFormatOpts) ([]byte, error) {
	raw, err := marshalJSONRaw(v, opts.noEscape)
	if err != nil {
		return nil, err
	}
	if !opts.pretty {
		return raw, nil
	}
	return formatJSONPretty(raw, opts)
}

func formatJSONPretty(raw []byte, opts jsonGetFormatOpts) ([]byte, error) {
	var indented bytes.Buffer
	if err := json.Indent(&indented, raw, "", opts.indent); err != nil {
		return raw, nil
	}
	s := indented.String()
	if opts.space != " " {
		s = strings.ReplaceAll(s, ": ", ":"+opts.space)
	}
	if opts.newline != "\n" {
		s = strings.ReplaceAll(s, "\n", opts.newline)
	}
	return []byte(s), nil
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
	if reply := validateBulkBytes(value); reply != nil {
		return reply
	}
	if len(args) == 3 {
		if reply := validateBulkBytes(args[1]); reply != nil {
			return reply
		}
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
	if reply := validateBulkBytes(args[1]); reply != nil {
		return reply
	}

	// Parse values
	values := make([]interface{}, len(args)-2)
	for i := 2; i < len(args); i++ {
		if reply := validateBulkBytes(args[i]); reply != nil {
			return reply
		}
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

// execJSONArrInsert inserts values into an array at index
// JSON.ARRINSERT key path index value [value ...]
func execJSONArrInsert(db *DB, args [][]byte) redis.Reply {
	if len(args) < 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.arrinsert' command")
	}
	key := string(args[0])
	path := string(args[1])
	index, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	values := make([]interface{}, len(args)-3)
	for i := 3; i < len(args); i++ {
		var val interface{}
		if err := json.Unmarshal(args[i], &val); err != nil {
			val = string(args[i])
		}
		values[i-3] = val
	}
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}
	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	newLen, err := jv.ArrInsert(path, index, values...)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}
	db.addAof(utils.ToCmdLine3("json.arrinsert", args...))
	return protocol.MakeIntReply(int64(newLen))
}

// execJSONMGet gets the same path from multiple keys
// JSON.MGET key [key ...] path
func execJSONMGet(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.mget' command")
	}
	path := string(args[len(args)-1])
	keys := args[:len(args)-1]
	results := make([][]byte, len(keys))
	for i, keyBytes := range keys {
		entity, exists := db.GetEntity(string(keyBytes))
		if !exists {
			results[i] = nil
			continue
		}
		jv, ok := entity.Data.(*godisjson.JSONValue)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
		val, err := jv.Get(path)
		if err != nil {
			results[i] = nil
			continue
		}
		data, err := json.Marshal(val)
		if err != nil {
			results[i] = nil
			continue
		}
		results[i] = data
	}
	return protocol.MakeMultiBulkReply(results)
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

// execJSONMSet sets multiple key/path/value triples
// JSON.MSET key path value [key path value ...]
func execJSONMSet(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 || len(args)%3 != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.mset' command")
	}
	// validate all values first (atomic-ish: fail before any write if JSON invalid)
	type triple struct {
		key   string
		path  string
		value interface{}
		raw   [][]byte
	}
	triples := make([]triple, 0, len(args)/3)
	for i := 0; i < len(args); i += 3 {
		var value interface{}
		if err := json.Unmarshal(args[i+2], &value); err != nil {
			return protocol.MakeErrReply(fmt.Sprintf("ERR invalid JSON: %v", err))
		}
		triples = append(triples, triple{
			key:   string(args[i]),
			path:  string(args[i+1]),
			value: value,
			raw:   args[i : i+3],
		})
	}
	for _, t := range triples {
		entity, exists := db.GetEntity(t.key)
		var jv *godisjson.JSONValue
		if !exists {
			jv, _ = godisjson.NewJSONValueFromString("{}")
			db.PutEntity(t.key, &database.DataEntity{Data: jv})
		} else {
			var ok bool
			jv, ok = entity.Data.(*godisjson.JSONValue)
			if !ok {
				return &protocol.WrongTypeErrReply{}
			}
		}
		if _, err := jv.Set(t.path, t.value, false, false); err != nil {
			return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
		}
	}
	db.addAof(utils.ToCmdLine3("json.mset", args...))
	return protocol.MakeOkReply()
}

// execJSONClear clears values at path (containers become empty, scalars zeroed)
// JSON.CLEAR key [path]
func execJSONClear(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 || len(args) > 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.clear' command")
	}
	key := string(args[0])
	path := "$"
	if len(args) > 1 {
		path = string(args[1])
	}
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}
	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	n, err := jv.Clear(path)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}
	if n > 0 {
		db.addAof(utils.ToCmdLine3("json.clear", args...))
	}
	return protocol.MakeIntReply(int64(n))
}

// execJSONToggle toggles a boolean at path
// JSON.TOGGLE key [path]
func execJSONToggle(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 || len(args) > 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.toggle' command")
	}
	key := string(args[0])
	path := "$"
	if len(args) > 1 {
		path = string(args[1])
	}
	entity, exists := db.GetEntity(key)
	if !exists {
		return &protocol.NullBulkReply{}
	}
	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	newVal, err := jv.Toggle(path)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}
	db.addAof(utils.ToCmdLine3("json.toggle", args...))
	if newVal {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// execJSONMerge applies JSON Merge Patch (RFC7396)
// JSON.MERGE key path value
func execJSONMerge(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.merge' command")
	}
	key := string(args[0])
	path := string(args[1])
	var patch interface{}
	if err := json.Unmarshal(args[2], &patch); err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR invalid JSON: %v", err))
	}
	entity, exists := db.GetEntity(key)
	var jv *godisjson.JSONValue
	if !exists {
		jv, _ = godisjson.NewJSONValueFromString("{}")
		db.PutEntity(key, &database.DataEntity{Data: jv})
	} else {
		var ok bool
		jv, ok = entity.Data.(*godisjson.JSONValue)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
	}
	if err := jv.Merge(path, patch); err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}
	db.addAof(utils.ToCmdLine3("json.merge", args...))
	return protocol.MakeOkReply()
}

// execJSONResp returns the value at path as RESP nested arrays
// JSON.RESP key [path]
func execJSONResp(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 || len(args) > 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.resp' command")
	}
	key := string(args[0])
	path := "$"
	if len(args) > 1 {
		path = string(args[1])
	}
	entity, exists := db.GetEntity(key)
	if !exists {
		return &protocol.NullBulkReply{}
	}
	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	val, err := jv.Get(path)
	if err != nil {
		return &protocol.NullBulkReply{}
	}
	return jsonValueToRESP(val)
}

func jsonValueToRESP(v interface{}) redis.Reply {
	switch val := v.(type) {
	case nil:
		return protocol.MakeNullBulkReply()
	case bool:
		if val {
			return protocol.MakeBulkReply([]byte("true"))
		}
		return protocol.MakeBulkReply([]byte("false"))
	case string:
		return protocol.MakeBulkReply([]byte(val))
	case float64:
		if val == float64(int64(val)) {
			return protocol.MakeIntReply(int64(val))
		}
		return protocol.MakeBulkReply([]byte(strconv.FormatFloat(val, 'f', -1, 64)))
	case float32:
		return jsonValueToRESP(float64(val))
	case int:
		return protocol.MakeIntReply(int64(val))
	case int64:
		return protocol.MakeIntReply(val)
	case []interface{}:
		replies := make([]redis.Reply, len(val))
		for i, item := range val {
			replies[i] = jsonValueToRESP(item)
		}
		return protocol.MakeMultiRawReply(replies)
	case map[string]interface{}:
		replies := make([]redis.Reply, 0, len(val)*2)
		for k, item := range val {
			replies = append(replies, protocol.MakeBulkReply([]byte(k)), jsonValueToRESP(item))
		}
		return protocol.MakeMultiRawReply(replies)
	default:
		b, err := json.Marshal(val)
		if err != nil {
			return protocol.MakeNullBulkReply()
		}
		return protocol.MakeBulkReply(b)
	}
}

// execJSONDebug handles JSON.DEBUG MEMORY|FIELDS|HELP
func execJSONDebug(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.debug' command")
	}
	sub := strings.ToUpper(string(args[0]))
	switch sub {
	case "HELP":
		help := []string{
			"JSON.DEBUG HELP",
			"JSON.DEBUG MEMORY key [path]",
			"JSON.DEBUG FIELDS key [path]",
		}
		replies := make([]redis.Reply, len(help))
		for i, h := range help {
			replies[i] = protocol.MakeBulkReply([]byte(h))
		}
		return protocol.MakeMultiRawReply(replies)
	case "MEMORY":
		if len(args) < 2 || len(args) > 3 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'json.debug|memory' command")
		}
		key := string(args[1])
		path := "$"
		if len(args) == 3 {
			path = string(args[2])
		}
		entity, exists := db.GetEntity(key)
		if !exists {
			return protocol.MakeIntReply(0)
		}
		jv, ok := entity.Data.(*godisjson.JSONValue)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
		n, err := jv.DebugMemory(path)
		if err != nil {
			return protocol.MakeIntReply(0)
		}
		return protocol.MakeIntReply(int64(n))
	case "FIELDS":
		if len(args) < 2 || len(args) > 3 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'json.debug|fields' command")
		}
		key := string(args[1])
		path := "$"
		if len(args) == 3 {
			path = string(args[2])
		}
		entity, exists := db.GetEntity(key)
		if !exists {
			return protocol.MakeIntReply(0)
		}
		jv, ok := entity.Data.(*godisjson.JSONValue)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
		n, err := jv.DebugFields(path)
		if err != nil {
			return protocol.MakeIntReply(0)
		}
		return protocol.MakeIntReply(int64(n))
	default:
		return protocol.MakeErrReply("ERR unknown subcommand '" + string(args[0]) + "'")
	}
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

// undoJSONKey restores JSON keys via DEL + GODIS.RESTORE (opaque) for MULTI rollback.
func undoJSONKey(db *DB, args [][]byte) []CmdLine {
	return rollbackFirstKey(db, args)
}

func prepareJSONMGet(args [][]byte) ([]string, []string) {
	if len(args) < 2 {
		return nil, nil
	}
	keys := make([]string, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys[i] = string(args[i])
	}
	return nil, keys
}

func prepareJSONMSet(args [][]byte) ([]string, []string) {
	keys := make([]string, 0, len(args)/3)
	for i := 0; i+2 < len(args); i += 3 {
		keys = append(keys, string(args[i]))
	}
	return keys, nil
}

func prepareJSONDebug(args [][]byte) ([]string, []string) {
	if len(args) < 2 {
		return nil, nil
	}
	sub := strings.ToUpper(string(args[0]))
	if sub == "MEMORY" || sub == "FIELDS" {
		return nil, []string{string(args[1])}
	}
	return nil, nil
}

func init() {
	registerCommand("JSON.Set", execJSONSet, prepareJSONKey, undoJSONKey, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("JSON.Get", execJSONGet, prepareJSONKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("JSON.Del", execJSONDel, prepareJSONKey, undoJSONKey, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.Forget", execJSONDel, prepareJSONKey, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.Type", execJSONType, prepareJSONKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("JSON.NumIncrBy", execJSONNumIncrBy, prepareJSONKey, nil, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.StrAppend", execJSONStrAppend, prepareJSONKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.ArrAppend", execJSONArrAppend, prepareJSONKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.ArrInsert", execJSONArrInsert, prepareJSONKey, nil, -5, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.MGet", execJSONMGet, prepareJSONMGet, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, -2, 1)
	registerCommand("JSON.MSet", execJSONMSet, prepareJSONMSet, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, -1, 3)
	registerCommand("JSON.ArrLen", execJSONArrLen, prepareJSONKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("JSON.Clear", execJSONClear, prepareJSONKey, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.Toggle", execJSONToggle, prepareJSONKey, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.Merge", execJSONMerge, prepareJSONKey, nil, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("JSON.Resp", execJSONResp, prepareJSONKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("JSON.Debug", execJSONDebug, prepareJSONDebug, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
	registerCommand("JSON.ObjKeys", execJSONObjKeys, prepareJSONKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("JSON.ObjLen", execJSONObjLen, prepareJSONKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
