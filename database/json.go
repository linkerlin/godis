package database

import (
	"bytes"
	"encoding/json"
	"errors"
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
// parseOrderedJSONArg decodes a JSON command argument into the ordered
// representation (objects keep document order; integers stay int64).
func parseOrderedJSONArg(data []byte) (interface{}, error) {
	jv, err := godisjson.NewJSONValueFromBytes(data)
	if err != nil {
		return nil, err
	}
	return jv.Data(), nil
}

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

	// Parse the JSON value (serde_json-style error texts, real RedisJSON)
	if err := godisjson.ValidateSerdeJSON(args[2]); err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	value, err := parseOrderedJSONArg(args[2])
	if err != nil {
		return protocol.MakeErrReply(err.Error())
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
		case "FPHA":
			// 8.8 homogeneous float-array storage hint: Godis has no such
			// encoding — reject explicitly instead of accepting and ignoring.
			return protocol.MakeErrReply("ERR FPHA is not supported: Godis JSON has no homogeneous float array encoding")
		default:
			return &protocol.SyntaxErrReply{}
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

	// Set the value. Real 8.10: an unreachable `$` path (e.g. `$.a.b`
	// where a is a scalar) silently replies null instead of an error.
	ok, err := jv.Set(path, value, nx, xx)
	if err != nil {
		if strings.HasPrefix(path, "$") {
			return &protocol.NullBulkReply{}
		}
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}

	if !ok {
		return &protocol.NullBulkReply{}
	}

	db.addAof(utils.ToCmdLine3("json.set", args...))
	reindexJSON(db, key)
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

	// No path: RedisJSON returns the root document unwrapped.
	if len(paths) == 0 {
		val, err := jv.Get("$")
		if err != nil {
			return &protocol.NullBulkReply{}
		}
		result, err := marshalJSONGet(val, opts)
		if err != nil {
			return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
		}
		return protocol.MakeBulkReply(result)
	}

	if len(paths) == 1 {
		val, err := jv.Get(paths[0])
		if err != nil {
			// Filter/grammar syntax errors are real errors; a missing `$`
			// path replies an empty array, a missing legacy path errors
			// "ERR Path does not exist" (probed against 8.10).
			if errors.Is(err, godisjson.ErrJSONPathSyntax) {
				return protocol.MakeErrReply(err.Error())
			}
			if strings.HasPrefix(paths[0], "$") {
				return protocol.MakeBulkReply([]byte("[]"))
			}
			return protocol.MakeErrReply("ERR Path does not exist")
		}
		if !godisjson.PathHasFilter(paths[0]) && !godisjson.PathHasNodelist(paths[0]) {
			val = wrapEnhancedJSONPathValue(paths[0], val)
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
		if !godisjson.PathHasFilter(path) && !godisjson.PathHasNodelist(path) {
			val = wrapEnhancedJSONPathValue(path, val)
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

// wrapEnhancedJSONPathValue wraps RedisJSON `$…` path results in a JSON array.
func wrapEnhancedJSONPathValue(path string, val interface{}) interface{} {
	if strings.HasPrefix(path, "$") {
		return []interface{}{val}
	}
	return val
}

// wrapEnhancedJSONNumReply returns a bulk number, or `[n]` for RedisJSON `$…` paths.
func wrapEnhancedJSONNumReply(path string, newVal float64) redis.Reply {
	s := strconv.FormatFloat(newVal, 'g', -1, 64)
	if strings.HasPrefix(path, "$") {
		return protocol.MakeBulkReply([]byte("[" + s + "]"))
	}
	return protocol.MakeBulkReply([]byte(s))
}

// wrapEnhancedJSONIntReply wraps RedisJSON `$…` length results in an array of integers.
func wrapEnhancedJSONIntReply(path string, wrap bool, n int64) redis.Reply {
	if wrap && strings.HasPrefix(path, "$") {
		return protocol.MakeMultiRawReply([]redis.Reply{protocol.MakeIntReply(n)})
	}
	return protocol.MakeIntReply(n)
}

// renderMatchValues renders real-8.10 per-match replies: one match → the
// bare value; several → an array (nil → null bulk, integers → integer,
// everything else → its JSON serialization).
func renderMatchValues(results []interface{}) redis.Reply {
	if len(results) == 1 {
		return matchValueReply(results[0])
	}
	replies := make([]redis.Reply, len(results))
	for i, v := range results {
		replies[i] = matchValueReply(v)
	}
	return protocol.MakeMultiRawReply(replies)
}

func matchValueReply(v interface{}) redis.Reply {
	switch t := v.(type) {
	case nil:
		return &protocol.NullBulkReply{}
	case int:
		return protocol.MakeIntReply(int64(t))
	case int64:
		return protocol.MakeIntReply(t)
	default:
		data, err := godisjson.MarshalValue(t)
		if err != nil {
			return &protocol.NullBulkReply{}
		}
		return protocol.MakeBulkReply(data)
	}
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
		removeJSONFromIndex(db, key)
	} else if ok {
		reindexJSON(db, key)
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
	explicitPath := false
	if len(args) > 1 {
		path = string(args[1])
		explicitPath = true
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

	if explicitPath && strings.HasPrefix(path, "$") {
		return protocol.MakeMultiBulkReply([][]byte{[]byte(typ)})
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
		return protocol.MakeErrReply("ERR could not perform this operation on a key that doesn't exist")
	}

	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	// Increment
	newVals, err := jv.NumIncrBy(path, increment)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	db.addAof(utils.ToCmdLine3("json.numincrby", args...))
	reindexJSON(db, key)
	return renderNumResults(path, newVals)
}

// renderNumResults serializes NUMINCRBY/NUMMULTBY replies the way real does:
// `$` paths reply one bulk JSON array of per-match results (integers bare,
// whole floats with `.0`, null for non-numbers); a legacy path replies the
// bare serialized number.
func renderNumResults(path string, results []interface{}) redis.Reply {
	if godisjson.IsLegacyJSONPath(path) {
		if len(results) == 0 {
			return &protocol.NullBulkReply{}
		}
		data, err := godisjson.MarshalValue(results[0])
		if err != nil {
			return protocol.MakeErrReply("ERR " + err.Error())
		}
		return protocol.MakeBulkReply(data)
	}
	data, err := godisjson.MarshalValue(results)
	if err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}
	return protocol.MakeBulkReply(data)
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

	// Real checks the target type before the value: a non-string match
	// contributes null regardless of the appended value's validity; when at
	// least one string matches, the value must parse as a JSON string.
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeErrReply("ERR could not perform this operation on a key that doesn't exist")
	}

	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	if godisjson.IsLegacyJSONPath(path) {
		cur, gerr := jv.Get(path)
		if gerr != nil {
			return &protocol.NullBulkReply{}
		}
		if _, isStr := cur.(string); !isStr {
			return &protocol.NullBulkReply{}
		}
		if err := godisjson.ValidateSerdeJSON(value); err != nil {
			return protocol.MakeErrReply("ERR " + err.Error())
		}
		parsed, perr := parseOrderedJSONArg(value)
		if perr != nil {
			return protocol.MakeErrReply("ERR " + perr.Error())
		}
		strVal, isStr := parsed.(string)
		if !isStr {
			return protocol.MakeErrReply("WRONGTYPE wrong type of path value - expected string")
		}
		newLen, err := jv.StrAppend(path, strVal)
		if err != nil {
			return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
		}
		db.addAof(utils.ToCmdLine3("json.strappend", args...))
		reindexJSON(db, key)
		return protocol.MakeIntReply(int64(newLen))
	}

	hasString := false
	for _, m := range jv.StrLenMatches(path) {
		if m != nil {
			hasString = true
			break
		}
	}
	if !hasString {
		return &protocol.NullBulkReply{}
	}
	if err := godisjson.ValidateSerdeJSON(value); err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}
	parsed, perr := parseOrderedJSONArg(value)
	if perr != nil {
		return protocol.MakeErrReply("ERR " + perr.Error())
	}
	strVal, isStr := parsed.(string)
	if !isStr {
		return protocol.MakeErrReply("WRONGTYPE wrong type of path value - expected string")
	}
	results := jv.StrAppendMatches(path, strVal)
	db.addAof(utils.ToCmdLine3("json.strappend", args...))
	reindexJSON(db, key)
	return renderMatchValues(results)
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

	// Parse values (ordered: integers stay int64 so popped elements render
	// bare like real — probed: ARRPOP of an ARRINSERTed 9 replies "9").
	values := make([]interface{}, len(args)-2)
	for i := 2; i < len(args); i++ {
		if reply := validateBulkBytes(args[i]); reply != nil {
			return reply
		}
		val, err := parseOrderedJSONArg(args[i])
		if err != nil {
			val = string(args[i])
		}
		values[i-2] = val
	}

	// Get JSON value
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeErrReply("ERR could not perform this operation on a key that doesn't exist")
	}

	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	// Append
	if godisjson.IsLegacyJSONPath(path) {
		newLen, err := jv.ArrAppend(path, values...)
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		db.addAof(utils.ToCmdLine3("json.arrappend", args...))
		reindexJSON(db, key)
		return protocol.MakeIntReply(int64(newLen))
	}
	results := jv.ArrAppendMatches(path, values...)
	changed := false
	for _, r := range results {
		if r != nil {
			changed = true
			break
		}
	}
	if changed {
		db.addAof(utils.ToCmdLine3("json.arrappend", args...))
		reindexJSON(db, key)
	}
	return renderMatchValues(results)
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
		val, err := parseOrderedJSONArg(args[i])
		if err != nil {
			val = string(args[i])
		}
		values[i-3] = val
	}
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeErrReply("ERR could not perform this operation on a key that doesn't exist")
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
	reindexJSON(db, key)
	return wrapEnhancedJSONIntReply(path, true, int64(newLen))
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
		return protocol.MakeErrReply("ERR could not perform this operation on a key that doesn't exist")
	}

	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	if godisjson.IsLegacyJSONPath(path) {
		length, err := jv.ArrLen(path)
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		return protocol.MakeIntReply(int64(length))
	}
	return renderMatchValues(jv.ArrLenMatches(path))
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
		// Real replies the bare serde_json parse error (probed: `{` ->
		// "EOF while parsing an object at line 1 column 1").
		if err := godisjson.ValidateSerdeJSON(args[i+2]); err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		value, err := parseOrderedJSONArg(args[i+2])
		if err != nil {
			return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
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
	// MSet may touch several keys; reindex each into matching FT indexes.
	for _, t := range triples {
		reindexJSON(db, t.key)
	}
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
	n, err := jv.Clear(path)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}
	if n > 0 {
		db.addAof(utils.ToCmdLine3("json.clear", args...))
		reindexJSON(db, key)
	}
	return wrapEnhancedJSONIntReply(path, explicitPath, int64(n))
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
		return protocol.MakeErrReply("ERR could not perform this operation on a key that doesn't exist")
	}
	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	results, err := jv.Toggle(path)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	db.addAof(utils.ToCmdLine3("json.toggle", args...))
	reindexJSON(db, key)
	if !strings.HasPrefix(path, "$") {
		// Legacy path: the new value as a bulk JSON string ("true"/"false").
		if b, ok := results[0].(bool); ok && b {
			return protocol.MakeBulkReply([]byte("true"))
		}
		return protocol.MakeBulkReply([]byte("false"))
	}
	replies := make([]redis.Reply, len(results))
	for i, r := range results {
		if n, ok := r.(int64); ok {
			replies[i] = protocol.MakeIntReply(n)
		} else {
			replies[i] = &protocol.NullBulkReply{}
		}
	}
	return protocol.MakeMultiRawReply(replies)
}

// execJSONMerge applies JSON Merge Patch (RFC7396)
// JSON.MERGE key path value
func execJSONMerge(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'json.merge' command")
	}
	key := string(args[0])
	path := string(args[1])
	patch, err := parseOrderedJSONArg(args[2])
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
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
	reindexJSON(db, key)
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
		// Real RedisJSON RESP form: `[` prefix, then elements (no close).
		replies := make([]redis.Reply, 0, len(val)+1)
		replies = append(replies, protocol.MakeBulkReply([]byte("[")))
		for _, item := range val {
			replies = append(replies, jsonValueToRESP(item))
		}
		return protocol.MakeMultiRawReply(replies)
	case *godisjson.OrderedObj:
		// Real RedisJSON RESP form: `{` prefix, then key/value pairs in
		// document order (no close).
		replies := make([]redis.Reply, 0, val.Len()*2+1)
		replies = append(replies, protocol.MakeBulkReply([]byte("{")))
		for _, k := range val.Keys() {
			v, _ := val.Get(k)
			replies = append(replies, protocol.MakeBulkReply([]byte(k)), jsonValueToRESP(v))
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
			"MEMORY <key> [path] - reports memory usage",
			"HELP                - this message",
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
		explicitPath := false
		if len(args) == 3 {
			path = string(args[2])
			explicitPath = true
		}
		entity, exists := db.GetEntity(key)
		if !exists {
			return wrapEnhancedJSONIntReply(path, explicitPath, 0)
		}
		jv, ok := entity.Data.(*godisjson.JSONValue)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
		n, err := jv.DebugMemory(path)
		if err != nil {
			return wrapEnhancedJSONIntReply(path, explicitPath, 0)
		}
		return wrapEnhancedJSONIntReply(path, explicitPath, int64(n))
	case "FIELDS":
		if len(args) < 2 || len(args) > 3 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'json.debug|fields' command")
		}
		key := string(args[1])
		path := "$"
		explicitPath := false
		if len(args) == 3 {
			path = string(args[2])
			explicitPath = true
		}
		entity, exists := db.GetEntity(key)
		if !exists {
			return wrapEnhancedJSONIntReply(path, explicitPath, 0)
		}
		jv, ok := entity.Data.(*godisjson.JSONValue)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
		n, err := jv.DebugFields(path)
		if err != nil {
			return wrapEnhancedJSONIntReply(path, explicitPath, 0)
		}
		return wrapEnhancedJSONIntReply(path, explicitPath, int64(n))
	default:
		return protocol.MakeErrReply("ERR unknown subcommand - try `JSON.DEBUG HELP`")
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

	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeErrReply("ERR could not perform this operation on a key that doesn't exist")
	}

	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	if godisjson.IsLegacyJSONPath(path) {
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
	keys, ok := jv.ObjKeysMatches(path)
	if !ok {
		return &protocol.NullBulkReply{}
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
	explicitPath := false
	if len(args) > 1 {
		path = string(args[1])
		explicitPath = true
	}

	// Get JSON value
	entity, exists := db.GetEntity(key)
	if !exists {
		if !explicitPath {
			return &protocol.NullBulkReply{}
		}
		return protocol.MakeErrReply("ERR Path does not exist or not an object")
	}

	jv, ok := entity.Data.(*godisjson.JSONValue)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	if godisjson.IsLegacyJSONPath(path) {
		length, err := jv.ObjLen(path)
		if err != nil {
			// Real: a missing path on OBJLEN replies nil (probed).
			if strings.Contains(err.Error(), "Path does not exist") {
				return protocol.MakeNullBulkReply()
			}
			return protocol.MakeErrReply(err.Error())
		}
		return protocol.MakeIntReply(int64(length))
	}
	results := jv.ObjLenMatches(path)
	if len(results) == 0 {
		return &protocol.NullBulkReply{}
	}
	return renderMatchValues(results)
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
