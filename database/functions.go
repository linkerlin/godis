package database

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/linkerlin/godis/functions"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// Global functions engine
var funcEngine *functions.Engine

// InitFunctionsEngine initializes the functions engine
func InitFunctionsEngine(db *DB) {
	funcDB = db
	if funcEngine != nil {
		if db != nil {
			funcEngine.SetDBExec(makeFuncDBExec())
		}
		return
	}
	funcEngine = functions.NewEngine(10)
	if db != nil {
		funcEngine.SetDBExec(makeFuncDBExec())
	}
}

// funcDB is the DB currently executing FCALL (redis.call target).
var funcDB *DB

func makeFuncDBExec() func(cmd string, args ...string) (interface{}, error) {
	return func(cmd string, args ...string) (interface{}, error) {
		cmdLine := make([][]byte, 0, len(args)+1)
		cmdLine = append(cmdLine, []byte(cmd))
		for _, arg := range args {
			cmdLine = append(cmdLine, []byte(arg))
		}
		target := funcDB
		if target == nil {
			return nil, fmt.Errorf("ERR functions engine not bound to a database")
		}
		// Nested redis.call must not re-lock (FCALL holds keys via prepare).
		result := target.execWithLock(nil, cmdLine)
		if errReply, ok := result.(*protocol.StandardErrReply); ok {
			return nil, fmt.Errorf("%s", errReply.Status)
		}
		return redisReplyToGo(result), nil
	}
}

// execFunctionLoad loads a library
// FUNCTION LOAD [REPLACE] "library code"
func execFunctionLoad(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'function load' command")
	}

	if funcEngine == nil {
		InitFunctionsEngine(db)
	}

	replace := false
	codeIdx := 0

	if strings.ToUpper(string(args[0])) == "REPLACE" {
		replace = true
		if len(args) < 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'function load' command")
		}
		codeIdx = 1
	}

	code := string(args[codeIdx])
	if reply := validateBulkBytes(args[codeIdx]); reply != nil {
		return reply
	}

	libName, errReply := parseFunctionShebang(code)
	if errReply != nil {
		return errReply
	}

	// Load library
	numFuncs, err := funcEngine.LoadLibrary(libName, code, replace)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}

	cmdLine := append([][]byte{[]byte("function"), []byte("load")}, args...)
	db.addAof(cmdLine)
	return protocol.MakeBulkReply([]byte(fmt.Sprintf("%s:%d", libName, numFuncs)))
}

// execFunctionList lists functions/libraries
// FUNCTION LIST [LIBRARYNAME library_name] [WITHCODE]
func execFunctionList(db *DB, args [][]byte) redis.Reply {
	if funcEngine == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	libName := ""
	withCode := false

	for i := 0; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))

		switch arg {
		case "LIBRARYNAME":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			libName = string(args[i+1])
			i++
		case "WITHCODE":
			withCode = true
		}
	}

	var reply [][]byte

	if libName != "" {
		// List specific library
		lib, exists := funcEngine.GetLibrary(libName)
		if !exists {
			return protocol.MakeEmptyMultiBulkReply()
		}

		reply = append(reply, formatLibraryInfo(lib, withCode)...)
	} else {
		// List all libraries
		for _, name := range funcEngine.ListLibraries() {
			lib, _ := funcEngine.GetLibrary(name)
			if lib != nil {
				reply = append(reply, formatLibraryInfo(lib, withCode)...)
			}
		}
	}

	return protocol.MakeMultiBulkReply(reply)
}

// execFunctionDelete deletes a library
// FUNCTION DELETE library_name
func execFunctionDelete(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'function delete' command")
	}

	if funcEngine == nil {
		return protocol.MakeOkReply()
	}

	libName := string(args[0])

	if err := funcEngine.DeleteLibrary(libName); err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}

	cmdLine := append([][]byte{[]byte("function"), []byte("delete")}, args...)
	db.addAof(cmdLine)
	return protocol.MakeOkReply()
}

// execFunctionFlush flushes all functions
// FUNCTION FLUSH [ASYNC|SYNC]
func execFunctionFlush(db *DB, args [][]byte) redis.Reply {
	if len(args) > 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'function flush' command")
	}
	if len(args) == 1 {
		mode := strings.ToUpper(string(args[0]))
		if mode != "ASYNC" && mode != "SYNC" {
			return protocol.MakeErrReply("ERR FUNCTION FLUSH only supports SYNC|ASYNC mode")
		}
	}

	if funcEngine == nil {
		return protocol.MakeOkReply()
	}

	if err := funcEngine.FlushAll(); err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}

	cmdLine := append([][]byte{[]byte("function"), []byte("flush")}, args...)
	db.addAof(cmdLine)
	return protocol.MakeOkReply()
}

// execFunctionKill kills a running function
// FUNCTION KILL
func execFunctionKill(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'function kill' command")
	}

	if funcEngine == nil {
		return protocol.MakeErrReply("ERR no functions engine")
	}

	if err := funcEngine.KillRunningFunction(); err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %s", err))
	}

	return protocol.MakeOkReply()
}

// execFunctionStats returns function statistics
// FUNCTION STATS
func execFunctionStats(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'function stats' command")
	}

	if funcEngine == nil {
		return protocol.MakeMultiBulkReply([][]byte{
			[]byte("running_script"),
			protocol.MakeEmptyMultiBulkReply().ToBytes(),
			[]byte("engines"),
			protocol.MakeEmptyMultiBulkReply().ToBytes(),
		})
	}

	stats := funcEngine.Stats()

	var engines [][]byte
	engines = append(engines, []byte("lua"))
	engines = append(engines, protocol.MakeMultiBulkReply([][]byte{
		[]byte("libraries_count"),
		[]byte(fmt.Sprintf("%d", stats["libraries"])),
		[]byte("functions_count"),
		[]byte(fmt.Sprintf("%d", stats["functions"])),
	}).ToBytes())

	// Check for running function
	var runningScriptReply redis.Reply = protocol.MakeEmptyMultiBulkReply()
	if running := funcEngine.GetRunningFunction(); running != nil {
		runningScriptReply = protocol.MakeMultiBulkReply([][]byte{
			[]byte("name"),
			[]byte(running.Name),
			[]byte("command"),
			[]byte(fmt.Sprintf("FCALL %s", running.Name)),
		})
	}

	reply := [][]byte{
		[]byte("running_script"),
		runningScriptReply.ToBytes(),
		[]byte("engines"),
		protocol.MakeMultiBulkReply(engines).ToBytes(),
	}

	return protocol.MakeMultiBulkReply(reply)
}

// Godis FUNCTION DUMP envelope (godis-internal; not Redis wire-compatible).
// Format: magic "GODISFN1" || u32be count || repeated (u32be nameLen||name||u32be engLen||eng||u32be codeLen||code)
var functionDumpMagic = []byte("GODISFN1")

// execFunctionDump dumps all functions
// FUNCTION DUMP
func execFunctionDump(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'function dump' command")
	}

	if funcEngine == nil {
		return protocol.MakeBulkReply([]byte{})
	}

	names := funcEngine.ListLibraries()
	var body []byte
	body = append(body, functionDumpMagic...)
	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], uint32(len(names)))
	body = append(body, countBuf[:]...)
	for _, name := range names {
		lib, _ := funcEngine.GetLibrary(name)
		if lib == nil {
			continue
		}
		eng := lib.Engine
		if eng == "" {
			eng = "LUA"
		}
		body = appendLengthPrefixed(body, []byte(lib.Name))
		body = appendLengthPrefixed(body, []byte(eng))
		body = appendLengthPrefixed(body, []byte(lib.Code))
	}

	return protocol.MakeBulkReply(body)
}

func appendLengthPrefixed(dst, val []byte) []byte {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(val)))
	dst = append(dst, n[:]...)
	return append(dst, val...)
}

func readLengthPrefixed(src []byte, off int) (val []byte, next int, ok bool) {
	if off+4 > len(src) {
		return nil, off, false
	}
	n := int(binary.BigEndian.Uint32(src[off : off+4]))
	off += 4
	if n < 0 || off+n > len(src) {
		return nil, off, false
	}
	return src[off : off+n], off + n, true
}

func parseFunctionDumpBinary(payload []byte) (map[string]string, bool) {
	if len(payload) < len(functionDumpMagic)+4 || string(payload[:len(functionDumpMagic)]) != string(functionDumpMagic) {
		return nil, false
	}
	off := len(functionDumpMagic)
	count := int(binary.BigEndian.Uint32(payload[off : off+4]))
	off += 4
	libs := make(map[string]string, count)
	for i := 0; i < count; i++ {
		nameB, n1, ok := readLengthPrefixed(payload, off)
		if !ok {
			return nil, false
		}
		off = n1
		_, n2, ok := readLengthPrefixed(payload, off) // engine (ignored for load)
		if !ok {
			return nil, false
		}
		off = n2
		codeB, n3, ok := readLengthPrefixed(payload, off)
		if !ok {
			return nil, false
		}
		off = n3
		libs[string(nameB)] = string(codeB)
	}
	return libs, true
}

// execFunctionRestore restores functions from dump
// FUNCTION RESTORE payload [FLUSH|APPEND|REPLACE]
func execFunctionRestore(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'function restore' command")
	}

	if funcEngine == nil {
		InitFunctionsEngine(db)
	}

	// Parse policy
	policy := "APPEND"
	if len(args) > 1 {
		policy = strings.ToUpper(string(args[1]))
	}

	payload := args[0]
	if reply := validateBulkBytes(payload); reply != nil {
		return reply
	}

	if policy == "FLUSH" {
		funcEngine.FlushAll()
	}

	libs, ok := parseFunctionDumpBinary(payload)
	if !ok {
		libs = parseLibraryDump(string(payload))
	}

	for name, code := range libs {
		replace := policy == "REPLACE" || policy == "FLUSH"
		_, err := funcEngine.LoadLibrary(name, code, replace)
		if err != nil && policy != "APPEND" {
			return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
		}
	}

	cmdLine := append([][]byte{[]byte("function"), []byte("restore")}, args...)
	db.addAof(cmdLine)
	return protocol.MakeOkReply()
}

// execFCall calls a function
// FCALL function_name numkeys [key ...] [arg ...]
func execFCall(db *DB, args [][]byte) redis.Reply {
	return execFCallInternal(db, args, false)
}

// execFCallRO calls a function read-only
// FCALL_RO function_name numkeys [key ...] [arg ...]
func execFCallRO(db *DB, args [][]byte) redis.Reply {
	return execFCallInternal(db, args, true)
}

func execFCallInternal(db *DB, args [][]byte, readonly bool) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'fcall' command")
	}

	funcDB = db
	if funcEngine == nil {
		InitFunctionsEngine(db)
	}
	if funcEngine == nil {
		return protocol.MakeErrReply("ERR Redis Functions not enabled")
	}

	funcName := string(args[0])

	numKeys, err := atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR numkeys should be integer")
	}

	if len(args) < 2+numKeys {
		return protocol.MakeErrReply("ERR wrong number of arguments")
	}

	// Check function exists
	fn, exists := funcEngine.GetFunction(funcName)
	if !exists {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Function '%s' not found", funcName))
	}

	// Check readonly constraint
	if readonly && !fn.IsReadOnly() {
		return protocol.MakeErrReply("ERR function is not read-only, use FCALL instead of FCALL_RO")
	}

	// Extract keys and args
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[2+i])
	}

	argsList := make([]string, len(args)-2-numKeys)
	for i := 0; i < len(argsList); i++ {
		argsList[i] = string(args[2+numKeys+i])
	}

	// Execute function
	result, err := funcEngine.Call(funcName, keys, argsList)
	if err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}

	return goToRedisReply(result)
}

// Helper functions

func prepareFirstKey(args [][]byte) ([]string, []string) {
	// FCALL function_name numkeys [key ...] [arg ...]
	if len(args) < 2 {
		return nil, nil
	}

	numKeys, err := atoi(string(args[1]))
	if err != nil {
		return nil, nil
	}

	var keys []string
	for i := 0; i < numKeys && i+2 < len(args); i++ {
		keys = append(keys, string(args[i+2]))
	}

	return keys, nil
}

func extractLibraryName(code string) string {
	name, err := parseFunctionShebang(code)
	if err != nil {
		return ""
	}
	return name
}

// parseFunctionShebang parses #!lua name=... [api_version=1.0]
func parseFunctionShebang(code string) (string, redis.Reply) {
	lines := strings.Split(code, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "#!") {
			continue
		}
		// Expect #!lua ...
		rest := strings.TrimSpace(strings.TrimPrefix(line, "#!"))
		fields := strings.Fields(rest)
		if len(fields) == 0 || !strings.EqualFold(fields[0], "lua") {
			return "", protocol.MakeErrReply("ERR Unknown library engine")
		}
		libName := ""
		apiVersion := "1.0"
		hasAPI := false
		for _, part := range fields[1:] {
			if strings.HasPrefix(part, "name=") {
				libName = strings.TrimPrefix(part, "name=")
			} else if strings.HasPrefix(part, "api_version=") {
				apiVersion = strings.TrimPrefix(part, "api_version=")
				hasAPI = true
			}
		}
		if libName == "" {
			return "", protocol.MakeErrReply("ERR Library name not specified (use '#!lua name=libname' shebang)")
		}
		if hasAPI && apiVersion != "1.0" {
			return "", protocol.MakeErrReply("ERR Invalid API version in shebang")
		}
		return libName, nil
	}
	return "", protocol.MakeErrReply("ERR Library name not specified (use '#!lua name=libname' shebang)")
}

func formatLibraryInfo(lib *functions.Library, withCode bool) [][]byte {
	var result [][]byte

	result = append(result, []byte("library_name"))
	result = append(result, []byte(lib.Name))

	result = append(result, []byte("engine"))
	result = append(result, []byte(lib.Engine))

	result = append(result, []byte("functions"))

	var funcs [][]byte
	for _, fn := range lib.Functions {
		var funcInfo [][]byte
		funcInfo = append(funcInfo, []byte("name"))
		funcInfo = append(funcInfo, []byte(fn.Name))
		funcInfo = append(funcInfo, []byte("description"))
		funcInfo = append(funcInfo, []byte(fn.Description))
		funcInfo = append(funcInfo, []byte("flags"))

		var flags [][]byte
		for _, f := range fn.Flags {
			flags = append(flags, []byte(f))
		}
		funcInfo = append(funcInfo, protocol.MakeMultiBulkReply(flags).ToBytes())

		funcs = append(funcs, protocol.MakeMultiBulkReply(funcInfo).ToBytes())
	}
	result = append(result, protocol.MakeMultiBulkReply(funcs).ToBytes())

	if withCode {
		result = append(result, []byte("library_code"))
		result = append(result, []byte(lib.Code))
	}

	return result
}

func parseLibraryDump(payload string) map[string]string {
	libs := make(map[string]string)

	// Split by shebang
	parts := strings.Split(payload, "#!lua")

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Find name=
		lines := strings.Split(part, "\n")
		if len(lines) == 0 {
			continue
		}

		firstLine := lines[0]
		name := ""

		// Parse name= from first line
		parts := strings.Fields(firstLine)
		for _, p := range parts {
			if strings.HasPrefix(p, "name=") {
				name = strings.TrimPrefix(p, "name=")
				break
			}
		}

		if name != "" {
			code := "#!lua " + part
			libs[name] = code
		}
	}

	return libs
}

func goToRedisReply(v interface{}) redis.Reply {
	if v == nil {
		return &protocol.NullBulkReply{}
	}

	switch val := v.(type) {
	case string:
		return protocol.MakeBulkReply([]byte(val))
	case int:
		return protocol.MakeIntReply(int64(val))
	case int64:
		return protocol.MakeIntReply(val)
	case float64:
		return protocol.MakeBulkReply([]byte(fmt.Sprintf("%g", val)))
	case bool:
		if val {
			return protocol.MakeIntReply(1)
		}
		return protocol.MakeIntReply(0)
	case []interface{}:
		var elems [][]byte
		for _, elem := range val {
			r := goToRedisReply(elem)
			elems = append(elems, r.ToBytes())
		}
		return protocol.MakeMultiBulkReply(elems)
	case map[string]interface{}:
		var elems [][]byte
		for k, v := range val {
			elems = append(elems, []byte(k))
			r := goToRedisReply(v)
			elems = append(elems, r.ToBytes())
		}
		return protocol.MakeMultiBulkReply(elems)
	default:
		return protocol.MakeBulkReply([]byte(fmt.Sprintf("%v", val)))
	}
}

func atoi(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}

func init() {
	// Initialize functions engine (without DB - will be set on first use)
	InitFunctionsEngine(nil)

	registerCommand("Function|Load", execFunctionLoad, noPrepare, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("Function|List", execFunctionList, noPrepare, nil, -1, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
	registerCommand("Function|Delete", execFunctionDelete, noPrepare, nil, 2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("Function|Flush", execFunctionFlush, noPrepare, nil, -1, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("Function|Kill", execFunctionKill, noPrepare, nil, 1, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin}, 0, 0, 0)
	registerCommand("Function|Stats", execFunctionStats, noPrepare, nil, 1, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
	registerCommand("Function|Dump", execFunctionDump, noPrepare, nil, 1, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
	registerCommand("Function|Restore", execFunctionRestore, noPrepare, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("FCall", execFCall, prepareFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("FCall|RO", execFCallRO, prepareFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
