package database

import (
	"fmt"
	"strings"

	"github.com/hdt3213/godis/functions"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
)

// Global functions engine
var funcEngine *functions.Engine

// InitFunctionsEngine initializes the functions engine
func InitFunctionsEngine() {
	funcEngine = functions.NewEngine(10)
}

// execFunctionLoad loads a library
// FUNCTION LOAD [REPLACE] "library code"
func execFunctionLoad(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'function load' command")
	}
	
	if funcEngine == nil {
		InitFunctionsEngine()
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
	
	// Extract library name from code
	// Format: #!lua name=mylib
	libName := extractLibraryName(code)
	if libName == "" {
		return protocol.MakeErrReply("ERR Library name not specified (use '#!lua name=libname' shebang)")
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
	
	if funcEngine == nil {
		return protocol.MakeOkReply()
	}
	
	// ASYNC/SYNC ignored for now
	
	if err := funcEngine.FlushAll(); err != nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR %v", err))
	}
	
		cmdLine := append([][]byte{[]byte("function"), []byte("flush")}, args...)
	db.addAof(cmdLine)
	return protocol.MakeOkReply()
}

// execFunctionKill kills a running function (not implemented)
// FUNCTION KILL
func execFunctionKill(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'function kill' command")
	}
	
	// Not implemented - would need execution tracking
	return protocol.MakeErrReply("ERR not implemented")
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
	
	reply := [][]byte{
		[]byte("running_script"),
		protocol.MakeEmptyMultiBulkReply().ToBytes(),
		[]byte("engines"),
		protocol.MakeMultiBulkReply(engines).ToBytes(),
	}
	
	return protocol.MakeMultiBulkReply(reply)
}

// execFunctionDump dumps all functions
// FUNCTION DUMP
func execFunctionDump(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'function dump' command")
	}
	
	if funcEngine == nil {
		return protocol.MakeBulkReply([]byte{})
	}
	
	// Serialize all libraries
	var dump []byte
	for _, name := range funcEngine.ListLibraries() {
		lib, _ := funcEngine.GetLibrary(name)
		if lib != nil {
			dump = append(dump, []byte(lib.Code)...)
			dump = append(dump, '\n')
		}
	}
	
	return protocol.MakeBulkReply(dump)
}

// execFunctionRestore restores functions from dump
// FUNCTION RESTORE payload [FLUSH|APPEND|REPLACE]
func execFunctionRestore(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'function restore' command")
	}
	
	if funcEngine == nil {
		InitFunctionsEngine()
	}
	
	// Parse policy
	policy := "APPEND"
	if len(args) > 1 {
		policy = strings.ToUpper(string(args[1]))
	}
	
	payload := string(args[0])
	
	// Parse payload (libraries separated by shebang)
	// Split by "#!lua name="
	
	if policy == "FLUSH" {
		funcEngine.FlushAll()
	}
	
	// Simple parsing - split by shebang and load each library
	libs := parseLibraryDump(payload)
	
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
	// Parse shebang: #!lua name=libname
	lines := strings.Split(code, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#!lua") {
			// Parse name= value
			parts := strings.Fields(line)
			for _, part := range parts {
				if strings.HasPrefix(part, "name=") {
					return strings.TrimPrefix(part, "name=")
				}
			}
		}
	}
	return ""
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
	// Initialize functions engine
	InitFunctionsEngine()
	
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
