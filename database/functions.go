package database

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"unicode/utf8"

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
		if protocol.IsErrorReply(result) {
			if er, ok := result.(error); ok {
				return nil, er
			}
			return nil, fmt.Errorf("%s", strings.TrimRight(string(result.ToBytes()), "\r\n"))
		}
		return redisReplyToGo(result, cmd, args...), nil
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

// execFunctionList lists functions/libraries.
// RESP2: array of flat field/value arrays (via MapReply.ToBytes per library).
// RESP3: array of Maps (one Map per library; nested function Maps).
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
				return protocol.MakeErrReply("ERR library name argument was not given")
			}
			libName = string(args[i+1])
			i++
		case "WITHCODE":
			withCode = true
		default:
			return protocol.MakeErrReply("ERR Unknown argument " + string(args[i]))
		}
	}

	var replies []redis.Reply
	if libName != "" {
		lib, exists := funcEngine.GetLibrary(libName)
		if !exists {
			return protocol.MakeEmptyMultiBulkReply()
		}
		replies = append(replies, formatLibraryInfoReply(lib, withCode))
	} else {
		for _, name := range funcEngine.ListLibraries() {
			lib, _ := funcEngine.GetLibrary(name)
			if lib != nil {
				replies = append(replies, formatLibraryInfoReply(lib, withCode))
			}
		}
	}
	if len(replies) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	return protocol.MakeMultiRawReply(replies)
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
		return protocol.MakeErrReply("ERR Library not found")
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
			return protocol.MakeErrReply("ERR FUNCTION FLUSH only supports SYNC|ASYNC option")
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
		return protocol.MakeErrReply("NOTBUSY No scripts in execution right now.")
	}

	if err := funcEngine.KillRunningFunction(); err != nil {
		return protocol.MakeErrReply("NOTBUSY No scripts in execution right now.")
	}

	return protocol.MakeOkReply()
}

// execFunctionStats returns function statistics
// FUNCTION STATS — RESP3 Map; RESP2 flat field array via MapReply.ToBytes.
func execFunctionStats(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'function stats' command")
	}

	m := protocol.MakeMapReply()
	if funcEngine == nil {
		m.Put("running_script", protocol.MakeNullBulkReply())
		m.Put("engines", protocol.MakeMapReply())
		return m
	}

	stats := funcEngine.Stats()
	lua := protocol.MakeMapReply()
	libCount, _ := stats["libraries"].(int)
	fnCount, _ := stats["functions"].(int)
	lua.Put("libraries_count", protocol.MakeIntReply(int64(libCount)))
	lua.Put("functions_count", protocol.MakeIntReply(int64(fnCount)))
	engines := protocol.MakeMapReply()
	engines.Put("LUA", lua)
	m.Put("engines", engines)

	if running := funcEngine.GetRunningFunction(); running != nil {
		rs := protocol.MakeMapReply()
		rs.Put("name", protocol.MakeBulkReply([]byte(running.Name)))
		rs.Put("command", protocol.MakeMultiBulkReply([][]byte{
			[]byte("FCALL"), []byte(running.Name),
		}))
		rs.Put("duration_ms", protocol.MakeIntReply(0))
		m.Put("running_script", rs)
	} else {
		m.Put("running_script", protocol.MakeNullBulkReply())
	}
	return m
}

// Godis FUNCTION DUMP envelope (godis-internal; not Redis wire-compatible).
// Format: magic "GODISFN1" || u32be count || repeated (u32be nameLen||name||u32be engLen||eng||u32be codeLen||code)
// Official Redis FUNCTION DUMP/RESTORE binary interchange is a non-goal; keep GODISFN1 only.
// RESTORE also accepts the legacy plain-text library dump for older Godis payloads.
// Corrupt GODISFN1 (truncated / bad lengths) returns ERR — never silent fallthrough.
// Official Redis dumps (RDB opcode 0xF5/0xF6, or REDIS#### RDB magic) and other
// non-GODISFN1 binary blobs return a clear non-interop ERR — never forged accept.
var functionDumpMagic = []byte("GODISFN1")

// Redis rdb.h: RDB_OPCODE_FUNCTION2=245, RDB_OPCODE_FUNCTION_PRE_GA=246.
// Official FUNCTION DUMP payloads start with one of these opcodes (then RDB-encoded
// libraries + 2-byte RDB version + 8-byte CRC64). We detect the leading byte only.
const (
	rdbOpcodeFunction2     byte = 0xF5
	rdbOpcodeFunctionPreGA byte = 0xF6
)

const errFunctionRestoreOfficialRedis = "ERR FUNCTION RESTORE rejects Redis official FUNCTION DUMP (RDB opcode 0xF5/0xF6 or REDIS####); Godis uses GODISFN1 only - no forged interop"
const errFunctionRestoreForeignBinary = "ERR FUNCTION RESTORE expects Godis GODISFN1 dump (not Redis official FUNCTION DUMP binary)"
const errFunctionRestoreCorruptGodis = "ERR FUNCTION RESTORE payload is truncated or corrupt (GODISFN1; Godis-internal format, not Redis official)"

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

// parseFunctionDumpBinaryStrict parses a GODISFN1 envelope.
// ok=false means truncated/corrupt GODISFN1 (caller must ERR, not fall through).
func parseFunctionDumpBinaryStrict(payload []byte) (libs map[string]string, ok bool) {
	if !bytes.HasPrefix(payload, functionDumpMagic) {
		return nil, false
	}
	if len(payload) < len(functionDumpMagic)+4 {
		return nil, false
	}
	off := len(functionDumpMagic)
	count := int(binary.BigEndian.Uint32(payload[off : off+4]))
	off += 4
	if count < 0 {
		return nil, false
	}
	libs = make(map[string]string, count)
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
	// Trailing garbage after a valid record list is treated as corrupt.
	if off != len(payload) {
		return nil, false
	}
	return libs, true
}

// looksLikeRedisOfficialFunctionDump detects Redis wire FUNCTION DUMP headers.
// Official format (redis/src/functions.c createFunctionDumpPayload): payload begins
// with RDB_OPCODE_FUNCTION2 (0xF5) or PRE_GA (0xF6); full RDB mistaken as dump
// often starts with "REDIS" + 4-digit version. Detection is prefix-only — we do
// not parse or accept the body.
func looksLikeRedisOfficialFunctionDump(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	switch payload[0] {
	case rdbOpcodeFunction2, rdbOpcodeFunctionPreGA:
		return true
	}
	// Full RDB header (REDIS0009 … REDIS0012+) — not Godis GODISFN1.
	if len(payload) >= 9 && bytes.HasPrefix(payload, []byte("REDIS")) {
		ver := payload[5:9]
		if ver[0] >= '0' && ver[0] <= '9' &&
			ver[1] >= '0' && ver[1] <= '9' &&
			ver[2] >= '0' && ver[2] <= '9' &&
			ver[3] >= '0' && ver[3] <= '9' {
			return true
		}
	}
	return false
}

// looksLikeNonGodisBinary rejects opaque binary that is neither GODISFN1 nor
// legacy text library dump — including Redis official FUNCTION DUMP bytes.
func looksLikeNonGodisBinary(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	if bytes.HasPrefix(payload, functionDumpMagic) {
		return false
	}
	if looksLikeRedisOfficialFunctionDump(payload) {
		return true
	}
	// NUL or invalid UTF-8 ⇒ not legacy shebang text.
	if bytes.IndexByte(payload, 0) >= 0 || !utf8.Valid(payload) {
		return true
	}
	// High ratio of non-printable (excluding tab/LF/CR) ⇒ binary.
	nonPrint := 0
	for _, b := range payload {
		if b < 0x20 && b != '\t' && b != '\n' && b != '\r' {
			nonPrint++
		} else if b > 0x7e {
			nonPrint++
		}
	}
	return nonPrint*4 > len(payload) // >25% control/hi bytes
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
		switch policy {
		case "FLUSH", "APPEND", "REPLACE":
		default:
			return protocol.MakeErrReply("ERR Wrong restore policy given, value should be either FLUSH, APPEND or REPLACE.")
		}
	}

	payload := args[0]
	if reply := validateBulkBytes(payload); reply != nil {
		return reply
	}

	// Classify payload before FLUSH so a rejected foreign dump cannot wipe libs.
	var libs map[string]string
	switch {
	case bytes.HasPrefix(payload, functionDumpMagic):
		parsed, ok := parseFunctionDumpBinaryStrict(payload)
		if !ok {
			return protocol.MakeErrReply(errFunctionRestoreCorruptGodis)
		}
		libs = parsed
	case looksLikeRedisOfficialFunctionDump(payload):
		return protocol.MakeErrReply(errFunctionRestoreOfficialRedis)
	case looksLikeNonGodisBinary(payload):
		return protocol.MakeErrReply(errFunctionRestoreForeignBinary)
	default:
		libs = parseLibraryDump(string(payload))
	}

	if policy == "FLUSH" {
		funcEngine.FlushAll()
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

	// Redis: missing function is reported before numkeys parse errors.
	fn, exists := funcEngine.GetFunction(funcName)
	if !exists {
		return protocol.MakeErrReply("ERR Function not found")
	}

	numKeys, err := atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	if numKeys < 0 {
		return protocol.MakeErrReply("ERR Number of keys can't be negative")
	}

	if len(args) < 2+numKeys {
		return protocol.MakeErrReply("ERR wrong number of arguments")
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
	foundShebang := false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "#!") {
			continue
		}
		foundShebang = true
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
			// Redis: shebang present but unusable → Invalid library metadata
			return "", protocol.MakeErrReply("ERR Invalid library metadata")
		}
		if hasAPI && apiVersion != "1.0" {
			return "", protocol.MakeErrReply("ERR Invalid API version in shebang")
		}
		return libName, nil
	}
	if !foundShebang {
		return "", protocol.MakeErrReply("ERR Missing library metadata")
	}
	return "", protocol.MakeErrReply("ERR Invalid library metadata")
}

func formatLibraryInfoReply(lib *functions.Library, withCode bool) redis.Reply {
	m := protocol.MakeMapReply()
	m.Put("library_name", protocol.MakeBulkReply([]byte(lib.Name)))
	engine := lib.Engine
	if engine == "" {
		engine = "LUA"
	}
	m.Put("engine", protocol.MakeBulkReply([]byte(engine)))

	fnReplies := make([]redis.Reply, 0, len(lib.Functions))
	for _, fn := range lib.Functions {
		fm := protocol.MakeMapReply()
		fm.Put("name", protocol.MakeBulkReply([]byte(fn.Name)))
		if fn.Description != "" {
			fm.Put("description", protocol.MakeBulkReply([]byte(fn.Description)))
		} else {
			fm.Put("description", protocol.MakeNullBulkReply())
		}
		flags := make([][]byte, len(fn.Flags))
		for i, f := range fn.Flags {
			flags[i] = []byte(f)
		}
		fm.Put("flags", protocol.MakeMultiBulkReply(flags))
		fnReplies = append(fnReplies, fm)
	}
	m.Put("functions", protocol.MakeMultiRawReply(fnReplies))

	if withCode {
		m.Put("library_code", protocol.MakeBulkReply([]byte(lib.Code)))
	}
	return m
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

// execFunctionHelp returns help for FUNCTION subcommands.
func execFunctionHelp(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'function|help' command")
	}
	return protocol.MakeMultiBulkReply([][]byte{
		[]byte("FUNCTION <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
		[]byte("LOAD [REPLACE] <LIBRARY CODE>"),
		[]byte("    Create a new library with the given library code."),
		[]byte("LIST [LIBRARYNAME <pattern>] [WITHCODE]"),
		[]byte("    Return general information on libraries."),
		[]byte("DELETE <LIBRARY NAME>"),
		[]byte("    Delete the given library."),
		[]byte("FLUSH [ASYNC|SYNC]"),
		[]byte("    Delete all libraries."),
		[]byte("KILL"),
		[]byte("    Kill the currently executing function."),
		[]byte("STATS"),
		[]byte("    Return function runtime stats."),
		[]byte("DUMP"),
		[]byte("    Dump all libraries into a serialized payload."),
		[]byte("RESTORE <PAYLOAD> [FLUSH|APPEND|REPLACE]"),
		[]byte("    Restore libraries from a payload."),
		[]byte("HELP"),
		[]byte("    Print this help."),
	})
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
	registerCommand("Function|Help", execFunctionHelp, noPrepare, nil, 1, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
	registerCommand("FCall", execFCall, prepareFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("FCall_RO", execFCallRO, prepareFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
