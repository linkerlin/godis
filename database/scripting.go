package database

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/scripting"
)

// Global scripting engine
var scriptEngine *scripting.Engine

// InitScriptingEngine initializes the scripting engine
func InitScriptingEngine(db *DB) {
	// Create database execution function
	dbExec := func(cmd string, args ...string) (interface{}, error) {
		// Build command line
		cmdLine := make([][]byte, 0, len(args)+1)
		cmdLine = append(cmdLine, []byte(cmd))
		for _, arg := range args {
			cmdLine = append(cmdLine, []byte(arg))
		}

		// Execute command
		result := db.Exec(nil, cmdLine)
		if errReply, ok := result.(*protocol.StandardErrReply); ok {
			return nil, fmt.Errorf("%s", errReply.Status)
		}

		// Convert result
		return redisReplyToGo(result), nil
	}

	scriptEngine = scripting.NewEngine(dbExec)
}

// execEval executes a Lua script
// EVAL script numkeys [key ...] [arg ...]
func execEval(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'eval' command")
	}

	if scriptEngine == nil {
		InitScriptingEngine(db)
	}

	script := string(args[0])
	if reply := validateBulkBytes(args[0]); reply != nil {
		return reply
	}

	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR Number of keys can't be greater than number of args")
	}

	if len(args) < 2+numKeys {
		return protocol.MakeErrReply("ERR Number of keys can't be greater than number of args")
	}

	// Extract keys
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[2+i])
	}

	// Extract args
	scriptArgs := make([]string, len(args)-2-numKeys)
	for i := 0; i < len(scriptArgs); i++ {
		scriptArgs[i] = string(args[2+numKeys+i])
	}

	// Execute script
	result, err := scriptEngine.Eval(script, keys, scriptArgs)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	return scripting.ConvertToRedisReply(result)
}

// execEvalSha executes a script by SHA1
// EVALSHA sha1 numkeys [key ...] [arg ...]
func execEvalSha(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'evalsha' command")
	}

	if scriptEngine == nil {
		InitScriptingEngine(db)
	}

	sha1 := string(args[0])
	if reply := validateBulkBytes(args[0]); reply != nil {
		return reply
	}

	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR Number of keys can't be greater than number of args")
	}

	if len(args) < 2+numKeys {
		return protocol.MakeErrReply("ERR Number of keys can't be greater than number of args")
	}

	// Extract keys
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[2+i])
	}

	// Extract args
	scriptArgs := make([]string, len(args)-2-numKeys)
	for i := 0; i < len(scriptArgs); i++ {
		scriptArgs[i] = string(args[2+numKeys+i])
	}

	// Execute script
	result, err := scriptEngine.EvalSha(sha1, keys, scriptArgs)
	if err != nil {
		if strings.HasPrefix(err.Error(), "NOSCRIPT") {
			return protocol.MakeErrReply("NOSCRIPT No matching script. Please use EVAL.")
		}
		return protocol.MakeErrReply(err.Error())
	}

	return scripting.ConvertToRedisReply(result)
}

// execScriptExists checks if scripts exist
// SCRIPT EXISTS sha1 [sha1 ...]
func execScriptExists(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'script|exists' command")
	}

	if scriptEngine == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	shas := make([]string, len(args))
	for i, arg := range args {
		shas[i] = string(arg)
	}

	exists := scriptEngine.Exists(shas)

	result := make([][]byte, len(exists))
	for i, e := range exists {
		result[i] = []byte(strconv.Itoa(e))
	}

	return protocol.MakeMultiBulkReply(result)
}

// execScriptLoad loads a script
// SCRIPT LOAD script
func execScriptLoad(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'script|load' command")
	}

	if scriptEngine == nil {
		InitScriptingEngine(db)
	}

	script := string(args[0])
	if reply := validateBulkBytes(args[0]); reply != nil {
		return reply
	}
	sha1 := scriptEngine.LoadScript(script)

	return protocol.MakeBulkReply([]byte(sha1))
}

// execScriptFlush flushes all scripts
// SCRIPT FLUSH [ASYNC|SYNC]
func execScriptFlush(db *DB, args [][]byte) redis.Reply {
	if len(args) > 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'script|flush' command")
	}

	if scriptEngine == nil {
		return protocol.MakeOkReply()
	}

	// ASYNC/SYNC ignored for now
	scriptEngine.Flush()

	return protocol.MakeOkReply()
}

// execScriptKill kills a running script
// SCRIPT KILL
func execScriptKill(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'script|kill' command")
	}

	if scriptEngine == nil {
		return protocol.MakeErrReply("ERR No scripts in execution right now.")
	}

	if err := scriptEngine.Kill(); err != nil {
		return protocol.MakeErrReply("ERR No scripts in execution right now.")
	}

	return protocol.MakeOkReply()
}

// execScriptDebug controls script debug mode
// SCRIPT DEBUG YES|SYNC|NO
func execScriptDebug(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'script|debug' command")
	}

	mode := strings.ToUpper(string(args[0]))
	debugger := scripting.GetFullDebugger()

	switch mode {
	case "YES":
		debugger.SetMode(scripting.DebugModeYes)
		scripting.SetDebugMode(scripting.DebugModeYes) // For backward compatibility
		return protocol.MakeOkReply()
	case "SYNC":
		debugger.SetMode(scripting.DebugModeSync)
		scripting.SetDebugMode(scripting.DebugModeSync) // For backward compatibility
		return protocol.MakeOkReply()
	case "NO":
		debugger.SetMode(scripting.DebugModeNo)
		scripting.SetDebugMode(scripting.DebugModeNo) // For backward compatibility
		return protocol.MakeOkReply()
	default:
		return protocol.MakeErrReply("ERR Unknown DEBUG subcommand or wrong number of arguments for 'debug'")
	}
}

// Helper function to convert Redis reply to Go value
func redisReplyToGo(reply redis.Reply) interface{} {
	if reply == nil {
		return nil
	}

	switch r := reply.(type) {
	case *protocol.BulkReply:
		if r.Arg == nil {
			return false // nil bulk → Lua false
		}
		return string(r.Arg)
	case *protocol.NullBulkReply:
		return false // Lua false (standard Redis Lua returns false for nil)
	case *protocol.EmptyMultiBulkReply:
		return []interface{}{}
	case *protocol.IntReply:
		return r.Code
	case *protocol.StatusReply:
		return r.Status
	case *protocol.OkReply:
		return "OK"
	case *protocol.PongReply:
		return "PONG"
	case *protocol.MultiBulkReply:
		result := make([]interface{}, len(r.Args))
		for i, arg := range r.Args {
			result[i] = redisReplyToGo(protocol.MakeBulkReply(arg))
		}
		return result
	case *protocol.MultiRawReply:
		result := make([]interface{}, len(r.Replies))
		for i, sub := range r.Replies {
			result[i] = redisReplyToGo(sub)
		}
		return result
	case *protocol.StandardErrReply:
		return fmt.Errorf("%s", r.Status)
	default:
		return string(reply.ToBytes())
	}
}

// execScriptStep steps to next line in debug mode
// SCRIPT STEP
func execScriptStep(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'script|step' command")
	}

	debugger := scripting.GetFullDebugger()
	if !debugger.IsDebugging() {
		return protocol.MakeErrReply("ERR Lua debugger is not enabled")
	}

	debugger.Step()
	return protocol.MakeOkReply()
}

// execScriptContinue continues execution in debug mode
// SCRIPT CONTINUE
func execScriptContinue(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'script|continue' command")
	}

	debugger := scripting.GetFullDebugger()
	if !debugger.IsDebugging() {
		return protocol.MakeErrReply("ERR Lua debugger is not enabled")
	}

	debugger.Continue()
	return protocol.MakeOkReply()
}

// execScriptNext steps over to next line in debug mode
// SCRIPT NEXT
func execScriptNext(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'script|next' command")
	}

	debugger := scripting.GetFullDebugger()
	if !debugger.IsDebugging() {
		return protocol.MakeErrReply("ERR Lua debugger is not enabled")
	}

	debugger.Next()
	return protocol.MakeOkReply()
}

// execScriptBreakpoint adds/removes a breakpoint
// SCRIPT BREAKPOINT line [condition]
func execScriptBreakpoint(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'script|breakpoint' command")
	}

	line, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR line number must be an integer")
	}

	debugger := scripting.GetFullDebugger()

	var condition string
	if len(args) > 1 {
		condition = string(args[1])
	}

	debugger.AddBreakpoint(line, condition)
	return protocol.MakeOkReply()
}

// execScriptFinish steps out of current function
// SCRIPT FINISH
func execScriptFinish(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'script|finish' command")
	}

	debugger := scripting.GetFullDebugger()
	if !debugger.IsDebugging() {
		return protocol.MakeErrReply("ERR Lua debugger is not enabled")
	}

	debugger.Finish()
	return protocol.MakeOkReply()
}

// execScriptInfo shows current debug info
// SCRIPT INFO
func execScriptInfo(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'script|info' command")
	}

	debugger := scripting.GetFullDebugger()
	info := debugger.GetDebugInfo()

	result := make([][]byte, 0)
	result = append(result, []byte(fmt.Sprintf("Debug mode: %v", info["mode"])))
	result = append(result, []byte(fmt.Sprintf("Trace enabled: %v", info["trace"])))
	result = append(result, []byte(fmt.Sprintf("Breakpoints: %v", info["breakpoints"])))
	result = append(result, []byte(fmt.Sprintf("Script: %v", info["script"])))

	return protocol.MakeMultiBulkReply(result)
}

func init() {
	registerCommand("Eval", execEval, nil, nil, -3, flagSpecial).
		attachCommandExtra([]string{redisFlagNoScript}, 0, 0, 0)
	registerCommand("EvalSha", execEvalSha, nil, nil, -3, flagSpecial).
		attachCommandExtra([]string{redisFlagNoScript}, 0, 0, 0)
	registerCommand("Script|Exists", execScriptExists, nil, nil, -2, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin}, 0, 0, 0)
	registerCommand("Script|Load", execScriptLoad, nil, nil, 2, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin}, 0, 0, 0)
	registerCommand("Script|Flush", execScriptFlush, nil, nil, -1, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin}, 0, 0, 0)
	registerCommand("Script|Kill", execScriptKill, nil, nil, 1, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin}, 0, 0, 0)
	registerCommand("Script|Debug", execScriptDebug, nil, nil, 2, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin}, 0, 0, 0)
	registerCommand("Script|Step", execScriptStep, nil, nil, 1, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin}, 0, 0, 0)
	registerCommand("Script|Continue", execScriptContinue, nil, nil, 1, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin}, 0, 0, 0)
	registerCommand("Script|Next", execScriptNext, nil, nil, 1, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin}, 0, 0, 0)
	registerCommand("Script|Breakpoint", execScriptBreakpoint, nil, nil, -2, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin}, 0, 0, 0)
	registerCommand("Script|Finish", execScriptFinish, nil, nil, 1, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin}, 0, 0, 0)
	registerCommand("Script|Info", execScriptInfo, nil, nil, 1, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin}, 0, 0, 0)
}
