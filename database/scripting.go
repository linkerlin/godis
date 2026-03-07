package database

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
	"github.com/hdt3213/godis/scripting"
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
			return nil, fmt.Errorf(errReply.Status)
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
	
	// Debug mode not implemented
	mode := strings.ToUpper(string(args[0]))
	switch mode {
	case "YES", "SYNC", "NO":
		// Simplified: just return OK
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
			return nil
		}
		return string(r.Arg)
	case *protocol.IntReply:
		return r.Code
	case *protocol.StatusReply:
		return r.Status
	case *protocol.MultiBulkReply:
		result := make([]interface{}, len(r.Args))
		for i, arg := range r.Args {
			if arg == nil {
				result[i] = nil
			} else {
				result[i] = string(arg)
			}
		}
		return result
	case *protocol.StandardErrReply:
		return fmt.Errorf(r.Status)
	default:
		return reply.ToBytes()
	}
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
}
