package database

import (
	"strconv"
	"strings"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
	"github.com/hdt3213/godis/scripting"
)

var scriptEngine *scripting.Engine

// InitScriptEngine initializes the Lua scripting engine
func (server *Server) InitScriptEngine() {
	scriptEngine = scripting.NewEngine(server)
}

// execEval executes a Lua script
// EVAL script numkeys key [key ...] arg [arg ...]
func execEval(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'eval' command")
	}
	
	script := string(args[0])
	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil || numKeys < 0 {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	
	if len(args) < 2+numKeys {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'eval' command")
	}
	
	// Extract keys and args
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[2+i])
	}
	
	scriptArgs := make([]string, len(args)-2-numKeys)
	for i := 0; i < len(scriptArgs); i++ {
		scriptArgs[i] = string(args[2+numKeys+i])
	}
	
	if scriptEngine == nil {
		return protocol.MakeErrReply("ERR Lua scripting engine not initialized")
	}
	
	result := scriptEngine.Execute(script, keys, scriptArgs)
	db.addAof(utils.ToCmdLine3("eval", args...))
	return result
}

// execEvalSHA executes a script by SHA1
// EVALSHA sha1 numkeys key [key ...] arg [arg ...]
func execEvalSHA(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'evalsha' command")
	}
	
	sha1Hash := string(args[0])
	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil || numKeys < 0 {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	
	if len(args) < 2+numKeys {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'evalsha' command")
	}
	
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[2+i])
	}
	
	scriptArgs := make([]string, len(args)-2-numKeys)
	for i := 0; i < len(scriptArgs); i++ {
		scriptArgs[i] = string(args[2+numKeys+i])
	}
	
	if scriptEngine == nil {
		return protocol.MakeErrReply("ERR Lua scripting engine not initialized")
	}
	
	return scriptEngine.ExecuteSHA(sha1Hash, keys, scriptArgs)
}

// execScriptLoad loads a script
// SCRIPT LOAD script
func execScriptLoad(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'script' command")
	}
	
	script := string(args[0])
	
	if scriptEngine == nil {
		return protocol.MakeErrReply("ERR Lua scripting engine not initialized")
	}
	
	hash := scriptEngine.ScriptLoad(script)
	return protocol.MakeBulkReply([]byte(hash))
}

// execScriptExists checks if scripts exist
// SCRIPT EXISTS sha1 [sha1 ...]
func execScriptExists(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'script' command")
	}
	
	if scriptEngine == nil {
		return protocol.MakeErrReply("ERR Lua scripting engine not initialized")
	}
	
	sha1s := make([]string, len(args))
	for i, arg := range args {
		sha1s[i] = string(arg)
	}
	
	exists := scriptEngine.ScriptExists(sha1s)
	result := make([][]byte, len(exists))
	for i, e := range exists {
		result[i] = []byte(strconv.Itoa(e))
	}
	
	return protocol.MakeMultiBulkReply(result)
}

// execScriptFlush flushes all scripts
// SCRIPT FLUSH [ASYNC|SYNC]
func execScriptFlush(db *DB, args [][]byte) redis.Reply {
	// Ignore ASYNC/SYNC for now
	if len(args) > 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'script' command")
	}
	
	if scriptEngine == nil {
		return protocol.MakeErrReply("ERR Lua scripting engine not initialized")
	}
	
	scriptEngine.ScriptFlush()
	return protocol.MakeOkReply()
}

// execScriptKill kills a running script
// SCRIPT KILL
func execScriptKill(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'script' command")
	}
	
	// TODO: Implement script killing when full Lua engine is integrated
	return protocol.MakeErrReply("ERR Not implemented")
}

// execScriptDebug sets debug mode
// SCRIPT DEBUG YES|SYNC|NO
func execScriptDebug(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'script' command")
	}
	
	mode := strings.ToUpper(string(args[0]))
	switch mode {
	case "YES", "SYNC", "NO":
		// TODO: Implement debug mode
		return protocol.MakeOkReply()
	default:
		return protocol.MakeErrReply("ERR unknown subcommand or wrong number of arguments for 'SCRIPT DEBUG'")
	}
}

// execScriptHelp returns help information
// SCRIPT HELP
func execScriptHelp(db *DB, args [][]byte) redis.Reply {
	help := []string{
		"SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
		"DEBUG (YES|SYNC|NO)",
		"    Set the debug mode for subsequent scripts executed.",
		"EXISTS <sha1> [<sha1> ...]", 
		"    Return information about the existence of the scripts in the script cache.",
		"FLUSH [ASYNC|SYNC]",
		"    Flush the Lua scripts cache.",
		"KILL",
		"    Kill the currently executing Lua script.",
		"LOAD <script>",
		"    Load the specified Lua script into the script cache.",
	}
	
	result := make([][]byte, len(help))
	for i, line := range help {
		result[i] = []byte(line)
	}
	
	return protocol.MakeMultiBulkReply(result)
}

// ScriptCommand handles SCRIPT subcommands
func ScriptCommand(args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'script' command")
	}
	
	subCmd := strings.ToUpper(string(args[0]))
	remainingArgs := args[1:]
	
	switch subCmd {
	case "LOAD":
		return execScriptLoad(nil, remainingArgs)
	case "EXISTS":
		return execScriptExists(nil, remainingArgs)
	case "FLUSH":
		return execScriptFlush(nil, remainingArgs)
	case "KILL":
		return execScriptKill(nil, remainingArgs)
	case "DEBUG":
		return execScriptDebug(nil, remainingArgs)
	case "HELP":
		return execScriptHelp(nil, remainingArgs)
	default:
		return protocol.MakeErrReply("ERR unknown subcommand '" + subCmd + "'. Try SCRIPT HELP.")
	}
}

func init() {
	registerCommand("Eval", execEval, noPrepare, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagMovableKeys}, 0, 0, 0)
	registerCommand("EvalSHA", execEvalSHA, noPrepare, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagMovableKeys}, 0, 0, 0)
}
