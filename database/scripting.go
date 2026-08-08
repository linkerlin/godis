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

// scriptDB is the DB currently executing EVAL/EVALSHA (redis.call target).
var scriptDB *DB

// scriptReadOnly marks EVAL_RO/EVALSHA_RO execution: nested redis.call writes
// are rejected. Script execution is single-threaded (one script at a time),
// so a plain bool is safe.
var scriptReadOnly bool

// InitScriptingEngine initializes the scripting engine
func InitScriptingEngine(db *DB) {
	scriptDB = db
	if scriptEngine == nil {
		// Nested redis.call must not re-lock; EVAL holds keys via prepareEval.
		dbExec := func(cmd string, args ...string) (interface{}, error) {
			// Read-only scripts (EVAL_RO/EVALSHA_RO) must not issue writes.
			// Write detection uses the attachCommandExtra string flags (the
			// numeric flagWrite is 0 by design, so bit-testing it is useless).
			if scriptReadOnly {
				if c, ok := cmdTable[strings.ToLower(cmd)]; ok && c.extra != nil {
					for _, s := range c.extra.signs {
						if s == "write" {
							return nil, fmt.Errorf("ERR Write commands are not allowed from read-only scripts")
						}
					}
				}
			}
			cmdLine := make([][]byte, 0, len(args)+1)
			cmdLine = append(cmdLine, []byte(cmd))
			for _, arg := range args {
				cmdLine = append(cmdLine, []byte(arg))
			}
			target := scriptDB
			if target == nil {
				return nil, fmt.Errorf("ERR scripting engine not bound to a database")
			}
			// set_repl without REPL_AOF bit: suppress AOF for nested writes.
			if scriptEngine != nil && scriptEngine.ShouldSuppressAOF() {
				saved := target.addAof
				target.addAof = func(CmdLine) {}
				defer func() { target.addAof = saved }()
			}
			result := target.execWithLock(nil, cmdLine)
			if protocol.IsErrorReply(result) {
				if er, ok := result.(error); ok {
					return nil, er
				}
				return nil, fmt.Errorf("%s", strings.TrimRight(string(result.ToBytes()), "\r\n"))
			}
			return redisReplyToGo(result, cmd, args...), nil
		}

		scriptEngine = scripting.NewEngine(dbExec)
	}
	scriptEngine.SetACLCheckCmd(scriptACLCheckCmd)
}

// scriptACLCheckCmd implements redis.acl_check_cmd using the default ACL user
// when no connection-bound user is available inside nested script execution.
func scriptACLCheckCmd(cmd string, args []string) bool {
	if aclEngine == nil {
		return true
	}
	user, ok := aclEngine.GetUser("default")
	if !ok {
		return true
	}
	if !user.CheckCommand(cmd) {
		return false
	}
	var writeKeys, readKeys []string
	cmdName := strings.ToLower(cmd)
	if c, ok := cmdTable[cmdName]; ok && c.prepare != nil && len(args) > 0 {
		argBytes := make([][]byte, len(args))
		for i, a := range args {
			argBytes[i] = []byte(a)
		}
		writeKeys, readKeys = c.prepare(argBytes)
	}
	return user.CheckPermission(cmd, writeKeys, readKeys)
}

// execEval executes a Lua script
// EVAL script numkeys [key ...] [arg ...]
func execEval(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'eval' command")
	}

	scriptDB = db
	InitScriptingEngine(db)

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
	// Redis caches EVAL scripts so EVALSHA works afterwards
	scriptEngine.LoadScript(script)

	return scripting.ConvertToRedisReplyWithResp(result, scriptEngine.GetRespVersion())
}

// execEvalRO executes a script read-only: nested redis.call write commands are
// rejected. EVAL_RO script numkeys [key ...] [arg ...]
func execEvalRO(db *DB, args [][]byte) redis.Reply {
	scriptReadOnly = true
	defer func() { scriptReadOnly = false }()
	return execEval(db, args)
}

// execEvalShaRO executes a cached script read-only (see execEvalRO).
func execEvalShaRO(db *DB, args [][]byte) redis.Reply {
	scriptReadOnly = true
	defer func() { scriptReadOnly = false }()
	return execEvalSha(db, args)
}

// execEvalSha executes a script by SHA1
// EVALSHA sha1 numkeys [key ...] [arg ...]
func execEvalSha(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'evalsha' command")
	}

	scriptDB = db
	InitScriptingEngine(db)

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

	return scripting.ConvertToRedisReplyWithResp(result, scriptEngine.GetRespVersion())
}

// execScriptExists checks if scripts exist
// SCRIPT EXISTS sha1 [sha1 ...] — returns array of integers (0/1).
func execScriptExists(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'script|exists' command")
	}

	replies := make([]redis.Reply, len(args))
	if scriptEngine == nil {
		for i := range replies {
			replies[i] = protocol.MakeIntReply(0)
		}
		return protocol.MakeMultiRawReply(replies)
	}

	shas := make([]string, len(args))
	for i, arg := range args {
		shas[i] = string(arg)
	}
	exists := scriptEngine.Exists(shas)
	for i, e := range exists {
		replies[i] = protocol.MakeIntReply(int64(e))
	}
	return protocol.MakeMultiRawReply(replies)
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
	if len(args) == 1 {
		mode := strings.ToUpper(string(args[0]))
		if mode != "ASYNC" && mode != "SYNC" {
			return protocol.MakeErrReply("ERR SCRIPT FLUSH only support SYNC|ASYNC mode")
		}
	}

	if scriptEngine == nil {
		return protocol.MakeOkReply()
	}

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
		return protocol.MakeErrReply("NOTBUSY No scripts in execution right now.")
	}

	if err := scriptEngine.Kill(); err != nil {
		return protocol.MakeErrReply("NOTBUSY No scripts in execution right now.")
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
func redisReplyToGo(reply redis.Reply, cmd string, args ...string) interface{} {
	if reply == nil {
		return nil
	}
	respVer := 2
	if scriptEngine != nil {
		respVer = scriptEngine.GetRespVersion()
	}

	switch r := reply.(type) {
	case *protocol.BulkReply:
		if r.Arg == nil {
			return false // nil bulk → Lua false
		}
		s := string(r.Arg)
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return i
		}
		if respVer == 3 && isScoreBulkCmd(cmd, args) {
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f
			}
		}
		return s
	case *protocol.NullBulkReply:
		return false
	case *protocol.NullReply:
		return nil
	case *protocol.BooleanReply:
		return r.Value
	case *protocol.DoubleReply:
		return r.Value
	case *protocol.MapReply:
		if respVer == 3 {
			result := make(map[string]interface{}, len(r.Data))
			for k, v := range r.Data {
				result[k] = redisReplyToGo(v, cmd, args...)
			}
			return result
		}
		// setresp(2): flatten Map to array like RESP2 wire form
		result := make([]interface{}, 0, len(r.Data)*2)
		for k, v := range r.Data {
			result = append(result, k, redisReplyToGo(v, cmd, args...))
		}
		return result
	case *protocol.SetReply:
		if respVer == 3 {
			m := make(map[string]interface{}, len(r.Data))
			for _, elem := range r.Data {
				m[fmt.Sprint(redisReplyToGo(elem, cmd, args...))] = true
			}
			return m
		}
		result := make([]interface{}, len(r.Data))
		for i, elem := range r.Data {
			result[i] = redisReplyToGo(elem, cmd, args...)
		}
		return result
	case *protocol.MultiBulkReply:
		if respVer == 3 && isSetFlatCmd(cmd, args) {
			m := make(map[string]interface{}, len(r.Args))
			for _, arg := range r.Args {
				m[string(arg)] = true
			}
			return m
		}
		if respVer == 3 && isMapFlatCmd(cmd, args) && len(r.Args)%2 == 0 {
			m := make(map[string]interface{}, len(r.Args)/2)
			for i := 0; i+1 < len(r.Args); i += 2 {
				m[string(r.Args[i])] = redisReplyToGo(protocol.MakeBulkReply(r.Args[i+1]), cmd, args...)
			}
			return m
		}
		result := make([]interface{}, len(r.Args))
		for i, arg := range r.Args {
			result[i] = redisReplyToGo(protocol.MakeBulkReply(arg), cmd, args...)
		}
		return result
	case *protocol.EmptyMultiBulkReply:
		if respVer == 3 && isMapFlatCmd(cmd, args) {
			return map[string]interface{}{}
		}
		if respVer == 3 && isSetFlatCmd(cmd, args) {
			return map[string]interface{}{}
		}
		return []interface{}{}
	case *protocol.IntReply:
		return r.Code
	case *protocol.StatusReply:
		return r.Status
	case *protocol.OkReply:
		return "OK"
	case *protocol.PongReply:
		return "PONG"
	case *protocol.MultiRawReply:
		if respVer == 3 && isStreamRangeCmd(cmd) {
			result := make([]interface{}, len(r.Replies))
			for i, sub := range r.Replies {
				entry, ok := sub.(*protocol.MultiRawReply)
				if !ok || len(entry.Replies) != 2 {
					result[i] = redisReplyToGo(sub, cmd, args...)
					continue
				}
				id := redisReplyToGo(entry.Replies[0], cmd, args...)
				fields := streamFieldsToGo(entry.Replies[1], cmd, args...)
				result[i] = []interface{}{id, fields}
			}
			return result
		}
		if respVer == 3 && isScanMapCmd(cmd) && len(r.Replies) == 2 {
			cursor := redisReplyToGo(r.Replies[0], cmd, args...)
			pairs := scanPairsToMap(r.Replies[1], cmd, args...)
			return []interface{}{cursor, pairs}
		}
		result := make([]interface{}, len(r.Replies))
		for i, sub := range r.Replies {
			result[i] = redisReplyToGo(sub, cmd, args...)
		}
		return result
	case *protocol.StandardErrReply:
		return fmt.Errorf("%s", r.Status)
	default:
		return string(reply.ToBytes())
	}
}

func streamFieldsToGo(reply redis.Reply, cmd string, args ...string) interface{} {
	fr, ok := reply.(*protocol.MultiRawReply)
	if !ok || len(fr.Replies)%2 != 0 {
		return redisReplyToGo(reply, cmd, args...)
	}
	m := make(map[string]interface{}, len(fr.Replies)/2)
	for i := 0; i+1 < len(fr.Replies); i += 2 {
		k := fmt.Sprint(redisReplyToGo(fr.Replies[i], cmd, args...))
		m[k] = redisReplyToGo(fr.Replies[i+1], cmd, args...)
	}
	return m
}

func isStreamRangeCmd(cmd string) bool {
	switch strings.ToLower(cmd) {
	case "xrange", "xrevrange":
		return true
	default:
		return false
	}
}

// isScoreBulkCmd reports whether bulk string values for this command are scores under setresp(3).
func isScoreBulkCmd(cmd string, args []string) bool {
	switch strings.ToLower(cmd) {
	case "zscore", "zmscore", "zpopmin", "zpopmax", "zscan", "geodist", "geopos",
		"zincrby", "hincrbyfloat", "incrbyfloat":
		return true
	case "zrank", "zrevrank":
		for _, a := range args {
			if strings.EqualFold(a, "WITHSCORE") {
				return true
			}
		}
		return false
	case "geosearch", "georadius", "georadiusbymember":
		for _, a := range args {
			u := strings.ToUpper(a)
			if u == "WITHDIST" || u == "WITHCOORD" || u == "WITHHASH" {
				return true
			}
		}
		return false
	case "zrange", "zrevrange", "zrangebyscore", "zrevrangebyscore", "zrandmember",
		"zunion", "zinter", "zdiff":
		for _, a := range args {
			if strings.EqualFold(a, "WITHSCORES") {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func isScanMapCmd(cmd string) bool {
	switch strings.ToLower(cmd) {
	case "hscan", "zscan":
		return true
	default:
		return false
	}
}

func scanPairsToMap(reply redis.Reply, cmd string, args ...string) interface{} {
	switch r := reply.(type) {
	case *protocol.MultiBulkReply:
		m := make(map[string]interface{}, len(r.Args)/2)
		for i := 0; i+1 < len(r.Args); i += 2 {
			m[string(r.Args[i])] = redisReplyToGo(protocol.MakeBulkReply(r.Args[i+1]), cmd, args...)
		}
		return m
	case *protocol.EmptyMultiBulkReply:
		return map[string]interface{}{}
	default:
		return map[string]interface{}{}
	}
}

func isMapFlatCmd(cmd string, args []string) bool {
	switch strings.ToLower(cmd) {
	case "hgetall", "config":
		return true
	case "zpopmin", "zpopmax":
		return true // always member/score pairs
	case "zrange", "zrevrange", "zrangebyscore", "zrevrangebyscore", "zrandmember",
		"zunion", "zinter", "zdiff":
		for _, a := range args {
			if strings.EqualFold(a, "WITHSCORES") {
				return true
			}
		}
		return false
	case "hrandfield":
		for _, a := range args {
			if strings.EqualFold(a, "WITHVALUES") {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func isSetFlatCmd(cmd string, args []string) bool {
	switch strings.ToLower(cmd) {
	case "smembers", "sinter", "sunion", "sdiff", "keys":
		return true
	case "zunion", "zinter", "zdiff":
		for _, a := range args {
			if strings.EqualFold(a, "WITHSCORES") {
				return false
			}
		}
		return true
	case "srandmember", "spop":
		if len(args) >= 2 {
			if n, err := strconv.Atoi(args[1]); err == nil && n > 0 {
				return true
			}
		}
		return false
	case "zrandmember":
		if len(args) < 2 {
			return false
		}
		for _, a := range args {
			if strings.EqualFold(a, "WITHSCORES") {
				return false
			}
		}
		if n, err := strconv.Atoi(args[1]); err == nil && n > 0 {
			return true
		}
		return false
	default:
		return false
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

// execScriptHelp returns help for SCRIPT subcommands.
func execScriptHelp(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'script|help' command")
	}
	return protocol.MakeMultiBulkReply([][]byte{
		[]byte("SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
		[]byte("EXISTS <sha1> [<sha1> ...]"),
		[]byte("    Return information about the existence of the scripts in the script cache."),
		[]byte("FLUSH [ASYNC|SYNC]"),
		[]byte("    Flush the Lua scripts cache."),
		[]byte("KILL"),
		[]byte("    Kill the currently executing Lua script."),
		[]byte("LOAD <script>"),
		[]byte("    Load a script into the scripts cache."),
		[]byte("DEBUG YES|SYNC|NO"),
		[]byte("    Set the debug mode for subsequent scripts executed with EVAL."),
		[]byte("HELP"),
		[]byte("    Print this help."),
	})
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

// prepareEval locks the keys declared by EVAL/EVALSHA (MULTI/EXEC + concurrent safety).
func prepareEval(args [][]byte) ([]string, []string) {
	if len(args) < 2 {
		return nil, nil
	}
	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil || numKeys < 0 {
		return nil, nil
	}
	if len(args) < 2+numKeys {
		return nil, nil
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[2+i])
	}
	return keys, nil
}

func init() {
	registerCommand("Eval", execEval, prepareEval, nil, -3, flagSpecial).
		attachCommandExtra([]string{redisFlagNoScript}, 0, 0, 0)
	registerCommand("EvalSha", execEvalSha, prepareEval, nil, -3, flagSpecial).
		attachCommandExtra([]string{redisFlagNoScript}, 0, 0, 0)
	registerCommand("Eval_RO", execEvalRO, prepareEval, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagNoScript}, 0, 0, 0)
	registerCommand("EvalSha_RO", execEvalShaRO, prepareEval, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagNoScript}, 0, 0, 0)
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
	registerCommand("Script|Help", execScriptHelp, nil, nil, 1, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin}, 0, 0, 0)
}
