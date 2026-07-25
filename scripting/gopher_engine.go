package scripting

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/linkerlin/godis/lib/logger"
	lua "github.com/yuin/gopher-lua"
)

// Lua redis.* version constants (keep in sync with database.godisVersion).
const (
	luaRedisVersion    = "8.0.0"
	luaRedisVersionNum = 0x080000 // 8.0.0 as Redis VERSION_NUM
)

// GopherEngine is a Lua scripting engine based on gopher-lua
type GopherEngine struct {
	mu      sync.RWMutex
	scripts map[string]string // SHA1 -> script body
	dbExec  func(cmd string, args ...string) (interface{}, error)

	// Optional ACL checker for redis.acl_check_cmd (nil = always allow).
	aclCheckCmd func(cmd string, args []string) bool

	// LState pool for concurrent execution
	statePool *lStatePool

	// Running scripts for SCRIPT KILL
	runningScripts map[string]*scriptExecution

	// Full debugger
	debugger *FullDebugger

	// Preferred RESP major version from redis.setresp (2 or 3); informational for now.
	respVersion int

	// Script replication flags from redis.set_repl (informational stub).
	replFlags int
}

// scriptExecution tracks a running script
type scriptExecution struct {
	ctx    context.Context
	cancel context.CancelFunc
	state  *lua.LState
	start  time.Time
}

// lStatePool is a pool of reusable LState instances
type lStatePool struct {
	mu    sync.Mutex
	pool  []*lua.LState
	newFn func() *lua.LState
	size  int
}

// newLStatePool creates a new LState pool
func newLStatePool(size int, newFn func() *lua.LState) *lStatePool {
	return &lStatePool{
		pool:  make([]*lua.LState, 0, size),
		newFn: newFn,
		size:  size,
	}
}

// Get gets an LState from the pool
func (p *lStatePool) Get() *lua.LState {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.pool) > 0 {
		L := p.pool[len(p.pool)-1]
		p.pool = p.pool[:len(p.pool)-1]
		return L
	}

	return p.newFn()
}

// Put returns an LState to the pool
func (p *lStatePool) Put(L *lua.LState) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.pool) < p.size {
		// Clear global variables before returning to pool
		L.SetGlobal("KEYS", lua.LNil)
		L.SetGlobal("ARGV", lua.LNil)
		p.pool = append(p.pool, L)
	} else {
		L.Close()
	}
}

// Close closes all LStates in the pool
func (p *lStatePool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, L := range p.pool {
		L.Close()
	}
	p.pool = p.pool[:0]
}

// NewGopherEngine creates a new GopherEngine
func NewGopherEngine(dbExec func(cmd string, args ...string) (interface{}, error)) *GopherEngine {
	e := &GopherEngine{
		scripts:        make(map[string]string),
		dbExec:         dbExec,
		runningScripts: make(map[string]*scriptExecution),
		debugger:       GetFullDebugger(),
	}

	// Initialize LState pool with default size 50
	e.statePool = newLStatePool(50, func() *lua.LState {
		L := lua.NewState(lua.Options{
			CallStackSize:       256,
			RegistrySize:        1024,
			MinimizeStackMemory: true,
		})
		e.registerRedisAPI(L)
		registerCJSON(L)
		registerCMsgPack(L)
		registerBit(L)
		return L
	})

	return e
}

// SetACLCheckCmd sets the optional redis.acl_check_cmd implementation.
func (e *GopherEngine) SetACLCheckCmd(fn func(cmd string, args []string) bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.aclCheckCmd = fn
}

// Eval executes a Lua script
func (e *GopherEngine) Eval(script string, keys []string, args []string) (interface{}, error) {
	return e.EvalWithContext(context.Background(), script, keys, args)
}

// EvalWithContext executes a Lua script with context (for timeout/cancellation)
func (e *GopherEngine) EvalWithContext(ctx context.Context, script string, keys []string, args []string) (interface{}, error) {
	L := e.statePool.Get()
	defer e.statePool.Put(L)

	// Create script execution context with timeout
	execCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Set context for cancellation support
	L.SetContext(execCtx)

	// Generate execution ID
	execID := generateExecID()

	// Register running script for SCRIPT KILL
	e.mu.Lock()
	e.runningScripts[execID] = &scriptExecution{
		ctx:    execCtx,
		cancel: cancel,
		state:  L,
		start:  time.Now(),
	}
	e.mu.Unlock()

	// Cleanup when done
	defer func() {
		e.mu.Lock()
		delete(e.runningScripts, execID)
		e.mu.Unlock()
	}()

	// Setup KEYS table
	keysTable := L.NewTable()
	for i, k := range keys {
		L.SetTable(keysTable, lua.LNumber(i+1), lua.LString(k))
	}
	L.SetGlobal("KEYS", keysTable)

	// Setup ARGV table
	argvTable := L.NewTable()
	for i, a := range args {
		L.SetTable(argvTable, lua.LNumber(i+1), lua.LString(a))
	}
	L.SetGlobal("ARGV", argvTable)

	// Execute script
	if err := L.DoString(script); err != nil {
		// Check if it's a context cancellation
		if execCtx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("ERR Error running script (call to f_): @user_script: script execution timed out")
		}
		if execCtx.Err() == context.Canceled {
			return nil, fmt.Errorf("ERR Error running script (call to f_): @user_script: script killed by user")
		}
		return nil, fmt.Errorf("ERR Error running script (call to f_): @user_script: %v", err)
	}

	// Convert return value
	retValue := L.Get(-1)
	L.Pop(1)

	return luaValueToGo(retValue), nil
}

// EvalSha executes a script by SHA1
func (e *GopherEngine) EvalSha(sha1 string, keys []string, args []string) (interface{}, error) {
	e.mu.RLock()
	script, ok := e.scripts[sha1]
	e.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("NOSCRIPT No matching script. Please use EVAL.")
	}

	return e.Eval(script, keys, args)
}

// EvalShaWithContext executes a script by SHA1 with context
func (e *GopherEngine) EvalShaWithContext(ctx context.Context, sha1 string, keys []string, args []string) (interface{}, error) {
	e.mu.RLock()
	script, ok := e.scripts[sha1]
	e.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("NOSCRIPT No matching script. Please use EVAL.")
	}

	return e.EvalWithContext(ctx, script, keys, args)
}

// LoadScript loads a script and returns its SHA1
func (e *GopherEngine) LoadScript(script string) string {
	sha := computeScriptSHA(script)

	e.mu.Lock()
	defer e.mu.Unlock()

	e.scripts[sha] = script
	return sha
}

// Exists checks if scripts exist
func (e *GopherEngine) Exists(shas []string) []int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]int, len(shas))
	for i, sha := range shas {
		if _, ok := e.scripts[sha]; ok {
			result[i] = 1
		} else {
			result[i] = 0
		}
	}

	return result
}

// Flush clears all scripts
func (e *GopherEngine) Flush() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.scripts = make(map[string]string)
}

// Kill stops all running scripts
func (e *GopherEngine) Kill() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	killed := 0
	for _, exec := range e.runningScripts {
		if exec.cancel != nil {
			exec.cancel()
			killed++
		}
	}

	if killed == 0 {
		return fmt.Errorf("ERR No scripts in execution right now")
	}

	logger.Infof("Killed %d running scripts", killed)
	return nil
}

// GetRunningScriptCount returns the number of currently running scripts
func (e *GopherEngine) GetRunningScriptCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return len(e.runningScripts)
}

// Close closes the engine and releases resources
func (e *GopherEngine) Close() {
	// Cancel all running scripts
	e.Kill()

	// Close LState pool
	if e.statePool != nil {
		e.statePool.Close()
	}
}

// generateExecID generates a unique execution ID
func generateExecID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// registerRedisAPI registers Redis API functions to the LState
func (e *GopherEngine) registerRedisAPI(L *lua.LState) {
	// Create redis table
	redisTable := L.NewTable()
	L.SetGlobal("redis", redisTable)

	// Register redis.call
	L.SetField(redisTable, "call", L.NewFunction(e.luaRedisCall))

	// Register redis.pcall
	L.SetField(redisTable, "pcall", L.NewFunction(e.luaRedisPCall))

	// Register redis.sha1hex
	L.SetField(redisTable, "sha1hex", L.NewFunction(e.luaRedisSha1Hex))

	// Register redis.log
	L.SetField(redisTable, "log", L.NewFunction(e.luaRedisLog))

	// Register redis.breakpoint (for debugging)
	L.SetField(redisTable, "breakpoint", L.NewFunction(e.luaRedisBreakpoint))

	// Register redis.debug (for debugging)
	L.SetField(redisTable, "debug", L.NewFunction(e.luaRedisDebug))

	// Register redis.setresp (compatibility, stores preferred RESP version)
	L.SetField(redisTable, "setresp", L.NewFunction(e.luaRedisSetResp))
	L.SetField(redisTable, "error_reply", L.NewFunction(luaRedisErrorReply))
	L.SetField(redisTable, "status_reply", L.NewFunction(luaRedisStatusReply))
	L.SetField(redisTable, "acl_check_cmd", L.NewFunction(e.luaRedisACLCheckCmd))
	L.SetField(redisTable, "set_repl", L.NewFunction(e.luaRedisSetRepl))
	L.SetField(redisTable, "replicate_commands", L.NewFunction(luaRedisReplicateCommands))
	// redis.LOG_* level constants (for redis.log)
	L.SetField(redisTable, "LOG_DEBUG", lua.LNumber(0))
	L.SetField(redisTable, "LOG_VERBOSE", lua.LNumber(1))
	L.SetField(redisTable, "LOG_NOTICE", lua.LNumber(2))
	L.SetField(redisTable, "LOG_WARNING", lua.LNumber(3))
	// redis.REPL_* flags for set_repl (Redis scripting API)
	L.SetField(redisTable, "REPL_NONE", lua.LNumber(0))
	L.SetField(redisTable, "REPL_AOF", lua.LNumber(1))
	L.SetField(redisTable, "REPL_SLAVE", lua.LNumber(2))
	L.SetField(redisTable, "REPL_REPLICA", lua.LNumber(2))
	L.SetField(redisTable, "REPL_ALL", lua.LNumber(3))
	// Version constants (aligned with Godis redis_version / godisVersion)
	L.SetField(redisTable, "REDIS_VERSION", lua.LString(luaRedisVersion))
	L.SetField(redisTable, "REDIS_VERSION_NUM", lua.LNumber(luaRedisVersionNum))
}

// luaRedisCall implements redis.call
func (e *GopherEngine) luaRedisCall(L *lua.LState) int {
	// Check for context cancellation
	if ctx := L.Context(); ctx != nil {
		select {
		case <-ctx.Done():
			L.RaiseError("script execution cancelled")
			return 0
		default:
		}
	}

	// Get command name
	cmd := L.CheckString(1)

	// Collect arguments
	var args []string
	for i := 2; i <= L.GetTop(); i++ {
		args = append(args, luaValueToArg(L.Get(i)))
	}

	// Execute Redis command
	result, err := e.dbExec(cmd, args...)
	if err != nil {
		L.RaiseError("%s", err.Error())
		return 0
	}

	// Push result to stack
	pushGoValue(L, result)
	return 1
}

// luaRedisPCall implements redis.pcall
func (e *GopherEngine) luaRedisPCall(L *lua.LState) int {
	// Check for context cancellation
	if ctx := L.Context(); ctx != nil {
		select {
		case <-ctx.Done():
			tbl := L.NewTable()
			L.SetField(tbl, "err", lua.LString("script execution cancelled"))
			L.Push(tbl)
			return 1
		default:
		}
	}

	// Get command name
	cmd := L.CheckString(1)

	// Collect arguments
	var args []string
	for i := 2; i <= L.GetTop(); i++ {
		args = append(args, luaValueToArg(L.Get(i)))
	}

	// Execute Redis command
	result, err := e.dbExec(cmd, args...)

	if err != nil {
		tbl := L.NewTable()
		L.SetField(tbl, "err", lua.LString(err.Error()))
		L.Push(tbl)
		return 1
	}

	// Redis: successful pcall returns the value directly (same as call)
	pushGoValue(L, result)
	return 1
}

// luaRedisSha1Hex implements redis.sha1hex
func (e *GopherEngine) luaRedisSha1Hex(L *lua.LState) int {
	str := L.CheckString(1)
	hash := sha1.Sum([]byte(str))
	L.Push(lua.LString(hex.EncodeToString(hash[:])))
	return 1
}

// luaRedisACLCheckCmd implements redis.acl_check_cmd(cmd, ...).
func (e *GopherEngine) luaRedisACLCheckCmd(L *lua.LState) int {
	cmd := L.CheckString(1)
	var args []string
	for i := 2; i <= L.GetTop(); i++ {
		args = append(args, luaValueToArg(L.Get(i)))
	}
	ok := true
	e.mu.RLock()
	fn := e.aclCheckCmd
	e.mu.RUnlock()
	if fn != nil {
		ok = fn(cmd, args)
	}
	L.Push(lua.LBool(ok))
	return 1
}

// luaRedisSetRepl implements redis.set_repl(flags) — records flags; AOF policy unchanged.
func (e *GopherEngine) luaRedisSetRepl(L *lua.LState) int {
	flags := L.OptInt(1, 0)
	e.mu.Lock()
	e.replFlags = flags
	e.mu.Unlock()
	return 0
}

// luaRedisReplicateCommands implements redis.replicate_commands() — always true (Redis 5+).
func luaRedisReplicateCommands(L *lua.LState) int {
	L.Push(lua.LTrue)
	return 1
}

// luaRedisLog implements redis.log
func (e *GopherEngine) luaRedisLog(L *lua.LState) int {
	level := L.CheckInt(1)
	msg := L.CheckString(2)

	var levelStr string
	switch level {
	case 0:
		levelStr = "DEBUG"
	case 1:
		levelStr = "VERBOSE"
	case 2:
		levelStr = "NOTICE"
	case 3:
		levelStr = "WARNING"
	default:
		levelStr = "UNKNOWN"
	}

	logger.Infof("[REDIS LOG %s] %s", levelStr, msg)
	return 0
}

// luaRedisBreakpoint implements redis.breakpoint (for debugging)
func (e *GopherEngine) luaRedisBreakpoint(L *lua.LState) int {
	// Check if debugging is enabled
	if !IsDebugEnabled() {
		return 0
	}

	debugger := GetDebugger()

	// Get current line info
	// Note: gopher-lua doesn't expose line info easily in this context
	// This is a simplified implementation
	vars := make(map[string]interface{})

	// Pause execution
	if debugger.OnLineExecute(0, vars) {
		debugger.WaitPause()
	}

	return 0
}

// luaRedisDebug implements redis.debug (for debugging)
func (e *GopherEngine) luaRedisDebug(L *lua.LState) int {
	if L.GetTop() == 0 {
		return 0
	}

	msg := L.CheckString(1)
	logger.Infof("[REDIS DEBUG] %s", msg)

	return 0
}

// luaRedisSetResp implements redis.setresp — records preferred RESP major version.
func (e *GopherEngine) luaRedisSetResp(L *lua.LState) int {
	n := L.CheckInt(1)
	if n != 2 && n != 3 {
		L.RaiseError("ERR Unknown RESP version")
		return 0
	}
	e.mu.Lock()
	e.respVersion = n
	e.mu.Unlock()
	return 0
}

func luaRedisErrorReply(L *lua.LState) int {
	msg := L.CheckString(1)
	t := L.NewTable()
	L.SetField(t, "err", lua.LString(msg))
	L.Push(t)
	return 1
}

func luaRedisStatusReply(L *lua.LState) int {
	msg := L.CheckString(1)
	t := L.NewTable()
	L.SetField(t, "ok", lua.LString(msg))
	L.Push(t)
	return 1
}
