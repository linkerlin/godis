package scripting

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"sync"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
)

// EngineType represents the type of Lua engine
type EngineType int

const (
	// EngineTypeLegacy uses the built-in simplified Lua interpreter
	EngineTypeLegacy EngineType = iota
	// EngineTypeGopherLua uses the gopher-lua library
	EngineTypeGopherLua
)

// Engine is the Lua scripting engine with pluggable backends
type Engine struct {
	mu      sync.RWMutex
	scripts map[string]string // SHA1 -> script body

	// Engine type selection
	engineType EngineType

	// Legacy engine (built-in)
	legacyEngine *LuaEngine

	// Gopher-lua engine
	gopherEngine *GopherEngine

	// Reference to database for redis.call
	dbExec func(cmd string, args ...string) (interface{}, error)
}

// NewEngine creates a new scripting engine
// By default, it uses the gopher-lua engine for better Lua compatibility
func NewEngine(dbExec func(cmd string, args ...string) (interface{}, error)) *Engine {
	return NewEngineWithType(dbExec, EngineTypeGopherLua)
}

// NewEngineWithType creates a new scripting engine with specific type
func NewEngineWithType(dbExec func(cmd string, args ...string) (interface{}, error), engineType EngineType) *Engine {
	e := &Engine{
		scripts:    make(map[string]string),
		dbExec:     dbExec,
		engineType: engineType,
	}

	switch engineType {
	case EngineTypeGopherLua:
		logger.Info("Using gopher-lua engine for Lua scripting")
		e.gopherEngine = NewGopherEngine(dbExec)
	case EngineTypeLegacy:
		logger.Info("Using legacy Lua engine for Lua scripting")
		e.legacyEngine = NewLuaEngine()
	default:
		logger.Info("Using gopher-lua engine (default) for Lua scripting")
		e.engineType = EngineTypeGopherLua
		e.gopherEngine = NewGopherEngine(dbExec)
	}

	return e
}

// SetEngineType changes the engine type at runtime
func (e *Engine) SetEngineType(engineType EngineType) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.engineType == engineType {
		return
	}

	// Close existing engine
	if e.legacyEngine != nil {
		e.legacyEngine = nil
	}
	if e.gopherEngine != nil {
		e.gopherEngine.Close()
		e.gopherEngine = nil
	}

	// Create new engine
	e.engineType = engineType
	switch engineType {
	case EngineTypeGopherLua:
		logger.Info("Switching to gopher-lua engine")
		e.gopherEngine = NewGopherEngine(e.dbExec)
	case EngineTypeLegacy:
		logger.Info("Switching to legacy Lua engine")
		e.legacyEngine = NewLuaEngine()
	}
}

// GetEngineType returns the current engine type
func (e *Engine) GetEngineType() EngineType {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.engineType
}

// Eval executes a Lua script
func (e *Engine) Eval(script string, keys []string, args []string) (interface{}, error) {
	switch e.GetEngineType() {
	case EngineTypeGopherLua:
		if e.gopherEngine == nil {
			return nil, fmt.Errorf("gopher-lua engine not initialized")
		}
		return e.gopherEngine.Eval(script, keys, args)
	case EngineTypeLegacy:
		if e.legacyEngine == nil {
			return nil, fmt.Errorf("legacy Lua engine not initialized")
		}
		return e.legacyEngine.Execute(script, keys, args, e.dbExec)
	default:
		return nil, fmt.Errorf("unknown engine type")
	}
}

// EvalWithContext executes a Lua script with context (timeout/cancellation)
// Only supported by gopher-lua engine
func (e *Engine) EvalWithContext(ctx context.Context, script string, keys []string, args []string) (interface{}, error) {
	if e.GetEngineType() != EngineTypeGopherLua {
		return nil, fmt.Errorf("EvalWithContext is only supported by gopher-lua engine")
	}

	if e.gopherEngine == nil {
		return nil, fmt.Errorf("gopher-lua engine not initialized")
	}

	return e.gopherEngine.EvalWithContext(ctx, script, keys, args)
}

// EvalSha executes a script by SHA1
func (e *Engine) EvalSha(sha1 string, keys []string, args []string) (interface{}, error) {
	// Get script from shared scripts map
	e.mu.RLock()
	script, ok := e.scripts[sha1]
	e.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("NOSCRIPT No matching script. Please use EVAL.")
	}

	return e.Eval(script, keys, args)
}

// LoadScript loads a script and returns its SHA1
func (e *Engine) LoadScript(script string) string {
	sha := computeScriptSHA(script)

	e.mu.Lock()
	defer e.mu.Unlock()

	e.scripts[sha] = script
	return sha
}

// Exists checks if scripts exist
func (e *Engine) Exists(shas []string) []int {
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
func (e *Engine) Flush() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.scripts = make(map[string]string)
}

// Kill stops a running script
func (e *Engine) Kill() error {
	switch e.GetEngineType() {
	case EngineTypeGopherLua:
		if e.gopherEngine == nil {
			return fmt.Errorf("gopher-lua engine not initialized")
		}
		return e.gopherEngine.Kill()
	case EngineTypeLegacy:
		// Legacy engine doesn't support killing scripts
		return fmt.Errorf("ERR Legacy Lua engine does not support SCRIPT KILL")
	default:
		return fmt.Errorf("unknown engine type")
	}
}

// GetRunningScriptCount returns the number of currently running scripts
func (e *Engine) GetRunningScriptCount() int {
	if e.GetEngineType() == EngineTypeGopherLua && e.gopherEngine != nil {
		return e.gopherEngine.GetRunningScriptCount()
	}
	return 0
}

// Close closes the engine and releases resources
func (e *Engine) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.gopherEngine != nil {
		e.gopherEngine.Close()
		e.gopherEngine = nil
	}

	e.legacyEngine = nil
	e.scripts = nil
}

// GetScript returns a script by its SHA1
func (e *Engine) GetScript(sha string) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	script, ok := e.scripts[sha]
	return script, ok
}

// computeScriptSHA computes SHA1 hash of script
func computeScriptSHA(script string) string {
	h := sha1.New()
	h.Write([]byte(script))
	return hex.EncodeToString(h.Sum(nil))
}

// ConvertToRedisReply converts Go value to Redis reply
// This is a convenience wrapper for external callers
func (e *Engine) ConvertToRedisReply(v interface{}) redis.Reply {
	return ConvertToRedisReply(v)
}

// init determines the engine type from environment variable
func init() {
	// GODIS_LUA_ENGINE can be "legacy" or "gopher" (default)
	engineEnv := os.Getenv("GODIS_LUA_ENGINE")
	switch engineEnv {
	case "legacy":
		logger.Info("Environment variable GODIS_LUA_ENGINE=legacy detected")
	case "gopher", "":
		logger.Info("Environment variable GODIS_LUA_ENGINE=gopher (default)")
	default:
		logger.Warn("Unknown GODIS_LUA_ENGINE value:", engineEnv, ", using default (gopher)")
	}
}

// GetEngineTypeFromEnv returns the engine type from environment variable
func GetEngineTypeFromEnv() EngineType {
	engineEnv := os.Getenv("GODIS_LUA_ENGINE")
	switch engineEnv {
	case "legacy":
		return EngineTypeLegacy
	case "gopher", "":
		return EngineTypeGopherLua
	default:
		return EngineTypeGopherLua
	}
}
