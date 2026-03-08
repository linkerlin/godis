package scripting

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
)

// Engine is the Lua scripting engine
type Engine struct {
	mu      sync.RWMutex
	scripts map[string]string // SHA1 -> script body
	lua     *LuaEngine        // Built-in Lua interpreter
	
	// Reference to database for redis.call
	dbExec func(cmd string, args ...string) (interface{}, error)
}

// NewEngine creates a new scripting engine
func NewEngine(dbExec func(cmd string, args ...string) (interface{}, error)) *Engine {
	return &Engine{
		scripts: make(map[string]string),
		lua:     NewLuaEngine(),
		dbExec:  dbExec,
	}
}

// Eval executes a Lua script
func (e *Engine) Eval(script string, keys []string, args []string) (interface{}, error) {
	return e.lua.Execute(script, keys, args, e.dbExec)
}

// EvalSha executes a script by SHA1
func (e *Engine) EvalSha(sha1 string, keys []string, args []string) (interface{}, error) {
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

// Kill stops a running script (simplified - not implemented)
func (e *Engine) Kill() error {
	return fmt.Errorf("NOT implemented")
}

// computeScriptSHA computes SHA1 hash of script
func computeScriptSHA(script string) string {
	h := sha1.New()
	h.Write([]byte(script))
	return hex.EncodeToString(h.Sum(nil))
}

// ConvertToRedisReply converts Go value to Redis reply
func ConvertToRedisReply(v interface{}) redis.Reply {
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
			r := ConvertToRedisReply(elem)
			elems = append(elems, r.ToBytes())
		}
		return protocol.MakeMultiBulkReply(elems)
	case map[string]interface{}:
		// Check for error reply
		if errVal, ok := val["err"]; ok {
			return protocol.MakeErrReply(fmt.Sprintf("-%v", errVal))
		}
		// Check for status reply
		if okVal, ok := val["ok"]; ok {
			return protocol.MakeStatusReply(fmt.Sprintf("%v", okVal))
		}
		var elems [][]byte
		for k, v := range val {
			elems = append(elems, []byte(k))
			r := ConvertToRedisReply(v)
			elems = append(elems, r.ToBytes())
		}
		return protocol.MakeMultiBulkReply(elems)
	case error:
		return protocol.MakeErrReply(val.Error())
	default:
		return protocol.MakeBulkReply([]byte(fmt.Sprintf("%v", val)))
	}
}
