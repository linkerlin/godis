// Package scripting provides Lua script execution support for Godis
package scripting

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
)

// Engine is the Lua script execution engine
type Engine struct {
	mu          sync.RWMutex
	db          database.DBEngine
	scripts     *dict.ConcurrentDict // SHA1 -> script content
	shaToScript map[string]string
}

// NewEngine creates a new Lua scripting engine
func NewEngine(db database.DBEngine) *Engine {
	return &Engine{
		db:          db,
		scripts:     dict.MakeConcurrent(64),
		shaToScript: make(map[string]string),
	}
}

// Execute executes a Lua script
func (e *Engine) Execute(script string, keys []string, args []string) redis.Reply {
	// TODO: Implement Lua execution using gopher-lua
	// For now, return a placeholder
	return protocol.MakeErrReply("ERR Lua scripting not yet fully implemented")
}

// ExecuteSHA executes a script by its SHA1 hash
func (e *Engine) ExecuteSHA(sha1 string, keys []string, args []string) redis.Reply {
	e.mu.RLock()
	script, exists := e.shaToScript[sha1]
	e.mu.RUnlock()
	
	if !exists {
		return protocol.MakeErrReply("NOSCRIPT No matching script. Please use EVAL.")
	}
	
	return e.Execute(script, keys, args)
}

// ScriptLoad loads a script into the engine
func (e *Engine) ScriptLoad(script string) string {
	hash := calculateSHA1(script)
	
	e.mu.Lock()
	defer e.mu.Unlock()
	
	e.shaToScript[hash] = script
	return hash
}

// ScriptExists checks if scripts exist
func (e *Engine) ScriptExists(sha1s []string) []int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	result := make([]int, len(sha1s))
	for i, sha := range sha1s {
		if _, exists := e.shaToScript[sha]; exists {
			result[i] = 1
		} else {
			result[i] = 0
		}
	}
	return result
}

// ScriptFlush flushes all scripts
func (e *Engine) ScriptFlush() {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	e.shaToScript = make(map[string]string)
}

// calculateSHA1 calculates SHA1 hash of a string
func calculateSHA1(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

// RedisValue represents a value in Lua scripts
type RedisValue struct {
	Value interface{}
}

// ToLuaValue converts a Redis value to Lua value
func ToLuaValue(val interface{}) interface{} {
	switch v := val.(type) {
	case []byte:
		return string(v)
	case string:
		return v
	case int64:
		return float64(v)
	case bool:
		return v
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = ToLuaValue(item)
		}
		return result
	default:
		if val == nil {
			return nil
		}
		return fmt.Sprintf("%v", val)
	}
}

// FromLuaValue converts a Lua value to Redis value
func FromLuaValue(val interface{}) interface{} {
	switch v := val.(type) {
	case string:
		return []byte(v)
	case float64:
		// Check if it's an integer
		if v == float64(int64(v)) {
			return int64(v)
		}
		return fmt.Sprintf("%v", v)
	case bool:
		return v
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = FromLuaValue(item)
		}
		return result
	case map[interface{}]interface{}:
		// Convert to table-like structure
		result := make(map[string]interface{})
		for k, val := range v {
			key := fmt.Sprintf("%v", k)
			result[key] = FromLuaValue(val)
		}
		return result
	default:
		if val == nil {
			return nil
		}
		return fmt.Sprintf("%v", val)
	}
}

// LuaReplyToRedisReply converts a Lua return value to Redis reply
func LuaReplyToRedisReply(val interface{}) redis.Reply {
	if val == nil {
		return &protocol.NullBulkReply{}
	}
	
	switch v := val.(type) {
	case string:
		return protocol.MakeBulkReply([]byte(v))
	case float64:
		return protocol.MakeIntReply(int64(v))
	case bool:
		if v {
			return protocol.MakeIntReply(1)
		}
		return protocol.MakeIntReply(0)
	case []interface{}:
		if len(v) == 0 {
			return protocol.MakeEmptyMultiBulkReply()
		}
		
		// Check if it's a status reply (table with "ok" field)
		if len(v) == 1 {
			if m, ok := v[0].(map[interface{}]interface{}); ok {
				if okVal, exists := m["ok"]; exists {
					return protocol.MakeStatusReply(fmt.Sprintf("%v", okVal))
				}
				if errVal, exists := m["err"]; exists {
					return protocol.MakeErrReply(fmt.Sprintf("%v", errVal))
				}
			}
		}
		
		// Regular array
		result := make([][]byte, len(v))
		for i, item := range v {
			if item == nil {
				result[i] = (&protocol.NullBulkReply{}).ToBytes()
			} else {
				result[i] = []byte(fmt.Sprintf("%v", item))
			}
		}
		return protocol.MakeMultiBulkReply(result)
	default:
		return protocol.MakeBulkReply([]byte(fmt.Sprintf("%v", v)))
	}
}

// Helper functions for Redis Lua API

// RedisCallArgs represents arguments for redis.call/pcall
type RedisCallArgs struct {
	Cmd  string
	Args []string
}

// ParseRedisCall parses redis.call arguments from Lua
func ParseRedisCall(args []interface{}) (*RedisCallArgs, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("wrong number of arguments")
	}
	
	cmd, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("command must be a string")
	}
	
	cmd = strings.ToUpper(cmd)
	callArgs := &RedisCallArgs{
		Cmd:  cmd,
		Args: make([]string, len(args)-1),
	}
	
	for i := 1; i < len(args); i++ {
		callArgs.Args[i-1] = fmt.Sprintf("%v", args[i])
	}
	
	return callArgs, nil
}
