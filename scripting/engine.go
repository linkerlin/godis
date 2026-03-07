package scripting

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/hdt3213/godis/interface/redis"
)

// Engine is the Lua scripting engine
type Engine struct {
	scripts map[string]string // SHA1 -> script body
	lua     *LuaEngine        // Built-in Lua interpreter
	mu      sync.RWMutex
	
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
		return &NullBulkReply{}
	}
	
	switch val := v.(type) {
	case string:
		return MakeBulkReply([]byte(val))
	case int:
		return MakeIntReply(int64(val))
	case int64:
		return MakeIntReply(val)
	case float64:
		return MakeBulkReply([]byte(fmt.Sprintf("%g", val)))
	case bool:
		if val {
			return MakeIntReply(1)
		}
		return MakeIntReply(0)
	case []interface{}:
		var elems [][]byte
		for _, elem := range val {
			r := ConvertToRedisReply(elem)
			elems = append(elems, r.ToBytes())
		}
		return MakeMultiBulkReply(elems)
	case map[string]interface{}:
		var elems [][]byte
		for k, v := range val {
			elems = append(elems, []byte(k))
			r := ConvertToRedisReply(v)
			elems = append(elems, r.ToBytes())
		}
		return MakeMultiBulkReply(elems)
	case error:
		return MakeErrReply(val.Error())
	default:
		return MakeBulkReply([]byte(fmt.Sprintf("%v", val)))
	}
}

// Helper types for compilation
type NullBulkReply struct{}

func (r *NullBulkReply) ToBytes() []byte {
	return []byte("$-1\r\n")
}

type BulkReply struct {
	Arg []byte
}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{Arg: arg}
}

func (r *BulkReply) ToBytes() []byte {
	if r.Arg == nil {
		return []byte("$-1\r\n")
	}
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(r.Arg), string(r.Arg)))
}

type IntReply struct {
	Value int64
}

func MakeIntReply(val int64) *IntReply {
	return &IntReply{Value: val}
}

func (r *IntReply) ToBytes() []byte {
	return []byte(fmt.Sprintf(":%d\r\n", r.Value))
}

type MultiBulkReply struct {
	Args [][]byte
}

func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{Args: args}
}

func (r *MultiBulkReply) ToBytes() []byte {
	var buf []byte
	buf = append(buf, fmt.Sprintf("*%d\r\n", len(r.Args))...)
	for _, arg := range r.Args {
		if arg == nil {
			buf = append(buf, "$-1\r\n"...)
		} else {
			buf = append(buf, fmt.Sprintf("$%d\r\n", len(arg))...)
			buf = append(buf, arg...)
			buf = append(buf, "\r\n"...)
		}
	}
	return buf
}

type ErrReply struct {
	Message string
}

func MakeErrReply(msg string) *ErrReply {
	return &ErrReply{Message: msg}
}

func (r *ErrReply) ToBytes() []byte {
	return []byte(fmt.Sprintf("-ERR %s\r\n", r.Message))
}
