package scripting

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// LuaEngine is an enhanced simplified Lua interpreter for Redis
type LuaEngine struct {
	mu sync.Mutex
}

// NewLuaEngine creates a new Lua engine
func NewLuaEngine() *LuaEngine {
	return &LuaEngine{}
}

// Execute executes Lua code with Redis API
func (e *LuaEngine) Execute(code string, keys []string, args []string, callFunc func(cmd string, args ...string) (interface{}, error)) (interface{}, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	// Create execution context
	ctx := &luaContext{
		keys:     keys,
		args:     args,
		callFunc: callFunc,
		vars:     make(map[string]interface{}),
		globals:  make(map[string]interface{}),
	}
	
	// Set up global tables
	ctx.setupGlobals()
	
	// Execute the Lua code
	result, err := ctx.execute(code)
	if err != nil {
		return nil, fmt.Errorf("ERR Error running script: %v", err)
	}
	
	return result, nil
}

// luaContext holds execution state
type luaContext struct {
	keys     []string
	args     []string
	callFunc func(cmd string, args ...string) (interface{}, error)
	vars     map[string]interface{}
	globals  map[string]interface{}
}

// setupGlobals initializes global variables and functions
func (ctx *luaContext) setupGlobals() {
	// Create KEYS table
	ctx.globals["KEYS"] = ctx.keys
	
	// Create ARGV table
	ctx.globals["ARGV"] = ctx.args
}

// execute runs the Lua code
func (ctx *luaContext) execute(code string) (interface{}, error) {
	// Remove comments
	code = removeComments(code)
	
	// Find return statement
	returnRegex := regexp.MustCompile(`(?i)return\s+(.+)$`)
	matches := returnRegex.FindStringSubmatch(code)
	
	if len(matches) > 1 {
		// Execute the code and return the result
		retExpr := matches[1]
		ctx.runStatements(code)
		return ctx.evaluateExpression(retExpr)
	}
	
	// No return, just execute
	return ctx.runStatements(code)
}

// runStatements runs Lua statements
func (ctx *luaContext) runStatements(code string) (interface{}, error) {
	lines := strings.Split(code, "\n")
	
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		
		// Handle redis.call
		if strings.Contains(line, "redis.call") {
			if _, err := ctx.handleRedisCall(line); err != nil {
				return nil, err
			}
		} else if strings.Contains(line, "redis.pcall") {
			if _, err := ctx.handleRedisPCall(line); err != nil {
				return nil, err
			}
		} else if strings.Contains(line, "redis.sha1hex") {
			if _, err := ctx.handleRedisSha1Hex(line); err != nil {
				return nil, err
			}
		} else if strings.Contains(line, "redis.log") {
			ctx.handleRedisLog(line)
		} else if strings.Contains(line, "=") && !strings.Contains(line, "==") && !strings.Contains(line, "~=") {
			// Variable assignment (but not comparison)
			if err := ctx.handleAssignment(line); err != nil {
				return nil, err
			}
		} else if strings.HasPrefix(line, "local ") {
			// Local variable declaration
			if err := ctx.handleLocalAssignment(line); err != nil {
				return nil, err
			}
		}
	}
	
	return nil, nil
}

// handleRedisCall handles redis.call('cmd', ...)
func (ctx *luaContext) handleRedisCall(line string) (interface{}, error) {
	cmd, args, err := ctx.parseRedisCall(line, "redis.call")
	if err != nil {
		return nil, err
	}
	
	return ctx.callFunc(cmd, args...)
}

// handleRedisPCall handles redis.pcall (protected call)
func (ctx *luaContext) handleRedisPCall(line string) (interface{}, error) {
	cmd, args, err := ctx.parseRedisCall(line, "redis.pcall")
	if err != nil {
		// pcall returns error as result, doesn't throw
		return map[string]interface{}{"err": err.Error()}, nil
	}
	
	result, err := ctx.callFunc(cmd, args...)
	if err != nil {
		return map[string]interface{}{"err": err.Error()}, nil
	}
	
	return result, nil
}

// handleRedisSha1Hex handles redis.sha1hex
func (ctx *luaContext) handleRedisSha1Hex(line string) (interface{}, error) {
	// Extract argument
	startIdx := strings.Index(line, "(")
	endIdx := strings.LastIndex(line, ")")
	if startIdx == -1 || endIdx == -1 {
		return nil, fmt.Errorf("invalid sha1hex syntax")
	}
	
	content := line[startIdx+1 : endIdx]
	arg := ctx.evalArg(strings.TrimSpace(content))
	
	hash := sha1.Sum([]byte(arg))
	return hex.EncodeToString(hash[:]), nil
}

// handleRedisLog handles redis.log
func (ctx *luaContext) handleRedisLog(line string) {
	// Parse: redis.log(level, message)
	// Simplified - just print to stdout
	startIdx := strings.Index(line, "(")
	endIdx := strings.LastIndex(line, ")")
	if startIdx == -1 || endIdx == -1 {
		return
	}
	
	content := line[startIdx+1 : endIdx]
	args := ctx.parseArguments(content)
	
	if len(args) >= 2 {
		level := ctx.evalArg(args[0])
		msg := ctx.evalArg(args[1])
		fmt.Printf("[REDIS LOG %s] %s\n", level, msg)
	}
}

// parseRedisCall parses a redis.call/pcall line
func (ctx *luaContext) parseRedisCall(line string, prefix string) (string, []string, error) {
	// Extract content inside parentheses
	startIdx := strings.Index(line, "(")
	endIdx := strings.LastIndex(line, ")")
	
	if startIdx == -1 || endIdx == -1 || startIdx >= endIdx {
		return "", nil, fmt.Errorf("invalid call syntax")
	}
	
	content := line[startIdx+1 : endIdx]
	
	// Parse arguments
	args := ctx.parseArguments(content)
	if len(args) == 0 {
		return "", nil, fmt.Errorf("no command specified")
	}
	
	// First arg is the command
	cmd := ctx.evalArg(args[0])
	if cmd == "" {
		return "", nil, fmt.Errorf("empty command")
	}
	
	// Convert remaining args
	var result []string
	for i := 1; i < len(args); i++ {
		arg := ctx.evalArg(args[i])
		result = append(result, arg)
	}
	
	return strings.ToUpper(cmd), result, nil
}

// parseArguments parses comma-separated arguments
func (ctx *luaContext) parseArguments(content string) []string {
	var args []string
	var current strings.Builder
	inString := false
	stringChar := byte(0)
	depth := 0
	
	for i := 0; i < len(content); i++ {
		ch := content[i]
		
		if !inString && (ch == '"' || ch == '\'') {
			inString = true
			stringChar = ch
			if current.Len() > 0 {
				args = append(args, strings.TrimSpace(current.String()))
				current.Reset()
			}
		} else if inString && ch == stringChar {
			inString = false
			args = append(args, current.String())
			current.Reset()
		} else if !inString && (ch == '(' || ch == '{' || ch == '[') {
			depth++
			current.WriteByte(ch)
		} else if !inString && (ch == ')' || ch == '}' || ch == ']') {
			depth--
			current.WriteByte(ch)
		} else if !inString && ch == ',' && depth == 0 {
			if current.Len() > 0 {
				args = append(args, strings.TrimSpace(current.String()))
				current.Reset()
			}
		} else {
			current.WriteByte(ch)
		}
	}
	
	if current.Len() > 0 {
		arg := strings.TrimSpace(current.String())
		if arg != "" {
			args = append(args, arg)
		}
	}
	
	return args
}

// evalArg evaluates an argument (handle KEYS[i], ARGV[i], variables)
func (ctx *luaContext) evalArg(arg string) string {
	arg = strings.TrimSpace(arg)
	
	// Handle string literals (already extracted)
	if !strings.Contains(arg, "[") && !strings.Contains(arg, "]") {
		// Check for global variable
		if val, ok := ctx.vars[arg]; ok {
			return fmt.Sprintf("%v", val)
		}
		return arg
	}
	
	// Handle KEYS[i]
	if strings.HasPrefix(arg, "KEYS[") {
		idxStr := arg[5 : len(arg)-1]
		idx, err := strconv.Atoi(idxStr)
		if err == nil && idx >= 1 && idx <= len(ctx.keys) {
			return ctx.keys[idx-1]
		}
		return ""
	}
	
	// Handle ARGV[i]
	if strings.HasPrefix(arg, "ARGV[") {
		idxStr := arg[5 : len(arg)-1]
		idx, err := strconv.Atoi(idxStr)
		if err == nil && idx >= 1 && idx <= len(ctx.args) {
			return ctx.args[idx-1]
		}
		return ""
	}
	
	// Handle variables with array access
	if idx := strings.Index(arg, "["); idx > 0 {
		varName := arg[:idx]
		if val, ok := ctx.vars[varName]; ok {
			if arr, ok := val.([]interface{}); ok {
				idxStr := arg[idx+1 : len(arg)-1]
				idx, err := strconv.Atoi(idxStr)
				if err == nil && idx >= 1 && idx <= len(arr) {
					return fmt.Sprintf("%v", arr[idx-1])
				}
			}
		}
	}
	
	// Handle variables
	if val, ok := ctx.vars[arg]; ok {
		return fmt.Sprintf("%v", val)
	}
	
	return arg
}

// handleAssignment handles variable assignment
func (ctx *luaContext) handleAssignment(line string) error {
	parts := strings.SplitN(line, "=", 2)
	if len(parts) != 2 {
		return nil
	}
	
	varName := strings.TrimSpace(parts[0])
	valueExpr := strings.TrimSpace(parts[1])
	
	// Evaluate the value
	value, err := ctx.evaluateExpression(valueExpr)
	if err != nil {
		return err
	}
	
	ctx.vars[varName] = value
	return nil
}

// handleLocalAssignment handles local variable declaration
func (ctx *luaContext) handleLocalAssignment(line string) error {
	// Remove "local " prefix
	line = strings.TrimPrefix(line, "local ")
	return ctx.handleAssignment(line)
}

// evaluateExpression evaluates a Lua expression
func (ctx *luaContext) evaluateExpression(expr string) (interface{}, error) {
	expr = strings.TrimSpace(expr)
	
	// Handle redis.call in expression
	if strings.Contains(expr, "redis.call") {
		return ctx.handleRedisCall(expr)
	}
	if strings.Contains(expr, "redis.pcall") {
		return ctx.handleRedisPCall(expr)
	}
	if strings.Contains(expr, "redis.sha1hex") {
		return ctx.handleRedisSha1Hex(expr)
	}
	
	// Handle table/array literals
	if strings.HasPrefix(expr, "{") && strings.HasSuffix(expr, "}") {
		return ctx.parseTable(expr)
	}
	
	// Handle string concatenation (..)
	if strings.Contains(expr, "..") {
		return ctx.evalConcatenation(expr)
	}
	
	// Handle string
	if (strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'")) ||
		(strings.HasPrefix(expr, "\"") && strings.HasSuffix(expr, "\"")) {
		return expr[1 : len(expr)-1], nil
	}
	
	// Handle number
	if num, err := strconv.ParseFloat(expr, 64); err == nil {
		return num, nil
	}
	
	// Handle boolean
	if expr == "true" {
		return true, nil
	}
	if expr == "false" {
		return false, nil
	}
	if expr == "nil" {
		return nil, nil
	}
	
	// Handle variable
	if val, ok := ctx.vars[expr]; ok {
		return val, nil
	}
	
	// Handle KEYS/ARGV
	if strings.HasPrefix(expr, "KEYS[") || strings.HasPrefix(expr, "ARGV[") {
		return ctx.evalArg(expr), nil
	}
	
	// Handle arithmetic (simplified)
	return ctx.evalArithmetic(expr)
}

// parseTable parses a Lua table literal
func (ctx *luaContext) parseTable(expr string) (interface{}, error) {
	content := expr[1 : len(expr)-1]
	content = strings.TrimSpace(content)
	
	// Check if it's an array or map
	if strings.Contains(content, "=") && !strings.Contains(content, "==") {
		// Map
		result := make(map[string]interface{})
		pairs := splitTopLevel(content, ',')
		
		for _, pair := range pairs {
			pair = strings.TrimSpace(pair)
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) == 2 {
				key := strings.Trim(strings.TrimSpace(kv[0]), `"'`)
				value, _ := ctx.evaluateExpression(strings.TrimSpace(kv[1]))
				result[key] = value
			}
		}
		return result, nil
	}
	
	// Array
	var result []interface{}
	items := splitTopLevel(content, ',')
	
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item != "" {
			value, _ := ctx.evaluateExpression(item)
			result = append(result, value)
		}
	}
	
	return result, nil
}

// splitTopLevel splits a string by delimiter, respecting nested structures
func splitTopLevel(s string, delim byte) []string {
	var result []string
	var current strings.Builder
	inString := false
	stringChar := byte(0)
	depth := 0
	
	for i := 0; i < len(s); i++ {
		ch := s[i]
		
		if !inString && (ch == '"' || ch == '\'') {
			inString = true
			stringChar = ch
		} else if inString && ch == stringChar {
			inString = false
		} else if !inString && (ch == '(' || ch == '{' || ch == '[') {
			depth++
		} else if !inString && (ch == ')' || ch == '}' || ch == ']') {
			depth--
		}
		
		if !inString && depth == 0 && ch == delim {
			if current.Len() > 0 {
				result = append(result, current.String())
				current.Reset()
			}
		} else {
			current.WriteByte(ch)
		}
	}
	
	if current.Len() > 0 {
		result = append(result, current.String())
	}
	
	return result
}

// evalConcatenation handles string concatenation (..)
func (ctx *luaContext) evalConcatenation(expr string) (interface{}, error) {
	parts := strings.Split(expr, "..")
	var result strings.Builder
	
	for _, part := range parts {
		val, err := ctx.evaluateExpression(strings.TrimSpace(part))
		if err != nil {
			return nil, err
		}
		result.WriteString(fmt.Sprintf("%v", val))
	}
	
	return result.String(), nil
}

// evalArithmetic evaluates simple arithmetic expressions
func (ctx *luaContext) evalArithmetic(expr string) (interface{}, error) {
	// Handle parentheses first
	expr = strings.TrimSpace(expr)
	if strings.HasPrefix(expr, "(") && strings.HasSuffix(expr, ")") {
		return ctx.evaluateExpression(expr[1 : len(expr)-1])
	}
	
	// Simple arithmetic: a + b, a - b, a * b, a / b
	for _, op := range []string{"+", "-", "*", "/"} {
		if idx := strings.Index(expr, op); idx > 0 {
			leftStr := strings.TrimSpace(expr[:idx])
			rightStr := strings.TrimSpace(expr[idx+1:])
			
			left, err := ctx.evaluateExpression(leftStr)
			if err != nil {
				return expr, nil // Not arithmetic, return as is
			}
			
			right, err := ctx.evaluateExpression(rightStr)
			if err != nil {
				return expr, nil
			}
			
			lNum, lok := toFloat(left)
			rNum, rok := toFloat(right)
			
			if lok && rok {
				switch op {
				case "+":
					return lNum + rNum, nil
				case "-":
					return lNum - rNum, nil
				case "*":
					return lNum * rNum, nil
				case "/":
					if rNum != 0 {
						return lNum / rNum, nil
					}
					return nil, fmt.Errorf("division by zero")
				}
			}
		}
	}
	
	return expr, nil
}

// toFloat converts value to float64
func toFloat(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// removeComments removes Lua comments
func removeComments(code string) string {
	lines := strings.Split(code, "\n")
	var result []string
	
	inBlockComment := false
	
	for _, line := range lines {
		if inBlockComment {
			if idx := strings.Index(line, "]]"); idx != -1 {
				inBlockComment = false
				line = line[idx+2:]
			} else {
				continue
			}
		}
		
		// Handle block comment start
		if idx := strings.Index(line, "--[["); idx != -1 {
			if endIdx := strings.Index(line[idx+3:], "]]"); endIdx != -1 {
				line = line[:idx] + line[idx+3+endIdx+2:]
			} else {
				inBlockComment = true
				line = line[:idx]
			}
		}
		
		// Handle line comments
		if idx := strings.Index(line, "--"); idx != -1 {
			line = line[:idx]
		}
		
		result = append(result, line)
	}
	
	return strings.Join(result, "\n")
}
