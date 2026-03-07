package scripting

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// LuaEngine is a simplified Lua interpreter for Redis
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
	}
	
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
		} else if strings.Contains(line, "=") && !strings.Contains(line, "==") {
			// Variable assignment
			if err := ctx.handleAssignment(line); err != nil {
				return nil, err
			}
		}
	}
	
	return nil, nil
}

// handleRedisCall handles redis.call('cmd', ...)
func (ctx *luaContext) handleRedisCall(line string) (interface{}, error) {
	// Parse: redis.call('CMD', key1, key2, arg1, arg2)
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
		} else if !inString && ch == ',' {
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
	
	// Handle table/array literals
	if strings.HasPrefix(expr, "{") && strings.HasSuffix(expr, "}") {
		return ctx.parseTable(expr)
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
	if strings.Contains(content, "=") {
		// Map
		result := make(map[string]interface{})
		pairs := strings.Split(content, ",")
		
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
	items := strings.Split(content, ",")
	
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item != "" {
			value, _ := ctx.evaluateExpression(item)
			result = append(result, value)
		}
	}
	
	return result, nil
}

// evalArithmetic evaluates simple arithmetic expressions
func (ctx *luaContext) evalArithmetic(expr string) (interface{}, error) {
	// Simple arithmetic: a + b, a - b, a * b, a / b
	
	for _, op := range []string{"+", "-", "*", "/"} {
		if strings.Contains(expr, op) {
			parts := strings.Split(expr, op)
			if len(parts) == 2 {
				left, _ := ctx.evaluateExpression(strings.TrimSpace(parts[0]))
				right, _ := ctx.evaluateExpression(strings.TrimSpace(parts[1]))
				
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


