package functions

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/linkerlin/godis/scripting"
)

// Function represents a Redis Function
type Function struct {
	Name        string
	Description string
	Flags       []string
	Code        string
	Library     string
	SHA         string
}

// IsReadOnly returns true if function is read-only
func (f *Function) IsReadOnly() bool {
	for _, flag := range f.Flags {
		if strings.EqualFold(flag, "readonly") {
			return true
		}
	}
	return false
}

// Library represents a library of functions
type Library struct {
	Name        string
	Engine      string
	Description string
	Functions   map[string]*Function
	Code        string
	SHA         string
}

// RunningFunction tracks an executing function
type RunningFunction struct {
	Name      string
	Library   string
	StartTime time.Time
	Cancel    chan struct{}
	Done      chan struct{}
}

// Engine manages Redis Functions
type Engine struct {
	libraries    map[string]*Library
	functions    map[string]*Function // Global function name -> Function
	scriptEngine *scripting.Engine
	dbExec       func(cmd string, args ...string) (interface{}, error)

	// Execution tracking for KILL
	running   *RunningFunction
	runningMu sync.RWMutex

	mu sync.RWMutex
}

// NewEngine creates a new Functions engine (FCALL uses gopher-lua via scripting.Engine).
func NewEngine(poolSize int) *Engine {
	return &Engine{
		libraries:    make(map[string]*Library),
		functions:    make(map[string]*Function),
		scriptEngine: scripting.NewEngine(nil),
		running:      nil,
	}
}

// SetDBExec sets the database execution function for Lua scripts
func (e *Engine) SetDBExec(dbExec func(cmd string, args ...string) (interface{}, error)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dbExec = dbExec
	e.scriptEngine = scripting.NewEngine(dbExec)
}

// LoadLibrary loads a library from code
// Returns the number of functions loaded and error
func (e *Engine) LoadLibrary(name, code string, replace bool) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Check if library exists
	if _, exists := e.libraries[name]; exists && !replace {
		return 0, fmt.Errorf("library '%s' already exists", name)
	}

	// Parse library
	library, err := e.parseLibrary(name, code)
	if err != nil {
		return 0, err
	}

	// Remove old library functions if replacing
	if oldLib, exists := e.libraries[name]; exists {
		for funcName := range oldLib.Functions {
			delete(e.functions, funcName)
		}
	}

	// Register new library
	e.libraries[name] = library

	// Register functions
	for funcName, fn := range library.Functions {
		e.functions[funcName] = fn
	}

	return len(library.Functions), nil
}

// parseLibrary parses library code and extracts functions
func (e *Engine) parseLibrary(name, code string) (*Library, error) {
	lib := &Library{
		Name:      name,
		Engine:    "LUA",
		Functions: make(map[string]*Function),
		Code:      code,
		SHA:       computeSHA(code),
	}

	// Extract description from first comment block
	lib.Description = extractDescription(code)

	// Parse function registrations
	// Format: redis.register_function('name', function(...) ... end)
	// Or: redis.register_function{
	//   name = 'name',
	//   callback = function(...) ... end,
	//   flags = { 'readonly' }
	// }

	// Simple regex-based parsing for function registrations
	registerPattern := regexp.MustCompile(`(?s)redis\.register_function\s*\(\s*['"]([^'"]+)['"]\s*,\s*(function\s*\([^)]*\)[^}]+)\s*\)`)
	matches := registerPattern.FindAllStringSubmatch(code, -1)

	for _, match := range matches {
		if len(match) >= 2 {
			funcName := match[1]
			funcCode := match[0]

			fn := &Function{
				Name:    funcName,
				Library: name,
				Code:    funcCode,
				SHA:     computeSHA(funcCode),
			}

			lib.Functions[funcName] = fn
		}
	}

	// Parse table-style registrations with flags
	tablePattern := regexp.MustCompile(`(?s)redis\.register_function\s*\{\s*name\s*=\s*['"]([^'"]+)['"]\s*,\s*callback\s*=\s*(function\s*\([^)]*\)[^}]+)\s*(?:,\s*flags\s*=\s*\{([^}]*)\})?\s*\}`)
	tableMatches := tablePattern.FindAllStringSubmatch(code, -1)

	for _, match := range tableMatches {
		if len(match) >= 2 {
			funcName := match[1]
			funcCode := match[0]
			flagsStr := ""
			if len(match) >= 4 {
				flagsStr = match[3]
			}

			// Parse flags
			var flags []string
			if flagsStr != "" {
				flagPattern := regexp.MustCompile(`['"]([^'"]+)['"]`)
				flagMatches := flagPattern.FindAllStringSubmatch(flagsStr, -1)
				for _, fm := range flagMatches {
					if len(fm) >= 2 {
						flags = append(flags, fm[1])
					}
				}
			}

			fn := &Function{
				Name:    funcName,
				Library: name,
				Flags:   flags,
				Code:    funcCode,
				SHA:     computeSHA(funcCode),
			}

			lib.Functions[funcName] = fn
		}
	}

	if len(lib.Functions) == 0 {
		return nil, fmt.Errorf("no functions found in library")
	}

	return lib, nil
}

// DeleteLibrary deletes a library and its functions
func (e *Engine) DeleteLibrary(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	lib, exists := e.libraries[name]
	if !exists {
		return fmt.Errorf("library '%s' not found", name)
	}

	// Remove functions
	for funcName := range lib.Functions {
		delete(e.functions, funcName)
	}

	delete(e.libraries, name)
	return nil
}

// GetFunction gets a function by name
func (e *Engine) GetFunction(name string) (*Function, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	fn, exists := e.functions[name]
	return fn, exists
}

// GetLibrary gets a library by name
func (e *Engine) GetLibrary(name string) (*Library, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	lib, exists := e.libraries[name]
	return lib, exists
}

// ListLibraries returns all library names
func (e *Engine) ListLibraries() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	names := make([]string, 0, len(e.libraries))
	for name := range e.libraries {
		names = append(names, name)
	}
	return names
}

// ListFunctions returns all function names
func (e *Engine) ListFunctions() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	names := make([]string, 0, len(e.functions))
	for name := range e.functions {
		names = append(names, name)
	}
	return names
}

// FlushAll deletes all libraries and functions
func (e *Engine) FlushAll() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.libraries = make(map[string]*Library)
	e.functions = make(map[string]*Function)

	return nil
}

// Call executes a function using Lua engine
func (e *Engine) Call(functionName string, keys []string, args []string) (interface{}, error) {
	fn, exists := e.GetFunction(functionName)
	if !exists {
		return nil, fmt.Errorf("function '%s' not found", functionName)
	}

	// Get library code
	lib, _ := e.GetLibrary(fn.Library)
	if lib == nil {
		return nil, fmt.Errorf("library '%s' not found", fn.Library)
	}

	// Build Lua script to execute
	// Wrap the library code and call the registered function
	script := e.buildExecutionScript(lib.Code, functionName, keys, args)

	// Create execution context with cancel
	cancel := make(chan struct{})
	done := make(chan struct{})

	running := &RunningFunction{
		Name:      functionName,
		Library:   fn.Library,
		StartTime: time.Now(),
		Cancel:    cancel,
		Done:      done,
	}

	// Register as running
	e.runningMu.Lock()
	if e.running != nil {
		e.runningMu.Unlock()
		return nil, fmt.Errorf("another function is currently running")
	}
	e.running = running
	e.runningMu.Unlock()

	// Clean up when done
	defer func() {
		close(done)
		e.runningMu.Lock()
		e.running = nil
		e.runningMu.Unlock()
	}()

	// Execute using gopher-lua (scripting.Engine); cancel channel maps to context.
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	go func() {
		select {
		case <-cancel:
			cancelFn()
		case <-ctx.Done():
		}
	}()

	result, err := e.scriptEngine.EvalWithContext(ctx, script, keys, args)
	if err != nil {
		return nil, fmt.Errorf("function execution failed: %v", err)
	}

	return result, nil
}

// GetRunningFunction returns currently running function info
func (e *Engine) GetRunningFunction() *RunningFunction {
	e.runningMu.RLock()
	defer e.runningMu.RUnlock()
	return e.running
}

// KillRunningFunction kills the currently running function
func (e *Engine) KillRunningFunction() error {
	e.runningMu.RLock()
	running := e.running
	e.runningMu.RUnlock()

	if running == nil {
		return fmt.Errorf("no running function to kill")
	}

	// Send cancel signal
	close(running.Cancel)

	// Wait for function to stop (with timeout)
	select {
	case <-running.Done:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout waiting for function to stop")
	}
}

// buildExecutionScript builds a Lua script that sets up the environment and calls the function
func (e *Engine) buildExecutionScript(libCode, funcName string, keys, args []string) string {
	// Strip Redis shebang lines (#!lua ...) — not valid Lua.
	body := stripFunctionShebang(libCode)
	// GopherEngine already provides redis.call/pcall; only add register_function.
	script := `
local __godis_funcs = {}
redis.register_function = function(name, callback, flags)
	if type(name) == "table" then
		__godis_funcs[name.name] = name.callback
	else
		__godis_funcs[name] = callback
	end
end

` + body + `

local __fn = __godis_funcs["` + funcName + `"]
if __fn == nil then
	error("Function not found")
end
return __fn(KEYS, ARGV)
`
	return script
}

func stripFunctionShebang(code string) string {
	lines := strings.Split(code, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#!") {
			continue
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

// Stats returns engine statistics
func (e *Engine) Stats() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return map[string]interface{}{
		"libraries": len(e.libraries),
		"functions": len(e.functions),
		"engine":    "LUA",
	}
}

func computeSHA(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func extractDescription(code string) string {
	// Extract first block comment as description
	lines := strings.Split(code, "\n")
	var desc []string
	inComment := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		if strings.HasPrefix(trimmed, "--[[") {
			inComment = true
			trimmed = strings.TrimPrefix(trimmed, "--[[")
		}

		if inComment {
			if strings.HasSuffix(trimmed, "]]") {
				trimmed = strings.TrimSuffix(trimmed, "]]")
				desc = append(desc, trimmed)
				break
			}
			desc = append(desc, trimmed)
		}
	}

	return strings.Join(desc, " ")
}
