package functions

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"sync"
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

// Engine manages Redis Functions
type Engine struct {
	libraries map[string]*Library
	functions map[string]*Function // Global function name -> Function
	
	mu        sync.RWMutex
}

// NewEngine creates a new Functions engine
func NewEngine(poolSize int) *Engine {
	return &Engine{
		libraries: make(map[string]*Library),
		functions: make(map[string]*Function),
	}
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

// Call executes a function (simplified - returns function info)
// Note: Full Lua execution requires github.com/yuin/gopher-lua
func (e *Engine) Call(functionName string, keys []string, args []string) (interface{}, error) {
	fn, exists := e.GetFunction(functionName)
	if !exists {
		return nil, fmt.Errorf("function '%s' not found", functionName)
	}
	
	// Simplified: return function metadata
	// In full implementation, this would execute the Lua code
	return map[string]interface{}{
		"function": fn.Name,
		"library":  fn.Library,
		"keys":     keys,
		"args":     args,
		"result":   "Lua execution requires gopher-lua module",
	}, nil
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
