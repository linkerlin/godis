package scripting

import (
	"fmt"
	"strings"
	"sync"
)

// DebugMode represents the script debug mode
type DebugMode int

const (
	DebugModeNo DebugMode = iota
	DebugModeYes
	DebugModeSync
)

// Debugger provides Lua script debugging capabilities
type Debugger struct {
	mu sync.RWMutex
	
	mode DebugMode
	
	// Breakpoints: line number -> condition
	breakpoints map[int]string
	
	// Current execution state
	currentScript string
	currentLine   int
	currentVars   map[string]interface{}
	
	// Execution control
	stepMode      bool // Step into next line
	nextMode      bool // Step over (next line)
	finishMode    bool // Run until return
	paused        bool
	pauseCond     chan struct{}
	
	// Call stack
	callStack []CallFrame
}

// CallFrame represents a function call frame
type CallFrame struct {
	FunctionName string
	Line         int
	Locals       map[string]interface{}
}

// NewDebugger creates a new script debugger
func NewDebugger() *Debugger {
	return &Debugger{
		breakpoints: make(map[int]string),
		currentVars: make(map[string]interface{}),
		pauseCond:   make(chan struct{}),
		callStack:   make([]CallFrame, 0),
	}
}

// SetMode sets the debug mode
func (d *Debugger) SetMode(mode DebugMode) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.mode = mode
}

// GetMode returns current debug mode
func (d *Debugger) GetMode() DebugMode {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.mode
}

// IsDebugging returns true if debugging is enabled
func (d *Debugger) IsDebugging() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.mode != DebugModeNo
}

// AddBreakpoint adds a breakpoint at a line
func (d *Debugger) AddBreakpoint(line int, condition string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	
	d.breakpoints[line] = condition
	return nil
}

// RemoveBreakpoint removes a breakpoint
func (d *Debugger) RemoveBreakpoint(line int) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	
	delete(d.breakpoints, line)
	return nil
}

// ClearBreakpoints removes all breakpoints
func (d *Debugger) ClearBreakpoints() {
	d.mu.Lock()
	defer d.mu.Unlock()
	
	d.breakpoints = make(map[int]string)
}

// GetBreakpoints returns all breakpoints
func (d *Debugger) GetBreakpoints() map[int]string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	
	result := make(map[int]string)
	for k, v := range d.breakpoints {
		result[k] = v
	}
	return result
}

// OnLineExecute is called before executing a line
// Returns true if execution should pause
func (d *Debugger) OnLineExecute(line int, vars map[string]interface{}) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	
	d.currentLine = line
	d.currentVars = vars
	
	// Check if we hit a breakpoint
	if condition, ok := d.breakpoints[line]; ok {
		if condition == "" || d.evaluateCondition(condition, vars) {
			d.paused = true
			return true
		}
	}
	
	// Check step mode
	if d.stepMode {
		d.stepMode = false
		d.paused = true
		return true
	}
	
	// Check next mode (simplified - would need call stack depth tracking)
	if d.nextMode && len(d.callStack) == 0 {
		d.nextMode = false
		d.paused = true
		return true
	}
	
	return false
}

// evaluateCondition evaluates a breakpoint condition
func (d *Debugger) evaluateCondition(condition string, vars map[string]interface{}) bool {
	// Simplified condition evaluation
	// Real implementation would need expression parser
	if strings.Contains(condition, "==") {
		parts := strings.Split(condition, "==")
		if len(parts) == 2 {
			left := strings.TrimSpace(parts[0])
			right := strings.TrimSpace(parts[1])
			if val, ok := vars[left]; ok {
				return fmt.Sprintf("%v", val) == right
			}
		}
	}
	return true
}

// Step sets step mode
func (d *Debugger) Step() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stepMode = true
	d.resume()
}

// Next sets next mode (step over)
func (d *Debugger) Next() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nextMode = true
	d.resume()
}

// Continue resumes execution
func (d *Debugger) Continue() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.resume()
}

// resume unpauses execution
func (d *Debugger) resume() {
	if d.paused {
		d.paused = false
		close(d.pauseCond)
		d.pauseCond = make(chan struct{})
	}
}

// WaitPause blocks until execution pauses
func (d *Debugger) WaitPause() {
	d.mu.RLock()
	ch := d.pauseCond
	d.mu.RUnlock()
	
	<-ch
}

// IsPaused returns true if execution is paused
func (d *Debugger) IsPaused() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.paused
}

// GetCurrentState returns current execution state
func (d *Debugger) GetCurrentState() DebugState {
	d.mu.RLock()
	defer d.mu.RUnlock()
	
	return DebugState{
		Script: d.currentScript,
		Line:   d.currentLine,
		Vars:   d.currentVars,
		Stack:  d.callStack,
	}
}

// DebugState represents the current debug state
type DebugState struct {
	Script string
	Line   int
	Vars   map[string]interface{}
	Stack  []CallFrame
}

// EnterFunction is called when entering a function
func (d *Debugger) EnterFunction(name string, line int, locals map[string]interface{}) {
	d.mu.Lock()
	defer d.mu.Unlock()
	
	frame := CallFrame{
		FunctionName: name,
		Line:         line,
		Locals:       locals,
	}
	d.callStack = append(d.callStack, frame)
}

// ExitFunction is called when exiting a function
func (d *Debugger) ExitFunction() {
	d.mu.Lock()
	defer d.mu.Unlock()
	
	if len(d.callStack) > 0 {
		d.callStack = d.callStack[:len(d.callStack)-1]
	}
}

// Global debugger instance
var globalDebugger = NewDebugger()

// SetDebugMode sets the global debug mode
func SetDebugMode(mode DebugMode) {
	globalDebugger.SetMode(mode)
}

// GetDebugMode returns the global debug mode
func GetDebugMode() DebugMode {
	return globalDebugger.GetMode()
}

// IsDebugEnabled returns true if debugging is enabled
func IsDebugEnabled() bool {
	return globalDebugger.IsDebugging()
}

// GetDebugger returns the global debugger instance
func GetDebugger() *Debugger {
	return globalDebugger
}
