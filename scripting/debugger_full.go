package scripting

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/hdt3213/godis/lib/logger"
	lua "github.com/yuin/gopher-lua"
)

// FullDebugger provides comprehensive Lua script debugging capabilities
// Note: This is a simplified implementation using gopher-lua
// Full line-by-line debugging would require modification to the gopher-lua library
type FullDebugger struct {
	mu sync.RWMutex

	mode DebugMode

	// Breakpoints: map of line -> breakpoint info
	// Note: Line numbers in Lua start from 1
	breakpoints map[int]*Breakpoint

	// Current execution state (simplified)
	currentScript string

	// Execution control
	stepMode   bool // Step into
	nextMode   bool // Step over
	finishMode bool // Step out
	paused     bool
	pauseCond  chan struct{}

	// Trace output
	traceEnabled bool
}

// Breakpoint represents a debugger breakpoint
type Breakpoint struct {
	Line      int
	Condition string
	HitCount  int
	HitTarget int // 0 = always break
	Enabled   bool
}

// NewFullDebugger creates a new full-featured debugger
func NewFullDebugger() *FullDebugger {
	return &FullDebugger{
		breakpoints: make(map[int]*Breakpoint),
		pauseCond:   make(chan struct{}),
	}
}

// SetMode sets the debug mode
func (d *FullDebugger) SetMode(mode DebugMode) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.mode = mode
	logger.Infof("Script debug mode set to: %v", mode)
}

// GetMode returns current debug mode
func (d *FullDebugger) GetMode() DebugMode {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.mode
}

// IsDebugging returns true if debugging is enabled
func (d *FullDebugger) IsDebugging() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.mode != DebugModeNo
}

// IsTraceEnabled returns true if tracing is enabled
func (d *FullDebugger) IsTraceEnabled() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.traceEnabled
}

// SetTraceEnabled enables/disables trace output
func (d *FullDebugger) SetTraceEnabled(enabled bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.traceEnabled = enabled
}

// AddBreakpoint adds a breakpoint at a line with optional condition
func (d *FullDebugger) AddBreakpoint(line int, condition string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.breakpoints[line] = &Breakpoint{
		Line:      line,
		Condition: condition,
		Enabled:   true,
	}

	logger.Infof("Breakpoint added at line %d, condition: %s", line, condition)
	return nil
}

// RemoveBreakpoint removes a breakpoint
func (d *FullDebugger) RemoveBreakpoint(line int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.breakpoints, line)

	logger.Infof("Breakpoint removed at line %d", line)
	return nil
}

// ClearBreakpoints removes all breakpoints
func (d *FullDebugger) ClearBreakpoints() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.breakpoints = make(map[int]*Breakpoint)
	logger.Info("All breakpoints cleared")
}

// GetBreakpoints returns all breakpoints
func (d *FullDebugger) GetBreakpoints() map[int]*Breakpoint {
	d.mu.RLock()
	defer d.mu.RUnlock()

	result := make(map[int]*Breakpoint)
	for _, bp := range d.breakpoints {
		result[bp.Line] = bp
	}
	return result
}

// Step sets step mode (step into)
// Note: In simplified implementation, this just logs the intent
func (d *FullDebugger) Step() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.stepMode = true
	d.nextMode = false
	d.finishMode = false
	d.resume()
	logger.Info("Debug: Step into")
}

// Next sets next mode (step over)
func (d *FullDebugger) Next() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.stepMode = false
	d.nextMode = true
	d.finishMode = false
	d.resume()
	logger.Info("Debug: Step over")
}

// Finish sets finish mode (step out)
func (d *FullDebugger) Finish() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.stepMode = false
	d.nextMode = false
	d.finishMode = true
	d.resume()
	logger.Info("Debug: Step out")
}

// Continue resumes execution
func (d *FullDebugger) Continue() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.stepMode = false
	d.nextMode = false
	d.finishMode = false
	d.resume()
	logger.Info("Debug: Continue")
}

// resume unpauses execution
func (d *FullDebugger) resume() {
	if d.paused {
		d.paused = false
		close(d.pauseCond)
		d.pauseCond = make(chan struct{})
	}
}

// IsPaused returns true if execution is paused
func (d *FullDebugger) IsPaused() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.paused
}

// GetDebugInfo returns current debug information
// Simplified version for gopher-lua compatibility
func (d *FullDebugger) GetDebugInfo() map[string]interface{} {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return map[string]interface{}{
		"script":   d.currentScript,
		"mode":     d.mode,
		"trace":    d.traceEnabled,
		"breakpoints": len(d.breakpoints),
	}
}

// PrintStack prints the current call stack
// Simplified for gopher-lua
func (d *FullDebugger) PrintStack() string {
	return "Stack trace not available in simplified debugger"
}

// PrintLocals prints local variables
// Simplified for gopher-lua
func (d *FullDebugger) PrintLocals() string {
	return "Local variables not available in simplified debugger"
}

// luaValueToString converts a Lua value to string representation
func luaValueToString(lv lua.LValue) string {
	if lv == nil || lv == lua.LNil {
		return "nil"
	}

	switch v := lv.(type) {
	case lua.LBool:
		return strconv.FormatBool(bool(v))
	case lua.LNumber:
		return fmt.Sprintf("%g", float64(v))
	case lua.LString:
		return fmt.Sprintf("\"%s\"", string(v))
	case *lua.LTable:
		return fmt.Sprintf("table: %p", v)
	case *lua.LFunction:
		return fmt.Sprintf("function: %p", v)
	case *lua.LUserData:
		return fmt.Sprintf("userdata: %p", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// Global full debugger instance
var globalFullDebugger = NewFullDebugger()

// GetFullDebugger returns the global full debugger instance
func GetFullDebugger() *FullDebugger {
	return globalFullDebugger
}
