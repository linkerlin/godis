package scripting

import (
	"context"
	"strings"
	"testing"
	"time"
)

// mockDBExec is a mock database execution function for testing
func mockDBExec(cmd string, args ...string) (interface{}, error) {
	switch strings.ToUpper(cmd) {
	case "SET":
		return "OK", nil
	case "GET":
		if len(args) > 0 {
			// Return the key name as the value for testing
			if args[0] == "key" {
				return "value", nil
			}
			if args[0] == "mykey" {
				return "myvalue", nil
			}
			return args[0] + "_value", nil
		}
		return "value", nil
	case "INCR":
		return int64(1), nil
	case "EXISTS":
		return int64(1), nil
	case "DEL":
		return int64(1), nil
	default:
		return "OK", nil
	}
}

func TestNewEngine(t *testing.T) {
	// Test default engine (gopher-lua)
	engine := NewEngine(mockDBExec)
	if engine == nil {
		t.Fatal("NewEngine returned nil")
	}
	if engine.GetEngineType() != EngineTypeGopherLua {
		t.Errorf("Expected default engine type to be EngineTypeGopherLua, got %v", engine.GetEngineType())
	}

	// Test legacy engine
	legacyEngine := NewEngineWithType(mockDBExec, EngineTypeLegacy)
	if legacyEngine.GetEngineType() != EngineTypeLegacy {
		t.Errorf("Expected engine type to be EngineTypeLegacy, got %v", legacyEngine.GetEngineType())
	}
}

func TestGopherEngineBasic(t *testing.T) {
	engine := NewEngineWithType(mockDBExec, EngineTypeGopherLua)
	defer engine.Close()

	// Test simple script
	script := `return redis.call("GET", "key")`
	result, err := engine.Eval(script, []string{}, []string{})
	if err != nil {
		t.Fatalf("Eval failed: %v", err)
	}
	if result != "value" {
		t.Errorf("Expected 'value', got %v", result)
	}
}

func TestGopherEngineKeysAndArgv(t *testing.T) {
	engine := NewEngineWithType(mockDBExec, EngineTypeGopherLua)
	defer engine.Close()

	// Test KEYS and ARGV
	script := `
		local key = KEYS[1]
		local value = ARGV[1]
		redis.call("SET", key, value)
		return redis.call("GET", key)
	`
	result, err := engine.Eval(script, []string{"mykey"}, []string{"myvalue"})
	if err != nil {
		t.Fatalf("Eval failed: %v", err)
	}
	if result != "myvalue" {
		t.Errorf("Expected 'myvalue', got %v", result)
	}
}

func TestGopherEngineLoop(t *testing.T) {
	engine := NewEngineWithType(mockDBExec, EngineTypeGopherLua)
	defer engine.Close()

	// Test for loop (not supported by legacy engine)
	script := `
		local sum = 0
		for i = 1, 5 do
			sum = sum + i
		end
		return sum
	`
	result, err := engine.Eval(script, []string{}, []string{})
	if err != nil {
		t.Fatalf("Eval failed: %v", err)
	}
	// sum should be 15 (1+2+3+4+5)
	if result != int64(15) {
		t.Errorf("Expected 15, got %v (type: %T)", result, result)
	}
}

func TestGopherEngineFunction(t *testing.T) {
	engine := NewEngineWithType(mockDBExec, EngineTypeGopherLua)
	defer engine.Close()

	// Test function definition (not supported by legacy engine)
	script := `
		local function add(a, b)
			return a + b
		end
		return add(10, 20)
	`
	result, err := engine.Eval(script, []string{}, []string{})
	if err != nil {
		t.Fatalf("Eval failed: %v", err)
	}
	if result != int64(30) {
		t.Errorf("Expected 30, got %v", result)
	}
}

func TestGopherEngineTable(t *testing.T) {
	engine := NewEngineWithType(mockDBExec, EngineTypeGopherLua)
	defer engine.Close()

	// Test table operations
	script := `
		local t = {a = 1, b = 2, c = 3}
		return t.a + t.b + t.c
	`
	result, err := engine.Eval(script, []string{}, []string{})
	if err != nil {
		t.Fatalf("Eval failed: %v", err)
	}
	if result != int64(6) {
		t.Errorf("Expected 6, got %v", result)
	}
}

func TestGopherEnginePCall(t *testing.T) {
	engine := NewEngineWithType(mockDBExec, EngineTypeGopherLua)
	defer engine.Close()

	// Test pcall
	script := `
		local result = redis.pcall("GET", "key")
		if result.ok then
			return result.ok
		else
			return "error: " .. (result.err or "unknown")
		end
	`
	result, err := engine.Eval(script, []string{}, []string{})
	if err != nil {
		t.Fatalf("Eval failed: %v", err)
	}
	if result != "value" {
		t.Errorf("Expected 'value', got %v", result)
	}
}

func TestGopherEngineSha1Hex(t *testing.T) {
	engine := NewEngineWithType(mockDBExec, EngineTypeGopherLua)
	defer engine.Close()

	script := `return redis.sha1hex("hello")`
	result, err := engine.Eval(script, []string{}, []string{})
	if err != nil {
		t.Fatalf("Eval failed: %v", err)
	}
	// SHA1 of "hello" is aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d
	expected := "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"
	if result != expected {
		t.Errorf("Expected '%s', got %v", expected, result)
	}
}

func TestGopherEngineScriptTimeout(t *testing.T) {
	engine := NewEngineWithType(mockDBExec, EngineTypeGopherLua)
	defer engine.Close()

	// Test script timeout with context
	script := `
		local sum = 0
		for i = 1, 1000000 do
			sum = sum + i
		end
		return sum
	`

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	_, err := engine.EvalWithContext(ctx, script, []string{}, []string{})
	if err == nil {
		t.Error("Expected timeout error, got nil")
	}
}

func TestGopherEngineScriptKill(t *testing.T) {
	engine := NewEngineWithType(mockDBExec, EngineTypeGopherLua)
	defer engine.Close()

	// Initially no scripts running
	err := engine.Kill()
	if err == nil {
		t.Error("Expected error when no scripts running, got nil")
	}
}

func TestLoadAndEvalSha(t *testing.T) {
	engine := NewEngineWithType(mockDBExec, EngineTypeGopherLua)
	defer engine.Close()

	script := `return redis.call("GET", KEYS[1])`
	sha := engine.LoadScript(script)

	if sha == "" {
		t.Error("LoadScript returned empty SHA")
	}

	result, err := engine.EvalSha(sha, []string{"mykey"}, []string{})
	if err != nil {
		t.Fatalf("EvalSha failed: %v", err)
	}
	if result != "myvalue" {
		t.Errorf("Expected 'myvalue', got %v", result)
	}

	// Test non-existent script
	_, err = engine.EvalSha("nonexistent", []string{}, []string{})
	if err == nil {
		t.Error("Expected error for non-existent script")
	}
}

func TestExists(t *testing.T) {
	engine := NewEngineWithType(mockDBExec, EngineTypeGopherLua)
	defer engine.Close()

	script1 := `return 1`
	script2 := `return 2`
	sha1 := engine.LoadScript(script1)
	sha2 := engine.LoadScript(script2)

	exists := engine.Exists([]string{sha1, sha2, "nonexistent"})
	if len(exists) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(exists))
	}
	if exists[0] != 1 {
		t.Errorf("Expected sha1 to exist")
	}
	if exists[1] != 1 {
		t.Errorf("Expected sha2 to exist")
	}
	if exists[2] != 0 {
		t.Errorf("Expected nonexistent to not exist")
	}
}

func TestFlush(t *testing.T) {
	engine := NewEngineWithType(mockDBExec, EngineTypeGopherLua)
	defer engine.Close()

	script := `return 1`
	sha := engine.LoadScript(script)

	exists := engine.Exists([]string{sha})
	if exists[0] != 1 {
		t.Error("Expected script to exist before flush")
	}

	engine.Flush()

	exists = engine.Exists([]string{sha})
	if exists[0] != 0 {
		t.Error("Expected script to not exist after flush")
	}
}

func TestLegacyEngine(t *testing.T) {
	engine := NewEngineWithType(mockDBExec, EngineTypeLegacy)
	defer engine.Close()

	// Test basic functionality with legacy engine
	// Note: Legacy engine uses a simplified parser that may handle arguments differently
	script := `return "legacy_test"`
	result, err := engine.Eval(script, []string{}, []string{})
	if err != nil {
		t.Fatalf("Legacy Eval failed: %v", err)
	}
	if result != "legacy_test" {
		t.Errorf("Expected 'legacy_test', got %v", result)
	}
}

func TestEngineSwitch(t *testing.T) {
	engine := NewEngineWithType(mockDBExec, EngineTypeGopherLua)
	defer engine.Close()

	if engine.GetEngineType() != EngineTypeGopherLua {
		t.Error("Expected gopher-lua engine initially")
	}

	// Switch to legacy
	engine.SetEngineType(EngineTypeLegacy)
	if engine.GetEngineType() != EngineTypeLegacy {
		t.Error("Expected legacy engine after switch")
	}

	// Switch back
	engine.SetEngineType(EngineTypeGopherLua)
	if engine.GetEngineType() != EngineTypeGopherLua {
		t.Error("Expected gopher-lua engine after second switch")
	}
}
