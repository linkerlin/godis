package functions

import (
	"fmt"
	"strings"
	"testing"
)

const testLibraryCode = `--[[ demo library ]]
#!lua name=demo
redis.register_function('plain', function(keys, args) return 1 end)
redis.register_function{name='reader', callback=function(keys, args) return 2 end, flags={'readonly','no-writes'}}
`

func TestEngineLibraryLifecycle(t *testing.T) {
	e := NewEngine(4)
	if got := e.ListLibraries(); len(got) != 0 {
		t.Fatalf("new engine libraries=%v", got)
	}
	if got := e.GetRunningFunction(); got != nil {
		t.Fatalf("new engine running=%v", got)
	}
	if err := e.KillRunningFunction(); err == nil {
		t.Fatal("KillRunningFunction without a function should error")
	}

	n, err := e.LoadLibrary("demo", testLibraryCode, false)
	if err != nil {
		t.Fatalf("LoadLibrary: %v", err)
	}
	if n != 2 {
		t.Fatalf("loaded %d functions, want 2", n)
	}
	if libs := e.ListLibraries(); len(libs) != 1 || libs[0] != "demo" {
		t.Fatalf("ListLibraries=%v", libs)
	}
	if fns := e.ListFunctions(); len(fns) != 2 {
		t.Fatalf("ListFunctions=%v", fns)
	}
	lib, ok := e.GetLibrary("demo")
	if !ok || lib.Engine != "LUA" || lib.Description != "demo library" || lib.SHA == "" {
		t.Fatalf("GetLibrary=%+v ok=%v", lib, ok)
	}
	plain, ok := e.GetFunction("plain")
	if !ok || plain.Library != "demo" || plain.SHA == "" || plain.IsReadOnly() {
		t.Fatalf("GetFunction(plain)=%+v ok=%v", plain, ok)
	}
	reader, ok := e.GetFunction("reader")
	if !ok || !reader.IsReadOnly() {
		t.Fatalf("GetFunction(reader)=%+v ok=%v", reader, ok)
	}

	stats := e.Stats()
	if stats["libraries"] != 1 || stats["functions"] != 2 || stats["engine"] != "LUA" {
		t.Fatalf("Stats=%v", stats)
	}

	if _, err := e.LoadLibrary("demo", testLibraryCode, false); err == nil {
		t.Fatal("duplicate LoadLibrary should error")
	}
	if n, err := e.LoadLibrary("demo", `#!lua name=demo
redis.register_function('replacement', function(keys, args) return 3 end)`, true); err != nil || n != 1 {
		t.Fatalf("replace: n=%d err=%v", n, err)
	}
	if _, ok := e.GetFunction("plain"); ok {
		t.Fatal("replaced library should remove old functions")
	}
	if fn, ok := e.GetFunction("replacement"); !ok || fn.Library != "demo" {
		t.Fatalf("replacement function missing: %+v %v", fn, ok)
	}
	if err := e.DeleteLibrary("demo"); err != nil {
		t.Fatalf("DeleteLibrary: %v", err)
	}
	if _, ok := e.GetLibrary("demo"); ok {
		t.Fatal("deleted library still present")
	}
	if err := e.DeleteLibrary("demo"); err == nil {
		t.Fatal("deleting a missing library should error")
	}

	_, _ = e.LoadLibrary("demo", testLibraryCode, false)
	if err := e.FlushAll(); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	if len(e.ListLibraries()) != 0 || len(e.ListFunctions()) != 0 {
		t.Fatal("FlushAll did not clear state")
	}
}

func TestEngineLoadWithoutFunctions(t *testing.T) {
	e := NewEngine(1)
	if _, err := e.LoadLibrary("empty", "#!lua name=empty\nreturn 1", false); err == nil {
		t.Fatal("library without redis.register_function should error")
	}
}

func TestEngineCallAndDBExec(t *testing.T) {
	e := NewEngine(1)
	e.SetDBExec(func(cmd string, args ...string) (interface{}, error) {
		if cmd != "GET" || len(args) != 1 || args[0] != "k" {
			return nil, fmt.Errorf("unexpected call %s %v", cmd, args)
		}
		return "value", nil
	})
	code := `#!lua name=calllib
redis.register_function('readit', function(keys, args) return redis.call('GET', 'k') end)
redis.register_function('answer', function(keys, args) return 42 end)
`
	if _, err := e.LoadLibrary("calllib", code, false); err != nil {
		t.Fatalf("LoadLibrary: %v", err)
	}
	got, err := e.Call("readit", nil, nil)
	if err != nil || got != "value" {
		t.Fatalf("Call readit = %#v, %v", got, err)
	}
	got, err = e.Call("answer", nil, nil)
	if err != nil || fmt.Sprint(got) != "42" {
		t.Fatalf("Call answer = %#v, %v", got, err)
	}
	if _, err := e.Call("missing", nil, nil); err == nil {
		t.Fatal("Call missing function should error")
	}
}

func TestFunctionHelpers(t *testing.T) {
	e := NewEngine(1)
	script := e.buildExecutionScript("#!lua name=x\nreturn 1", "f", nil, nil)
	if !strings.Contains(script, `__godis_funcs["f"]`) {
		t.Fatal("execution script missing function lookup")
	}
	if hasShebang(script) {
		t.Fatal("execution script still contains shebang")
	}
	if got := parseEngineFromShebang("#!lua name=x\nreturn 1"); got != "LUA" {
		t.Fatalf("parseEngineFromShebang=%q", got)
	}
	if got := parseEngineFromShebang("return 1"); got != "LUA" {
		t.Fatalf("default engine=%q", got)
	}
	if got := extractDescription("--[[ hello world ]]\nreturn 1"); strings.TrimSpace(got) != "hello world" {
		t.Fatalf("extractDescription=%q", got)
	}
	if got := computeSHA("abc"); got != "a9993e364706816aba3e25717850c26c9cd0d89d" {
		t.Fatalf("computeSHA=%q", got)
	}
	if got := stripFunctionShebang("#!lua name=x\nreturn 1"); got != "return 1" {
		t.Fatalf("stripFunctionShebang=%q", got)
	}
}

func hasShebang(code string) bool {
	for _, line := range strings.Split(code, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#!") {
			return true
		}
	}
	return false
}

// Real Redis documents the table form with function_name=/callback= keys;
// name= is a godis superset. Regression for the load-vs-call divergence: the
// parser accepted both, but the runtime shim only resolved name=, so the
// function loaded yet failed at call time with "table index is nil" (bug #19).
func TestEngineTableFormFunctionNameKey(t *testing.T) {
	e := NewEngine(4)
	code := "#!lua name=tl\n" +
		"redis.register_function{function_name=\"tf\", callback=function(keys, args) return \"ok\" end}\n"
	if _, err := e.LoadLibrary("tl", code, false); err != nil {
		t.Fatalf("LoadLibrary: %v", err)
	}
	if _, ok := e.GetFunction("tf"); !ok {
		t.Fatal("function_name= table form did not register tf")
	}
	v, err := e.Call("tf", nil, nil)
	if err != nil || v != "ok" {
		t.Fatalf("Call tf: %v %v", v, err)
	}
}

// The read-only rejection must carry the line in original library-source
// coordinates, not the execution-wrapper frame.
func TestRemapRORejectionLine(t *testing.T) {
	code := "#!lua name=w\nredis.register_function{function_name=\"wfn\", callback=function() return redis.call(\"SET\",\"k\",\"v\") end, flags={\"no-writes\"}}\n"
	msg, ok := remapRORejection("ERR Write commands are not allowed from read-only scripts. script: wfn, on @user_function:11.", code)
	if !ok || msg != "ERR Write commands are not allowed from read-only scripts. script: wfn, on @user_function:2." {
		t.Fatalf("remapRORejection: %q ok=%v", msg, ok)
	}
	if _, ok := remapRORejection("ERR function execution failed: oops", code); ok {
		t.Fatal("generic error must not be treated as a read-only rejection")
	}
}
