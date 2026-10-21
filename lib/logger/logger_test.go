package logger

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestFormatJSONEntry(t *testing.T) {
	line := formatJSONEntry(INFO, "/tmp/some/dir/server.go", 42, true, "hello\n")
	var rec jsonLogEntry
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("invalid JSON log line %q: %v", line, err)
	}
	if rec.Level != "INFO" {
		t.Fatalf("level = %q, want INFO", rec.Level)
	}
	if rec.Caller != "server.go:42" {
		t.Fatalf("caller = %q, want server.go:42", rec.Caller)
	}
	if rec.Msg != "hello" {
		t.Fatalf("msg = %q, want hello", rec.Msg)
	}
	if _, err := time.Parse(time.RFC3339Nano, rec.Time); err != nil {
		t.Fatalf("time %q is not RFC3339Nano: %v", rec.Time, err)
	}
	if strings.Contains(line, "\n") {
		t.Fatalf("JSON log line must be single-line: %q", line)
	}
}

func TestSetFormatValidation(t *testing.T) {
	l := NewStdoutLogger()
	if err := l.setFormat("bogus"); err == nil {
		t.Fatal("setFormat(bogus) should fail")
	}
	if err := l.setFormat("JSON"); err != nil {
		t.Fatalf("setFormat(JSON) failed: %v", err)
	}
	if !l.jsonMode.Load() {
		t.Fatal("json mode not enabled")
	}
	if err := l.setFormat("text"); err != nil {
		t.Fatalf("setFormat(text) failed: %v", err)
	}
	if l.jsonMode.Load() {
		t.Fatal("json mode not disabled")
	}
}
