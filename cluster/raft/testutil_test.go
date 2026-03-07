package raft

import (
	"runtime"
	"testing"
)

// skipHeavyTests checks if heavy tests should be skipped
func skipHeavyTests(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy test in short mode")
	}
	if runtime.GOOS == "windows" {
		t.Skip("skipping raft test on Windows (file locking issues)")
	}
}
