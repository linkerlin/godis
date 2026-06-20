package core

import (
	"strings"
	"testing"
)

func TestDoMigrateSlotRequiresRaft(t *testing.T) {
	cluster := &Cluster{}
	err := cluster.doMigrateSlot(0, "127.0.0.1:6399", "127.0.0.1:6499")
	if err == nil {
		t.Fatal("expected error without raft node")
	}
	if !strings.Contains(err.Error(), "raft") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDoMigrateSlotSameNodeNoOp(t *testing.T) {
	// Without raft this fails early; with same from/to the check is after leader check.
	// Covered by integration tests on Linux/macOS.
	t.Skip("requires raft leader; see TestDoMigrateSlot")
}
