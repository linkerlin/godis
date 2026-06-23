package database

import (
	"testing"
)

func TestCommandStatsLifecycle(t *testing.T) {
	ResetCommandStats()

	RecordCommand("get", 100, false)
	RecordCommand("get", 200, true)

	stats := GetCommandStats("get")
	if stats == nil {
		t.Fatal("expected stats for get")
	}
	if stats.Calls() != 2 {
		t.Fatalf("calls=%d", stats.Calls())
	}
	if stats.FailedCalls() != 1 {
		t.Fatalf("failed=%d", stats.FailedCalls())
	}
	if stats.UsecTotal() != 300 {
		t.Fatalf("usec=%d", stats.UsecTotal())
	}

	all := GetAllCommandStats()
	if len(all) == 0 || all["get"] == nil {
		t.Fatal("GetAllCommandStats missing get")
	}

	ResetCommandStats()
	if GetCommandStats("get") != nil {
		t.Fatal("expected stats cleared")
	}
}
