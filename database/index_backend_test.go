package database

import (
	"testing"

	"github.com/linkerlin/godis/config"
)

func TestSelectSearchBackend(t *testing.T) {
	if got := selectSearchBackend("native").Name(); got != "native" {
		t.Fatalf("expected native, got %s", got)
	}
	if got := selectSearchBackend("sqlite").Name(); got != "sqlite" {
		t.Fatalf("expected sqlite, got %s", got)
	}
	if got := selectSearchBackend("unknown").Name(); got != "native" {
		t.Fatalf("unknown fallback should be native, got %s", got)
	}
}

func TestSelectVectorBackend(t *testing.T) {
	if got := selectVectorBackend("native").Name(); got != "native" {
		t.Fatalf("expected native, got %s", got)
	}
	if got := selectVectorBackend("sqlite").Name(); got != "sqlite" {
		t.Fatalf("expected sqlite, got %s", got)
	}
	if got := selectVectorBackend("unknown").Name(); got != "native" {
		t.Fatalf("unknown fallback should be native, got %s", got)
	}
}

func TestCurrentBackendReadsConfig(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{
		SearchBackend: "sqlite",
		VectorBackend: "sqlite",
	}
	defer func() {
		config.Properties = old
	}()

	if got := currentSearchBackend().Name(); got != "sqlite" {
		t.Fatalf("expected sqlite search backend, got %s", got)
	}
	if got := currentVectorBackend().Name(); got != "sqlite" {
		t.Fatalf("expected sqlite vector backend, got %s", got)
	}
}

func TestSQLiteBackendAvailableProbe(t *testing.T) {
	_ = sqliteBackendAvailable()
}
