//go:build sqlite_backend

package database

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenSQLiteIndexDBEnablesWALAndMmap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "idx.db")

	db, err := OpenSQLiteIndexDB(path, 64*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var journalMode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		t.Fatalf("expected wal journal_mode, got %q", journalMode)
	}

	var mmapSize int64
	if err := db.QueryRow("PRAGMA mmap_size").Scan(&mmapSize); err != nil {
		t.Fatal(err)
	}
	if mmapSize <= 0 {
		t.Fatalf("expected positive mmap_size, got %d", mmapSize)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected db file created: %v", err)
	}
	if _, err := os.Stat(path + "-wal"); err != nil {
		// WAL file appears after first write; create a table to force it.
		if _, err := db.Exec("CREATE TABLE IF NOT EXISTS _probe (id INTEGER PRIMARY KEY)"); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(path + "-wal"); err != nil {
			t.Fatalf("expected wal sidecar after write: %v", err)
		}
	}
}
