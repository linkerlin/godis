//go:build sqlite_backend

package database

import (
	"path/filepath"
	"testing"
)

func TestSQLiteFTCreateSearchAdd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ft_test.db")
	sqlDB, err := OpenSQLiteIndexDB(path, 64*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	if err := initSQLiteSearchSchema(sqlDB); err != nil {
		t.Fatal(err)
	}
	if err := sqliteFTCreateIndex(sqlDB, "idx", []string{"title", "body"}); err != nil {
		t.Fatal(err)
	}
	if err := sqliteFTAddDocument(sqlDB, "idx", "doc1", map[string]string{
		"title": "hello world",
		"body":  "sqlite fts5",
	}); err != nil {
		t.Fatal(err)
	}

	hits, total, err := sqliteFTSearchDocs(sqlDB, "idx", "hello", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(hits) != 1 {
		t.Fatalf("expected 1 hit, total=%d len=%d", total, len(hits))
	}
	if hits[0].docID != "doc1" {
		t.Fatalf("unexpected doc id: %s", hits[0].docID)
	}
}
