//go:build sqlite_backend

package database

import (
	"path/filepath"
	"testing"
)

func TestSQLiteVSAddSearch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vec_test.db")
	sqlDB, err := OpenSQLiteIndexDB(path, 64*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	if err := initSQLiteVectorSchema(sqlDB); err != nil {
		t.Fatal(err)
	}

	isNew, err := sqliteVSAddVector(sqlDB, "embeddings", "a", []float64{1, 0, 0}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !isNew {
		t.Fatal("expected new vector")
	}
	if _, err := sqliteVSAddVector(sqlDB, "embeddings", "b", []float64{0.9, 0.1, 0}, nil); err != nil {
		t.Fatal(err)
	}

	hits, err := sqliteVSSearchVectors(sqlDB, "embeddings", []float64{1, 0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) == 0 {
		t.Fatal("expected search hits")
	}
}

func TestSQLiteVSDimensionMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vec_dim.db")
	sqlDB, err := OpenSQLiteIndexDB(path, 64*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	if err := initSQLiteVectorSchema(sqlDB); err != nil {
		t.Fatal(err)
	}
	if _, err := sqliteVSAddVector(sqlDB, "s", "x", []float64{1, 2, 3}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := sqliteVSAddVector(sqlDB, "s", "y", []float64{1, 2}, nil); err == nil {
		t.Fatal("expected dimension mismatch")
	}
}
