//go:build sqlite_backend

package database

import (
	"os"

	_ "modernc.org/sqlite"
	_ "modernc.org/sqlite/vec"
)

type sqliteSearchBackend struct{}

func (sqliteSearchBackend) Name() string { return backendSQLite }

type sqliteVectorBackend struct{}

func (sqliteVectorBackend) Name() string { return backendSQLite }

// sqliteBackendAvailable opens a temp database with WAL+mmap to verify the driver stack.
func sqliteBackendAvailable() bool {
	f, err := os.CreateTemp("", "godis-sqlite-probe-*.db")
	if err != nil {
		return false
	}
	path := f.Name()
	_ = f.Close()
	defer os.Remove(path)

	db, err := OpenSQLiteIndexDB(path, defaultSQLiteMmapSize)
	if err != nil {
		return false
	}
	defer db.Close()
	return db.Ping() == nil
}
