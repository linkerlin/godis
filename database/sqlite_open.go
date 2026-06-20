//go:build sqlite_backend

package database

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"
	_ "modernc.org/sqlite/vec"
)

var errSQLiteJournalMode = errors.New("sqlite journal_mode is not wal")

// OpenSQLiteIndexDB opens the on-disk index database with WAL and mmap enabled.
func OpenSQLiteIndexDB(path string, mmapSize int64) (*sql.DB, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("sqlite index path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	dsn := BuildSQLiteDSN(path, mmapSize)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)

	if err := applySQLiteIndexPragmas(db, mmapSize); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func applySQLiteIndexPragmas(db *sql.DB, mmapSize int64) error {
	if mmapSize <= 0 {
		mmapSize = defaultSQLiteMmapSize
	}
	statements := []string{
		"PRAGMA journal_mode=WAL",
		fmt.Sprintf("PRAGMA mmap_size=%d", mmapSize),
		"PRAGMA busy_timeout=5000",
		"PRAGMA synchronous=NORMAL",
	}
	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}

	var journalMode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		return err
	}
	if !strings.EqualFold(journalMode, "wal") {
		return errSQLiteJournalMode
	}
	return nil
}
