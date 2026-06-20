//go:build !sqlite_backend

package database

import (
	"database/sql"
	"errors"
)

var errSQLiteBackendDisabled = errors.New("sqlite backend disabled (build with -tags sqlite_backend)")

// OpenSQLiteIndexDB is unavailable unless built with -tags sqlite_backend.
func OpenSQLiteIndexDB(path string, mmapSize int64) (*sql.DB, error) {
	return nil, errSQLiteBackendDisabled
}
