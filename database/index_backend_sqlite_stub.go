//go:build !sqlite_backend

package database

type sqliteSearchBackend struct{}

func (sqliteSearchBackend) Name() string { return backendSQLite }

type sqliteVectorBackend struct{}

func (sqliteVectorBackend) Name() string { return backendSQLite }

// sqliteBackendAvailable is a stub in default builds.
// Real driver probe is enabled with -tags sqlite_backend.
func sqliteBackendAvailable() bool {
	return false
}
