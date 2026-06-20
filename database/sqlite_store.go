package database

import (
	"fmt"
	"path/filepath"

	"github.com/linkerlin/godis/config"
)

const (
	// defaultSQLiteMmapSize is 256 MiB; SQLite may cap to platform maximum.
	defaultSQLiteMmapSize int64 = 256 * 1024 * 1024
	defaultSQLiteIndexFile        = "search_index.db"
)

// sqliteIndexOptions holds SQLite index store tuning from config.
type sqliteIndexOptions struct {
	Path     string
	MmapSize int64
}

func sqliteIndexOptionsFromConfig() sqliteIndexOptions {
	opts := sqliteIndexOptions{
		Path:     defaultSQLiteIndexFile,
		MmapSize: defaultSQLiteMmapSize,
	}
	if config.Properties == nil {
		return opts
	}
	if config.Properties.SearchSQLitePath != "" {
		opts.Path = config.Properties.SearchSQLitePath
	} else if config.Properties.Dir != "" {
		opts.Path = filepath.Join(config.Properties.Dir, defaultSQLiteIndexFile)
	}
	if config.Properties.SqliteMmapSize > 0 {
		opts.MmapSize = config.Properties.SqliteMmapSize
	}
	return opts
}

// BuildSQLiteDSN builds a modernc.org/sqlite DSN with WAL + mmap pragmas.
func BuildSQLiteDSN(path string, mmapSize int64) string {
	if mmapSize <= 0 {
		mmapSize = defaultSQLiteMmapSize
	}
	slashPath := filepath.ToSlash(path)
	return fmt.Sprintf(
		"file:%s?_pragma=journal_mode(WAL)&_pragma=mmap_size(%d)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)",
		slashPath,
		mmapSize,
	)
}
