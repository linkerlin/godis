package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/config"
)

func TestBuildSQLiteDSNWALAndMmap(t *testing.T) {
	dsn := BuildSQLiteDSN(`C:\godis\search_index.db`, 268435456)
	if !strings.Contains(dsn, "journal_mode(WAL)") {
		t.Fatalf("expected WAL in dsn: %s", dsn)
	}
	if !strings.Contains(dsn, "mmap_size(268435456)") {
		t.Fatalf("expected mmap_size in dsn: %s", dsn)
	}
}

func TestSQLiteIndexOptionsFromConfig(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{
		Dir:              "/var/lib/godis",
		SearchSQLitePath: "",
		SqliteMmapSize:   536870912,
	}
	defer func() { config.Properties = old }()

	opts := sqliteIndexOptionsFromConfig()
	if !strings.HasSuffix(opts.Path, "search_index.db") {
		t.Fatalf("unexpected path: %s", opts.Path)
	}
	if opts.MmapSize != 536870912 {
		t.Fatalf("unexpected mmap size: %d", opts.MmapSize)
	}
}
