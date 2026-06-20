package database

import (
	"strings"

	"github.com/linkerlin/godis/config"
)

const (
	backendNative = "native"
	backendSQLite = "sqlite"
)

// SearchIndexBackend is the abstraction point for FT.* index implementations.
// This stage only wires backend selection; command migration is incremental.
type SearchIndexBackend interface {
	Name() string
}

// VectorIndexBackend is the abstraction point for VS.* index implementations.
// This stage only wires backend selection; command migration is incremental.
type VectorIndexBackend interface {
	Name() string
}

type nativeSearchBackend struct{}

func (nativeSearchBackend) Name() string { return backendNative }

type nativeVectorBackend struct{}

func (nativeVectorBackend) Name() string { return backendNative }

// selectSearchBackend resolves configured search backend.
// Unknown values degrade to native for safe startup.
func selectSearchBackend(raw string) SearchIndexBackend {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", backendNative:
		return nativeSearchBackend{}
	case backendSQLite:
		return sqliteSearchBackend{}
	default:
		return nativeSearchBackend{}
	}
}

// selectVectorBackend resolves configured vector backend.
// Unknown values degrade to native for safe startup.
func selectVectorBackend(raw string) VectorIndexBackend {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", backendNative:
		return nativeVectorBackend{}
	case backendSQLite:
		return sqliteVectorBackend{}
	default:
		return nativeVectorBackend{}
	}
}

func currentSearchBackend() SearchIndexBackend {
	if config.Properties == nil {
		return nativeSearchBackend{}
	}
	return selectSearchBackend(config.Properties.SearchBackend)
}

func currentVectorBackend() VectorIndexBackend {
	if config.Properties == nil {
		return nativeVectorBackend{}
	}
	return selectVectorBackend(config.Properties.VectorBackend)
}
