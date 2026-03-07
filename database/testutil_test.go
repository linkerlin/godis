package database

import (
	"os"
	"runtime"
	"sync"
	"testing"

	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/lib/logger"
)

var (
	testServerOnce sync.Once
	testServerInst *Server
	testDBOnce     sync.Once
	testDBInst     *DB
)

// skipHeavyTests checks if heavy tests should be skipped
func skipHeavyTests(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy test in short mode")
	}
}

// skipReplicationTests skips replication tests on Windows or in short mode
// due to cross-drive file rename issues and external server requirements
func skipReplicationTests(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping replication test in short mode")
	}
	if runtime.GOOS == "windows" {
		t.Skip("skipping replication test on Windows (cross-drive file issues)")
	}
}

// getTestServer returns a shared test server instance (lazy initialization)
func getTestServer() *Server {
	testServerOnce.Do(func() {
		// Disable AOF and persistence for unit tests to save memory
		config.Properties = &config.ServerProperties{
			Databases:  1, // Use only 1 database instead of 16
			AppendOnly: false,
		}
		var err error
		testServerInst, err = NewTestServer()
		if err != nil {
			logger.Fatal("failed to create test server: " + err.Error())
		}
	})
	return testServerInst
}

// getTestDB returns a shared test DB instance (lazy initialization)
func getTestDB() *DB {
	testDBOnce.Do(func() {
		testDBInst = makeDBWithSize(testDictSize)
	})
	return testDBInst
}

// isCI checks if running in CI environment
func isCI() bool {
	return os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != ""
}
