package stats

import (
	"sync"
	"sync/atomic"
)

// NetStats tracks network I/O statistics
type NetStats struct {
	inputBytes  uint64
	outputBytes uint64
	mu          sync.RWMutex
}

// Global network statistics
var globalNetStats = &NetStats{}

// RecordInput records incoming bytes
func RecordInput(bytes int) {
	atomic.AddUint64(&globalNetStats.inputBytes, uint64(bytes))
}

// RecordOutput records outgoing bytes
func RecordOutput(bytes int) {
	atomic.AddUint64(&globalNetStats.outputBytes, uint64(bytes))
}

// GetStats returns current network statistics
func GetStats() (inputBytes, outputBytes uint64) {
	return atomic.LoadUint64(&globalNetStats.inputBytes),
		atomic.LoadUint64(&globalNetStats.outputBytes)
}

// Reset resets all network statistics
func Reset() {
	globalNetStats.mu.Lock()
	defer globalNetStats.mu.Unlock()
	globalNetStats.inputBytes = 0
	globalNetStats.outputBytes = 0
}
