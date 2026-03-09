package stats

import (
	"sync"
	"sync/atomic"
	"time"
)

// NetStats tracks network I/O statistics
type NetStats struct {
	inputBytes       uint64
	outputBytes      uint64
	lastInputBytes   uint64
	lastOutputBytes  uint64
	inputRate        float64 // bytes per second
	outputRate       float64 // bytes per second
	lastUpdateTime   time.Time
	mu               sync.RWMutex
}

// Global network statistics
var globalNetStats = &NetStats{
	lastUpdateTime: time.Now(),
}

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

// GetRates returns current I/O rates in KB/s
func GetRates() (inputKBps, outputKBps float64) {
	globalNetStats.mu.Lock()
	defer globalNetStats.mu.Unlock()
	
	now := time.Now()
	elapsed := now.Sub(globalNetStats.lastUpdateTime).Seconds()
	
	if elapsed >= 1.0 {
		// Update rates
		currentInput := atomic.LoadUint64(&globalNetStats.inputBytes)
		currentOutput := atomic.LoadUint64(&globalNetStats.outputBytes)
		
		inputDelta := float64(currentInput - globalNetStats.lastInputBytes)
		outputDelta := float64(currentOutput - globalNetStats.lastOutputBytes)
		
		globalNetStats.inputRate = inputDelta / elapsed
		globalNetStats.outputRate = outputDelta / elapsed
		
		globalNetStats.lastInputBytes = currentInput
		globalNetStats.lastOutputBytes = currentOutput
		globalNetStats.lastUpdateTime = now
	}
	
	// Return rates in KB/s
	return globalNetStats.inputRate / 1024, globalNetStats.outputRate / 1024
}

// Reset resets all network statistics
func Reset() {
	globalNetStats.mu.Lock()
	defer globalNetStats.mu.Unlock()
	globalNetStats.inputBytes = 0
	globalNetStats.outputBytes = 0
	globalNetStats.lastInputBytes = 0
	globalNetStats.lastOutputBytes = 0
	globalNetStats.inputRate = 0
	globalNetStats.outputRate = 0
	globalNetStats.lastUpdateTime = time.Now()
}
