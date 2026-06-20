package database

import (
	"sync"
	"sync/atomic"
)

// CommandStats tracks statistics for each command
type CommandStats struct {
	calls        uint64
	usec         uint64 // Total time in microseconds
	usecPerCall  float64 // Average time per call
	rejectedKeys uint64
	failedCalls  uint64
}

// Global command statistics
type globalCmdStats struct {
	mu    sync.RWMutex
	stats map[string]*CommandStats
}

var cmdStats = &globalCmdStats{
	stats: make(map[string]*CommandStats),
}

// RecordCommand records command execution
func RecordCommand(cmdName string, usec uint64, failed bool) {
	cmdStats.mu.Lock()
	defer cmdStats.mu.Unlock()
	
	stats, exists := cmdStats.stats[cmdName]
	if !exists {
		stats = &CommandStats{}
		cmdStats.stats[cmdName] = stats
	}
	
	atomic.AddUint64(&stats.calls, 1)
	atomic.AddUint64(&stats.usec, usec)
	
	if failed {
		atomic.AddUint64(&stats.failedCalls, 1)
	}
	
	// Update average
	calls := atomic.LoadUint64(&stats.calls)
	usecTotal := atomic.LoadUint64(&stats.usec)
	if calls > 0 {
		stats.usecPerCall = float64(usecTotal) / float64(calls)
	}
}

// GetCommandStats returns stats for a command
func GetCommandStats(cmdName string) *CommandStats {
	cmdStats.mu.RLock()
	defer cmdStats.mu.RUnlock()
	
	if stats, ok := cmdStats.stats[cmdName]; ok {
		// Return a copy
		return &CommandStats{
			calls:        atomic.LoadUint64(&stats.calls),
			usec:         atomic.LoadUint64(&stats.usec),
			usecPerCall:  stats.usecPerCall,
			rejectedKeys: atomic.LoadUint64(&stats.rejectedKeys),
			failedCalls:  atomic.LoadUint64(&stats.failedCalls),
		}
	}
	return nil
}

// GetAllCommandStats returns all command stats
func GetAllCommandStats() map[string]*CommandStats {
	cmdStats.mu.RLock()
	defer cmdStats.mu.RUnlock()
	
	result := make(map[string]*CommandStats)
	for name, stats := range cmdStats.stats {
		result[name] = &CommandStats{
			calls:        atomic.LoadUint64(&stats.calls),
			usec:         atomic.LoadUint64(&stats.usec),
			usecPerCall:  stats.usecPerCall,
			rejectedKeys: atomic.LoadUint64(&stats.rejectedKeys),
			failedCalls:  atomic.LoadUint64(&stats.failedCalls),
		}
	}
	return result
}

func (s *CommandStats) Calls() uint64       { return s.calls }
func (s *CommandStats) FailedCalls() uint64 { return s.failedCalls }
func (s *CommandStats) UsecTotal() uint64   { return s.usec }

// ResetCommandStats resets all command statistics
func ResetCommandStats() {
	cmdStats.mu.Lock()
	defer cmdStats.mu.Unlock()
	
	cmdStats.stats = make(map[string]*CommandStats)
}
