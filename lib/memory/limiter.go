package memory

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

// EvictionPolicy defines the memory eviction policy
type EvictionPolicy int

const (
	// NoEviction returns error when memory limit is reached
	NoEviction EvictionPolicy = iota
	// AllKeysLRU evict any key using LRU
	AllKeysLRU
	// AllKeysLFU evict any key using LFU
	AllKeysLFU
	// AllKeysRandom evict random keys
	AllKeysRandom
	// VolatileLRU evict keys with expire using LRU
	VolatileLRU
	// VolatileLFU evict keys with expire using LFU
	VolatileLFU
	// VolatileTTL evict keys with expire using shortest TTL
	VolatileTTL
	// VolatileRandom evict random keys with expire
	VolatileRandom
)

// String returns string representation of eviction policy
func (p EvictionPolicy) String() string {
	switch p {
	case NoEviction:
		return "noeviction"
	case AllKeysLRU:
		return "allkeys-lru"
	case AllKeysLFU:
		return "allkeys-lfu"
	case AllKeysRandom:
		return "allkeys-random"
	case VolatileLRU:
		return "volatile-lru"
	case VolatileLFU:
		return "volatile-lfu"
	case VolatileTTL:
		return "volatile-ttl"
	case VolatileRandom:
		return "volatile-random"
	default:
		return "unknown"
	}
}

// ParseEvictionPolicy parses policy string
func ParseEvictionPolicy(s string) EvictionPolicy {
	switch s {
	case "noeviction":
		return NoEviction
	case "allkeys-lru":
		return AllKeysLRU
	case "allkeys-lfu":
		return AllKeysLFU
	case "allkeys-random":
		return AllKeysRandom
	case "volatile-lru":
		return VolatileLRU
	case "volatile-lfu":
		return VolatileLFU
	case "volatile-ttl":
		return VolatileTTL
	case "volatile-random":
		return VolatileRandom
	default:
		return NoEviction
	}
}

// Limiter manages memory limit and eviction
type Limiter struct {
	maxMemory       int64
	policy          EvictionPolicy
	samples         int // Number of samples for LRU/LFU
	
	// Stats
	stats           *Stats
	
	// Callbacks
	evictCallback   func(key string) // Called when a key is evicted
	memUsageFunc    func() int64     // Returns current memory usage
	
	mu              sync.RWMutex
	running         bool
	stopCh          chan struct{}
}

// Stats tracks memory limiter statistics
type Stats struct {
	EvictedKeys    uint64
	EvictedNonVolatile uint64
	EvictedByPolicy  map[string]uint64
}

// Config configures the memory limiter
type Config struct {
	MaxMemory    int64
	Policy       string
	Samples      int
	EvictCallback func(key string)
	MemUsageFunc  func() int64
}

// NewLimiter creates a new memory limiter
func NewLimiter(config *Config) *Limiter {
	if config == nil {
		config = &Config{}
	}
	
	if config.Samples <= 0 {
		config.Samples = 5 // Default samples for LRU/LFU
	}
	
	l := &Limiter{
		maxMemory:     config.MaxMemory,
		policy:        ParseEvictionPolicy(config.Policy),
		samples:       config.Samples,
		evictCallback: config.EvictCallback,
		memUsageFunc:  config.MemUsageFunc,
		stats: &Stats{
			EvictedByPolicy: make(map[string]uint64),
		},
		stopCh: make(chan struct{}),
	}
	
	return l
}

// Start starts the memory limiter background task
func (l *Limiter) Start() {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	if l.running {
		return
	}
	
	l.running = true
	go l.monitor()
}

// Stop stops the memory limiter
func (l *Limiter) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	if !l.running {
		return
	}
	
	l.running = false
	close(l.stopCh)
}

// monitor monitors memory usage and triggers eviction if needed
func (l *Limiter) monitor() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			if l.shouldEvict() {
				l.EvictIfNeeded()
			}
		case <-l.stopCh:
			return
		}
	}
}

// shouldEvict checks if memory eviction should be triggered
func (l *Limiter) shouldEvict() bool {
	if l.maxMemory <= 0 {
		return false
	}
	
	memUsed := l.getMemoryUsage()
	return memUsed >= l.maxMemory
}

// getMemoryUsage returns current memory usage
func (l *Limiter) getMemoryUsage() int64 {
	if l.memUsageFunc != nil {
		return l.memUsageFunc()
	}
	
	// Default: use runtime stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.TotalAlloc)
}

// EvictIfNeeded performs eviction if memory limit is exceeded
func (l *Limiter) EvictIfNeeded() bool {
	if l.maxMemory <= 0 || l.policy == NoEviction {
		return false
	}
	
	// Try to free enough memory (10% of maxmemory)
	target := l.maxMemory / 10
	freed := int64(0)
	
	for freed < target && l.shouldEvict() {
		key, ok := l.selectKeyToEvict()
		if !ok {
			return false // No key could be evicted
		}
		
		if l.evictCallback != nil {
			l.evictCallback(key)
		}
		
		l.stats.EvictedKeys++
		l.stats.EvictedByPolicy[l.policy.String()]++
		
		// Estimate freed memory (simplified)
		freed += 1024 // Assume 1KB per key
	}
	
	return true
}

// selectKeyToEvict selects a key to evict based on policy
func (l *Limiter) selectKeyToEvict() (string, bool) {
	// This is a simplified implementation
	// Real implementation would need access to the database to:
	// 1. Sample keys according to the policy
	// 2. Track access patterns for LRU/LFU
	// 3. Check TTL for volatile-* policies
	
	// For now, return empty - the actual eviction logic would be
	// implemented in the database layer
	return "", false
}

// CheckWriteAllowed checks if a write operation should be allowed
func (l *Limiter) CheckWriteAllowed(writeSize int64) error {
	if l.maxMemory <= 0 {
		return nil
	}
	
	if l.policy == NoEviction {
		memUsed := l.getMemoryUsage()
		if memUsed+writeSize > l.maxMemory {
			return ErrMemoryLimitExceeded
		}
	}
	
	// For other policies, try to make room
	if l.shouldEvict() {
		l.EvictIfNeeded()
	}
	
	return nil
}

// GetStats returns memory limiter statistics
func (l *Limiter) GetStats() Stats {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	// Copy stats
	s := *l.stats
	s.EvictedByPolicy = make(map[string]uint64)
	for k, v := range l.stats.EvictedByPolicy {
		s.EvictedByPolicy[k] = v
	}
	
	return s
}

// GetConfig returns current configuration
func (l *Limiter) GetConfig() (maxMemory int64, policy string) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	return l.maxMemory, l.policy.String()
}

// SetMaxMemory sets the maximum memory limit
func (l *Limiter) SetMaxMemory(maxMemory int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	l.maxMemory = maxMemory
}

// SetPolicy sets the eviction policy
func (l *Limiter) SetPolicy(policy string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	l.policy = ParseEvictionPolicy(policy)
}

// SetEvictCallback sets the callback function for key eviction
func (l *Limiter) SetEvictCallback(cb func(key string)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	l.evictCallback = cb
}

// IsEvictionAllowed returns true if keys can be evicted
func (l *Limiter) IsEvictionAllowed() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	return l.policy != NoEviction && l.maxMemory > 0
}

// MemoryInfo returns memory information
func (l *Limiter) MemoryInfo() map[string]interface{} {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	memUsed := l.getMemoryUsage()
	
	return map[string]interface{}{
		"maxmemory":            l.maxMemory,
		"maxmemory_human":      humanReadableSize(uint64(l.maxMemory)),
		"used_memory":          memUsed,
		"used_memory_human":    humanReadableSize(uint64(memUsed)),
		"maxmemory_policy":     l.policy.String(),
		"eviction_allowed":     l.policy != NoEviction,
	}
}

// common errors
var (
	ErrMemoryLimitExceeded = &MemoryError{Message: "memory limit exceeded"}
)

// MemoryError represents a memory-related error
type MemoryError struct {
	Message string
}

func (e *MemoryError) Error() string {
	return e.Message
}

// humanReadableSize converts bytes to human readable format
func humanReadableSize(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%dB", bytes)
	}
	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f%cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
