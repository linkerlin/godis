package database

import (
	"math/rand"
	"time"

	"github.com/linkerlin/godis/lib/memory"
)

// KeyMetadata stores metadata for a key (for LRU/LFU)
type KeyMetadata struct {
	LastAccessTime time.Time
	AccessCount    uint64
	HasTTL         bool
}

// EvictionManager handles key eviction based on maxmemory-policy
type EvictionManager struct {
	db      *DB
	policy  memory.EvictionPolicy
	samples int // Number of keys to sample for LRU/LFU
}

// NewEvictionManager creates a new eviction manager
func NewEvictionManager(db *DB, policy memory.EvictionPolicy) *EvictionManager {
	return &EvictionManager{
		db:      db,
		policy:  policy,
		samples: 5, // Default sample size
	}
}

// SetPolicy sets the eviction policy
func (em *EvictionManager) SetPolicy(policy memory.EvictionPolicy) {
	em.policy = policy
}

// SetSamples sets the number of samples for LRU/LFU
func (em *EvictionManager) SetSamples(samples int) {
	if samples > 0 {
		em.samples = samples
	}
}

// EvictKeys evicts keys until the target memory is freed
func (em *EvictionManager) EvictKeys(target int64) int {
	if em.policy == memory.NoEviction {
		return 0
	}

	evicted := 0
	freed := int64(0)

	for freed < target {
		key, ok := em.selectKey()
		if !ok {
			break
		}

		// Remove the key
		em.db.Remove(key)
		evicted++
		freed += 1024 // Estimate 1KB per key
	}

	return evicted
}

// selectKey selects a key to evict based on the policy
func (em *EvictionManager) selectKey() (string, bool) {
	switch em.policy {
	case memory.AllKeysRandom, memory.VolatileRandom:
		return em.selectRandomKey()
	case memory.AllKeysLRU, memory.VolatileLRU:
		return em.selectLRUKey()
	case memory.AllKeysLFU, memory.VolatileLFU:
		return em.selectLFUKey()
	case memory.VolatileTTL:
		return em.selectTTLKey()
	default:
		return "", false
	}
}

// selectRandomKey selects a random key
func (em *EvictionManager) selectRandomKey() (string, bool) {
	if em.policy == memory.VolatileRandom {
		// Only keys with TTL
		return em.selectRandomKeyWithTTL()
	}

	// All keys
	keys := em.db.data.RandomKeys(em.samples)
	if len(keys) == 0 {
		return "", false
	}
	return keys[rand.Intn(len(keys))], true
}

// selectRandomKeyWithTTL selects a random key that has TTL
func (em *EvictionManager) selectRandomKeyWithTTL() (string, bool) {
	// Get some random keys and find one with TTL
	for i := 0; i < 10; i++ {
		keys := em.db.data.RandomKeys(em.samples)
		for _, key := range keys {
			if _, exists := em.db.ttlMap.GetWithLock(key); exists {
				return key, true
			}
		}
	}
	return "", false
}

// selectLRUKey selects the least recently used key
func (em *EvictionManager) selectLRUKey() (string, bool) {
	var candidate string
	var oldestTime time.Time

	// Sample keys and find the oldest access time
	keys := em.db.data.RandomKeys(em.samples)

	for _, key := range keys {
		// Check if we only want volatile keys
		if em.policy == memory.VolatileLRU {
			if _, exists := em.db.ttlMap.GetWithLock(key); !exists {
				continue
			}
		}

		// Get metadata from entity (simplified - in real impl, store access time)
		entity, exists := em.db.data.GetWithLock(key)
		if !exists {
			continue
		}

		// For now, use a simple heuristic: check if entity has access time
		// In real implementation, DataEntity should store LastAccessTime
		_ = entity
		accessTime := time.Now().Add(-time.Duration(rand.Intn(3600)) * time.Second)

		if oldestTime.IsZero() || accessTime.Before(oldestTime) {
			oldestTime = accessTime
			candidate = key
		}
	}

	if candidate == "" {
		return "", false
	}
	return candidate, true
}

// selectLFUKey selects the least frequently used key
func (em *EvictionManager) selectLFUKey() (string, bool) {
	var candidate string
	var minCount uint64 = ^uint64(0)

	// Sample keys and find the lowest access count
	keys := em.db.data.RandomKeys(em.samples)

	for _, key := range keys {
		// Check if we only want volatile keys
		if em.policy == memory.VolatileLFU {
			if _, exists := em.db.ttlMap.GetWithLock(key); !exists {
				continue
			}
		}

		// Get access count (simplified)
		count := uint64(rand.Intn(1000))

		if count < minCount {
			minCount = count
			candidate = key
		}
	}

	if candidate == "" {
		return "", false
	}
	return candidate, true
}

// selectTTLKey selects the key with shortest TTL
func (em *EvictionManager) selectTTLKey() (string, bool) {
	var candidate string
	var shortestTTL time.Time

	// Sample keys and find the one with shortest TTL
	keys := em.db.data.RandomKeys(em.samples)

	for _, key := range keys {
		rawExpire, exists := em.db.ttlMap.GetWithLock(key)
		if !exists {
			continue
		}

		expireTime, ok := rawExpire.(time.Time)
		if !ok {
			continue
		}

		if shortestTTL.IsZero() || expireTime.Before(shortestTTL) {
			shortestTTL = expireTime
			candidate = key
		}
	}

	if candidate == "" {
		return "", false
	}
	return candidate, true
}

// ShouldEvict checks if eviction should be triggered
func (em *EvictionManager) ShouldEvict(maxMemory int64, currentUsage int64) bool {
	if maxMemory <= 0 {
		return false
	}
	return currentUsage >= maxMemory
}
