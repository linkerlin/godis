package database

import (
	"sync"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/memory"
	"github.com/linkerlin/godis/redis/protocol"
)

const bytesPerKeyEstimate = 128

// EvictionManager handles key eviction based on maxmemory-policy
type EvictionManager struct {
	db      *DB
	policy  memory.EvictionPolicy
	samples int

	mu          sync.Mutex
	lastAccess  map[string]time.Time
	accessCount map[string]uint64
}

// NewEvictionManager creates a new eviction manager
func NewEvictionManager(db *DB, policy memory.EvictionPolicy) *EvictionManager {
	return &EvictionManager{
		db:          db,
		policy:      policy,
		samples:     5,
		lastAccess:  make(map[string]time.Time),
		accessCount: make(map[string]uint64),
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

// Touch records an access for LRU/LFU accounting.
func (em *EvictionManager) Touch(key string) {
	if em == nil {
		return
	}
	em.mu.Lock()
	em.lastAccess[key] = time.Now()
	em.accessCount[key]++
	em.mu.Unlock()
}

// SeedIdle sets last-access so OBJECT IDLETIME reports approximately idleSec.
func (em *EvictionManager) SeedIdle(key string, idleSec int64) {
	if em == nil || idleSec < 0 {
		return
	}
	em.mu.Lock()
	em.lastAccess[key] = time.Now().Add(-time.Duration(idleSec) * time.Second)
	em.mu.Unlock()
}

// SeedFreq sets LFU access count for OBJECT FREQ / eviction.
func (em *EvictionManager) SeedFreq(key string, freq uint64) {
	if em == nil {
		return
	}
	em.mu.Lock()
	em.accessCount[key] = freq
	em.mu.Unlock()
}

// Forget drops metadata when a key is removed.
func (em *EvictionManager) Forget(key string) {
	if em == nil {
		return
	}
	em.mu.Lock()
	delete(em.lastAccess, key)
	delete(em.accessCount, key)
	em.mu.Unlock()
}

// IdleSeconds returns seconds since last access (0 if unknown).
func (em *EvictionManager) IdleSeconds(key string) int64 {
	if em == nil {
		return 0
	}
	em.mu.Lock()
	t, ok := em.lastAccess[key]
	em.mu.Unlock()
	if !ok {
		return 0
	}
	sec := int64(time.Since(t).Seconds())
	if sec < 0 {
		return 0
	}
	return sec
}

// Freq returns access count for LFU (0 if unknown).
func (em *EvictionManager) Freq(key string) int64 {
	if em == nil {
		return 0
	}
	em.mu.Lock()
	n := em.accessCount[key]
	em.mu.Unlock()
	return int64(n)
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
		em.db.Remove(key)
		em.Forget(key)
		evicted++
		freed += bytesPerKeyEstimate
	}

	return evicted
}

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

func (em *EvictionManager) selectRandomKey() (string, bool) {
	if em.policy == memory.VolatileRandom {
		return em.selectRandomKeyWithTTL()
	}
	keys := em.db.data.RandomKeys(em.samples)
	if len(keys) == 0 {
		return "", false
	}
	return keys[0], true
}

func (em *EvictionManager) selectRandomKeyWithTTL() (string, bool) {
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

func (em *EvictionManager) selectLRUKey() (string, bool) {
	keys := em.db.data.RandomKeys(em.samples)
	var candidate string
	var oldest time.Time
	em.mu.Lock()
	defer em.mu.Unlock()
	for _, key := range keys {
		if em.policy == memory.VolatileLRU {
			if _, exists := em.db.ttlMap.GetWithLock(key); !exists {
				continue
			}
		}
		t, ok := em.lastAccess[key]
		if !ok {
			return key, true
		}
		if oldest.IsZero() || t.Before(oldest) {
			oldest = t
			candidate = key
		}
	}
	if candidate == "" {
		return "", false
	}
	return candidate, true
}

func (em *EvictionManager) selectLFUKey() (string, bool) {
	keys := em.db.data.RandomKeys(em.samples)
	var candidate string
	var minCount uint64 = ^uint64(0)
	em.mu.Lock()
	defer em.mu.Unlock()
	for _, key := range keys {
		if em.policy == memory.VolatileLFU {
			if _, exists := em.db.ttlMap.GetWithLock(key); !exists {
				continue
			}
		}
		count := em.accessCount[key]
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

func (em *EvictionManager) selectTTLKey() (string, bool) {
	var candidate string
	var shortestTTL time.Time
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

// ensureMemoryForWrite enforces maxmemory before a write command.
func (server *Server) ensureMemoryForWrite(db *DB, approxWrite int64) redis.Reply {
	if server == nil || server.memLimiter == nil || db == nil {
		return nil
	}
	maxMem, policy := server.memLimiter.GetConfig()
	if maxMem <= 0 {
		return nil
	}
	if approxWrite <= 0 {
		approxWrite = bytesPerKeyEstimate
	}
	for i := 0; i < 256; i++ {
		used := server.memLimiter.UsedMemory()
		if used+approxWrite <= maxMem {
			return nil
		}
		if policy == "noeviction" {
			return protocol.MakeErrReply("OOM command not allowed when used memory > 'maxmemory'")
		}
		n := 0
		if db.evictionManager != nil {
			n = db.evictionManager.EvictKeys(bytesPerKeyEstimate)
		}
		if n == 0 {
			for _, holder := range server.dbSet {
				other := holder.Load().(*DB)
				if other == db || other.evictionManager == nil {
					continue
				}
				n = other.evictionManager.EvictKeys(bytesPerKeyEstimate)
				if n > 0 {
					break
				}
			}
		}
		if n == 0 {
			return protocol.MakeErrReply("OOM command not allowed when used memory > 'maxmemory'")
		}
	}
	return protocol.MakeErrReply("OOM command not allowed when used memory > 'maxmemory'")
}

// syncMemoryConfig pushes Properties maxmemory settings into runtime limiter/managers.
func (server *Server) syncMemoryConfig() {
	if server == nil || server.memLimiter == nil || config.Properties == nil {
		return
	}
	server.memLimiter.SetMaxMemory(config.Properties.Maxmemory)
	pol := config.Properties.MaxmemoryPolicy
	if pol == "" {
		pol = "noeviction"
	}
	server.memLimiter.SetPolicy(pol)
	ep := memory.ParseEvictionPolicy(pol)
	for _, holder := range server.dbSet {
		db := holder.Load().(*DB)
		if db.evictionManager != nil {
			db.evictionManager.SetPolicy(ep)
		}
	}
}

// approxKeyMemoryUsage estimates used memory from key counts (test-friendly).
func (server *Server) approxKeyMemoryUsage() int64 {
	if server == nil {
		return 0
	}
	var n int64
	for _, holder := range server.dbSet {
		db := holder.Load().(*DB)
		n += int64(db.data.Len()) * bytesPerKeyEstimate
	}
	return n
}
