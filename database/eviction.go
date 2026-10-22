package database

import (
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/memory"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

const (
	bytesPerKeyEstimate = 128
	evictionShardCount  = 64
)

type evictionShard struct {
	mu           sync.RWMutex
	lastAccess   map[string]time.Time
	accessCount  map[string]uint64
	lastModified map[string]time.Time
}

// EvictionManager handles key eviction based on maxmemory-policy
type EvictionManager struct {
	db      *DB
	policy  memory.EvictionPolicy
	samples int

	shards [evictionShardCount]evictionShard
}

// NewEvictionManager creates a new eviction manager
func NewEvictionManager(db *DB, policy memory.EvictionPolicy) *EvictionManager {
	em := &EvictionManager{
		db:      db,
		policy:  policy,
		samples: 5,
	}
	for i := range em.shards {
		em.shards[i] = evictionShard{
			lastAccess:   make(map[string]time.Time),
			accessCount:  make(map[string]uint64),
			lastModified: make(map[string]time.Time),
		}
	}
	return em
}

func (em *EvictionManager) shardFor(key string) *evictionShard {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return &em.shards[h&(evictionShardCount-1)]
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

const (
	lfuInitVal   = 5
	lfuLogFactor = 10
)

// Touch records an access for LRU/LFU accounting.
func (em *EvictionManager) Touch(key string) {
	if em == nil {
		return
	}
	sh := em.shardFor(key)
	sh.mu.Lock()
	sh.lastAccess[key] = time.Now()
	switch em.policy {
	case memory.AllKeysLFU, memory.VolatileLFU:
		sh.accessCount[key] = lfuLogIncr(sh.accessCount[key])
	default:
		sh.accessCount[key]++
	}
	sh.mu.Unlock()
}

// lfuLogIncr approximates Redis LFULogIncr: probabilistic counter 0..255.
func lfuLogIncr(counter uint64) uint64 {
	if counter == 0 {
		return lfuInitVal
	}
	if counter >= 255 {
		return 255
	}
	baseval := float64(counter) - lfuInitVal
	if baseval < 0 {
		baseval = 0
	}
	p := 1.0 / (baseval*float64(lfuLogFactor) + 1)
	if rand.Float64() < p {
		counter++
	}
	return counter
}

// SeedIdle sets last-access so OBJECT IDLETIME reports approximately idleSec.
func (em *EvictionManager) SeedIdle(key string, idleSec int64) {
	if em == nil || idleSec < 0 {
		return
	}
	sh := em.shardFor(key)
	sh.mu.Lock()
	sh.lastAccess[key] = time.Now().Add(-time.Duration(idleSec) * time.Second)
	sh.mu.Unlock()
}

// SeedFreq sets LFU access count for OBJECT FREQ / eviction.
func (em *EvictionManager) SeedFreq(key string, freq uint64) {
	if em == nil {
		return
	}
	sh := em.shardFor(key)
	sh.mu.Lock()
	sh.accessCount[key] = freq
	sh.mu.Unlock()
}

// Modify records a write for LRM accounting (Redis 8.6: the timestamp is
// only bumped by writes, unlike LRU's read+write Touch).
func (em *EvictionManager) Modify(keys ...string) {
	if em == nil || len(keys) == 0 {
		return
	}
	now := time.Now()
	for _, key := range keys {
		sh := em.shardFor(key)
		sh.mu.Lock()
		sh.lastModified[key] = now
		sh.mu.Unlock()
	}
}

// SeedModify backdates the modification time (testing / RESTORE-style seeding).
func (em *EvictionManager) SeedModify(key string, agoSec int64) {
	if em == nil || agoSec < 0 {
		return
	}
	sh := em.shardFor(key)
	sh.mu.Lock()
	sh.lastModified[key] = time.Now().Add(-time.Duration(agoSec) * time.Second)
	sh.mu.Unlock()
}

// Forget drops metadata when a key is removed.
func (em *EvictionManager) Forget(key string) {
	if em == nil {
		return
	}
	sh := em.shardFor(key)
	sh.mu.Lock()
	delete(sh.lastAccess, key)
	delete(sh.accessCount, key)
	delete(sh.lastModified, key)
	sh.mu.Unlock()
}

// IdleSeconds returns seconds since last access (0 if unknown).
func (em *EvictionManager) IdleSeconds(key string) int64 {
	if em == nil {
		return 0
	}
	sh := em.shardFor(key)
	sh.mu.RLock()
	t, ok := sh.lastAccess[key]
	sh.mu.RUnlock()
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
	sh := em.shardFor(key)
	sh.mu.RLock()
	n := sh.accessCount[key]
	sh.mu.RUnlock()
	return int64(n)
}

// EvictKeys evicts keys until the target memory is freed.
// Propagates DEL via AOF (and thus the replication backlog), matching Redis
// and the key-expiry path in DB.Expire / IsExpired.
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
		em.db.RWLocks([]string{key}, nil)
		freed += estimateStoredKeyBytes(em.db, key)
		// removeKey(false): skip generic "del" notify; emit "evicted" below.
		em.db.removeKey(key, false)
		em.db.addVersion(key)
		notifyKeyspaceEvent(em.db, "evicted", key)
		if em.db.addAof != nil {
			em.db.addAof(utils.ToCmdLine3("del", []byte(key)))
		}
		em.db.RWUnLocks([]string{key}, nil)
		evicted++
	}

	return evicted
}

func (em *EvictionManager) selectKey() (string, bool) {
	switch em.policy {
	case memory.AllKeysRandom, memory.VolatileRandom:
		return em.selectRandomKey()
	case memory.AllKeysLRU, memory.VolatileLRU:
		return em.selectLRUKey()
	case memory.AllKeysLRM, memory.VolatileLRM:
		return em.selectLRMKey()
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
			if _, exists := em.db.ttlMap.Get(key); exists {
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
	for _, key := range keys {
		if em.policy == memory.VolatileLRU {
			if _, exists := em.db.ttlMap.Get(key); !exists {
				continue
			}
		}
		sh := em.shardFor(key)
		sh.mu.RLock()
		t, ok := sh.lastAccess[key]
		sh.mu.RUnlock()
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

// selectLRMKey samples keys and picks the one whose last write is oldest.
// Keys without a recorded modification time count as oldest (never written
// through a tracked command path).
func (em *EvictionManager) selectLRMKey() (string, bool) {
	keys := em.db.data.RandomKeys(em.samples)
	var candidate string
	var oldest time.Time
	for _, key := range keys {
		if em.policy == memory.VolatileLRM {
			if _, exists := em.db.ttlMap.Get(key); !exists {
				continue
			}
		}
		sh := em.shardFor(key)
		sh.mu.RLock()
		t, ok := sh.lastModified[key]
		sh.mu.RUnlock()
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
	for _, key := range keys {
		if em.policy == memory.VolatileLFU {
			if _, exists := em.db.ttlMap.Get(key); !exists {
				continue
			}
		}
		sh := em.shardFor(key)
		sh.mu.RLock()
		count := sh.accessCount[key]
		sh.mu.RUnlock()
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
		rawExpire, exists := em.db.ttlMap.Get(key)
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
			n = db.evictionManager.EvictKeys(approxWrite)
		}
		if n == 0 {
			for _, holder := range server.dbSet {
				other := holder.Load().(*DB)
				if other == db || other.evictionManager == nil {
					continue
				}
				n = other.evictionManager.EvictKeys(approxWrite)
				if n > 0 {
					break
				}
			}
		}
		if n == 0 {
			return protocol.MakeErrReply("OOM command not allowed when used memory > 'maxmemory'")
		}
		atomic.AddUint64(&serverStats.EvictedKeys, uint64(n))
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

// approxKeyMemoryUsage reports dataset memory for maxmemory in O(databases):
// incremental per-DB counters instead of the O(N) full walk that used to run
// on every write command and in the 100ms limiter monitor (docs/PERFORMANCE.md
// E5-3: the walk was the dominant cost under eviction pressure).
// Still approximate (not jemalloc / Go allocator truth).
func (server *Server) approxKeyMemoryUsage() int64 {
	if server == nil {
		return 0
	}
	var n int64
	for _, holder := range server.dbSet {
		n += holder.Load().(*DB).estimatedBytes.Load()
	}
	return n
}

// accountedKeyBytes is the per-key maxmemory estimate: floor at
// bytesPerKeyEstimate so small keys stay counted (~128B) while large values
// are not severely undercounted. Same rule the pre-E5-3 full walk used;
// nil entities/data count as the floor (placeholder puts).
func accountedKeyBytes(key string, data interface{}) int64 {
	if data == nil {
		return bytesPerKeyEstimate
	}
	if est := estimateEntityBytes(key, data, 5); est > bytesPerKeyEstimate {
		return est
	}
	return bytesPerKeyEstimate
}

// walkEstimatedBytes re-derives a DB's accounted bytes from a full scan
// (reconciliation path; also the pre-E5-3 hot-path behavior).
func walkEstimatedBytes(db *DB) int64 {
	var n int64
	db.data.ForEach(func(key string, val interface{}) bool {
		if entity, ok := val.(*database.DataEntity); ok && entity != nil {
			n += accountedKeyBytes(key, entity.Data)
		} else {
			n += bytesPerKeyEstimate
		}
		return true
	})
	return n
}

// reconcileMemoryUsage re-derives each DB counter from a full walk, healing
// drift from in-place mutations of stored collections.
func (server *Server) reconcileMemoryUsage() {
	if server == nil {
		return
	}
	for _, holder := range server.dbSet {
		db := holder.Load().(*DB)
		db.estimatedBytes.Store(walkEstimatedBytes(db))
	}
}

// startMemoryReconciler reconciles incremental accounting once a minute.
func (server *Server) startMemoryReconciler() {
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				server.reconcileMemoryUsage()
			case <-server.stopCron:
				return
			}
		}
	}()
}

// approxCmdWriteBytes estimates bytes a write command may add (for maxmemory).
func approxCmdWriteBytes(cmdLine [][]byte) int64 {
	if len(cmdLine) < 2 {
		return bytesPerKeyEstimate
	}
	cmd := strings.ToLower(string(cmdLine[0]))
	switch cmd {
	case "set", "setnx", "setex", "psetex", "getset":
		if len(cmdLine) >= 3 {
			return floorKeyEstimate(int64(len(cmdLine[1]) + len(cmdLine[2]) + 64))
		}
	case "mset", "msetnx":
		var sum int64
		for i := 1; i+1 < len(cmdLine); i += 2 {
			sum += int64(len(cmdLine[i]) + len(cmdLine[i+1]) + 64)
		}
		return floorKeyEstimate(sum)
	case "append", "setrange":
		if len(cmdLine) >= 3 {
			return floorKeyEstimate(int64(len(cmdLine[len(cmdLine)-1]) + 64))
		}
	}
	return bytesPerKeyEstimate
}

func floorKeyEstimate(n int64) int64 {
	if n < bytesPerKeyEstimate {
		return bytesPerKeyEstimate
	}
	return n
}

func estimateStoredKeyBytes(db *DB, key string) int64 {
	if db == nil {
		return bytesPerKeyEstimate
	}
	entity, ok := db.GetEntity(key)
	if !ok || entity == nil {
		return bytesPerKeyEstimate
	}
	return accountedKeyBytes(key, entity.Data)
}
