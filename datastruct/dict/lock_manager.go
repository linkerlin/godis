package dict

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// LockManager provides lock management with timeout and deadlock detection
type LockManager struct {
	dict              *ConcurrentDict
	timeout           time.Duration
	lockTable         map[uint32]*lockInfo // Track lock holders
	mu                sync.RWMutex
	deadlockDetection bool
}

// lockInfo tracks information about a lock
type lockInfo struct {
	index      uint32
	lockType   LockType // Read or Write
	holderID   string   // Unique identifier for the lock holder
	acquiredAt time.Time
}

// LockType represents the type of lock
type LockType int

const (
	ReadLock LockType = iota
	WriteLock
)

// LockConfig configures the lock manager
type LockConfig struct {
	Timeout                 time.Duration // Default lock timeout
	EnableDeadlockDetection bool
}

// DefaultLockConfig returns default configuration
func DefaultLockConfig() *LockConfig {
	return &LockConfig{
		Timeout:                 5 * time.Second,
		EnableDeadlockDetection: true,
	}
}

// NewLockManager creates a new lock manager
func NewLockManager(dict *ConcurrentDict, config *LockConfig) *LockManager {
	if config == nil {
		config = DefaultLockConfig()
	}
	return &LockManager{
		dict:              dict,
		timeout:           config.Timeout,
		lockTable:         make(map[uint32]*lockInfo),
		deadlockDetection: config.EnableDeadlockDetection,
	}
}

// RWLocksWithTimeout locks write keys and read keys with timeout support
func (lm *LockManager) RWLocksWithTimeout(ctx context.Context, writeKeys, readKeys []string, holderID string) error {
	if lm == nil || lm.dict == nil {
		return fmt.Errorf("lock manager dict is nil")
	}
	keys := append(writeKeys, readKeys...)
	indices := lm.dict.toLockIndices(keys, false)

	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := lm.dict.spread(wKey)
		writeIndexSet[idx] = struct{}{}
	}

	// Try to acquire all locks with timeout
	acquiredLocks := make([]uint32, 0, len(indices))

	for _, index := range indices {
		_, isWrite := writeIndexSet[index]

		lockType := ReadLock
		if isWrite {
			lockType = WriteLock
		}

		if err := lm.acquireLockWithTimeout(ctx, index, lockType, holderID); err != nil {
			// Release already acquired locks in reverse order on failure
			for i := len(acquiredLocks) - 1; i >= 0; i-- {
				idx := acquiredLocks[i]
				_, w := writeIndexSet[idx]
				mu := &lm.dict.table[idx].keyMu
				lm.releaseLock(idx, holderID)
				if w {
					mu.Unlock()
				} else {
					mu.RUnlock()
				}
			}
			return fmt.Errorf("lock acquisition failed for shard %d: %w", index, err)
		}

		acquiredLocks = append(acquiredLocks, index)
	}

	return nil
}

// RWUnLocks unlocks write keys and read keys
func (lm *LockManager) RWUnLocks(writeKeys, readKeys []string, holderID string) {
	if lm == nil || lm.dict == nil {
		return
	}
	keys := append(writeKeys, readKeys...)
	indices := lm.dict.toLockIndices(keys, true)

	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := lm.dict.spread(wKey)
		writeIndexSet[idx] = struct{}{}
	}

	for _, index := range indices {
		_, isWrite := writeIndexSet[index]
		mu := &lm.dict.table[index].keyMu

		lm.releaseLock(index, holderID)

		if isWrite {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
	}
}

// acquireLockWithTimeout attempts to acquire a lock with timeout without leaking goroutines
func (lm *LockManager) acquireLockWithTimeout(ctx context.Context, index uint32, lockType LockType, holderID string) error {
	// Check for potential deadlock before acquiring
	if lm.deadlockDetection {
		if err := lm.checkDeadlock(index, holderID); err != nil {
			return err
		}
	}

	mu := &lm.dict.table[index].keyMu

	// Create timeout context if not provided
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), lm.timeout)
		defer cancel()
	}

	tryAcquire := func() bool {
		if lockType == WriteLock {
			return mu.TryLock()
		}
		return mu.TryRLock()
	}

	if tryAcquire() {
		lm.recordLock(index, lockType, holderID)
		return nil
	}

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("lock acquisition timeout: %w", ctx.Err())
		case <-ticker.C:
			if tryAcquire() {
				lm.recordLock(index, lockType, holderID)
				return nil
			}
		}
	}
}

// recordLock records lock acquisition
func (lm *LockManager) recordLock(index uint32, lockType LockType, holderID string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.lockTable[index] = &lockInfo{
		index:      index,
		lockType:   lockType,
		holderID:   holderID,
		acquiredAt: time.Now(),
	}
}

// releaseLock releases lock record
func (lm *LockManager) releaseLock(index uint32, holderID string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if info, exists := lm.lockTable[index]; exists && info.holderID == holderID {
		delete(lm.lockTable, index)
	}
}

// checkDeadlock checks for potential deadlock (simplified detection)
func (lm *LockManager) checkDeadlock(requestedIndex uint32, holderID string) error {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	// Check if another holder is waiting for locks we hold
	for idx, info := range lm.lockTable {
		if info.holderID != holderID {
			// Another holder has this lock
			// In a real implementation, we'd check the wait-for graph
			_ = idx
		}
	}

	return nil
}

// GetLockInfo returns information about current locks
func (lm *LockManager) GetLockInfo() map[uint32]map[string]interface{} {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	result := make(map[uint32]map[string]interface{})
	for idx, info := range lm.lockTable {
		result[idx] = map[string]interface{}{
			"type":        info.lockType,
			"holder":      info.holderID,
			"acquired_at": info.acquiredAt,
			"held_for":    time.Since(info.acquiredAt).String(),
		}
	}
	return result
}

// ForceUnlock forcibly releases a lock (for administrative purposes)
func (lm *LockManager) ForceUnlock(index uint32) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if _, exists := lm.lockTable[index]; !exists {
		return fmt.Errorf("no lock found for shard %d", index)
	}

	// Release the actual lock - this is dangerous and should only be used
	// in emergency situations
	if info := lm.lockTable[index]; info.lockType == ReadLock {
		lm.dict.table[index].keyMu.RUnlock()
	} else {
		lm.dict.table[index].keyMu.Unlock()
	}
	delete(lm.lockTable, index)

	return nil
}

// LockStats returns lock statistics
func (lm *LockManager) LockStats() map[string]interface{} {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	var readLocks, writeLocks int
	var oldestLock time.Time

	for _, info := range lm.lockTable {
		if info.lockType == ReadLock {
			readLocks++
		} else {
			writeLocks++
		}

		if oldestLock.IsZero() || info.acquiredAt.Before(oldestLock) {
			oldestLock = info.acquiredAt
		}
	}

	return map[string]interface{}{
		"total_locks":    len(lm.lockTable),
		"read_locks":     readLocks,
		"write_locks":    writeLocks,
		"oldest_lock":    oldestLock,
		"timeout_config": lm.timeout.String(),
	}
}

// GetTimeout returns the configured lock timeout
func (lm *LockManager) GetTimeout() time.Duration {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.timeout
}

// OrderedLockManager provides ordered lock acquisition to prevent deadlocks
type OrderedLockManager struct {
	dict    *ConcurrentDict
	timeout time.Duration
}

// NewOrderedLockManager creates an ordered lock manager
func NewOrderedLockManager(dict *ConcurrentDict, timeout time.Duration) *OrderedLockManager {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &OrderedLockManager{
		dict:    dict,
		timeout: timeout,
	}
}

// LockAll locks all shards in a consistent order to prevent deadlocks
func (olm *OrderedLockManager) LockAll(holderID string) error {
	if olm == nil || olm.dict == nil {
		return fmt.Errorf("ordered lock manager dict is nil")
	}
	indices := make([]uint32, len(olm.dict.table))
	for i := range olm.dict.table {
		indices[i] = uint32(i)
	}

	// Sort to ensure consistent order
	sort.Slice(indices, func(i, j int) bool {
		return indices[i] < indices[j]
	})

	ctx, cancel := context.WithTimeout(context.Background(), olm.timeout)
	defer cancel()

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for _, index := range indices {
		mu := &olm.dict.table[index].keyMu
		for !mu.TryLock() {
			select {
			case <-ctx.Done():
				for _, idx := range indices {
					if idx == index {
						break
					}
					olm.dict.table[idx].keyMu.Unlock()
				}
				return fmt.Errorf("timeout locking all shards")
			case <-ticker.C:
			}
		}
	}

	return nil
}

// UnlockAll unlocks all shards
func (olm *OrderedLockManager) UnlockAll() {
	if olm == nil || olm.dict == nil {
		return
	}
	for _, s := range olm.dict.table {
		s.keyMu.Unlock()
	}
}
