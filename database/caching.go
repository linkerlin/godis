package database

import (
	"sync"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
)

// ClientCache manages client-side caching
type ClientCache struct {
	// Tracked keys for each client
	// clientID -> set of keys
	trackedKeys map[string]map[string]bool
	
	// Key -> set of clients tracking it
	keyClients map[string]map[string]bool
	
	// Client tracking mode
	trackingEnabled map[string]bool
	
	// Invalidation messages queue per client
	invalidationQueues map[string]chan []string
	
	mu sync.RWMutex
}

// Global client cache instance
var clientCache = &ClientCache{
	trackedKeys:        make(map[string]map[string]bool),
	keyClients:         make(map[string]map[string]bool),
	trackingEnabled:    make(map[string]bool),
	invalidationQueues: make(map[string]chan []string),
}

// TrackKey tracks a key for potential invalidation
func TrackKey(clientID string, key string) {
	clientCache.mu.Lock()
	defer clientCache.mu.Unlock()
	
	// Check if client has tracking enabled
	if !clientCache.trackingEnabled[clientID] {
		return
	}
	
	// Add to tracked keys
	if clientCache.trackedKeys[clientID] == nil {
		clientCache.trackedKeys[clientID] = make(map[string]bool)
	}
	clientCache.trackedKeys[clientID][key] = true
	
	// Add to key clients
	if clientCache.keyClients[key] == nil {
		clientCache.keyClients[key] = make(map[string]bool)
	}
	clientCache.keyClients[key][clientID] = true
}

// InvalidateKey invalidates a key for all tracking clients
func InvalidateKey(key string) {
	clientCache.mu.Lock()
	defer clientCache.mu.Unlock()
	
	clients := clientCache.keyClients[key]
	if clients == nil {
		return
	}
	
	// Send invalidation to all clients
	for clientID := range clients {
		if queue, ok := clientCache.invalidationQueues[clientID]; ok {
			select {
			case queue <- []string{key}:
			default:
				// Queue full, skip
			}
		}
		
		// Remove tracking
		delete(clientCache.trackedKeys[clientID], key)
	}
	
	// Clean up
	delete(clientCache.keyClients, key)
}

// SendInvalidation sends invalidation message to client
// This would be called when there's a tracked key change
func SendInvalidation(conn redis.Connection, keys []string) {
	// Format as RESP3 push message
	// >3\r\n$10\r\ninvalidate\r\n*1\r\n$...\r\nkey\r\n
	
	// Build invalidation message
	var keyBytes [][]byte
	for _, k := range keys {
		keyBytes = append(keyBytes, []byte(k))
	}
	
	// Create push reply
	push := protocol.MakePushReply("invalidate", []redis.Reply{
		protocol.MakeMultiBulkReply(keyBytes),
	})
	
	// Send to client (this would need connection write method)
	_ = conn
	_ = push
}

// EnableTracking enables tracking for a client
func EnableTracking(clientID string) {
	clientCache.mu.Lock()
	defer clientCache.mu.Unlock()
	
	clientCache.trackingEnabled[clientID] = true
	if clientCache.trackedKeys[clientID] == nil {
		clientCache.trackedKeys[clientID] = make(map[string]bool)
	}
	if clientCache.invalidationQueues[clientID] == nil {
		clientCache.invalidationQueues[clientID] = make(chan []string, 100)
	}
}

// DisableTracking disables tracking for a client
func DisableTracking(clientID string) {
	clientCache.mu.Lock()
	defer clientCache.mu.Unlock()
	
	// Untrack all keys
	for key := range clientCache.trackedKeys[clientID] {
		if clients := clientCache.keyClients[key]; clients != nil {
			delete(clients, clientID)
			if len(clients) == 0 {
				delete(clientCache.keyClients, key)
			}
		}
	}
	
	delete(clientCache.trackingEnabled, clientID)
	delete(clientCache.trackedKeys, clientID)
	delete(clientCache.invalidationQueues, clientID)
}

// GetInvalidation gets pending invalidations for a client
func GetInvalidation(clientID string) ([]string, bool) {
	clientCache.mu.RLock()
	queue, ok := clientCache.invalidationQueues[clientID]
	clientCache.mu.RUnlock()
	
	if !ok {
		return nil, false
	}
	
	select {
	case keys := <-queue:
		return keys, true
	default:
		return nil, false
	}
}

// IsTrackingEnabled checks if tracking is enabled for a client
func IsTrackingEnabled(clientID string) bool {
	clientCache.mu.RLock()
	defer clientCache.mu.RUnlock()
	
	return clientCache.trackingEnabled[clientID]
}

// TrackKeysOnRead should be called when a key is read
// This tracks the key for clients with BCAST mode or prefix matching
func TrackKeysOnRead(clientID string, keys []string) {
	// In full implementation, would check BCAST mode and prefixes
	// For now, track all keys if tracking is enabled
	if IsTrackingEnabled(clientID) {
		for _, key := range keys {
			TrackKey(clientID, key)
		}
	}
}

// InvalidateKeysOnWrite should be called when keys are modified
func InvalidateKeysOnWrite(keys []string) {
	for _, key := range keys {
		InvalidateKey(key)
	}
}

// Hook into database operations
func init() {
	// Set up hooks for key invalidation
	// This would be called from database operations
}
