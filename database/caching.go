package database

import (
	"strconv"
	"sync"
	"time"

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
	
	// Tracking mode: broadcast, prefix, optin, optout
	trackingMode map[string]string
	prefixes     map[string][]string // client -> prefixes
	
	// Invalidation messages queue per client
	invalidationQueues map[string]chan []string
	
	// Connection mapping for sending pushes
	connections map[string]redis.Connection
	
	mu sync.RWMutex
}

// Global client cache instance
var clientCache = &ClientCache{
	trackedKeys:        make(map[string]map[string]bool),
	keyClients:         make(map[string]map[string]bool),
	trackingEnabled:    make(map[string]bool),
	trackingMode:       make(map[string]string),
	prefixes:           make(map[string][]string),
	invalidationQueues: make(map[string]chan []string),
	connections:        make(map[string]redis.Connection),
}

// EnableTracking enables client tracking for a connection
func EnableTracking(conn redis.Connection, mode string, prefixes []string) string {
	clientCache.mu.Lock()
	defer clientCache.mu.Unlock()
	
	clientID := conn.Name()
	if clientID == "" {
		clientID = generateClientID()
	}
	
	clientCache.trackingEnabled[clientID] = true
	clientCache.trackingMode[clientID] = mode
	clientCache.prefixes[clientID] = prefixes
	clientCache.connections[clientID] = conn
	
	if clientCache.trackedKeys[clientID] == nil {
		clientCache.trackedKeys[clientID] = make(map[string]bool)
	}
	if clientCache.invalidationQueues[clientID] == nil {
		clientCache.invalidationQueues[clientID] = make(chan []string, 100)
	}
	
	// Start background sender for this client
	go clientCache.invalidationSender(clientID)
	
	return clientID
}

// DisableTracking disables client tracking
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
	delete(clientCache.trackingMode, clientID)
	delete(clientCache.prefixes, clientID)
	delete(clientCache.trackedKeys, clientID)
	delete(clientCache.connections, clientID)
	
	// Close and delete queue
	if queue, ok := clientCache.invalidationQueues[clientID]; ok {
		close(queue)
		delete(clientCache.invalidationQueues, clientID)
	}
}

// TrackKey tracks a key for potential invalidation
func TrackKey(clientID string, key string) {
	clientCache.mu.Lock()
	defer clientCache.mu.Unlock()
	
	if !clientCache.trackingEnabled[clientID] {
		return
	}
	
	// Check prefix match for BCAST mode
	mode := clientCache.trackingMode[clientID]
	if mode == "bcast" {
		prefixes := clientCache.prefixes[clientID]
		if len(prefixes) > 0 {
			matched := false
			for _, prefix := range prefixes {
				if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
					matched = true
					break
				}
			}
			if !matched {
				return
			}
		}
	}
	
	// Add to tracked keys
	clientCache.trackedKeys[clientID][key] = true
	
	// Add to key clients
	if clientCache.keyClients[key] == nil {
		clientCache.keyClients[key] = make(map[string]bool)
	}
	clientCache.keyClients[key][clientID] = true
}

// InvalidateKey invalidates a key for all tracking clients
func InvalidateKey(key string) {
	clientCache.mu.RLock()
	clients := clientCache.keyClients[key]
	clientCache.mu.RUnlock()
	
	if clients == nil {
		return
	}
	
	// Send invalidation to all clients
	for clientID := range clients {
		clientCache.mu.RLock()
		queue, ok := clientCache.invalidationQueues[clientID]
		mode := clientCache.trackingMode[clientID]
		clientCache.mu.RUnlock()
		
		if ok {
			select {
			case queue <- []string{key}:
			default:
				// Queue full, skip
			}
		}
		
		// For BCAST mode, don't remove tracking
		if mode != "bcast" {
			clientCache.mu.Lock()
			delete(clientCache.trackedKeys[clientID], key)
			clientCache.mu.Unlock()
		}
	}
	
	// Clean up for non-bcast
	clientCache.mu.Lock()
	if keyClients := clientCache.keyClients[key]; keyClients != nil {
		for clientID := range keyClients {
			if clientCache.trackingMode[clientID] != "bcast" {
				delete(keyClients, clientID)
			}
		}
		if len(keyClients) == 0 {
			delete(clientCache.keyClients, key)
		}
	}
	clientCache.mu.Unlock()
}

// InvalidateKeysOnWrite invalidates multiple keys after write
func InvalidateKeysOnWrite(keys []string) {
	for _, key := range keys {
		InvalidateKey(key)
	}
}

// invalidationSender sends invalidation messages to client
func (cc *ClientCache) invalidationSender(clientID string) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		cc.mu.RLock()
		queue, ok := cc.invalidationQueues[clientID]
		conn, hasConn := cc.connections[clientID]
		cc.mu.RUnlock()
		
		if !ok {
			return
		}
		
		select {
		case keys, ok := <-queue:
			if !ok {
				return
			}
			if hasConn {
				sendInvalidation(conn, keys)
			}
		case <-ticker.C:
			// Continue
		}
	}
}

// sendInvalidation sends invalidation push message
func sendInvalidation(conn redis.Connection, keys []string) {
	push := protocol.MakeInvalidatePush(keys)
	
	// Write to connection
	conn.Write(push.ToBytes())
}

// IsTrackingEnabled checks if tracking is enabled for a client
func IsTrackingEnabled(clientID string) bool {
	clientCache.mu.RLock()
	defer clientCache.mu.RUnlock()
	
	return clientCache.trackingEnabled[clientID]
}

// TrackKeysOnRead should be called when keys are read
func TrackKeysOnRead(clientID string, keys []string) {
	if !IsTrackingEnabled(clientID) {
		return
	}
	
	for _, key := range keys {
		TrackKey(clientID, key)
	}
}

// generateClientID generates a unique client ID
func generateClientID() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}

// GetTrackingInfo returns tracking info for a client
func GetTrackingInfo(clientID string) map[string]interface{} {
	clientCache.mu.RLock()
	defer clientCache.mu.RUnlock()
	
	return map[string]interface{}{
		"enabled": clientCache.trackingEnabled[clientID],
		"mode":    clientCache.trackingMode[clientID],
		"prefixes": clientCache.prefixes[clientID],
		"keys":    len(clientCache.trackedKeys[clientID]),
	}
}

// Hook into database write operations
func init() {
	// This would be called when keys are modified
}


