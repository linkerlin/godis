package database

import (
	"strconv"
	"sync"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
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

	// Redirect mapping: clientID -> redirect target clientID
	redirects map[string]string

	// No-loop flags per client
	noLoop map[string]bool

	// OPTIN/OPTOUT one-shot directives from CLIENT CACHING YES|NO
	cachingOptInNext  map[string]bool
	cachingOptOutNext map[string]bool

	// Stats
	trackingClientsCount int
	invalidationMsgsSent uint64

	mu sync.RWMutex
}

// Global client cache instance
var clientCache = &ClientCache{
	trackedKeys:          make(map[string]map[string]bool),
	keyClients:           make(map[string]map[string]bool),
	trackingEnabled:      make(map[string]bool),
	trackingMode:         make(map[string]string),
	prefixes:             make(map[string][]string),
	invalidationQueues:   make(map[string]chan []string),
	connections:          make(map[string]redis.Connection),
	redirects:            make(map[string]string),
	noLoop:               make(map[string]bool),
	cachingOptInNext:     make(map[string]bool),
	cachingOptOutNext:    make(map[string]bool),
	trackingClientsCount: 0,
	invalidationMsgsSent: 0,
}

// EnableTracking enables client tracking for a connection
func EnableTracking(conn redis.Connection, mode string, prefixes []string, redirectID string, noLoop bool) string {
	clientCache.mu.Lock()
	defer clientCache.mu.Unlock()

	clientID := strconv.FormatInt(conn.GetClientID(), 10)
	if clientID == "0" {
		clientID = generateClientID()
	}

	clientCache.trackingEnabled[clientID] = true
	clientCache.trackingMode[clientID] = mode
	clientCache.prefixes[clientID] = prefixes
	clientCache.connections[clientID] = conn
	clientCache.redirects[clientID] = redirectID
	clientCache.noLoop[clientID] = noLoop

	if clientCache.trackedKeys[clientID] == nil {
		clientCache.trackedKeys[clientID] = make(map[string]bool)
	}
	if clientCache.invalidationQueues[clientID] == nil {
		clientCache.invalidationQueues[clientID] = make(chan []string, 100)
	}

	// Update stats
	clientCache.trackingClientsCount++

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
	delete(clientCache.redirects, clientID)
	delete(clientCache.noLoop, clientID)
	delete(clientCache.cachingOptInNext, clientID)
	delete(clientCache.cachingOptOutNext, clientID)

	// Update stats
	clientCache.trackingClientsCount--

	// Close and delete queue
	if queue, ok := clientCache.invalidationQueues[clientID]; ok {
		close(queue)
		delete(clientCache.invalidationQueues, clientID)
	}
}

// SetCachingDirective records CLIENT CACHING YES|NO for OPTIN/OPTOUT modes.
func SetCachingDirective(clientID string, yes bool) {
	clientCache.mu.Lock()
	defer clientCache.mu.Unlock()
	if yes {
		clientCache.cachingOptInNext[clientID] = true
		delete(clientCache.cachingOptOutNext, clientID)
	} else {
		clientCache.cachingOptOutNext[clientID] = true
		delete(clientCache.cachingOptInNext, clientID)
	}
}

// TrackKey tracks a key for potential invalidation
func TrackKey(clientID string, key string) {
	clientCache.mu.Lock()
	defer clientCache.mu.Unlock()

	if !clientCache.trackingEnabled[clientID] {
		return
	}

	mode := clientCache.trackingMode[clientID]

	// OPTIN requires CLIENT CACHING YES for the next command.
	if mode == "optin" {
		if !clientCache.cachingOptInNext[clientID] {
			return
		}
	}
	// OPTOUT: CLIENT CACHING NO skips tracking for the next command.
	if mode == "optout" {
		if clientCache.cachingOptOutNext[clientID] {
			return
		}
	}

	// Check prefix match for BCAST mode
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
	InvalidateKeyFrom(key, "")
}

// InvalidateKeyFrom invalidates a key; writerID with NOLOOP skips self-invalidation.
func InvalidateKeyFrom(key string, writerID string) {
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
		noloop := clientCache.noLoop[clientID]
		clientCache.mu.RUnlock()

		if writerID != "" && noloop && clientID == writerID {
			continue
		}

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
			if writerID != "" && clientCache.noLoop[clientID] && clientID == writerID {
				continue
			}
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
	InvalidateKeysOnWriteFrom(keys, "")
}

// InvalidateKeysOnWriteFrom invalidates keys after a write by writerID.
func InvalidateKeysOnWriteFrom(keys []string, writerID string) {
	for _, key := range keys {
		InvalidateKeyFrom(key, writerID)
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
		redirectID := cc.redirects[clientID]
		cc.mu.RUnlock()

		if !ok {
			return
		}

		// Resolve redirect target connection if configured
		targetConn := conn
		if redirectID != "" {
			cc.mu.RLock()
			targetConn, hasConn = cc.connections[redirectID]
			cc.mu.RUnlock()
			if !hasConn {
				if rid, err := strconv.ParseInt(redirectID, 10, 64); err == nil {
					if c := FindClientByID(rid); c != nil {
						targetConn = c
						hasConn = true
					}
				}
			}
		}

		select {
		case keys, ok := <-queue:
			if !ok {
				return
			}
			if hasConn {
				sendInvalidation(targetConn, keys)
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
	_, err := conn.Write(push.ToBytes())
	if err != nil {
		return
	}
	clientCache.mu.Lock()
	clientCache.invalidationMsgsSent++
	clientCache.mu.Unlock()
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

	// Consume one-shot CACHING YES/NO after this command.
	clientCache.mu.Lock()
	delete(clientCache.cachingOptInNext, clientID)
	delete(clientCache.cachingOptOutNext, clientID)
	clientCache.mu.Unlock()
}

// generateClientID generates a unique client ID
func generateClientID() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}

// GetTrackingInfo returns tracking info for a client
func GetTrackingInfo(clientID string) map[string]interface{} {
	clientCache.mu.RLock()
	defer clientCache.mu.RUnlock()

	redirect := clientCache.redirects[clientID]
	if redirect == "" {
		redirect = "0"
	}

	return map[string]interface{}{
		"enabled":  clientCache.trackingEnabled[clientID],
		"mode":     clientCache.trackingMode[clientID],
		"prefixes": clientCache.prefixes[clientID],
		"keys":     len(clientCache.trackedKeys[clientID]),
		"redirect": redirect,
		"noloop":   clientCache.noLoop[clientID],
	}
}

// GetTrackingStats returns global tracking statistics
func GetTrackingStats() map[string]interface{} {
	clientCache.mu.RLock()
	defer clientCache.mu.RUnlock()

	return map[string]interface{}{
		"tracking_clients":       clientCache.trackingClientsCount,
		"total_tracked_keys":     len(clientCache.keyClients),
		"invalidation_msgs_sent": clientCache.invalidationMsgsSent,
	}
}

// GetTrackingClientsCount returns the number of clients with tracking enabled
func GetTrackingClientsCount() int {
	clientCache.mu.RLock()
	defer clientCache.mu.RUnlock()
	return clientCache.trackingClientsCount
}

// GetTotalTrackedKeys returns keys with at least one tracking client.
func GetTotalTrackedKeys() int {
	clientCache.mu.RLock()
	defer clientCache.mu.RUnlock()
	return len(clientCache.keyClients)
}

// GetTotalTrackedItems returns sum of tracked key entries across clients.
func GetTotalTrackedItems() int {
	clientCache.mu.RLock()
	defer clientCache.mu.RUnlock()
	n := 0
	for _, keys := range clientCache.trackedKeys {
		n += len(keys)
	}
	return n
}

// GetTotalTrackedPrefixes returns total configured tracking prefixes across clients.
func GetTotalTrackedPrefixes() int {
	clientCache.mu.RLock()
	defer clientCache.mu.RUnlock()
	n := 0
	for _, prefs := range clientCache.prefixes {
		n += len(prefs)
	}
	return n
}

// Hook into database write operations
func init() {
	// This would be called when keys are modified
}
