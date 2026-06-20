package database

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/linkerlin/godis/interface/redis"
)

type trackingTestConn struct {
	name   string
	writes [][]byte
	mu     sync.Mutex
}

func (c *trackingTestConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	c.writes = append(c.writes, append([]byte(nil), b...))
	c.mu.Unlock()
	return len(b), nil
}
func (c *trackingTestConn) Close() error                   { return nil }
func (c *trackingTestConn) RemoteAddr() string             { return "test-addr" }
func (c *trackingTestConn) SetPassword(string)             {}
func (c *trackingTestConn) GetPassword() string            { return "" }
func (c *trackingTestConn) Subscribe(string)               {}
func (c *trackingTestConn) UnSubscribe(string)             {}
func (c *trackingTestConn) SubsCount() int                 { return 0 }
func (c *trackingTestConn) GetChannels() []string          { return nil }
func (c *trackingTestConn) InMultiState() bool             { return false }
func (c *trackingTestConn) SetMultiState(bool)             {}
func (c *trackingTestConn) GetQueuedCmdLine() [][][]byte   { return nil }
func (c *trackingTestConn) EnqueueCmd([][]byte)            {}
func (c *trackingTestConn) ClearQueuedCmds()               {}
func (c *trackingTestConn) GetWatching() map[string]uint64 { return nil }
func (c *trackingTestConn) AddTxError(error)               {}
func (c *trackingTestConn) GetTxErrors() []error           { return nil }
func (c *trackingTestConn) GetDBIndex() int                { return 0 }
func (c *trackingTestConn) SelectDB(int)                   {}
func (c *trackingTestConn) SetSlave()                      {}
func (c *trackingTestConn) IsSlave() bool                  { return false }
func (c *trackingTestConn) SetMaster()                     {}
func (c *trackingTestConn) IsMaster() bool                 { return false }
func (c *trackingTestConn) Name() string                   { return c.name }
func (c *trackingTestConn) SetACLUser(string)              {}
func (c *trackingTestConn) GetACLUser() string             { return "" }
func (c *trackingTestConn) SetACLAuthenticated(bool)       {}
func (c *trackingTestConn) IsACLAuthenticated() bool       { return false }

func (c *trackingTestConn) lastWrite() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.writes) == 0 {
		return ""
	}
	return string(c.writes[len(c.writes)-1])
}

func resetClientCacheForTest(t *testing.T) {
	t.Helper()
	clientCache.mu.Lock()
	defer clientCache.mu.Unlock()
	for id, q := range clientCache.invalidationQueues {
		close(q)
		delete(clientCache.invalidationQueues, id)
	}
	clientCache.trackedKeys = make(map[string]map[string]bool)
	clientCache.keyClients = make(map[string]map[string]bool)
	clientCache.trackingEnabled = make(map[string]bool)
	clientCache.trackingMode = make(map[string]string)
	clientCache.prefixes = make(map[string][]string)
	clientCache.invalidationQueues = make(map[string]chan []string)
	clientCache.connections = make(map[string]redis.Connection)
	clientCache.trackingClientsCount = 0
	clientCache.invalidationMsgsSent = 0
}

func TestEnableDisableTracking(t *testing.T) {
	resetClientCacheForTest(t)
	conn := &trackingTestConn{name: "client-a"}

	id := EnableTracking(conn, "", nil)
	if id == "" {
		t.Fatal("expected client id")
	}
	if !IsTrackingEnabled(id) {
		t.Fatal("tracking should be enabled")
	}
	if GetTrackingClientsCount() != 1 {
		t.Fatalf("expected 1 tracking client, got %d", GetTrackingClientsCount())
	}

	info := GetTrackingInfo(id)
	if enabled, ok := info["enabled"].(bool); !ok || !enabled {
		t.Fatalf("unexpected tracking info: %#v", info)
	}

	DisableTracking(id)
	if IsTrackingEnabled(id) {
		t.Fatal("tracking should be disabled")
	}
	if GetTrackingClientsCount() != 0 {
		t.Fatalf("expected 0 tracking clients, got %d", GetTrackingClientsCount())
	}
}

func TestTrackKeyAndInvalidate(t *testing.T) {
	resetClientCacheForTest(t)
	conn := &trackingTestConn{name: "client-b"}
	id := EnableTracking(conn, "", nil)

	TrackKey(id, "cache-key")
	TrackKeysOnRead(id, []string{"cache-key", "cache-key"})

	info := GetTrackingInfo(id)
	if keys, ok := info["keys"].(int); !ok || keys != 1 {
		t.Fatalf("expected 1 tracked key, got %#v", info)
	}

	InvalidateKey("cache-key")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(conn.lastWrite(), "invalidate") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected invalidation push, last write: %q", conn.lastWrite())
}

func TestTrackKeyBCASTPrefixFilter(t *testing.T) {
	resetClientCacheForTest(t)
	conn := &trackingTestConn{name: "client-c"}
	id := EnableTracking(conn, "bcast", []string{"user:"})

	TrackKey(id, "other:key")
	if info := GetTrackingInfo(id); info["keys"].(int) != 0 {
		t.Fatalf("non-prefix key should not be tracked: %#v", info)
	}

	TrackKey(id, "user:1001")
	if info := GetTrackingInfo(id); info["keys"].(int) != 1 {
		t.Fatalf("prefix key should be tracked: %#v", info)
	}
}

func TestInvalidateKeysOnWrite(t *testing.T) {
	resetClientCacheForTest(t)
	conn := &trackingTestConn{name: "client-d"}
	id := EnableTracking(conn, "", nil)
	TrackKey(id, "k1")
	TrackKey(id, "k2")

	InvalidateKeysOnWrite([]string{"k1", "k2"})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(conn.lastWrite(), "invalidate") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("expected invalidation push after InvalidateKeysOnWrite")
}

func TestGetTrackingStats(t *testing.T) {
	resetClientCacheForTest(t)
	conn := &trackingTestConn{name: "client-e"}
	EnableTracking(conn, "", nil)
	TrackKey(conn.Name(), "stat-key")

	stats := GetTrackingStats()
	if stats["tracking_clients"].(int) != 1 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
	if stats["total_tracked_keys"].(int) != 1 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
}
