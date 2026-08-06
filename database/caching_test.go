package database

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

type trackingTestConn struct {
	name       string
	clientID   int64
	clientName string
	trackingID string
	protocol   int
	writes     [][]byte
	mu         sync.Mutex
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
func (c *trackingTestConn) PSubscribe(string)              {}
func (c *trackingTestConn) PUnSubscribe(string)            {}
func (c *trackingTestConn) PSubsCount() int                { return 0 }
func (c *trackingTestConn) GetPatterns() []string          { return nil }
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
func (c *trackingTestConn) Name() string {
	if c.clientName != "" {
		return c.clientName
	}
	return c.name
}
func (c *trackingTestConn) GetClientID() int64 {
	if c.clientID == 0 {
		c.clientID = 1
	}
	return c.clientID
}
func (c *trackingTestConn) SetClientName(name string)        { c.clientName = name }
func (c *trackingTestConn) GetClientName() string            { return c.clientName }
func (c *trackingTestConn) SetTrackingID(id string)          { c.trackingID = id }
func (c *trackingTestConn) GetTrackingID() string            { return c.trackingID }
func (c *trackingTestConn) SetACLUser(string)              {}
func (c *trackingTestConn) GetACLUser() string             { return "" }
func (c *trackingTestConn) SetACLAuthenticated(bool)       {}
func (c *trackingTestConn) IsACLAuthenticated() bool       { return false }
func (c *trackingTestConn) SetProtocolVersion(v int)  { c.protocol = v }
func (c *trackingTestConn) GetProtocolVersion() int {
	if c.protocol == 0 {
		return 3
	}
	return c.protocol
}

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

	id := EnableTracking(conn, "", nil, "", false)
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
	id := EnableTracking(conn, "", nil, "", false)

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
	id := EnableTracking(conn, "bcast", []string{"user:"}, "", false)

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
	id := EnableTracking(conn, "", nil, "", false)
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

func TestCachingInvalidateKeyNoClients(t *testing.T) {
	resetClientCacheForTest(t)
	InvalidateKey("missing-key") // should not panic
}

func TestCachingTrackKeyWhenDisabled(t *testing.T) {
	resetClientCacheForTest(t)
	TrackKey("no-such-client", "k")
	if GetTotalTrackedKeys() != 0 {
		t.Fatalf("expected 0 tracked keys, got %d", GetTotalTrackedKeys())
	}
}

func TestCachingBcastKeepsTrackingAfterInvalidate(t *testing.T) {
	resetClientCacheForTest(t)
	conn := &trackingTestConn{name: "bcast-client"}
	id := EnableTracking(conn, "bcast", []string{"app:"}, "", false)
	TrackKey(id, "app:item")

	InvalidateKey("app:item")

	if info := GetTrackingInfo(id); info["keys"].(int) != 1 {
		t.Fatalf("BCAST should keep tracked key after invalidate: %#v", info)
	}
	if GetTotalTrackedKeys() != 1 {
		t.Fatalf("expected key still tracked globally")
	}
}

func TestCachingGetTotalTrackedKeys(t *testing.T) {
	resetClientCacheForTest(t)
	conn := &trackingTestConn{clientID: 7}
	id := EnableTracking(conn, "", nil, "", false)
	TrackKey(id, "k1")
	TrackKey(id, "k2")

	if GetTotalTrackedKeys() != 2 {
		t.Fatalf("expected 2 tracked keys, got %d", GetTotalTrackedKeys())
	}
}

func TestClientTrackingInfoCommand(t *testing.T) {
	resetClientCacheForTest(t)
	server := getTestServer()
	conn := &trackingTestConn{clientID: 11}

	asserts.AssertStatusReply(t, server.Exec(conn, utils.ToCmdLine("CLIENT", "TRACKING", "ON")), "OK")
	reply := server.Exec(conn, utils.ToCmdLine("CLIENT", "TRACKINGINFO"))
	multi, ok := reply.(*protocol.MultiRawReply)
	if !ok {
		t.Fatalf("expected TRACKINGINFO multi raw reply, got %T", reply)
	}
	foundFlags := false
	for i := 0; i+1 < len(multi.Replies); i += 2 {
		if b, ok := multi.Replies[i].(*protocol.BulkReply); ok && string(b.Arg) == "flags" {
			// The flags value must be a nested array (not a bulk string of
			// serialized array bytes — the historical wire corruption).
			if _, ok := multi.Replies[i+1].(*protocol.MultiBulkReply); !ok {
				t.Fatalf("TRACKINGINFO flags value should be a nested array, got %T", multi.Replies[i+1])
			}
			foundFlags = true
			break
		}
	}
	if !foundFlags {
		t.Fatalf("TRACKINGINFO missing flags field: %s", reply.ToBytes())
	}
}

func TestGetTrackingStats(t *testing.T) {
	resetClientCacheForTest(t)
	conn := &trackingTestConn{name: "client-e", clientID: 5}
	id := EnableTracking(conn, "", nil, "", false)
	conn.SetTrackingID(id)
	TrackKey(id, "stat-key")

	stats := GetTrackingStats()
	if stats["tracking_clients"].(int) != 1 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
	if stats["total_tracked_keys"].(int) != 1 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
}
