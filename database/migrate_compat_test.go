package database

import (
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/parser"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
	"github.com/linkerlin/godis/tcp"
)

// startMigrateTargetServer starts an in-process Godis TCP listener for MIGRATE tests.
func startMigrateTargetServer(t *testing.T) (addr string, cleanup func()) {
	t.Helper()
	old := config.Properties
	config.Properties = &config.ServerProperties{
		Databases:  16,
		AppendOnly: false,
		Bind:       "127.0.0.1",
	}
	server := MustNewStandaloneServer()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		config.Properties = old
		t.Fatalf("listen: %v", err)
	}
	closeChan := make(chan struct{})
	h := &migrateTestHandler{db: server}
	go tcp.ListenAndServe(ln, h, closeChan)
	addr = ln.Addr().String()
	cleanup = func() {
		close(closeChan)
		_ = ln.Close()
		server.Close()
		config.Properties = old
	}
	// Wait until accept loop is ready.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return addr, cleanup
		}
		time.Sleep(10 * time.Millisecond)
	}
	cleanup()
	t.Fatal("target server did not become ready")
	return "", nil
}

type migrateTestHandler struct {
	db *Server
}

func (h *migrateTestHandler) Handle(_ context.Context, conn net.Conn) {
	client := connection.NewConn(conn)
	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err != io.EOF &&
				!strings.Contains(payload.Err.Error(), "use of closed network connection") {
				_ = conn.Close()
			}
			return
		}
		if payload.Data == nil {
			continue
		}
		multi, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			_, _ = conn.Write(protocol.MakeErrReply("ERR expected multi bulk").ToBytes())
			continue
		}
		reply := h.db.Exec(client, multi.Args)
		if reply == nil {
			reply = protocol.MakeErrReply("ERR nil reply")
		}
		_, _ = conn.Write(reply.ToBytes())
	}
	h.db.AfterClientClose(client)
}

func (h *migrateTestHandler) Close() error { return nil }

func hostPort(addr string) (string, string) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "127.0.0.1", "0"
	}
	return host, port
}

func TestMigrateParseErrors(t *testing.T) {
	db := makeTestDB()

	r := db.Exec(nil, utils.ToCmdLine("MIGRATE", "127.0.0.1"))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("expected arity error, got %s", r.ToBytes())
	}

	r = db.Exec(nil, utils.ToCmdLine("MIGRATE", "127.0.0.1", "bad", "k", "0", "1000"))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("expected port error, got %s", r.ToBytes())
	}

	r = db.Exec(nil, utils.ToCmdLine("MIGRATE", "127.0.0.1", "6399", "", "0", "1000"))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("expected empty-key without KEYS error, got %s", r.ToBytes())
	}

	r = db.Exec(nil, utils.ToCmdLine("MIGRATE", "127.0.0.1", "6399", "k", "0", "1000", "KEYS", "a"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "empty string") {
		t.Fatalf("expected KEYS+nonempty key error, got %s", r.ToBytes())
	}
}

func TestMigrateIOERRUnreachable(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "k", "v")), "OK")

	// Port unlikely to accept connections; short timeout.
	r := db.Exec(nil, utils.ToCmdLine("MIGRATE", "127.0.0.1", "1", "k", "0", "200"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "IOERR") {
		t.Fatalf("expected IOERR, got %s", r.ToBytes())
	}
	// Key must remain on source after IOERR.
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "k")), "v")
}

func TestMigrateNOKEY(t *testing.T) {
	db := makeTestDB()
	addr, cleanup := startMigrateTargetServer(t)
	defer cleanup()
	host, port := hostPort(addr)

	r := db.Exec(nil, utils.ToCmdLine("MIGRATE", host, port, "missing", "0", "2000"))
	asserts.AssertStatusReply(t, r, "NOKEY")
}

func TestMigrateSingleKeySuccess(t *testing.T) {
	src := makeTestDB()
	asserts.AssertStatusReply(t, src.Exec(nil, utils.ToCmdLine("SET", "mk", "hello")), "OK")

	addr, cleanup := startMigrateTargetServer(t)
	defer cleanup()
	host, port := hostPort(addr)

	r := src.Exec(nil, utils.ToCmdLine("MIGRATE", host, port, "mk", "0", "3000"))
	asserts.AssertStatusReply(t, r, "OK")

	// Source deleted.
	asserts.AssertNullBulk(t, src.Exec(nil, utils.ToCmdLine("GET", "mk")))

	// Destination has value — probe via fresh connection to target.
	got := migrateProbeGET(t, addr, 0, "mk")
	if got != "hello" {
		t.Fatalf("dest GET = %q, want hello", got)
	}
}

func TestMigrateCopyKeepsSource(t *testing.T) {
	src := makeTestDB()
	asserts.AssertStatusReply(t, src.Exec(nil, utils.ToCmdLine("SET", "ck", "copyme")), "OK")

	addr, cleanup := startMigrateTargetServer(t)
	defer cleanup()
	host, port := hostPort(addr)

	r := src.Exec(nil, utils.ToCmdLine("MIGRATE", host, port, "ck", "0", "3000", "COPY"))
	asserts.AssertStatusReply(t, r, "OK")
	asserts.AssertBulkReply(t, src.Exec(nil, utils.ToCmdLine("GET", "ck")), "copyme")
	if migrateProbeGET(t, addr, 0, "ck") != "copyme" {
		t.Fatal("dest missing copied key")
	}
}

func TestMigrateReplaceAndBusyKey(t *testing.T) {
	src := makeTestDB()
	asserts.AssertStatusReply(t, src.Exec(nil, utils.ToCmdLine("SET", "bk", "new")), "OK")

	addr, cleanup := startMigrateTargetServer(t)
	defer cleanup()
	host, port := hostPort(addr)

	// Seed dest with same key.
	seedConn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = seedConn.Write(protocol.MakeMultiBulkReply(utils.ToCmdLine("SET", "bk", "old")).ToBytes())
	_ = readOneReply(t, seedConn)
	_ = seedConn.Close()

	r := src.Exec(nil, utils.ToCmdLine("MIGRATE", host, port, "bk", "0", "3000", "COPY"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "BUSYKEY") {
		t.Fatalf("expected BUSYKEY wrap, got %s", r.ToBytes())
	}
	asserts.AssertBulkReply(t, src.Exec(nil, utils.ToCmdLine("GET", "bk")), "new")

	r = src.Exec(nil, utils.ToCmdLine("MIGRATE", host, port, "bk", "0", "3000", "REPLACE"))
	asserts.AssertStatusReply(t, r, "OK")
	asserts.AssertNullBulk(t, src.Exec(nil, utils.ToCmdLine("GET", "bk")))
	if migrateProbeGET(t, addr, 0, "bk") != "new" {
		t.Fatal("dest not replaced")
	}
}

func TestMigrateKEYSMultiAndDestDB(t *testing.T) {
	src := makeTestDB()
	asserts.AssertStatusReply(t, src.Exec(nil, utils.ToCmdLine("SET", "a", "1")), "OK")
	asserts.AssertStatusReply(t, src.Exec(nil, utils.ToCmdLine("SET", "b", "2")), "OK")

	addr, cleanup := startMigrateTargetServer(t)
	defer cleanup()
	host, port := hostPort(addr)

	r := src.Exec(nil, utils.ToCmdLine(
		"MIGRATE", host, port, "", "1", "3000", "KEYS", "a", "b",
	))
	asserts.AssertStatusReply(t, r, "OK")
	asserts.AssertNullBulk(t, src.Exec(nil, utils.ToCmdLine("GET", "a")))
	asserts.AssertNullBulk(t, src.Exec(nil, utils.ToCmdLine("GET", "b")))

	if migrateProbeGET(t, addr, 1, "a") != "1" {
		t.Fatal("dest db1 missing a")
	}
	if migrateProbeGET(t, addr, 1, "b") != "2" {
		t.Fatal("dest db1 missing b")
	}
}

func TestMigrateWithTTL(t *testing.T) {
	src := makeTestDB()
	asserts.AssertStatusReply(t, src.Exec(nil, utils.ToCmdLine("SET", "tk", "ttl", "PX", "60000")), "OK")

	addr, cleanup := startMigrateTargetServer(t)
	defer cleanup()
	host, port := hostPort(addr)

	asserts.AssertStatusReply(t,
		src.Exec(nil, utils.ToCmdLine("MIGRATE", host, port, "tk", "0", "3000")), "OK")

	pttl := migrateProbePTTL(t, addr, 0, "tk")
	if pttl <= 0 || pttl > 60000 {
		t.Fatalf("dest PTTL=%d, want (0,60000]", pttl)
	}
}

func migrateProbeGET(t *testing.T, addr string, db int, key string) string {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	ch := parser.ParseStream(conn)
	if db != 0 {
		_, _ = conn.Write(protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(db))).ToBytes())
		_ = readPayload(t, ch)
	}
	_, _ = conn.Write(protocol.MakeMultiBulkReply(utils.ToCmdLine("GET", key)).ToBytes())
	reply := readPayload(t, ch)
	bulk, ok := reply.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("GET reply: %T %s", reply, reply.ToBytes())
	}
	return string(bulk.Arg)
}

func migrateProbePTTL(t *testing.T, addr string, db int, key string) int64 {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	ch := parser.ParseStream(conn)
	if db != 0 {
		_, _ = conn.Write(protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(db))).ToBytes())
		_ = readPayload(t, ch)
	}
	_, _ = conn.Write(protocol.MakeMultiBulkReply(utils.ToCmdLine("PTTL", key)).ToBytes())
	reply := readPayload(t, ch)
	ir, ok := reply.(*protocol.IntReply)
	if !ok {
		t.Fatalf("PTTL reply: %T %s", reply, reply.ToBytes())
	}
	return ir.Code
}

func readOneReply(t *testing.T, conn net.Conn) redis.Reply {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	ch := parser.ParseStream(conn)
	return readPayload(t, ch)
}

func readPayload(t *testing.T, ch <-chan *parser.Payload) redis.Reply {
	t.Helper()
	payload := <-ch
	if payload == nil || payload.Err != nil {
		err := error(nil)
		if payload != nil {
			err = payload.Err
		}
		t.Fatalf("read reply: %v", err)
	}
	return payload.Data
}
