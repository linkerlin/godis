package database

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2bjFTSearchDefaultLimit(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "m2bjlim", "ON", "HASH", "PREFIX", "1", "k:",
		"SCHEMA", "t", "TEXT",
	)), "OK")
	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("k:%d", i)
		_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "m2bjlim", key, "FIELDS", "t", "hello"))
	}

	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "m2bjlim", "hello"))
	raw := ftSearchMultiRaw(r)
	if raw == nil {
		t.Fatalf("FT.SEARCH: %T %s", r, r.ToBytes())
	}
	// [total, id, fields] × 10 docs → 1+20
	if len(raw.Replies) != 21 {
		t.Fatalf("default LIMIT 0 10: want 21 elems, got %d", len(raw.Replies))
	}
	asserts.AssertIntReply(t, raw.Replies[0], 15)

	r5 := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "m2bjlim", "hello", "LIMIT", "0", "5"))
	raw5 := ftSearchMultiRaw(r5)
	if raw5 == nil {
		t.Fatalf("LIMIT 5: %T", r5)
	}
	if len(raw5.Replies) != 11 {
		t.Fatalf("LIMIT 0 5: want 11 elems, got %d", len(raw5.Replies))
	}
}

func TestM2bjClientListFlagsTOAndRedir(t *testing.T) {
	c := connection.NewFakeConn()
	c.SetNoTouch(true)
	line := formatClientListLine(c)
	if !strings.Contains(line, "T") {
		t.Fatalf("want flag T: %q", line)
	}
	if !strings.Contains(line, "redir=-1") {
		t.Fatalf("tracking off want redir=-1: %q", line)
	}

	AddMonitorClient(c)
	defer RemoveMonitorClient(c)
	line = formatClientListLine(c)
	if !strings.Contains(line, "O") {
		t.Fatalf("want flag O: %q", line)
	}

	id := EnableTracking(c, "ON", nil, "", false)
	c.SetTrackingID(id)
	defer DisableTracking(id)
	line = formatClientListLine(c)
	if !strings.Contains(line, "redir=0") {
		t.Fatalf("tracking on no redirect want redir=0: %q", line)
	}

	tgt := connection.NewFakeConn()
	RegisterClient(tgt)
	defer UnregisterClient(tgt)
	redirID := tgt.GetClientID()
	DisableTracking(id)
	c.SetTrackingID("")
	id = EnableTracking(c, "ON", nil, strconv.FormatInt(redirID, 10), false)
	c.SetTrackingID(id)
	defer DisableTracking(id)
	line = formatClientListLine(c)
	want := "redir=" + strconv.FormatInt(redirID, 10)
	if !strings.Contains(line, want) {
		t.Fatalf("want %s in %q", want, line)
	}
}

func TestM2bjInfoReplicationSlaveFields(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	oldRO := config.Properties.ReplicaReadOnly
	config.Properties.ReplicaReadOnly = true
	defer func() { config.Properties.ReplicaReadOnly = oldRO }()

	atomic.StoreInt32(&server.role, slaveRole)
	server.slaveStatus.mutex.Lock()
	server.slaveStatus.masterHost = "127.0.0.1"
	server.slaveStatus.masterPort = 6379
	server.slaveStatus.replOffset = 42
	server.slaveStatus.mutex.Unlock()

	s := genReplicationInfo(server)
	for _, want := range []string{
		"role:slave",
		"master_host:127.0.0.1",
		"master_port:6379",
		"master_link_status:down",
		"slave_read_only:1",
		"slave_repl_offset:42",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("missing %q in:\n%s", want, s)
		}
	}
}

func TestM2bjConfigAofPreambleAndMasterAuth(t *testing.T) {
	oldProps := config.Properties
	config.Properties = &config.ServerProperties{
		AofUseRdbPreamble: false,
		MasterAuth:        "",
		Databases:         16,
	}
	defer func() { config.Properties = oldProps }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "aof-use-rdb-preamble", "yes")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "masterauth", "secret")), "OK")
	if !config.Properties.AofUseRdbPreamble || config.Properties.MasterAuth != "secret" {
		t.Fatalf("props not updated: preamble=%v auth=%q", config.Properties.AofUseRdbPreamble, config.Properties.MasterAuth)
	}

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "aof-use-rdb-preamble")),
		[]string{"aof-use-rdb-preamble", "yes"})
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "masterauth")),
		[]string{"masterauth", "secret"})

	bad := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "aof-use-rdb-preamble", "maybe"))
	if _, ok := bad.(*protocol.StandardErrReply); !ok {
		t.Fatalf("want error for invalid bool, got %T %s", bad, bad.ToBytes())
	}
}
