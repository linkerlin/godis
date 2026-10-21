package database

import (
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/pubsub"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
)

// subscribeSubkey subscribes a fake connection and consumes the subscribe
// confirmation so callers can compare buffer lengths afterwards.
func subscribeSubkey(t *testing.T, server *Server, channel string) *connection.FakeConn {
	t.Helper()
	sub := connection.NewFakeConn()
	if r := pubsub.Subscribe(server.hub, sub, [][]byte{[]byte(channel)}); protocol.IsErrorReply(r) {
		t.Fatalf("subscribe %q: %s", channel, r.ToBytes())
	}
	_ = sub.Bytes() // confirmation is already buffered; callers use Contains
	return sub
}

func setNotifyFlags(t *testing.T, flags string) {
	t.Helper()
	old := config.Properties.NotifyKeyspaceEvents
	config.Properties.NotifyKeyspaceEvents = flags
	t.Cleanup(func() { config.Properties.NotifyKeyspaceEvents = old })
}

// TestR5SubkeyHashSetDel verifies all four Redis 8.8 subkey channel formats,
// length-prefixed binary-safe fields, and hdel field filtering.
func TestR5SubkeyHashSetDel(t *testing.T) {
	setNotifyFlags(t, "STIV")
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	subS := subscribeSubkey(t, server, "__subkeyspace@0__:sk")
	subT := subscribeSubkey(t, server, "__subkeyevent@0__:hset")
	subTDel := subscribeSubkey(t, server, "__subkeyevent@0__:hdel")
	subI := subscribeSubkey(t, server, "__subkeyspaceitem@0__:sk\nf1")
	subV := subscribeSubkey(t, server, "__subkeyspaceevent@0__:hset|sk")
	subVDel := subscribeSubkey(t, server, "__subkeyspaceevent@0__:hdel|sk")

	if r := server.Exec(c, utils.ToCmdLine("HSET", "sk", "f1", "v1", "f2", "v2")); protocol.IsErrorReply(r) {
		t.Fatalf("HSET: %s", r.ToBytes())
	}
	// A comma inside a field name must stay inside its length prefix.
	if r := server.Exec(c, utils.ToCmdLine("HSET", "sk", "a,b", "v3")); protocol.IsErrorReply(r) {
		t.Fatalf("HSET binary field: %s", r.ToBytes())
	}

	if body := string(subS.Bytes()); !strings.Contains(body, "hset|2:f1,2:f2") || !strings.Contains(body, "hset|3:a,b") {
		t.Fatalf("subkeyspace payload: %q", body)
	}
	if body := string(subT.Bytes()); !strings.Contains(body, "2:sk|2:f1,2:f2") || !strings.Contains(body, "2:sk|3:a,b") {
		t.Fatalf("subkeyevent payload: %q", body)
	}
	if body := string(subI.Bytes()); !strings.Contains(body, "hset") {
		t.Fatalf("subkeyspaceitem payload: %q", body)
	}
	if body := string(subV.Bytes()); !strings.Contains(body, "2:f1,2:f2") || !strings.Contains(body, "3:a,b") {
		t.Fatalf("subkeyspaceevent payload: %q", body)
	}

	// HDEL reports only fields that actually existed.
	if r := server.Exec(c, utils.ToCmdLine("HDEL", "sk", "f1", "missing")); protocol.IsErrorReply(r) {
		t.Fatalf("HDEL: %s", r.ToBytes())
	}
	if body := string(subS.Bytes()); !strings.Contains(body, "hdel|2:f1") {
		t.Fatalf("hdel subkeyspace payload: %q", body)
	}
	if body := string(subTDel.Bytes()); !strings.Contains(body, "2:sk|2:f1") {
		t.Fatalf("hdel subkeyevent payload: %q", body)
	}
	if body := string(subVDel.Bytes()); !strings.Contains(body, "2:f1") {
		t.Fatalf("hdel subkeyspaceevent payload: %q", body)
	}
}

// TestR5SubkeyHashExpirePersist verifies HEXPIRE/HPERSIST/HGETDEL/HSETEX
// subkey events, and that lazy/active expiration emits hexpired.
func TestR5SubkeyHashExpirePersist(t *testing.T) {
	setNotifyFlags(t, "ST")
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	subS := subscribeSubkey(t, server, "__subkeyspace@0__:ex")
	if r := server.Exec(c, utils.ToCmdLine("HSET", "ex", "f", "v")); protocol.IsErrorReply(r) {
		t.Fatalf("HSET: %s", r.ToBytes())
	}
	if r := server.Exec(c, utils.ToCmdLine("HEXPIRE", "ex", "100", "FIELDS", "1", "f")); protocol.IsErrorReply(r) {
		t.Fatalf("HEXPIRE: %s", r.ToBytes())
	}
	if r := server.Exec(c, utils.ToCmdLine("HPERSIST", "ex", "FIELDS", "1", "f")); protocol.IsErrorReply(r) {
		t.Fatalf("HPERSIST: %s", r.ToBytes())
	}
	if r := server.Exec(c, utils.ToCmdLine("HSETEX", "ex", "EX", "100", "FIELDS", "1", "g", "v2")); protocol.IsErrorReply(r) {
		t.Fatalf("HSETEX: %s", r.ToBytes())
	}
	if r := server.Exec(c, utils.ToCmdLine("HGETDEL", "ex", "FIELDS", "1", "g")); protocol.IsErrorReply(r) {
		t.Fatalf("HGETDEL: %s", r.ToBytes())
	}

	body := string(subS.Bytes())
	for _, want := range []string{
		"hset|1:f", "hexpire|1:f", "hpersist|1:f",
		"hset|1:g", "hexpire|1:g", "hdel|1:g",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in subkey notifications: %q", want, body)
		}
	}

	// Active/lazy field expiration → hexpired with the field name.
	if r := server.Exec(c, utils.ToCmdLine("HSET", "xp", "f", "v")); protocol.IsErrorReply(r) {
		t.Fatalf("HSET xp: %s", r.ToBytes())
	}
	subXp := subscribeSubkey(t, server, "__subkeyspace@0__:xp")
	if r := server.Exec(c, utils.ToCmdLine("HPEXPIRE", "xp", "10", "FIELDS", "1", "f")); protocol.IsErrorReply(r) {
		t.Fatalf("HPEXPIRE: %s", r.ToBytes())
	}
	// The time wheel may be disabled or have a coarse tick in a full-suite run;
	// a read triggers lazy field expiry deterministically.
	time.Sleep(30 * time.Millisecond)
	if r := server.Exec(c, utils.ToCmdLine("HGET", "xp", "f")); protocol.IsErrorReply(r) {
		t.Fatalf("HGET xp: %s", r.ToBytes())
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(string(subXp.Bytes()), "hexpired|1:f") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if strings.Contains(string(subXp.Bytes()), "hexpired|1:f") {
		return
	}
	t.Fatalf("missing hexpired subkey notification: %q", subXp.Bytes())
}

// TestR5SubkeyFlagsIndependent verifies A/K/E do not enable subkey channels,
// while S alone does, and events are independent from key-level flags.
func TestR5SubkeyFlagsIndependent(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	setNotifyFlags(t, "A")
	subA := subscribeSubkey(t, server, "__subkeyspace@0__:ind")
	before := len(subA.Bytes())
	if r := server.Exec(c, utils.ToCmdLine("HSET", "ind", "f", "v")); protocol.IsErrorReply(r) {
		t.Fatalf("HSET: %s", r.ToBytes())
	}
	if got := len(subA.Bytes()); got != before {
		t.Fatalf("A must not enable subkey notifications, got %q", subA.Bytes()[before:])
	}

	setNotifyFlags(t, "S")
	subS := subscribeSubkey(t, server, "__subkeyspace@0__:ind2")
	if r := server.Exec(c, utils.ToCmdLine("HSET", "ind2", "f", "v")); protocol.IsErrorReply(r) {
		t.Fatalf("HSET: %s", r.ToBytes())
	}
	if body := string(subS.Bytes()); !strings.Contains(body, "hset|1:f") {
		t.Fatalf("S flag subkey payload: %q", body)
	}
	// S alone must not emit the key-level __keyspace event.
	if strings.Contains(string(subS.Bytes()), "__keyspace@") {
		t.Fatalf("subkey-only flag leaked a key-level notification: %q", subS.Bytes())
	}

	// A newline in the key skips subkeyspaceitem (channel would be ambiguous)
	// but keeps the other three channel formats.
	setNotifyFlags(t, "STIV")
	subI := subscribeSubkey(t, server, "__subkeyspaceitem@0__:nl\nkey\nf")
	beforeI := len(subI.Bytes())
	subS2 := subscribeSubkey(t, server, "__subkeyspace@0__:nl\nkey")
	if r := server.Exec(c, utils.ToCmdLine("HSET", "nl\nkey", "f", "v")); protocol.IsErrorReply(r) {
		t.Fatalf("HSET newline key: %s", r.ToBytes())
	}
	if got := len(subI.Bytes()); got != beforeI {
		t.Fatalf("newline key must skip subkeyspaceitem, got %q", subI.Bytes()[beforeI:])
	}
	if body := string(subS2.Bytes()); !strings.Contains(body, "hset|1:f") {
		t.Fatalf("newline key subkeyspace payload: %q", body)
	}
}
