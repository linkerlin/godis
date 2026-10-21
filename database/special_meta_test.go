package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

func TestSpecialCommandMetadata(t *testing.T) {
	db := makeTestDB()
	names := []string{
		"bgrewriteaof", "copy", "dbsize", "function", "himport", "move",
		"psubscribe", "psync", "punsubscribe", "script", "slowlog", "swapdb",
		"sync", "unsubscribe", "wait", "waitaof",
	}
	list, ok := db.Exec(nil, utils.ToCmdLine("COMMAND", "LIST")).(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("COMMAND LIST: %T", list)
	}
	listed := make(map[string]bool, len(list.Args))
	for _, arg := range list.Args {
		listed[string(arg)] = true
	}
	for _, name := range names {
		if !listed[name] {
			t.Fatalf("COMMAND LIST missing %q", name)
		}
		info, ok := db.Exec(nil, utils.ToCmdLine("COMMAND", "INFO", name)).(*protocol.MultiRawReply)
		if !ok || len(info.Replies) != 1 {
			t.Fatalf("COMMAND INFO %s: %T", name, info)
		}
		if _, unknown := info.Replies[0].(*protocol.NullBulkReply); unknown {
			t.Fatalf("COMMAND INFO %s returned null", name)
		}
	}

	if !isReadOnlyCommand("dbsize") || !isReadOnlyCommand("psubscribe") || !isReadOnlyCommand("wait") {
		t.Fatal("read-only special commands must be recognized for replica checks")
	}
	if isReadOnlyCommand("copy") || isReadOnlyCommand("swapdb") {
		t.Fatal("write special commands must not be marked read-only")
	}

	keys, ok := db.Exec(nil, utils.ToCmdLine("COMMAND", "GETKEYS", "COPY", "src", "dst")).(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("COMMAND GETKEYS COPY: %T", keys)
	}
	if len(keys.Args) != 2 || string(keys.Args[0]) != "src" || string(keys.Args[1]) != "dst" {
		t.Fatalf("COMMAND GETKEYS COPY order: %s", keys.ToBytes())
	}
}
