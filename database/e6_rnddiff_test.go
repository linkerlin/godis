package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// E6-2 fixes: GETRANGE clamps negative bounds (string family), LRANGE keeps a
// beyond-size negative stop as empty (list family). Pinned to live Redis 8.10.
func TestE6RangeSemantics(t *testing.T) {
	db := getTestDB()

	set := utils.ToCmdLine("SET", "e6s", "49750")
	if r := db.Exec(nil, set); !isOKReply(r) {
		t.Fatalf("set: %s", r.ToBytes())
	}
	getrange := func(args ...string) string {
		r := db.Exec(nil, utils.ToCmdLine(append([]string{"GETRANGE", "e6s"}, args...)...))
		b, ok := r.(*protocol.BulkReply)
		if !ok {
			t.Fatalf("getrange %v => %s", args, r.ToBytes())
		}
		return string(b.Arg)
	}
	if got := getrange("-8", "43"); got != "49750" {
		t.Errorf("GETRANGE -8 43 = %q, want whole", got)
	}
	if got := getrange("-8", "-7"); got != "4" {
		t.Errorf("GETRANGE -8 -7 = %q, want first char", got)
	}
	if got := getrange("3", "-7"); got != "" {
		t.Errorf("GETRANGE 3 -7 = %q, want empty", got)
	}

	for _, v := range []string{"a", "b", "c"} {
		db.Exec(nil, utils.ToCmdLine("RPUSH", "e6l", v))
	}
	lrange := func(args ...string) int {
		r := db.Exec(nil, utils.ToCmdLine(append([]string{"LRANGE", "e6l"}, args...)...))
		switch m := r.(type) {
		case *protocol.MultiBulkReply:
			return len(m.Args)
		case *protocol.EmptyMultiBulkReply:
			return 0
		}
		t.Fatalf("lrange %v => %s", args, r.ToBytes())
		return -1
	}
	if n := lrange("-100", "-99"); n != 0 {
		t.Errorf("LRANGE -100 -99 = %d elems, want 0", n)
	}
	if n := lrange("-100", "100"); n != 3 {
		t.Errorf("LRANGE -100 100 = %d elems, want 3", n)
	}
}

func isOKReply(r interface{ ToBytes() []byte }) bool {
	return string(r.ToBytes()) == "+OK\r\n"
}
