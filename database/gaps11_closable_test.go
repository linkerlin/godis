package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps11ExpireOptionConflicts(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SET", "k", "v"))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "k", "10", "NX", "XX")),
		"ERR NX and XX, GT or LT options at the same time are not compatible")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "k", "10", "GT", "LT")),
		"ERR GT and LT options at the same time are not compatible")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "k", "10", "NX", "GT")),
		"ERR NX and XX, GT or LT options at the same time are not compatible")
	// XX+GT is allowed (Redis 8); without TTL XX fails → 0.
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "k", "100", "XX", "GT")), 0)
	db.Exec(nil, utils.ToCmdLine("PEXPIRE", "k", "50000"))
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "k", "200", "XX", "GT")), 1)
}

func TestGaps11XPendingNoGroupAndSummary(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XPENDING", "miss", "g")),
		"NOGROUP No such key 'miss' or consumer group 'g'")
	db.Exec(nil, utils.ToCmdLine("XADD", "xs", "1-0", "f", "v"))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XPENDING", "xs", "g")),
		"NOGROUP No such key 'xs' or consumer group 'g'")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "xs", "g", "0")), "OK")
	r := db.Exec(nil, utils.ToCmdLine("XPENDING", "xs", "g"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 4 {
		t.Fatalf("empty PEL summary: %T %s", r, r.ToBytes())
	}
	asserts.AssertIntReply(t, mr.Replies[0], 0)
	asserts.AssertNullBulk(t, mr.Replies[1])
	asserts.AssertNullBulk(t, mr.Replies[2])
	asserts.AssertNullBulk(t, mr.Replies[3])
}
