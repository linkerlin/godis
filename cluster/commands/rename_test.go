package commands

import (
	"testing"

	"github.com/linkerlin/godis/cluster/core"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestRename(t *testing.T) {
	id1 := "1"
	id2 := "2"
	nodes := core.MakeTestCluster([]string{id1, id2})
	node1 := nodes[id1]
	c := connection.NewFakeConn()
	core.RegisterDefaultCmd("set")
	core.RegisterDefaultCmd("get")
	node1.Exec(c, utils.ToCmdLine("set", "1", "1"))

	// 1, 2 will be routed to node1 and node2, see MakeTestCluster
	res := execRename(node1, c, utils.ToCmdLine("rename", "1", "2"))
	asserts.AssertStatusReply(t, res, "OK")

	res = node1.Exec(c, utils.ToCmdLine("get", "2"))
	asserts.AssertBulkReply(t, res, "1")
}

func TestRenameNx(t *testing.T) {
	id1 := "1"
	id2 := "2"
	nodes := core.MakeTestCluster([]string{id1, id2})
	node1 := nodes[id1]
	c := connection.NewFakeConn()
	core.RegisterDefaultCmd("set")
	core.RegisterDefaultCmd("get")
	node1.Exec(c, utils.ToCmdLine("set", "1", "1"))

	// 1, 2 will be routed to node1 and node2, see MakeTestCluster
	res := execRenameNx(node1, c, utils.ToCmdLine("rename", "1", "2"))
	asserts.AssertIntReply(t, res, 1)

	res = node1.Exec(c, utils.ToCmdLine("get", "2"))
	asserts.AssertBulkReply(t, res, "1")

	node1.Exec(c, utils.ToCmdLine("set", "3", "3"))
	res = execRenameNx(node1, c, utils.ToCmdLine("rename", "2", "3"))
	asserts.AssertIntReply(t, res, 0)
}

func TestClusterSameNodeRenameAndUnifiedRouting(t *testing.T) {
	id1 := "1"
	id2 := "2"
	nodes := core.MakeTestCluster([]string{id1, id2})
	node1 := nodes[id1]
	c := connection.NewFakeConn()

	// Same-slot keys using hash tag {same} must not trigger infinite recursion on RENAME / RENAMENX
	node1.Exec(c, utils.ToCmdLine("SET", "{same}a", "val_a"))
	res := node1.Exec(c, utils.ToCmdLine("RENAME", "{same}a", "{same}b"))
	asserts.AssertStatusReply(t, res, "OK")
	res = node1.Exec(c, utils.ToCmdLine("GET", "{same}b"))
	asserts.AssertBulkReply(t, res, "val_a")

	res = node1.Exec(c, utils.ToCmdLine("RENAMENX", "{same}b", "{same}c"))
	asserts.AssertIntReply(t, res, 1)
	res = node1.Exec(c, utils.ToCmdLine("GET", "{same}c"))
	asserts.AssertBulkReply(t, res, "val_a")

	// MSETNX via cluster.Exec (verifies MSETNX registration returns IntReply 1/0, not OK)
	res = node1.Exec(c, utils.ToCmdLine("MSETNX", "{same}nx1", "v1", "{same}nx2", "v2"))
	asserts.AssertIntReply(t, res, 1)
	res = node1.Exec(c, utils.ToCmdLine("MSETNX", "{same}nx1", "v1_new", "{same}nx3", "v3"))
	asserts.AssertIntReply(t, res, 0)

	// Unified command routing: JSON.SET / JSON.GET, HEXPIRE, and EVAL in cluster mode
	res = node1.Exec(c, utils.ToCmdLine("JSON.SET", "{same}doc", "$", `{"n":42}`))
	asserts.AssertStatusReply(t, res, "OK")
	res = node1.Exec(c, utils.ToCmdLine("JSON.GET", "{same}doc"))
	asserts.AssertBulkReply(t, res, `{"n":42}`)

	// Multi-key command with same hash tag succeeds; different slots return CROSSSLOT
	res = node1.Exec(c, utils.ToCmdLine("SADD", "{same}s1", "m1", "m2"))
	asserts.AssertIntReply(t, res, 2)
	res = node1.Exec(c, utils.ToCmdLine("SADD", "{same}s2", "m2", "m3"))
	asserts.AssertIntReply(t, res, 2)
	res = node1.Exec(c, utils.ToCmdLine("SUNIONCARD", "2", "{same}s1", "{same}s2"))
	asserts.AssertIntReply(t, res, 3)

	crossRes := node1.Exec(c, utils.ToCmdLine("SUNION", "1", "2"))
	asserts.AssertErrReply(t, crossRes, "CROSSSLOT Keys in request don't hash to the same slot")
}
