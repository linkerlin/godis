package database

import (
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"testing"
)

// Test ZUNION, ZINTER, ZDIFF commands
func TestZUnion(t *testing.T) {
	testDB.Flush()
	
	// Prepare data
	asserts.AssertNotError(t, testDB.Exec(nil, utils.ToCmdLine("ZAdd", "zset1", "1", "a", "2", "b", "3", "c")))
	asserts.AssertNotError(t, testDB.Exec(nil, utils.ToCmdLine("ZAdd", "zset2", "1", "b", "2", "c", "3", "d")))
	
	// Test ZUNION basic (sorted by score: a:1, b:3, d:3, c:5)
	result := testDB.Exec(nil, utils.ToCmdLine("ZUNION", "2", "zset1", "zset2"))
	asserts.AssertMultiBulkReply(t, result, []string{"a", "b", "d", "c"})
	
	// Test ZUNION with scores
	result = testDB.Exec(nil, utils.ToCmdLine("ZUNION", "2", "zset1", "zset2", "WITHSCORES"))
	// a:1, b:2+1=3, c:3+2=5, d:3
	asserts.AssertMultiBulkReplySize(t, result, 8)
	
	// Test ZUNIONSTORE
	result = testDB.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "dest", "2", "zset1", "zset2"))
	asserts.AssertIntReply(t, result, 4)
	
	result = testDB.Exec(nil, utils.ToCmdLine("ZCARD", "dest"))
	asserts.AssertIntReply(t, result, 4)
}

func TestZInter(t *testing.T) {
	testDB.Flush()
	
	// Prepare data
	asserts.AssertNotError(t, testDB.Exec(nil, utils.ToCmdLine("ZAdd", "zset1", "1", "a", "2", "b", "3", "c")))
	asserts.AssertNotError(t, testDB.Exec(nil, utils.ToCmdLine("ZAdd", "zset2", "1", "b", "2", "c", "3", "d")))
	
	// Test ZINTER basic
	result := testDB.Exec(nil, utils.ToCmdLine("ZINTER", "2", "zset1", "zset2"))
	asserts.AssertMultiBulkReply(t, result, []string{"b", "c"})
	
	// Test ZINTER with scores
	result = testDB.Exec(nil, utils.ToCmdLine("ZINTER", "2", "zset1", "zset2", "WITHSCORES"))
	// b:2+1=3, c:3+2=5
	asserts.AssertMultiBulkReplySize(t, result, 4)
	
	// Test ZINTERSTORE
	result = testDB.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "dest", "2", "zset1", "zset2"))
	asserts.AssertIntReply(t, result, 2)
	
	result = testDB.Exec(nil, utils.ToCmdLine("ZCARD", "dest"))
	asserts.AssertIntReply(t, result, 2)
}

func TestZDiff(t *testing.T) {
	testDB.Flush()
	
	// Prepare data
	asserts.AssertNotError(t, testDB.Exec(nil, utils.ToCmdLine("ZAdd", "zset1", "1", "a", "2", "b", "3", "c")))
	asserts.AssertNotError(t, testDB.Exec(nil, utils.ToCmdLine("ZAdd", "zset2", "1", "b", "2", "c", "3", "d")))
	
	// Test ZDIFF basic (zset1 - zset2 = {a})
	result := testDB.Exec(nil, utils.ToCmdLine("ZDIFF", "2", "zset1", "zset2"))
	asserts.AssertMultiBulkReply(t, result, []string{"a"})
	
	// Test ZDIFF with scores
	result = testDB.Exec(nil, utils.ToCmdLine("ZDIFF", "2", "zset1", "zset2", "WITHSCORES"))
	asserts.AssertMultiBulkReplySize(t, result, 2)
	
	// Test ZDIFFSTORE
	result = testDB.Exec(nil, utils.ToCmdLine("ZDIFFSTORE", "dest", "2", "zset1", "zset2"))
	asserts.AssertIntReply(t, result, 1)
	
	result = testDB.Exec(nil, utils.ToCmdLine("ZCARD", "dest"))
	asserts.AssertIntReply(t, result, 1)
}

func TestZSetOperationsWithWeights(t *testing.T) {
	testDB.Flush()
	
	// Prepare data
	asserts.AssertNotError(t, testDB.Exec(nil, utils.ToCmdLine("ZAdd", "zset1", "1", "a", "2", "b")))
	asserts.AssertNotError(t, testDB.Exec(nil, utils.ToCmdLine("ZAdd", "zset2", "1", "b", "2", "c")))
	
	// Test ZUNION with weights
	result := testDB.Exec(nil, utils.ToCmdLine("ZUNION", "2", "zset1", "zset2", "WEIGHTS", "2", "3", "WITHSCORES"))
	// a:1*2=2, b:2*2+1*3=7, c:2*3=6
	asserts.AssertMultiBulkReplySize(t, result, 6)
	
	// Test ZINTER with weights
	result = testDB.Exec(nil, utils.ToCmdLine("ZINTER", "2", "zset1", "zset2", "WEIGHTS", "2", "3", "WITHSCORES"))
	// b:2*2+1*3=7
	asserts.AssertMultiBulkReplySize(t, result, 2)
}

func TestZSetOperationsAggregate(t *testing.T) {
	testDB.Flush()
	
	// Prepare data
	asserts.AssertNotError(t, testDB.Exec(nil, utils.ToCmdLine("ZAdd", "zset1", "1", "a", "2", "b")))
	asserts.AssertNotError(t, testDB.Exec(nil, utils.ToCmdLine("ZAdd", "zset2", "3", "a", "4", "b")))
	
	// Test with AGGREGATE MIN
	result := testDB.Exec(nil, utils.ToCmdLine("ZUNION", "2", "zset1", "zset2", "AGGREGATE", "MIN", "WITHSCORES"))
	// a:min(1,3)=1, b:min(2,4)=2
	asserts.AssertMultiBulkReplySize(t, result, 4)
	
	// Test with AGGREGATE MAX
	result = testDB.Exec(nil, utils.ToCmdLine("ZUNION", "2", "zset1", "zset2", "AGGREGATE", "MAX", "WITHSCORES"))
	// a:max(1,3)=3, b:max(2,4)=4
	asserts.AssertMultiBulkReplySize(t, result, 4)
}

// Test ZMPOP command
func TestZMPop(t *testing.T) {
	testDB.Flush()
	
	// Prepare data
	asserts.AssertNotError(t, testDB.Exec(nil, utils.ToCmdLine("ZAdd", "zset1", "1", "a", "2", "b", "3", "c")))
	
	// Test ZMPOP MIN
	result := testDB.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "zset1", "MIN"))
	// Returns: [zset1, [[a, 1]]]
	asserts.AssertMultiBulkReplySize(t, result, 2)
	
	// Check remaining elements
	result = testDB.Exec(nil, utils.ToCmdLine("ZCARD", "zset1"))
	asserts.AssertIntReply(t, result, 2)
	
	// Test ZMPOP MAX with count
	asserts.AssertNotError(t, testDB.Exec(nil, utils.ToCmdLine("ZAdd", "zset2", "1", "x", "2", "y", "3", "z")))
	result = testDB.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "zset2", "MAX", "COUNT", "2"))
	// Returns: [zset2, [[z, 3], [y, 2]]]
	asserts.AssertMultiBulkReplySize(t, result, 2)
	
	// Test ZMPOP on non-existent key
	result = testDB.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "nonexistent", "MIN"))
	asserts.AssertNullBulk(t, result)
}

// Test ZMSCORE command (if not already tested)
func TestZMScore(t *testing.T) {
	testDB.Flush()
	
	// Prepare data
	asserts.AssertNotError(t, testDB.Exec(nil, utils.ToCmdLine("ZAdd", "zset1", "1", "a", "2", "b", "3", "c")))
	
	// Test ZMSCORE
	result := testDB.Exec(nil, utils.ToCmdLine("ZMSCORE", "zset1", "a", "b", "nonexistent"))
	asserts.AssertMultiBulkReplySize(t, result, 3)
}
