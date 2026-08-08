package asserts

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// AssertIntReply checks if the given redis.Reply is the expected integer
func AssertIntReply(t *testing.T, actual redis.Reply, expected int) {
	intResult, ok := actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s, %s", actual.ToBytes(), printStack())
		return
	}
	if intResult.Code != int64(expected) {
		t.Errorf("expected %d, actually %d, %s", expected, intResult.Code, printStack())
	}
}

func AssertIntReplyGreaterThan(t *testing.T, actual redis.Reply, expected int) {
	intResult, ok := actual.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s, %s", actual.ToBytes(), printStack())
		return
	}
	if intResult.Code < int64(expected) {
		t.Errorf("expected %d, actually %d, %s", expected, intResult.Code, printStack())
	}
}

// AssertBulkReply checks if the given redis.Reply is the expected string
func AssertBulkReply(t *testing.T, actual redis.Reply, expected string) {
	bulkReply, ok := actual.(*protocol.BulkReply)
	if ok {
		if !utils.BytesEquals(bulkReply.Arg, []byte(expected)) {
			t.Errorf("expected %s, actually %s, %s", expected, actual.ToBytes(), printStack())
		}
		return
	}
	// DoubleReply (and other types) encode as bulk on RESP2 / ToBytes.
	expectBytes := protocol.MakeBulkReply([]byte(expected)).ToBytes()
	if utils.BytesEquals(actual.ToBytes(), expectBytes) {
		return
	}
	t.Errorf("expected bulk protocol, actually %s, %s", actual.ToBytes(), printStack())
}

// AssertStatusReply checks if the given redis.Reply is the expected status
func AssertStatusReply(t *testing.T, actual redis.Reply, expected string) {
	statusReply, ok := actual.(*protocol.StatusReply)
	if !ok {
		// may be a protocol.OkReply e.g.
		expectBytes := protocol.MakeStatusReply(expected).ToBytes()
		if utils.BytesEquals(actual.ToBytes(), expectBytes) {
			return
		}
		t.Errorf("expected bulk protocol, actually %s, %s", actual.ToBytes(), printStack())
		return
	}
	if statusReply.Status != expected {
		t.Errorf("expected %s, actually %s, %s", expected, actual.ToBytes(), printStack())
	}
}

// AssertErrReply checks if the given redis.Reply is the expected error
func AssertErrReply(t *testing.T, actual redis.Reply, expected string) {
	errReply, ok := actual.(protocol.ErrorReply)
	if !ok {
		expectBytes := protocol.MakeErrReply(expected).ToBytes()
		if utils.BytesEquals(actual.ToBytes(), expectBytes) {
			return
		}
		t.Errorf("expected err protocol, actually %s, %s", actual.ToBytes(), printStack())
		return
	}
	if errReply.Error() != expected {
		t.Errorf("expected %s, actually %s, %s", expected, actual.ToBytes(), printStack())
	}
}

// AssertNotError checks if the given redis.Reply is not error protocol
func AssertNotError(t *testing.T, result redis.Reply) {
	if result == nil {
		t.Errorf("result is nil %s", printStack())
		return
	}
	bytes := result.ToBytes()
	if len(bytes) == 0 {
		t.Errorf("result is empty %s", printStack())
		return
	}
	if bytes[0] == '-' {
		t.Errorf("result is err protocol %s", printStack())
	}
}

// AssertNullBulk checks if the given redis.Reply is protocol.NullBulkReply
func AssertNullBulk(t *testing.T, result redis.Reply) {
	if result == nil {
		t.Errorf("result is nil %s", printStack())
		return
	}
	bytes := result.ToBytes()
	if len(bytes) == 0 {
		t.Errorf("result is empty %s", printStack())
		return
	}
	expect := (&protocol.NullBulkReply{}).ToBytes()
	if !utils.BytesEquals(expect, bytes) {
		t.Errorf("result is not null-bulk-protocol %s", printStack())
	}
}

// AssertMultiBulkReply checks if the given redis.Reply has the expected content
func AssertMultiBulkReply(t *testing.T, actual redis.Reply, expected []string) {
	switch r := actual.(type) {
	case *protocol.MultiBulkReply:
		if len(r.Args) != len(expected) {
			t.Errorf("expected %d elements, actually %d, %s",
				len(expected), len(r.Args), printStack())
			return
		}
		for i, v := range r.Args {
			str := string(v)
			if str != expected[i] {
				t.Errorf("expected %s, actually %s, %s", expected[i], actual, printStack())
			}
		}
	case *protocol.ScorePairsReply:
		expArgs := make([][]byte, len(expected))
		for i, s := range expected {
			expArgs[i] = []byte(s)
		}
		if !utils.BytesEquals(r.ToBytes(), protocol.MakeMultiBulkReply(expArgs).ToBytes()) {
			t.Errorf("expected %v, actually %s, %s", expected, r.ToBytes(), printStack())
		}
	case *protocol.SetReply:
		if len(r.Data) != len(expected) {
			t.Errorf("expected %d elements, actually %d, %s",
				len(expected), len(r.Data), printStack())
			return
		}
		got := make(map[string]bool, len(r.Data))
		for _, elem := range r.Data {
			bulk, ok := elem.(*protocol.BulkReply)
			if !ok {
				t.Errorf("expected bulk set elem, got %T %s", elem, printStack())
				return
			}
			got[string(bulk.Arg)] = true
		}
		for _, e := range expected {
			if !got[e] {
				t.Errorf("missing expected member %s, %s", e, printStack())
			}
		}
	case *protocol.MapReply:
		if len(r.Data)*2 != len(expected) {
			t.Errorf("expected %d elements, actually %d map entries, %s",
				len(expected), len(r.Data), printStack())
			return
		}
		for i := 0; i+1 < len(expected); i += 2 {
			v, ok := r.Data[expected[i]]
			if !ok {
				t.Errorf("missing field %s, %s", expected[i], printStack())
				continue
			}
			bulk, ok := v.(*protocol.BulkReply)
			if !ok || string(bulk.Arg) != expected[i+1] {
				t.Errorf("expected %s=%s, got %s, %s", expected[i], expected[i+1], v.ToBytes(), printStack())
			}
		}
	default:
		t.Errorf("expected bulk protocol, actually %T %s, %s", actual, actual.ToBytes(), printStack())
	}
}

// AssertMultiBulkReplySize check if redis.Reply has expected length
func AssertMultiBulkReplySize(t *testing.T, actual redis.Reply, expected int) {
	if multiBulk, ok := actual.(*protocol.MultiBulkReply); ok {
		if len(multiBulk.Args) != expected {
			t.Errorf("expected %d elements, actually %d, %s", expected, len(multiBulk.Args), printStack())
		}
		return
	}
	if multiRaw, ok := actual.(*protocol.MultiRawReply); ok {
		if len(multiRaw.Replies) != expected {
			t.Errorf("expected %d elements, actually %d, %s", expected, len(multiRaw.Replies), printStack())
		}
		return
	}
	if set, ok := actual.(*protocol.SetReply); ok {
		if len(set.Data) != expected {
			t.Errorf("expected %d elements, actually %d, %s", expected, len(set.Data), printStack())
		}
		return
	}
	if pairs, ok := actual.(*protocol.ScorePairsReply); ok {
		// Flat pair list length = 2 * N; tests often assert wire element count.
		if len(pairs.Members)*2 != expected {
			t.Errorf("expected %d elements, actually %d pairs (%d elems), %s",
				expected, len(pairs.Members), len(pairs.Members)*2, printStack())
		}
		return
	}
	if expected == 0 &&
		utils.BytesEquals(actual.ToBytes(), protocol.MakeEmptyMultiBulkReply().ToBytes()) {
		return
	}
	t.Errorf("expected bulk protocol, actually %s, %s", actual.ToBytes(), printStack())
}

func printStack() string {
	_, file, no, ok := runtime.Caller(2)
	if ok {
		return fmt.Sprintf("at %s:%d", file, no)
	}
	return ""
}
