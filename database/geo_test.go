package database

import (
	"strconv"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGeoHash(t *testing.T) {
	execFlushDB(testDB, utils.ToCmdLine())
	key := utils.RandString(10)
	pos := utils.RandString(10)
	result := execGeoAdd(testDB, utils.ToCmdLine(key, "13.361389", "38.115556", pos))
	asserts.AssertIntReply(t, result, 1)
	result = execGeoHash(testDB, utils.ToCmdLine(key, pos))
	asserts.AssertMultiBulkReply(t, result, []string{"sqc8b49rnys0"})
}

func TestGeoRadius(t *testing.T) {
	execFlushDB(testDB, utils.ToCmdLine())
	key := utils.RandString(10)
	pos1 := utils.RandString(10)
	pos2 := utils.RandString(10)
	execGeoAdd(testDB, utils.ToCmdLine(key,
		"13.361389", "38.115556", pos1,
		"15.087269", "37.502669", pos2,
	))
	result := execGeoRadius(testDB, utils.ToCmdLine(key, "15", "37", "200", "km"))
	asserts.AssertMultiBulkReplySize(t, result, 2)
}

func TestGeoRadiusByMember(t *testing.T) {
	execFlushDB(testDB, utils.ToCmdLine())
	key := utils.RandString(10)
	pos1 := utils.RandString(10)
	pos2 := utils.RandString(10)
	pivot := utils.RandString(10)
	execGeoAdd(testDB, utils.ToCmdLine(key,
		"13.361389", "38.115556", pos1,
		"17.087269", "38.502669", pos2,
		"13.583333", "37.316667", pivot,
	))
	result := execGeoRadiusByMember(testDB, utils.ToCmdLine(key, pivot, "100", "km"))
	asserts.AssertMultiBulkReplySize(t, result, 2)
}

func TestGeoPos(t *testing.T) {
	execFlushDB(testDB, utils.ToCmdLine())
	key := utils.RandString(10)
	pos1 := utils.RandString(10)
	pos2 := utils.RandString(10)
	execGeoAdd(testDB, utils.ToCmdLine(key,
		"13.361389", "38.115556", pos1,
	))
	result := execGeoPos(testDB, utils.ToCmdLine(key, pos1, pos2))
	mr, ok := result.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 2 {
		t.Fatalf("GEOPOS shape: %s", result.ToBytes())
	}
	coord, ok := mr.Replies[0].(*protocol.MultiBulkReply)
	if !ok || len(coord.Args) != 2 {
		t.Fatalf("member coords: %s", result.ToBytes())
	}
	lng, err1 := strconv.ParseFloat(string(coord.Args[0]), 64)
	lat, err2 := strconv.ParseFloat(string(coord.Args[1]), 64)
	if err1 != nil || err2 != nil {
		t.Fatal(err1, err2)
	}
	if lng < 13.361 || lng > 13.362 || lat < 38.115 || lat > 38.116 {
		t.Fatalf("coords out of range: %f %f", lng, lat)
	}
	if _, ok := mr.Replies[1].(*protocol.NullBulkReply); !ok {
		// Null bulk may also appear as MultiBulk nil-style
		if string(mr.Replies[1].ToBytes()) != "$-1\r\n" {
			t.Fatalf("missing member should be null: %s", mr.Replies[1].ToBytes())
		}
	}
}

func TestGeoDist(t *testing.T) {
	execFlushDB(testDB, utils.ToCmdLine())
	key := utils.RandString(10)
	pos1 := utils.RandString(10)
	pos2 := utils.RandString(10)
	execGeoAdd(testDB, utils.ToCmdLine(key,
		"13.361389", "38.115556", pos1,
		"15.087269", "37.502669", pos2,
	))
	result := execGeoDist(testDB, utils.ToCmdLine(key, pos1, pos2, "km"))
	dist, err := parseReplyFloat(result)
	if err != nil {
		t.Error(err)
		return
	}
	if dist < 166.274 || dist > 166.275 {
		t.Errorf("expected 166.274, actual: %f", dist)
	}

	result = execGeoDist(testDB, utils.ToCmdLine(key, pos1, pos2, "m"))
	dist, err = parseReplyFloat(result)
	if err != nil {
		t.Error(err)
		return
	}
	if dist < 166274 || dist > 166275 {
		t.Errorf("expected 166274, actual: %f", dist)
	}
}

func parseReplyFloat(r interface{ ToBytes() []byte }) (float64, error) {
	switch v := r.(type) {
	case *protocol.BulkReply:
		return strconv.ParseFloat(string(v.Arg), 64)
	case *protocol.DoubleReply:
		return v.Value, nil
	default:
		return 0, strconv.ErrSyntax
	}
}
