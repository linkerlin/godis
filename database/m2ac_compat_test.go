package database

import (
	"testing"
	"time"

	"github.com/linkerlin/godis/datastruct/timeseries"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2acSInterCardLimitEarlyStop(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SADD", "a", "1", "2", "3", "4", "5"))
	db.Exec(nil, utils.ToCmdLine("SADD", "b", "1", "2", "3", "4", "5"))
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"SINTERCARD", "2", "a", "b", "LIMIT", "2",
	)), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"SINTERCARD", "2", "a", "b",
	)), 5)
}

func TestM2acGeoRadiusWithDist(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"GEOADD", "g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania",
	)), 2)
	r := db.Exec(nil, utils.ToCmdLine(
		"GEORADIUS", "g", "15", "37", "200", "km", "WITHDIST", "ASC", "COUNT", "2",
	))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) < 1 {
		t.Fatalf("GEORADIUS WITHDIST: %s", r.ToBytes())
	}
	row, ok := mr.Replies[0].(*protocol.MultiRawReply)
	if !ok || len(row.Replies) < 2 {
		t.Fatalf("expected member+dist row: %s", r.ToBytes())
	}
}

func TestM2acTSRangeTWA(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TS.CREATE", "ts")), "OK")
	db.Exec(nil, utils.ToCmdLine("TS.ADD", "ts", "1", "10"))
	db.Exec(nil, utils.ToCmdLine("TS.ADD", "ts", "100", "20"))
	db.Exec(nil, utils.ToCmdLine("TS.ADD", "ts", "200", "30"))

	r := db.Exec(nil, utils.ToCmdLine(
		"TS.RANGE", "ts", "1", "200", "AGGREGATION", "twa", "100",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("TS.RANGE twa: %s", r.ToBytes())
	}
}

func TestM2acTWAUnit(t *testing.T) {
	ts := timeseries.NewTimeSeries("t", 0)
	_, _ = ts.Add(1, 10)
	_, _ = ts.Add(100, 20)
	samples := ts.RangeWithAggregation(1, 99, 100*time.Millisecond, timeseries.TwaAggregation)
	if len(samples) != 1 {
		t.Fatalf("buckets: %+v", samples)
	}
	// Sample at t=1 holds until next sample (100) or bucket end (100): weight covers [1,100)
	if samples[0].Value < 9.9 || samples[0].Value > 10.1 {
		t.Fatalf("twa want ~10 got %v", samples[0].Value)
	}
}
