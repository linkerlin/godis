package aof

import (
	"testing"
	"time"

	godisjson "github.com/linkerlin/godis/datastruct/json"
	"github.com/linkerlin/godis/datastruct/stream"
	"github.com/linkerlin/godis/datastruct/vector"
	"github.com/linkerlin/godis/interface/database"
)

func TestOpaqueStreamRoundTrip(t *testing.T) {
	s := stream.NewStream()
	if _, err := s.Add("1-0", map[string]string{"a": "1"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateGroup("g", "$"); err != nil {
		t.Fatal(err)
	}
	payload, ok := EncodeOpaque(&database.DataEntity{Data: s})
	if !ok {
		t.Fatal("encode")
	}
	entity, ok := DecodeOpaque(payload)
	if !ok {
		t.Fatal("decode")
	}
	got := entity.Data.(*stream.Stream)
	if got.Len() != 1 {
		t.Fatalf("len=%d", got.Len())
	}
	if _, err := got.GetGroup("g"); err != nil {
		t.Fatal(err)
	}
}

func TestOpaqueStreamPELRoundTrip(t *testing.T) {
	s := stream.NewStream()
	if _, err := s.Add("1-0", map[string]string{"a": "1"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateGroup("g", "0-0"); err != nil {
		t.Fatal(err)
	}
	grp, err := s.GetGroup("g")
	if err != nil {
		t.Fatal(err)
	}
	grp.LastID = stream.StreamID{Timestamp: 1, Sequence: 0}
	grp.EntriesRead = 1
	cons := grp.GetConsumer("c1")
	cons.SeenTime = time.UnixMilli(1_700_000_000_000)
	id := stream.StreamID{Timestamp: 1, Sequence: 0}
	pe := &stream.PendingEntry{
		ID:            id,
		Consumer:      "c1",
		DeliveryCount: 2,
		DeliveryTime:  time.UnixMilli(1_700_000_000_100),
	}
	grp.Pending[id] = pe
	cons.Pending[id] = pe

	payload, ok := EncodeOpaque(&database.DataEntity{Data: s})
	if !ok {
		t.Fatal("encode")
	}
	entity, ok := DecodeOpaque(payload)
	if !ok {
		t.Fatal("decode")
	}
	got := entity.Data.(*stream.Stream)
	gg, err := got.GetGroup("g")
	if err != nil {
		t.Fatal(err)
	}
	if gg.LastID.String() != "1-0" || gg.EntriesRead != 1 {
		t.Fatalf("group cursor lid=%s er=%d", gg.LastID, gg.EntriesRead)
	}
	gc := gg.GetConsumer("c1")
	if gc.SeenTime.UnixMilli() != 1_700_000_000_000 {
		t.Fatalf("seen=%d", gc.SeenTime.UnixMilli())
	}
	gpe, ok := gg.Pending[id]
	if !ok || gpe.DeliveryCount != 2 || gpe.Consumer != "c1" {
		t.Fatalf("group PEL=%v ok=%v", gpe, ok)
	}
	cpe, ok := gc.Pending[id]
	if !ok || cpe != gpe {
		t.Fatalf("consumer PEL missing or not shared pointer")
	}
	if gpe.DeliveryTime.UnixMilli() != 1_700_000_000_100 {
		t.Fatalf("delivery time=%d", gpe.DeliveryTime.UnixMilli())
	}
}

func TestOpaqueJSONAndVector(t *testing.T) {
	jv, err := godisjson.NewJSONValueFromString(`{"x":1}`)
	if err != nil {
		t.Fatal(err)
	}
	p, ok := EncodeOpaque(&database.DataEntity{Data: jv})
	if !ok {
		t.Fatal("json encode")
	}
	e, ok := DecodeOpaque(p)
	if !ok {
		t.Fatal("json decode")
	}
	if _, err := e.Data.(*godisjson.JSONValue).ToString(); err != nil {
		t.Fatal(err)
	}

	vs := vector.NewVectorSet()
	vs.Add("a", vector.NewVectorFromFloat64([]float64{1, 0}), nil)
	p, ok = EncodeOpaque(&database.DataEntity{Data: vs})
	if !ok {
		t.Fatal("vector encode")
	}
	e, ok = DecodeOpaque(p)
	if !ok {
		t.Fatal("vector decode")
	}
	if e.Data.(*vector.VectorSet).Len() != 1 {
		t.Fatal("vector len")
	}
}
