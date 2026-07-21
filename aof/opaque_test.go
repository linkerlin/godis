package aof

import (
	"testing"

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
