package scripting

import (
	"strings"
	"testing"
)

func TestM2arCJSONEncodeDecode(t *testing.T) {
	eng := NewGopherEngine(nil)
	out, err := eng.Eval(`return cjson.encode({a=1, b="x"})`, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, ok := out.(string)
	if !ok || !strings.Contains(s, `"a"`) || !strings.Contains(s, `"b"`) {
		t.Fatalf("encode: %v", out)
	}
	out, err = eng.Eval(`local t = cjson.decode('{"n":2}'); return t.n`, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	switch n := out.(type) {
	case float64:
		if n != 2 {
			t.Fatalf("decode float: %v", out)
		}
	case int64:
		if n != 2 {
			t.Fatalf("decode int64: %v", out)
		}
	case int:
		if n != 2 {
			t.Fatalf("decode int: %v", out)
		}
	default:
		t.Fatalf("decode: %T %v", out, out)
	}
}
