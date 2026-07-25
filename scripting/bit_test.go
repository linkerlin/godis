package scripting

import "testing"

func TestM2auBitOps(t *testing.T) {
	eng := NewGopherEngine(nil)
	out, err := eng.Eval(`
		if bit.band(7, 3) ~= 3 then return "band" end
		if bit.bor(1, 2) ~= 3 then return "bor" end
		if bit.bxor(7, 3) ~= 4 then return "bxor" end
		if bit.bnot(0) ~= -1 then return "bnot" end
		if bit.lshift(1, 3) ~= 8 then return "lshift" end
		if bit.rshift(8, 2) ~= 2 then return "rshift" end
		if bit.arshift(-8, 2) ~= -2 then return "arshift" end
		return "ok"
	`, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out != "ok" {
		t.Fatalf("got %#v", out)
	}
}
