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
		if bit.tohex(255, 2) ~= "ff" then return "tohex" end
		if bit.tohex(255, -2) ~= "FF" then return "tohexU" end
		if bit.rol(1, 3) ~= 8 then return "rol" end
		if bit.ror(8, 3) ~= 1 then return "ror" end
		if bit.bswap(0x01020304) ~= 0x04030201 then return "bswap" end
		return "ok"
	`, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out != "ok" {
		t.Fatalf("got %#v", out)
	}
}
