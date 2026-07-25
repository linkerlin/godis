package scripting

import "testing"

func TestM2asCMsgPackRoundTrip(t *testing.T) {
	eng := NewGopherEngine(nil)
	out, err := eng.Eval(`
		local b = cmsgpack.pack("hi", 7)
		local a, n = cmsgpack.unpack(b)
		if a ~= "hi" or n ~= 7 then
			return "fail"
		end
		return "ok"
	`, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out != "ok" {
		t.Fatalf("got %#v", out)
	}
}
