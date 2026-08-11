package hashslot

import "testing"

func TestCRC16KnownVector(t *testing.T) {
	// Redis XMODEM CRC16: "123456789" → 0x31C3
	if got := CRC16("123456789"); got != 0x31C3 {
		t.Fatalf("CRC16(123456789)=%04X want 31C3", got)
	}
}

func TestSlotFoo(t *testing.T) {
	// redis-cli CLUSTER KEYSLOT foo → 12182
	if got := Slot("foo"); got != 12182 {
		t.Fatalf("Slot(foo)=%d want 12182", got)
	}
}

func TestSlotHashtag(t *testing.T) {
	a := Slot("user:{1000}:profile")
	b := Slot("user:{1000}:cart")
	c := Slot("{1000}")
	if a != b || a != c {
		t.Fatalf("hashtag slots differ: profile=%d cart=%d tag=%d", a, b, c)
	}
}

func TestTagEdges(t *testing.T) {
	if Tag("plain") != "plain" {
		t.Fatal(Tag("plain"))
	}
	if Tag("{ab}c") != "ab" {
		t.Fatal(Tag("{ab}c"))
	}
	// empty tag {} is ignored by Redis; whole key is used
	if Tag("x{}y") != "x{}y" {
		t.Fatal(Tag("x{}y"))
	}
	// unmatched braces
	if Tag("{abc") != "{abc" {
		t.Fatal(Tag("{abc"))
	}
	if Tag("a{b{c}d") != "b{c" {
		t.Fatal(Tag("a{b{c}d"))
	}
	if Slot("") != 0 {
		t.Fatalf("empty key slot=%d", Slot(""))
	}
}
