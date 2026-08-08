package protocol

import (
	"bytes"
	"testing"
)

func TestScorePairsReplyEncoding(t *testing.T) {
	r := MakeScorePairsReply([]string{"a", "b"}, []float64{1.5, 2}, true)

	// RESP2: flat bulks
	got2 := r.ToBytes()
	want2 := []byte("*4\r\n$1\r\na\r\n$3\r\n1.5\r\n$1\r\nb\r\n$1\r\n2\r\n")
	if !bytes.Equal(got2, want2) {
		t.Fatalf("RESP2: want %q got %q", want2, got2)
	}

	// RESP3 nested
	got3 := ReplyToRESP3(r)
	want3 := []byte("*2\r\n*2\r\n$1\r\na\r\n,1.5\r\n*2\r\n$1\r\nb\r\n,2\r\n")
	if !bytes.Equal(got3, want3) {
		t.Fatalf("RESP3 nest: want %q got %q", want3, got3)
	}

	flat := MakeScorePairsReply([]string{"a"}, []float64{1.5}, false)
	gotFlat := ReplyToRESP3(flat)
	wantFlat := []byte("*2\r\n$1\r\na\r\n,1.5\r\n")
	if !bytes.Equal(gotFlat, wantFlat) {
		t.Fatalf("RESP3 flat: want %q got %q", wantFlat, gotFlat)
	}
}
