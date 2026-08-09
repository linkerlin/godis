package pubsub

import "testing"

func TestShardedHubSlotMatchesRedis(t *testing.T) {
	sh := NewShardedHub()
	if got := sh.getSlot("foo"); got != 12182 {
		t.Fatalf("getSlot(foo)=%d want 12182", got)
	}
	a := sh.getSlot("user:{1000}:a")
	b := sh.getSlot("user:{1000}:b")
	if a != b {
		t.Fatalf("hashtag slots differ: %d vs %d", a, b)
	}
}
