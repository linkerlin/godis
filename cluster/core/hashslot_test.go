package core

import (
	"testing"

	"github.com/linkerlin/godis/lib/hashslot"
)

func TestSlotCountIs16384(t *testing.T) {
	if SlotCount != hashslot.Count || hashslot.Count != 16384 {
		t.Fatalf("SlotCount=%d hashslot.Count=%d want 16384", SlotCount, hashslot.Count)
	}
}

func TestDefaultGetSlotFoo(t *testing.T) {
	// Same as redis-cli CLUSTER KEYSLOT foo
	if got := defaultGetSlotImpl(nil, "foo"); got != 12182 {
		t.Fatalf("slot(foo)=%d want 12182", got)
	}
}

func TestGetPartitionKeyHashtag(t *testing.T) {
	if got := GetPartitionKey("user:{1000}:x"); got != "1000" {
		t.Fatalf("GetPartitionKey=%q want 1000", got)
	}
}
