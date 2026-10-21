package atomic

import (
	"sync"
	"testing"
)

func TestBoolean(t *testing.T) {
	var b Boolean
	if b.Get() {
		t.Fatal("zero value should be false")
	}
	b.Set(true)
	if !b.Get() {
		t.Fatal("Set(true) not visible")
	}
	b.Set(false)
	if b.Get() {
		t.Fatal("Set(false) not visible")
	}
}

func TestBooleanConcurrent(t *testing.T) {
	var b Boolean
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				b.Set(true)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				_ = b.Get()
			}
		}()
	}
	wg.Wait()
}
