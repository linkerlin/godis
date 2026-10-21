package wait

import (
	"testing"
	"time"
)

func TestWaitCompleted(t *testing.T) {
	var w Wait
	if w.WaitWithTimeout(10 * time.Millisecond) {
		t.Fatal("empty Wait should complete immediately")
	}
	w.Add(1)
	w.Done()
	if w.WaitWithTimeout(10 * time.Millisecond) {
		t.Fatal("completed Wait should not time out")
	}
	w.Wait()
}

func TestWaitTimeoutAndLateDone(t *testing.T) {
	var w Wait
	w.Add(1)
	if !w.WaitWithTimeout(10 * time.Millisecond) {
		t.Fatal("outstanding Wait should time out")
	}
	done := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Millisecond)
		w.Done()
		close(done)
	}()
	if w.WaitWithTimeout(time.Second) {
		t.Fatal("Wait should complete after Done")
	}
	<-done
}
