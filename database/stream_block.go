package database

import "sync"

// Stream blocking waiters (XREAD / XREADGROUP BLOCK).
var (
	streamWaitMu  sync.Mutex
	streamWaiters = make(map[string][]*blockWaiter) // key -> waiters
)

func registerStreamWaiter(keys []string) *blockWaiter {
	w := &blockWaiter{
		keys:     append([]string(nil), keys...),
		ch:       make(chan struct{}, 1),
		clientID: peekBlockingClientID(),
	}
	streamWaitMu.Lock()
	for _, k := range keys {
		streamWaiters[k] = append(streamWaiters[k], w)
	}
	streamWaitMu.Unlock()
	trackBlocker(w)
	return w
}

func unregisterStreamWaiter(w *blockWaiter) {
	untrackBlocker(w)
	streamWaitMu.Lock()
	defer streamWaitMu.Unlock()
	for _, k := range w.keys {
		ws := streamWaiters[k]
		for i, x := range ws {
			if x == w {
				streamWaiters[k] = append(ws[:i], ws[i+1:]...)
				break
			}
		}
		if len(streamWaiters[k]) == 0 {
			delete(streamWaiters, k)
		}
	}
}

// signalStreamWaiters wakes clients blocked on XREAD/XREADGROUP for key.
func signalStreamWaiters(key string) {
	streamWaitMu.Lock()
	ws := append([]*blockWaiter(nil), streamWaiters[key]...)
	streamWaitMu.Unlock()
	for _, w := range ws {
		select {
		case w.ch <- struct{}{}:
		default:
		}
	}
}

// GetBlockedStreamClientsCount counts unique stream blockers.
func GetBlockedStreamClientsCount() int64 {
	streamWaitMu.Lock()
	defer streamWaitMu.Unlock()
	seen := make(map[*blockWaiter]struct{})
	for _, ws := range streamWaiters {
		for _, w := range ws {
			seen[w] = struct{}{}
		}
	}
	return int64(len(seen))
}
