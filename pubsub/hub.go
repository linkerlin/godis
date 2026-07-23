package pubsub

import (
	"sync"

	"github.com/linkerlin/godis/datastruct/dict"
	"github.com/linkerlin/godis/datastruct/list"
	"github.com/linkerlin/godis/datastruct/lock"
)

// Hub stores all subscribe relations
type Hub struct {
	// channel -> list(*Client)
	subs dict.Dict
	// lock channel
	subsLocker *lock.Locks

	// pattern -> list(*Client)
	psubs   map[string]*list.LinkedList
	psubsMu sync.RWMutex
}

// MakeHub creates new hub
func MakeHub() *Hub {
	return &Hub{
		subs:       dict.MakeConcurrent(4),
		subsLocker: lock.Make(16),
		psubs:      make(map[string]*list.LinkedList),
	}
}

// NumSub returns the number of subscribers on a channel.
func (hub *Hub) NumSub(channel string) int {
	raw, ok := hub.subs.Get(channel)
	if !ok {
		return 0
	}
	subscribers, _ := raw.(*list.LinkedList)
	if subscribers == nil {
		return 0
	}
	return subscribers.Len()
}

// NumPat returns the number of unique pattern subscriptions.
func (hub *Hub) NumPat() int {
	hub.psubsMu.RLock()
	defer hub.psubsMu.RUnlock()
	return len(hub.psubs)
}

// ForEachChannel visits each channel and its subscriber count.
func (hub *Hub) ForEachChannel(fn func(channel string, n int) bool) {
	hub.subs.ForEach(func(key string, val interface{}) bool {
		subscribers, _ := val.(*list.LinkedList)
		n := 0
		if subscribers != nil {
			n = subscribers.Len()
		}
		return fn(key, n)
	})
}
