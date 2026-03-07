package pubsub

import (
	"sync"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
)

// ShardedHub manages sharded pub/sub channels
// Each channel is mapped to a specific slot based on its name
type ShardedHub struct {
	// slot -> channel -> subscribers
	slots map[int]map[string]map[redis.Connection]struct{}
	mu    sync.RWMutex
}

// NewShardedHub creates a new sharded pub/sub hub
func NewShardedHub() *ShardedHub {
	return &ShardedHub{
		slots: make(map[int]map[string]map[redis.Connection]struct{}),
	}
}

// getSlot calculates the slot for a channel (simplified CRC16)
func (sh *ShardedHub) getSlot(channel string) int {
	// Simple hash for now - in production use CRC16
	hash := 0
	for i := 0; i < len(channel); i++ {
		hash = ((hash << 5) - hash) + int(channel[i])
		hash = hash & 0x7FFF // Keep positive
	}
	return hash % 16384 // Redis cluster has 16384 slots
}

// Subscribe subscribes a connection to sharded channels
func (sh *ShardedHub) Subscribe(conn redis.Connection, channels []string) redis.Reply {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	
	for _, channel := range channels {
		slot := sh.getSlot(channel)
		
		// Initialize slot
		if sh.slots[slot] == nil {
			sh.slots[slot] = make(map[string]map[redis.Connection]struct{})
		}
		
		// Initialize channel
		if sh.slots[slot][channel] == nil {
			sh.slots[slot][channel] = make(map[redis.Connection]struct{})
		}
		
		// Add subscriber
		sh.slots[slot][channel][conn] = struct{}{}
	}
	
	// Return subscribe confirmation
	return &protocol.MultiBulkReply{
		Args: [][]byte{[]byte("subscribe"), []byte(channels[0]), []byte("1")},
	}
}

// Unsubscribe unsubscribes from sharded channels
func (sh *ShardedHub) Unsubscribe(conn redis.Connection, channels []string) redis.Reply {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	
	if len(channels) == 0 {
		// Unsubscribe from all
		for slot, slotMap := range sh.slots {
			for channel, subs := range slotMap {
				delete(subs, conn)
				if len(subs) == 0 {
					delete(slotMap, channel)
				}
			}
			if len(slotMap) == 0 {
				delete(sh.slots, slot)
			}
		}
		return &protocol.MultiBulkReply{
			Args: [][]byte{[]byte("unsubscribe"), []byte{}, []byte("0")},
		}
	}
	
	for _, channel := range channels {
		slot := sh.getSlot(channel)
		
		if slotMap, ok := sh.slots[slot]; ok {
			if subs, ok := slotMap[channel]; ok {
				delete(subs, conn)
				if len(subs) == 0 {
					delete(slotMap, channel)
				}
			}
			if len(slotMap) == 0 {
				delete(sh.slots, slot)
			}
		}
	}
	
	return &protocol.MultiBulkReply{
		Args: [][]byte{[]byte("unsubscribe"), []byte(channels[0]), []byte("1")},
	}
}

// Publish publishes a message to a sharded channel
func (sh *ShardedHub) Publish(channel string, message []byte) int {
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	
	slot := sh.getSlot(channel)
	
	slotMap, ok := sh.slots[slot]
	if !ok {
		return 0
	}
	
	subs, ok := slotMap[channel]
	if !ok {
		return 0
	}
	
	// Send to all subscribers
	for conn := range subs {
		// Send sharded message push
		// >2\r\n$7\r\nsmessage\r\n$...\r\nchannel\r\n$...\r\nmessage\r\n
		reply := MakeSMessageReply(channel, message)
		conn.Write(reply.ToBytes())
	}
	
	return len(subs)
}

// GetSlot returns the slot for a channel
func (sh *ShardedHub) GetSlot(channel string) int {
	return sh.getSlot(channel)
}

// AfterClientClose cleans up when client disconnects
func (sh *ShardedHub) AfterClientClose(conn redis.Connection) {
	sh.Unsubscribe(conn, nil)
}

// MakeSMessageReply creates a sharded message reply
func MakeSMessageReply(channel string, message []byte) *protocol.MultiBulkReply {
	return &protocol.MultiBulkReply{
		Args: [][]byte{[]byte("smessage"), []byte(channel), message},
	}
}
