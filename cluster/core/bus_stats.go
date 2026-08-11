package core

import "sync/atomic"

// busStats counts Godis peer RPCs that loosely map onto CLUSTER INFO gossip keys.
//
// These are NOT Redis CLUSTERMSG_* bus frames: Godis has no gossip bus
// (cluster_bus_port stays 0). Counters mirror:
//   - cluster.heartbeat send/recv (followers → Raft leader) as ping/pong
//   - CLUSTER MEET success as meet_sent
// Other message types remain honest zeros until a real bus exists.
type busStats struct {
	pingSent     uint64
	pingReceived uint64
	pongSent     uint64
	pongReceived uint64
	meetSent     uint64
}

type busStatsSnap struct {
	pingSent, pingReceived, pongSent, pongReceived, meetSent uint64
}

func (s *busStats) snapshot() busStatsSnap {
	if s == nil {
		return busStatsSnap{}
	}
	return busStatsSnap{
		pingSent:     atomic.LoadUint64(&s.pingSent),
		pingReceived: atomic.LoadUint64(&s.pingReceived),
		pongSent:     atomic.LoadUint64(&s.pongSent),
		pongReceived: atomic.LoadUint64(&s.pongReceived),
		meetSent:     atomic.LoadUint64(&s.meetSent),
	}
}

func (s *busStats) incrPingSent() {
	if s != nil {
		atomic.AddUint64(&s.pingSent, 1)
	}
}

func (s *busStats) incrPingReceived() {
	if s != nil {
		atomic.AddUint64(&s.pingReceived, 1)
	}
}

func (s *busStats) incrPongSent() {
	if s != nil {
		atomic.AddUint64(&s.pongSent, 1)
	}
}

func (s *busStats) incrPongReceived() {
	if s != nil {
		atomic.AddUint64(&s.pongReceived, 1)
	}
}

func (s *busStats) incrMeetSent() {
	if s != nil {
		atomic.AddUint64(&s.meetSent, 1)
	}
}

func (cluster *Cluster) busSnapshot() busStatsSnap {
	if cluster == nil {
		return busStatsSnap{}
	}
	return cluster.bus.snapshot()
}
