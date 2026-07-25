package database

import (
	"net"
	"strconv"
	"sync/atomic"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// execRole returns the replication role of this instance (Redis ROLE).
// Master: ["master", offset, [[ip, port, offset], ...]]
// Slave:  ["slave", host, port, state, offset]
func (server *Server) execRole(args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeArgNumErrReply("role")
	}
	role := atomic.LoadInt32(&server.role)
	if role == slaveRole {
		server.slaveStatus.mutex.Lock()
		host := server.slaveStatus.masterHost
		port := server.slaveStatus.masterPort
		offset := server.slaveStatus.replOffset
		server.slaveStatus.mutex.Unlock()
		if host == "" {
			host = "?"
		}
		state := "connect"
		if offset >= 0 {
			state = "connected"
		}
		return protocol.MakeMultiRawReply([]redis.Reply{
			protocol.MakeBulkReply([]byte("slave")),
			protocol.MakeBulkReply([]byte(host)),
			protocol.MakeIntReply(int64(port)),
			protocol.MakeBulkReply([]byte(state)),
			protocol.MakeIntReply(offset),
		})
	}

	offset := int64(0)
	slaves := make([]redis.Reply, 0)
	if server.masterStatus != nil {
		server.masterStatus.mu.Lock()
		if server.masterStatus.backlog != nil {
			offset = server.masterStatus.backlog.currentOffset
		}
		for _, slave := range server.masterStatus.slaveMap {
			ip := slave.announceIp
			port := slave.announcePort
			if ip == "" {
				host, p, err := net.SplitHostPort(clientAddr(slave.conn))
				if err == nil {
					ip = host
					if port == 0 {
						port, _ = strconv.Atoi(p)
					}
				} else {
					ip = clientAddr(slave.conn)
				}
			}
			slaves = append(slaves, protocol.MakeMultiRawReply([]redis.Reply{
				protocol.MakeBulkReply([]byte(ip)),
				protocol.MakeBulkReply([]byte(strconv.Itoa(port))),
				protocol.MakeBulkReply([]byte(strconv.FormatInt(slave.offset, 10))),
			}))
		}
		server.masterStatus.mu.Unlock()
	}

	return protocol.MakeMultiRawReply([]redis.Reply{
		protocol.MakeBulkReply([]byte("master")),
		protocol.MakeIntReply(offset),
		protocol.MakeMultiRawReply(slaves),
	})
}
