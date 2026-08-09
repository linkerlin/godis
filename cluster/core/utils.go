package core

import (
	"errors"
	"net"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/hashslot"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
)

// SlotCount is Redis Cluster hash-slot count (CRC16-XMODEM % 16384).
const SlotCount int = hashslot.Count

const getCommittedIndexCommand = "raft.committedindex"

func init() {
	RegisterCmd(getCommittedIndexCommand, execRaftCommittedIndex)
}

// relay function relays command to peer or calls cluster.Exec
func (cluster *Cluster) Relay(peerId string, c redis.Connection, cmdLine [][]byte) redis.Reply {
	// use a variable to allow injecting stub for testing, see defaultRelayImpl
	if peerId == cluster.SelfID() {
		// to self db
		return cluster.Exec(c, cmdLine)
	}
	// peerId is peer.Addr
	cli, err := cluster.connections.BorrowPeerClient(peerId)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.connections.ReturnPeerClient(cli)
	}()
	return cli.Send(cmdLine)
}

// LocalExec executes command at local node
func (cluster *Cluster) LocalExec(c redis.Connection, cmdLine [][]byte) redis.Reply {
	return cluster.db.Exec(c, cmdLine)
}

// LocalExec executes command at local node
func (cluster *Cluster) LocalExecWithinLock(c redis.Connection, cmdLine [][]byte) redis.Reply {
	return cluster.db.ExecWithLock(c, cmdLine)
}

func (cluster *Cluster) SlaveOf(master string) error {
	host, port, err := net.SplitHostPort(master)
	if err != nil {
		return errors.New("invalid master address")
	}
	c := connection.NewFakeConn()
	reply := cluster.db.Exec(c, utils.ToCmdLine("slaveof", host, port))
	if err := protocol.Try2ErrorReply(reply); err != nil {
		return err
	}
	return nil
}

// GetPartitionKey extract hashtag (same rules as Redis Cluster / hashslot.Tag).
func GetPartitionKey(key string) string {
	return hashslot.Tag(key)
}

func defaultGetSlotImpl(cluster *Cluster, key string) uint32 {
	return hashslot.Slot(key)
}

func (cluster *Cluster) GetSlot(key string) uint32 {
	return cluster.getSlotImpl(key)
}

func defaultPickNodeImpl(cluster *Cluster, slotID uint32) string {
	return cluster.raftNode.FSM.PickNode(slotID)
}

// pickNode returns the node id hosting the given slot.
// If the slot is migrating, return the node which is exporting the slot
func (cluster *Cluster) PickNode(slotID uint32) string {
	return cluster.pickNodeImpl(slotID)
}

// format: raft.committedindex
func execRaftCommittedIndex(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	index, err := cluster.raftNode.CommittedIndex()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeIntReply(int64(index))
}

// LocalExists returns existed ones from `keys` in local node
func (cluster *Cluster) LocalExists(keys []string) []string {
	var exists []string
	for _, key := range keys {
		_, ok, _ := cluster.db.GetEntity(0, key)
		if ok {
			exists = append(exists, key)
		}
	}
	return exists
}
