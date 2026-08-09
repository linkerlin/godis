package core

import (
	"github.com/cockroachdb/errors"

	rdbcore "github.com/hdt3213/rdb/core"
	"github.com/linkerlin/godis/cluster/raft"
	"github.com/linkerlin/godis/config"
	dbimpl "github.com/linkerlin/godis/database"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/logger"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

type Cluster struct {
	raftNode    *raft.Node
	db          database.DBEngine
	connections ConnectionFactory
	config      *Config

	slotsManager     *slotsManager
	rebalanceManager *RebalanceManager
	transactions     *TransactionManager
	replicaManager   *replicaManager

	closeChan chan struct{}

	// allow inject route implementation
	getSlotImpl  func(key string) uint32
	pickNodeImpl func(slotID uint32) string
	id_          string // for tests only

	// inmemProxy: MakeTestCluster sets true so DefaultFunc Relays (in-process multi-node).
	// Real clusters leave false and return MOVED/ASK for Redis Cluster clients.
	inmemProxy bool

	// humanName is CLUSTER SETNAME / GETNAME (Redis 7.2+).
	humanName string

	// slow log record
	slogLogger *dbimpl.SlowLogger
}

type Config struct {
	raft.RaftConfig
	StartAsSeed    bool
	JoinAddress    string
	Master         string
	connectionStub ConnectionFactory // for test
	noCron         bool              // for test
}

func (c *Cluster) SelfID() string {
	if c.raftNode == nil {
		return c.id_
	}
	return c.raftNode.Cfg.ID()
}

func NewCluster(cfg *Config) (*Cluster, error) {
	var connections ConnectionFactory
	if cfg.connectionStub != nil {
		connections = cfg.connectionStub
	} else {
		connections = newDefaultClientFactory()
	}
	db, err := dbimpl.NewStandaloneServer()
	if err != nil {
		return nil, errors.Wrap(err, "create standalone server failed")
	}
	raftNode, err := raft.StartNode(&cfg.RaftConfig)
	if err != nil {
		return nil, err
	}

	cluster := &Cluster{
		raftNode:       raftNode,
		db:             db,
		connections:    connections,
		config:         cfg,
		slotsManager:   newSlotsManager(),
		transactions:   newTransactionManager(),
		replicaManager: newReplicaManager(),
		closeChan:      make(chan struct{}),
	}

	// Initialize rebalance manager after cluster is created
	cluster.rebalanceManager = NewRebalanceManager(cluster)
	cluster.pickNodeImpl = func(slotID uint32) string {
		return defaultPickNodeImpl(cluster, slotID)
	}
	cluster.getSlotImpl = func(key string) uint32 {
		return defaultGetSlotImpl(cluster, key)
	}
	cluster.injectInsertCallback()
	cluster.injectDeleteCallback()
	cluster.registerOnFailover()

	// setup
	hasState, err := raftNode.HasExistingState()
	if err != nil {
		return nil, err
	}
	if !hasState {
		if cfg.StartAsSeed {
			err = raftNode.BootstrapCluster(SlotCount)
			if err != nil {
				return nil, err
			}
		} else {
			// join cluster
			conn, err := connections.BorrowPeerClient(cfg.JoinAddress)
			if err != nil {
				return nil, err
			}
			defer connections.ReturnPeerClient(conn)
			joinCmdLine := utils.ToCmdLine(joinClusterCommand, cfg.RedisAdvertiseAddr, cfg.RaftAdvertiseAddr)
			if cfg.Master != "" {
				joinCmdLine = append(joinCmdLine, []byte(cfg.Master))
			}
			logger.Infof("send join cluster request to %s", cfg.JoinAddress)
			result := conn.Send(joinCmdLine)
			if err := protocol.Try2ErrorReply(result); err != nil {
				return nil, err
			}
		}
	} else {
		masterAddr := cluster.raftNode.FSM.GetMaster(cluster.SelfID())
		if masterAddr != "" {
			err := cluster.SlaveOf(masterAddr)
			if err != nil {
				return nil, errors.Wrap(err, "slave of master failed")
			}
		}
	}

	// record slow log
	cluster.slogLogger = dbimpl.NewSlowLogger(config.Properties.SlowLogMaxLen, config.Properties.SlowLogSlowerThan)

	go cluster.clusterCron()
	return cluster, nil
}

// AfterClientClose does some clean after client close connection
func (cluster *Cluster) AfterClientClose(c redis.Connection) {
	cluster.db.AfterClientClose(c)
}

func (cluster *Cluster) Close() {
	close(cluster.closeChan)
	cluster.db.Close()
	err := cluster.raftNode.Close()
	if err != nil {
		logger.Errorf("close raft node failed: %+v", err)
	}
}

// LoadRDB real implementation of loading rdb file
func (cluster *Cluster) LoadRDB(dec *rdbcore.Decoder) error {
	return cluster.db.LoadRDB(dec)
}
