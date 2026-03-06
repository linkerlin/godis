// Package cluster provides a server side cluster which is transparent to client. You can connect to any node in the cluster to access all data in the cluster
package cluster

import (
	"os"
	"path"

	"github.com/cockroachdb/errors"

	_ "github.com/hdt3213/godis/cluster/commands" // register commands
	"github.com/hdt3213/godis/cluster/core"
	"github.com/hdt3213/godis/cluster/raft"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/lib/logger"
)

type Cluster = core.Cluster

// MakeCluster creates and starts a node of cluster
func MakeCluster() (*Cluster, error) {
	raftPath := path.Join(config.Properties.Dir, "raft")
	err := os.MkdirAll(raftPath, os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "create raft directory failed")
	}
	cluster, err := core.NewCluster(&core.Config{
		RaftConfig: raft.RaftConfig{
			RedisAdvertiseAddr: config.Properties.AnnounceAddress(),
			RaftListenAddr:     config.Properties.RaftListenAddr,
			RaftAdvertiseAddr:  config.Properties.RaftAnnounceAddress(),
			Dir:                raftPath,
		},
		StartAsSeed: config.Properties.ClusterAsSeed,
		JoinAddress: config.Properties.ClusterSeed,
		Master:      config.Properties.MasterInCluster,
	})
	if err != nil {
		return nil, errors.Wrap(err, "create cluster failed")
	}
	return cluster, nil
}

// MustMakeCluster creates cluster or fatals on error
func MustMakeCluster() *Cluster {
	cluster, err := MakeCluster()
	if err != nil {
		logger.Fatalf("make cluster failed: %+v", err)
	}
	return cluster
}
