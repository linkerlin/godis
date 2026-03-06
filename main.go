package main

import (
	"fmt"
	"os"

	"github.com/cockroachdb/errors"

	"github.com/hdt3213/godis/cluster"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/database"
	idatabase "github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/server/gnet"
	stdserver "github.com/hdt3213/godis/redis/server/std"
)

var banner = `
   ______          ___
  / ____/___  ____/ (_)____
 / / __/ __ \/ __  / / ___/
/ /_/ / /_/ / /_/ / (__  )
\____/\____/\__,_/_/____/
`

var defaultProperties = &config.ServerProperties{
	Bind:           "0.0.0.0",
	Port:           6399,
	AppendOnly:     false,
	AppendFilename: "",
	MaxClients:     1000,
	RunID:          utils.RandString(40),
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	print(banner)
	if err := logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	}); err != nil {
		fmt.Fprintf(os.Stderr, "setup logger failed: %v\n", err)
	}
	
	if err := setupConfig(); err != nil {
		logger.Fatalf("setup config failed: %+v", err)
	}
	
	listenAddr := fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port)
	
	var err error
	if config.Properties.UseGnet {
		err = runGnetServer(listenAddr)
	} else {
		err = runStdServer(listenAddr)
	}
	if err != nil {
		logger.Fatalf("start server failed: %+v", err)
	}
}

func setupConfig() error {
	configFilename := os.Getenv("CONFIG")
	if configFilename == "" {
		if fileExists("redis.conf") {
			if err := config.SetupConfig("redis.conf"); err != nil {
				return errors.Wrap(err, "setup config from redis.conf failed")
			}
		} else {
			config.Properties = defaultProperties
		}
	} else {
		if err := config.SetupConfig(configFilename); err != nil {
			return errors.Wrapf(err, "setup config from %s failed", configFilename)
		}
	}
	return nil
}

func runGnetServer(listenAddr string) error {
	var db idatabase.DB
	var err error
	if config.Properties.ClusterEnable {
		db, err = cluster.MakeCluster()
		if err != nil {
			return errors.Wrap(err, "create cluster failed")
		}
	} else {
		db, err = database.NewStandaloneServer()
		if err != nil {
			return errors.Wrap(err, "create standalone server failed")
		}
	}
	server := gnet.NewGnetServer(db)
	if err := server.Run(listenAddr); err != nil {
		return errors.Wrap(err, "run gnet server failed")
	}
	return nil
}

func runStdServer(listenAddr string) error {
	handler, err := stdserver.MakeHandler()
	if err != nil {
		return errors.Wrap(err, "create handler failed")
	}
	if err := stdserver.Serve(listenAddr, handler); err != nil {
		return errors.Wrap(err, "serve failed")
	}
	return nil
}
