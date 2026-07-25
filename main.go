package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/cockroachdb/errors"

	"github.com/linkerlin/godis/cluster"
	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/database"
	idatabase "github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/lib/logger"
	"github.com/linkerlin/godis/lib/metrics"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/server/gnet"
	stdserver "github.com/linkerlin/godis/redis/server/std"
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

// Command line flags
var (
	flagBind   string
	flagPort   int
	flagConfig string
)

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func init() {
	flag.StringVar(&flagBind, "bind", "", "Server bind address (overrides config file)")
	flag.StringVar(&flagBind, "b", "", "Server bind address shorthand")
	flag.IntVar(&flagPort, "port", 0, "Server port (overrides config file)")
	flag.IntVar(&flagPort, "p", 0, "Server port shorthand")
	flag.StringVar(&flagConfig, "config", "", "Config file path")
	flag.StringVar(&flagConfig, "c", "", "Config file path shorthand")
}

func main() {
	flag.Parse()

	print(banner)
	// Boot with stdout; after config, applyLogFileFromConfig may switch to logfile.
	if err := logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	}); err != nil {
		fmt.Fprintf(os.Stderr, "setup logger failed: %v\n", err)
	}

	if err := setupConfig(); err != nil {
		logger.Fatal(fmt.Sprintf("setup config failed: %+v", err))
	}
	applyLogLevelFromConfig()
	applyLogFileFromConfig()
	applyPidFileFromConfig()

	startMetricsServer()

	listenAddr := fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port)

	var err error
	if config.Properties.UseGnet {
		err = runGnetServer(listenAddr)
	} else {
		err = runStdServer(listenAddr)
	}
	if err != nil {
		logger.Fatal(fmt.Sprintf("start server failed: %+v", err))
	}
}

func applyLogLevelFromConfig() {
	if config.Properties == nil {
		return
	}
	lvl := config.Properties.LogLevel
	if lvl == "" {
		lvl = "notice"
	}
	if lv, ok := logger.ParseRedisLogLevel(lvl); ok {
		logger.SetMinLevel(lv)
	}
}

func applyLogFileFromConfig() {
	if config.Properties == nil {
		return
	}
	// Empty logfile → stdout only (Redis default). Non-empty → that path.
	if err := logger.ReconfigureOutput(config.Properties.LogFile); err != nil {
		fmt.Fprintf(os.Stderr, "apply logfile %q failed: %v\n", config.Properties.LogFile, err)
	}
}

func applyPidFileFromConfig() {
	if config.Properties == nil {
		return
	}
	if err := config.WritePidFile(config.Properties.PidFile); err != nil {
		fmt.Fprintf(os.Stderr, "write pidfile %q failed: %v\n", config.Properties.PidFile, err)
	}
}

func setupConfig() error {
	// Priority: command line flag > environment variable > default
	configFilename := flagConfig
	if configFilename == "" {
		configFilename = os.Getenv("CONFIG")
	}

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

	// Apply command line overrides for bind and port
	if flagBind != "" {
		config.Properties.Bind = flagBind
	}
	if flagPort != 0 {
		config.Properties.Port = flagPort
	}

	return nil
}

func startMetricsServer() {
	addr := config.Properties.MetricsAddr
	if addr == "" {
		return
	}
	go func() {
		logger.Info(fmt.Sprintf("metrics listening on %s/metrics", addr))
		if err := metrics.Start(addr); err != nil {
			logger.Errorf("metrics server failed: %+v", err)
		}
	}()
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

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Run(listenAddr)
	}()

	select {
	case sig := <-sigCh:
		logger.Info(fmt.Sprintf("received signal %v, shutting down gnet server...", sig))
		if err := server.Close(); err != nil {
			return errors.Wrap(err, "close gnet server failed")
		}
		return nil
	case err := <-errCh:
		if err != nil {
			return errors.Wrap(err, "run gnet server failed")
		}
		return nil
	}
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
