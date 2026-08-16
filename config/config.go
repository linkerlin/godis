package config

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/linkerlin/godis/lib/utils"
)

var (
	ClusterMode    = "cluster"
	StandaloneMode = "standalone"
)

// ServerProperties defines global config properties
type ServerProperties struct {
	// for Public configuration
	RunID             string `cfg:"runid"` // runID always different at every exec.
	Bind              string `cfg:"bind"`
	Port              int    `cfg:"port"`
	Dir               string `cfg:"dir"`
	AnnounceHost      string `cfg:"announce-host"`
	AppendOnly        bool   `cfg:"appendonly"`
	AppendFilename    string `cfg:"appendfilename"`
	AppendFsync       string `cfg:"appendfsync"`
	AofUseRdbPreamble bool   `cfg:"aof-use-rdb-preamble"`
	MaxClients        int    `cfg:"maxclients"`
	RequirePass       string `cfg:"requirepass"`
	Databases         int    `cfg:"databases"`
	RDBFilename       string `cfg:"dbfilename"`
	MasterAuth        string `cfg:"masterauth"`
	MasterUser        string `cfg:"masteruser"`
	SlaveAnnouncePort int    `cfg:"slave-announce-port"`
	SlaveAnnounceIP   string `cfg:"slave-announce-ip"`
	ReplTimeout       int    `cfg:"repl-timeout"`
	// LuaTimeLimit is script busy timeout in milliseconds (Redis lua-time-limit; 0 = no limit).
	LuaTimeLimit     int64  `cfg:"lua-time-limit"`
	UseGnet          bool   `cfg:"use-gnet"`
	SearchBackend    string `cfg:"search-backend"`
	VectorBackend    string `cfg:"vector-backend"`
	SearchSQLitePath string `cfg:"search-sqlite-path"`
	SqliteMmapSize   int64  `cfg:"sqlite-mmap-size"`
	MetricsAddr      string `cfg:"metrics-addr"`
	AclFile          string `cfg:"aclfile"`

	SlowLogSlowerThan int64 `cfg:"slowlog-log-slower-than"`
	SlowLogMaxLen     int   `cfg:"slowlog-max-len"`
	AclLogMaxLen      int   `cfg:"acllog-max-len"`

	Maxmemory       int64  `cfg:"maxmemory"`
	MaxmemoryPolicy string `cfg:"maxmemory-policy"`

	// ReplicaReadOnly mirrors Redis replica-read-only / slave-read-only (default true).
	ReplicaReadOnly bool `cfg:"replica-read-only"`

	// Accepted for Redis conf compatibility (semantics may be partial / noop).
	Timeout              int    `cfg:"timeout"`
	TCPKeepAlive         int    `cfg:"tcp-keepalive"`
	LogLevel             string `cfg:"loglevel"`
	LogFile              string `cfg:"logfile"`
	ProtectedMode        bool   `cfg:"protected-mode"`
	Daemonize            bool   `cfg:"daemonize"`
	PidFile              string `cfg:"pidfile"`
	LazyfreeLazyEviction bool   `cfg:"lazyfree-lazy-eviction"`
	ProtoMaxBulkLen      int64  `cfg:"proto-max-bulk-len"`
	// Save is Redis "save <sec> <changes> ..." as a single CONFIG string (e.g. "3600 1 300 100").
	// Autosave is driven by checkSavePoints (PE-4).
	Save       string `cfg:"save"`
	TCPBacklog int    `cfg:"tcp-backlog"`
	// Hz is Redis server cron frequency (INFO hz / CONFIG hz); Godis uses it as a reported value.
	Hz int `cfg:"hz"`

	// NotifyKeyspaceEvents is Redis-compatible; when non-empty, keyspace/keyevent
	// Pub/Sub notifications are emitted (see database/keyspace_notify.go).
	NotifyKeyspaceEvents string `cfg:"notify-keyspace-events"`

	// ActiveDefrag is a Redis-compatible CONFIG stub (no real defrag).
	ActiveDefrag bool `cfg:"activedefrag"`

	// BusyReplyThreshold is Redis busy-reply-threshold (ms); stub for CONFIG GET/SET.
	BusyReplyThreshold int64 `cfg:"busy-reply-threshold"`

	// DynamicHz is Redis dynamic-hz; stub for CONFIG GET/SET (default yes).
	DynamicHz bool `cfg:"dynamic-hz"`

	// LazyfreeLazyExpire / LazyfreeLazyServerDel / JemallocBgThread are Redis CONFIG stubs.
	LazyfreeLazyExpire    bool `cfg:"lazyfree-lazy-expire"`
	LazyfreeLazyServerDel bool `cfg:"lazyfree-lazy-server-del"`
	JemallocBgThread      bool `cfg:"jemalloc-bg-thread"`
	LazyfreeLazyUserDel   bool `cfg:"lazyfree-lazy-user-del"`
	LazyfreeLazyUserFlush bool `cfg:"lazyfree-lazy-user-flush"`
	ReplicaLazyFlush      bool `cfg:"replica-lazy-flush"`
	AofLoadTruncated      bool `cfg:"aof-load-truncated"`
	// RdbDelSyncFiles / AofDisableAutoGC / AppendDirname are Redis CONFIG stubs.
	RdbDelSyncFiles  bool   `cfg:"rdb-del-sync-files"`
	AofDisableAutoGC bool   `cfg:"aof-disable-auto-gc"`
	AppendDirname    string `cfg:"appenddirname"`

	// ActiveRehashing / SanitizeDumpPayload / IgnoreWarnings are Redis CONFIG stubs.
	ActiveRehashing     bool   `cfg:"activerehashing"`
	SanitizeDumpPayload bool   `cfg:"sanitize-dump-payload"`
	IgnoreWarnings      string `cfg:"ignore-warnings"`

	// ReplicaAnnounced / SetProcTitle / AlwaysShowLogo / LuaReplicateCommands are Redis CONFIG stubs.
	ReplicaAnnounced      bool `cfg:"replica-announced"`
	SetProcTitle          bool `cfg:"set-proc-title"`
	AlwaysShowLogo        bool `cfg:"always-show-logo"`
	LuaReplicateCommands  bool `cfg:"lua-replicate-commands"`

	// ClientQueryBufferLimit / ClientOutputBufferLimit / MinReplicas* / ClusterRequireFullCoverage are Redis CONFIG stubs.
	ClientQueryBufferLimit     int64  `cfg:"client-query-buffer-limit"`
	ClientOutputBufferLimit    string `cfg:"client-output-buffer-limit"`
	MinReplicasToWrite         int    `cfg:"min-replicas-to-write"`
	MinReplicasMaxLag          int    `cfg:"min-replicas-max-lag"`
	ClusterRequireFullCoverage bool   `cfg:"cluster-require-full-coverage"`
	ClusterNodeTimeout         int64  `cfg:"cluster-node-timeout"`
	ClusterMigrationBarrier    int    `cfg:"cluster-migration-barrier"`
	ClusterAllowReadsWhenDown  bool   `cfg:"cluster-allow-reads-when-down"`

	// Persistence / AOF rewrite CONFIG stubs (GET/SET only).
	StopWritesOnBgsaveError  bool  `cfg:"stop-writes-on-bgsave-error"`
	RDBCompression           bool  `cfg:"rdbcompression"`
	RDBChecksum              bool  `cfg:"rdbchecksum"`
	NoAppendFsyncOnRewrite   bool  `cfg:"no-appendfsync-on-rewrite"`
	AutoAofRewritePercentage int   `cfg:"auto-aof-rewrite-percentage"`
	AutoAofRewriteMinSize    int64 `cfg:"auto-aof-rewrite-min-size"`

	// IO / replication / eviction sample CONFIG stubs (GET/SET only).
	IOThreads             int   `cfg:"io-threads"`
	IOThreadsDoReads      bool  `cfg:"io-threads-do-reads"`
	ReplDisklessSync      bool  `cfg:"repl-diskless-sync"`
	ReplDisklessSyncDelay int   `cfg:"repl-diskless-sync-delay"`
	MaxmemorySamples      int   `cfg:"maxmemory-samples"`
	TrackingTableMaxKeys  int64 `cfg:"tracking-table-max-keys"`

	// Replication / AOF / hash encoding CONFIG stubs (GET/SET only).
	ReplBacklogTTL               int  `cfg:"repl-backlog-ttl"`
	ReplicaIgnoreMaxmemory       bool `cfg:"replica-ignore-maxmemory"`
	AofRewriteIncrementalFsync   bool `cfg:"aof-rewrite-incremental-fsync"`
	RdbSaveIncrementalFsync      bool `cfg:"rdb-save-incremental-fsync"`
	ClusterAllowReplicaMigration bool `cfg:"cluster-allow-replica-migration"`
	ClusterReplicaValidityFactor int  `cfg:"cluster-replica-validity-factor"`
	HashMaxListpackEntries       int  `cfg:"hash-max-listpack-entries"`

	// Latency / ACL pubsub / replica ping CONFIG stubs (GET/SET only).
	LatencyMonitorThreshold int64  `cfg:"latency-monitor-threshold"`
	AclPubsubDefault        string `cfg:"acl-pubsub-default"`
	ReplPingReplicaPeriod   int    `cfg:"repl-ping-replica-period"`

	// Eviction / list / AOF / crash / latency / cluster CONFIG stubs (GET/SET only).
	LFULogFactor                  int    `cfg:"lfu-log-factor"`
	LFUDecayTime                  int    `cfg:"lfu-decay-time"`
	MaxmemoryEvictionTenacity     int    `cfg:"maxmemory-eviction-tenacity"`
	ListCompressDepth             int    `cfg:"list-compress-depth"`
	AofTimestampEnabled           bool   `cfg:"aof-timestamp-enabled"`
	ReplDisableTCPNodelay         bool   `cfg:"repl-disable-tcp-nodelay"`
	LatencyTracking               bool   `cfg:"latency-tracking"`
	CrashLogEnabled               bool   `cfg:"crash-log-enabled"`
	CrashMemcheckEnabled          bool   `cfg:"crash-memcheck-enabled"`
	ReplDisklessLoad              string `cfg:"repl-diskless-load"`
	ClusterPreferredEndpointType  string `cfg:"cluster-preferred-endpoint-type"`
	ClusterLinkSendbufLimit       int64  `cfg:"cluster-link-sendbuf-limit"`
	ClusterAnnounceHostname       string `cfg:"cluster-announce-hostname"`
	ClusterAnnounceHumanNodename  string `cfg:"cluster-announce-human-nodename"`

	// Shutdown / locale / latency percentiles / active-defrag CONFIG stubs (GET/SET only).
	ShutdownTimeout                  int    `cfg:"shutdown-timeout"`
	ShutdownOnSigint                 string `cfg:"shutdown-on-sigint"`
	ShutdownOnSigterm                string `cfg:"shutdown-on-sigterm"`
	LocaleCollate                    string `cfg:"locale-collate"`
	LatencyTrackingInfoPercentiles   string `cfg:"latency-tracking-info-percentiles"`
	ActiveDefragIgnoreBytes          int64  `cfg:"active-defrag-ignore-bytes"`
	ActiveDefragThresholdLower       int    `cfg:"active-defrag-threshold-lower"`
	ActiveDefragThresholdUpper       int    `cfg:"active-defrag-threshold-upper"`
	ActiveDefragCycleMin             int    `cfg:"active-defrag-cycle-min"`
	ActiveDefragCycleMax             int    `cfg:"active-defrag-cycle-max"`
	ActiveDefragMaxScanFields        int64  `cfg:"active-defrag-max-scan-fields"`

	// OOM / propagation / cluster / process title CONFIG stubs (GET/SET only).
	OOMScoreAdjValues               string `cfg:"oom-score-adj-values"`
	PropagationErrorBehavior        string `cfg:"propagation-error-behavior"`
	HideUserDataFromLog             bool   `cfg:"hide-user-data-from-log"`
	ClusterReplicaNoFailover        bool   `cfg:"cluster-replica-no-failover"`
	ClusterAllowPubsubshardWhenDown bool   `cfg:"cluster-allow-pubsubshard-when-down"`
	ProcTitleTemplate               string `cfg:"proc-title-template"`

	// Active expire / TLS CONFIG stubs (GET/SET only; no real TLS stack).
	ActiveExpireEffort         int    `cfg:"active-expire-effort"`
	TLSCertFile                string `cfg:"tls-cert-file"`
	TLSKeyFile                 string `cfg:"tls-key-file"`
	TLSCACertFile              string `cfg:"tls-ca-cert-file"`
	TLSProtocols               string `cfg:"tls-protocols"`
	TLSCiphers                 string `cfg:"tls-ciphers"`
	TLSAuthClients             string `cfg:"tls-auth-clients"`
	TLSReplication             bool   `cfg:"tls-replication"`
	TLSCluster                 bool   `cfg:"tls-cluster"`
	TLSSessionCaching          bool   `cfg:"tls-session-caching"`
	TLSSessionCacheSize        int    `cfg:"tls-session-cache-size"`
	TLSSessionCacheTimeout     int    `cfg:"tls-session-cache-timeout"`
	TLSPreferServerCiphers     bool   `cfg:"tls-prefer-server-ciphers"`
	ClusterAnnounceTLSPort     int    `cfg:"cluster-announce-tls-port"`
	TLSPort                    int    `cfg:"tls-port"`
	TLSDhParamsFile            string `cfg:"tls-dh-params-file"`
	TLSCiphersuites            string `cfg:"tls-ciphersuites"`
	TLSClientCertFile          string `cfg:"tls-client-cert-file"`
	TLSClientKeyFile           string `cfg:"tls-client-key-file"`
	TLSKeyFilePass             string `cfg:"tls-key-file-pass"`
	TLSClientKeyFilePass       string `cfg:"tls-client-key-file-pass"`
	MaxmemoryClients           string `cfg:"maxmemory-clients"`

	// Encoding / structure size CONFIG stubs (GET/SET only).
	ListMaxListpackSize    int   `cfg:"list-max-listpack-size"`
	SetMaxIntsetEntries    int   `cfg:"set-max-intset-entries"`
	ZSetMaxListpackEntries int   `cfg:"zset-max-listpack-entries"`
	ZSetMaxListpackValue   int   `cfg:"zset-max-listpack-value"`
	StreamNodeMaxBytes     int64 `cfg:"stream-node-max-bytes"`
	HLLSparseMaxBytes      int   `cfg:"hll-sparse-max-bytes"`

	// Cluster announce / additional encoding / OOM CONFIG stubs (GET/SET only).
	ClusterAnnounceIP      string `cfg:"cluster-announce-ip"`
	ClusterAnnouncePort    int    `cfg:"cluster-announce-port"`
	ClusterAnnounceBusPort int    `cfg:"cluster-announce-bus-port"`
	StreamNodeMaxEntries   int64  `cfg:"stream-node-max-entries"`
	HashMaxListpackValue   int    `cfg:"hash-max-listpack-value"`
	SetMaxListpackEntries  int    `cfg:"set-max-listpack-entries"`
	SetMaxListpackValue    int    `cfg:"set-max-listpack-value"`
	// OOMScoreAdj is Redis CONFIG enum: no|yes|relative|absolute (relative stored/reported as yes).
	OOMScoreAdj string `cfg:"oom-score-adj"`

	// ReplicaOf is Redis CONFIG replicaof/slaveof string ("host port" or empty when master).
	ReplicaOf string `cfg:"replicaof"`
	// ReplicaServeStaleData / ReplicaPriority are Redis CONFIG stubs.
	ReplicaServeStaleData bool `cfg:"replica-serve-stale-data"`
	ReplicaPriority       int  `cfg:"replica-priority"`

	// ReplBacklogSize is Redis repl-backlog-size (bytes); used when allocating master backlog.
	ReplBacklogSize int64 `cfg:"repl-backlog-size"`

	ClusterEnable     bool   `cfg:"cluster-enable"`
	ClusterAsSeed     bool   `cfg:"cluster-as-seed"`
	ClusterSeed       string `cfg:"cluster-seed"`
	RaftListenAddr    string `cfg:"raft-listen-address"`
	RaftAdvertiseAddr string `cfg:"raft-advertise-address"`
	// If the node join the cluster as a replica of another node,
	// set MasterInCluster as the RedisAdvertiseAddr of it's master node
	MasterInCluster string `cfg:"master-in-cluster"`
}

var configFilePath string

func GetConfigFilePath() string {
	return configFilePath
}

// SetConfigFilePath sets the path used by CONFIG REWRITE (mainly for tests).
func SetConfigFilePath(path string) {
	configFilePath = path
}

type ServerInfo struct {
	StartUpTime time.Time
}

func (p *ServerProperties) AnnounceAddress() string {
	if p.AnnounceHost != "" {
		return p.AnnounceHost + ":" + strconv.Itoa(p.Port)
	}
	return p.Bind + ":" + strconv.Itoa(p.Port)
}

func (p *ServerProperties) RaftAnnounceAddress() string {
	if p.RaftAdvertiseAddr != "" {
		return p.RaftAdvertiseAddr
	}
	return p.RaftListenAddr
}

// Properties holds global config properties
var Properties *ServerProperties
var EachTimeServerInfo *ServerInfo

func init() {
	// A few stats we don't want to reset: server startup time, and peak mem.
	EachTimeServerInfo = &ServerInfo{
		StartUpTime: time.Now(),
	}

	// default config
	Properties = &ServerProperties{
		Bind:            "127.0.0.1",
		Port:            6379,
		AppendOnly:      false,
		SearchBackend:   "native",
		VectorBackend:   "native",
		Hz:              10,
		TCPKeepAlive:    300,
		ProtectedMode:   true,
		ReplicaReadOnly: true,
		DynamicHz:            true,
		ActiveRehashing:      true,
		ReplicaAnnounced:     true,
		SetProcTitle:         true,
		LuaReplicateCommands: true,
		ClientQueryBufferLimit:  1073741824,
		ClientOutputBufferLimit: "normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60",
		MinReplicasMaxLag:          10,
		ClusterRequireFullCoverage: true,
		ClusterNodeTimeout:         15000,
		ClusterMigrationBarrier:    1,
		StopWritesOnBgsaveError:  true,
		RDBCompression:           true,
		RDBChecksum:              true,
		AutoAofRewritePercentage: 100,
		AutoAofRewriteMinSize:    67108864,
		IOThreads:                1,
		ReplDisklessSyncDelay:    5,
		MaxmemorySamples:         5,
		TrackingTableMaxKeys:     1000000,
		ReplBacklogTTL:               3600,
		ReplicaIgnoreMaxmemory:       true,
		AofRewriteIncrementalFsync:   true,
		RdbSaveIncrementalFsync:      true,
		RdbDelSyncFiles:              false,
		AofDisableAutoGC:             false,
		AppendDirname:                "appendonlydir",
		ClusterAllowReplicaMigration: true,
		ClusterReplicaValidityFactor: 10,
		HashMaxListpackEntries:       512,
		LatencyMonitorThreshold:      0,
		AclPubsubDefault:             "resetchannels",
		ReplPingReplicaPeriod:        10,
		LFULogFactor:                 10,
		LFUDecayTime:                 1,
		MaxmemoryEvictionTenacity:    10,
		ListCompressDepth:            0,
		AofTimestampEnabled:          false,
		ReplDisableTCPNodelay:        false,
		LatencyTracking:              true,
		CrashLogEnabled:              true,
		CrashMemcheckEnabled:         true,
		ReplDisklessLoad:             "disabled",
		ClusterPreferredEndpointType: "ip",
		ClusterLinkSendbufLimit:      0,
		ClusterAnnounceHostname:      "",
		ClusterAnnounceHumanNodename: "",
		ShutdownTimeout:              0,
		ShutdownOnSigint:             "default",
		ShutdownOnSigterm:            "default",
		LocaleCollate:                "",
		LatencyTrackingInfoPercentiles: "50 99 99.9",
		ActiveDefragIgnoreBytes:        104857600,
		ActiveDefragThresholdLower:     10,
		ActiveDefragThresholdUpper:     100,
		ActiveDefragCycleMin:           1,
		ActiveDefragCycleMax:           25,
		ActiveDefragMaxScanFields:      1000,
		OOMScoreAdjValues:              "0 200 800",
		OOMScoreAdj:                    "no",
		PropagationErrorBehavior:       "ignore",
		HideUserDataFromLog:            false,
		ClusterReplicaNoFailover:       false,
		ClusterAllowPubsubshardWhenDown: false,
		ProcTitleTemplate:              "{title} {listen-addr} {server-mode}",
		ActiveExpireEffort:             1,
		TLSAuthClients:                 "no",
		TLSReplication:                 false,
		TLSCluster:                     false,
		TLSSessionCaching:              true,
		TLSSessionCacheSize:            0,
		TLSSessionCacheTimeout:         300,
		TLSPreferServerCiphers:         false,
		ClusterAnnounceTLSPort:         0,
		TLSPort:                        0,
		MaxmemoryClients:               "0",
		ListMaxListpackSize:    -2,
		SetMaxIntsetEntries:    512,
		ZSetMaxListpackEntries: 128,
		ZSetMaxListpackValue:   64,
		StreamNodeMaxBytes:     4096,
		HLLSparseMaxBytes:      3000,
		StreamNodeMaxEntries:  100,
		HashMaxListpackValue:  64,
		SetMaxListpackEntries: 128,
		SetMaxListpackValue:   64,
		ReplicaServeStaleData:      true,
		ReplicaPriority:            100,
		LuaTimeLimit:               5000,
		RunID:           utils.RandString(40),
	}
}

func parse(src io.Reader) (*ServerProperties, error) {
	config := &ServerProperties{}

	// read config file
	rawMap := make(map[string]string)
	scanner := bufio.NewScanner(src)
	for scanner.Scan() {
		line := scanner.Text()
		key, value, ok := splitConfigDirective(line)
		if !ok {
			continue
		}
		rawMap[strings.ToLower(key)] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.Wrap(err, "scan config file failed")
	}

	// parse format
	t := reflect.TypeOf(config)
	v := reflect.ValueOf(config)
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldVal := v.Elem().Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok || strings.TrimLeft(key, " ") == "" {
			key = field.Name
		}
		value, ok := rawMap[strings.ToLower(key)]
		if ok {
			// fill config
			switch field.Type.Kind() {
			case reflect.String:
				fieldVal.SetString(value)
			case reflect.Int:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err == nil {
					fieldVal.SetInt(intValue)
				}
			case reflect.Int64:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err == nil {
					fieldVal.SetInt(intValue)
				}
			case reflect.Bool:
				ok, boolValue := ParseConfigBool(value)
				if ok {
					fieldVal.SetBool(boolValue)
				}
			case reflect.Slice:
				if field.Type.Elem().Kind() == reflect.String {
					slice := strings.Split(value, ",")
					fieldVal.Set(reflect.ValueOf(slice))
				}
			}
		}
	}
	// replica-read-only defaults to yes; slave-read-only is an alias.
	if v, ok := rawMap["slave-read-only"]; ok {
		if _, has := rawMap["replica-read-only"]; !has {
			if okb, b := ParseConfigBool(v); okb {
				config.ReplicaReadOnly = b
			}
		}
	}
	if _, ok := rawMap["replica-read-only"]; !ok {
		if _, ok2 := rawMap["slave-read-only"]; !ok2 {
			config.ReplicaReadOnly = true
		}
	}
	return config, nil
}

// SetupConfig read config file and store properties into Properties
func SetupConfig(configFilename string) error {
	file, err := os.Open(configFilename)
	if err != nil {
		return errors.Wrap(err, "open config file failed")
	}
	defer file.Close()

	config, err := parse(file)
	if err != nil {
		return errors.Wrap(err, "parse config file failed")
	}

	Properties = config
	Properties.RunID = utils.RandString(40)
	configFilePath, err = filepath.Abs(configFilename)
	if err != nil {
		return errors.Wrap(err, "get config file absolute path failed")
	}
	if Properties.Dir == "" {
		Properties.Dir = "."
	}
	return nil
}

func GetTmpDir() string {
	return Properties.Dir + "/tmp"
}

// splitConfigDirective splits a redis.conf line into key and value.
// Supports multi-space separators and quoted values (spaces inside quotes).
func splitConfigDirective(line string) (key, value string, ok bool) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", "", false
	}
	i := 0
	for i < len(line) && line[i] != ' ' && line[i] != '\t' {
		i++
	}
	if i == 0 || i >= len(line) {
		return "", "", false
	}
	key = line[:i]
	rest := strings.TrimSpace(line[i:])
	if rest == "" {
		return "", "", false
	}
	return key, unquoteConfigValue(rest), true
}

func unquoteConfigValue(s string) string {
	if len(s) < 2 {
		return s
	}
	quote := s[0]
	if quote != '"' && quote != '\'' {
		return s
	}
	var b strings.Builder
	escaped := false
	for i := 1; i < len(s); i++ {
		ch := s[i]
		if escaped {
			b.WriteByte(ch)
			escaped = false
			continue
		}
		if ch == '\\' {
			escaped = true
			continue
		}
		if ch == quote {
			return b.String()
		}
		b.WriteByte(ch)
	}
	// Unclosed quote: treat as literal (best-effort).
	return s
}

// WritePidFile writes the current process ID to path.
// Empty path is a no-op (Redis empty pidfile).
func WritePidFile(path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())+"\n"), 0644)
}

// ParseConfigBool parses Redis-style config booleans.
// Accepts yes/on/true/1 and no/off/false/0 (case-insensitive).
func ParseConfigBool(value string) (ok bool, result bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "yes", "on", "true", "1":
		return true, true
	case "no", "off", "false", "0":
		return true, false
	default:
		return false, false
	}
}
