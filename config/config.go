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
	SlaveAnnouncePort int    `cfg:"slave-announce-port"`
	SlaveAnnounceIP   string `cfg:"slave-announce-ip"`
	ReplTimeout       int    `cfg:"repl-timeout"`
	UseGnet           bool   `cfg:"use-gnet"`
	SearchBackend     string `cfg:"search-backend"`
	VectorBackend     string `cfg:"vector-backend"`
	SearchSQLitePath  string `cfg:"search-sqlite-path"`
	SqliteMmapSize    int64  `cfg:"sqlite-mmap-size"`
	MetricsAddr       string `cfg:"metrics-addr"`
	AclFile           string `cfg:"aclfile"`

	SlowLogSlowerThan int64 `cfg:"slowlog-log-slower-than"`
	SlowLogMaxLen     int   `cfg:"slowlog-max-len"`
	AclLogMaxLen      int   `cfg:"acllog-max-len"`

	Maxmemory       int64  `cfg:"maxmemory"`
	MaxmemoryPolicy string `cfg:"maxmemory-policy"`

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
		Bind:          "127.0.0.1",
		Port:          6379,
		AppendOnly:    false,
		SearchBackend: "native",
		VectorBackend: "native",
		Hz:            10,
		RunID:         utils.RandString(40),
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
