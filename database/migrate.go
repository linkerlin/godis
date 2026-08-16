package database

import (
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/parser"
	"github.com/linkerlin/godis/redis/protocol"
)

// migrateOpts holds parsed MIGRATE options.
type migrateOpts struct {
	host     string
	port     int
	destDB   int
	timeout  time.Duration
	copy     bool
	replace  bool
	authPass string
	authUser string // non-empty => AUTH2 / ACL AUTH
	keys     []string
}

// parseMigrateArgs parses:
//
//	host port key|"" destination-db timeout [COPY] [REPLACE] [AUTH password]
//	  [AUTH2 username password] [KEYS key [key ...]]
func parseMigrateArgs(args [][]byte) (*migrateOpts, redis.Reply) {
	if len(args) < 5 {
		return nil, protocol.MakeArgNumErrReply("migrate")
	}

	opts := &migrateOpts{
		host: string(args[0]),
	}
	port, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return nil, protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	opts.port = port

	keyArg := string(args[2])

	destDB, err := strconv.Atoi(string(args[3]))
	if err != nil || destDB < 0 {
		return nil, protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	opts.destDB = destDB

	timeoutMs, err := strconv.ParseInt(string(args[4]), 10, 64)
	if err != nil || timeoutMs < 0 {
		return nil, protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	opts.timeout = time.Duration(timeoutMs) * time.Millisecond

	hasKEYS := false
	for i := 5; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "COPY":
			opts.copy = true
		case "REPLACE":
			opts.replace = true
		case "AUTH":
			if i+1 >= len(args) {
				return nil, protocol.MakeSyntaxErrReply()
			}
			opts.authPass = string(args[i+1])
			opts.authUser = ""
			i++
		case "AUTH2":
			if i+2 >= len(args) {
				return nil, protocol.MakeSyntaxErrReply()
			}
			opts.authUser = string(args[i+1])
			opts.authPass = string(args[i+2])
			i += 2
		case "KEYS":
			hasKEYS = true
			if i+1 >= len(args) {
				return nil, protocol.MakeErrReply("ERR There must be at least one key to MIGRATE")
			}
			for j := i + 1; j < len(args); j++ {
				opts.keys = append(opts.keys, string(args[j]))
			}
			i = len(args)
		default:
			return nil, protocol.MakeSyntaxErrReply()
		}
	}

	if hasKEYS {
		if keyArg != "" {
			return nil, protocol.MakeErrReply("ERR When using MIGRATE KEYS option, the key argument must be set to the empty string")
		}
		if len(opts.keys) == 0 {
			return nil, protocol.MakeErrReply("ERR There must be at least one key to MIGRATE")
		}
	} else {
		if keyArg == "" {
			// Redis: empty key without KEYS → NOKEY (no keys to migrate).
			opts.keys = nil
		} else {
			opts.keys = []string{keyArg}
		}
	}
	return opts, nil
}

func prepareMigrate(args [][]byte) ([]string, []string) {
	opts, errReply := parseMigrateArgs(args)
	if errReply != nil {
		return nil, nil
	}
	return opts.keys, nil
}

func undoMigrate(db *DB, args [][]byte) []CmdLine {
	opts, errReply := parseMigrateArgs(args)
	if errReply != nil {
		return nil
	}
	return rollbackGivenKeys(db, opts.keys...)
}

// execMigrate implements Redis MIGRATE (client-style DUMP→RESTORE→DEL).
func execMigrate(db *DB, args [][]byte) redis.Reply {
	opts, errReply := parseMigrateArgs(args)
	if errReply != nil {
		return errReply
	}

	type payload struct {
		key  string
		dump []byte
		ttl  int64
	}
	var items []payload
	for _, key := range opts.keys {
		dumpReply := execDump(db, [][]byte{[]byte(key)})
		if _, isNull := dumpReply.(*protocol.NullBulkReply); isNull {
			continue
		}
		bulk, ok := dumpReply.(*protocol.BulkReply)
		if !ok {
			if protocol.IsErrorReply(dumpReply) {
				return dumpReply
			}
			return protocol.MakeErrReply("ERR DUMP failed")
		}
		items = append(items, payload{
			key:  key,
			dump: bulk.Arg,
			ttl:  migrateKeyTTL(db, key),
		})
	}
	if len(items) == 0 {
		return protocol.MakeStatusReply("NOKEY")
	}

	client, err := dialMigrateTarget(opts.host, opts.port, opts.timeout)
	if err != nil {
		return migrateIOErr()
	}
	defer client.close()

	if opts.authPass != "" {
		var authArgs [][]byte
		if opts.authUser != "" {
			authArgs = utils.ToCmdLine("AUTH", opts.authUser, opts.authPass)
		} else {
			authArgs = utils.ToCmdLine("AUTH", opts.authPass)
		}
		if reply := client.call(authArgs); protocol.IsErrorReply(reply) {
			return wrapTargetErr(reply)
		}
	}

	selReply := client.call(utils.ToCmdLine("SELECT", strconv.Itoa(opts.destDB)))
	if protocol.IsErrorReply(selReply) {
		return wrapTargetErr(selReply)
	}

	for _, item := range items {
		restoreArgs := [][]byte{
			[]byte("RESTORE"),
			[]byte(item.key),
			[]byte(strconv.FormatInt(item.ttl, 10)),
			item.dump,
		}
		if opts.replace {
			restoreArgs = append(restoreArgs, []byte("REPLACE"))
		}
		reply := client.call(restoreArgs)
		if protocol.IsErrorReply(reply) {
			return wrapTargetErr(reply)
		}
		if !opts.copy {
			_ = execDel(db, [][]byte{[]byte(item.key)})
		}
	}
	return protocol.MakeOkReply()
}

func migrateKeyTTL(db *DB, key string) int64 {
	raw, ok := db.ttlMap.Get(key)
	if !ok {
		return 0
	}
	expireTime, _ := raw.(time.Time)
	ttl := expireTime.Sub(time.Now()).Milliseconds()
	if ttl < 1 {
		return 1
	}
	return ttl
}

func migrateIOErr() redis.Reply {
	return protocol.MakeErrReply("IOERR error or timeout writing to target instance")
}

func wrapTargetErr(reply redis.Reply) redis.Reply {
	if er, ok := reply.(protocol.ErrorReply); ok {
		return protocol.MakeErrReply("ERR Target instance replied with error: " + er.Error())
	}
	msg := strings.TrimPrefix(string(reply.ToBytes()), "-")
	msg = strings.TrimSuffix(msg, "\r\n")
	return protocol.MakeErrReply("ERR Target instance replied with error: " + msg)
}

// migrateClient is a synchronous RESP client with per-call deadlines.
type migrateClient struct {
	conn    net.Conn
	ch      <-chan *parser.Payload
	timeout time.Duration
}

func dialMigrateTarget(host string, port int, timeout time.Duration) (*migrateClient, error) {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	dialer := net.Dialer{}
	if timeout > 0 {
		dialer.Timeout = timeout
	}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	if timeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(timeout))
	}
	return &migrateClient{
		conn:    conn,
		ch:      parser.ParseStream(conn),
		timeout: timeout,
	}, nil
}

func (c *migrateClient) close() {
	_ = c.conn.Close()
}

func (c *migrateClient) call(args [][]byte) redis.Reply {
	if c.timeout > 0 {
		_ = c.conn.SetDeadline(time.Now().Add(c.timeout))
	}
	if _, err := c.conn.Write(protocol.MakeMultiBulkReply(args).ToBytes()); err != nil {
		return migrateIOErr()
	}

	var payload *parser.Payload
	if c.timeout > 0 {
		timer := time.NewTimer(c.timeout)
		select {
		case payload = <-c.ch:
			timer.Stop()
		case <-timer.C:
			return migrateIOErr()
		}
	} else {
		payload = <-c.ch
	}
	if payload == nil {
		return migrateIOErr()
	}
	if payload.Err != nil {
		return migrateIOErr()
	}
	if payload.Data == nil {
		return migrateIOErr()
	}
	return payload.Data
}

func init() {
	registerCommand("Migrate", execMigrate, prepareMigrate, undoMigrate, -6, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 3, 3, 1)
}
