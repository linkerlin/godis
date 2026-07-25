package connection

import (
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linkerlin/godis/lib/logger"
	"github.com/linkerlin/godis/lib/sync/wait"
)

var globalClientID atomic.Uint64

const (
	// flagSlave means this a connection with slave
	flagSlave = uint64(1 << iota)
	// flagSlave means this a connection with master
	flagMaster
	// flagMulti means this connection is within a transaction
	flagMulti
)

// Connection represents a connection with a redis-cli
type Connection struct {
	conn net.Conn

	// wait until finish sending data, used for graceful shutdown
	sendingData wait.Wait

	// lock while server sending response
	mu    sync.Mutex
	flags uint64

	// subscribing channels
	subs map[string]bool
	// pattern subscriptions (PSUBSCRIBE)
	psubs map[string]bool
	// sharded channel subscriptions (SSUBSCRIBE)
	ssubs map[string]bool

	// password may be changed by CONFIG command during runtime, so store the password
	password string

	// ACL user bound to this connection (empty means "default")
	aclUser string
	// aclAuthed is true after successful AUTH / HELLO AUTH
	aclAuthed bool

	// queued commands for `multi`
	queue    [][][]byte
	watching map[string]uint64
	txErrors []error

	// selected db
	selectedDB int

	clientID        uint64
	clientName      string
	trackingID      string
	protocolVersion int
	libName         string
	libVer          string
	noEvict         bool
	noTouch         bool
	replyMode       int // 0=ON, 1=OFF, 2=SKIP (suppress next)
	createdAt       time.Time
	lastActive      time.Time
	localAddr       string // optional override / cached local bind address
	lastCommand     string // lowercase last command name for CLIENT LIST
}

var connPool = sync.Pool{
	New: func() interface{} {
		return &Connection{}
	},
}

// RemoteAddr returns the remote network address
func (c *Connection) RemoteAddr() string {
	if c.conn == nil {
		return ""
	}
	return c.conn.RemoteAddr().String()
}

// LocalAddr returns the local network address of the connection.
func (c *Connection) LocalAddr() string {
	if c.localAddr != "" {
		return c.localAddr
	}
	if c.conn == nil {
		return ""
	}
	return c.conn.LocalAddr().String()
}

// SetLocalAddr sets/overrides the local address (tests / synthetic conns).
func (c *Connection) SetLocalAddr(addr string) {
	c.localAddr = addr
}

// Close disconnect with the client
func (c *Connection) Close() error {
	c.sendingData.WaitWithTimeout(10 * time.Second)
	if c.conn != nil { // may be a fake conn for tests
		_ = c.conn.Close()
	}
	c.subs = nil
	c.psubs = nil
	c.password = ""
	c.aclUser = ""
	c.aclAuthed = false
	c.queue = nil
	c.watching = nil
	c.txErrors = nil
	c.selectedDB = 0
	c.clientName = ""
	c.trackingID = ""
	c.protocolVersion = 0
	c.libName = ""
	c.libVer = ""
	c.noEvict = false
	c.noTouch = false
	c.replyMode = 0
	c.createdAt = time.Time{}
	c.lastActive = time.Time{}
	c.clientID = 0
	c.lastCommand = ""
	connPool.Put(c)
	return nil
}

// NewConn creates Connection instance
func NewConn(conn net.Conn) *Connection {
	c, ok := connPool.Get().(*Connection)
	if !ok {
		logger.Error("connection pool make wrong type")
		return &Connection{
			conn: conn,
		}
	}
	c.conn = conn
	now := time.Now()
	c.createdAt = now
	c.lastActive = now
	if c.clientID == 0 {
		c.clientID = globalClientID.Add(1)
	}
	return c
}

// SetClientTimesForTest sets age/idle clocks (tests only).
func (c *Connection) SetClientTimesForTest(created, lastActive time.Time) {
	c.createdAt = created
	c.lastActive = lastActive
}

// AgeSeconds returns seconds since connection creation.
func (c *Connection) AgeSeconds() int64 {
	if c.createdAt.IsZero() {
		return 0
	}
	return int64(time.Since(c.createdAt).Seconds())
}

// IdleSeconds returns seconds since last activity.
func (c *Connection) IdleSeconds() int64 {
	if c.lastActive.IsZero() {
		return 0
	}
	return int64(time.Since(c.lastActive).Seconds())
}

// Write sends response to client over tcp connection
func (c *Connection) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	c.sendingData.Add(1)
	defer func() {
		c.sendingData.Done()
	}()

	return c.conn.Write(b)
}

func (c *Connection) Name() string {
	if c.clientName != "" {
		return c.clientName
	}
	if c.conn != nil {
		return c.conn.RemoteAddr().String()
	}
	return ""
}

func (c *Connection) GetClientID() int64 {
	if c.clientID == 0 {
		c.clientID = globalClientID.Add(1)
	}
	return int64(c.clientID)
}

func (c *Connection) SetClientName(name string) {
	c.clientName = name
}

func (c *Connection) GetClientName() string {
	return c.clientName
}

func (c *Connection) SetTrackingID(id string) {
	c.trackingID = id
}

func (c *Connection) GetTrackingID() string {
	return c.trackingID
}

func (c *Connection) SetLastCommand(cmd string) {
	c.lastCommand = cmd
}

func (c *Connection) GetLastCommand() string {
	return c.lastCommand
}

func (c *Connection) SetLibName(name string) {
	c.libName = name
}

func (c *Connection) GetLibName() string {
	return c.libName
}

func (c *Connection) SetLibVer(ver string) {
	c.libVer = ver
}

func (c *Connection) GetLibVer() string {
	return c.libVer
}

// SetNoEvict sets CLIENT NO-EVICT flag (eviction engine may ignore for now).
func (c *Connection) SetNoEvict(v bool) {
	c.noEvict = v
}

// GetNoEvict returns whether CLIENT NO-EVICT is enabled.
func (c *Connection) GetNoEvict() bool {
	return c.noEvict
}

// SetNoTouch sets CLIENT NO-TOUCH (reads skip LRU/LFU touch).
func (c *Connection) SetNoTouch(v bool) {
	c.noTouch = v
}

// GetNoTouch returns whether CLIENT NO-TOUCH is enabled.
func (c *Connection) GetNoTouch() bool {
	return c.noTouch
}

const (
	replyModeOn = iota
	replyModeOff
	replyModeSkip
)

// SetReplyMode sets CLIENT REPLY ON|OFF|SKIP.
func (c *Connection) SetReplyMode(mode string) {
	switch strings.ToUpper(mode) {
	case "OFF":
		c.replyMode = replyModeOff
	case "SKIP":
		c.replyMode = replyModeSkip
	default:
		c.replyMode = replyModeOn
	}
}

// ShouldSuppressReply reports whether the next write should be skipped.
// SKIP suppresses once then returns to ON. OFF suppresses until ON.
func (c *Connection) ShouldSuppressReply() bool {
	switch c.replyMode {
	case replyModeOff:
		return true
	case replyModeSkip:
		c.replyMode = replyModeOn
		return true
	default:
		return false
	}
}

// SetProtocolVersion stores the RESP protocol version negotiated via HELLO.
func (c *Connection) SetProtocolVersion(v int) {
	c.protocolVersion = v
}

// GetProtocolVersion returns the RESP protocol version (default 2).
func (c *Connection) GetProtocolVersion() int {
	if c.protocolVersion == 0 {
		return 2
	}
	return c.protocolVersion
}

// Subscribe add current connection into subscribers of the given channel
func (c *Connection) Subscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subs == nil {
		c.subs = make(map[string]bool)
	}
	c.subs[channel] = true
}

// UnSubscribe removes current connection into subscribers of the given channel
func (c *Connection) UnSubscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.subs) == 0 {
		return
	}
	delete(c.subs, channel)
}

// PSubscribe adds a glob pattern subscription.
func (c *Connection) PSubscribe(pattern string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.psubs == nil {
		c.psubs = make(map[string]bool)
	}
	c.psubs[pattern] = true
}

// PUnSubscribe removes a glob pattern subscription.
func (c *Connection) PUnSubscribe(pattern string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.psubs) == 0 {
		return
	}
	delete(c.psubs, pattern)
}

// SSubscribe adds a sharded pub/sub channel subscription.
func (c *Connection) SSubscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ssubs == nil {
		c.ssubs = make(map[string]bool)
	}
	c.ssubs[channel] = true
}

// SUnSubscribe removes a sharded pub/sub channel subscription.
func (c *Connection) SUnSubscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.ssubs) == 0 {
		return
	}
	delete(c.ssubs, channel)
}

// SubsCount returns classic channel + pattern + sharded subscription count
// (used for "in subscribed mode" checks).
func (c *Connection) SubsCount() int {
	return len(c.subs) + len(c.psubs) + len(c.ssubs)
}

// PSubsCount returns pattern subscription count.
func (c *Connection) PSubsCount() int {
	return len(c.psubs)
}

// SSubsCount returns sharded channel subscription count.
func (c *Connection) SSubsCount() int {
	return len(c.ssubs)
}

// GetChannels returns all subscribing channels
func (c *Connection) GetChannels() []string {
	if c.subs == nil {
		return make([]string, 0)
	}
	channels := make([]string, 0, len(c.subs))
	for ch := range c.subs {
		channels = append(channels, ch)
	}
	return channels
}

// GetPatterns returns all pattern subscriptions.
func (c *Connection) GetPatterns() []string {
	if c.psubs == nil {
		return make([]string, 0)
	}
	out := make([]string, 0, len(c.psubs))
	for p := range c.psubs {
		out = append(out, p)
	}
	return out
}

// SetPassword stores password for authentication
func (c *Connection) SetPassword(password string) {
	c.password = password
}

// GetPassword get password for authentication
func (c *Connection) GetPassword() string {
	return c.password
}

// SetACLUser stores the ACL username for this connection.
func (c *Connection) SetACLUser(username string) {
	c.aclUser = username
}

// GetACLUser returns the ACL username (empty means "default").
func (c *Connection) GetACLUser() string {
	return c.aclUser
}

// SetACLAuthenticated marks the connection as ACL-authenticated.
func (c *Connection) SetACLAuthenticated(authed bool) {
	c.aclAuthed = authed
}

// IsACLAuthenticated reports whether AUTH / HELLO AUTH succeeded.
func (c *Connection) IsACLAuthenticated() bool {
	return c.aclAuthed
}

// InMultiState tells is connection in an uncommitted transaction
func (c *Connection) InMultiState() bool {
	return c.flags&flagMulti > 0
}

// SetMultiState sets transaction flag
func (c *Connection) SetMultiState(state bool) {
	if !state { // reset data when cancel multi
		c.watching = nil
		c.queue = nil
		c.flags &= ^flagMulti // clean multi flag
		return
	}
	c.flags |= flagMulti
}

// GetQueuedCmdLine returns queued commands of current transaction
func (c *Connection) GetQueuedCmdLine() [][][]byte {
	return c.queue
}

// EnqueueCmd  enqueues command of current transaction
func (c *Connection) EnqueueCmd(cmdLine [][]byte) {
	c.queue = append(c.queue, cmdLine)
}

// AddTxError stores syntax error within transaction
func (c *Connection) AddTxError(err error) {
	c.txErrors = append(c.txErrors, err)
}

// GetTxErrors returns syntax error within transaction
func (c *Connection) GetTxErrors() []error {
	return c.txErrors
}

// ClearQueuedCmds clears queued commands of current transaction
func (c *Connection) ClearQueuedCmds() {
	c.queue = nil
}

// GetWatching returns watching keys and their version code when started watching
func (c *Connection) GetWatching() map[string]uint64 {
	if c.watching == nil {
		c.watching = make(map[string]uint64)
	}
	return c.watching
}

// GetDBIndex returns selected db
func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

// SelectDB selects a database
func (c *Connection) SelectDB(dbNum int) {
	c.selectedDB = dbNum
}

func (c *Connection) SetSlave() {
	c.flags |= flagSlave
}

func (c *Connection) IsSlave() bool {
	return c.flags&flagSlave > 0
}

// SetMaster marks c as a connection with master
func (c *Connection) SetMaster() {
	c.flags |= flagMaster
}

func (c *Connection) IsMaster() bool {
	return c.flags&flagMaster > 0
}
