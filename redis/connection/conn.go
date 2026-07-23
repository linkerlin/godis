package connection

import (
	"net"
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
}

var connPool = sync.Pool{
	New: func() interface{} {
		return &Connection{}
	},
}

// RemoteAddr returns the remote network address
func (c *Connection) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
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
	c.clientID = 0
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
	if c.clientID == 0 {
		c.clientID = globalClientID.Add(1)
	}
	return c
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

// SubsCount returns channel + pattern subscription count.
func (c *Connection) SubsCount() int {
	return len(c.subs) + len(c.psubs)
}

// PSubsCount returns pattern subscription count.
func (c *Connection) PSubsCount() int {
	return len(c.psubs)
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
