// Package acl provides Access Control List functionality for Godis
package acl

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	"github.com/linkerlin/godis/datastruct/dict"
)

// User represents an ACL user
type User struct {
	Name        string
	Enabled     bool
	Passwords   []Password
	Commands    *CommandPermissions
	KeyPatterns []KeyPattern
	Channels    []ChannelPattern
	Selectors   []*Selector
	mu          sync.RWMutex
}

// Password represents a user password (can be plaintext or SHA256 hashed)
type Password struct {
	Hash  string // SHA256 hash of password
	IsSHA bool   // Whether Hash is already SHA256
}

// CommandPermissions represents command permissions for a user
type CommandPermissions struct {
	AllowedCategories map[string]bool // @read, @write, etc.
	AllowedCommands   map[string]bool // Specific +command
	DeniedCommands    map[string]bool // Specific -command
	AllCommands       bool            // +@all
}

// KeyPattern represents a key access pattern
type KeyPattern struct {
	Pattern string
	Allowed bool // true for allow, false for deny
	Read    bool // %R~ or ~ / %RW~
	Write   bool // %W~ or ~ / %RW~
}

// ChannelPattern represents a Pub/Sub channel access pattern
type ChannelPattern struct {
	Pattern string
	Allowed bool
}

// Selector represents an ACL selector (Redis 7.0+)
type Selector struct {
	Commands    *CommandPermissions
	KeyPatterns []KeyPattern
	Channels    []ChannelPattern
}

// NewUser creates a new ACL user
func NewUser(name string) *User {
	return &User{
		Name:    name,
		Enabled: true,
		Commands: &CommandPermissions{
			AllowedCategories: make(map[string]bool),
			AllowedCommands:   make(map[string]bool),
			DeniedCommands:    make(map[string]bool),
		},
		KeyPatterns: make([]KeyPattern, 0),
		Channels:    make([]ChannelPattern, 0),
	}
}

// SetPassword sets password for the user
func (u *User) SetPassword(password string, isSHA256 bool) {
	u.mu.Lock()
	defer u.mu.Unlock()

	if isSHA256 {
		u.Passwords = []Password{{Hash: password, IsSHA: true}}
	} else {
		hash := sha256.Sum256([]byte(password))
		u.Passwords = []Password{{Hash: hex.EncodeToString(hash[:]), IsSHA: true}}
	}
}

// AddPassword adds an additional password
func (u *User) AddPassword(password string, isSHA256 bool) {
	u.mu.Lock()
	defer u.mu.Unlock()

	var pwd Password
	if isSHA256 {
		pwd = Password{Hash: password, IsSHA: true}
	} else {
		hash := sha256.Sum256([]byte(password))
		pwd = Password{Hash: hex.EncodeToString(hash[:]), IsSHA: true}
	}
	u.Passwords = append(u.Passwords, pwd)
}

// VerifyPassword verifies if password matches
func (u *User) VerifyPassword(password string) bool {
	u.mu.RLock()
	defer u.mu.RUnlock()

	if !u.Enabled {
		return false
	}

	// Hash the provided password
	hash := sha256.Sum256([]byte(password))
	hashStr := hex.EncodeToString(hash[:])

	for _, pwd := range u.Passwords {
		if pwd.Hash == hashStr {
			return true
		}
	}
	return false
}

// HasPassword checks if user has any password set
func (u *User) HasPassword() bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return len(u.Passwords) > 0
}

// AllowCategory allows a command category
func (u *User) AllowCategory(category string) {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.Commands.AllowedCategories[category] = true
}

// DenyCategory denies a command category
func (u *User) DenyCategory(category string) {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.Commands.AllowedCategories[category] = false
}

// AllowCommand allows a specific command
func (u *User) AllowCommand(cmd string) {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.Commands.AllowedCommands[strings.ToLower(cmd)] = true
	delete(u.Commands.DeniedCommands, strings.ToLower(cmd))
}

// DenyCommand denies a specific command
func (u *User) DenyCommand(cmd string) {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.Commands.DeniedCommands[strings.ToLower(cmd)] = true
	delete(u.Commands.AllowedCommands, strings.ToLower(cmd))
}

// AllowAllCommands allows all commands
func (u *User) AllowAllCommands() {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.Commands.AllCommands = true
}

// CheckCommand checks if user can execute a command (root or any selector).
func (u *User) CheckCommand(cmd string) bool {
	u.mu.RLock()
	defer u.mu.RUnlock()

	if !u.Enabled {
		return false
	}
	cmd = strings.ToLower(cmd)
	if commandsAllow(u.Commands, cmd) {
		return true
	}
	for _, sel := range u.Selectors {
		if sel != nil && commandsAllow(sel.Commands, cmd) {
			return true
		}
	}
	return false
}

// CheckPermission checks command + keys as Redis ACL selectors do:
// root match OR any selector match (each set evaluated independently).
func (u *User) CheckPermission(cmd string, writeKeys, readKeys []string) bool {
	u.mu.RLock()
	defer u.mu.RUnlock()

	if !u.Enabled {
		return false
	}
	cmd = strings.ToLower(cmd)
	if permissionSetAllows(u.Commands, u.KeyPatterns, cmd, writeKeys, readKeys) {
		return true
	}
	for _, sel := range u.Selectors {
		if sel == nil {
			continue
		}
		if permissionSetAllows(sel.Commands, sel.KeyPatterns, cmd, writeKeys, readKeys) {
			return true
		}
	}
	return false
}

// CheckChannel checks Pub/Sub channel access (&pattern). Empty channels = allow all.
func (u *User) CheckChannel(channel string) bool {
	u.mu.RLock()
	defer u.mu.RUnlock()

	if !u.Enabled {
		return false
	}
	if len(u.Channels) == 0 {
		return true
	}
	for _, ch := range u.Channels {
		if ch.Allowed && matchPattern(ch.Pattern, channel) {
			return true
		}
	}
	return false
}

func commandsAllow(cp *CommandPermissions, cmd string) bool {
	if cp == nil {
		return false
	}
	if cp.DeniedCommands[cmd] {
		return false
	}
	if cp.AllowedCommands[cmd] {
		return true
	}
	categories := GetCommandCategories(cmd)
	for _, cat := range categories {
		if allowed, exists := cp.AllowedCategories[cat]; exists {
			if !allowed {
				return false
			}
			return true
		}
	}
	return cp.AllCommands
}

func keysAllow(patterns []KeyPattern, key string, write bool) bool {
	if len(patterns) == 0 {
		return true
	}
	allowed := false
	hasPattern := false
	for _, kp := range patterns {
		need := kp.Read
		if write {
			need = kp.Write
		}
		if !kp.Read && !kp.Write {
			need = true
		}
		if !need {
			continue
		}
		if matchPattern(kp.Pattern, key) {
			hasPattern = true
			allowed = kp.Allowed
		}
	}
	if !hasPattern {
		return false
	}
	return allowed
}

func permissionSetAllows(cp *CommandPermissions, patterns []KeyPattern, cmd string, writeKeys, readKeys []string) bool {
	if !commandsAllow(cp, cmd) {
		return false
	}
	for _, k := range writeKeys {
		if !keysAllow(patterns, k, true) {
			return false
		}
	}
	for _, k := range readKeys {
		if !keysAllow(patterns, k, false) {
			return false
		}
	}
	return true
}

// AddKeyPattern adds a key pattern with both read and write access.
func (u *User) AddKeyPattern(pattern string, allowed bool) {
	u.AddKeyPatternRW(pattern, allowed, true, true)
}

// AddKeyPatternRW adds a key pattern with explicit read/write flags (Redis %R~/%W~/%RW~).
func (u *User) AddKeyPatternRW(pattern string, allowed, read, write bool) {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.KeyPatterns = append(u.KeyPatterns, KeyPattern{
		Pattern: pattern,
		Allowed: allowed,
		Read:    read,
		Write:   write,
	})
}

// CheckKey checks if user can access a key for any operation (read or write).
func (u *User) CheckKey(key string) bool {
	return u.CheckKeyAccess(key, true) || u.CheckKeyAccess(key, false)
}

// CheckKeyAccess checks read (write=false) or write (write=true) on root key patterns.
func (u *User) CheckKeyAccess(key string, write bool) bool {
	u.mu.RLock()
	defer u.mu.RUnlock()

	if !u.Enabled {
		return false
	}
	return keysAllow(u.KeyPatterns, key, write)
}

// Engine is the ACL engine
type Engine struct {
	mu          sync.RWMutex
	users       *dict.ConcurrentDict // username -> *User
	defaultUser string
}

// NewEngine creates a new ACL engine
func NewEngine() *Engine {
	return &Engine{
		users:       dict.MakeConcurrent(16),
		defaultUser: "default",
	}
}

// GetUser gets a user by name
func (e *Engine) GetUser(name string) (*User, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	val, exists := e.users.Get(name)
	if !exists {
		return nil, false
	}
	return val.(*User), true
}

// SetUser creates or incrementally updates a user (Redis ACL SETUSER semantics).
func (e *Engine) SetUser(name string, rules []string) (*User, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var u *User
	if val, exists := e.users.Get(name); exists {
		u = val.(*User)
	} else {
		u = NewUser(name)
	}
	if err := applyRules(u, rules); err != nil {
		return nil, err
	}

	e.users.Put(name, u)
	return u, nil
}

// DelUser deletes a user
func (e *Engine) DelUser(names []string) int {
	e.mu.Lock()
	defer e.mu.Unlock()

	deleted := 0
	for _, name := range names {
		if _, ok := e.users.Get(name); ok {
			e.users.Remove(name)
			deleted++
		}
	}
	return deleted
}

// GetAllUsers returns all users
func (e *Engine) GetAllUsers() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var users []string
	e.users.ForEach(func(key string, val interface{}) bool {
		users = append(users, key)
		return true
	})
	return users
}

// Authenticate authenticates a user
func (e *Engine) Authenticate(username, password string) (*User, error) {
	user, exists := e.GetUser(username)
	if !exists {
		return nil, fmt.Errorf("invalid username or password")
	}

	if !user.HasPassword() {
		return user, nil
	}

	if !user.VerifyPassword(password) {
		return nil, fmt.Errorf("invalid username or password")
	}

	return user, nil
}

// GetCommandCategories returns the categories a command belongs to
func GetCommandCategories(cmd string) []string {
	cmd = strings.ToLower(cmd)

	categories := make([]string, 0)
	for cat, cmds := range CommandCategoryMap {
		for _, c := range cmds {
			if strings.ToLower(c) == cmd {
				categories = append(categories, cat)
				break
			}
		}
	}

	return categories
}

// CommandCategoryMap maps categories to their commands
var CommandCategoryMap = map[string][]string{
	"@keyspace":    {"del", "dump", "exists", "expire", "expireat", "keys", "move", "persist", "pexpire", "pexpireat", "pttl", "randomkey", "rename", "renamenx", "restore", "sort", "ttl", "type", "scan"},
	"@read":        {"get", "mget", "exists", "ttl", "pttl", "type", "strlen", "getrange", "hexists", "hget", "hgetall", "hkeys", "hvals", "hlen", "hmget", "hstrlen", "lindex", "llen", "lrange", "scard", "sismember", "smembers", "srandmember", "sscan", "zcard", "zcount", "zrange", "zrangebyscore", "zrank", "zrevrange", "zrevrangebyscore", "zrevrank", "zscore", "zscan"},
	"@write":       {"set", "mset", "setex", "psetex", "setnx", "incr", "incrby", "incrbyfloat", "decr", "decrby", "append", "setrange", "hdel", "hset", "hsetnx", "hmset", "hincrby", "hincrbyfloat", "lpush", "lpushx", "lpop", "rpush", "rpushx", "rpop", "lrem", "lset", "ltrim", "sadd", "scard", "srem", "spop", "zadd", "zincrby", "zrem", "zremrangebyrank", "zremrangebyscore"},
	"@admin":       {"acl", "bgrewriteaof", "bgsave", "client", "cluster", "config", "dbsize", "debug", "flushall", "flushdb", "info", "lastsave", "monitor", "role", "save", "shutdown", "slaveof", "slowlog", "sync"},
	"@dangerous":   {"flushall", "flushdb", "keys", "shutdown", "debug", "config"},
	"@connection":  {"auth", "echo", "ping", "quit", "select", "swapdb"},
	"@transaction": {"discard", "exec", "multi", "unwatch", "watch"},
	"@pubsub":      {"psubscribe", "publish", "pubsub", "punsubscribe", "subscribe", "unsubscribe"},
	"@set":         {"sadd", "scard", "sdiff", "sdiffstore", "sinter", "sinterstore", "sismember", "smembers", "smove", "spop", "srandmember", "srem", "sscan", "sunion", "sunionstore"},
	"@sortedset":   {"zadd", "zcard", "zcount", "zincrby", "zinterstore", "zlexcount", "zrange", "zrangebylex", "zrangebyscore", "zrank", "zrem", "zremrangebylex", "zremrangebyrank", "zremrangebyscore", "zrevrange", "zrevrangebylex", "zrevrangebyscore", "zrevrank", "zscan", "zscore", "zunionstore"},
	"@list":        {"blpop", "brpop", "brpoplpush", "lindex", "linsert", "llen", "lpop", "lpush", "lpushx", "lrange", "lrem", "lset", "ltrim", "rpop", "rpoplpush", "rpush", "rpushx"},
	"@hash":        {"hdel", "hexists", "hget", "hgetall", "hincrby", "hincrbyfloat", "hkeys", "hlen", "hmget", "hmset", "hscan", "hset", "hsetnx", "hstrlen", "hvals"},
	"@string":      {"append", "bitcount", "bitfield", "bitop", "bitpos", "decr", "decrby", "get", "getbit", "getrange", "getset", "incr", "incrby", "incrbyfloat", "mget", "mset", "msetnx", "psetex", "set", "setbit", "setex", "setnx", "setrange", "strlen"},
	"@bitmap":      {"bitcount", "bitfield", "bitop", "bitpos", "getbit", "setbit"},
	"@hyperloglog": {"pfadd", "pfcount", "pfmerge", "pfselftest"},
	"@geo":         {"geoadd", "geodist", "geohash", "geopos", "georadius", "georadiusbymember"},
	"@stream":      {"xadd", "xack", "xclaim", "xdel", "xgroup", "xinfo", "xlen", "xpending", " xrange", "xread", "xreadgroup", "xrevrange", "xtrim"},
	"@scripting":   {"eval", "evalsha", "script"},
}

// matchPattern checks if a string matches a glob pattern
func matchPattern(pattern, s string) bool {
	// Simplified glob matching - in production use proper glob library
	if pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, "*") && strings.HasPrefix(s, pattern[:len(pattern)-1]) {
		return true
	}
	if strings.HasPrefix(pattern, "*") && strings.HasSuffix(s, pattern[1:]) {
		return true
	}
	return pattern == s
}

// IsDangerousCommand checks if a command is dangerous
func IsDangerousCommand(cmd string) bool {
	cmd = strings.ToLower(cmd)
	dangerous := []string{"flushall", "flushdb", "keys", "shutdown", "debug", "config", "save", "bgsave", "bgrewriteaof"}
	for _, d := range dangerous {
		if d == cmd {
			return true
		}
	}
	return false
}
