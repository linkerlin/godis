// Package consts defines global constants for the godis server
package consts

import "time"

// Size limits for keys and values
const (
	MaxKeySize   = 512 * 1024       // 512KB
	MaxValueSize = 512 * 1024 * 1024 // 512MB
	MaxArgCount  = 1000000          // Maximum number of arguments for a command
)

// Time related constants
const (
	DefaultExpireTaskPrefix = "expire:"
	DefaultLockTimeout      = 5 * time.Second
	TransactionTTL          = time.Minute
)

// Default configuration values
const (
	DefaultDBCount    = 16
	DefaultPort       = 6379
	DefaultMaxClients = 1000
	DefaultBind       = "127.0.0.1"
)

// Data structure sizes
const (
	DataDictSize = 1 << 16
	TTLDictSize  = 1 << 10
)

// AOF related
const (
	AOFQueueSize = 1 << 20
	FsyncAlways  = "always"
	FsyncEverySec = "everysec"
	FsyncNo      = "no"
)

// Replication roles
const (
	MasterRole = iota
	SlaveRole
)

// Command flags
const (
	FlagWrite = 0
	FlagReadOnly = 1 << iota
	FlagSpecial
)

// Sorted set limits
const (
	MaxLevel = 16
)
