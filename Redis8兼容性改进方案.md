# Godis Redis 8.x 兼容性改进方案

> 目标：实现 Godis 与 Redis 8.x 的 100% 命令级兼容
> 版本：v1.0
> 日期：2026-03-07

---

## 目录

1. [现状分析](#1-现状分析)
2. [差距分析](#2-差距分析)
3. [改进计划总览](#3-改进计划总览)
4. [详细改进方案](#4-详细改进方案)
5. [实施路线图](#5-实施路线图)

---

## 1. 现状分析

### 1.1 Godis 当前实现概览

| 模块 | 状态 | 已支持命令数 |
|------|------|-------------|
| String | ✅ 完整 | 22 |
| List | ✅ 完整 | 13 |
| Hash | ⚠️ 部分 | 16 |
| Set | ✅ 完整 | 13 |
| SortedSet | ✅ 完整 | 22 |
| Bitmap | ✅ 完整 | 5 |
| Geo | ⚠️ 部分 | 6 |
| Pub/Sub | ⚠️ 基础 | 3 |
| Keys | ⚠️ 部分 | 14 |
| Server | ⚠️ 部分 | 7 |
| Transaction | ✅ 完整 | 5 |
| Connection | ❌ 缺失 | 0 |
| Scripting (Lua) | ❌ 缺失 | 0 |
| Stream | ❌ 缺失 | 0 |
| JSON | ❌ 缺失 | 0 |
| Time Series | ❌ 缺失 | 0 |
| Probabilistic | ❌ 缺失 | 0 |
| Vector Set | ❌ 缺失 | 0 |
| ACL | ❌ 缺失 | 0 |
| Search (FT.*) | ❌ 缺失 | 0 |

### 1.2 当前命令统计

- **已实现命令**: ~130 个
- **Redis 8.x 总命令数**: ~400+ 个
- **覆盖率**: ~32%

---

## 2. 差距分析

### 2.1 Redis 8.x 新特性清单

#### 2.1.1 全新数据类型 (Redis 8.0+)

| 数据类型 | 优先级 | 复杂度 | 说明 |
|---------|--------|--------|------|
| **Vector Set** | P1 | 极高 | 向量相似度搜索，用于 AI/RAG |
| **JSON** | P1 | 高 | 原生 JSON 支持 |
| **Time Series** | P2 | 高 | 时序数据支持 |
| **Bloom Filter** | P2 | 中 | 布隆过滤器 |
| **Cuckoo Filter** | P2 | 中 | 布谷鸟过滤器 |
| **Count-Min Sketch** | P3 | 中 | 概率计数 |
| **Top-K** | P3 | 中 | Top-K 统计 |
| **t-digest** | P3 | 高 | 分位数计算 |

#### 2.1.2 Redis 8.0 新增核心命令

```
# Hash 增强 (Redis 7.4+)
HGETEX key field [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|PERSIST]
HSETEX key field value [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|KEEPTTL]
HGETDEL key field [field ...]

# Vector Set (Redis 8.0)
VS.ADD key vector [vector ...] [REDUCE dim REDUCE_METHOD method]
VS.CARD key
VS.DIM key
VS.GET key vectorid
VS.MGET key vectorid [vectorid ...]
VS.QUERY key query_vector [K num] [EPSILON epsilon] [PARAMS name value [name value ...]]
VS.REM key vectorid [vectorid ...]
VS.SEARCH key query_vector [K num] [EPSILON epsilon] [PARAMS name value [name value ...]]
VS.SIM key vectorid1 vectorid2 [PARAMS name value [name value ...]]

# Stream 新增 (Redis 8.2+)
XDELEX key group id [id ...]
XACKDEL key group id [id ...]

# BITOP 扩展 (Redis 8.2+)
BITOP DIFF destkey srckey [srckey ...]
BITOP DIFF1 destkey srckey [srckey ...]
BITOP ANDOR destkey srckey [srckey ...]
BITOP ONE destkey srckey [srckey ...]
```

#### 2.1.3 缺失的核心功能模块

| 模块 | 优先级 | 缺失命令数量 | 关键性 |
|------|--------|-------------|--------|
| **Stream** | P0 | ~25 | 高可用/日志追踪必备 |
| **Scripting (Lua)** | P0 | ~8 | 复杂业务逻辑支持 |
| **Connection** | P1 | ~12 | 客户端管理 |
| **ACL** | P1 | ~12 | 安全认证 |
| **Cluster** | P1 | ~15 | 分布式部署 |
| **HyperLogLog** | P2 | ~5 | 基数统计 |
| **Client Tracking** | P2 | ~5 | 缓存失效 |
| **Function** | P3 | ~6 | Redis Functions |
| **Search (FT)** | P3 | ~60+ | 全文搜索/向量搜索 |

---

## 3. 改进计划总览

### 3.1 优先级矩阵

```
                    高影响
                      ↑
         P0 Stream   |   P0 Lua Script
         P0 JSON     |   P1 ACL
         P1 Vector   |   P1 Connection
                      |
低复杂度 ←——————————→ 高复杂度
                      |
         P2 HLL      |   P2 Client Tracking
         P2 Bloom    |   P3 Functions
         P3 CMS      |   P3 Search
                      ↓
                    低影响
```

### 3.2 阶段规划

| 阶段 | 周期 | 目标 | 预计新增命令 |
|------|------|------|-------------|
| Phase 1 | 8-10 周 | 核心功能补齐 | 60+ |
| Phase 2 | 6-8 周 | 高级功能实现 | 80+ |
| Phase 3 | 4-6 周 | 性能优化完善 | 50+ |
| Phase 4 | 持续 | 新特性跟进 | - |

---

## 4. 详细改进方案

### 4.1 P0 - 核心缺失功能

#### 4.1.1 Stream 数据类型实现

**现状**: Godis 完全缺失 Stream 类型

**Redis 8.x Stream 命令清单**:

```
# 写入命令
XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] *|id field value [field value ...]
XDEL key id [id ...]
XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]
XGROUP CREATE key groupname id|$ [MKSTREAM] [ENTRIESREAD entries-read]
XGROUP CREATECONSUMER key groupname consumername
XGROUP DELCONSUMER key groupname consumername
XGROUP DESTROY key groupname
XGROUP SETID key groupname id|$ [ENTRIESREAD entries-read]

# 读取命令
XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]
XRANGE key start end [COUNT count]
XREVRANGE key end start [COUNT count]
XLEN key
XINFO STREAM key [FULL [COUNT count]]
XINFO GROUPS key
XINFO CONSUMERS key groupname
XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms-unix-time] [RETRYCOUNT count] [FORCE] [JUSTID]
XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
XPENDING key group [[start end count] [consumer]]

# Redis 8.2 新增
XDELEX key group id [id ...]
XACKDEL key group id [id ...]
```

**实现方案**:

```go
// datastruct/stream/stream.go
package stream

import (
    "github.com/hdt3213/godis/datastruct/dict"
    "sync"
    "time"
)

// StreamEntry 流条目
type StreamEntry struct {
    ID     StreamID
    Fields map[string]string
}

// StreamID 流 ID (毫秒时间戳-序列号)
type StreamID struct {
    Timestamp int64
    Sequence  int64
}

// ConsumerGroup 消费者组
type ConsumerGroup struct {
    Name            string
    LastID          StreamID          // 最后递送 ID
    PendingEntries  *dict.ConcurrentDict // 待处理条目
    Consumers       *dict.ConcurrentDict // 消费者
    EntriesRead     int64             // 已读取条目数 (Redis 7.4+)
}

// Consumer 消费者
type Consumer struct {
    Name          string
    SeenTime      time.Time
    Pending       map[StreamID]*PendingEntry
}

// PendingEntry 待处理条目
type PendingEntry struct {
    ID            StreamID
    Consumer      string
    DeliveryTime  time.Time
    DeliveryCount int
}

// Stream 流数据结构
type Stream struct {
    mu             sync.RWMutex
    entries        *dict.ConcurrentDict  // StreamID -> StreamEntry
    groups         *dict.ConcurrentDict  // group name -> ConsumerGroup
    lastID         StreamID
    maxlen         int64                 // 最大长度限制
    entriesAdded   int64                 // 总添加条目数
}

// 核心方法
func (s *Stream) Add(id string, fields map[string]string, opts *AddOptions) (StreamID, error)
func (s *Stream) Range(start, end StreamID, count int) []*StreamEntry
func (s *Stream) CreateGroup(name string, startID string) error
func (s *Stream) ReadGroup(group, consumer string, count int, block time.Duration, ids []string) []*StreamEntry
func (s *Stream) Claim(group, consumer string, idleTime time.Duration, ids []StreamID) []*StreamEntry
func (s *Stream) AutoClaim(group, consumer string, idleTime time.Duration, start StreamID, count int) ([]*StreamEntry, StreamID)

// Redis 8.2 新增
func (s *Stream) DeleteFromGroup(group string, ids []StreamID) error
func (s *Stream) AckAndDelete(group string, ids []StreamID) ([]StreamID, error)
```

**工作量评估**: 4-5 周

---

#### 4.1.2 Lua Scripting 实现

**现状**: Godis 不支持 Lua 脚本

**Redis Script 命令**:

```
EVAL script numkeys key [key ...] arg [arg ...]
EVALSHA sha1 numkeys key [key ...] arg [arg ...]
SCRIPT EXISTS sha1 [sha1 ...]
SCRIPT FLUSH [ASYNC|SYNC]
SCRIPT KILL
SCRIPT LOAD script
SCRIPT DEBUG YES|SYNC|NO
```

**实现方案**:

```go
// scripting/scripting.go
package scripting

import (
    "github.com/yuin/gopher-lua"
    "github.com/hdt3213/godis/interface/database"
)

// ScriptEngine Lua 脚本执行引擎
type ScriptEngine struct {
    lstate      *lua.LState
    db          database.DBEngine
    shaToScript map[string]string  // SHA -> script content
    mu          sync.RWMutex
}

// 初始化 Lua 环境，注入 Redis 函数
func (se *ScriptEngine) initLuaEnv() {
    // redis.call(command, arg1, arg2, ...)
    // redis.pcall(command, arg1, arg2, ...)
    // redis.sha1hex(string)
    // redis.error_reply(string)
    // redis.status_reply(string)
    // redis.log(level, message)
    // redis.setresp(version)
    // redis.acl_check_cmd(command, arg1, ...)
    // redis.breakpoint()
    // redis.debug(msg)
    // redis.replicate_commands()
}

// 执行脚本
func (se *ScriptEngine) Eval(script string, keys, args [][]byte) redis.Reply
func (se *ScriptEngine) EvalSha(sha1 string, keys, args [][]byte) redis.Reply
func (se *ScriptEngine) ScriptLoad(script string) string
func (se *ScriptEngine) ScriptExists(sha1s []string) []int
func (se *ScriptEngine) ScriptFlush(async bool)
func (se *ScriptEngine) ScriptKill() error

// Redis 7.0+ Function 支持
type FunctionLib struct {
    Name     string
    Engine   string  // LUA or JS (Redis 8.0+)
    Code     string
    Functions map[string]*Function
}

func (se *ScriptEngine) FunctionLoad(code string, replace bool) error
func (se *ScriptEngine) FunctionCall(name string, keys, args [][]byte) redis.Reply
```

**注意事项**:
1. 脚本执行需要事务隔离
2. 脚本中调用的命令需要计入慢查询日志
3. 脚本执行时间限制 (lua-time-limit)
4. 脚本修改的数据需要触发 AOF 和复制

**工作量评估**: 3-4 周

---

#### 4.1.3 Hash 字段级过期 (Redis 7.4+)

**现状**: Godis Hash 不支持字段级过期

**实现方案**:

```go
// datastruct/dict/expire_dict.go
package dict

import (
    "time"
    "sync"
)

// ExpireDict 支持字段级过期的字典
type ExpireDict struct {
    data    map[string]*hashEntry
    expire  map[string]time.Time  // field -> expire time
    mu      sync.RWMutex
}

type hashEntry struct {
    value  interface{}
    expire *time.Time
}

func (ed *ExpireDict) GetWithExpire(key string) (value interface{}, ttl time.Duration, exists bool)
func (ed *ExpireDict) SetWithExpire(key string, value interface{}, ttl time.Duration)
func (ed *ExpireDict) Expire(key string, expireAt time.Time) bool
func (ed *ExpireDict) TTL(key string) time.Duration
func (ed *ExpireDict) Persist(key string) bool

// 新增命令实现
database/hash_expire.go

func execHGetEx(db *DB, args [][]byte) redis.Reply {
    // HGETEX key field [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|PERSIST]
}

func execHSetEx(db *DB, args [][]byte) redis.Reply {
    // HSETEX key field value [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|KEEPTTL]
}

func execHGetDel(db *DB, args [][]byte) redis.Reply {
    // HGETDEL key field [field ...]
}
```

**工作量评估**: 1-2 周

---

### 4.2 P1 - 重要功能

#### 4.2.1 ACL (访问控制列表)

**Redis ACL 命令**:

```
ACL LIST
ACL USERS
ACL GETUSER username
ACL SETUSER username [rule [rule ...]]
ACL DELUSER username [username ...]
ACL CAT [categoryname]
ACL LOG [count | RESET]
ACL HELP
ACL LOAD
ACL SAVE
ACL GENPASS [bits]
ACL WHOAMI
ACL DRYRUN username command [arg [arg ...]]
```

**实现方案**:

```go
// acl/acl.go
package acl

import (
    "github.com/hdt3213/godis/datastruct/dict"
    "sync"
)

// User ACL 用户
type User struct {
    Name           string
    Enabled        bool
    Passwords      []Password
    Commands       *CommandPermission  // 命令权限
    Keys           []KeyPattern        // 键模式权限
    Channels       []ChannelPattern    // 频道权限
    Selectors      []*Selector         // 选择器 (Redis 7.0+)
}

type Password struct {
    Hash  string  // SHA256
    IsSHA bool    // 是否是哈希值
}

type CommandPermission struct {
    AllowedCategories map[string]bool
    AllowedCommands   map[string]bool
    DeniedCommands    map[string]bool
}

type KeyPattern struct {
    Pattern  string
    Allowed  bool
}

// ACL 引擎
type Engine struct {
    mu         sync.RWMutex
    users      *dict.ConcurrentDict  // username -> *User
    defaultUser string
    log        []*LogEntry
}

func (e *Engine) Authenticate(username, password string) (*User, error)
func (e *Engine) CheckCommand(user *User, cmd string, args [][]byte) bool
func (e *Engine) SetUser(username string, rules []string) error
func (e *Engine) GetUser(username string) (*User, error)
func (e *Engine) DelUser(usernames []string) int

// 命令分类 (Redis 8.x 新增)
var CommandCategories = map[string][]string{
    "@read":      {"get", "mget", "hget", "smembers", "zrange", ...},
    "@write":     {"set", "del", "hset", "sadd", "zadd", ...},
    "@admin":     {"acl", "config", "debug", "shutdown", ...},
    "@dangerous": {"flushall", "flushdb", "keys", ...},
    "@search":    {"ft.search", "ft.aggregate", ...},       // Redis 8
    "@json":      {"json.get", "json.set", ...},            // Redis 8
    "@timeseries":{"ts.add", "ts.range", ...},             // Redis 8
    "@bloom":     {"bf.add", "bf.exists", ...},             // Redis 8
    "@vector":    {"vs.add", "vs.query", ...},             // Redis 8
}
```

**工作量评估**: 2-3 周

---

#### 4.2.2 Connection 管理

**缺失命令**:

```
AUTH [username] password
HELLO [protocol-version] [AUTH username password] [SETNAME clientname]
ECHO message
PING [message]
QUIT
SELECT index
SWAPDB index1 index2
CLIENT LIST [TYPE type] [ID id [id ...]]
CLIENT INFO
CLIENT SETNAME connection-name
CLIENT GETNAME
CLIENT KILL [ip:port] [ID client-id] [TYPE type] [USER username] [ADDR ip:port] [SKIPME yes/no]
CLIENT PAUSE timeout [WRITE|ALL]
CLIENT UNPAUSE
CLIENT REPLY ON|OFF|SKIP
CLIENT CACHING YES|NO
CLIENT TRACKING ON|OFF [REDIRECT client-id] [PREFIX prefix [prefix ...]] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]
CLIENT TRACKINGINFO
CLIENT ID
CLIENT UNBLOCK client-id [TIMEOUT|ERROR]
CLIENT SETINFO lib-name lib-version
READONLY
READWRITE
```

**实现方案**:

```go
// connection/manager.go
package connection

import (
    "sync"
    "time"
)

// Manager 连接管理器
type Manager struct {
    mu          sync.RWMutex
    connections map[int64]*ServerConnection // client ID -> connection
    nextID      int64
}

func (cm *Manager) Register(conn *ServerConnection) int64
func (cm *Manager) Unregister(id int64)
func (cm *Manager) Kill(filter *KillFilter) int
func (cm *Manager) List(filter *ListFilter) []*ClientInfo
func (cm *Manager) Pause(timeout time.Duration, mode PauseMode)
func (cm *Manager) Unpause()

// ClientInfo 客户端信息
type ClientInfo struct {
    ID          int64
    Addr        string
    Name        string
    DB          int
    Cmd         string
    Age         time.Duration
    Idle        time.Duration
    Flags       []string // N(normal), M(master), S(slave), O(OK), P(pubsub), etc.
    Multi       int      // 事务状态
    Watch       int      // WATCH 数量
    User        string   // ACL 用户名
    LibName     string   // 客户端库名
    LibVer      string   // 客户端库版本
}

// Client Tracking
type Tracking struct {
    Enabled    bool
    Redirect   int64               // 重定向 client ID
    Prefixes   []string            // 前缀追踪
    BCAST      bool                // 广播模式
    OptIn      bool                // 可选加入模式
    OptOut     bool                // 可选退出模式
    NoLoop     bool                // 不接收自己的修改通知
    invalidations map[string]bool  // 待发送的失效消息
}

func (c *ServerConnection) EnableTracking(opts *TrackingOptions)
func (c *ServerConnection) DisableTracking()
func (c *ServerConnection) TrackKey(key string)  // 记录追踪的键
func (c *ServerConnection) Invalidate(keys []string) // 发送失效通知
```

**工作量评估**: 2 周

---

#### 4.2.3 Vector Set (Redis 8.0 Beta)

**命令清单**:

```
VS.ADD key vector [vector ...] [REDUCE dim REDUCE_METHOD method]
VS.CARD key
VS.DIM key
VS.GET key vectorid
VS.MGET key vectorid [vectorid ...]
VS.QUERY key query_vector [K num] [EPSILON epsilon] [PARAMS name value [name value ...]]
VS.REM key vectorid [vectorid ...]
VS.SEARCH key query_vector [K num] [EPSILON epsilon] [PARAMS name value [name value ...]]
VS.SIM key vectorid1 vectorid2 [PARAMS name value [name value ...]]
```

**实现方案**:

```go
// datastruct/vectorset/vectorset.go
package vectorset

import (
    "gonum.org/v1/gonum/mat"
    "sync"
)

// VectorSet 向量集合
type VectorSet struct {
    mu         sync.RWMutex
    dimension  int
    vectors    map[string]*Vector // id -> vector
    index      VectorIndex        // 向量索引 (HNSW/FLAT)
    reduceDim  int                // 降维后维度
    reduceAlg  ReduceAlgorithm    // 降维算法
}

type Vector struct {
    ID     string
    Data   []float32  // 原始向量
    Reduced []float32 // 降维后向量 (可选)
    Attrs  map[string]interface{} // 额外属性
}

// VectorIndex 向量索引接口
type VectorIndex interface {
    Build(vectors []*Vector)
    Search(query []float32, k int) ([]SearchResult, error)
    Add(id string, vector []float32)
    Remove(id string)
}

// HNSWIndex HNSW 索引实现
type HNSWIndex struct {
    M              int       // 每个节点的最大连接数
    EfConstruction int       // 构建时的搜索深度
    EfSearch       int       // 搜索时的搜索深度
    // ...
}

// FlatIndex 暴力搜索索引 (用于小规模数据)
type FlatIndex struct {
    vectors map[string][]float32
    metric  DistanceMetric
}

type DistanceMetric int

const (
    Euclidean DistanceMetric = iota
    Cosine
    InnerProduct
)

func (vs *VectorSet) Add(id string, vector []float32, opts *AddOptions) error
func (vs *VectorSet) Search(query []float32, k int, epsilon float64) ([]SearchResult, error)
func (vs *VectorSet) Similarity(id1, id2 string, metric DistanceMetric) (float64, error)

// 降维算法
type ReduceAlgorithm int

const (
    None ReduceAlgorithm = iota
    PCA
    RandomProjection
)

func ReducePCA(vectors [][]float32, targetDim int) ([][]float32, error)
func ReduceRandomProjection(vectors [][]float32, targetDim int) ([][]float32, error)
```

**工作量评估**: 4-5 周 (需要研究向量索引算法)

---

### 4.3 P2 - 重要但非核心

#### 4.3.1 HyperLogLog

```
PFADD key element [element ...]
PFCOUNT key [key ...]
PFMERGE destkey sourcekey [sourcekey ...]
PFSELFTEST
```

**实现方案**: 使用现成的 HLL 实现或基于 Redis 的 HLL 算法实现

---

#### 4.3.2 增强的位图操作 (Redis 8.2)

```go
// BITOP 扩展操作
func execBitOpDIFF(db *DB, args [][]byte) redis.Reply  // DIFF, DIFF1
func execBitOpANDOR(db *DB, args [][]byte) redis.Reply  // ANDOR
func execBitOpONE(db *DB, args [][]byte) redis.Reply    // ONE
```

---

#### 4.3.3 增强的 Pub/Sub

```
PSUBSCRIBE pattern [pattern ...]
PUNSUBSCRIBE [pattern [pattern ...]]
PUBLISH channel message
SPUBLISH shardchannel message
SSUBSCRIBE shardchannel [shardchannel ...]
SUNSUBSCRIBE [shardchannel [shardchannel ...]]
PUBSUB CHANNELS [pattern]
PUBSUB NUMSUB [channel [channel ...]]
PUBSUB NUMPAT
nPUBSUB SHARDCHANNELS [pattern]
PUBSUB SHARDNUMSUB [shardchannel [shardchannel ...]]
```

---

### 4.4 P3 - 高级功能

#### 4.4.1 JSON 数据类型

```
JSON.SET key [NX|XX] path value
JSON.GET key [INDENT indent] [NEWLINE newline] [SPACE space] [path ...]
JSON.DEL key [path]
JSON.TYPE key [path]
JSON.NUMINCRBY key path number
JSON.NUMMULTBY key path number
JSON.STRAPPEND key [path] value
JSON.STRLEN key [path]
JSON.ARRAPPEND key path value [value ...]
JSON.ARRINDEX key path value [start [stop]]
JSON.ARRINSERT key path index value [value ...]
JSON.ARRLEN key [path]
JSON.ARRPOP key [path [index]]
JSON.ARRTRIM key path start stop
JSON.OBJKEYS key [path]
JSON.OBJLEN key [path]
JSON.FORGET key [path]
JSON.RESP key [path]
JSON.DEBUG MEMORY key [path]
JSON.DEBUG FIELDS key [path]
JSON.MGET key [key ...] path
```

**实现方案**: 集成 jsoncons 或类似的高性能 JSON 库

---

#### 4.4.2 Time Series

```
TS.CREATE key [RETENTION retentionPeriod] [ENCODING encoding] [CHUNK_SIZE size] [DUPLICATE_POLICY policy] [LABELS label value [label value ...]]
TS.ALTER key [RETENTION retentionPeriod] [CHUNK_SIZE size] [DUPLICATE_POLICY policy] [LABELS label value [label value ...]]
TS.ADD key timestamp value [RETENTION retentionPeriod] [ENCODING encoding] [CHUNK_SIZE size] [ON_DUPLICATE policy] [LABELS label value [label value ...]]
TS.MADD [key timestamp value [key timestamp value ...]]
TS.INCRBY key value [TIMESTAMP timestamp] [RETENTION retentionPeriod] [UNCOMPRESSED] [CHUNK_SIZE size] [ON_DUPLICATE policy] [LABELS label value [label value ...]]
TS.DECRBY key value [TIMESTAMP timestamp] [RETENTION retentionPeriod] [UNCOMPRESSED] [CHUNK_SIZE size] [ON_DUPLICATE policy] [LABELS label value [label value ...]]
TS.DEL key fromTimestamp toTimestamp
nTS.GET key
TS.MGET FILTER filter [WITHLABELS]
TS.RANGE key fromTimestamp toTimestamp [COUNT count] [AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
TS.REVRANGE key fromTimestamp toTimestamp [COUNT count] [AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
TS.MRANGE fromTimestamp toTimestamp [LATEST] [FILTER_BY_TS TS...] [FILTER_BY_VALUE min max] [COUNT count] [WITHLABELS] [GROUPBY label REDUCE reducer] [FILTER filter [filter ...]] [AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
```

---

#### 4.4.3 Probabilistic 数据结构

**Bloom Filter**:
```
BF.RESERVE key error_rate capacity [EXPANSION expansion] [NONSCALING]
BF.ADD key item
BF.MADD key item [item ...]
BF.INSERT key [CAPACITY capacity] [ERROR error_rate] [EXPANSION expansion] [NOCREATE] [NONSCALING] ITEMS item [item ...]
BF.EXISTS key item
BF.MEXISTS key item [item ...]
BF.SCANDUMP key iterator
BF.LOADCHUNK key iterator data
BF.INFO key
BF.DEBUG key
n```

**Cuckoo Filter**:
```
CF.RESERVE key capacity [BUCKETSIZE bucketsize] [MAXITERATIONS maxiterations] [EXPANSION expansion]
CF.ADD key item
CF.ADDNX key item
CF.INSERT key [CAPACITY capacity] [NOCREATE] ITEMS item [item ...]
CF.INSERTNX key [CAPACITY capacity] [NOCREATE] ITEMS item [item ...]
CF.EXISTS key item
CF.DEL key item
CF.COUNT key item
CF.SCANDUMP key iterator
CF.LOADCHUNK key iterator data
CF.INFO key
```

---

### 4.5 架构改进

#### 4.5.1 命令注册系统增强

支持 Redis 8.x 的命令分类:

```go
// database/router.go 改进

type CommandCategory string

const (
    CatRead          CommandCategory = "@read"
    CatWrite         CommandCategory = "@write"
    CatAdmin         CommandCategory = "@admin"
    CatDangerous     CommandCategory = "@dangerous"
    CatSearch        CommandCategory = "@search"        // Redis 8
    CatJSON          CommandCategory = "@json"          // Redis 8
    CatTimeSeries    CommandCategory = "@timeseries"    // Redis 8
    CatBloom         CommandCategory = "@bloom"         // Redis 8
    CatCuckoo        CommandCategory = "@cuckoo"        // Redis 8
    CatTopK          CommandCategory = "@topk"          // Redis 8
    CatCMS           CommandCategory = "@cms"           // Redis 8
    CatTDigest       CommandCategory = "@tdigest"       // Redis 8
    CatVector        CommandCategory = "@vector"        // Redis 8
)

type CommandMetadata struct {
    Name        string
    Executor    ExecFunc
    Prepare     PreFunc
    Rollback    RollbackFunc
    Arity       int
    Flags       []CommandFlag
    Categories  []CommandCategory  // 命令所属分类
    ACLFlags    []string           // ACL 相关标志
    KeySpecs    []KeySpec          // 键规范 (Redis 7.0+)
    Tips        []string           // 客户端提示
}

type KeySpec struct {
    Flags       []string  // READ, WRITE, INACCESSIBLE
    BeginSearch string    // INDEX, KEYWORD
    FindKeys    string    // RANGE, KEYNUM
}

func (cm *CommandMetadata) HasCategory(cat CommandCategory) bool
func GetCommandsByCategory(cat CommandCategory) []*CommandMetadata
```

#### 4.5.2 响应协议升级

支持 RESP3 (Redis 6.0+):

```go
// redis/protocol/resp3.go
package protocol

// RESP3 类型
const (
    _ byte = ':' // Integer
    _ byte = '+' // Simple String
    _ byte = '-' // Error
    _ byte = '$' // Blob String
    _ byte = '*' // Array
    // RESP3 新增
    _ byte = '_' // Null
    _ byte = '#' // Boolean
    _ byte = ',' // Double
    _ byte = '(' // Big Number
    _ byte = '!' // Blob Error
    _ byte = '=' // Verbatim String
    _ byte = '%' // Map
    _ byte = '~' // Set
    _ byte = '|' // Attributes
    _ byte = '`' // Push
)

type MapReply struct {
    Data map[Reply]Reply
}

type SetReply struct {
    Data map[Reply]struct{}  // 无序集合
}

type PushReply struct {
    Kind string
    Data []Reply
}
```

#### 4.5.3 集群模式增强

```go
// cluster/core/cluster.go 改进

type Cluster struct {
    // 现有字段...
    
    // Redis 8.x 新增
    ShardedPubSub   *ShardedPubSubManager  // 分片 Pub/Sub
    SlotStats       *SlotStatsManager       // 槽位统计
    ClientTracking  *ClientTrackingManager  // 客户端追踪
}

// 分片 Pub/Sub
func (c *Cluster) SPublish(channel string, message []byte) int
func (c *Cluster) SSubscribe(channels []string, conn redis.Connection)
func (c *Cluster) SUnsubscribe(channels []string, conn redis.Connection)

// 槽位迁移优化
func (c *Cluster) MigrateSlot(slot int, targetNode string) error
func (c *Cluster) ImportSlot(slot int, sourceNode string) error
func (c *Cluster) SetSlot(slot int, state SlotState, nodeID string)
```

---

## 5. 实施路线图

### Phase 1: 核心基础 (8-10 周)

| 周次 | 任务 | 产出 |
|------|------|------|
| 1-2 | Stream 数据结构设计 | stream/ 包基础实现 |
| 3-4 | Stream 命令实现 | XADD, XREAD, XRANGE 等核心命令 |
| 5-6 | Stream Consumer Group | XGROUP, XREADGROUP, XACK |
| 7-8 | Lua Scripting 集成 | scripting/ 包，EVAL/EVALSHA |
| 9-10 | Hash 字段级过期 | HGETEX, HSETEX, HGETDEL |

### Phase 2: 安全与连接 (4-6 周)

| 周次 | 任务 | 产出 |
|------|------|------|
| 1-2 | ACL 系统设计 | acl/ 包基础实现 |
| 3-4 | ACL 命令实现 | ACL SETUSER, ACL GETUSER 等 |
| 5-6 | Connection 管理增强 | CLIENT 系列命令 |

### Phase 3: 高级功能 (6-8 周)

| 周次 | 任务 | 产出 |
|------|------|------|
| 1-3 | Vector Set 实现 | vectorset/ 包 |
| 4-5 | HyperLogLog | hyperloglog/ 包 |
| 6-7 | RESP3 协议支持 | protocol/resp3.go |
| 8 | 测试与优化 | 单元测试、性能测试 |

### Phase 4: 扩展功能 (4-6 周)

| 周次 | 任务 | 产出 |
|------|------|------|
| 1-2 | JSON 数据类型 | json/ 包 |
| 3-4 | Pub/Sub 增强 | Pattern 订阅、分片 Pub/Sub |
| 5-6 | 集群功能增强 | Slot 迁移优化 |

### Phase 5: 专业功能 (8-10 周)

| 周次 | 任务 | 产出 |
|------|------|------|
| 1-3 | Time Series | timeseries/ 包 |
| 4-6 | Probabilistic 数据结构 | bloom/, cuckoo/, cms/, topk/ |
| 7-8 | Redis Functions | functions/ 包 |
| 9-10 | Search 引擎 | search/ 包 (简化版) |

---

## 附录 A: Redis 8.x 完整命令清单

### A.1 核心命令组

<details>
<summary>String (22个命令)</summary>

```
APPEND, DECR, DECRBY, GET, GETDEL, GETEX, GETRANGE, GETSET, INCR, INCRBY, 
INCRBYFLOAT, MGET, MSET, MSETNX, PSETEX, SET, SETEX, SETNX, SETRANGE, STRLEN, 
SUBSTR, GETBIT, SETBIT, BITCOUNT, BITPOS, BITOP, BITFIELD, BITFIELD_RO
```
</details>

<details>
<summary>List (17个命令)</summary>

```
BLMOVE, BLMPOP, BLPOP, BRPOP, BRPOPLPUSH, LINDEX, LINSERT, LLEN, LMOVE, 
LMPOP, LPOP, LPOS, LPUSH, LPUSHX, LRANGE, LREM, LSET, LTRIM, RPOP, RPOPLPUSH, 
RPUSH, RPUSHX
```
</details>

<details>
<summary>Hash (19个命令)</summary>

```
HDEL, HEXISTS, HGET, HGETALL, HGETDEL, HGETEX, HINCRBY, HINCRBYFLOAT, HKEYS, 
HLEN, HMGET, HMSET, HRANDFIELD, HSCAN, HSET, HSETEX, HSETNX, HSTRLEN, HVALS
```
</details>

<details>
<summary>Set (16个命令)</summary>

```
SADD, SCARD, SDIFF, SDIFFSTORE, SINTER, SINTERCARD, SINTERSTORE, SISMEMBER, 
SMEMBERS, SMISMEMBER, SMOVE, SPOP, SRANDMEMBER, SREM, SSCAN, SUNION, SUNIONSTORE
```
</details>

<details>
<summary>Sorted Set (29个命令)</summary>

```
BZMPOP, BZPOPMAX, BZPOPMIN, ZADD, ZCARD, ZCOUNT, ZDIFF, ZDIFFSTORE, ZINCRBY, 
ZINTER, ZINTERCARD, ZINTERSTORE, ZLEXCOUNT, ZMPOP, ZMSCORE, ZPOPMAX, ZPOPMIN, 
ZRANDMEMBER, ZRANGE, ZRANGEBYLEX, ZRANGEBYSCORE, ZRANGESTORE, ZRANK, ZREM, 
ZREMRANGEBYLEX, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREVRANGE, ZREVRANGEBYLEX, 
ZREVRANGEBYSCORE, ZREVRANK, ZSCAN, ZSCORE, ZUNION, ZUNIONSTORE
```
</details>

<details>
<summary>Stream (25个命令)</summary>

```
XACK, XADD, XAUTOCLAIM, XCLAIM, XDELEX, XDEL, XGROUP, XINFO, XLEN, XPENDING, 
XRANGE, XREAD, XREADGROUP, XREVRANGE, XSETID, XTRIM, XACKDEL
```
</details>

<details>
<summary>JSON (32个命令)</summary>

```
JSON.ARRAPPEND, JSON.ARRINDEX, JSON.ARRINSERT, JSON.ARRLEN, JSON.ARRPOP, 
JSON.ARRTRIM, JSON.DEBUG, JSON.DEL, JSON.FORGET, JSON.GET, JSON.MGET, 
JSON.NUMINCRBY, JSON.NUMMULTBY, JSON.OBJKEYS, JSON.OBJLEN, JSON.RESP, 
JSON.SET, JSON.STRAPPEND, JSON.STRLEN, JSON.TOGGLE, JSON.TYPE
```
</details>

<details>
<summary>Vector Set (10个命令)</summary>

```
VS.ADD, VS.CARD, VS.DIM, VS.GET, VS.MGET, VS.QUERY, VS.REM, VS.SEARCH, VS.SIM
```
</details>

### A.2 管理命令组

<details>
<summary>Server (45+命令)</summary>

```
ACL, BGREWRITEAOF, BGSAVE, CLIENT, CLUSTER, COMMAND, CONFIG, DBSIZE, DEBUG, 
FAILOVER, FLUSHALL, FLUSHDB, INFO, LASTSAVE, LATENCY, LCS, LOLWUT, MEMORY, 
MIGRATE, MODULE, MONITOR, PFDEBUG, PFSELFTEST, PING, POST, PSYNC, REPLCONF, 
RESTORE, RESTORE-ASKING, ROLE, SAVE, SHUTDOWN, SLAVEOF, SLOWLOG, SWAPDB, 
SYNC, TIME
```
</details>

<details>
<summary>Connection (12个命令)</summary>

```
AUTH, CLIENT, ECHO, HELLO, PING, QUIT, READONLY, READWRITE, SELECT, SWAPDB
```
</details>

---

## 附录 B: 测试策略

### B.1 单元测试覆盖目标

| 模块 | 目标覆盖率 |
|------|-----------|
| datastruct/* | 90%+ |
| database/* | 85%+ |
| redis/protocol | 90%+ |
| redis/parser | 85%+ |
| cluster/* | 80%+ |

### B.2 集成测试

1. **Redis 兼容性测试**: 使用 redis-rb-test 或类似套件
2. **集群测试**: 3/5/7 节点集群场景
3. **故障恢复测试**: 节点故障、网络分区
4. **性能基准测试**: redis-benchmark 对比

### B.3 混沌测试

```bash
# 使用 toxiproxy 模拟网络延迟/丢包
# 使用 kill -STOP/CONT 模拟节点暂停
# 使用 iptables 模拟网络分区
```

---

## 附录 C: 性能目标

| 指标 | 当前 | 目标 (Redis 8.x 90%) |
|------|------|---------------------|
| SET | 158K ops/s | 300K+ ops/s |
| GET | 157K ops/s | 350K+ ops/s |
| LPUSH | 151K ops/s | 280K+ ops/s |
| ZADD | 165K ops/s | 250K+ ops/s |
| 内存占用 | 基准 | <= Redis 的 1.2x |
| P99 延迟 | 基准 | <= Redis 的 1.5x |

---

## 结语

本方案旨在系统性地将 Godis 提升至与 Redis 8.x 100% 兼容的水平。建议采用渐进式实现策略，优先完成 P0 级别功能，确保核心可用性，再逐步扩展到高级功能。

**文档版本**: 1.0  
**最后更新**: 2026-03-07  
**作者**: AI Assistant
