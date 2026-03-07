# AGENTS.md

本文档为在 Godis 代码库中工作的 AI 代理提供必要信息——Godis 是一个 Go 语言实现的 Redis 兼容服务器。

## 项目概述

**Godis** 是一个用 Go 语言编写的 Redis 兼容服务器，支持：
- String（字符串）、List（列表）、Hash（哈希）、Set（集合）、Sorted Set（有序集合）、Bitmap（位图）、GEO（地理位置）数据类型
- TTL（生存时间）
- 发布/订阅
- AOF 持久化和 RDB 读写
- 事务（原子性，支持回滚）
- 主从复制
- 基于 Raft 的集群

## 常用命令

### 构建与运行
```bash
# 构建
go build -o godis ./

# 使用默认配置运行（监听 0.0.0.0:6399）
./godis

# 使用配置文件运行
CONFIG=redis.conf ./godis

# 集群模式运行（以 2 节点为例）
CONFIG=node1.conf ./godis &
CONFIG=node2.conf ./godis &
```

### 测试
```bash
# 运行所有测试
go test ./...

# 运行测试并生成覆盖率报告
go test -v -coverprofile=profile.cov ./...

# 运行特定包的测试
go test -v ./database/...
go test -v ./datastruct/...
```

### 交叉编译
```bash
# 多平台构建
./build-all.sh
# 生成的二进制文件位于 target/ 目录
```

## 项目结构

```
godis/
├── main.go                 # 入口点，服务器初始化
├── config/                 # 配置解析器
├── interface/              # 接口定义
│   ├── database/           # 数据库接口，DataEntity
│   └── redis/              # 连接接口
├── lib/                    # 工具库
│   ├── logger/             # 日志
│   ├── utils/              # 辅助函数（ToCmdLine 等）
│   ├── pool/               # 缓冲区和连接池
│   ├── timewheel/          # 延迟任务调度（用于 TTL）
│   ├── wildcard/           # 模式匹配
│   ├── consistenthash/     # 一致性哈希（集群）
│   ├── sync/               # 同步原语
│   └── validate/           # 输入验证
├── tcp/                    # TCP 服务器实现
├── redis/
│   ├── protocol/           # RESP 协议（回复类型，错误）
│   ├── parser/             # 协议解析器
│   ├── client/             # 客户端实现
│   ├── connection/         # 连接管理
│   └── server/             # 服务器实现（std 和 gnet）
├── datastruct/             # 核心数据结构
│   ├── dict/               # 并发哈希表（分片锁）
│   ├── list/               # 链表和快速列表
│   ├── set/                # 哈希集合
│   ├── sortedset/          # 基于跳表的有序集合
│   ├── bitmap/             # 位图操作
│   └── lock/               # 键级锁
├── database/               # 存储引擎核心
│   ├── database.go         # DB 结构体和基本操作
│   ├── server.go           # 多数据库服务器
│   ├── router.go           # 命令表和注册
│   ├── commandinfo.go      # 命令元数据
│   ├── string.go           # 字符串命令
│   ├── list.go             # 列表命令
│   ├── hash.go             # 哈希命令
│   ├── set.go              # 集合命令
│   ├── sortedset.go        # 有序集合命令
│   ├── keys.go             # 键命令（DEL、EXPIRE 等）
│   ├── transaction.go      # 事务支持
│   ├── persistence.go      # AOF 集成
│   ├── replication_*.go    # 主从复制
│   └── geo.go              # 地理位置命令
├── cluster/                # 集群模式
│   ├── cluster.go          # 集群初始化
│   ├── core/               # 集群核心逻辑
│   │   ├── core.go         # 集群结构体
│   │   ├── node_manager.go # 节点管理
│   │   └── tcc.go          # 分布式事务
│   ├── commands/           # 集群感知命令
│   └── raft/               # Raft 共识
├── aof/                    # AOF 持久化
│   ├── aof.go              # AOF 写入
│   ├── rewrite.go          # AOF 重写
│   └── rdb.go              # RDB 格式支持
└── pubsub/                 # 发布/订阅实现
```

## 代码模式

### 命令注册模式

命令在 `init()` 函数中使用 `registerCommand()` 注册：

```go
// 在 database/string.go 中
func init() {
    registerCommand("Set", execSet, writeFirstKey, rollbackFirstKey, -3, flagWrite).
        attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
    registerCommand("Get", execGet, readFirstKey, nil, 2, flagReadOnly).
        attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
}
```

**注册参数：**
1. 命令名称（不区分大小写）
2. 执行函数：`func(db *DB, args [][]byte) redis.Reply`
3. 准备函数：返回读/写键用于加锁
4. 回滚函数：生成回滚命令
5. 参数数量：期望的参数个数（负数表示 >= -arity）
6. 标志：`flagWrite`、`flagReadOnly`、`flagSpecial`

### 命令处理器模式

```go
func execSet(db *DB, args [][]byte) redis.Reply {
    key := string(args[0])
    value := args[1]
    // ... 逻辑 ...
    entity := &database.DataEntity{Data: value}
    db.PutEntity(key, entity)
    db.addAof(utils.ToCmdLine3("set", args...))
    return &protocol.OkReply{}
}
```

### 准备函数（用于加锁）

```go
// 对第一个键加写锁
func writeFirstKey(args [][]byte) ([]string, []string) {
    return []string{string(args[0])}, nil
}

// 对第一个键加读锁
func readFirstKey(args [][]byte) ([]string, []string) {
    return nil, []string{string(args[0])}
}

// 多个键（如 MSET）
func prepareMSet(args [][]byte) ([]string, []string) {
    size := len(args) / 2
    keys := make([]string, size)
    for i := 0; i < size; i++ {
        keys[i] = string(args[2*i])
    }
    return keys, nil
}
```

### 回复类型（redis/protocol/reply.go）

```go
// 字符串/二进制数据
protocol.MakeBulkReply([]byte("value"))     // $5\r\nvalue\r\n
protocol.MakeNullBulkReply()                // $-1\r\n

// 整数
protocol.MakeIntReply(42)                   // :42\r\n

// 状态/OK
protocol.MakeStatusReply("OK")              // +OK\r\n
protocol.MakeOkReply()                      // +OK\r\n

// 错误
protocol.MakeErrReply("ERR message")        // -ERR message\r\n
protocol.MakeSyntaxErrReply()               // -ERR syntax error\r\n

// 数组
protocol.MakeMultiBulkReply([][]byte{...})  // *n\r\n...
```

### 测试模式

```go
// 创建测试数据库
var testDB = makeTestDB()
var testServer = MustNewStandaloneServer()

// 执行命令
result := testDB.Exec(nil, utils.ToCmdLine("SET", "key", "value"))

// 断言结果类型
bulkReply, ok := result.(*protocol.BulkReply)
if !ok {
    t.Errorf("expected bulk reply, got %s", result.ToBytes())
}

// 使用断言辅助函数
asserts.AssertBulkReply(t, result, "expected value")
```

### 工具函数（lib/utils/utils.go）

```go
// 将字符串转换为命令行格式
utils.ToCmdLine("SET", "key", "value")      // [][]byte
utils.ToCmdLine2("SET", "key", "value")     // [][]byte（包含命令名）
utils.ToCmdLine3("set", []byte("key"), ...) // 用于 AOF

// 比较字节切片
utils.BytesEquals(a, b) bool

// 将 Redis 索引转换为 Go 切片索引
utils.ConvertRange(start, end, size) (int, int)
```

## 关键架构要点

### 并发字典（datastruct/dict/concurrent.go）

使用分片锁实现并发：
- 键通过 FNV-32 哈希分布到各个分片
- 每个分片有自己的 RWMutex
- `RWLocks()`/`RWUnLocks()` 按排序顺序锁定多个键以防止死锁

### DB 结构（database/database.go）

```go
type DB struct {
    index       int
    data        *dict.ConcurrentDict  // key -> DataEntity
    ttlMap      *dict.ConcurrentDict  // key -> expireTime
    versionMap  *dict.ConcurrentDict  // key -> version（用于 WATCH）
    addAof      func(CmdLine)         // AOF 回调
}
```

### 事务流程（database/transaction.go）

1. `MULTI` - 开始事务状态
2. 命令被排队（不执行）
3. `EXEC` - 使用加锁原子地执行所有命令
4. 出错时：使用回滚日志回滚

### 集群模式（cluster/）

- 使用 Hashicorp Raft 进行共识
- 键通过一致性哈希分布（槽位）
- 跨节点操作使用 TCC（Try-Commit-Cancel）模式
- `MSET`、`DEL`、`RENAME` 可以跨节点原子执行

## 配置

配置文件格式（参见 `example.conf`）：

```
bind 0.0.0.0
port 6399
databases 16
appendonly no
appendfilename appendonly.aof
requirepass yourpassword

# 集群模式
cluster-enable yes
raft-listen-address 0.0.0.0:16666
cluster-as-seed yes
cluster-seed 127.0.0.1:6399
```

环境变量 `CONFIG` 指定配置文件路径。

## 常见注意事项

1. **键验证**：在字符串命令中使用 `validate.ValidateKey()` 和 `validate.ValidateValue()` 进行大小限制检查。

2. **AOF 写入**：修改数据后始终调用 `db.addAof()`，对字节参数使用 `utils.ToCmdLine3()`。

3. **加锁**：命令必须通过准备函数声明读/写键。锁在执行前获取。

4. **类型断言**：DataEntity.Data 是 `interface{}`。使用前必须进行类型断言：
   ```go
   bytes, ok := entity.Data.([]byte)
   if !ok {
       return &protocol.WrongTypeErrReply{}
   }
   ```

5. **TTL 处理**：获取实体后检查 `db.IsExpired(key)`。

6. **命令命名**：注册使用小写，但命令匹配不区分大小写。

7. **集群命令**：位于 `cluster/commands/`，不在 `database/`。使用不同的注册模式。

8. **错误处理**：使用 `github.com/cockroachdb/errors` 进行错误包装。

## 依赖

- `github.com/hashicorp/raft` - Raft 共识
- `github.com/hashicorp/raft-boltdb` - Raft 存储
- `github.com/hdt3213/rdb` - RDB 格式支持
- `github.com/panjf2000/gnet/v2` - 高性能网络（可选）
- `github.com/cockroachdb/errors` - 错误处理

## 支持的命令

完整列表请参见 `commands.md`。主要类别：
- 键：DEL、EXPIRE、TTL、EXISTS、TYPE、RENAME
- 字符串：SET、GET、INCR、APPEND、MSET、MGET
- 列表：LPUSH、RPUSH、LPOP、RPOP、LRANGE、LLEN
- 哈希：HSET、HGET、HGETALL、HINCRBY
- 集合：SADD、SREM、SINTER、SUNION、SPOP
- 有序集合：ZADD、ZRANGE、ZREM、ZSCORE
- 发布/订阅：PUBLISH、SUBSCRIBE、UNSUBSCRIBE
- 地理位置：GEOADD、GEOPOS、GEODIST、GEORADIUS
