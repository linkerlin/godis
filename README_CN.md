# Godis

![license](https://img.shields.io/github/license/HDT3213/godis)
[![Build Status](https://github.com/hdt3213/godis/actions/workflows/coverall.yml/badge.svg)](https://github.com/HDT3213/godis/actions?query=branch%3Amaster)
[![Coverage Status](https://coveralls.io/repos/github/HDT3213/godis/badge.svg?branch=master)](https://coveralls.io/github/HDT3213/godis?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/HDT3213/godis)](https://goreportcard.com/report/github.com/HDT3213/godis)
<br>
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge-flat.svg)](https://github.com/avelino/awesome-go)

**Godis** 是一个用 Go 语言实现的 **Redis 8.x 兼容** 服务器。本项目旨在为尝试使用 Go 语言开发高并发中间件的朋友提供参考。

## 关键特性

### 核心数据结构
- String、List、Hash、Set、Sorted Set、Bitmap、Stream
- **JSON 数据类型** (Redis 8.x): JSON.SET, JSON.GET, JSON.ARRAPPEND 等
- **Vector Set 向量集** (Redis 8.x): VS.ADD, VS.SEARCH 用于 AI/向量搜索

### Redis 8.x 新功能

#### 协议与连接
- **RESP3 协议**: 完整支持，包括 Null、Double、Map、Set、Push 类型
- **HELLO 命令**: 协议版本协商
- **客户端缓存**: CLIENT TRACKING + 失效推送 (RESP3)

#### 搜索与分析
- **RediSearch**: FT.CREATE, FT.SEARCH, FT.AGGREGATE, 同义词支持 (FT.SYNADD)
- **Vector Similarity Search**: AI 向量搜索，支持余弦相似度

#### 时序与概率数据结构
- **Time Series**: TS.CREATE, TS.ADD, TS.MRANGE, TS.MGET
- **Bloom Filter**: BF.ADD, BF.EXISTS, BF.RESERVE
- **Cuckoo Filter**: CF.ADD, CF.EXISTS, CF.DEL
- **Count-Min Sketch**: CMS.INCRBY, CMS.QUERY
- **Top-K**: TOPK.ADD, TOPK.QUERY
- **T-Digest**: TD.ADD, TD.QUANTILE

#### 函数与脚本
- **Redis Functions**: FUNCTION LOAD, FCALL 支持 Lua 函数持久化
- **Lua 脚本**: EVAL, EVALSHA, 支持 SCRIPT DEBUG 调试

#### 集群与发布订阅
- **Sharded Pub/Sub**: SSUBSCRIBE, SPUBLISH (Redis 8.x 集群)
- **内置集群**: Raft 共识，透明分片，自动故障转移

#### 安全与管理
- **ACL (访问控制列表)**: ACL LIST, ACL SETUSER, ACL LOG
- **审计日志**: ACL LOG 记录命令执行历史

### 经典功能
- 并行内核，提供优秀的并发性能
- 自动过期功能 (TTL)
- 发布订阅 (Pub/Sub)
- 地理位置 (GEO)
- AOF 持久化、RDB 持久化、混合持久化 (aof-use-rdb-preamble)
- 主从复制
- **事务**: MULTI/EXEC 具有原子性和隔离性，执行出错时自动回滚
- **Hash Field 过期**: HEXPIRE, HTTL (Redis 8.x)

## 快速开始

### 下载运行

在 GitHub Release 页下载对应平台的可执行文件：

```bash
# macOS
./godis-darwin

# Linux
./godis-linux

# Windows
./godis.exe
```

Godis 默认监听 `0.0.0.0:6399`，可使用 redis-cli 或任何 Redis 客户端连接：

```bash
redis-cli -p 6399
127.0.0.1:6399> SET key value
OK
127.0.0.1:6399> GET key
"value"
127.0.0.1:6399> HELLO 3  # 切换到 RESP3
1# "server" => "godis"
...
```

### 配置文件

Godis 从 `CONFIG` 环境变量读取配置文件，或使用工作目录中的 `redis.conf`：

```bash
CONFIG=redis.conf ./godis
```

所有配置项说明请参考 [example.conf](./example.conf)。

### 集群模式

使用提供的配置文件启动多节点集群：

```bash
CONFIG=node1.conf ./godis &
CONFIG=node2.conf ./godis &
```

集群对客户端透明，连接任意节点即可访问所有数据。

## Redis 8.x 功能示例

### JSON 数据类型

```bash
127.0.0.1:6399> JSON.SET user $ '{"name":"张三","age":30}'
OK
127.0.0.1:6399> JSON.GET user $.name
"\"张三\""
127.0.0.1:6399> JSON.NUMINCRBY user $.age 1
"31"
```

### Vector Set 向量搜索

```bash
127.0.0.1:6399> VS.ADD products item1 "[1.0, 0.5, 0.2]"
127.0.0.1:6399> VS.ADD products item2 "[0.9, 0.4, 0.3]"
127.0.0.1:6399> VS.SEARCH products "[1.0, 0.5, 0.2]" KNN 2
1) "item1"
2) "item2"
```

### Redis Functions

```bash
127.0.0.1:6399> FUNCTION LOAD "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return redis.call('GET', keys[1]) end)"
127.0.0.1:6399> FCALL myfunc 1 mykey
"myvalue"
```

### 客户端缓存 (RESP3)

```bash
127.0.0.1:6399> HELLO 3
127.0.0.1:6399> CLIENT TRACKING ON
+OK
127.0.0.1:6399> GET cached_key
# 当 cached_key 被其他客户端修改时，将收到失效推送
```

### Sharded Pub/Sub (集群模式)

```bash
# 订阅分片频道
127.0.0.1:6399> SSUBSCRIBE mychannel

# 在另一个客户端发布
127.0.0.1:6399> SPUBLISH mychannel "hello"
```

## 支持的命令

完整命令列表请参考 [commands.md](./commands.md)

### 命令分类

| 分类 | 命令数 | 说明 |
|------|--------|------|
| 字符串 | 20+ | GET, SET, INCR, APPEND, 等 |
| 列表 | 15+ | LPUSH, RPUSH, LRANGE, 等 |
| 哈希 | 15+ | HSET, HGET, HINCRBY, 等 |
| 集合 | 15+ | SADD, SINTER, SUNION, 等 |
| 有序集合 | 20+ | ZADD, ZRANGE, ZRANGEBYSCORE, 等 |
| Stream | 10+ | XADD, XREAD, XGROUP, 等 |
| JSON | 25+ | JSON.SET, JSON.GET, JSON.ARRAPPEND, 等 |
| Vector Set | 4 | VS.ADD, VS.SEARCH, VS.REM, VS.DROPINDEX |
| RediSearch | 15+ | FT.CREATE, FT.SEARCH, FT.AGGREGATE, 等 |
| Time Series | 10+ | TS.CREATE, TS.ADD, TS.MRANGE, 等 |
| Bloom Filter | 8 | BF.ADD, BF.EXISTS, BF.MADD, 等 |
| Cuckoo Filter | 10 | CF.ADD, CF.EXISTS, CF.DEL, 等 |
| CMS/Top-K/T-Digest | 15 | CMS.INCRBY, TOPK.ADD, TD.ADD, 等 |
| 概率数据结构 | 5 | PFADD, PFCOUNT, 等 |
| 地理位置 | 6 | GEOADD, GEOPOS, GEODIST, 等 |
| 键管理 | 20+ | DEL, EXPIRE, TTL, SCAN, 等 |
| 事务 | 5 | MULTI, EXEC, DISCARD, WATCH, UNWATCH |
| 发布订阅 | 8 | PUBLISH, SUBSCRIBE, PSUBSCRIBE, 等 |
| 集群 | 10+ | CLUSTER NODES, CLUSTER MEET, 等 |
| 管理 | 20+ | INFO, CONFIG, CLIENT, ACL, 等 |
| Lua/函数 | 8 | EVAL, EVALSHA, FUNCTION LOAD, FCALL, 等 |

## 性能测试

环境: Go 1.23, macOS Monterey 12.5 M2 Air

```
PING_INLINE: 179211.45 requests per second, p50=1.031 msec                    
PING_MBULK: 173611.12 requests per second, p50=1.071 msec                    
SET: 158478.61 requests per second, p50=1.535 msec                    
GET: 156985.86 requests per second, p50=1.127 msec                    
INCR: 164473.69 requests per second, p50=1.063 msec                    
LPUSH: 151285.92 requests per second, p50=1.079 msec                    
RPUSH: 176678.45 requests per second, p50=1.023 msec                    
LPOP: 177619.89 requests per second, p50=1.039 msec                    
RPOP: 172413.80 requests per second, p50=1.039 msec                    
SADD: 159489.64 requests per second, p50=1.047 msec                    
HSET: 175131.36 requests per second, p50=1.031 msec                    
SPOP: 170648.45 requests per second, p50=1.031 msec                    
ZADD: 165289.25 requests per second, p50=1.039 msec                    
ZPOPMIN: 185528.77 requests per second, p50=0.999 msec                    
LRANGE_100: 46511.62 requests per second, p50=4.063 msec                   
MSET (10 keys): 88417.33 requests per second, p50=3.687 msec  
```

## 源码阅读指南

```
godis/
├── main.go                 # 入口点
├── config/                 # 配置解析
├── interface/              # 接口定义
├── lib/                    # 工具库 (logger, 同步, 通配符等)
├── tcp/                    # TCP 服务器
├── redis/                  # RESP 协议解析器
├── scripting/              # Lua 引擎和 Redis Functions
├── datastruct/             # 核心数据结构
│   ├── dict/               # 并发哈希表
│   ├── list/               # 链表和快速列表
│   ├── set/                # 哈希集合
│   ├── sortedset/          # 跳表有序集合
│   ├── bitmap/             # 位图
│   ├── json/               # JSON 数据类型
│   ├── vector/             # Vector Set 向量集
│   ├── timeseries/         # 时序数据
│   └── lock/               # 键级锁
├── database/               # 存储引擎
│   ├── database.go         # 单个数据库实现
│   ├── server.go           # 多数据库服务器
│   ├── router.go           # 命令路由
│   ├── string.go           # 字符串命令
│   ├── list.go             # 列表命令
│   ├── hash.go             # 哈希命令
│   ├── set.go              # 集合命令
│   ├── sortedset.go        # 有序集合命令
│   ├── stream.go           # Stream 命令
│   ├── json.go             # JSON 命令
│   ├── vector.go           # Vector Set 命令
│   ├── timeseries.go       # 时序命令
│   ├── rediSearch.go       # 全文搜索
│   ├── bloom.go            # Bloom Filter
│   ├── cuckoo.go           # Cuckoo Filter
│   ├── probal.go           # 概率数据结构
│   ├── acl.go              # ACL 访问控制
│   ├── caching.go          # 客户端缓存
│   ├── transaction.go      # 事务
│   └── geo.go              # 地理位置
├── cluster/                # 集群模式
│   ├── cluster.go          # 集群入口
│   ├── raft/               # Raft 共识
│   └── commands/           # 集群感知命令
├── aof/                    # AOF/RDB 持久化
└── pubsub/                 # 发布订阅
```

## 相关项目

- [ godis-client ](https://github.com/hdt3213/godis): Godis 的 Go 语言客户端
- [ godis-dashboard ](https://github.com/hdt3213/godis-dashboard): Godis 的可视化管理界面

## 博客

在[我的博客](https://www.cnblogs.com/Finley/category/1598973.html)了解更多关于 Godis 的设计与实现。

## 贡献

欢迎提交 Issue 和 PR！请确保：
1. 代码通过 `go test ./...` 测试
2. 新功能包含测试用例
3. 遵循现有代码风格

## 许可证

Godis 采用 [MIT 许可证](./LICENSE)。
