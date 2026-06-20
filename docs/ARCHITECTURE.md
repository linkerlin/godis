# Godis 架构概览

## 进程与网络

```
Client (redis-cli)
    │ RESP2/3 TCP
    ▼
tcp.Listener  ──►  std.Handler / gnet.GnetServer
                        │
                        ▼
                   Server.Exec (database)
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
       DB[0..N]      pubsub.Hub    ACL / AUTH
          │
          ▼
    dict.ConcurrentDict (keys → DataEntity)
```

- **std**：每连接一 goroutine，适合开发与小规模部署
- **gnet**：事件驱动，适合高并发（`use-gnet yes`）

## 命令执行路径

1. `Server.Exec(conn, cmdLine)` — 认证、ACL、`SELECT`、事务、`CLIENT PAUSE`
2. `DB.Exec(conn, cmdLine)` — 路由到 `cmdTable` 中注册的命令
3. `prepare` 函数声明读/写键 → `RWLocks` 加锁
4. `executor` 执行 → `RecordCommand` → 客户端缓存 hook

集群模式下 `cluster.MakeCluster()` 实现 `database.DB`，部分命令在 `cluster/commands/` 注册。

## 认证与 ACL

| 层 | 职责 |
|----|------|
| `requirepass` | 遗留单密码；与 default 用户密码联动 |
| `acl.Engine` | 用户、命令类别、键模式 |
| `aclfile` | 启动加载 / `ACL SAVE` 持久化 |
| 连接状态 | `Connection` 上 `IsAuthenticated()` / ACL 用户名 |

写命令前必须满足 ACL；`NOPERM` 与 Redis 语义一致。

## 客户端缓存（RESP3）

- `CLIENT TRACKING ON` → `caching.go` 记录读键
- 写命令成功 → `InvalidateKeysOnWrite` 推送 RESP3 invalidate
- 连接关闭 → `AfterClientClose` → `DisableTracking`

## 持久化

- **AOF**：`aof.Persister` 挂到 `Server`；`db.addAof` 回调
- **RDB**：由 AOF rewrite 生成（Godis 当前策略）
- 混合持久化见 `example.conf`

## 搜索后端

| 后端 | 配置 | 实现 |
|------|------|------|
| `native` | 默认 | `datastruct/redisearch`、`datastruct/vector` |
| `sqlite` | `-tags sqlite_backend` | FTS5 + sqlite-vec，WAL+mmap |

`FT.*` / `VS.*` 命令在 `redisearch.go` / `vector.go` 内按 `currentSearchBackend()` 分发。

## 可观测性

| 组件 | 数据源 |
|------|--------|
| `INFO` | `systemcmd.go` + `tcp.ClientCounter` + `caching` |
| `/metrics` | `lib/metrics` ← `cmdstats` + `lib/stats` |
| SLOWLOG | `Server.slogLogger` |

## 集群限制（当前周期）

- Raft 共识 + 槽位迁移；非全量 Redis Cluster 命令
- 跨槽位 `MSET`/`DEL`/`RENAME` 有部分 TCC 支持
- Sharded Pub/Sub 见 `pubsub/sharded.go`

详细兼容边界见 [`COMPATIBILITY.md`](COMPATIBILITY.md)。
