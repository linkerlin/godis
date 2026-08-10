# Changelog

本文件遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/) 格式。

## [Unreleased]

### Fixed

- 纯 AOF rewrite：`HPEXPIREAT` 补 `FIELDS n field…`，否则 LoadAof 语法错导致 hash 字段 TTL 丢失
- PSYNC 增量：`isValidOffset` 接纳 tip（`offset == currentOffset`）与空 backlog，已追上的副本重连走 `CONTINUE` 而非误 `FULLRESYNC`（与 `getSnapshotAfter` 上界一致）
- 消除 `TestM2amClientKillLAddrMaxAge` / `TestM2boInfoKeyspaceAvgTTLAndSubexpiry` / `TestM2buLatencyHistogram` 全量跑 flake：`LATENCY RESET` 在 Exec 采样回写后再次清空 histogram；KILL 按预先捕获的 client id 断言并放宽计数；avg_ttl 对照 PTTL 且关闭主动过期干扰
- `MEMORY STATS` / `INFO memory`：`peak.allocated`/`used_memory_peak` 跟踪 `runtime.MemStats.Alloc` 高水位（不再用 `TotalAlloc` 冒充峰值）；`overhead.total` 与 INFO 一致为 `Alloc−dataset`；Limiter 默认用量改 `Alloc`（仍非 jemalloc）
- `CLUSTER` 迁移闭环缝：`execFinishExport` 成功才 `dropSlot`+清 `importingTask`、失败不丢键；真实 migrate 设 `IMPORTING`/`migratePeer`；`startExporting` 对 SETSLOT MIGRATING 幂等；ASK 可读 FSM `Migratings`（不强制本地 exporting）；`SETSLOT NODE` 转发后在本节点清迁移态
- Failover / 复制关闭死锁：`stopSlaveWithMutex` 不再持 `slaveStatus.mutex` 等待 `receiveAOF`；`Connection.Write` 串行化（Windows 并发 net.Conn.Write 会卡 fdMutex）；`Close` 停 `startReplCron`；测试 listener 关闭已接受连接；`TestSlaveCloseAfterReplicationDoesNotHang`
- Failover 集成测加固：关 TCP listener、`UnregisterClient`、短 `TIMEOUT`、`failoverState` Cleanup（缓解 Windows 挂起 flake）
- 测试对齐 RESP3 双形回复：`BZPOPMIN` MultiRaw+Double、`ZRANGE WITHSCORES` ScorePairs、`ACL LOG` Map 数组；空 `ACL LOG` 统一为 `MultiRawReply`
- `TestP8FTPersistence`：恢复全局 `config.Properties` 并用 `filepath.Join`，避免 Windows 上误删临时目录后拖垮后续用例

### Added

- `CLUSTER GETKEYSINSLOT`：返回本节点 `slotsManager` 登记的槽内键（与 `COUNTKEYSINSLOT` 同源）
- `CLUSTER REPLICAS`/`SLAVES`：从 Raft FSM `MasterSlaves` 返回副本 NODES 行；未知节点 `ERR Unknown node`
- `INFO cluster`：有 Cluster 实例时与 `CLUSTER INFO` 同源（`SetClusterInfoSectionProvider`）
- Stream opaque：保留 `entriesAdded` / `maxDeletedID`（及既有 consumers/PEL）
- `CLUSTER MEET ip port [raft-port]`：走 Raft `AddToRaft`+`EventJoin`（或 FSM-only）；**非** Redis gossip；Raft 就绪时必须带 `raft-port`
- `CLUSTER SETSLOT` `MIGRATING`/`IMPORTING`/`STABLE`：更新本地 `slotsManager`，与 ASK/ASKING 一致；`NODE` 写 Raft FSM 归属（`EventAssignSlots`）并清理本地迁移态
- Hash field TTL：`HEXPIRE`/`HPEXPIRE`/`HEXPIREAT`/`HPEXPIREAT` 命令本身写 AOF；字段到期（时间轮主动 / 访问惰性）写 AOF `HDEL`；主动过期持 DB 键写锁；键空间 `hexpired`
- 集群槽位统一为 **CRC16-XMODEM % 16384**（`lib/hashslot`），Sharded Pub/Sub 共用；单键 `MOVED`/`ASK`/`ASKING`/`READONLY`
- `CLUSTER NODES` / `SLOTS` / `INFO` / `SHARDS` 从 Raft FSM 读取真拓扑（无 FSM 时本节点占满 0–16383）
- `CLUSTER ADDSLOTS` / `DELSLOTS` / `*RANGE` / `FLUSHSLOTS` 写入 Raft FSM（`EventAddSlots`/`EventRemoveSlots`）
- TimeSeries opaque（AOF/RDB）保留 `DuplicatePolicy` 与 `ChunkSize` 与 `DownsampleRules`（TS.CREATERULE）
- Vector opaque 保留 item `Attributes`（VSETATTR）
- maxmemory 估账：`approxKeyMemoryUsage` 按实体估算（大 value 计入；小键仍不低于 128B floor）；写路径 `approxCmdWriteBytes`；仍为近似，非 jemalloc
- RESP 解析器 Fuzz 测试（`FuzzParseOne` / `FuzzParseBytes` / `FuzzParseV2`）及 fuzz corpus
- 解析器 bulk/array 长度上限（512 MiB bulk、1M 数组元素），防止恶意帧 OOM/挂起
- `lib/validate` 单元测试
- 集群 `doMigrateSlot` 实现与测试
- AOF/RDB 后台 rewrite 互斥锁（`ErrRewriteInProgress`）
- ACL 启动集成、统一 AUTH/HELLO、命令级 ACL 权限检查
- std 服务器优雅关闭（in-flight 等待 + 超时）
- `INFO` 复制/RDB 字段与 `CLIENT PAUSE/UNPAUSE` 加锁
- `docs/COMPATIBILITY.md` 兼容性矩阵
- CI：`go vet` 与选定包的 `go test -race`
- gnet 服务器优雅关闭（`closing` + in-flight 等待）
- `commands.md` 与代码对齐；`database/caching.go` 单元测试
- RediSearch 全面对齐 Redis 8.x（P1~P10 + GEOSHAPE + FT.HYBRID + COLLECT + VAMANA，63 测试；见 `docs/REDISEARCH_ALIGNMENT.md`）
- Redis 8 全兼容修复批次（过期语义 / ACL 安全 / 协议 / 命令补齐 / 行为对齐 / 配置 / 元数据 / 键空间事件 / HLL 重构 / Stream 性能 / DEBUG 收口；见 `docs/REDIS8_COMPAT_AUDIT.md`）
- `ZINTERCARD` / `SORT_RO` / `EVAL_RO` / `EVALSHA_RO` 命令
- `datastruct/hll`（Redis 兼容 dense 编码 + xxHash64）与 `datastruct/stream` 有序存储
- `FAILOVER` 真实协调切换：等待目标从库 ACK 同步 → 复制流注入 `REPLCONF FAILOVER` → 从库自提升、原主降级为从（FORCE/ABORT/TIMEOUT、ABORT 状态机、并发 FAILOVER 互斥；设计见 `docs/FAILOVER_DESIGN.md`）

### Changed

- Legacy Lua（`GODIS_LUA_ENGINE=legacy`）：拒绝 `os.`/`io.`/`package.`/`debug.`/`require`/`dofile`/`loadfile`（对齐 gopher-lua SkipOpenLibs 意图）
- `CLUSTER BUMPEPOCH`：明确为 no-op（`BUMPED 0`；FSM 无 config epoch；远期/非目标：真 epoch / gossip）
- 文档明确非「100% 兼容」：以 `docs/COMPATIBILITY.md` 与 `commands.md` 为准；opaque 扩展类型非 Redis 原生模块 RDB 互通；远期项见 COMPATIBILITY「仍延期/非本轮」

### Security

- gopher-lua 引擎默认 `SkipOpenLibs`，仅开放 base/table/string/math；拒绝 `os`/`io`/`require`/`dofile`/`loadfile`；legacy 引擎同等关键字拒绝（`TestEvalDeniesOsExecute` 双引擎）
- 修复 ACL 未启动、`execAuth` 在无 ACL 时直接返回 OK 的认证绕过
- 集群命令路径接入完整 ACL 校验（之前仅 requirepass）

### Fixed

- maxmemory 淘汰路径未写 AOF `DEL`：主从复制积压（由 AOF 驱动）收不到删除，淘汰后从库仍残留键；现与过期路径一致并发送键空间 `evicted`
- Hash field TTL 惰性/主动过期未写 AOF `HDEL`（及未接时间轮）：主从不一致；现与键过期路径对齐；`HEXPIRE*` 命令本身此前未写 AOF
- `CLUSTER SETSLOT REPLICATE` / `FAILOVER` / `FORGET` / `RESET` / `SAVECONFIG` / `SET-CONFIG-EPOCH` 对未实现写路径返回明确 `ERR … is not supported`（不再假 OK）
- `parse0` panic 时未关闭 channel 导致 `ParseOne` 可能挂起
- `parseArray` 内层 bulk 未校验长度导致超大 `make` 分配
- `go vet`：`database/stream.go` 未键控 `StreamID` 字面量；`tcp/server.go` signal channel 缓冲
- ACL `+@all` 被误解析为 category 的 bug
- EXPIRE/PEXPIRE 非正 TTL 不删键、时间轮小数秒 TTL 永不主动过期、主库过期不传播从库
- WATCH 失败写命令误报版本变更
- 集群模式完全绕过 ACL；ACL 类别表残缺（122 命令无类别）
- CLIENT TRACKINGINFO 嵌套数组格式损坏；协议错误回复缺 `ERR Protocol error:` 前缀
- SETBIT 超限 OOM；GETEX 只读标志错误；RESET 不清认证
- HLL 计数恒 1（非新键不写回）；PFADD 存储类型与 Redis 不一致
- Stream XRANGE/TRIM O(n²)
- ACL Authenticate 不检查 `user.Enabled`
- 复制：receiveAOF 持 `slaveStatus.mutex` 时 Exec 导致 GETACK 自死锁（复制首次 GETACK 即冻结）；REPLCONF announce 在 PSYNC 前到达被丢弃；主库推送负 offset 切片越界 panic；`saveForReplication` 未关闭 TempFile 句柄（Windows rename Access denied → 全量同步失败）
- FLUSHDB/FLUSHALL 后 `loadDB` 未继承 `server`/`lockManager`/`evictionManager`，新 DB 的 `db.server` 为 nil → CLIENT PAUSE、UNPAUSE 及所有 `db.server` 支撑路径失效
- `OBJECT ENCODING` 对 HLL 返回 raw 而非 `hyperloglog`；RESTORE 的 IDLETIME/FREQ 被索引重建的内部读取（Touch）覆盖 → OBJECT IDLETIME 恒 0
---

## 更早版本

详见 `docs/history/` 中的 2026-04/2026-06 审阅与兼容性报告，以及 [`改进意见_2026-06.md`](改进意见_2026-06.md) 实施索引。
