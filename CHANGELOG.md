# Changelog

本文件遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/) 格式。

## [Unreleased]

### Added

- `CLUSTER MEET ip port [raft-port]`：走 Raft `AddToRaft`+`EventJoin`（或 FSM-only）；**非** Redis gossip；Raft 就绪时必须带 `raft-port`
- `CLUSTER SETSLOT` `MIGRATING`/`IMPORTING`/`STABLE`：更新本地 `slotsManager`，与 ASK/ASKING 一致；`NODE` 仍 `ERR not supported`
- 集群槽位统一为 **CRC16-XMODEM % 16384**（`lib/hashslot`），Sharded Pub/Sub 共用；单键 `MOVED`/`ASK`/`ASKING`/`READONLY`
- `CLUSTER NODES` / `SLOTS` / `INFO` / `SHARDS` 从 Raft FSM 读取真拓扑（无 FSM 时本节点占满 0–16383）
- `CLUSTER ADDSLOTS` / `DELSLOTS` / `*RANGE` / `FLUSHSLOTS` 写入 Raft FSM（`EventAddSlots`/`EventRemoveSlots`）
- TimeSeries opaque（AOF/RDB）保留 `DuplicatePolicy` 与 `ChunkSize` 与 `DownsampleRules`（TS.CREATERULE）
- Vector opaque 保留 item `Attributes`（VSETATTR）
- Stream opaque 保留 consumers 与 PEL（id/consumer/deliveryCount/deliveryTime）
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

- 文档明确非「100% 兼容」：以 `docs/COMPATIBILITY.md` 与 `commands.md` 为准；opaque 扩展类型非 Redis 原生模块 RDB 互通

### Security

- gopher-lua 引擎默认 `SkipOpenLibs`，仅开放 base/table/string/math；拒绝 `os`/`io`/`require`/`dofile`/`loadfile`（`TestEvalDeniesOsExecute`）
- 修复 ACL 未启动、`execAuth` 在无 ACL 时直接返回 OK 的认证绕过
- 集群命令路径接入完整 ACL 校验（之前仅 requirepass）

### Fixed

- `CLUSTER SETSLOT NODE` / `REPLICATE` / `FAILOVER` / `FORGET` / `RESET` / `SAVECONFIG` / `SET-CONFIG-EPOCH` 对未实现写路径返回明确 `ERR … is not supported`（不再假 OK）
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
