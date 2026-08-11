# Changelog

本文件遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/) 格式。

## [Unreleased]

### Fixed

- `TYPE`：识别 `*stream.Stream` → 返回 `stream`（此前 `ERR unknown error`）；R4-1 取消对应 `@skip`
- 空 ZSet：`ZREM` / `ZREMRANGE*` / `ZPOPMIN|MAX` 删空后删除键（对齐 Redis；R4-1 `EXISTS` 断言）

### Added

- 官方模块 RDB/DUMP **边界诚实化**：`RESTORE` 拒绝 `typeModule`/`typeModule2` 标记（明确 ERR）；`LoadRDB` 遇 `ModuleTypeObject` **中止并 ERR**（不再静默丢键）；合成负向测 + `docs/COMPATIBILITY.md`「官方模块 RDB/DUMP 边界」（**不**宣称互通）
- `FUNCTION RESTORE`：**官方 FUNCTION DUMP 边界**——显式拒绝 Redis `0xF5`/`0xF6` 与 `REDIS####` 头（拒绝先于 FLUSH；负向测+`docs/COMPATIBILITY.md`）；**禁止假互通**
- INFO/MEMORY **内存核算 / jemalloc 边界**：专节文档 + 负向测试锁定 `mem_allocator:go`、MALLOC-STATS 不伪造 arena、`jemalloc-bg-thread` 桩不改 allocator；**不**实现 jemalloc（`used_memory*` 仍为 Go runtime / 进程估账）
- R4-1：用例表扩 **Stream/Geo/Bitops/HLL lite**（显式 XADD/XLEN、GEOADD/ZCARD、SETBIT/GETBIT/BITCOUNT/BITOP、PFADD/PFCOUNT）；驱动支持 `@skip`/`@todo`；文档 **「R4-1 套件边界」**（sidecar≠官方全量 Test）
- `CLUSTER REPLICATE`/`FORGET`：**接 FSM**（MasterSlaves / EventForget 安全清理；**非** Redis gossip bus）
- `CLUSTER INFO`：ping/pong/meet 等消息计数映射内部 **heartbeat/MEET RPC**（`cluster_bus_port:0`；不宣称完整 gossip）
- R4-1：**用例表** `scripts/r4-1-cases.txt` 驱动 sidecar 比对；扩 **Set/ZSet/TTL**（SADD/SCARD/SISMEMBER/SREM、ZADD/ZSCORE/ZCARD/ZREM、TTL/PTTL/PEXPIRE/EXPIRE/PERSIST）；失败多行 FAIL；诚实标注非 FT/模块/DUMP/集群全量
- BM25：**IDF 稀有词优先**与**多 TEXT 字段加权求和**可验；**`BM25STD_TANH_FACTOR Y`**（非法 factor→ERR）
- KNN：**`$YIELD_DISTANCE_AS`** 属性块；**HYBRID_POLICY**∈{ADHOC_BF,BATCHES} 校验；空预过滤→0；预过滤路径贯通 DIALECT/GEOSHAPE
- DIALECT：D1 拒绝 **tag 空格**与 **`@f1|f2`**（防 fallback 吞错）；比较/`ismissing`/GEOSHAPE 更具体 ERR
- R4-1 allowlist 扩 **Hash/List** 安全命令：HSET/HGET/HLEN/HEXISTS/HDEL、LPUSH/LLEN/LINDEX/LPOP；两侧统一 `redis-cli --raw`；自检列出完整 allowlist
- FT **DIALECT/KNN 边界错误路径**可测：非法 DIALECT；缺 PARAMS；非 VECTOR 字段；dim 不符；残缺 KNN 子句；`SplitKNNClause` 单元（`No such parameter` 文案对齐）
- R4-1 allowlist 再扩安全样例：ECHO/STRLEN/APPEND/DECR；`.sh`/`.ps1` + `--selfcheck`/自检文案同步
- R4-2：**观察式门槛**书面说明（Coveralls 趋势、无私有 % fail gate；见 `coverall.yml` 注释）
- BM25STD.**TANH** 可验收：`tanh(raw/4)` 分数 ∈(0,1) 且低于未绑定 BM25STD
- DIALECT 子集：`RequiresDialect3` — **GEOSHAPE** 谓词在 DIALECT&lt;3 明确 ERR（堵 DIALECT 2+PARAMS 静默洞）
- `MEMORY STATS`：`allocator=go`；INFO `allocator_*` 注释锁定为 Go MemStats 镜像（非 jemalloc）
- R4-1 allowlist 增强：PING/SET/GET/DEL/EXISTS/INCR/TYPE；`.ps1` + `--selfcheck`；CI smoke 对 Redis 8 sidecar **实跑**（非全量 diff）
- Vector **Q8 图内 int8 距离**：HNSW/VSIM 对 int8 codes 直接算 cosine/L2/dot（不物化搜索态 f32）；与反量化路径数值对齐验收测
- BM25STD：**文档长度归一化可验**（短文档优先）+ `avgdl<=0` 守卫；**BM25STD.NORM 真 min-max**；TEXT WEIGHT 保持
- `RESTORE`：**拒绝矩阵**（空/过短/坏版本/坏 CRC/截断 GODIS1/异己模块样载荷→ERR；不伪装模块 RDB 互通）
- FAILOVER 集成测：ACK 预同步 + TIMEOUT 放宽，收敛 Windows flake
- Vector **BIN 图内 Hamming**：HNSW/VSIM 对 packed bits 真 Hamming（报告 cosine=`(dim-2h)/dim`）
- FT+KNN **最小路径验收测**：`*=>[KNN…]` + 预过滤（`database/redisearch_knn_min_test.go`）
- `FUNCTION RESTORE`：**GODISFN1** 截断/尾随垃圾/异己二进制明确 ERR（不伪造 Redis 官方 Functions dump）
- RDB / RDB-preamble：**FT 索引定义**经 Godis opaque `ft`（`CreateArgs`）；LoadRDB 延迟 `FT.CREATE` 回填文档（非官方模块 RDB）
- VADD **BIN 真二值量化**：1-bit/dim 打包；`VINFO quant-type=bin`；opaque 可保留 BIN codes
- BM25STD：**TEXT WEIGHT** 计入字段贡献（可验排序）；DIALECT 1/2/3 子集诚实文档
- HLL **sparse 真读**：Redis sparse RLE→dense 提升；`PFCOUNT`/`PFADD` 可读迁移 blob；写出仍 dense；损坏编码→`INVALIDOBJ`
- 纯 AOF rewrite：**FT.CREATE** 自引擎 `CreateArgs` 回放（剥 `SKIPINITIALSCAN`）
- VADD **Q8 真量化存储**：int8+range；`VINFO quant-type=int8`；opaque 可保留 Q8 codes
- `INFO memory`：`used_memory_scripts`（≈ lua）；`mem_allocator:go` 测试锁定（绝不写 jemalloc）
- FT VECTOR **BFLOAT16/INT8/UINT8** blob→float32 解码（与 FLOAT16 同路径 widen）
- `CLUSTER INFO`：`cluster_bus_port:0`（诚实无 Redis gossip bus）
- R4-1 脚手架：`scripts/redis-sidecar-diff.sh`（allowlist；非全量）
- `INFO memory`：`used_memory_rss` 优先真进程 RSS（Windows WorkingSet / Linux VmRSS）；`MEMORY STATS` 增加 `process.rss`（仍非 jemalloc）
- `CLUSTER INFO`：补齐 ping/pong/fail 等 gossip 消息计数键（恒 0，诚实无 bus）
- `RESTORE-ASKING`：注册为可调用命令（强制 REPLACE）
- FT VECTOR **FLOAT16** blob→float32 解码
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

- **兼容里程碑（2026-08-11 deep8）**：远期清单仍 **7** 项 + 可独立 **0**（FT DIALECT/KNN 错误路径；R4-1 allowlist 扩样；R4-2 观察式门槛注释；小步非远期完成）
- **兼容里程碑（2026-08-11 deep7）**：远期清单仍 **7** 项 + 可独立 **0**（BM25STD.TANH；GEOSHAPE DIALECT≥3；jemalloc 字段边界；R4-1 allowlist CI 实跑；小步非远期完成）
- **兼容里程碑（2026-08-11 deep6）**：远期清单 **7** 项 + 可独立 **0**（Q8 图内 int8；BM25STD.NORM 真 min-max；RESTORE 拒绝矩阵；FAILOVER 测预同步；小步非远期完成）
- **兼容里程碑（2026-08-11 deep5）**：远期清单曾为 **8** 项 + 可独立 **0**（BIN Hamming；FT+KNN 最小验收；GODISFN1 边界；小步非远期完成）
- **兼容里程碑（2026-08-11 deep4）**：远期清单 **8** 项 + 可独立 **0**（FT RDB opaque；VADD BIN；BM25 WEIGHT；小步非远期完成）
- **兼容里程碑（2026-08-11 deep3）**：远期清单曾为 **9** 项（HLL sparse；FT 纯 AOF rewrite / VADD Q8）
- `CLUSTER REPLICATE`/`FORGET`/`FAILOVER`：ERR 文案标明无 Redis gossip bus
- `RESTORE` 坏载荷 ERR：注明非 Redis module RDB
- **兼容里程碑关闭（2026-08-10）**：可独立正确性/兼容小项已清至书面远期非目标；**不宣称 100% Redis 兼容**。远期非目标见 `docs/COMPATIBILITY.md`「兼容里程碑关闭」
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
