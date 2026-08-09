# Godis 生产化演进 TODO

> **目标：** 从「功能演示型 Redis 实现」推进到「可在内网/测试环境长期跑的服务」。  
> **原则：** 每阶段有可验证交付物（测试 / CI / 文档）；先收敛核心路径，再扩展功能面。  
> **索引：** 历史审阅见 [`改进意见_2026-06.md`](改进意见_2026-06.md)；兼容边界见 [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md)。

---

## 进度总览

| 阶段 | 主题 | 状态 |
|------|------|------|
| **P0** | 可长期运行基线 | ✅ 已完成 |
| **P1** | 运维与可观测 | 🔄 进行中 |
| **P2** | 稳定性与质量门禁 | 🔄 进行中 |
| **P3** | 部署与发布 | ⬜ 待开始 |
| **Redis 8.x 兼容** | 协议/命令/集群深度对齐 | 🔄 RESP3 双形回复进行中 |

---

## 进行中：Redis 8.x 协议与命令兼容性深度落地

> 来源：[`分析报告.md`](分析报告.md)（2026-06-29）。  
> 目标：把“功能面很广、深度不足”的兼容层，先收敛到 **RESP3 端到端 + 标准命令分发 + 关键 Redis 8.x 命令**，再逐步补齐集群/ACL/持久化。

### R0 — 协议与命令分发基线（当前迭代）

- [x] **R0-1 命令分发标准化**
  - `database/router.go` 增加 `ResolveCommandLine` 与运行时别名映射，自动处理 `.` / `|` 分隔的模块/Function/Script 命令，并手动补齐 `VS.*` / `TDIGEST.*`。
  - `DB.Exec`、`execNormalCommand`、`execWithLock`、`EnqueueCmd`、`ExecMulti`、`COMMAND GETKEYS` 均接入别名解析。
  - 新增测试：`TestCommandAliasDispatch`、`TestCommandGetKeysModule`。

- [x] **R0-2 连接 RESP 协议版本状态**
  - `interface/redis/conn.go` 与 `redis/connection/conn.go` 增加 `GetProtocolVersion` / `SetProtocolVersion`。
  - `HELLO` 写入协议版本并调用 `SetClientName`；`CLIENT SETNAME/GETNAME/ID` 真实读写连接状态。
  - 修复 `database/commandinfo.go` 中 `COMMAND INFO/DOCS/GETKEYS` 未小写命令名的 bug。
  - 新增测试：`TestHelloProtocolVersion`、`TestClientSetNameGetNameID`。

- [x] **R0-3 修复明显命令 bug**
  - 删除 `database/hash.go` 中重复的 `HGet` 注册。
  - `RENAME` / `RENAMENX` flags 改为 `flagWrite`。
  - 统一修正 `FT.*`、`TS.*`、`BF.*`、`CF.*`、`CMS.*`、`TopK.*`、`TDigest.*`、`BF/CF.ScanDump/LoadChunk` 的 `firstKey/lastKey/keyStep`。
  - 顺手修复 `database/tdigest.go` `TDIGEST.INFO` 对整数字段 panic 的问题。
  - 新增测试：`TestHGetArity`、`TestRenameCommandFlag`。

**验收结果：** `go test ./...` 全绿；新增 6 个兼容性测试全部通过。

### R1 — RESP3 端到端（已完成）

- [x] **R1-1 RESP3 请求解析器接入**
  - `redis/parser/parser.go` 已支持 `_ # , ( = % ~ > |` 等 RESP3 类型帧与嵌套聚合。
  - 修复了嵌套聚合与 Push 类型的解析 bug。

- [x] **R1-2 回复按协议版本编码**
  - 新增 `redis/protocol/resp3.go:ReplyToRESP3`。
  - `std`/`gnet` server 根据 `conn.GetProtocolVersion()` 切换 RESP3 编码。
  - `HELLO 3` 返回 `%` Map。

- [x] **R1-3 客户端缓存高级语义**
  - `CLIENT TRACKING` 强制要求 RESP3。
  - 实现 `REDIRECT` 转发；解析 `OPTIN/OPTOUT/NOLOOP`。
  - 修复相关测试，新增 RESP3 解析/编码测试。

**验收结果：** `go test ./...` 全绿；`go vet ./...` 无告警。

### R1b — RESP3 命令回复形态对齐（核心已收口，2026-08）

双形回复：`ToBytes`=RESP2，`ToRESP3`/`ReplyToRESP3`=RESP3。边界见 [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md)「RESP3 核心回复形态」。

- [x] Double/Boolean 等 RESP2 降级；`HGETALL`→Map；`SMEMBERS`→Set；`ZSCORE`/`ZMSCORE`→Double
- [x] `ScorePairsReply`（ZRANGE WITHSCORES / ZPOP / ZINCRBY / GEODIST 等）
- [x] `SINTER`/`SUNION`/`SDIFF`→Set；`CONFIG GET` / `HRANDFIELD`+WITHVALUES→Map
- [x] `SPOP`/`SRANDMEMBER` 正 count→Set；`ZUNION`/`ZINTER`/`ZDIFF` 无 WITHSCORES→Set
- [x] `XREAD`/`XREADGROUP`→`StreamReadReply`（顶层 Map）；`XINFO STREAM/GROUPS/CONSUMERS`→Map
- [x] `ZRANDMEMBER` 正 count→Set（WITHSCORES→ScorePairs）
- [x] `MEMORY STATS`→Map（百分比字段 Double）
- [x] `ACL GETUSER`→Map（selectors 为 Map 数组）
- [x] `COMMAND DOCS`→外层 Map + 每命令文档 Map；`CLIENT TRACKINGINFO`→Map
- [x] `FUNCTION STATS`→Map（`engines`/`running_script` 嵌套 Map；无运行中为 null）
- [x] `FUNCTION LIST`→库项 Map 数组；`ACL LOG`→条目 Map 数组（`age-seconds` Double）；`MODULE LIST` 空数组结构就绪
- [x] `BF/CMS/TOPK/TDIGEST.INFO`→Map；`FT.CONFIG GET`→Map
- [x] `TS.INFO`→Map（`labels` 嵌套）；`LCS IDX`→Map；`LATENCY HISTOGRAM`→嵌套 Map
- [ ] 次要残留（非核心路径）：`FT.SYNDUMP` / `FT.SPELLCHECK` / `FT.PROFILE`；`CLUSTER SHARDS` 嵌套 Map
- [ ] 刻意不做：`INFO`/`CLIENT LIST` 文本；`HSCAN`/`ZSCAN` 第二段 Array；`VINFO`/`ROLE`/`XPENDING` 等官方仍标 Array；`CF.INFO` 未实现

### R2 — 关键 Redis 8.x 命令补齐

- [x] **R2-1 Hash Field TTL**：`HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT`。
  - `datastruct/dict/expire_dict.go` 补齐 `Dict` 接口，解决 `ExpireDict` 与普通 Hash 命令互斥问题。
  - `database/hash_expire.go` 实现四个 HEXPIRE 命令，支持 `NX/XX/GT/LT` 条件与正确返回值。
  - 新增事务回滚 `undoHExpire`。
  - AOF 重写（非 RDB 前导）支持 `ExpireDict`，重写为 `HMSET` + `HPEXPIREAT`。
  - RDB 前导模式降级为普通 Hash 并记录 warning。
  - 新增 `database/hash_expire_test.go` 覆盖基本、条件、绝对时间、duality、事务等场景。

- [ ] **R2-2 基础缺失命令**：`BITOP`、`SMOVE`、`XCLAIM/XAUTOCLAIM`、`WAIT`、`UNWATCH`。
- [ ] **R2-2 基础缺失命令**：`BITOP`、`SMOVE`、`XCLAIM/XAUTOCLAIM`、`WAIT`、`UNWATCH`。
- [ ] **R2-3 ZSet 新选项**：`ZRANGE BYSCORE/BYLEX/REV/LIMIT`、`ZRANK WITHSCORE`。
- [ ] **R2-4 String 新选项**：`SET KEEPTTL/GET`、`GETEX EXAT/PXAT`。

### R3 — 集群/ACL/持久化（后续轮次）

- [ ] **R3-1 集群槽位统一为 16384**；补齐 `ASK/MOVED/READONLY/READWRITE`；Sharded Pub/Sub 用 CRC16 并与集群转发集成。
- [x] **R3-2 ACL 细粒度权限**：key/channel/selectors 检查（含 `%R~`/`%W~`/`(...)` 选择器/`&channel` DRYRUN）；集群路径也接入 ACL（`CheckACLPermission`）。
- [ ] **R3-3 持久化扩展**：AOF marshal 与 RDB 加载覆盖 JSON/Vector/Timeseries/Stream/概率结构；RDB aux 版本号更新。

### R4 — 测试与文档

- [ ] **R4-1 Redis 8.x 响应比对套件**：CI 中用 Redis 8 sidecar 做参考，diff 关键命令输出。
- [ ] **R4-2 覆盖率提升**：`aof`、`pubsub`、`redis/protocol`、`redis/connection`、新数据类型包达到可接受覆盖。
- [ ] **R4-3 文档同步**：`commands.md`、`AGENTS.md`、`README` 与代码一致，移除“100% 兼容”表述；新增 `CHANGELOG.md`。

---

## P0 — 可长期运行基线（优先）

内网/测试环境 7×24 运行的最低要求：安全闭环、优雅退出、客户端缓存、基础指标。

### P0-1 CLIENT 子系统打通 `caching.go`

- [x] `CLIENT TRACKING ON/OFF` 调用 `EnableTracking` / `DisableTracking`
- [x] `CLIENT TRACKINGINFO` 返回真实状态
- [x] `CLIENT SETNAME` / `GETNAME` / `ID` 绑定连接状态
- [x] 读命令成功后 `TrackKeysOnRead`；写命令成功后 `InvalidateKeysOnWrite`
- [x] 连接关闭时 `DisableTracking`（`AfterClientClose`）
- [x] 集成测试：`CLIENT TRACKING ON` + `GET` + `SET` → invalidate push

**验收：** `go test ./database/ -run Tracking -v`；redis-cli 手动验证 push。

### P0-2 进程级优雅关闭（gnet + std 统一）

- [x] std：`Handler.Close` in-flight 等待（已完成）
- [x] gnet：`GnetServer.Close` in-flight 等待（已完成）
- [x] `main.go` gnet 路径捕获 SIGTERM/SIGINT → `Close()` → 退出
- [x] 运维文档：`docs/OPERATIONS.md`（CONFIG / 信号 / 探活）

**验收：** `kill -TERM` 后进程在 10s 内退出且不丢已在途回复（现有 shutdown 测试扩展）。

### P0-3 Prometheus `/metrics` 导出

- [x] 配置项 `metrics-addr`（如 `127.0.0.1:9090`，空则禁用）
- [x] 导出 `lib/stats` 网络 IO + `database/cmdstats` 命令计数（单一数据源，避免重复计数）
- [x] 导出连接数、tracking 客户端数、慢查询条数
- [x] 单元测试：metrics HTTP handler 返回预期 metric 名

**验收：** `curl localhost:9090/metrics` 可见 `godis_commands_total` 等。

### P0-4 ACL 持久化（`aclfile`）

- [x] `config` 增加 `aclfile` 字段
- [x] 启动时加载；`ACL SAVE` / `ACL LOAD` 生效
- [x] 测试：写入 acl 文件 → 重启 Server → 用户权限保留

**验收：** `go test ./database/ -run ACL -v`。

### P0-5 CI 与冒烟测试

- [x] GitHub Actions Redis sidecar 升级到 **7 或 8**
- [x] 新增 `scripts/smoke-test.sh`（或 Go test）：PING、AUTH、SET/GET、INFO、ACL WHOAMI
- [x] CI job：build godis → 启动 → smoke → 退出

**验收：** PR 门禁绿；smoke 失败可定位到具体命令。

---

## P1 — 运维与可观测

### P1-1 INFO 字段补全

- [x] `used_cpu_*`、`blocked_clients`、`connected_clients`（真实值）
- [x] 客户端缓存：`tracking_clients`、`tracking_total_keys`

### P1-2 慢查询与 ACL 日志运维化

- [x] SLOWLOG 与 ACL LOG 最大长度可配置且热更新（`CONFIG SET slowlog-max-len` / `acllog-max-len`）
- [ ] 可选：结构化 JSON 日志输出（`lib/logger`）

### P1-3 健康检查

- [x] HTTP `/health`（随 `metrics-addr` 启用）+ `PING` 文档化（见 `docs/OPERATIONS.md`）
- [x] `maxclients` 拒绝连接时返回明确错误 + metric

### P1-4 架构文档

- [x] `docs/ARCHITECTURE.md`：`Server.Exec` vs `DB.Exec`、auth/ACL 边界、集群限制

### P1-5 搜索后端抽象（SQLite 路线）

- [x] 定义 `SearchIndexBackend` / `VectorIndexBackend` 接口与后端选择器
- [x] 配置开关：`search-backend`、`vector-backend`（默认 `native`）
- [x] SQLite 后端骨架：build tag `sqlite_backend` + WAL/mmap 打开逻辑
- [x] 适配最小子集：`FT.CREATE/FT.SEARCH`、`FT.ADD`（TEXT 字段 + FTS5）
- [x] 适配 `VS.ADD/VS.SEARCH`（sqlite-vec）
- [x] 双后端一致性测试（native vs sqlite）

---

## P2 — 稳定性与质量门禁

### P2-1 输入校验全覆盖

- [x] 审计 `prepare=nil` 命令（rediSearch、vector、geo、timeseries、probabilistic）并补 `writeFirstKey`/`readFirstKey`
- [x] 统一走 `lib/validate`（修复 `errs.Newf` 错误码丢失；集成测试 `validate_cmd_test.go`）

### P2-2 测试与覆盖率

- [x] `database` 包覆盖率 33% → **50%+**（当前 **50.1%**，stream/ACL/connection/dump/misc 等测试）
- [x] CI 全量 `go test -race`（nightly `race.yml`）
- [x] 核心命令兼容性对照测试（`database/compat_test.go` 子集）

### P2-3 静态分析门禁

- [x] CI 恢复 `go vet ./...`
- [x] `golangci-lint` 配置 + CI（`ineffassign` + `misspell` 基线）

---

## P3 — 部署与发布

### P3-1 Release 工作流

- [x] tag 触发 GitHub Actions：linux/darwin/windows × amd64/arm64
- [ ] `CHANGELOG.md` 随版本更新

### P3-2 配置与示例

- [x] `example.conf` 补充 `maxmemory` 说明（metrics/aclfile/acllog 已补充）
- [ ] Docker / docker-compose 示例（可选）

### P3-3 Fork 维护

- [x] module 路径 `github.com/linkerlin/godis`（如已完成）
- [ ] README 指向 fork 的 CI badge 与 release

---

## 明确不做（当前周期）

- 全量 CLUSTER 15+ 子命令
- 100% Redis 命令兼容数字目标
- 消除 `datastruct/*` 全部内部 panic
- 完整跨 DB 事务

---

## 实施顺序（执行用）

```
P0-1 CLIENT/caching  →  P0-2 信号关闭  →  P0-3 Prometheus
        ↓
P0-4 aclfile  →  P0-5 CI smoke  →  P1 INFO/ARCHITECTURE  →  P2 校验/覆盖率
```

---

**最后更新：** 2026-06-29  
**维护：** 每完成一项将 `[ ]` 改为 `[x]` 并注明 PR/commit。
