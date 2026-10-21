# Godis 生产化演进 TODO

> **目标：** 从「功能演示型 Redis 实现」推进到「可在内网/测试环境长期跑的服务」。  
> **原则：** 每阶段有可验证交付物（测试 / CI / 文档）；先收敛核心路径，再扩展功能面。  
> **索引：** 历史审阅见 [`改进意见_2026-06.md`](改进意见_2026-06.md)；兼容边界见 [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md)。

---

## 进度总览

| 阶段 | 主题 | 状态 |
|------|------|------|
| **P0** | 可长期运行基线 | ✅ 已完成 |
| **P1** | 运维与可观测 | 🔄 进行中（非兼容清单） |
| **P2** | 稳定性与质量门禁 | 🔄 进行中（非兼容清单） |
| **P3** | 部署与发布 | ⬜ 待开始 |
| **Redis 8.0–8.2 兼容** | 协议/命令/文案可独立项 | ✅ 已关闭（非 100%；远期 7 项见下） |
| **R5 Redis 8.4–8.10** | 新命令面 | ⬜ 2026-10-12 重开，见下 |

---

## 已关闭：Redis 8.x 协议与命令兼容性深度落地

> 来源：[`分析报告.md`](分析报告.md)（2026-06-29）。  
> **关闭定义（2026-08-10）：** 8.4 之前可独立完成的正确性/兼容小项已扫清。进行中清单里的书面远期仍是下面 7 项。**该关闭不覆盖 Redis 8.4–8.10 新增命令**；那些项在 R5。**禁止宣称 100% Redis 兼容。** 边界见 [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md)。

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
- [x] `FT.SYNDUMP`→Map（term→group ids）；`FT.SPELLCHECK`→RESP3 `results` Map；`FT.PROFILE`→RESP3 `Results`/`Profile` Map
- [x] `CLUSTER SHARDS`→外层 Array + shard/node Map
- [x] `CF.INFO`→Map（与其它概率 *INFO 一致）
- [x] 刻意不做（官方仍标文本/Array，非缺口）：`INFO`/`CLIENT LIST` 文本；`HSCAN`/`ZSCAN` 第二段 Array；`VINFO`/`ROLE`/`XPENDING` 等

### R2 — 关键 Redis 8.x 命令补齐

- [x] **R2-1 Hash Field TTL**：`HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT`。
  - `datastruct/dict/expire_dict.go` 补齐 `Dict` 接口，解决 `ExpireDict` 与普通 Hash 命令互斥问题。
  - `database/hash_expire.go` 实现四个 HEXPIRE 命令，支持 `NX/XX/GT/LT` 条件与正确返回值。
  - 新增事务回滚 `undoHExpire`。
  - AOF 重写（非 RDB 前导）支持 `ExpireDict`，重写为 `HMSET` + `HPEXPIREAT`。
  - RDB 前导模式降级为普通 Hash 并记录 warning。
  - 新增 `database/hash_expire_test.go` 覆盖基本、条件、绝对时间、duality、事务等场景。
  - HEXPIRE 族命令本身写 AOF（`mutated` 时）；主动过期时间轮经 DB 键写锁；到期仍写 `HDEL`。

- [x] **R2-2 基础命令**：`BITOP`、`SMOVE`、`XCLAIM`/`XAUTOCLAIM`、`WAIT`/`WAITAOF`、`UNWATCH`（含 MULTI 内排队）已实现。
- [x] **R2-3 ZSet 选项**：`ZRANGE BYSCORE/BYLEX/REV/LIMIT`、`ZRANK`/`ZREVRANK WITHSCORE` 已实现。
- [x] **R2-4 String 选项**：`SET KEEPTTL/GET`、`GETEX EX/PX/EXAT/PXAT/PERSIST` 已实现。

### R3 — 集群/ACL/持久化（后续轮次）

- [x] **R3-1 集群槽位统一为 16384**：`lib/hashslot` CRC16-XMODEM；`cluster/core` 与 Sharded Pub/Sub 共用；对外 `CLUSTER KEYSLOT` / INFO 一致。
- [x] **R3-1a 重定向协议面**：`MOVED`/`ASK` 错误线；`ASKING`；`READONLY`/`READWRITE` 连接标志（单键 DefaultFunc；内部 Relay 仍用于 TCC 等）。
- [x] **R3-2 ACL 细粒度权限**：key/channel/selectors 检查（含 `%R~`/`%W~`/`(...)` 选择器/`&channel` DRYRUN）；集群路径也接入 ACL（`CheckACLPermission`）。
- [x] **R3-3 持久化扩展（阶段）**：Godis opaque（`GODIS1`）覆盖 JSON/Vector/TS/Stream/概率结构的 AOF 重写与 RDB 伪-String；TS opaque 保留 `DuplicatePolicy`/`ChunkSize`/`DownsampleRules`；Vector opaque 保留 `Attributes`（VSETATTR）；Stream opaque 保留 consumers + PEL + `entriesAdded`/`maxDeletedID`。仍延期（远期/非目标）：与 Redis 原生 Stream/模块 RDB 互通。
- [x] **R3-1b 集群管理查询面**：`CLUSTER NODES`/`SLOTS`/`INFO`/`SHARDS` 读 Raft FSM 真拓扑（无 FSM 时本节点占满槽）。
- [x] **R3-1c ADDSLOTS 写 FSM**：`ADDSLOTS`/`DELSLOTS`/`ADDSLOTSRANGE`/`DELSLOTSRANGE`/`FLUSHSLOTS` 经 Raft Propose（或 FSM-only `ApplyLocal`）；busy/unassigned 校验。
- [x] **R3-1d 集群管理 seam（最小）**：`CLUSTER MEET ip port [raft-port]`→与 `cluster.join` 等价的 AddToRaft/EventJoin（或 FSM-only ApplyLocal）；无 Raft 时明确 ERR。`SETSLOT MIGRATING|IMPORTING|STABLE` 写本地 `slotsManager`（ASK/ASKING）；`SETSLOT NODE`→FSM `EventAssignSlots`。**`REPLICATE`/`FORGET` 已接 FSM**（MasterSlaves / EventForget；非 gossip bus）。`RESET`/`SAVECONFIG`/`SET-CONFIG-EPOCH`/`CLUSTER FAILOVER` 仍显式 `ERR … is not supported`。仍延期（远期）：完整 Redis gossip bus。
- [x] **R3-1e 读面诚实化**：`GETKEYSINSLOT` 返回本地 slotsManager 键；`REPLICAS`/`SLAVES` 读 FSM；`INFO cluster` 与 CLUSTER INFO 同源；BUMPEPOCH 为 `BUMPED 0` no-op（FSM 无 config epoch，远期）。
- [x] **R3-1f 迁移闭环缝（最小）**：`execFinishExport` 成功才 dropSlot+清 importingTask；`doImports` 标 IMPORTING；`startExporting` 幂等 SETSLOT；ASK 读 FSM Migratings；`SETSLOT NODE` 本节点清迁移态。仍延期：完整 redis-cli reshard/gossip、与官方迁移编排 100% 一致。
- [x] **R3-mem 一小步**：`used_memory_peak`/`peak.allocated` 跟踪 `Alloc` 高水位；INFO/MEMORY STATS overhead 口径对齐；Limiter 默认 `Alloc`；**进程 RSS→`used_memory_rss`**（Win/Linux）。仍延期：jemalloc 级会计。

### R4 — 测试与文档

- [ ] **R4-1 Redis 8.x 响应比对套件**：CI 中用 Redis 8 sidecar 做参考，diff 关键命令输出。（**远期非目标**；allowlist 已到 **12,623** 条，`scripts/r4-1-cases.txt`。runner 已修 `@CMD` 前缀、引号感知分词、`R41_CONTINUE=1` 收集模式；真实 Redis 8.10 全量 **12,623 断言 0 失败**。R5 收口后补 Array 扩展 8 命令与 JSONPath `[*]`/`..`/nodelist 共 37 条新样例；`ARINFO`（map 字段序）、subkey 通知（需 Pub/Sub）、GEOHASH 末位精度、CLAIM idle 抖动等以 `@skip` 诚实标注。**仍非** FT/模块/DUMP/集群全量）
- [ ] **R4-2 覆盖率提升**：`aof`、`pubsub`、`redis/protocol`、`redis/connection`、新数据类型包达到可接受覆盖。（**远期非目标**；**观察式门槛**见 coverall.yml 注释：Coveralls 趋势、无私有 % fail gate）
- [x] **R4-3 文档同步 + 里程碑关闭**：`CHANGELOG` / `docs/COMPATIBILITY.md` / `TODO.md` / `兼容性改进计划.md` 勾选口径清空至仅剩远期篇；标明明确非目标列表。2026-10-12 再次同步：基线改为 Redis 8.10，R5 重开。

### R5 — Redis 8.4–8.10 命令面（2026-10-12 重开）

> 差距表：[`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md)「Redis 8.10 增量」。  
> 顺序与验收：[`兼容性改进计划.md`](兼容性改进计划.md)「〇、Redis 8.10 演进方案」。  
> 原则：先补客户端会直接调用、且能落在现有类型上的命令；每项带 arity / 缺键 / 错类型 / 一条成功路径测试。确定性回复再加一条 R4-1 sidecar。

#### 第一波：现有类型上的新命令

- [x] **R5-1 错误回复去掉裸 CR/LF**（Redis 8.10 安全修复）。用户数据进错误文本时，协议帧仍是一行。（`protocol.SanitizeErrText` 在 `StandardErrReply`/`ProtocolErrReply` 的 `ToBytes` 收口；CR/LF 映射为空格，`Error()` 保留原文）
- [x] **R5-2 `SUNIONCARD` / `SDIFFCARD`**。对齐 `SINTERCARD` 的 `numkeys` 与 `LIMIT`；`SUNIONCARD` 另有 `APPROX`。（`APPROX` 接受并返回精确计数——精确值恒在近似误差界内；文档已注明）
- [x] **R5-3 `LMOVEM` / `BLMOVEM`**。一次搬多个元素；源列表空则删键。语法以 8.10 命令页为准。（`COUNT`/`EXACTLY` × `OBO`/`BULK`；AOF 以 `EXACTLY 实际条数` 回放保证确定性）
- [x] **R5-4 `XREAD` / `XREADGROUP` `MAXCOUNT` / `MAXSIZE`**。限制回复条目合计与字节合计。（字节按条目 RESP 线格式估算；`MAXCOUNT<COUNT` 报错文案未对实机校验；顺带修正 `XREADGROUP` 截断后 `LastID` 越过未递送条目的旧问题）
- [x] **R5-5 `SLOWLOG GET` 第七字段**：参数总个数。可选：`slowlog-entry-max-argc`、`slowlog-entry-max-string-len`。（第七字段=含命令名的参数总数；截断配置仍缺，列第二波后补）
- [x] **R5-6 `ZUNION` / `ZINTER` / `ZUNIONSTORE` / `ZINTERSTORE` 的 `AGGREGATE COUNT`**。（`WEIGHTS` 语义：每含该成员的输入集贡献其权重）
- [x] **R5-7 `SET` `IFEQ`/`IFNE`/`IFDEQ`/`IFDNE`，以及 `DELEX`、`DIGEST`**。`DIGEST` 是单键摘要，与 `DEBUG DIGEST` 分开。（XXH3-64 十六进制，与 8.4 命令页示例值一致；非字符串现值按「不相等」处理）
- [x] **R5-8 `MSETEX`**。（NX/XX 全有或全无；相对过期在 AOF 以绝对 `PEXPIREAT` 回放）
- [x] **R5-9 命令标志 `script_runner`**。挂到 `EVAL`/`EVALSHA`/`FCALL`/`FUNCTION` 等真正执行脚本的命令上，经 `COMMAND INFO`/`DOCS` 可见。标志原文抄 Redis 8.10。（挂到 `EVAL`/`EVALSHA`/`EVAL_RO`/`EVALSHA_RO`/`FCALL`/`FCALL_RO`）
- [x] **R5-10 `FT.ALIASLIST`**。（按索引名或别名查询；RESP2 数组 / RESP3 set，按名排序）
- [x] **R5-11 `volatile-lrm` / `allkeys-lrm`**。写入路径记修改时间，淘汰按修改时间采样。在此之前，错误文案与 `ParseEvictionPolicyStrict` 必须一致（现在文案列出、解析器拒绝）。（两策略可设可选；修改时间在 `execNormalCommand`/`execWithLock` 成功写后按 prepare 声明的写键记录，采样淘汰，无修改记录视为最旧；非 prepare 路径不记账，近似口径已写入兼容文档）

#### 第二波：流、限速、时序、搜索超时

- [x] **R5-12 `XREADGROUP CLAIM`**。（`>` 读取前按 min-idle 认领空闲 PEL：XNACK 释放者优先、其次按空闲时长降序；被认领条目用四字段扩展回复 `[id, fields, idle-ms, deliveries]`；`COUNT`/`MAXCOUNT`/`MAXSIZE` 覆盖认领+新条目合计，被预算丢弃的条目不改 PEL；`NOACK` 不作用于认领条目；非 `>` ID 忽略 `CLAIM`）
- [x] **R5-13 `XNACK`**。（`SILENT`/`FAIL`/`FATAL` 三模式 + `RETRYCOUNT`/`FORCE`；释放为无主（consumer=""、delivery time 0），任何 min-idle 立即可认领；`XPENDING` 明细行 idle 对释放条目显示 -1；XNACK 释放条目间的顺序按 ID 升序近似 FIFO）
- [x] **R5-14 `XADD` `IDMP` / `IDMPAUTO`**，以及 `stream-idmp-duration`、`stream-idmp-maxsize`。（`IDMPAUTO` 的幂等 ID 取自条目内容摘要（排序字段 xxh3），同 producer+同内容去重；`XCFGSET key IDMP-DURATION n IDMP-MAXSIZE n` 按流配置，全局默认 `stream-idmp-duration`/`stream-idmp-maxsize`（300s/10000，Godis 自选默认）；IDMP 记录不持久化（重启后去重窗口清空），未过 RDB/AOF 边界）
- [x] **R5-15 `INCREX`**。`BYINT`/`BYFLOAT`、上下界、`SATURATE`、过期选项、`ENX`；回复为两元素数组。（越界默认跳过回复 `[当前值, 0]`；`SATURATE` 封顶并回报实际增量；AOF 回放实际增量 + 绝对过期效果；`LBOUND>UBOUND` 报错文案未对实机校验）
- [x] **R5-16 时序增量**：`NaN` 样本、`COUNTNAN`/`COUNTALL`、一条命令多个聚合器、`EXCLUDEEMPTY`、`TS.QUERYLABELS`、`TS.NRANGE`/`TS.NREVRANGE`、`TS.READ`（含 `BLOCK`）。（聚合器除 countNaN/countAll 外忽略 NaN，全 NaN 桶 count=0 其余为 NaN；`TS.NRANGE`/`TS.NREVRANGE` 支持裸 pivot + 每键聚合器（含逗号多聚合器）+ `COUNT`，`LATEST`/`FILTER_BY_TS`/`FILTER_BY_VALUE`/`ALIGN`/`BUCKETTIMESTAMP`/`EMPTY` 明确报不支持。**`TS.READ` 已接通**：游标 `-`/`+`/`$` 哨兵命令开始时一次解析；`BLOCK ms min_count` 由专用等待注册表唤醒——TS.ADD/TS.MADD/TS.INCRBY/TS.DECRBY 追加即唤醒、键删除（removeKey 钩子）立即回空数组、超时部分刷新；`MAX_COUNT` 上限与 `min_count ≤ max_count` 校验；MULTI/LUA 内 BLOCK 不特判（与既有阻塞命令同口径））
- [x] **R5-17 `RETURN_STRICT` 在聚合后处理阶段也截止**。单机引擎即可，不模拟 `search-workers`。（`FT.SEARCH`/`FT.AGGREGATE` 的回复构建循环每 256 条检查截止，超时返回已构建的部分结果；粒度为阶段边界/批级，非逐条）
- [x] **R5-18 Hash 字段级键空间通知（subkey notifications，Redis 8.8）**。按官方 `subkey notifications` 页实现四种独立通道：`__subkeyspace@<db>__:<key>`、`__subkeyevent@<db>__:<event>`、`__subkeyspaceitem@<db>__:<key>\n<subkey>`、`__subkeyspaceevent@<db>__:<event>|<key>`，由 `S`/`T`/`I`/`V` 标志启用（与 `K`/`E`/`A` 独立）。覆盖 `HSET`/`HMSET`/`HSETNX`/`HDEL`/`HGETDEL`/`HGETEX`/`HINCRBY`/`HINCRBYFLOAT`/`HEXPIRE` 族/`HPERSIST`/`HSETEX` 及主动/惰性字段过期；字段名按 `<len>:<field>` 长度前缀编码，支持二进制安全字段。已知近似：主动/惰性过期按字段逐条发布（官方口径为同批过期字段合并为一条）；`HGETEX`/`HSETEX` 的立即过期路径未额外合成 `hdel`。测试 `database/subkey_notify_test.go`

#### 第三波：新类型，范围写死

- [x] **R5-19 Array preview 全 17 命令**：基础 9 命令（`ARSET`/`ARMSET`/`ARGET`/`ARMGET`/`ARGETRANGE`/`ARLEN`/`ARCOUNT`/`ARDEL`/`ARDELRANGE`）+ 扩展 8 命令（`ARINSERT`/`ARNEXT`/`ARSEEK`/`ARINFO`/`ARLASTITEMS`/`AROP`/`ARGREP`/`ARRING`），`TYPE` 返回 `array`，持久化走 Godis opaque（含 cursor/插入序）。官方仍标 preview。（17 条命令页全部核实，测试断言取自官方示例；opaque 类型码 `array`；近似口径：扁平槽模型，`ARINFO` 的 slice 统计为合成值、`FULL` 只返回基础字段；`ARLASTITEMS` 按写入序（覆盖移到最近），`ARRING` 与 `ARINSERT` 共用 cursor；`AROP` 数值聚合忽略非数值、位运算按 float 向零截断；`ARGREP` GLOB 用 Godis 通配符、RE 用 Go regexp；全删空后保留键、`ARGETRANGE` 越界下标为 nil、`ARDelRange` 重叠区间按元素只计一次；AOF 回放完整命令行）
- [x] **R5-20 JSONPath 分批（已完成）**：比较与 `!`，然后 `in`/`nin`，然后数值与字符串函数。`JSON.SET FPHA` 在没有同质浮点数组编码之前返回明确错误。（**批次 1 已落地**：`[?(…)]` 过滤表达式——`==`/`!=`/`<`/`<=`/`>`/`>=` 对任意字面量（数字/字符串/布尔/null/JSON 数组与对象，按深度相等）、`!` 否定、`&&`/`||`、括号、`@` 相对路径、裸操作数真值；过滤后的剩余路径逐元素投影（未命中丢弃）；嵌套方括号字面量在段内保留；语法错误回 `ERR Path syntax error` 而非旧式 null；`FPHA` 明确拒绝。**批次 2/3a 已落地**：`in`/`nin` 成员判定（缺失左值两者皆不匹配）；算术 `+ - * / %` 与一元 `-`（非数值/除零得 Nothing→条件为假，不报错；乘除优先于加减）；`size`/`sizeof`（串/数组/对象长度，后接比较符缺省 `==`）与 `empty`。**批次 3 已落地**：过滤内函数——`length()`（串/数组/对象）、`abs()`/`ceiling()`/`floor()`、`first`/`last`（词形与 `first()`/`last()` 方法形）、`min()`/`max()`/`avg()`/`sum()`/`stddev()`（样本标准差）、`keys()`（排序数组）、`match()`（全串锚定）/`search()`（部分）、`concat()`（词形带参 `@.id concat ":"`）、`subsetof`/`anyof`/`noneof`（集合关系，词形带数组参）、`index(n)`（数组取位，越界 Nothing）、`append(v)`（查询期只读投影）；方法形 `@.recv.fn(arg)` 与词形后缀均可；顶层投影 `fn($)` / `fn($.path)`（单函数单路径；`min`/`sum`/`length`/`count` 等）。**收尾（2026-10-21）**：`[*]`/`..` 通配/递归下降顶层投影（含 `$..price`、`$.items[*].price`、`$.*`、`$..*`）、nodelist 多值语义（多值路径在比较/`in`/`nin`/集合关系/函数中任一节点满足即真）已接通；`~` 取键算子与嵌套函数调用此前已接通（`TestE4_JSONPathNestedProjectionsParenArithAndKeyOperator`、`TestR5JPWildcardAndRecursive`）。**R5-20 关闭。**）
- [x] **R5-21 `HOTKEYS`**。采样计数，并在兼容文档写明算法，不声称与 Redis 内部结构相同。（`START METRICS n [CPU][NET] [COUNT k][DURATION s][SAMPLE r]`/`STOP`/`GET`/`RESET`；指标口径=命令耗时按键分摊 + 参数字节数，与 Redis 的按槽概率结构不同；`SLOTS` 在非集群明确报错；`DURATION` 到期惰性停止；`total-cpu-time-user-ms` 亦为 Godis 计账、无 user/sys 拆分）
- [x] **R5-22 `HIMPORT` 会话 fieldset 写入普通 Hash**。开工前按 8.10 命令页列全子命令。紧凑模板编码不做。（子命令=`PREPARE`/`SET`/`DISCARD`/`DISCARDALL`，按命令页实现；fieldset 按连接隔离、`RESET` 清空；无断连钩子故每连接上限 128 个 fieldset；`SET` 走 HSET 路径写普通 Hash，compact hash 不做）

### 本里程碑明确非目标（勿当缺口重开）

> 8.4 之前的可独立项已归零。下面 7 项仍是书面远期（2026-08-11 之后未变）。R5 是另一份开放清单，不把这 7 项算进 R5，也不把 R5 的小步写成这 7 项已完成。

- jemalloc 级 `used_memory` / 完整 OS 级内存会计（**远期仍开**；已有诚实边界：进程 RSS→`used_memory_rss`；`mem_allocator:go`；`allocator_*=Go MemStats`；负向测禁止假 jemalloc；见 [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md)「内存核算 / jemalloc 边界」）
- 完整 Redis gossip bus；`RESET`/`SAVECONFIG`/`SET-CONFIG-EPOCH`/`CLUSTER FAILOVER`；BUMPEPOCH 真 config epoch（已有：`REPLICATE`/`FORGET`→FSM；ping/pong/meet 计数映射内部 heartbeat/MEET；`cluster_bus_port:0`；非宣称完整 gossip）
- 官方模块原生 RDB·DUMP 互通（Godis opaque / `GODISFN1` 自洽即可；RESTORE/LoadRDB 对官方模块 type **明确 ERR**；含 FT 官方模块 RDB；**不**伪造互通）
- 完整 BM25 / 完整 KNN 方言 / 完整 DIALECT（已有：IDF/多字段 WEIGHT/NORM/TANH+FACTOR；KNN `$YIELD_DISTANCE_AS`/HYBRID_POLICY；DIALECT 1/2/3 子集；**非**论文级完整）
- FUNCTION DUMP 官方二进制互通（远期/非目标）：`GODISFN1` 自洽；官方 `0xF5`/`0xF6`/`REDIS####` 与异己二进制**明确拒绝**（拒绝先于 FLUSH）；**禁止假互通**／不伪造 Redis payload；见 `docs/COMPATIBILITY.md`「官方 FUNCTION DUMP 边界」
- R4-1 扩大 allowlist（用例表驱动；仍非 FT/模块/DUMP/集群全量）、R4-2 覆盖率专项
- **R5 明确不做：** compact hash 模板及 `hash_templates` 指标；`BACKUP` / multi-part AOF；`CLUSTER MIGRATION`；TLS 证书登录；Malay/Tagalog 词干；C Modules API；用 SIMD 或 jemalloc 移植去追性能数字

~~HLL sparse 读取~~ ✅（sparse→dense 解码提升；写出仍 dense）  
~~RDB / RDB-preamble 写出 FT 索引定义~~ ✅（Godis opaque `ft` + Load 后回填；纯 AOF rewrite→FT.CREATE）
~~BIN 仅反量化 f32 搜索~~ ✅（deep5：图内 Hamming）  
~~Vector 图内真 int8 距离~~ ✅（deep6：Q8→图内 int8 码距离；BIN→Hamming）

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
- [x] 可选：结构化 JSON 日志输出（`lib/logger`）：`log-format text|json`，每行 `time`/`level`/`caller`/`msg`；启动读取，`CONFIG SET log-format` 热切换；`logfile` 重配置/轮转保留 JSON 模式；测试 `lib/logger/logger_test.go`、`TestConfigLogFormat`

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
- [x] Docker / docker-compose 示例（可选）：`Dockerfile` 多阶段构建、非 root 运行、`/data` 卷；`docker-compose.yml` 一键启动；`.dockerignore`；README/OPERATIONS 使用说明。已本地 `docker build` 并通过容器内 PING/SET/GET 冒烟。

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

**最后更新：** 2026-08-10（Redis 8.x 兼容里程碑关闭）  
**维护：** 每完成一项将 `[ ]` 改为 `[x]` 并注明 PR/commit。兼容清单仅远期篇保留 `[ ]`。
