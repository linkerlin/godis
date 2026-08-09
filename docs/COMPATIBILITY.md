# Godis 与 Redis 8.x 兼容性说明

> 本文档描述 Godis **当前实现**与 Redis 8.x 的差异，避免「100% 兼容」的误导性表述。  
> 详细分项与里程碑见仓库根目录 [`兼容性改进计划.md`](../兼容性改进计划.md)（含 2026-07-24 进度更新）。  
> 完整命令列表见 [`commands.md`](../commands.md)（部分条目可能尚未与代码同步）。

## 评估维度

| 维度 | 大致覆盖 | 说明 |
|------|----------|------|
| RESP2/RESP3 协议 | 高 | HELLO 3、Push、客户端缓存、blob error `!`；核心命令双形 Map/Set/Double（见下节） |
| String/List/Hash/Set/ZSet | 高 | 常用命令齐全；List/ZSet **阻塞命令真阻塞** |
| Stream / Bitmap / Geo | 中–高 | XCLAIM/XAUTOCLAIM/BITOP/BITFIELD；**XREAD BLOCK 真阻塞**；Stream 范围操作 O(log n)（有序切片+二分） |
| FAILOVER | ✅ 真实协调切换 | TO/FORCE/ABORT/TIMEOUT、复制流注入 REPLCONF FAILOVER、从库自提升+原主降级；见 [`FAILOVER_DESIGN.md`](FAILOVER_DESIGN.md) |
| JSON / Vector / Time Series | 中–高 | 子集 + 持续补全；Vector 保留 VS* 与 Redis 名双名 |
| RediSearch (FT.*) | 中–高 | Phase A/B：初始扫描、STOPWORDS、同义词、内联 GEO、AGGREGATE WITHCURSOR/APPLY；见下文 |
| 集群 (CLUSTER *) | 中–高 | 16384+CRC16；MOVED/ASK；NODES/SLOTS/INFO/SHARDS 读 FSM；MEET/gossip 仍缺 |
| ACL / 安全 | 中–高 | ACL 引擎；CONFIG `aclfile` 可存取（M2bh）；文件见 ACL LOAD/SAVE |
| 配置 | 中–高 | 布尔解析；CONFIG SET 含 maxmemory/save/tcp-backlog；**eviction 写路径已接**（per-key 估算，大 value 计入；非 jemalloc）；部分 CF-3 为存取桩 |
| 概率数据结构 (BF/CF/CMS…) | 中–高 | 见 `database/probabilistic.go`；CF EXPANSION 已接扩容 |

**M2 里程碑：** 至 **M2cm**。M2cl：Pub/Sub RESP3 Push、Lua HKEYS/HVALS/SSCAN→Array。M2cm：UNWATCH 可在 MULTI 内排队；CLIENT LIST 字段 `watch=`（及 tot-net-in/out、rbs/rbp）；ACL GETUSER 完整 `#`+SHA256；CLUSTER ADDSLOTS/DELSLOTS 写 FSM；SETSLOT 等未实现写路径显式 `ERR not supported`。

**RediSearch Phase A（2026-07-29）：** FT.CREATE 初始扫描回填 + SKIPINITIALSCAN；按 index 的 STOPWORDS（含 `STOPWORDS 0` 关闭过滤）；FT.SEARCH 查询词按 FT.SYNADD 同义词组展开；`@field:[lon lat radius unit]` 内联 GEO 范围查询。

**RediSearch Phase B（2026-07-29）：** FT.AGGREGATE `WITHCURSOR [COUNT n]` + `FT.CURSOR READ/DEL`（内存游标表，按 COUNT 分页，耗尽返回游标 0，空闲 1 分钟惰性回收）；FT.AGGREGATE `APPLY <expr> AS <name>` 最小表达式子集（`@field` 引用、数字字面量、`+ - * /` 标准优先级、括号、一元负号、非数值 `+` 退化为字符串拼接），按出现位置分为 GROUPBY 前（作用于逐文档字段，供后续 REDUCE 引用）与 GROUPBY 后（作用于结果行）；顺带修正：无 GROUPBY 且无 REDUCE 时按文档逐行返回（此前会错误地把所有文档折叠成一个空字段分组）。不含 FT.SEARCH WITHCURSOR（仍延期，见下）。

仍延期：精确 jemalloc 级 `used_memory`、FUNCTION DUMP 官方互通、Vector **量化**（Q8/BIN）、真 BM25/FT+KNN/完整 DIALECT 等（见计划文档）。

**兼容续研批次（2026-07-29）：** WAITAOF 真等待（本地 AOF fsync + 副本 ACK 循环）；LATENCY 命令路径采样 + HISTOGRAM；`notify-keyspace-events` 最小 K/E/g/$/x/e/A 发射；MIGRATE（DUMP→RESTORE→DEL，COPY/REPLACE/AUTH/KEYS）；LFU 对数计数逼近 Redis；FT.SEARCH WITHCURSOR（复用 FT.CURSOR 表）。

**Vector HNSW（2026-07-29）：** 内存 f32 HNSW 图已接入 VADD/VSIM/VREM/VINFO/VLINKS；`M`/`EF`（构建）与 VSIM `EF`/`TRUTH` 生效；小集合（≤64）与 `TRUTH` 走精确扫描。不含 Q8/BIN 量化与图持久化（DUMP 仍只存向量，恢复后重建图）。

## 已知差异（抽样，以代码为准）

| 主题 | 说明 |
|------|------|
| 默认端口 | Redis 6379；Godis **6399** |
| 集群 | ✅ 16384 槽 + CRC16；MOVED/ASK/ASKING/READONLY；NODES/SLOTS/INFO/SHARDS 读 FSM；ADDSLOTS/DELSLOTS* 写 FSM；SETSLOT/REPLICATE/FORGET 等未实现写路径显式 ERR |
| HLL | ✅ 算法/编码与 Redis 互通（xxHash64 + dense `HYLL` 编码 + 大范围修正）；稀疏 blob 拒绝 |
| EXEC | 已按 Redis：出错继续、不整事务回滚 |
| BLPOP / XREAD BLOCK | 真阻塞（等待队列 + 写路径唤醒） |
| 订阅态 | ✅ 仅 (P\|S)SUBSCRIBE/(P\|S)UNSUBSCRIBE/PING/QUIT/RESET；SSUBSCRIBE 真连接 |
| CLIENT REPLY / NO-TOUCH | ✅ REPLY 抑制写回；NO-TOUCH 跳过 LRU Touch |
| timeout | ✅ std Handler 按秒设 ReadDeadline 踢空闲连接 |
| COPY / FCALL | ✅ COPY/MOVE 经 DUMP 深拷贝；FCALL 用 gopher-lua，`redis.call` 走 execWithLock |
| Hash field TTL 命令 | ✅ HGETEX/HSETEX/HGETDEL 支持 Redis 8 `FIELDS`（兼旧语法） |
| CLIENT LIST / INFO | ✅ 真连接表；age/idle 同源格式 |
| XDELEX / XACKDEL | ✅ 逐 ID 状态数组（-1/1/2） |
| 协议错误 | ✅ 回写后关闭连接（std；gnet 本已 Close） |
| 配置文件 | ✅ 支持引号值与多空白分隔 |
| TS FILTER | ✅ `label=` / `label!=` 存在性语义 |
| GEOSEARCH / GEOHASH | ✅ FROMMEMBER 含自身；缺键按成员 null 数组 |
| GEORADIUS | ✅ WITH*/COUNT/ASC/DESC/STORE 经 GEOSEARCH 路径 |
| XREADGROUP / XPENDING | ✅ 历史重读递增 DeliveryCount；`-`/`+` 范围 |
| TS 聚合 | ✅ 含 **twa**（时间加权平均） |
| TS DUPLICATE_POLICY | ✅ BLOCK/FIRST/LAST/MIN/MAX/SUM + ON_DUPLICATE |
| SINTERCARD | ✅ LIMIT 提前终止 |
| VSIM FILTER | ✅ 最小 `.field==value` / `!=` 属性过滤 |
| VADD 选项 | ✅ NX/XX/SETATTR；**M/EF 接入真 HNSW**；CAS/NOQUANT/Q8/BIN/REDUCE 等仍 accept-no-op（无量化） |
| FT 短语 / SLOP | ✅ 引号短语 + positions 邻近；SLOP/INORDER/TIMEOUT 可解析 |
| save 自动快照 | ✅ `CONFIG save` 点位 + dirty 计数触发 BGSAVE |
| GEO geohash | ✅ 52-bit（float64 无损，对齐 Redis） |
| 复制 backlog | ✅ 固定容量环形缓冲 |
| Hash field TTL 持久化 | ✅ RDB/DUMP 经 Godis opaque（非 Redis 互通） |
| CONFIG RESETSTAT | ✅ 清零 INFO/cmd/net/rejected 计数 |
| CONFIG REWRITE | ✅ 写回配置文件（无配置文件时报错） |
| FUNCTION DUMP | ✅ Godis `GODISFN1` 二进制（非 Redis 互通） |
| CLIENT TRACKING | ✅ RESP3 本地 Push；RESP2 需 REDIRECT；GETREDIR 正确 |
| RESTORE IDLETIME/FREQ | ✅ 写入 eviction 元数据 |
| PING message | ✅ Bulk 回复（对齐 Redis） |
| MEMORY USAGE SAMPLES | ✅ 嵌套类型按 SAMPLES 采样估算（默认 5，0=全量） |
| LCS 缺键 | ✅ 视为空串（非空数组） |
| Protocol error | ✅ 消息用双引号包裹 |
| SRANDMEMBER 负 count | ✅ SimpleDict 真随机（可重复） |
| DUMP 扩展类型 | ✅ stream/JSON/vector/TS 经 Godis opaque（非 Redis 互通）；TS 含 DownsampleRules；Vector item 含 Attributes |
| FT.CREATE ON | ✅ HASH/JSON 校验并记录；仅 HASH 自动索引 HSET |
| CLIENT LIST TYPE | ✅ normal/master/replica/pubsub 过滤；flags/psub 对齐 |
| CLIENT PAUSE | ✅ WRITE/ALL 在 Exec 路径真正阻塞（UNPAUSE/CLIENT 豁免） |
| INFO stats | ✅ instantaneous_ops_per_sec；pubsub_channels/patterns 接 hub |
| SCRIPT/FUNCTION FLUSH | ✅ 校验 SYNC\|ASYNC 模式参数 |
| CLIENT KILL TYPE/USER | ✅ 按类型与 ACL 用户过滤 |
| CLIENT KILL LADDR/MAXAGE | ✅ 本地地址与最大存活时间过滤 |
| CLIENT LIST laddr | ✅ 输出 `laddr=` |
| HELLO id | ✅ 返回真实 client id |
| HELLO role | ✅ 随主从角色变化（master/slave） |
| ROLE | ✅ master/slave 角色与副本列表 |
| DUMP Bloom/HLL/Cuckoo | ✅ Godis opaque（非 Redis 互通） |
| DUMP CMS/TopK/TDigest | ✅ Godis opaque（非 Redis 互通） |
| INFO sync_* | ✅ sync_full / sync_partial_ok / sync_partial_err 计数（RESETSTAT 清零） |
| INFO blocked_clients | ✅ 含 List/Stream/**ZSet** 阻塞等待者 |
| OBJECT ENCODING | ✅ 含 hyperloglog / vectorset |
| XINFO STREAM / GROUPS / CONSUMERS | ✅ STREAM（含 FULL）为 Map；GROUPS/CONSUMERS 外层数组、项为 Map |
| XREAD / XREADGROUP | ✅ RESP2 正确嵌套 `[[stream,entries]]`；RESP3 顶层 Map（字段 Map） |
| ZRANDMEMBER | ✅ 正 count→Set；负 count 仍数组；WITHSCORES→ScorePairs |
| FT.SEARCH 选项 | ✅ 未知选项 syntax error；VERBATIM/NOSTOPWORDS/FILTER |
| Pub/Sub RESP3 | ✅ 无参 UNSUBSCRIBE/PUNSUBSCRIBE 用 `_` |
| MEMORY HELP | ✅ 子命令帮助数组 |
| CF.RESERVE EXPANSION | ✅ 存因子并在满时扩容 |
| WAIT | ✅ 循环内对副本发 GETACK |
| TDIGEST.ADD | ✅ VALUES / WEIGHTS |
| FT.ADD NOSAVE | ✅ 跳过 AOF |
| ACL %R~/%W~/%RW~ | ✅ 读写分离 key 模式；DRYRUN 按 prepare 读写键校验 |
| ACL `(...)` 选择器 | ✅ 独立权限集 + clearselectors；DRYRUN 联合校验 |
| ACL `&channel` | ✅ 解析/序列化；PUBLISH DRYRUN 校验 |
| ACL 运行时键/频道 | ✅ Exec 路径按 prepare 键与 pub/sub 频道 NOPERM |
| Lua cjson | ✅ encode/decode 最小集（gopher-lua） |
| Lua cmsgpack | ✅ pack/unpack 最小 MessagePack |
| Lua bit | ✅ tobit/band/bor/bxor/bnot/lshift/rshift/arshift/tohex/rol/ror/bswap |
| FT SCHEMA AS | ✅ `$.path AS name`（CREATE/ALTER 共用解析） |
| FT ON JSON 自动索引 | ✅ JSON.SET/DEL 喂/撤索引 |
| FT.SEARCH DIALECT/WITHSORTKEYS | ✅ DIALECT 接受忽略；WITHSORTKEYS 插入 sortkey |
| JSON.GET `$` 封装 | ✅ 显式 `$…` 路径包数组；无路径返回裸文档 |
| CONFIG/INFO hz | ✅ 可配置 hz（默认 10） |
| CLIENT 无连接 | ✅ SETNAME/GETNAME/ID/REPLY 要求连接 |
| COMMAND LIST | ✅ 枚举命令；FILTERBY PATTERN |
| GETRANGE 缺键 | ✅ 空 bulk（非 null） |
| OBJECT ENCODING int | ✅ 整数字符串报 `int` |
| Lua error/status_reply | ✅ 及 setresp 记录版本 |
| proto-max-bulk-len | ✅ 解析器读 CONFIG |
| INFO clients_in_timeout_table | ✅ 对齐 blocked 等待者计数 |
| SCAN 坏 MATCH | ✅ `ERR Invalid argument` |
| MONITOR | ✅ 流式广播（`BroadcastMonitor`） |
| FAILOVER | ✅ 真实协调切换（TO/FORCE/ABORT/TIMEOUT；复制流注入 REPLCONF FAILOVER，从库自提升、原主降级；ABORT 中断等待、并发互斥、错误文本对齐 Redis） |
| CLIENT UNBLOCK | ✅ 可唤醒 BLPOP/BZPOP/XREAD 等 |
| FT.CREATE SKIPINITIALSCAN | ✅ 缺省对已存在且匹配前缀/类型的键同步初始建库；给出该选项则跳过 |
| FT.CREATE STOPWORDS | ✅ 按 index 定制停用词表；`STOPWORDS 0` 关闭该索引的停用词过滤 |
| FT.SEARCH 同义词展开 | ✅ 查询词按 FT.SYNADD/SYNUPDATE 分组展开为 `term \| syn1 \| syn2`（不含短语内部展开） |
| FT.SEARCH 内联 GEO | ✅ `@field:[lon lat radius unit]` 语法（此前仅支持顶层 GEOFILTER 选项） |
| EXPIRE 非正 TTL | ✅ 立即删键；时间轮 tick 向上取整（小数秒 TTL 不再永不过期） |
| 主从过期传播 | ✅ 过期删除写 AOF → 经复制积压同步到从库 |
| WATCH 版本 | ✅ 仅在写命令成功后 bump（失败不误报） |
| 集群 ACL | ✅ 集群路径接入完整 ACL 校验（此前仅 requirepass） |
| ACL 命令类别 | ✅ @string/@list/@search/@json/@vector 等 + 扩展类别前缀回退 |
| 子命令错误 | ✅ `... . Try X HELP.` 后缀（Redis 8 格式） |
| 协议错误 | ✅ `ERR Protocol error:` 前缀（std + gnet）；RESP3 长错误用 BlobError `!` |
| TRACKINGINFO | ✅ 嵌套数组格式（flags/redirect/prefixes 分组） |
| ZINTERCARD / SORT_RO | ✅ 新命令 |
| EVAL_RO / EVALSHA_RO | ✅ 脚本内写命令拒绝 |
| SETBIT 上限 | ✅ 偏移 ≤ 2^32-1（防 OOM） |
| GETEX | ✅ 注册为写命令（只读副本不漂移） |
| RESET | ✅ 清认证 + REPLY 模式恢复 |
| 键空间通知 | ✅ 命令级事件（HSET/LPUSH/SADD/ZADD/XADD/LPOP/RPOP/SREM/SPOP/ZREM/HINCRBY/LSET/LTRIM/SETRANGE/APPEND/INCRBY）+ 事件字符校验 |
| ACL LOG | ✅ entry-id / created-at / client-info 字段 |
| COMMAND DOCS/INFO | ✅ acl_categories / tips / key_specs / subcommands |
| ACL SETUSER | ✅ `>` 追加、`#` SHA256、`<pw`/`!hash` 删除、resetpass/reset；Authenticate 检查 Enabled |
| DEBUG | ✅ SET-ACTIVE-EXPIRE（真开关）、DIGEST/DIGEST-VALUE（真 SHA1）、CHANGE-REPL-ID（轮换 replId）；RELOAD/JMAP/FLUSHALL 有意 stub |
| FLUSHDB/FLUSHALL | ✅ loadDB 继承 server/lockManager/evictionManager（清库后 CLIENT PAUSE 等不再失效） |
| RESTORE IDLETIME/FREQ | ✅ 不再被索引重建内部读取覆盖（no-touch） |

### 曾误标为「未实现」、现已有（勿再当缺口）

UNWATCH、WAIT（简版）、BITOP、BITFIELD、SMOVE、LPOS、XCLAIM、SHUTDOWN、RESET、PSUBSCRIBE、LOLWUT、LASTSAVE、ZRANGESTORE 等。

## 语义与运维

| 主题 | Redis | Godis |
|------|-------|-------|
| Lua 引擎 | 内置 | 默认 **gopher-lua**（`GODIS_LUA_ENGINE`）；**FCALL 同引擎**（M2z）；沙箱仅 base/table/string/math（无 os/io/package/debug） |
| 优雅关闭 | SIGTERM | std 路径 in-flight 等待；`SHUTDOWN` + hook |

## RESP3 核心回复形态（双形编码）

模式：命令返回实现 `RESP3Reply` 的类型（或 `MapReply`/`SetReply`/`DoubleReply`/`ScorePairsReply`）；`ToBytes()` 保持 RESP2 线格式，连接协议为 3 时经 `ReplyToRESP3` 发原生 RESP3。

| 类别 | 命令 / 回复 | RESP3 |
|------|-------------|--------|
| 简单类型 | Double/Boolean/BigNumber/Verbatim/Null | `,` `#` `(` `=` `_`（RESP2 降级为 bulk/int） |
| HELLO 3 | `HELLO 3` | 顶层 Map |
| Hash / Config | `HGETALL`、`CONFIG GET`、`HRANDFIELD` 正 count+WITHVALUES | Map |
| Set | `SMEMBERS`、`SINTER`/`SUNION`/`SDIFF`、`SPOP`/`SRANDMEMBER` 正 count | Set |
| ZSet 分数 | `ZSCORE`/`ZMSCORE`、`ZRANGE…WITHSCORES`、`ZPOP*`、`ZINCRBY`、`ZUNION…WITHSCORES` 等 | Double / ScorePairs |
| ZSet 成员 | `ZUNION`/`ZINTER`/`ZDIFF` 无 WITHSCORES；`ZRANDMEMBER` 正 count | Set |
| Stream | `XREAD`/`XREADGROUP`；`XINFO STREAM/GROUPS/CONSUMERS` | Map（GROUPS/CONSUMERS 为 Map 数组） |
| Introspection | `MEMORY STATS`；`ACL GETUSER`；`COMMAND DOCS`；`CLIENT TRACKINGINFO`；`FUNCTION STATS` | Map（嵌套 docs/engines；百分比 Double） |
| Functions / ACL / Module | `FUNCTION LIST`（每库 Map）；`ACL LOG`（条目 Map 数组）；`MODULE LIST`（空数组，有模块时为 Map） | 外层 Array + 元素 Map |
| Probabilistic / Search 配置 | `BF.INFO`/`CF.INFO`/`CMS.INFO`/`TOPK.INFO`/`TDIGEST.INFO`；`FT.CONFIG GET` | Map（数值 Int/Double） |
| TimeSeries / LCS / Latency | `TS.INFO`（`labels` 嵌套 Map）；`LCS … IDX`；`LATENCY HISTOGRAM`（嵌套 histogram_usec） | Map |
| Search / Cluster | `FT.SYNDUMP`；`FT.SPELLCHECK`（`results`）；`FT.PROFILE`（`Results`/`Profile`）；`CLUSTER SHARDS` | Map / 外层 Array+Map |

**仍延期 / 非本轮：** HSCAN/ZSCAN 第二段官方仍为 Array；jemalloc 级精确 `used_memory`（当前 maxmemory 为 per-key/大 value 近似）；Vector Q8/BIN；完整 BM25/KNN；FUNCTION DUMP 官方互通；集群 MEET/gossip 与 SETSLOT 写 FSM；opaque 与 Redis 原生模块 RDB 互通。

## 测试

- `go test ./...`；兼容批次测试见 `database/m2*_compat_test.go`、`m1_block_tx_bitmap_test.go` 等；RESP3 线格式见 `database/resp3_core_types_test.go`。

---

**最后更新：** 2026-08-09（opaque TS rules + vector attrs；maxmemory 大 value 估账）
