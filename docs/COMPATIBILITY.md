# Godis 与 Redis 8.x 兼容性说明

> 本文档描述 Godis **当前实现**与 Redis 8.x 的差异，避免「100% 兼容」的误导性表述。  
> 详细分项与里程碑见仓库根目录 [`兼容性改进计划.md`](../兼容性改进计划.md)（含 2026-07-24 进度更新）。  
> 完整命令列表见 [`commands.md`](../commands.md)（部分条目可能尚未与代码同步）。

## 评估维度

| 维度 | 大致覆盖 | 说明 |
|------|----------|------|
| RESP2/RESP3 协议 | 高 | HELLO 3、Push、客户端缓存、blob error `!`；核心命令双形 Map/Set/Double（见下节） |
| String/List/Hash/Set/ZSet | 高 | 常用命令齐全；List/ZSet **阻塞命令真阻塞** |
| Stream / Bitmap / Geo | 中–高 | XCLAIM/XAUTOCLAIM/BITOP/BITFIELD；**XREAD BLOCK 真阻塞**；`COUNT≤0`=不限；BLOCK 负/非法分文案；Stream 范围操作 O(log n)；`MAXLEN 0`/`LIMIT` 仅配 `~` 对齐 Redis |
| FAILOVER | ✅ 真实协调切换 | TO/FORCE/ABORT/TIMEOUT、复制流注入 REPLCONF FAILOVER、从库自提升+原主降级；见 [`FAILOVER_DESIGN.md`](FAILOVER_DESIGN.md) |
| JSON / Vector / Time Series | 中–高 | 子集 + 持续补全；Vector 保留 VS* 与 Redis 名双名 |
| RediSearch (FT.*) | 中–高 | Phase A/B：初始扫描、STOPWORDS、同义词、内联 GEO、AGGREGATE WITHCURSOR/APPLY；见下文 |
| 集群 (CLUSTER *) | 中–高 | 16384+CRC16；MOVED/ASK；NODES/SLOTS/INFO/SHARDS 读 FSM；MEET→Raft/FSM join（非 gossip）；SETSLOT + doMigrate 闭环缝（Importing/ASK/finish dropSlot） |
| ACL / 安全 | 中–高 | ACL 引擎；CONFIG `aclfile` 可存取（M2bh）；文件见 ACL LOAD/SAVE |
| 配置 | 中–高 | 布尔解析；CONFIG SET 含 maxmemory/save/tcp-backlog；**eviction 写路径已接**（per-key 估算，大 value 计入；淘汰写 AOF `DEL` 供复制）；`used_memory`≈`MemStats.Alloc`+峰值；`used_memory_rss`≈进程 RSS（Win/Linux；非 jemalloc）；`mem_allocator:go`；部分 CF-3 为存取桩 |
| 概率数据结构 (BF/CF/CMS…) | 中–高 | 见 `database/probabilistic.go`；CF EXPANSION 已接扩容 |

**M2 里程碑：** 至 **M2cm**（+ 集群管理 seam）。M2cl：Pub/Sub RESP3 Push、Lua HKEYS/HVALS/SSCAN→Array。M2cm：UNWATCH 可在 MULTI 内排队；CLIENT LIST 字段 `watch=`（及 tot-net-in/out、rbs/rbp）；ACL GETUSER 完整 `#`+SHA256；CLUSTER ADDSLOTS/DELSLOTS 写 FSM。后续已补：`CLUSTER MEET`→Raft/FSM join；`SETSLOT MIGRATING|IMPORTING|STABLE`→本地 slotsManager；`SETSLOT NODE`→`EventAssignSlots` 写 FSM 归属。

**RediSearch Phase A（2026-07-29）：** FT.CREATE 初始扫描回填 + SKIPINITIALSCAN；按 index 的 STOPWORDS（含 `STOPWORDS 0` 关闭过滤）；FT.SEARCH 查询词按 FT.SYNADD 同义词组展开；`@field:[lon lat radius unit]` 内联 GEO 范围查询。

**RediSearch Phase B（2026-07-29）：** FT.AGGREGATE `WITHCURSOR [COUNT n]` + `FT.CURSOR READ/DEL`（内存游标表，按 COUNT 分页，耗尽返回游标 0，空闲 1 分钟惰性回收）；FT.AGGREGATE `APPLY <expr> AS <name>` 最小表达式子集（`@field` 引用、数字字面量、`+ - * /` 标准优先级、括号、一元负号、非数值 `+` 退化为字符串拼接），按出现位置分为 GROUPBY 前（作用于逐文档字段，供后续 REDUCE 引用）与 GROUPBY 后（作用于结果行）；顺带修正：无 GROUPBY 且无 REDUCE 时按文档逐行返回（此前会错误地把所有文档折叠成一个空字段分组）。**FT.SEARCH WITHCURSOR** 已续研落地（复用 FT.CURSOR 表）。

仍延期：精确 jemalloc 级 `used_memory`（现已贴近 `MemStats.Alloc` 峰值跟踪 + 进程 RSS→`used_memory_rss`，`allocator_*` 为 Go MemStats 镜像、仍非 jemalloc）、FUNCTION DUMP 官方互通、完整 BM25/完整 DIALECT/完整 KNN 方言等（见计划文档；**FT+KNN 最小路径**已接通；**Q8 图内 int8 距离**与 **BIN Hamming** 已落地；**GEOSHAPE 强制 DIALECT≥3**）。

**兼容续研批次（2026-07-29）：** WAITAOF 真等待（本地 AOF fsync + 副本 ACK 循环）；LATENCY 命令路径采样 + HISTOGRAM；`notify-keyspace-events` 最小 K/E/g/$/x/e/A 发射；MIGRATE（DUMP→RESTORE→DEL，COPY/REPLACE/AUTH/KEYS）；LFU 对数计数逼近 Redis；FT.SEARCH WITHCURSOR（复用 FT.CURSOR 表）。

**Vector HNSW（2026-07-29 / Q8+BIN / BIN Hamming / Q8 int8 2026-08-11）：** 内存 HNSW 图已接入 VADD/VSIM/VREM/VINFO/VLINKS；`M`/`EF` 与 VSIM `EF`/`TRUTH` 生效。**VADD Q8** 存 int8+range，**HNSW/VSIM 对 int8 codes 算距离**（cosine 无 f32 缓冲；L2/dot 用 range 缩放，不物化反量化数组）；**VADD BIN** 存 1-bit/dim，**HNSW/VSIM 用 Hamming**；默认/NOQUANT 仍为 f32。DUMP opaque 可保留 Q8/BIN codes。

## 已知差异（抽样，以代码为准）

| 主题 | 说明 |
|------|------|
| 默认端口 | Redis 6379；Godis **6399** |
| 集群 | ✅ 16384 槽 + CRC16；MOVED/ASK/ASKING/READONLY；NODES/SLOTS/INFO/SHARDS 读 FSM；ADDSLOTS/DELSLOTS* 写 FSM；MEET→`cluster.join`/EventJoin（需 `raft-port`，**非** Redis gossip）；SETSLOT 本地 MIGRATING/IMPORTING/STABLE；`NODE`→FSM `EventAssignSlots`（本节点清迁移态）；真实 `doMigrateSlot` 设 Importing/migratePeer、成功 finish 才 dropSlot；ASK 读 FSM Migratings；GETKEYSINSLOT/COUNTKEYSINSLOT 读本地 slotsManager；REPLICAS 读 FSM；**REPLICATE→FSM MasterSlaves**；**FORGET→EventForget 安全清理**；BUMPEPOCH 为 no-op（`BUMPED 0`）；`INFO cluster` 与 CLUSTER INFO 同源；消息计数映射 `cluster.heartbeat`/MEET/`cluster.join`（**`meet_received` 接本地 join**；**`cluster_bus_port:0`**，无真 gossip bus）；RESET/SAVECONFIG/CLUSTER FAILOVER 等仍 ERR |
| HLL | ✅ 算法/编码与 Redis 互通（xxHash64 + dense `HYLL` 编码 + 大范围修正）；sparse **读取→dense 提升**（写出仍 dense） |
| EXEC | 已按 Redis：出错继续、不整事务回滚 |
| BLPOP / XREAD BLOCK | 真阻塞（等待队列 + 写路径唤醒） |
| 订阅态 | ✅ 仅 (P\|S)SUBSCRIBE/(P\|S)UNSUBSCRIBE/PING/QUIT/RESET；SSUBSCRIBE 真连接 |
| CLIENT REPLY / NO-TOUCH | ✅ REPLY 抑制写回；NO-TOUCH 跳过 LRU Touch |
| timeout | ✅ std Handler 按秒设 ReadDeadline 踢空闲连接 |
| COPY / FCALL | ✅ COPY/MOVE 经 DUMP 深拷贝；FCALL 用 gopher-lua，`redis.call` 走 execWithLock |
| Hash field TTL 命令 | ✅ HGETEX/HSETEX/HGETDEL + HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT（TTL/`0` 立即删字段、`<0` 拒）；命令本身写 AOF；字段到期写 AOF `HDEL`（时间轮+惰性）；主动过期持 DB 键锁；纯 AOF rewrite 写 `HPEXPIREAT … FIELDS n …` |
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
| VADD 选项 | ✅ NX/XX/SETATTR；**M/EF 接入真 HNSW**；**VSIM EPSILON**；**Q8/BIN 真量化存储**；**BIN 图内 Hamming**；**Q8 图内 int8 距离**；CAS/NOTHREAD/REDUCE 仍 accept-no-op |
| FT 短语 / SLOP | ✅ 引号短语 + positions 邻近；SLOP/INORDER/TIMEOUT 可解析 |
| save 自动快照 | ✅ `CONFIG save` 点位 + dirty 计数触发 BGSAVE |
| GEO geohash | ✅ 52-bit（float64 无损，对齐 Redis） |
| 复制 backlog | ✅ 固定容量环形缓冲；PSYNC 已追上 tip（含空 backlog）允许 CONTINUE |
| Hash field TTL 持久化 | ✅ RDB/DUMP 经 Godis opaque（非 Redis 互通） |
| CONFIG RESETSTAT | ✅ 清零 INFO/cmd/net/rejected 计数 |
| CONFIG REWRITE | ✅ 写回配置文件（无配置文件时报错） |
| FUNCTION DUMP | ✅ Godis `GODISFN1`（非 Redis 互通；官方 `0xF5`/`0xF6`/`REDIS####` 与异己二进制明确 ERR） |
| CLIENT TRACKING | ✅ RESP3 本地 Push；RESP2 需 REDIRECT；GETREDIR 正确；`CLIENT CACHING` 仅 OPTIN/OPTOUT（YES↔OPTIN、NO↔OPTOUT） |
| RESTORE IDLETIME/FREQ | ✅ 写入 eviction 元数据 |
| PING message | ✅ Bulk 回复（对齐 Redis） |
| MEMORY USAGE SAMPLES | ✅ 嵌套类型按 SAMPLES 采样估算（默认 5，0=全量） |
| LCS 缺键 | ✅ 视为空串（非空数组） |
| Protocol error | ✅ 消息用双引号包裹 |
| SRANDMEMBER 负 count | ✅ SimpleDict 真随机（可重复） |
| DUMP 扩展类型 | ✅ stream/JSON/vector/TS 经 Godis opaque（非 Redis 互通）；TS 含 DownsampleRules；Vector item 含 Attributes；Stream 含 consumers/PEL + entriesAdded/maxDeletedID |
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
| WAIT | ✅ 循环内对副本发 GETACK；以 REPLCONF ACK offset 为准（发送路径不抬 ACK） |
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
| FT.SEARCH DIALECT/WITHSORTKEYS | ✅ DIALECT 1/2/3 子集生效（非完整方言；GEOSHAPE 强制 ≥3；tag 空格/多字段强制 ≥2）；WITHSORTKEYS 插入 sortkey |
| JSON.GET `$` 封装 | ✅ 显式 `$…` 路径包数组；无路径返回裸文档 |
| CONFIG/INFO hz | ✅ 可配置 hz（默认 10） |
| CLIENT 无连接 | ✅ SETNAME/GETNAME/ID/REPLY 要求连接 |
| COMMAND LIST | ✅ 枚举命令；FILTERBY PATTERN |
| GETRANGE 缺键 | ✅ 空 bulk（非 null） |
| OBJECT ENCODING int | ✅ 整数字符串报 `int` |
| TYPE 扩展类型 | ✅ JSON/TS/Bloom/Cuckoo/CMS/TopK/TDigest/Vector/FT 索引对齐 Redis 模块 TYPE 名（非 `ERR unknown`） |
| Pop count=0 | ✅ `SPOP`/`LPOP`/`RPOP`/`ZPOPMIN|MAX` 空数组且不改键；`ZMPOP`/`LMPOP` COUNT≤0 → `ERR count should be greater than 0` |
| BLMPOP 无 COUNT | ✅ 最少 4 参合法；负超时分文案（`negative` / `out of range` / `not a float…`） |
| ZADD 选项互斥 | ✅ `NX`+`GT`/`LT`、`GT`+`LT`、`XX`+`NX` ERR 文案对齐 Redis |
| PFADD 仅 key | ✅ 创建空 HLL 返回 1（arity -2） |
| INCRBYFLOAT NaN | ✅ 存值/`nan` 增量 / `inf` 增量 ERR 与 Redis 8.x 对齐（含 HINCRBYFLOAT） |
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
| 主从淘汰传播 | ✅ maxmemory 淘汰写 AOF `DEL` + 键空间 `evicted`（与过期路径一致） |
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
| Lua 引擎 | 内置 | 默认 **gopher-lua**（`GODIS_LUA_ENGINE`）；**FCALL 同引擎**（M2z）；沙箱仅 base/table/string/math（无 os/io/package/debug）；legacy 引擎同等拒绝 `os./io./require/dofile/loadfile` |
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
| ZSet 阻塞弹出 | `BZPOPMIN`/`BZPOPMAX` | MultiRaw `[key, member, Double]`（RESP2 score 仍为 bulk） |
| ZSet 成员 | `ZUNION`/`ZINTER`/`ZDIFF` 无 WITHSCORES；`ZRANDMEMBER` 正 count | Set |
| Stream | `XREAD`/`XREADGROUP`；`XINFO STREAM/GROUPS/CONSUMERS` | Map（GROUPS/CONSUMERS 为 Map 数组） |
| Introspection | `MEMORY STATS`；`ACL GETUSER`；`COMMAND DOCS`；`CLIENT TRACKINGINFO`；`FUNCTION STATS` | Map（嵌套 docs/engines；百分比 Double） |
| Functions / ACL / Module | `FUNCTION LIST`（每库 Map）；`ACL LOG`（条目 Map 数组）；`MODULE LIST`（空数组，有模块时为 Map） | 外层 Array + 元素 Map |
| Probabilistic / Search 配置 | `BF.INFO`/`CF.INFO`/`CMS.INFO`/`TOPK.INFO`/`TDIGEST.INFO`；`FT.CONFIG GET` | Map（数值 Int/Double） |
| TimeSeries / LCS / Latency | `TS.INFO`（`labels` 嵌套 Map）；`LCS … IDX`；`LATENCY HISTOGRAM`（嵌套 histogram_usec） | Map |
| Search / Cluster | `FT.SYNDUMP`；`FT.SPELLCHECK`（`results`）；`FT.PROFILE`（`Results`/`Profile`）；`CLUSTER SHARDS` | Map / 外层 Array+Map |

**仍延期 / 非本轮（远期/非目标）：** 见下节「兼容里程碑关闭」。

## 兼容里程碑关闭（2026-08-10）

> **关闭口径：** 可独立完成的正确性/兼容小项已扫尽；**不宣称 100% Redis 兼容**。进行中的兼容清单清空到仅剩下列远期非目标（写明「不是假装已实现」）。

| 远期非目标 | 现状（诚实） | 本轮小步（非宣称完成） |
|------------|--------------|------------------------|
| jemalloc / 真 OS RSS | `used_memory`≈`MemStats.Alloc` 峰值 + per-key dataset；`mem_allocator:go`；`allocator_*`=`HeapAlloc`/`HeapSys`/`Sys`（**非** jemalloc arenas）；`MEMORY STATS` `allocator=go` + `process.rss` | RSS 优先真进程；**绝不**写 jemalloc；详见下节「内存核算 / jemalloc 边界」 |
| 完整 Redis gossip bus | MEET→Raft/FSM join；**REPLICATE/FORGET 接 FSM**（非 gossip）；BUMPEPOCH=`BUMPED 0`；无 CLUSTERMSG 二进制 bus | **`cluster_bus_port:0`**；ping/pong + **meet_sent（MEET 发起）/meet_received（`cluster.join` 本地 apply）** 映射内部 RPC（非宣称已有 gossip bus）；RESET/SAVECONFIG/CLUSTER FAILOVER/真 epoch/`fail` 传播仍延期 |
| 官方模块原生 RDB·DUMP 互通 | Stream/JSON/Vector/TS/概率结构/FT 走 Godis opaque `GODIS1`；见「官方模块 RDB/DUMP 边界」 | RESTORE / LoadRDB **明确 ERR**（模块 type 标记 / ModuleTypeObject；兼拒绝矩阵）；**不**与 Redis 模块 RDB 互通；远期仍开放 |
| FUNCTION DUMP 官方互通 | Godis `GODISFN1` 自洽；**显式拒绝**官方 `0xF5`/`0xF6`/`REDIS####`；截断/异己二进制明确 ERR；兼旧文本 | 见「官方 FUNCTION DUMP 边界」；**禁止假互通** |
| 完整 BM25 / 完整 KNN 方言 / 完整 DIALECT | BM25STD：WEIGHT + 文档长度 + **可测 IDF** + **多字段加权求和**；NORM min-max；**TANH + BM25STD_TANH_FACTOR**；KNN：`AS`/`$YIELD_DISTANCE_AS`、**HYBRID_POLICY** 校验、空预过滤；DIALECT 1/2/3 子集（GEOSHAPE≥3；**tag 空格/多字段 `@f1\|f2` 强制 ≥2**） | **非**论文级完整 BM25（无 slop 罚分进打分、无全局 NORM over collection 等）；非完整 KNN/DIALECT 4；见 REDISEARCH_ALIGNMENT |
| ~~Vector 图内真 int8 距离~~ | **VADD Q8/BIN 真存储**；**BIN→图内 Hamming**；**Q8→图内 int8 距离**（无搜索态 f32 缓冲）；FT VECTOR 窄类型解码已有 | ✅ 2026-08-11 deep6；VEMB 仍可显示反量化近似 |
| ~~AOF rewrite / RDB 写出 FT 索引定义~~ | 命令 AOF + **纯 AOF rewrite→FT.CREATE** + **RDB Godis opaque `ft`**（Load 后回填） | ✅ 2026-08-11；**非**官方 RediSearch 模块 RDB |
| ~~HLL sparse 读取~~ | dense 互通；**sparse 安全解码→内存 dense**（写回仍 dense） | ✅ 2026-08-11：corrupt/非 dense·sparse 编码→`INVALIDOBJ`；已移出远期清单 |
| CI Redis sidecar 全量输出 diff（R4-1） | 未做全量 | **扩大 allowlist**（`scripts/r4-1-cases.txt`）：String/Hash/List/Set/ZSet/TTL + **Stream lite**（显式 ID XADD/XLEN）+ **Geo lite**（GEOADD/ZCARD/TYPE）+ **Bitops**（SETBIT/GETBIT/BITCOUNT/BITOP）+ **HLL lite**（PFADD/PFCOUNT 小基数）；`@skip`/`@todo` 诚实标注已知洞；**仍非** FT/模块/DUMP/集群/无序回复全量套件 |
| 覆盖率专项冲高（R4-2） | 未做 | 书面远期；**观察式门槛**见 `.github/workflows/coverall.yml` 注释（Coveralls 趋势、无私有 % 门禁、不因覆盖率 fail） |

### R4-1 套件边界

> Sidecar allowlist（`scripts/r4-1-cases.txt` + `redis-sidecar-diff.{sh,ps1}`）是**对照样例**，不是「已跑通官方 Redis Test」或全命令面兼容声明。

| 边界 | 说明 |
|------|------|
| 范围 | Standalone 稳定命令：String/Hash/List/Set/ZSet/TTL + Stream/Geo/Bitops/HLL **lite** 子集 |
| 排除 | **FT.\*** / 模块 / DUMP·RESTORE / ACL / cluster·gossip / FUNCTIONS ——除非用例表显式列入（当前未列） |
| Honesty | `@skip` / `@todo` 记录已知洞；**禁止**把未对齐行为改成假 PASS |
| 已知 skip/todo | `GEODIST`/`GEOPOS` 浮点与嵌套 `--raw`（不做脆弱精确浮点）；`XRANGE` 嵌套布局；大基数 `PFCOUNT` 近似漂移；`BITOP DIFF*` Godis 已实现（单测覆盖）但 CI Redis tag `8` 可能 &lt;8.2 故 R4-1 不断言；XGROUP/XREAD BLOCK |
| 跑法 | 两侧 `redis-cli --raw` 等值（或 `>=N`/`<=N` 整数）；CI smoke 非全量 diff |

### BM25 / KNN / DIALECT 推进笔记（2026-08-11，`compat/bm25-knn-dialect`）

> **不宣称 100% 论文级 BM25 / 完整 RediSearch 方言。** 下列为对本仓可验证子集的诚实盘点。

| 能力 | 已落地（可测） | 仍缺 / 故意简化 |
|------|----------------|-----------------|
| BM25STD 核心 | IDF `ln((N−df+0.5)/(df+0.5)+1)`；`b` 文档长度归一；按 TEXT 字段 WEIGHT 求和；k1=1.2 / b=0.09 | BM25 slop 罚分进分数；与 Redis 二进制级分数对齐；全局 collection 统计变体细枝 |
| BM25STD.NORM / .TANH | 结果集真 min-max；`tanh(raw/factor)`，默认 factor 4；**`BM25STD_TANH_FACTOR Y`** | —.NORM 依赖全 hit 集非全集强制扫描 |
| KNN | `*=>[KNN …]` / 预过滤；`AS` 与 **`=>{$YIELD_DISTANCE_AS}`**；`EF_RUNTIME`；`HYBRID_POLICY`∈{ADHOC_BF,BATCHES}（均暴力）；空候选→0 | 真 BATCHES 迭代；`$SHARD_K_RATIO` 集群语义；HNSW 近似保证≠暴力 |
| DIALECT | 1/2 优先级；2：PARAMS/KNN/比较/`ismissing`/tag 空格/`@f1\|f2`；3：GEOSHAPE；非法值与具体 ERR | DIALECT 3 多值 JSON 全量返回；DIALECT 4/WITHOUTCOUNT 排序优化 |

### Godis opaque / FUNCTION 信封边界（非 Redis 互通）

| Magic | 用途 | 互通 |
|-------|------|------|
| `GODIS1\0` + JSON `{t,d}` | DUMP/RDB/AOF 扩展类型（stream/json/vector/ts/hexpire/bloom/cuckoo/cms/topk/tdigest/**ft**） | 仅 Godis↔Godis |
| `GODISFN1` + 长度前缀库列表 | FUNCTION DUMP/RESTORE | 仅 Godis↔Godis（兼旧文本 RESTORE；截断 GODISFN1 / 异己二进制→ERR） |

### 官方模块 RDB/DUMP 边界

> **不支持、不伪造互通。** Godis 扩展类型（JSON / Vector / TS / 概率结构 / FT 索引定义等）只经自研 `GODIS1` opaque 往返；与 Redis 官方模块（ReJSON、RediSearch、RedisBloom…）的原生 RDB / `DUMP` 字节 **不** 双向兼容。

| 路径 | 行为（诚实） |
|------|----------------|
| `RESTORE` | 识别 RDB `typeModule`/`typeModule2` 标记 → 明确 `ERR … module type …`；截断/坏版本/坏 CRC/异己样载荷 → `ERR DUMP payload version or checksum…`（文案标明非模块 RDB） |
| `LoadRDB` / 启动读 `dump.rdb` | 解析到 `ModuleTypeObject` → **返回 error 中止加载**（不再静默丢弃该键） |
| Godis 自写入 | 扩展类型编码为 string 载体上的 `GODIS1` opaque；**不是**官方模块 RDB type |

负向测：`database/dump_test.go`（`TestRestoreRejectsOfficialModuleTypeMarkers` / `TestRestoreRejectsSyntheticModuleDUMP`）、`database/module_rdb_boundary_test.go`（`TestLoadRDBRejectsOfficialModuleType`）。合成字节夹具，不依赖下载 Redis。

### 官方 FUNCTION DUMP 边界

> **禁止假互通。** Godis **不**解析、**不**接受官方 Redis `FUNCTION DUMP` 二进制；也不把 `GODISFN1` 伪装成 Redis wire。

| 载荷形态 | Godis 行为 |
|----------|------------|
| Godis `GODISFN1` 信封 | DUMP 写出 / RESTORE 严格解析；截断、坏长度、尾随垃圾→明确 ERR |
| 官方 Redis DUMP（首字节 `0xF5`=`RDB_OPCODE_FUNCTION2` 或 `0xF6`=`FUNCTION_PRE_GA`；或误投完整 `REDIS####` RDB 头） | **立即 ERR**（文案含 official / `0xF5`/`0xF6`）；**不**静默 OK；拒绝发生在 FLUSH **之前** |
| 其它异己二进制（NUL / 非法 UTF-8 / 高比例控制字节） | ERR（要求 GODISFN1；标明非 Redis official） |
| 旧版纯文本库 dump（`#!lua name=…`） | 仍接受（Godis 兼容路径，非 Redis 二进制） |

远期「FUNCTION DUMP 官方互通」仍为**非目标**；本边界仅诚实拒绝，不宣称完成互通。

### 内存核算 / jemalloc 边界

> **不实现 jemalloc。** 字段名可与 Redis 客户端对齐，数值是 Go runtime / 进程估账，**不是** OS jemalloc RSS 对账。

| INFO / MEMORY 字段 | Godis 口径（诚实） | 明确不是 |
|--------------------|-------------------|----------|
| `mem_allocator` | 恒为 **`go`** | **绝不** `jemalloc`（`CONFIG jemalloc-bg-thread` 仅存取桩，不改此字段） |
| `used_memory` / `_human` / `_peak*` | `runtime.MemStats.Alloc` 及 Alloc 高水位 | jemalloc `allocated` / arena 账 |
| `used_memory_rss` / `_human` | 优先真进程 RSS（Win WorkingSet / Linux VmRSS）；不可得时回退 `MemStats.Sys` | jemalloc resident / Redis jemalloc RSS 对齐 |
| `used_memory_startup` | 启动时 `MemStats.Sys` 粗基线 | jemalloc 启动账 |
| `used_memory_dataset` / `_perc` / `_overhead` | per-key 估算（`keyCount×floor`）相对 Alloc | jemalloc 精确 dataset |
| `used_memory_lua` / `_scripts` | ≈ 全局 Lua 引擎占用 | jemalloc arenas |
| `allocator_allocated` / `_active` / `_resident` / `_frag_ratio` | `HeapAlloc` / `HeapSys` / `Sys` / `HeapSys÷HeapAlloc` 镜像 | jemalloc arenas / bins / extents |
| `MEMORY STATS` `allocator` | **`go`**；并附 `process.rss` | `allocator=jemalloc*` |
| `MEMORY MALLOC-STATS` | 明示 Go runtime 不可用（无 arena 伪表） | 伪造 jemalloc `malloc_stats_print` |

**远期仍开放：** jemalloc 级 `used_memory` / 完整 OS 级内存会计（见 TODO「明确非目标」）。本边界小步=诚实化+负向测试，**≠** jemalloc 完成。

## 测试

- `go test ./...`；兼容批次测试见 `database/m2*_compat_test.go`、`m1_block_tx_bitmap_test.go` 等；RESP3 线格式见 `database/resp3_core_types_test.go`。
- jemalloc 边界负向测：`database/memory_jemalloc_boundary_test.go`（`mem_allocator:go`、MALLOC-STATS 无 arena 伪表、`jemalloc-bg-thread` 不改 allocator）。

---

**最后更新：** 2026-08-16（第四十二批可关闭：FCALL_RO；LOLWUT 负 VERSION；CONFIG cob/shutdown/locale/human-nodename/latency-percentiles/active-defrag-*；对照 Redis **8.10.0**；远期仍 **7**）
