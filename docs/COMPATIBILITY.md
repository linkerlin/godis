# Godis 与 Redis 8.x 兼容性说明

> 本文档描述 Godis **当前实现**与 Redis 8.x 的差异，避免「100% 兼容」的误导性表述。  
> 详细分项与里程碑见仓库根目录 [`兼容性改进计划.md`](../兼容性改进计划.md)（含 2026-07-24 进度更新）。  
> 完整命令列表见 [`commands.md`](../commands.md)（部分条目可能尚未与代码同步）。

## 评估维度

| 维度 | 大致覆盖 | 说明 |
|------|----------|------|
| RESP2/RESP3 协议 | 高 | HELLO 3、Push、客户端缓存、blob error `!` |
| String/List/Hash/Set/ZSet | 高 | 常用命令齐全；List/ZSet **阻塞命令真阻塞** |
| Stream / Bitmap / Geo | 中–高 | XCLAIM/XAUTOCLAIM/BITOP/BITFIELD；**XREAD BLOCK 真阻塞** |
| FAILOVER | 最小子集 | 选项解析 + 无副本错误；完整协调切换仍缺 |
| JSON / Vector / Time Series | 中–高 | 子集 + 持续补全；Vector 保留 VS* 与 Redis 名双名 |
| RediSearch (FT.*) | 中–高 | SEARCH/AGGREGATE/ALIAS/ALTER/EXPLAIN/CONFIG/PROFILE 等 |
| 集群 (CLUSTER *) | 低–中 | 槽位算法与官方不兼容（**延期**）；子集命令 |
| ACL / 安全 | 中–高 | ACL 引擎；`aclfile` 配置项尚未实现 |
| 配置 | 中–高 | 布尔解析；CONFIG SET 含 maxmemory/save/tcp-backlog；**eviction 写路径已接**（键数估算）；部分 CF-3 为存取桩 |
| 概率数据结构 (BF/CF/CMS…) | 中–高 | 见 `database/probabilistic.go`；CF EXPANSION 已接扩容 |

**M2 里程碑：** 至 **M2as** 已含 ACL 选择器/`cjson`/`cmsgpack`/JSON 自动索引。仍延期：集群 CRC16/MOVED、HLL 互通、真 HNSW、完整 FAILOVER、精确 `used_memory`、FUNCTION DUMP 官方互通、MIGRATE 等（见计划文档）。

## 已知差异（抽样，以代码为准）

| 主题 | 说明 |
|------|------|
| 默认端口 | Redis 6379；Godis **6399** |
| 集群 | 1024+CRC32 vs 官方 16384+CRC16；无 MOVED/ASK |
| HLL | 算法/编码与 Redis **不互通**（延期对齐） |
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
| DUMP 扩展类型 | ✅ stream/JSON/vector/TS 经 Godis opaque（非 Redis 互通） |
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
| XINFO STREAM FULL | ✅ COUNT 截断 entries 嵌套数组 |
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
| Lua cjson | ✅ encode/decode 最小集（gopher-lua） |
| Lua cmsgpack | ✅ pack/unpack 最小 MessagePack |
| FT ON JSON 自动索引 | ✅ JSON.SET/DEL 喂/撤索引 |
| MONITOR | ✅ 流式广播（`BroadcastMonitor`） |
| FAILOVER | ✅ 最小子集（ABORT/FORCE/TO/TIMEOUT）；完整协调切换仍缺 |
| CLIENT UNBLOCK | ✅ 可唤醒 BLPOP/BZPOP/XREAD 等 |

### 曾误标为「未实现」、现已有（勿再当缺口）

UNWATCH、WAIT（简版）、BITOP、BITFIELD、SMOVE、LPOS、XCLAIM、SHUTDOWN、RESET、PSUBSCRIBE、LOLWUT、LASTSAVE、ZRANGESTORE 等。

## 语义与运维

| 主题 | Redis | Godis |
|------|-------|-------|
| Lua 引擎 | 内置 | 默认 **gopher-lua**（`GODIS_LUA_ENGINE`）；**FCALL 同引擎**（M2z） |
| 优雅关闭 | SIGTERM | std 路径 in-flight 等待；`SHUTDOWN` + hook |

## 测试

- `go test ./...`；兼容批次测试见 `database/m2*_compat_test.go`、`m1_block_tx_bitmap_test.go` 等。

---

**最后更新：** 2026-07-25
