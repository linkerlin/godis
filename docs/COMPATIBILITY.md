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
| 概率数据结构 (BF/CF/CMS…) | 中–高 | 见 `database/probabilistic.go` |

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
| COPY / FCALL | ✅ COPY/MOVE 经 DUMP 深拷贝；FCALL redis.call 走 execWithLock |
| Hash field TTL 命令 | ✅ HGETEX/HSETEX/HGETDEL 支持 Redis 8 `FIELDS`（兼旧语法） |
| CLIENT LIST | ✅ 真连接表；age/idle 按连接时钟 |
| MONITOR | ✅ 流式广播（`BroadcastMonitor`） |
| FAILOVER | ✅ 最小子集（ABORT/FORCE/TO/TIMEOUT）；完整协调切换仍缺 |
| CLIENT UNBLOCK | ✅ 可唤醒 BLPOP/BZPOP/XREAD 等 |

### 曾误标为「未实现」、现已有（勿再当缺口）

UNWATCH、WAIT（简版）、BITOP、BITFIELD、SMOVE、LPOS、XCLAIM、SHUTDOWN、RESET、PSUBSCRIBE、LOLWUT、LASTSAVE、ZRANGESTORE 等。

## 语义与运维

| 主题 | Redis | Godis |
|------|-------|-------|
| Lua 引擎 | 内置 | 默认 **gopher-lua**（`GODIS_LUA_ENGINE`） |
| 优雅关闭 | SIGTERM | std 路径 in-flight 等待；`SHUTDOWN` + hook |

## 测试

- `go test ./...`；兼容批次测试见 `database/m2*_compat_test.go`、`m1_block_tx_bitmap_test.go` 等。

---

**最后更新：** 2026-07-24
