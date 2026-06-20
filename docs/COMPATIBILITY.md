# Godis 与 Redis 8.x 兼容性说明

> 本文档描述 Godis **当前实现**与 Redis 8.x 的差异，避免「100% 兼容」的误导性表述。  
> 命令级覆盖约 **70–85%**（因模块而异）；RESP 协议与经典数据类型覆盖较高。  
> 完整命令列表见 [`commands.md`](../commands.md)（部分条目尚未与代码同步）。

## 评估维度

| 维度 | 大致覆盖 | 说明 |
|------|----------|------|
| RESP2/RESP3 协议 | 高 | HELLO 3、Push、客户端缓存失效推送 |
| String/List/Hash/Set/ZSet | 高 | 常用命令齐全，部分边缘命令缺失 |
| Stream / Bitmap / Geo | 中–高 | 核心命令有，XCLAIM/WAIT 等缺失 |
| JSON / Vector / Time Series | 中 | 子集实现，与 Redis Stack/8.x 有差距 |
| RediSearch (FT.*) | 中 | 搜索/聚合有，Sug*、部分 Syn 命令名不一致 |
| 集群 (CLUSTER *) | 低–中 | 仅 NODES/INFO/SLOTS/KEYSLOT/HELP 等子集 |
| ACL / 安全 | 中 | ACL 引擎已集成；`aclfile` 配置项尚未实现 |
| 概率数据结构 (BF/CF/CMS…) | 中–高 | 见 `database/probabilistic.go` |

## 已知命令差异（抽样）

### commands.md 与代码不一致

| 文档/Redis 惯例 | Godis 实际 |
|-----------------|------------|
| HEXPIRE 族 | **HGETEX / HSETEX / HGETDEL**（`database/hash_expire.go`） |
| FT.SYNADD | **FT.SynUpdate / FT.SynDump** |
| VS.DROPINDEX | **未实现** |
| CLUSTER MEET / ADDSLOTS 等 15+ 子命令 | **未实现**（见 `cluster/commands/`） |
| SLOWLOG 不支持（旧文档） | **已实现**（`database/slowlog.go`） |
| UNWATCH | ACL 分类有，**无执行路径** |
| BITOP / SMOVE / XCLAIM / WAIT | **未实现** |

### 代码已实现但 commands.md 未列

GETEX/GETDEL、COPY/MOVE/SWAPDB、LMPop/BLMPop/ZMPop、GEOSEARCH、FT.Sug*、SCRIPT DEBUG 系列等。

## 语义与运维差异

| 主题 | Redis | Godis |
|------|-------|-------|
| 默认端口 | 6379 | **6399**（`main.go` / `example.conf`） |
| 多 DB 事务 | 不支持跨 DB 原子 | 同 Redis；`MULTI/EXEC` 仅单 DB |
| Lua 引擎 | Redis 内置 | 默认 **gopher-lua**（`GODIS_LUA_ENGINE`） |
| 优雅关闭 | SIGTERM 等待命令完成 | std 服务器已实现 in-flight 等待；gnet 路径较简 |
| AOF rewrite | 单实例互斥 | 已加 rewrite 互斥锁 |

## 测试与质量

- `go test ./...` 全绿；部分包无测试或覆盖率偏低（见 [`改进意见_2026-06.md`](../改进意见_2026-06.md) §4）。
- `redis/parser` 提供 Fuzz 测试；CI 含 `go vet` 与选定包的 `-race`。

## 历史报告

2026 年兼容性过程报告已归档至 [`docs/history/`](history/)，仅供查阅，**以本文与代码为准**。

---

**最后更新：** 2026-06-20
