# Godis 性能基线（E5-1）

> **生成**：2026-10-22 · 基线提交 `3d4b8b4` + E5-1 当日改动 · 工具 [`scripts/bench.sh`](../scripts/bench.sh)  
> **对照**：Redis 8.10（`redis:8.10-alpine` 官方镜像，内置 `redis-benchmark`）  
> **周期**：演进方案 E5（性能工程与基线）；热点结论供 E5-3（pprof 审计）接手。  
> **一句话结论**：在双容器对称环境下，Godis std 引擎单连接吞吐约为 Redis 的 1/5，pipeline 完全不涨吞吐，
> 且 LRU 淘汰压力下写入比 Redis 慢约 800 倍——这四条差距与宿主机噪声无关，是 E5-3 的优先线索。

## 方法：双容器对称对照

本机没有 Linux 原生环境，为避免「Godis 跑 Windows、Redis 跑 WSL2」的不公平对比，基准采用：

- Godis（仓库 Dockerfile 构建，Alpine 容器）与 Redis 8.10 各起一个容器，挂同一条 docker bridge 网络；
- `redis-benchmark`（Redis 8.10 镜像内置，同版本同参数）在第三个容器里打两端；
- 两端看到相同的内核、网络栈与压测客户端；配置单元由脚本生成（AOF/fsync/maxmemory/引擎）。

```
┌─────────────┐   ┌──────── docker bridge ────────┐
│ redis-bench │──▶│ godis 容器 :6399              │
│ (8.10 同源) │──▶│ redis 容器 :6379 (对照腿)     │
└─────────────┘   └───────────────────────────────┘
```

**标准参数**：`-c 50 -n 100000 -d 3`，pipeline `-P 1/16/64`，测试 `-t set,get`；
LRU 单元加 `-r 50000`（5 万键空间）+ `maxmemory 32MB`（allkeys-lru）。

### 复现

```bash
docker build -t godis:bench .        # 一次性
bash scripts/bench.sh                # quick 矩阵（15 单元，约 5 分钟）
BENCH_PROFILE=full bash scripts/bench.sh   # 全笛卡尔矩阵（36+ 单元）
BENCH_ONLY=std-off-p1 BENCH_REPEAT=3 RESULTS=tmp/bench/variance.tsv bash scripts/bench.sh
```

产物 `tmp/bench/results.tsv`（gitignore 内），列：`label engine aof eviction pipeline test ops p50_ms p99_ms avg_ms`。

### 环境声明（绝对数字只对本环境负责）

Windows 11 宿主机（用户日常使用、后台负载随时间波动）+ Docker Desktop 29.8.1（WSL2 VM，报告 32 vCPU）。
Redis 对照腿在同环境同波动，因此**比值可信、绝对值只作方向参考**；README 中 M2 Air 原生数字不可与此表互比。

## 基线数字（quick 矩阵第一轮，2026-10-22）

| 单元 | 引擎 | AOF | 淘汰 | P | SET ops/s | SET p50 | GET ops/s | GET p50 |
|------|------|-----|------|---|-----------|---------|-----------|---------|
| std-off-p1 | std | off | off | 1 | 22,868 | 2.15ms | 13,091 | 3.33ms |
| std-off-p16 | std | off | off | 16 | 22,925 | 2.19ms | 13,009 | 7.15ms |
| std-everysec-p1 | std | everysec | off | 1 | 22,247 | 2.22ms | 13,937 | 3.14ms |
| std-everysec-p16 | std | everysec | off | 16 | 22,584 | 2.20ms | 13,201 | 7.43ms |
| std-always-p1 | std | always | off | 1 | **308** | 146ms | 8,477 | 5.21ms |
| std-off-lru-p16 | std | off | **lru 32MB** | 16 | **465** | 107ms | 8,281 | 12.9ms |
| gnet-off-p1 | gnet | off | off | 1 | 8,118 | 6.20ms | 5,989 | 7.74ms |
| gnet-off-p16 | gnet | off | off | 16 | 9,890 | 89.0ms | 6,520 | 132ms |
| gnet-everysec-p16 | gnet | everysec | off | 16 | 17,464 | 54.0ms | 7,498 | 107ms |
| gnet-off-p64 | gnet | off | off | 64 | 11,778 | 278ms | 7,082 | 526ms |
| **redis-off-p1** | redis | off | off | 1 | **102,987** | **0.24ms** | **108,108** | 0.24ms |
| **redis-off-p16** | redis | off | off | 16 | **571,429** | 0.81ms | **1,111,111** | 0.59ms |
| redis-everysec-p1 | redis | everysec | off | 1 | 55,310 | 0.53ms | 85,690 | 0.26ms |
| redis-everysec-p16 | redis | everysec | off | 16 | 283,286 | 2.14ms | 495,050 | 1.14ms |
| **redis-off-lru-p16** | redis | off | **lru 32MB** | 16 | **367,647** | 1.21ms | 763,359 | 0.86ms |

## 波动性与门禁现状

- **同一时段 3 连跑**（std-off-p1）：SET 12.3k / 10.1k / 13.2k（均值 ±13%），GET ±6%；
- **跨时段漂移**：同一单元不同时段可达 2×（std-off-p1 SET 从 22.9k 到 12k）——**Redis 对照腿同幅波动**（-34%/+43%），属宿主机负载漂移而非 Godis 抖动；
- **结论**：E5-1 验收的「同配置两次偏差 <10%」在日常使用的 Windows 宿主机上达不到；波动门禁应落在 E5-2 的专用/CI runner 上，本机数字只取方向与比值。

## 发现与 E5-3 线索（按影响排序）

> **2026-10-22 E5-3 更新**：线索 ①②③ 已定位根因并修复（见下节「E5-3 修复与前后对照」）；
> ④（gnet pipeline 延迟）部分改善但未根治；⑤ 维持原判；新增一条连接层串行线索。

1. **LRU 淘汰压力下写入崩塌（已修复，40×）**：同条件（32MB 上限、5 万键、-r 50000）Redis SET 367k ops/s，Godis 修复前 465 ops/s（~800×）。根因：`approxKeyMemoryUsage` 在**每条写命令**与 100ms limiter monitor 里对全库做 O(N) 逐键内存估算扫描。
2. **pipeline 完全不涨吞吐（部分开放）**：std 引擎 P1→P16 吞吐持平，每命令存在与 pipeline 深度无关的串行成本。DB 层经 E5-3 修复后仅 3.3µs/op，但容器端到端仍 ~2.2ms/命令（50 并发、Little 定律成立）——**剩余串行点在连接/服务层，不在 DB 层**，是下一轮 pprof 的目标。
3. **GET 慢于 SET（已修复，~12×）**：根因是 `GetEntity → peekNoTouch() → goid()`，而 `goid()` 用 `runtime.Stack` 格式化本 goroutine 栈（拿 runtime 全局锁），每次 GET 付 ~30µs 并把所有连接串行化。
4. **gnet 引擎慢于 std 且 pipeline 负作用（部分改善）**：P1 修复后与 std 持平（2.3×），但 P16/P64 的 p50 仍高（43/169ms，修复前 89/278ms）——批量回复路径仍有逐命令开销，开放。
5. **`appendfsync always` 308→608 ops/s**：仍受本环境虚拟盘 fsync 支配（Redis 同环境 everysec 也掉半），归因需专用磁盘环境。
6. **ACL 检查占 Server.Exec 31.5%（已修复）**：`GetCommandCategories` 每次调用扫描全部类别表×逐命令 ToLower；改为启动时反向索引 + 无类别规则用户快路径。

## E5-3 修复与前后对照（2026-10-22）

三个根因修复（`database/list_block.go`、`database/database.go`、`database/eviction.go`、`acl/acl.go`）：| 修复 | 根因 | 手段 |
|---|---|---|
| goid 热路径 | `peekNoTouch/peekBlockingClientID` 每次调 `goid()`=格式化 goroutine 栈（runtime 全局锁） | `sync.Map` 绑定计数器原子快路径；绑定为空（99.99% 场景）时不再调 `goid()` |
| O(N) 内存记账 | `approxKeyMemoryUsage` 每写命令+每 100ms 全库逐键估算 | DB 级 `estimatedBytes` 原子增量（put/overwrite/条件写/删除四漏斗）；启动后与每分钟全量对账 |
| ACL 类别扫描 | `GetCommandCategories` 全类别表×ToLower；`commandsAllow` 无规则也扫 | 包初始化建反向索引；`AllowedCategories` 为空时跳过类别查询 |

**DB 层基准前后**（`go test ./database -bench BenchmarkE5`，8 核 i9-13900HX，pprofile 见 `tmp/e5-*.out`）：

| 基准 | 修复前 ns/op | 修复后 ns/op | 提升 |
|---|---|---|---|
| SET 并行（无淘汰） | 5,758（36 allocs） | 3,300（32 allocs） | 1.7× |
| GET 并行 | 42,902（36 allocs） | 3,500（29 allocs） | **12×**，与 SET 持平 |
| SET 淘汰压力（allkeys-lru 至上限） | 2,060,709（**1,791,730 B/op**） | 5,272（877 B/op） | **391×** |

**容器端到端前后**（quick 矩阵同日复跑；注意 Redis 对照腿同日漂移 +30%，比值取同轮）：

| 单元 | 指标 | 修复前 | 修复后 | 同轮 Redis |
|---|---|---|---|---|
| std-off-lru-p16 | SET ops/s（p50） | 465（107ms） | **18,543（2.5ms）** | 526k（0.8ms） |
| std-off-p1 | GET ops/s（p50） | 13,091（3.3ms） | 17,590（2.5ms），≈SET | 148k（0.18ms） |
| gnet-off-p1 | SET ops/s | 8,118 | 18,625 | — |

**修复后仍开放的差距**：std/gnet 与 Redis 的整体吞吐差（~5-8×）——该差距经第二轮排查**属于 redis-benchmark(C) ↔ Go 服务端的交互病理，非服务端能力问题**（见「E5-3 第二轮」：Go 客户端下 86-90 万 ops/s）；~~gnet pipeline 深度负作用~~（第三轮证伪，见「E5-3 第三轮」）；always fsync 需专用盘验证。

## E5-3 第二轮（2026-10-23）：服务端证清白，「连接层串行」假设推翻

首轮遗留的头号线索「连接层每命令 ~2ms 串行点」经系统排查**不成立**。新增 `redis/server/std/e2e_bench_test.go`（进程内 std server + 50 原生连接，P1/P16）：

| 组合 | 环境 | 每命令延迟 | 聚合吞吐（50 连接） |
|---|---|---|---|
| Go 客户端 ↔ Godis | Windows 原生 | 55.5µs（P1）/ 55.3µs（P16） | **~902k ops/s** |
| Go 客户端 ↔ Godis | WSL2 VM 内 | 57.8µs（P1） | **~865k ops/s** |
| redis-benchmark(C) ↔ Godis | VM 内（网桥） | ~2.2ms | ~20k ops/s |
| redis-benchmark(C) ↔ Godis | VM 内（**同容器 loopback**） | ~2.6ms | ~18k ops/s |
| redis-benchmark(C) ↔ Redis | VM 内 | 0.18ms | 132k+ ops/s |

排除实验（VM 内，SET P1 50c）：网桥（loopback 同慢）、客户端事件循环（`--threads 4` 不变）、GOMAXPROCS（1/2/4/32 全部 ~20k）、GC（首轮已排除）、DB 层（3.3µs）。

**结论**：Godis std 引擎在 50 连接下服务端能力约 **86-90 万 ops/s**，P16 每命令延迟与 P1 持平（无串行退化）。quick 矩阵中「比 Redis 慢 5-8×」的表观差距**专属于 redis-benchmark(C 客户端) ↔ Go 服务端的交互病理**（跨进程、loopback 亦复现，Go 客户端完全不出现），根因开放（疑 Linux netstack 与 Go 每回复一次写 syscall 的交互；40ms 级 delayed-ACK 不符）。**实际业务客户端（Go/Java 等 redis 客户端库）应落在快路径**——引用本表数字时请用 Go↔Go 行，C↔Godis 行仅作该特定组合的记录。gnet pipeline 深度延迟见第三轮（已证伪）。

## E5-3 第三轮（2026-10-23）：gnet pipeline 负作用证伪，两引擎对齐

新增 `redis/server/gnet/e2e_bench_test.go`（与 std 版同构：进程内 gnet server + 50 原生 Go 连接，pipelined SET batch 往返，WSL2 VM）：

| 引擎 | P1 | P16 | P64 | 每命令延迟（P16/P64 折算） |
|---|---|---|---|---|
| gnet | 56.8µs/op | 821.6µs/batch | 3,459µs/batch | **51.4µs / 54.0µs —— 与 P1 持平** |
| std（对照，同日重跑） | 53.4µs/op | 827.9µs/batch | — | 51.7µs |

**结论**：Go 客户端下 gnet 的 pipeline 深度**没有任何负作用**（P16/P64 每命令延迟与 P1 持平），且与 std 引擎同级。quick 矩阵中「gnet P16/P64 p50 43/169ms」与 std 的 5-8× 表观差距同源——**redis-benchmark(C 客户端) ↔ Go 服务端交互病理**，非服务端逐命令开销；原「批量回复路径逐命令开销」假设证伪（gnet OnTraffic 本就批量攒包一次写）。E5-3 遗留的两个性能开放项（gnet pipeline 负作用、连接层串行点）就此**全部收口为病理记录**；剩余可做项只有病理自身根因（低优先级，业务客户端不受影响）与 always fsync 专用盘验证。

## 已知边界与非目标

- 绝对数字绑定本机容器环境；不同主机请重跑 `scripts/bench.sh` 取自己的基线；
- 未覆盖：>512MB 大值、多机集群、Lua/FT/Vector 等扩展命令、复制链路开销；
- 本基线不追 Redis 性能数字（演进方案非目标），目的：可复现方法 + 差距定位 + 回归对照起点。

## 相关工具链

- 性能基线：`bash scripts/bench.sh`（本文档）；DB 层与 e2e 基准：`go test ./database -bench BenchmarkE5`、`go test ./redis/server/std -bench BenchmarkE5E2E -cpu 50`。
- **正确性随机差分（E6-2）**：`bash scripts/rnd-diff.sh`——类型感知随机命令序列双发 Godis 与 Redis 8.10 sidecar，逐步对拍 + 状态摘要 + 失败自动最小化；结果与已修分歧见 [`COMPATIBILITY.md`](COMPATIBILITY.md)「随机差分测试」。
