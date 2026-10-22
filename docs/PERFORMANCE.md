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

1. **LRU 淘汰压力下写入崩塌（最高优先）**：同条件（32MB 上限、5 万键、-r 50000）Redis SET 367k ops/s，Godis 465 ops/s——约 **800×** 差距，p50 107ms。嫌疑：写路径逐命令内存估算 / 采样淘汰的锁与复杂度（E2 治理过 `EvictionManager`，但压力场景仍崩）。
2. **pipeline 完全不涨吞吐**：std 引擎 P1→P16 吞吐持平（~22.5k），Redis 同环境 103k→571k。50 并发 / 22.5k ≈ 每命令 ~2.2ms 服务端成本，与 pipeline 深度无关——像是逐命令串行点（锁/通道/系统调用）。**GC 已排除**：`GOGC=800` 探针吞吐不变（18.0k→18.4k），小值场景几乎无 GC 周期。
3. **GET 慢于 SET 且尾延迟差**：std 引擎 GET 13k vs SET 22.5k，P16 时 GET p99 高达 120-130ms（SET 4-5ms）。读路径比写路径多做的事（TRACKING 记账、惰性过期检查、ttlMap 读锁等）需要 pprof 定位。
4. **gnet 引擎慢于 std 且 pipeline 负作用**：P1 就差 2.8×；P16 起 p50 爆到 89-132ms、P64 到 278-526ms——pipeline 越深延迟越差，怀疑批量回复路径（AsyncWrite 合并/唤醒）有逐命令开销。生产默认仍应选 std，gnet 待 E5-3 结论。
5. **`appendfsync always` 308 ops/s**：本环境单次 fsync ~150ms（虚拟盘），Redis 同环境 everysec 也从 103k 掉到 55k；always 的差距归因需专用磁盘环境，此处只记录现象。

## 已知边界与非目标

- 绝对数字绑定本机容器环境；不同主机请重跑 `scripts/bench.sh` 取自己的基线；
- 未覆盖：>512MB 大值、多机集群、Lua/FT/Vector 等扩展命令、复制链路开销；
- 本基线不追 Redis 性能数字（演进方案非目标），目的：可复现方法 + 差距定位 + 回归对照起点。
