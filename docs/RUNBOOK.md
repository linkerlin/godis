# Godis 运维 Runbook（E8-3）

> 三份操作手册：升级与回滚、官方 Redis 迁移边界、备份恢复。只写已验证的行为；
> 兼容边界细节见 [`COMPATIBILITY.md`](COMPATIBILITY.md)，信号/探活/Prometheus 见 [`OPERATIONS.md`](OPERATIONS.md)。

## 一、升级与回滚（Godis → Godis）

**推荐：复制切换（近乎不停机）**

1. 起新版本实例（同配置文件，`port`/`dir` 指向新目录），确认 `PING`、`INFO server` 正常。
2. 新实例执行 `REPLICAOF <老主> <port>`，等 `INFO replication` 中 `slave_repl_offset` 追平（`master_repl_offset` 差值为 0）。
3. `WAIT 1 <timeout>` 确认落盘/复制确认数达标后，在老主上执行 `FAILOVER TO <新主host> <port> [TIMEOUT ms]`——真实协调切换：老主阻止新写入、向新实例注入切换指令、新实例自提升为主、老主降级为副本（语义详见 [`FAILOVER_DESIGN.md`](FAILOVER_DESIGN.md)）。
4. 观察新主 `INFO replication`；异常时 `FAILOVER ABORT` 只能在切换**发起前**中止——发起后回退方向是把客户端切回老实例并 `REPLICAOF NO ONE`。

**回滚**：旧版本二进制 + 升级前保留的数据文件可直接启动。注意：若新版本已接受写入且 AOF 中含旧版本不认识的命令/格式（如新增命令、opaque 变体），旧版本回放会报错——**升级前先 `BGREWRITEAOF` 并备份重写后的文件**，回滚以该备份为基线，之后的增量按业务补偿。

## 二、官方 Redis → Godis 迁移边界

| 数据 | 可迁移 | 方式 |
|---|---|---|
| String/List/Hash/Set/ZSet/Bitmap、HLL | ✅ 二进制互通 | 官方 `BGSAVE` 产出 RDB → 放入 Godis `dir` 并配置 `dbfilename` 重启加载；或 `DUMP`/`RESTORE` 逐键搬运（TTL 随载荷） |
| Lua Functions | ✅ 双向互通 | 官方 `FUNCTION DUMP` 载荷（`0xF5`+LZF）直接 `FUNCTION RESTORE`（Redis 8.10 双向实测） |
| 脚本缓存（EVALSHA） | ❌ 不随数据走 | 迁移后首次 `EVAL` 重建，或业务侧 SCRIPT LOAD |
| JSON / Vector / TimeSeries / 概率结构 / Array / FT 索引（官方为模块） | ❌ 明确报错 | Godis `RESTORE` 对官方模块 type 二进制**明确 ERR，不伪造互通**；这些类型需按命令语义重建（如 JSON 文档重新 `JSON.SET`、索引重建 `FT.CREATE` + 重写或 `ON HASH` 初始扫描） |
| 主从直连（官方 master ← Godis replica） | 未验证 | 不做承诺；需要时先在测试环境验证 PSYNC 兼容性 |

**迁移前自检**：源库 `INFO keyspace` + `DEBUG DIGEST` 记录基线；迁移后同键集对拍（确定性对拍可参考 `scripts/redis-sidecar-diff.sh` 的方法）。

## 三、备份与恢复

**单机**
- AOF（`appendonly yes`，默认 `appendfsync everysec`；逐条落盘用 `always`）+ 混合持久化（RDB preamble，重写时自动生成）。
- 快照：`BGSAVE`（后台）或 `SAVE`（阻塞）；`save <sec> <changes>` 自动快照已接通。
- 恢复：备份文件放回 `dir`，启动即加载（优先 AOF；`aof-use-rdb-preamble` 混合格式自动识别）。加载前用副本操作，勿直接在唯一备份上启动。

**复制拓扑**
- 副本即热备：故障时 `REPLICAOF NO ONE` 提升任一追平的副本（或用上面 FAILOVER 流程做计划内切换）。
- 慢副本落后超过 backlog 会被主库断开触发重同步（不会静默截断数据）。

**集群（Raft）**
- 每节点各自的 AOF/RDB + Raft 存储（快照/日志在 Raft 数据目录，见 `example.conf` 的 `dir` 说明）。
- 整簇恢复：停写 → 各节点留一份 Raft 数据与 AOF → 按原拓扑拉起；成员变更（`CLUSTER MEET`/`REPLICATE`/`FORGET`）后做一次 `CLUSTER SLOTS` 与 `GETKEYSINSLOT` 抽查对账。
- 定期演练：按本节在测试环境完整走一遍，`DEBUG DIGEST` 校验数据一致。

## 四、集群运维（E7：槽位与成员）

**扩缩容/槽位重分配（已验证路径）**：加入新节点（`cluster-seed` 指向任一节点，或 `CLUSTER MEET ip port raft-port`）→ 在现有节点 `CLUSTER DELSLOTSRANGE a b` 释放区间 → 新节点 `CLUSTER ADDSLOTSRANGE a b` 认领 → 轮询**每个节点**的 `CLUSTER SLOTS` 直到视图一致（Raft 应用有秒级延迟）→ 客户端按 `CLUSTER SLOTS`/`-MOVED` 重定向自然迁移读写。该路径由 `go test ./e7cluster` 与 `scripts/cluster-interop.sh`（真实 `redis-cli -c`）双向验证。注意：DELSLOT 后区间内已存在的键仍留在原节点（无自动搬迁）——重分配应在新键写入前完成，或配合应用侧双写。

**数据在线搬迁（reshard，已验证路径，2026-10-23；多键批修复 2026-10-24）**：源节点 `CLUSTER SETSLOT <slot> MIGRATING <目标id>` → 目标节点 `CLUSTER SETSLOT <slot> IMPORTING <源id>` → 循环 `CLUSTER GETKEYSINSLOT <slot> <batch>` + 在源执行 `MIGRATE <目标host> <目标port> "" 0 <timeout> KEYS k1 k2 …`（MIGRATE 内部在**每条** RESTORE 前自动重发 `ASKING` 放行目标端 IMPORTING 槽——标志一次性，只发一次会让多键批从第 2 键起被 `-MOVED` 弹回，2026-10-24 修复并由 `TestE7ClusterAskRetryContract` 钉死）→ 源与目标各执行 `CLUSTER SETSLOT <slot> NODE <目标id>` 收尾。源端在该槽仍有键残留时执行 `SETSLOT NODE` 会被拒绝（防孤儿键，文案与 Redis 8.10 一致）。全链路由 `go test ./e7cluster -run TestE7ClusterReshardKeys`（真实双子进程集群）钉死；`redis-cli --cluster reshard` 工具本体未逐项认证，但其协议序列即上述命令（依赖的 `CLUSTER MYID`/`GETKEYSINSLOT`/`MIGRATE` 均已支持）。

**成员管理（已验证）**：`CLUSTER REPLICATE <node>` 使本节点成为指定主节点的从库（写 FSM）；`CLUSTER FORGET <node>` 安全移除节点。`CLUSTER FAILOVER`/`RESET`/`SAVECONFIG`/`SET-CONFIG-EPOCH` 显式报不支持。

**韧性事实（有测试背书）**：主库宕机后从库保持 replica 角色并退避重连（`e6chaos` 主宕场景）；复制中断重启后自动重同步收敛（`e6chaos` 中断场景）；**分区形状的主库失联**（SIGSTOP 冻结、无 RST/FIN）从库保持 replica、恢复后增量追平且分区前写全在（`e6chaos` 分区场景，Linux）；计划内切换走 `FAILOVER TO`（见第一节）。**未演练**：Raft 多数派双分区仲裁（双节点互看不见的真分区拓扑）。

## 附：验证工具索引

| 工具 | 用途 |
|---|---|
| `go test ./e7cluster` | 两节点集群互操作契约（拓扑/路由/MOVED/CROSSSLOT/hash-tag；e7retry：迁移窗口 ASK/ASKING 重试契约 + 陈旧拓扑 MOVED 收敛；e7goredis：go-redis v9 真实 ClusterClient 认证——拓扑发现/路由/实时 reshard 重试/Sharded PubSub） |
| `bash scripts/cluster-interop.sh` | 真实 `redis-cli -c` 跟随 MOVED 冒烟（需 docker） |
| `go test ./e6chaos` | 崩溃一致性/复制韧性故障注入 |
| `go test ./e6soak`（`SOAK_CYCLES` 可调） | 长稳负载循环 + 内存/goroutine/FD 基线 |
| `bash scripts/rnd-diff.sh` | 与 Redis 8.10 随机差分对拍 |
