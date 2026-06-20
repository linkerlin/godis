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
| **P2** | 稳定性与质量门禁 | ⬜ 待开始 |
| **P3** | 部署与发布 | ⬜ 待开始 |

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

- [ ] SLOWLOG 与 ACL LOG 最大长度可配置且热更新
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

- [ ] 审计 `prepare=nil` 命令（rediSearch、vector、geo、timeseries、probabilistic）
- [ ] 统一走 `lib/validate`

### P2-2 测试与覆盖率

- [ ] `database` 包覆盖率 33% → **50%+**（优先 `server.go`、`stream*.go`、`caching.go`）
- [ ] CI 全量 `go test -race ./...`（ nightly 或 PR 可选）
- [ ] 核心命令兼容性对照测试（Godis vs Redis 8 子集）

### P2-3 静态分析门禁

- [x] CI 恢复 `go vet ./...`
- [ ] `golangci-lint` 配置 + CI

---

## P3 — 部署与发布

### P3-1 Release 工作流

- [x] tag 触发 GitHub Actions：linux/darwin/windows × amd64/arm64
- [ ] `CHANGELOG.md` 随版本更新

### P3-2 配置与示例

- [ ] `example.conf` 补充 `maxmemory` 说明（metrics/aclfile 已补充）
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

**最后更新：** 2026-06-20  
**维护：** 每完成一项将 `[ ]` 改为 `[x]` 并注明 PR/commit。
