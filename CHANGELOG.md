# Changelog

本文件遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/) 格式。

## [Unreleased]

### Added

- RESP 解析器 Fuzz 测试（`FuzzParseOne` / `FuzzParseBytes` / `FuzzParseV2`）及 fuzz corpus
- 解析器 bulk/array 长度上限（512 MiB bulk、1M 数组元素），防止恶意帧 OOM/挂起
- `lib/validate` 单元测试
- 集群 `doMigrateSlot` 实现与测试
- AOF/RDB 后台 rewrite 互斥锁（`ErrRewriteInProgress`）
- ACL 启动集成、统一 AUTH/HELLO、命令级 ACL 权限检查
- std 服务器优雅关闭（in-flight 等待 + 超时）
- `INFO` 复制/RDB 字段与 `CLIENT PAUSE/UNPAUSE` 加锁
- `docs/COMPATIBILITY.md` 兼容性矩阵
- CI：`go vet` 与选定包的 `go test -race`
- gnet 服务器优雅关闭（`closing` + in-flight 等待）
- `commands.md` 与代码对齐；`database/caching.go` 单元测试

### Fixed

- `parse0` panic 时未关闭 channel 导致 `ParseOne` 可能挂起
- `parseArray` 内层 bulk 未校验长度导致超大 `make` 分配
- `go vet`：`database/stream.go` 未键控 `StreamID` 字面量；`tcp/server.go` signal channel 缓冲
- ACL `+@all` 被误解析为 category 的 bug

### Changed

- 根目录历史过程报告移至 `docs/history/`

### Security

- 修复 ACL 未启动、`execAuth` 在无 ACL 时直接返回 OK 的认证绕过

---

## 更早版本

详见 `docs/history/` 中的 2026-04/2026-06 审阅与兼容性报告，以及 [`改进意见_2026-06.md`](改进意见_2026-06.md) 实施索引。
