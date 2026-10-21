# Godis 运维说明

## 配置加载

优先级（高 → 低）：

1. 命令行 `-config` / `-c`
2. 环境变量 `CONFIG`
3. 当前目录 `redis.conf`
4. 内置默认值（`127.0.0.1:6379`；`main.go` 中 standalone 默认 `6399`）

常用命令行覆盖：`-bind`、`-port`。

## 信号与优雅关闭

| 模式 | 信号 | 行为 |
|------|------|------|
| **std**（默认 `use-gnet no`） | `SIGINT` / `SIGTERM` / `SIGHUP` / `SIGQUIT` | 停止 accept → `Handler.Close()` 等待在途请求 → 退出 |
| **gnet**（`use-gnet yes`） | 同上 | 停止 accept → `GnetServer.Close()` 等待在途请求 → 退出 |

建议生产环境使用 `systemd` / 容器编排发送 **SIGTERM**，留 10s 以上 grace period。

## 探活

| 方式 | 说明 |
|------|------|
| **Redis `PING`** | 标准探活，兼容 redis-cli / Sentinel |
| **HTTP `/health`** | 需配置 `metrics-addr`；返回 `200 OK` + 正文 `OK` |
| **maxclients** | 超限返回 `-ERR max number of clients reached`；指标 `godis_rejected_connections_total` |
| **HTTP `/metrics`** | Prometheus 指标，见 `lib/metrics` |

```conf
metrics-addr 127.0.0.1:9090
```

```bash
curl -sf http://127.0.0.1:9090/health
curl -sf http://127.0.0.1:9090/metrics | head
```

## ACL 持久化

```conf
aclfile users.acl
```

- 启动时若文件存在则自动加载
- `ACL SAVE` 写入文件；`ACL LOAD` 热加载

## SQLite 搜索后端

```conf
search-backend sqlite
vector-backend sqlite
search-sqlite-path /var/lib/godis/search_index.db
sqlite-mmap-size 268435456
```

构建：`go build -tags sqlite_backend -o godis ./`

## CI 冒烟

```bash
go build -o godis ./
CONFIG=scripts/ci-smoke.conf ./godis &
bash scripts/smoke-test.sh
```

## Docker / Compose

镜像采用多阶段构建：`golang:1.25-alpine` 编译静态二进制，运行层为 `alpine:3.21`，以 UID 10001 的非 root 用户运行。

```bash
docker build -t godis .
docker run --rm -p 6399:6399 -v godis-data:/data godis
```

或使用 `docker-compose.yml`：

```bash
docker compose up -d --build
docker compose logs -f godis
```

- `CONFIG` 默认指向镜像内 `/etc/godis/godis.conf`（由仓库 `example.conf` 复制）
- `/data` 是工作目录/日志卷；如启用 AOF/RDB，请把配置中的 `dir` 指向该卷
- 自定义配置：`-v "$PWD/my.conf:/etc/godis/godis.conf:ro"`
- 需要 Prometheus 指标时，在配置中设置 `metrics-addr 0.0.0.0:9090` 并额外暴露 `9090` 端口
