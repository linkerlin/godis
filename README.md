# Godis

![license](https://img.shields.io/github/license/linkerlin/godis)
[![Build Status](https://github.com/linkerlin/godis/actions/workflows/coverall.yml/badge.svg)](https://github.com/linkerlin/godis/actions?query=branch%3Amaster)
[![Coverage Status](https://coveralls.io/repos/github/linkerlin/godis/badge.svg?branch=master)](https://coveralls.io/github/linkerlin/godis?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/linkerlin/godis)](https://goreportcard.com/report/github.com/linkerlin/godis)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge-flat.svg)](https://github.com/avelino/awesome-go)

**中文** | **[English](./README_EN.md)**

---

## 按下回车的那一毫秒，到底发生了什么？

打开终端，连上 `6399` 端口，敲进一行字：

```text
SET greeting "hello"
```

你按下回车，屏幕立刻蹦出 `OK`。这中间到底发生了什么？

没有任何魔法。你的客户端把这行字打包成一串像 `*3\r\n$3\r\nSET\r\n$8\r\ngreeting\r\n$5\r\nhello\r\n` 这样的字节，顺着网线推过来。服务器把字节切开，用 FNV-32 算一个哈希值，在内存里成排的抽屉（65,536 个带读写锁的分片）里找到对应的那一个，拉开抽屉把字节放进去，顺手在旁边的纸条（AOF 文件）上记一笔，最后往网线里塞回五个字节：`+OK\r\n`。

就这么回事。

那我们为什么要费劲用 Go 语言从头写一个目标对齐 **Redis 8.10** 的服务器？

因为**知道一个东西叫什么名字，和真正搞清楚它怎么运作，完全是两码事**。你可以把“跳表”、“时间轮”、“RESP3 客户端缓存”、“HNSW 向量图”、“流消费组 PEL”、“Raft 共识”这些词背得滚瓜烂熟，在架构图上画满漂亮的方框；但直到你亲手用代码把它们一个个搭出来，并且让真正的 `redis-cli` 连上来跑过一万多条测试而不露怯——你才算真的搞清楚了里面每一颗齿轮是怎么咬合的。

**Godis** 就是那台把外壳拆掉、让你直接看见齿轮怎么转的机器。

---

## 别听我吹，先花 30 秒自己试一下

纸上谈兵没意思。把机器跑起来，亲手试几个命令，比读一百页文档都管用。

### 1. 启动它

你可以直接从 [GitHub Releases](https://github.com/linkerlin/godis/releases) 下载对应系统的二进制，或者在源码目录下直接编译：

```bash
go build -o godis ./
./godis
```

它默认监听 `0.0.0.0:6399`（故意避开你本机可能正在跑的 `6379`，免得打架）。如需指定配置文件，传个环境变量或者参数就行：

```bash
CONFIG=example.conf ./godis
# 或者
./godis -c example.conf -p 6399
```

也可以直接用 Docker（镜像以非 root 用户运行，`/data` 为工作/日志卷）：

```bash
docker build -t godis .
docker run --rm -p 6399:6399 -v godis-data:/data godis
# 或者
docker compose up -d
```

自定义配置可挂载到 `/etc/godis/godis.conf`：

```bash
docker run --rm -p 6399:6399 \
  -v "$PWD/my.conf:/etc/godis/godis.conf:ro" \
  -v godis-data:/data godis
```

### 2. 做五个 10 秒钟的小实验

现在打开 `redis-cli -p 6399`，试试下面这五件事。它们能让你一眼看穿 Redis 8.x 最有意思的几个机制：

#### 实验一：看“客户端缓存”怎么抓内鬼（RESP3 Push）
以前客户端想缓存数据，最头疼的是“别人改了数据库我怎么知道”。现在开两个终端窗口：

```text
# 窗口 A：切到 RESP3 协议，打开追踪，读一次 price
127.0.0.1:6399> HELLO 3
127.0.0.1:6399> CLIENT TRACKING ON
OK
127.0.0.1:6399> GET price
"100"

# 窗口 B：偷偷改掉 price
127.0.0.1:6399> SET price 99
OK
```
回头看**窗口 A**——你什么都不用敲，服务器已经顺着同一根连接主动推了一条失效通知过来：`>2\r\n+invalidate\r\n*1\r\n$5\r\nprice\r\n`。记下谁读过哪个键，写的时候挨个拍肩膀通知，客户端缓存就是这么运作的。

#### 实验二：给哈希表里的单个字段装上倒计时（Redis 8.x `HEXPIRE`）
以前想让用户资料里的“短信验证码” 5 秒后过期，你只能把整张用户 Hash 表删掉，或者另起炉灶存个 String。现在不用了：

```text
127.0.0.1:6399> HSET session:42 user "alice" otp "8848"
(integer) 2
127.0.0.1:6399> HEXPIRE session:42 5 FIELDS 1 otp
1) (integer) 1
127.0.0.1:6399> HTTL session:42 FIELDS 2 user otp
1) (integer) -1   # user 永久有效
2) (integer) 5    # otp 还在倒计时
```
等 5 秒再 `HGETALL session:42`，`otp` 自己消失了，`user` 稳稳当当还在那儿。

#### 实验三：先验指纹再动刀（Redis 8.4–8.8 `DIGEST` / `SET IFDEQ` / `INCREX`）
如果值很大（比如几兆的配置），做乐观锁 CAS 时难道要把整段文本再传一遍？算个 64 位 XXH3 摘要就行：

```text
127.0.0.1:6399> SET doc "v1-content"
OK
127.0.0.1:6399> DIGEST doc
"6f5b999a01b6f50b"
# 只有当摘要还是 6f5b999a01b6f50b 时才允许覆盖，否则原封不动：
127.0.0.1:6399> SET doc "v2-content" IFDEQ 6f5b999a01b6f50b
OK
# 给计数器加 10，但死死卡在上限 100（SATURATE 封顶并返回 [新值, 实际增量]）：
127.0.0.1:6399> SET quota 95
OK
127.0.0.1:6399> INCREX quota BYINT 10 LBOUND 0 UBOUND 100 SATURATE
1) "100"
2) "5"
```

#### 实验四：在内存图里找最像的向量，连 `float32` 都不用全存（`VADD` / `VSIM`）
所谓的“AI 向量数据库”，扒掉华丽的词藻，核心就是在高维空间里找距离最近的点。在 Godis 里，你可以直接用 HNSW 图，还能把 32 位浮点数压成 8 位整数（`Q8`）甚至 1 个比特（`BIN`）直接在图里算距离：

```text
127.0.0.1:6399> VADD items VALUES 3 1.0 0.5 0.2 itemA Q8 SETATTR '{"year":2026,"tag":"go"}'
(integer) 1
127.0.0.1:6399> VADD items VALUES 3 0.9 0.4 0.3 itemB Q8 SETATTR '{"year":2024,"tag":"c"}'
(integer) 1
127.0.0.1:6399> VSIM items VALUES 3 1.0 0.5 0.2 COUNT 2 FILTER '.year >= 2025' WITHSCORES
1) "itemA"
2) "1"
```

#### 实验五：顺手抢救超时任务与阻塞读时序（Redis 8.4–8.10 `XREADGROUP CLAIM` / `TS.READ`）
以前在 Stream 消费组里，如果某个消费者挂了，你得单独跑定时任务调 `XAUTOCLAIM`。现在读新消息时顺手就能把空闲超时的待处理消息（PEL）认领过来，或者用 `XNACK` 把干不了的活儿退回公共池：

```text
# 读取新消息的同时，自动认领空闲超过 5000ms 的旧消息：
127.0.0.1:6399> XREADGROUP GROUP g1 worker2 COUNT 10 CLAIM 5000 STREAMS orders >

# 像订阅流一样阻塞等待时序数据的新样本写进来（Redis 8.10 TS.READ BLOCK）：
127.0.0.1:6399> TS.READ BLOCK 5000 1 STREAMS cpu:load $
```

---

## 拆开引擎盖：里面装了哪些积木？

我们最反感用一堆故弄玄虚的词把人绕晕。下面这张表，说的不是“它有哪些宏大特性”，而是**每个模块到底在干什么活儿、代码写在哪儿**：

| 模块 | 抛开术语，它到底在干什么？ | 核心命令代表 | 去哪看代码？ |
| :--- | :--- | :--- | :--- |
| **并发字典与五大基石** | 用 FNV-32 把键分到分片里，对多键操作按固定顺序加读写锁防止死锁；上面跑着字符串、双向快速列表、哈希表、集合和跳表 | `SET` (`IFEQ`/`IFDEQ`), `MSETEX`, `LMOVEM`/`BLMOVEM`, `SUNIONCARD`/`SDIFFCARD`, `ZUNION` (`AGGREGATE COUNT`) | [`datastruct/dict/`](datastruct/dict/)<br>[`database/`](database/) |
| **RESP2 / RESP3 双形协议** | 同一个命令返回同一个结构体：如果你是旧客户端就序列化成数组 `*`，如果你发过 `HELLO 3` 就吐出原生的 Map `%`、Set `~`、Double `,` 和 Push `>` | `HELLO 3`, `CLIENT TRACKING`, `HGETALL`, `XREAD` | [`redis/protocol/`](redis/protocol/)<br>[`redis/parser/`](redis/parser/) |
| **Stream 流引擎** | 基数树/有序切片存消息，维护消费组与待确认列表（PEL）；支持阻塞等待、生产者幂等去重、直接认领空闲消息与退回重试 | `XADD` (`IDMP`/`IDMPAUTO`), `XCFGSET`, `XREADGROUP` (`CLAIM`/`MAXCOUNT`/`MAXSIZE`), `XNACK`, `XDELEX` | [`datastruct/stream/`](datastruct/stream/)<br>[`database/stream*.go`](database/stream.go) |
| **JSON 与 JSONPath** | 在内存里维护文档树，支持用 `[?(...)]` 过滤表达式（含比较、逻辑、算术、`in`/`nin`、`size`/`empty`、正则与聚合函数）精准掏出或修改子树 | `JSON.SET`, `JSON.GET`, `JSON.NUMINCRBY`, `JSON.ARRAPPEND` | [`datastruct/json/`](datastruct/json/)<br>[`database/json*.go`](database/json.go) |
| **Vector Set 向量集** | 内存 HNSW 多层导航图（可选 SQLite `sqlite-vec` 后端）。支持 `f32`、`Q8`（图内直接算 `int8` 距离，不反复反量化）与 `BIN`（1-bit 汉明距离） | `VADD`, `VSIM`, `VREM`, `VLINKS`, `VRANGE`, `VSETATTR`（兼容 `VS.*` 别名） | [`datastruct/vector/`](datastruct/vector/)<br>[`database/vector*.go`](database/vector.go) |
| **RediSearch 全文与混合检索** | 倒排索引 + BM25 打分 + 同义词组展开 + 游标聚合管道（可选 SQLite `FTS5` 后端）；支持文本与向量 KNN 的 RRF/线性混合检索 | `FT.CREATE`, `FT.SEARCH`, `FT.AGGREGATE` (`COLLECT`/`APPLY`), `FT.HYBRID`, `FT.ALIASLIST` | [`datastruct/redisearch/`](datastruct/redisearch/)<br>[`database/redisearch*.go`](database/redisearch.go) |
| **Time Series 时序数据** | 时间戳与浮点样本序列（含 `NaN` 样本感知），支持降采样规则、时间加权平均（`twa`）、单命令多聚合器与阻塞流式读取 | `TS.CREATE`, `TS.ADD`, `TS.MRANGE`, `TS.NRANGE`, `TS.QUERYLABELS`, `TS.READ` (`BLOCK`) | [`datastruct/timeseries/`](datastruct/timeseries/)<br>[`database/timeseries*.go`](database/timeseries.go) |
| **概率数据结构与 HLL** | 用极小且固定的内存换取可控误差的统计：判存在、去重计数、查频次、算分位数。其中 HyperLogLog 与 Redis `HYLL` 字节级互通 | `PFADD`/`PFCOUNT`, `BF.*`, `CF.*`, `CMS.*`, `TOPK.*`, `TDIGEST.*` | [`datastruct/hll/`](datastruct/hll/)<br>[`database/probabilistic.go`](database/probabilistic.go) |
| **Lua 脚本与 Functions** | 内嵌沙箱化的 `gopher-lua`（屏蔽 `os`/`io` 宿主逃逸），内置 `cjson`、`cmsgpack`、`bit` 库，支持持久化函数库 | `EVAL`, `EVAL_RO`, `FUNCTION LOAD`, `FCALL` | [`scripting/`](scripting/)<br>[`database/scripting.go`](database/scripting.go) |
| **持久化、主从与集群** | AOF / RDB / 混合持久化；PSYNC2 环形积压缓冲区复制与 `FAILOVER` 协调切换；16,384 槽 CRC16 路由 + Hashicorp Raft 拓扑共识 | `BGREWRITEAOF`, `REPLICAOF`, `FAILOVER`, `WAIT`/`WAITAOF`, `CLUSTER *` | [`aof/`](aof/)<br>[`cluster/`](cluster/)<br>[`database/replication*.go`](database/replication_master.go) |

完整命令清单可随时查阅 [`commands.md`](commands.md)。

---

## 第一原则是不骗自己：诚实边界

绝大多数项目的 README 只会告诉你它能做什么，然后悄悄把做不到的地方藏起来。但搞工程和搞科学一样，**第一原则是绝不欺骗自己——而你自己是最容易被糊弄过去的人**。

把 Godis 当成工具或研究样本之前，请你先看清下面这份诚实的账单（完整细节全公开在 [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md)，历史执行记录归档于 [`docs/history/`](docs/history/)）：

1. **我们是怎么验证“兼容”的？**
   - 不是靠肉眼看几个 `OK`。仓库里的 [`scripts/r4-1-cases.txt`](scripts/r4-1-cases.txt) 攒下了 **12,634 条**确定性命令用例，直接拿真实的 Redis 8.10 实例当裁判（Sidecar），比对两边的输出和错误文案。
   - 任何错误回复里如果夹带了用户输入的回车换行（`\r\n`），在协议出口都会被清洗成单行（对齐 Redis 8.10 安全修复），绝不让脏数据劈开 RESP 协议帧。

2. **哪些地方我们换了算法，但把账写在了明面上？**
   - **内存统计不装 `jemalloc`**：这是 Go 写的程序。当你执行 `INFO memory` 或 `MEMORY STATS` 时，`mem_allocator` 永远诚实地返回 `go`。我们读取操作系统真实的进程工作集（`used_memory_rss`），但绝不伪造一套 `jemalloc` 的 arena 数字去骗监控面板。
   - **`HOTKEYS` 与 `allkeys-lrm`**：Redis 8.6/8.10 的 `HOTKEYS` 内部用的是按槽概率结构，Godis 用的是命令耗时按键分摊 + 参数字节采样；`volatile-lrm`/`allkeys-lrm` 按命令声明的写键记录修改时间。
   - **`SUNIONCARD ... APPROX`**：Redis 8.10 允许用近似计数加速并集基数统计；Godis 接受 `APPROX` 参数并直接返回精确值——毕竟精确值永远落在任何合法的近似误差界之内。

3. **哪些地方我们宁可报错，也绝不假装互通？**
   - **RDB 模块二进制**：五大基础类型与 HyperLogLog 的 RDB 可以跟 Redis 互通。但 JSON、Vector、TimeSeries、概率结构、Array 以及 RediSearch 索引定义，在持久化时走的是 Godis 自家的 `GODIS1` 信封。如果你把官方 Redis 模块生成的二进制 `DUMP` 载荷喂给 Godis 的 `RESTORE`，Godis 会在清空任何现有数据**之前**直接报错拒绝，绝不假装看懂然后静默丢掉你的数据。
   - **`FUNCTION DUMP` 已改为官方互通（不是拒绝项）**：Godis 写出 Redis RDB 风格 `0xF5` + RDB string + version/CRC64 载荷；`FUNCTION RESTORE` 支持官方格式（含 LZF 压缩），并兼容旧 `GODISFN1`。已用 Redis 8.10 做双向 DUMP→RESTORE→`FCALL` 验证。

4. **哪些功能我们明确没做（或仅提供子集）？**
   - **事务（`MULTI`/`EXEC`）**：已对齐 Redis 标准行为——入队时语法错误会中止事务（`EXECABORT`），但运行期单条命令报错（比如对字符串做 `LPUSH`）不会回滚前面已经成功的命令，而是把错误放进结果数组继续往下跑。
   - **Array 类型（Redis 8.8 Preview）**：官方 18 条预览命令已全部接通（`ARSET`/`ARMSET`/`ARGET`/`ARMGET`/`ARGETRANGE`/`ARLEN`/`ARCOUNT`/`ARDEL`/`ARDELRANGE`/`ARINSERT`/`ARNEXT`/`ARSEEK`/`ARINFO`/`ARLASTITEMS`/`AROP`/`ARGREP`/`ARRING`/`ARSCAN`）与 `TYPE array`。内部是扁平槽模型，`ARINFO` 的 slice 统计为合成值、`FULL` 只返回基础字段。
   - **明确列为非目标的事项**：Compact Hash 内部模板编码（`HIMPORT` 会话已支持，落地为普通 Hash）、`BACKUP` 多分片 AOF、完整的 Redis 二进制 Gossip 总线（Godis 集群拓扑走 Raft）、以及 C Modules API。

事情就是这样。清楚知道边界在哪，用起来才踏实。

---

## 怎么逛这座代码库？

如果你想读源码，别从几千行的命令表开始硬啃。顺着**一条命令的一生**走一趟，半小时就能摸清全貌：

```text
godis/
├── main.go                     # 1. 从这儿进门：解析配置、挂好信号、启动网络监听
├── tcp/ & redis/server/        # 2. 接客：std（一连接一协程）或 gnet（事件驱动）
├── redis/parser/ & protocol/   # 3. 拆信与回信：把字节流切成命令行，把结果包成 RESP2/RESP3
├── database/
│   ├── server.go               # 4. 总调度台：鉴权、ACL、CLIENT PAUSE、慢日志、多库切换
│   ├── router.go               # 5. 查电话簿：440+ 条命令在哪执行、要锁哪些键，全在这张表里
│   ├── database.go             # 6. 单库引擎：按 prepare 声明锁住分片 -> 执行 -> 记 AOF -> 触发缓存失效
│   ├── string.go / list.go / hash.go / set.go / sortedset.go / stream*.go
│   ├── json*.go / vector*.go / timeseries*.go / redisearch*.go / array.go
│   ├── probabilistic.go / tdigest.go / hyperloglog.go / hotkeys_himport.go
│   └── replication_*.go / failover.go / caching.go / acl.go / transaction.go
├── datastruct/                 # 7. 真正的动手部位：并发字典、跳表、快速列表、HNSW 向量图、倒排索引
├── aof/                        # 8. 记账员：AOF 追加、后台重写、RDB 与 GODIS1 信封编解码
├── scripting/                  # 9. 脚本室：gopher-lua 虚拟机池与 Redis Functions
└── cluster/                    # 10. 分布式：16384 槽路由、MOVED/ASK、TCC 与 Raft 状态机
```

想开启 SQLite 驱动的全文与向量索引后端（`FTS5` + `sqlite-vec`）？编译时加个 tag 即可：

```bash
go build -tags sqlite_backend -o godis ./
```

更多架构与运维细节请参阅：
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — 进程模型、执行链路与组件分工
- [`docs/OPERATIONS.md`](docs/OPERATIONS.md) — 信号优雅退出、`/health` 探活、Prometheus `/metrics` 与 ACL 持久化
- [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md) — 与 Redis 8.10 的逐项对齐账本

---

## 跑得有多快？

下面是基于 `Go 1.23` 在 `macOS M2 Air` 上用标准 `redis-benchmark` 跑出的一组数字：

```text
PING_INLINE:    179,211.45 requests per second, p50=1.031 msec
PING_MBULK:     173,611.12 requests per second, p50=1.071 msec
SET:            158,478.61 requests per second, p50=1.535 msec
GET:            156,985.86 requests per second, p50=1.127 msec
INCR:           164,473.69 requests per second, p50=1.063 msec
LPUSH:          151,285.92 requests per second, p50=1.079 msec
RPUSH:          176,678.45 requests per second, p50=1.023 msec
LPOP:           177,619.89 requests per second, p50=1.039 msec
SADD:           159,489.64 requests per second, p50=1.047 msec
HSET:           175,131.36 requests per second, p50=1.031 msec
ZADD:           165,289.25 requests per second, p50=1.039 msec
ZPOPMIN:        185,528.77 requests per second, p50=0.999 msec
LRANGE_100:      46,511.62 requests per second, p50=4.063 msec
MSET (10 keys):  88,417.33 requests per second, p50=3.687 msec
```

不过，千万别轻信别人贴在 README 里的跑分——操作系统调度、Pipeline 深度、是否开启 AOF `appendfsync`、用的是 `std` 还是 `use-gnet yes`，都会让数字天差地别。最靠谱的办法永远是：在你的机器上拉起 `./godis`，然后自己敲一行 `redis-benchmark -p 6399`。实验结果不会骗人。

---

## 一起折腾（Contributing）

发现哪里不对劲？或者想亲手把某个命令、某块数据结构写得更漂亮？太好了！

提交 PR 前，只需要确认三件小事：
1. 跑一遍 `go test ./...` 和 `go vet ./...`，确保没把别人的积木碰倒。
2. 新写的命令或修好的 Bug，一定配上一条能复现它的测试（如果行为是确定性的，欢迎顺手往 [`scripts/r4-1-cases.txt`](scripts/r4-1-cases.txt) 里加一行对照样例）。
3. 如果某个特性只实现了子集或者用了近似算法，请在 [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md) 里老老实实写清楚。

## 相关衍生与许可证

- [godis-dashboard](https://github.com/linkerlin/godis-dashboard)：Godis 的可视化管理界面
- [技术博客专栏](https://www.cnblogs.com/Finley/category/1598973.html)：记录最初从零搭建各个模块时的思考草稿
- 本项目采用 [MIT 许可证](./LICENSE)。拿去拆、拿去改、拿去玩吧！
