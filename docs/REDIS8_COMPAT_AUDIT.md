# Godis Redis 8.x 兼容审计与修复记录

> 本文档记录对 Godis 的 Redis 8.x 全兼容差距分析及后续修复批次。
> RediSearch 专项对齐见 [`REDISEARCH_ALIGNMENT.md`](REDISEARCH_ALIGNMENT.md)。
> 2026-08 起持续更新。

---

## 1. 差距分析(起点)

对 4 个维度做静态审计(命令面 / 协议连接 / ACL 监控 / 数据语义),关键发现:

### 数据一致性
- **EXPIRE/PEXPIRE 非正 TTL 不删键**:时间轮对负 delay 静默丢弃,键永不主动过期
- **时间轮秒级截断**:`int(d.Seconds())` 导致 `PEXPIRE 1500` 提前 1 tick 调度、
  回调时 deadline 未到、任务被移除且不重排——**小数秒 TTL 永久无法主动过期**
- **主库过期不传播从库**:过期删除不写 AOF,复制积压(AOF 驱动)收不到 DEL
- **WATCH 版本误报**:执行前无条件 bump,失败的写命令也 invalidate 观察者
- **HLL 与 Redis 不兼容**:FNV-1a vs xxHash64、独立类型而非 string、无大范围修正

### 安全
- **集群模式完全绕过 ACL**:只查 requirepass
- **ACL 类别表残缺**:284 命令中 122 个无类别,`+@search` 等扩展类别对强制层无效;
  `" xrange"` 笔误导致永不被命中
- **SETBIT 无 offset 上限**:超限触发多 GB 分配 OOM
- **GETEX 注册为只读**:只读副本上可改 TTL 造成漂移

### 协议
- **CLIENT TRACKINGINFO 线上格式损坏**:嵌套数组被编码为 bulk string
- 子命令错误缺 `. Try X HELP.` 后缀;解析错误回复缺 `ERR Protocol error:` 前缀;
  gnet 解析失败直接断连不回错;RESP3 长错误不用 BlobError

### 其它
- 命令缺失:ZINTERCARD / SORT_RO / EVAL_RO / EVALSHA_RO
- 键空间通知只发 4 种事件(set/del/expire/expired),命令级事件全缺
- COMMAND INFO/DOCS 为 6.x 格式(缺 acl_categories/key_specs/subcommands)
- Stream XRANGE 全表扫描+冒泡、XTRIM O(n²)
- RESET 不清认证/REPLY 模式;ACL LOG 缺 entry-id/client-info

---

## 2. 修复批次(按时间)

| # | 提交 | 内容 | 测试 |
|---|---|---|---|
| 1 | `4ddf5b4` | 过期三 bug(非正 TTL / 时间轮 Ceil / 主从传播)+ WATCH 后置 bump | 4 |
| 2 | `637c20b` | 集群 ACL 接入 + 类别表补全(77+ 命令 + 扩展类别前缀回退 + 笔误) | 3 |
| 3 | `0523c34` | TRACKINGINFO 嵌套修复 + 子命令错误后缀 + 协议错误前缀 + RESP3 BlobError | 5 |
| 4 | `32be7a8` | ZINTERCARD / SORT_RO / EVAL_RO / EVALSHA_RO(脚本内写命令拒绝) | 3 |
| 5 | `cfce872` | GETEX 写标志 / SETBIT 2^32-1 上限 / RESET 清认证+REPLY / 命令级键空间事件 | 3 |
| 6 | `2157ad7` | notify-keyspace-events 校验 / requirepass 同步 ACL default | 3 |
| 7 | `705c6e1` | 键空间事件补完(LPOP/RPOP/SREM/ZREM/HINCRBY…)/ ACL LOG 增强 / COMMAND DOCS 元数据 | 3 |
| 8 | `fde7a92` | LSET/LTRIM/SETRANGE 事件收尾 | 0 |
| 9 | `2e18edc` | **HLL 重构**:string 存储 + xxHash64 + Redis dense 编码 + 全修正 | 5 |
| 10 | `5ce2624` | **Stream 性能** O(n²)→O(log n)(ordered slice + 二分)+ ACL SETUSER 补齐(>/# 追加、</! 删除、resetpass、reset)+ Authenticate 修 Enabled | 9 |
| 11 | `8cf364c` | DEBUG stub 收口:SET-ACTIVE-EXPIRE 真语义 + DIGEST 真 SHA1 | 2 |

累计 ~40 个新测试。

---

## 3. 关键技术决策

- **HLL**:Redis dense 编码(`HYLL` 头 + 16384×6-bit 位图)+ xxHash64(seed 0),
  string 存储 → GET 可读、RDB/AOF 走 string、与 Redis 字节级互迁移。
- **Stream**:双结构(dict 查找 + ordered slice 顺序),新 ID 恒最大→尾部追加,
  范围查询二分、裁剪批量截断。
- **ACL 类别**:静态表 + 命令名前缀回退(`ft.*`→@search 等),强制层与 ACL CAT 同源。
- **键空间事件**:命令确认修改后发事件(HSET/LPUSH/SADD/ZADD/XADD/LPOP/RPOP/
  SREM/ZREM/HINCRBY/LSET/LTRIM/SETRANGE/APPEND/INCRBY + 底层 set/del/expire)。

---

## 4. 剩余项

| 项 | 状态 | 说明 |
|---|---|---|
| FAILOVER 真实切换 | 设计中 | 见 [`FAILOVER_DESIGN.md`](FAILOVER_DESIGN.md) |
| DEBUG RELOAD / CHANGE-REPL-ID | stub | RELOAD 需 RDB 往返(风险);CHANGE-REPL-ID 复制内部 |
| DEBUG JMAP / FLUSHALL | stub | JMAP JVM-only;FLUSHALL 是有意的防破坏偏离 |
| 稀疏 HLL 读取 | 拒绝 | godis 恒 dense;稀疏 blob 报错(与 Redis 拒绝损坏 HLL 一致) |
