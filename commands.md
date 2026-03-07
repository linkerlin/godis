# Godis 支持命令列表

本文档列出 Godis 支持的所有 Redis 命令。

> **注意**: Godis 目标兼容 **Redis 8.x** 协议，支持 RESP3 和多项 Redis 8.x 新特性。

---

## 连接命令

| 命令 | 描述 | RESP3 支持 |
|------|------|------------|
| AUTH | 认证 | ✅ |
| PING | 测试连接 | ✅ |
| ECHO | 回显消息 | ✅ |
| SELECT | 选择数据库 | ✅ |
| QUIT | 关闭连接 | ✅ |
| HELLO | 协议版本协商 | ✅ |
| SWAPDB | 交换数据库 | ✅ |

---

## 服务端命令

| 命令 | 描述 |
|------|------|
| COMMAND | 命令信息 |
| DBSIZE | 数据库键数量 |
| FLUSHDB | 清空当前数据库 |
| FLUSHALL | 清空所有数据库 |
| INFO | 服务器信息 |
| CONFIG GET | 获取配置 |
| CONFIG SET | 设置配置 |
| TIME | 服务器时间 |

---

## 键管理命令

| 命令 | 描述 |
|------|------|
| DEL | 删除键 |
| EXISTS | 检查键是否存在 |
| EXPIRE | 设置过期时间（秒） |
| EXPIREAT | 设置过期时间戳 |
| PEXPIRE | 设置过期时间（毫秒） |
| PEXPIREAT | 设置过期时间戳（毫秒） |
| TTL | 获取剩余生存时间 |
| PTTL | 获取剩余生存时间（毫秒） |
| PERSIST | 移除过期时间 |
| TYPE | 获取键类型 |
| KEYS | 查找匹配模式的键 |
| SCAN | 迭代键空间 |
| RENAME | 重命名键 |
| RENAMENX | 安全重命名 |
| DUMP | 序列化键值 |
| RESTORE | 反序列化键值 |
| MOVE | 移动键到另一数据库 |
| RANDOMKEY | 随机返回键 |
| TOUCH | 更新键访问时间 |
| UNLINK | 异步删除键 |
| WAIT | 等待副本同步 |

---

## 字符串命令

| 命令 | 描述 |
|------|------|
| SET | 设置字符串值 |
| GET | 获取字符串值 |
| GETSET | 获取并设置值 |
| MSET | 设置多个键值 |
| MGET | 获取多个值 |
| SETNX | 仅在键不存在时设置 |
| SETEX | 设置值并指定过期时间 |
| PSETEX | 设置值并指定过期时间（毫秒） |
| MSETNX | 原子性设置多个键值 |
| APPEND | 追加字符串 |
| STRLEN | 获取字符串长度 |
| INCR | 数值加1 |
| INCRBY | 数值加N |
| DECR | 数值减1 |
| DECRBY | 数值减N |
| INCRBYFLOAT | 浮点数加法 |
| GETRANGE | 获取子字符串 |
| SETRANGE | 设置子字符串 |
| GETBIT | 获取位值 |
| SETBIT | 设置位值 |
| BITCOUNT | 统计1的位数 |
| BITPOS | 查找第一个位 |
| BITOP | 位运算 |

---

## 列表命令

| 命令 | 描述 |
|------|------|
| LPUSH | 从左侧插入元素 |
| RPUSH | 从右侧插入元素 |
| LPOP | 从左侧弹出元素 |
| RPOP | 从右侧弹出元素 |
| LPUSHX | 仅当列表存在时左侧插入 |
| RPUSHX | 仅当列表存在时右侧插入 |
| LINDEX | 获取指定索引元素 |
| LINSERT | 在指定元素前/后插入 |
| LLEN | 获取列表长度 |
| LRANGE | 获取指定范围元素 |
| LREM | 移除指定元素 |
| LSET | 设置指定索引值 |
| LTRIM | 裁剪列表 |
| RPOPLPUSH | 弹出并推入 |
| BLPOP | 阻塞式左弹出 |
| BRPOP | 阻塞式右弹出 |
| BRPOPLPUSH | 阻塞式弹出并推入 |
| LMOVE | 列表间移动元素 |
| BLMOVE | 阻塞式列表移动 |

---

## 哈希命令

| 命令 | 描述 |
|------|------|
| HSET | 设置字段值 |
| HGET | 获取字段值 |
| HMSET | 设置多个字段 |
| HMGET | 获取多个字段 |
| HGETALL | 获取所有字段和值 |
| HDEL | 删除字段 |
| HLEN | 获取字段数量 |
| HEXISTS | 检查字段是否存在 |
| HKEYS | 获取所有字段名 |
| HVALS | 获取所有字段值 |
| HINCRBY | 整数字段递增 |
| HINCRBYFLOAT | 浮点字段递增 |
| HSETNX | 仅当字段不存在时设置 |
| HSTRLEN | 获取字段值长度 |
| HRANDFIELD | 随机获取字段 |
| HSCAN | 迭代哈希字段 |

### Hash Field 过期 (Redis 8.x)

| 命令 | 描述 |
|------|------|
| HEXPIRE | 设置字段过期时间 |
| HPEXPIRE | 设置字段过期时间（毫秒） |
| HEXPIREAT | 设置字段过期时间戳 |
| HPEXPIREAT | 设置字段过期时间戳（毫秒） |
| HTTL | 获取字段剩余生存时间 |
| HPTTL | 获取字段剩余生存时间（毫秒） |
| HPERSIST | 移除字段过期时间 |

---

## 集合命令

| 命令 | 描述 |
|------|------|
| SADD | 添加元素 |
| SREM | 移除元素 |
| SISMEMBER | 检查元素是否存在 |
| SMISMEMBER | 批量检查元素 |
| SCARD | 获取元素数量 |
| SMEMBERS | 获取所有元素 |
| SMOVE | 移动元素到另一集合 |
| SPOP | 随机移除指定数量元素 |
| SRANDMEMBER | 随机获取指定数量元素 |
| SINTER | 返回交集 |
| SINTERSTORE | 存储交集结果 |
| SUNION | 返回并集 |
| SUNIONSTORE | 存储并集结果 |
| SDIFF | 返回差集 |
| SDIFFSTORE | 存储差集结果 |
| SSCAN | 迭代集合元素 |

---

## 有序集合命令

| 命令 | 描述 |
|------|------|
| ZADD | 添加成员 |
| ZSCORE | 获取成员分数 |
| ZMscore | 批量获取成员分数 |
| ZINCRBY | 增加成员分数 |
| ZCARD | 获取成员数量 |
| ZCOUNT | 统计指定分数范围成员数 |
| ZRANGE | 按排名范围返回成员 |
| ZREVRANGE | 按排名倒序返回成员 |
| ZRANGEBYSCORE | 按分数范围返回成员 |
| ZREVRANGEBYSCORE | 按分数范围倒序返回 |
| ZRANK | 获取成员排名 |
| ZREVRANK | 获取成员倒序排名 |
| ZREM | 移除成员 |
| ZREMRANGEBYRANK | 按排名范围移除 |
| ZREMRANGEBYSCORE | 按分数范围移除 |
| ZLEXCOUNT | 按字典序统计 |
| ZRANGEBYLEX | 按字典序返回 |
| ZREMRANGEBYLEX | 按字典序移除 |
| ZPOPMIN | 弹出最小分数成员 |
| ZPOPMAX | 弹出最大分数成员 |
| BZPOPMIN | 阻塞式弹出最小 |
| BZPOPMAX | 阻塞式弹出最大 |
| ZUNIONSTORE | 并集计算并存储 |
| ZINTERSTORE | 交集计算并存储 |
| ZDIFFSTORE | 差集计算并存储 |
| ZUNION | 返回并集 |
| ZINTER | 返回交集 |
| ZDIFF | 返回差集 |
| ZRANGESTORE | 存储排名范围 |
| ZSCAN | 迭代有序集合 |
| ZMSCORE | 批量获取分数 |

---

## Bitmap 命令

| 命令 | 描述 |
|------|------|
| SETBIT | 设置位 |
| GETBIT | 获取位 |
| BITCOUNT | 统计位数 |
| BITPOS | 定位位 |
| BITOP | 位运算 |
| BITFIELD | 操作位域 |

---

## Stream 命令

| 命令 | 描述 |
|------|------|
| XADD | 添加条目 |
| XREAD | 读取条目 |
| XREADGROUP | 消费者组读取 |
| XGROUP | 管理消费者组 |
| XACK | 确认消息 |
| XCLAIM | 认领待处理消息 |
| XPENDING | 查看待处理消息 |
| XDEL | 删除条目 |
| XTRIM | 裁剪流 |
| XLEN | 获取条目数量 |
| XRANGE | 范围查询 |
| XREVRANGE | 倒序范围查询 |
| XINFO | 流信息 |

---

## JSON 命令 (Redis 8.x)

| 命令 | 描述 |
|------|------|
| JSON.SET | 设置 JSON 值 |
| JSON.GET | 获取 JSON 值 |
| JSON.DEL | 删除 JSON 值 |
| JSON.MGET | 批量获取 |
| JSON.TYPE | 获取 JSON 类型 |
| JSON.NUMINCRBY | 数字递增 |
| JSON.NUMMULTBY | 数字相乘 |
| JSON.STRAPPEND | 字符串追加 |
| JSON.STRLEN | 字符串长度 |
| JSON.ARRAPPEND | 数组追加 |
| JSON.ARRINDEX | 数组查找 |
| JSON.ARRINSERT | 数组插入 |
| JSON.ARRLEN | 数组长度 |
| JSON.ARRPOP | 数组弹出 |
| JSON.ARRTRIM | 数组裁剪 |
| JSON.OBJKEYS | 对象键列表 |
| JSON.OBJLEN | 对象字段数 |
| JSON.DEBUG | 调试信息 |
| JSON.FORGET | 删除键 |
| JSON.RESP | 返回 RESP 格式 |

---

## Vector Set 命令 (Redis 8.x)

用于 AI/向量搜索的向量集合。

| 命令 | 描述 |
|------|------|
| VS.ADD | 添加向量 |
| VS.SEARCH | 向量相似度搜索 |
| VS.REM | 移除向量 |
| VS.DROPINDEX | 删除索引 |

---

## RediSearch 命令

### 索引管理
| 命令 | 描述 |
|------|------|
| FT.CREATE | 创建索引 |
| FT.DROPINDEX | 删除索引 |
| FT.ALTER | 修改索引 |
| FT.INFO | 索引信息 |
| FT._LIST | 列出索引 |

### 搜索和查询
| 命令 | 描述 |
|------|------|
| FT.SEARCH | 全文搜索 |
| FT.AGGREGATE | 聚合查询 |
| FT.EXPLAIN | 解释查询计划 |
| FT.SPELLCHECK | 拼写检查 |
| FT.TAGVALS | 获取标签值 |

### 同义词
| 命令 | 描述 |
|------|------|
| FT.SYNADD | 添加同义词组 |
| FT.SYNDUMP | 导出同义词 |
| FT.SYNUPDATE | 更新同义词 |

---

## Time Series 命令 (Redis 8.x)

| 命令 | 描述 |
|------|------|
| TS.CREATE | 创建时序 |
| TS.ALTER | 修改时序 |
| TS.ADD | 添加样本 |
| TS.MADD | 批量添加 |
| TS.INCRBY | 递增 |
| TS.DECRBY | 递减 |
| TS.CREATERULE | 创建聚合规则 |
| TS.DELETERULE | 删除聚合规则 |
| TS.RANGE | 范围查询 |
| TS.REVRANGE | 倒序范围 |
| TS.MRANGE | 多序列范围 |
| TS.MREVRANGE | 多序列倒序范围 |
| TS.GET | 获取最新 |
| TS.MGET | 批量获取最新 |
| TS.INFO | 序列信息 |
| TS.QUERYINDEX | 索引查询 |

---

## Bloom Filter 命令

| 命令 | 描述 |
|------|------|
| BF.RESERVE | 创建过滤器 |
| BF.ADD | 添加元素 |
| BF.MADD | 批量添加 |
| BF.INSERT | 插入元素 |
| BF.EXISTS | 检查存在 |
| BF.MEXISTS | 批量检查 |
| BF.SCANDUMP | 序列化 |
| BF.LOADCHUNK | 反序列化 |
| BF.INFO | 过滤器信息 |

---

## Cuckoo Filter 命令

| 命令 | 描述 |
|------|------|
| CF.RESERVE | 创建过滤器 |
| CF.ADD | 添加元素 |
| CF.ADDNX | 仅当不存在时添加 |
| CF.INSERT | 插入元素 |
| CF.INSERTNX | 条件插入 |
| CF.DEL | 删除元素 |
| CF.COUNT | 计数元素 |
| CF.EXISTS | 检查存在 |
| CF.MEXISTS | 批量检查 |
| CF.SCANDUMP | 序列化 |
| CF.LOADCHUNK | 反序列化 |
| CF.INFO | 过滤器信息 |

---

## Count-Min Sketch 命令

| 命令 | 描述 |
|------|------|
| CMS.INITBYDIM | 按维度初始化 |
| CMS.INITBYPROB | 按概率初始化 |
| CMS.INCRBY | 增加计数 |
| CMS.QUERY | 查询计数 |
| CMS.MERGE | 合并 Sketch |
| CMS.INFO | 信息查询 |

---

## Top-K 命令

| 命令 | 描述 |
|------|------|
| TOPK.RESERVE | 创建 Top-K |
| TOPK.ADD | 添加元素 |
| TOPK.INCRBY | 增加分数 |
| TOPK.QUERY | 查询排名 |
| TOPK.COUNT | 查询计数 |
| TOPK.LIST | 列出 Top-K |
| TOPK.INFO | 信息查询 |

---

## T-Digest 命令

| 命令 | 描述 |
|------|------|
| TD.CREATE | 创建 T-Digest |
| TD.RESET | 重置 |
| TD.ADD | 添加值 |
| TD.MERGE | 合并 |
| TD.MIN | 最小值 |
| TD.MAX | 最大值 |
| TD.QUANTILE | 分位数 |
| TD.CDF | 累积分布 |
| TD.TRIMMED_MEAN | 截断均值 |
| TD.RANK | 排名 |
| TD.REVRANK | 倒序排名 |
| TD.BYRANK | 按排名取值 |
| TD.BYREVRANK | 按倒序排名取值 |
| TD.INFO | 信息查询 |

---

## 事务命令

| 命令 | 描述 |
|------|------|
| MULTI | 开始事务 |
| EXEC | 执行事务 |
| DISCARD | 放弃事务 |
| WATCH | 监视键 |
| UNWATCH | 取消监视 |

---

## Lua 脚本命令

| 命令 | 描述 |
|------|------|
| EVAL | 执行脚本 |
| EVALSHA | 通过 SHA 执行 |
| SCRIPT LOAD | 加载脚本 |
| SCRIPT EXISTS | 检查脚本 |
| SCRIPT FLUSH | 清空脚本缓存 |
| SCRIPT KILL | 终止脚本 |
| SCRIPT DEBUG | 调试模式 |

---

## Redis Functions 命令 (Redis 8.x)

| 命令 | 描述 |
|------|------|
| FUNCTION LOAD | 加载函数库 |
| FUNCTION DELETE | 删除函数库 |
| FUNCTION KILL | 终止执行 |
| FUNCTION FLUSH | 清空所有函数 |
| FUNCTION LIST | 列出函数 |
| FUNCTION STATS | 统计信息 |
| FUNCTION DUMP | 序列化函数 |
| FUNCTION RESTORE | 反序列化函数 |
| FCALL | 调用函数 |
| FCALL_RO | 只读调用 |

---

## 发布订阅命令

### 经典 Pub/Sub
| 命令 | 描述 |
|------|------|
| PUBLISH | 发布消息 |
| SUBSCRIBE | 订阅频道 |
| UNSUBSCRIBE | 取消订阅 |
| PSUBSCRIBE | 模式订阅 |
| PUNSUBSCRIBE | 取消模式订阅 |
| PUBSUB | 发布订阅状态 |

### Sharded Pub/Sub (Redis 8.x 集群)
| 命令 | 描述 |
|------|------|
| SPUBLISH | 分片发布 |
| SSUBSCRIBE | 分片订阅 |
| SUNSUBSCRIBE | 分片取消订阅 |
| PUBSUB SHARDCHANNELS | 分片频道 |
| PUBSUB SHARDNUMSUB | 分片订阅数 |

---

## 客户端命令

| 命令 | 描述 |
|------|------|
| CLIENT LIST | 列出客户端 |
| CLIENT SETNAME | 设置名称 |
| CLIENT GETNAME | 获取名称 |
| CLIENT KILL | 断开客户端 |
| CLIENT PAUSE | 暂停客户端 |
| CLIENT REPLY | 控制回复 |
| CLIENT TRACKING | 启用缓存跟踪 |
| CLIENT TRACKINGINFO | 跟踪信息 |
| CLIENT CACHING | 控制缓存 |
| CLIENT GETREDIR | 获取重定向 |
| CLIENT ID | 获取客户端 ID |
| CLIENT INFO | 客户端信息 |
| CLIENT UNBLOCK | 解阻塞 |

---

## ACL 命令 (Redis 8.x)

| 命令 | 描述 |
|------|------|
| ACL LIST | 列出用户 |
| ACL USERS | 列出用户名 |
| ACL GETUSER | 获取用户信息 |
| ACL SETUSER | 设置用户 |
| ACL DELUSER | 删除用户 |
| ACL CAT | 列出命令类别 |
| ACL LOG | 审计日志 |
| ACL HELP | 帮助信息 |
| ACL GENPASS | 生成密码 |
| ACL WHOAMI | 当前用户 |
| ACL LOAD | 加载 ACL 文件 |
| ACL SAVE | 保存 ACL 文件 |

---

## 集群命令

| 命令 | 描述 |
|------|------|
| CLUSTER NODES | 集群节点信息 |
| CLUSTER MEET | 添加节点 |
| CLUSTER FORGET | 移除节点 |
| CLUSTER REPLICATE | 设置副本 |
| CLUSTER SLOTS | 获取槽位信息 |
| CLUSTER ADDSLOTS | 分配槽位 |
| CLUSTER DELSLOTS | 删除槽位 |
| CLUSTER FLUSHSLOTS | 清空槽位 |
| CLUSTER KEYSLOT | 计算键槽位 |
| CLUSTER COUNTKEYSINSLOT | 槽位键数 |
| CLUSTER GETKEYSINSLOT | 获取槽位键 |
| CLUSTER INFO | 集群信息 |
| CLUSTER SAVECONFIG | 保存配置 |
| CLUSTER FAILOVER | 故障转移 |
| CLUSTER SET-CONFIG-EPOCH | 设置纪元 |
| CLUSTER BUMPEPOCH | 增加纪元 |
| CLUSTER RESET | 重置节点 |
| CLUSTER SLAVES | 列出副本 |
| CLUSTER REPLICAS | 列出副本 |
| CLUSTER COUNT-FAILURE-REPORTS | 故障报告数 |

---

## 地理位置命令

| 命令 | 描述 |
|------|------|
| GEOADD | 添加位置 |
| GEOPOS | 获取位置 |
| GEODIST | 计算距离 |
| GEORADIUS | 范围搜索 |
| GEORADIUSBYMEMBER | 以成员为中心搜索 |
| GEOHASH | 获取 GeoHash |
| GEORADIUS_RO | 只读范围搜索 |
| GEORADIUSBYMEMBER_RO | 只读成员搜索 |

---

## HyperLogLog 命令

| 命令 | 描述 |
|------|------|
| PFADD | 添加元素 |
| PFCOUNT | 估算基数 |
| PFMERGE | 合并 HyperLogLog |

---

## 键空间通知

Godis 支持 Redis 键空间通知机制，可通过配置启用。

---

## 不支持的命令

以下命令因设计原因或实现优先级暂未支持：

- **模块相关**: `MODULE LOAD/UNLOAD/LIST` (使用内置模块替代)
- **内存管理**: `MEMORY DOCTOR/MEMORY HELP` (保留 INFO 统计)
- **慢日志**: `SLOWLOG GET/LEN/RESET` (使用 ACL LOG 替代)
- **副本相关**: `REPLICAOF SLAVEOF` (仅支持内部复制)
- **迁移命令**: `MIGRATE/RESTORE-ASKING` (使用 DUMP/RESTORE)

---

## 命令统计

截至最新版本，Godis 支持约 **300+** 个 Redis 命令：

| 类别 | 数量 |
|------|------|
| 键管理 | 20+ |
| 字符串 | 20+ |
| 列表 | 15+ |
| 哈希 | 15+ |
| 集合 | 15+ |
| 有序集合 | 25+ |
| Stream | 15+ |
| JSON | 25+ |
| Vector Set | 4 |
| RediSearch | 15+ |
| Time Series | 15+ |
| 概率数据结构 | 40+ |
| 事务 | 5 |
| 发布订阅 | 10+ |
| 集群 | 15+ |
| 管理 | 30+ |
| Lua/Functions | 15+ |
| ACL | 10+ |

---

## 更新日志

### 2024 (Redis 8.x 兼容性)
- 添加 RESP3 协议支持
- 添加 JSON 数据类型
- 添加 Vector Set 向量搜索
- 添加 RediSearch 全文搜索
- 添加 Time Series 时序数据
- 添加 Bloom/Cuckoo 过滤器
- 添加 CMS/Top-K/T-Digest
- 添加 Redis Functions
- 添加客户端缓存
- 添加 Sharded Pub/Sub
- 添加 ACL LOG 审计日志
- 添加 Hash Field 过期

### 早期版本
- 基础数据类型 (String, List, Hash, Set, ZSet)
- Stream
- Bitmap
- 事务
- 主从复制
- 集群 (Raft)
- AOF/RDB 持久化
