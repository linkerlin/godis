# Godis RediSearch 全面对齐 Redis 8.x — 实现总结

> 本文档总结 Godis 对 Redis 8.x RediSearch(8.0 起并入 Redis 核心,又称 Redis Query Engine)
> 的全面对齐工作:目标、分阶段实现、覆盖矩阵、架构决策、使用示例、已知限制。
> 对齐参照见 [`REDISEARCH_8X_FEATURE_MATRIX.md`](../REDISEARCH_8X_FEATURE_MATRIX.md)。

---

## 1. 概述

Redis 8.0 将 Redis Stack 的 RediSearch 模块并入核心(命令面 24 个 FT.\* 命令,旧的
`FT.SUGADD/SYNADD/FT.ADD` 等已在 2.0 移除)。Godis 此前只有 Phase A/B 的粗略实现
(基础 SCHEMA/搜索/简单 AGGREGATE),存在大量正确性 bug 和功能空洞:

- `HDEL` 不触发索引更新、`FT.PROFILE` 造假时间、`FT.SPELLCHECK` 丢弃 TERMS 字典
- `VECTOR` 字段类型只被 lexer 吃掉,无 FLAT/HNSW 参数、无 KNN 查询
- DIALECT 只校验不生效;`MINPREFIX/MAXEXPANSIONS` 存了不读
- 打分是单一手写 TF-IDF;APPLY 表达式只有 `+-*/`
- 索引触发面缺 HINCRBY/JSON.\*/RENAME/TTL 系列
- 回复永远是 RESP2 数组,无 8.x Map 形态

本次工作分 12 个阶段逐一补齐,累计 **63 个新测试**,9 次提交,`go build` + `go vet` 全程干净。

| 指标 | 值 |
|---|---|
| 新增/修改文件 | ~30 |
| 新增测试 | 63(全部通过) |
| 提交数 | 9 |
| 覆盖阶段 | P1~P10 + GEOSHAPE + COLLECT/VAMANA + 收尾 |

---

## 2. 分阶段实现明细

### P1 — 正确性 + 字段选项(16 测试)

**正确性修复:**
- `HDEL` 删除索引字段后不再残留 stale 文档(删除最后一个字段时 `removeHashFromIndex`,否则 `reindexHash`)。
- `FT.SPELLCHECK` 真正消费 `TERMS INCLUDE/EXCLUDE` 字典:新增 `SpellCheckWithDicts`,INCLUDE 扩充候选池、EXCLUDE 屏蔽建议,分数改为真实 `1/(1+Levenshtein)`,并走别名解析。
- `FT.PROFILE` 不再造假 5/95 时间拆分,改报诚实总耗时 + 结果数。
- `MINPREFIX`/`MAXEXPANSIONS` 接进查询引擎:`ValidateExpansions` 解析后对 AST 校验,短前缀/超限展开报错(SEARCH 与 AGGREGATE 都生效)。

**字段类型选项(`Field` 结构扩展 + 解析器):**
- 新增 `Phonetic`(dm:en/fr/pt/es,带校验)、`IndexMissing`、`IndexEmpty`、`WithSuffixTrie`、`CaseSensitive`、`SortableUNF`、`CoordinateSystem`。
- 行为接通:`CASESENSITIVE`(TAG 保留大小写)、`INDEXEMPTY`(空串可检索,标记 token 索引)。
- `GEOSHAPE` 字段类型 + `FLAT|SPHERICAL` 坐标系解析。
- `FT.INFO` 的 `attributes` 反映全部新选项。

**索引级选项(FT.CREATE):** `NOOFFSETS`(丢位置→短语失效)、`NOFIELDS`(禁字段过滤,查询侧退化为全局)、`NOFREQS`(频率压平)行为接通;`NOHL/MAXTEXTFIELDS/TEMPORARY/FILTER/INDEXALL` 解析 + 存储 + FT.INFO 暴露。

**查询扩展:** 后缀 `*foo`、中缀 `*foo*` 搜索;fuzzy 扩到 1/2/3 档(`%t%`/`%%t%%`/`%%%t%%%`);两个 parser 同步支持。

### P2 — FT 内 VECTOR + KNN(6 测试)

- **`VectorFieldConfig` 解析**:`VECTOR <FLAT|HNSW> <count> TYPE DIM DISTANCE_METRIC M EF_CONSTRUCTION EF_RUNTIME INITIAL_CAP BLOCK_SIZE`(count 为总 token 数,Redis 约定)。
- **`FTVectorIndex`**(`datastruct/redisearch/vector.go`):L2/IP/COSINE 距离、`FLOAT32/FLOAT64` 解码、FLAT 暴力 + HNSW 图加速(复用 `datastruct/vector.HNSW`,distFn 闭包查共享 vectors map)。
- **自动索引**:HSET/JSON.SET 的 VECTOR blob 经 `IndexDocument` 解码入索引;删除同步清理。
- **KNN 查询**:`*=>[KNN K @field $param [AS score] [EF_RUNTIME n]]`,`SplitKNNClause` 解析(引号感知),DIALECT ≥ 2 强制。
- **PARAMS**:`PARAMS n name value ...` 解析 + DIALECT 校验延迟到选项循环后(允许 DIALECT 出现在任意位置)。
- **混合预过滤**:`@price:[10 100]=>[KNN 10 @v $v]` —— 基础查询先求候选集,`SearchKNNFiltered` 只对候选打分。
- **`engine.SearchKNN`**:统一入口,距离作为 score、`AS` 别名注入结果字段。

### P3 — DIALECT 2 查询语义(6 测试)

- **比较算符**:`@n == 5 / != / > / >= / < / <=` → `NumericCompareNode` + `idx.NumericCompare`(`!=` 对缺失/非数值字段也匹配)。DIALECT < 2 时 `RequiresDialect2` AST 校验拒绝。
- **ismissing**:`ismissing(@field)` → `MissingNode` + `idx.MissingDocIDs`,在 `parsePrimary` 拦截关键字。
- **多字段**:`@f1|f2:term` → 读取 `|` 分隔字段列表,`withField` 克隆 atom 做 OR 展开(6 种可携带字段的节点全支持)。
- **DIALECT 1 vs 2 `|` 优先级**:parser 加 `dialect` 字段;D2 走 `parseAndD2`(`|` 比空格紧,`a b|c` = `a (b|c)`),D1 走原 `parseOr`(`|` 更松)。`SearchOptions`/`AggregationRequest` 加 `Dialect`,SEARCH 与 AGGREGATE 都接通。

### P4 — FT.AGGREGATE 完整化(3 测试)

- **Reducers 补全**:`STDDEV`(总体标准差)、`QUANTILE`(nearest-rank)、`COUNT_DISTINCT`(精确)、`COUNT_DISTINCTISH`(14-bit HyperLogLog)、`FIRST_VALUE`(支持 `BY @f [ASC|DESC]`)、`RANDOM_SAMPLE`(蓄水池采样)。`Reducer` 加 `Args []string` 承载多参数。
- **APPLY 表达式重写**(`apply.go`):完整递归下降求值器,统一服务 APPLY 和 FILTER。新增 `%`(模)、`^`(幂,右结合)、`**` 别名;内建算术/字符串/布尔函数，并按 Redis 8.10 实测补齐 UTC `timefmt/parsetime/day/hour/minute/month/dayofweek/dayofmonth/dayofyear/year/monthofyear` 与 `geodistance`;单/双引号字符串;裸标识符(非函数调用)作字符串字面量(让 `@cat == active` 生效)。
- **FILTER 布尔组合**:`filterGroups` 重写,复用统一求值器,支持 `&& || !` 嵌套 + 全套比较算符 + 括号分组(替代原单条二元比较)。

### P5 — 打分函数 + SCORER(3 测试)

- **8 个打分器**(`scoring.go`):`BM25STD`(8.x 默认,k1=1.2/b=0.09,长度归一化)、`BM25`(废弃别名)、`BM25STD.NORM/.TANH`、`TFIDF/TFIDF.DOCNORM`、`DISMAX`、`DOCSCORE`、`HAMMING`(payload 汉明距离)。
- **文档长度跟踪**:`InvertedIndex` 加 `docLengths/totalLength`(索引时累加、删除时回收),`buildScoreContext` 一次算好 avgdl/查询词项/可选词项供每文档复用。
- **`SCORER` 选项解析** + PAYLOAD 流入 opts(HAMMING 用)。
- **optional `~` 打分加成**:修 `OptionalNode.Evaluate`(原返回空导致 AND 链清空结果),现返回全文档(不过滤);`CollectOptionalTerms` 从 AST 提取 `~term`,打分器对含可选词的文档加成才。

### P6 — 索引触发面补全(5 测试)

对照 RediSearch `notifications.c` 的 keyspace 触发清单补齐:
- **Hash**:`HINCRBY/HINCRBYFLOAT`(各 2 个 return 点)。
- **JSON**:`NUMINCRBY/STRAPPEND/ARRAPPEND/ARRINSERT/MSET/CLEAR/TOGGLE/MERGE`。
- **键迁移**:`RENAME/RENAME NX`(源清除 + 目标重建)、`COPY`(跨 DB 目标重建)、`RESTORE`(先清后建)、`SET` 覆盖 hash/JSON 时清旧索引。
- **TTL**:`EXPIRE` 系列统一入口 + `PERSIST`;`HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT`(mutated 标志,字段删除/TTL 变化时重建)+ `HPERSIST`。
- 新增类型安全辅助 `reindexKey/removeKeyFromIndex`(hash+JSON 双路,错类型自动 no-op)。

### P7 — RESP3 Map 回复(3 测试)

利用服务器层 `ReplyToRESP3` 对实现 `RESP3Reply` 接口的类型自动调 `ToRESP3()` 的机制,设计**双形回复类型**(RESP2 字节完全不变,向后兼容):
- **`FTSearchReply`**:`ToBytes()` = 既有 positional array;`ToRESP3()` = 8.x map(`total_results/results/attributes/format/warning`,results 内每文档子 map,按 withScores/noContent 等标志算 stride 重建)。
- **`FTAggregateReply`**:同模式,rows 透传。
- **`FT.INFO`**:改返 `MapReply` + 递归 `interfaceToReply`,嵌套结构(attributes/index_definition)在两协议下都正确。
- 测试辅助 `ftSearchMultiRaw/aggTotal` 解包新类型,既有断言同步修正。

### P8 — 持久化(1 端到端测试)

- **AOF 缺口**:`FT.CONFIG SET`(DEFAULT_DIALECT/MINPREFIX/TIMEOUT 等配置重启丢失)、`FT.DICTADD/DICTDEL`(拼写字典用 `PutEntity` 直接写 map 不走命令,永不落盘)补 `addAof`(仅变更时)。
- **端到端验证**:`TestP8FTPersistence` —— 建索引 + HSET + CONFIG SET + DICTADD → 关服务器刷 AOF → 重启回放 → 索引/数据/配置/字典全部恢复。
- **已知限制**:官方 RediSearch 模块 RDB 仍不互通；**纯 AOF rewrite** + **Godis opaque `ft`（RDB/RDB-preamble）** 已写出定义并在 Load 后回填（`TestP8FTAofRewritePersistsIndexDef` / `TestP8FTRDBPersistsIndexDef`）。

### P9 — FT.HYBRID(8.4,6 测试)

- **熔合逻辑**(`hybrid.go`):`CombineHybrid` —— RRF(`score = Σ 1/(CONSTANT+rank)`,默认 C=60/WINDOW=20)与 LINEAR(`alpha*text + beta*vec`,配 `NormalizeScores` min-max 归一化);文档只在单边时另一边贡献 0。
- **命令**(`redisearch_hybrid.go`):解析 `SEARCH/SCORER/VSIM KNN/COMBINE RRF|LINEAR/LIMIT/SORTBY/NOSORT/PARAMS`;复用 `engine.Search`(文本侧)+ `VectorIndex.SearchKNN`(向量侧),熔合后输出 `__hybrid_score/__text_score/__vec_score`。RANGE/FILTER/POLICY 接受但延后(KNN 已是 ADHOC_BF 暴力路径)。

### P10 — search-\* 配置命名空间(4 测试)

8.0 把 `FT.CONFIG` 的 ALLCAPS 键重命名为 `search-*` kebab-case 并入 `CONFIG`:
- `ftKebabMap` 映射 7 键(`search-timeout/on-timeout/max-search-results/max-aggregate-results/min-prefix/max-expansions/default-dialect`)到内部 ALLCAPS 键,`ftConfig` 加 `MAXAGGREGATERESULTS`。
- `CONFIG GET search-*` 经 `searchKebabPairs()` 取值;`CONFIG SET search-*` 经 `setSearchKebab()` 路由(数值/方言/ON_TIMEOUT 校验)。与 FT.CONFIG 双向共享同一份值。

### GEOSHAPE 空间谓词(4 测试,补完 P1-h)

- **WKT 解析**(`geoshape.go`):`POINT/MULTIPOINT/LINESTRING/POLYGON`(含多环/洞)。
- **空间谓词** `RelateGeoShape`:WITHIN/CONTAINS(顶点全在内 + 无边交叉)、INTERSECTS(任一顶点在内或任一边交叉,射线投射 + 线段相交)、DISJOINT(`!INTERSECTS`)。
- **索引**:`geoshapeIndices`(field→docID→shape),AddDocument 解析 WKT 存储。
- **查询**:`@geom:[WITHIN $poly]`(DIALECT 3 + PARAMS)→ `GeoShapeNode`(Evaluate 粗筛字段存在)+ `filterByGeoshapeNodes` 后过滤精筛(从 `opts.Params` 解析 WKT)。

### COLLECT reducer(8.8,3 测试)+ SVS-VAMANA(8.2,1 测试)

- **COLLECT**:`REDUCE COLLECT nargs FIELDS (*|n @f...) [DISTINCT] [SORTBY @f [ASC|DESC]] [LIMIT o c]`。解析器特殊处理内嵌关键字(按 nargs 精确消费,仅 `AS` 终止);`reducerCollect` 实现投影(`@__key/@__score` 支持)/去重/组内排序/截断;返回 `[]CollectEntry`,`aggRowBytes` 序列化为嵌套数组。
- **SVS-VAMANA**:算法解析接受,`COMPRESSION`(LVQ4/LVQ8/LeanVec,Intel 专有)接受并忽略(OSS Redis 本就是 8-bit 标量回退,godis 存 FLOAT32 更准);图后端复用 HNSW。

### 收尾打磨(2 测试)

- **FT.EXPLAIN 真实计划树**:`ExplainNode` 递归渲染 16 种 AST 节点(`TERM/PHRASE/PREFIX/SUFFIX/INFIX/FUZZY/TAG/RANGE/COMPARE/GEORANGE/GEOSHAPE/MISSING/INTERSECT/UNION/NOT/OPTIONAL`),替换原手写浅树;execFTExplain 解析真实 AST(dialect-aware)。
- **MAXAGGREGATERESULTS 钳制**:execFTAggregate 的 LIMIT 上限从错误的 MAXSEARCHRESULTS 改为 MAXAGGREGATERESULTS(同款"配置存了不读"bug 修复)。

---

## 3. 功能覆盖矩阵

| 能力 | Redis 8.x | Godis | 说明 |
|---|---|---|---|
| **命令面** | 24 个 FT.\* + FT.HYBRID(8.4) | ✅ | 含 FT.HYBRID;已移除的老命令(SUGADD/SYNADD/FT.ADD)按 8.x 不提供 |
| FT.CREATE 索引级选项 | NOOFFSETS/NOHL/NOFIELDS/NOFREQS/MAXTEXTFIELDS/TEMPORARY/FILTER/INDEXALL | ✅ | 前 4 个行为接通,其余解析+存储+INFO |
| 字段类型 | TEXT/NUMERIC/TAG/GEO/VECTOR/GEOSHAPE | ✅ | 全部 6 种 |
| 字段选项 | PHONETIC/INDEXMISSING/INDEXEMPTY/WITHSUFFIXTRIE/CASESENSITIVE/UNF/SORTABLE_UNF/SEPARATOR/WEIGHT/NOSTEM | ✅ | 解析+校验+部分行为(索引 MISSING 标记查询侧随 DIALECT 2) |
| VECTOR 算法 | FLAT/HNSW/SVS-VAMANA | ✅ | VAMANA 走 HNSW 后端(OSS 语义) |
| VECTOR 类型 | FLOAT32/64/16/BFLOAT16/INT8/UINT8 | ✅ | blob→float32 解码齐全；存储仍 widen 为 f32 |
| KNN 查询 | `*=>[KNN K @f $p]` + HYBRID 预过滤 | ✅ | 含 EF_RUNTIME/AS 别名 |
| FT.HYBRID | RRF/LINEAR 熔合(8.4) | ✅ | RANGE 向量搜索延后 |
| DIALECT 2 | PARAMS/比较算符/ismissing/多字段/\| 优先级 | ✅ | |
| 查询语法 | 前缀/后缀/中缀/fuzzy 1-3/短语/SLOP/INORDER/标签/数值/geo 半径 | ✅ | |
| GEOSHAPE | WKT + WITHIN/CONTAINS/INTERSECTS/DISJOINT(DIALECT 3) | ✅ | |
| AGGREGATE reducers | COUNT/SUM/MIN/MAX/AVG/STDDEV/QUANTILE/COUNT_DISTINCT(ISH)/TOLIST/FIRST_VALUE/RANDOM_SAMPLE/COLLECT | ✅ | |
| APPLY 表达式 | 算术 + % ^ + 数值/字符串/日期/geo + **matched_terms / 多值 split** | ✅ 子集 | 无 stemmer 变体；非完整 strftime |
| FILTER | 布尔组合 + 比较算符 | ✅ | |
| 打分 | BM25STD(默认)/TFIDF/DISMAX/DOCSCORE/HAMMING + 变体 | ✅ | .NORM 真 min-max；.TANH=tanh(raw/4) |
| 可选词 ~ | 打分加成、不过滤 | ✅ | |
| 索引触发面 | hash/json 全 keyspace 通知 | ✅ | 与 notifications.c 清单对齐 |
| RESP3 | SEARCH/AGGREGATE/INFO Map 形态 | ✅ | 双形,RESP2 兼容 |
| 持久化 | AOF/RDB | ✅ AOF | RDB 索引定义不持久(已知限制) |
| 配置 | search-\* kebab(8.0) | ✅ | 与 FT.CONFIG 互通 |
| FT.EXPLAIN | 真实执行计划 | ✅ | AST 渲染 |
| FT.PROFILE | 迭代器级 profile | 🔶 | 诚实总耗时,迭代器细分延后 |
| ACL 类别 | @search | ✅ | ACL CAT/SETUSER `+@search`；`ft.*` 前缀回退（见 `database/acl.go`） |

✅ 完整  🔶 部分/近似  ❌ 未实现

---

## 4. 架构决策与设计模式

1. **双形回复类型**(P7):不把连接对象线程进 exec 签名,而是让回复类型实现 `RESP3Reply.ToRESP3()`,服务器层 `ReplyToRESP3` 按连接协议自动选形。零破坏性,RESP2 字节不变。
2. **后过滤精筛**(GEOSHAPE/GEO):`QueryNode.Evaluate` 只做"字段存在"粗筛,真实空间谓词由 `engine.Search` 的 post-filter 应用(与既有 GeoRangeNode 同构,参数经 `SearchOptions.Params` 流入)。
3. **统一表达式求值器**(P4):APPLY 与 FILTER 共享同一递归下降求值器(算符→函数→比较→布尔分层),避免两套语法引擎漂移。
4. **AST 校验而非运行时**(P1/P3):`ValidateExpansions`/`RequiresDialect2`/`CollectOptionalTerms` 均解析后单遍走 AST,保持 `Evaluate` 接口不变。
5. **类型安全辅助**(P6):`reindexKey/removeKeyFromIndex` 双路调用,错类型自动 no-op,消除调用方类型判断散落。
6. **图后端复用**(P2/P9):FT 向量 HNSW 复用 `datastruct/vector.HNSW`,通过 distFn 闭包绑定共享 vectors map;VAMANA 同后端。
7. **诚实优先**(P1/P5):FT.PROFILE 不再造假时间;打分器未知名回退默认而非静默归零;`ponytail:` 注释标记简化点与升级路径。

---

## 5. 使用示例

```bash
# VECTOR 索引 + KNN
FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA title TEXT vec VECTOR HNSW 12 \
  TYPE FLOAT32 DIM 3 DISTANCE_METRIC COSINE M 16 EF_CONSTRUCTION 200 EF_RUNTIME 10
HSET doc:1 title "hello" vec "<float32 blob>"
FT.SEARCH idx "*=>[KNN 5 @vec $q]" PARAMS 2 q "<blob>" DIALECT 2

# 混合预过滤 KNN
FT.SEARCH idx "@price:[10 100]=>[KNN 5 @vec $q]" PARAMS 2 q "<blob>" DIALECT 2

# FT.HYBRID (8.4) — 文本+向量 RRF 熔合
FT.HYBRID idx SEARCH "golang" VSIM @vec "<blob>" KNN 1 K 10 \
  COMBINE RRF 3 CONSTANT 60 WINDOW 20 LIMIT 0 10

# GEOSHAPE (DIALECT 3)
FT.CREATE gidx SCHEMA geom GEOSHAPE
FT.ADD gidx d1 FIELDS geom "POINT(5 5)"
FT.SEARCH gidx "@geom:[WITHIN $poly]" PARAMS 2 poly "POLYGON((0 0,0 10,10 10,10 0,0 0))" DIALECT 3

# 聚合 COLLECT (8.8) — top-N per group
FT.AGGREGATE idx "*" GROUPBY 1 @cat \
  REDUCE COLLECT 9 FIELDS * SORTBY @price DESC LIMIT 0 3 AS top

# DIALECT 2 语法
FT.SEARCH idx "@price >= 100 && @cat:{books}" DIALECT 2
FT.SEARCH idx "ismissing(@optional_field)" DIALECT 2
FT.SEARCH idx "@title|body:golang" DIALECT 2

# 8.0 配置命名空间
CONFIG SET search-default-dialect 2
CONFIG GET search-max-search-results

# RESP3 下 FT.SEARCH 返回 Map 形态
HELLO 3
FT.SEARCH idx "hello"  # -> %5 total_results / results / attributes / format / warning
```

---

## 6. 测试与质量

- **63 个新测试**,覆盖每个阶段的核心行为:单元测试(WKT/谓词/熔合/表达式)+ 命令级端到端(建索引→写入→查询→断言回复)+ 持久化重启验证。
- 全部通过 `go test`;`go build ./...` 与 `go vet ./database/... ./datastruct/redisearch/...` 干净。
- 测试辅助统一:`searchTotalIs/ftSearchMultiRaw/aggTotal` 等解包双形回复,既有测试(phase_a/schema_test 等)同步适配。
- 注:原全量跑 flake（`TestM2boInfoKeyspaceAvgTTLAndSubexpiry`/`TestM2buLatencyHistogram`）已按测试隔离 + `LATENCY RESET` 回写修复收口。

---

## 7. 已知限制与后续工作

| 项 | 状态 | 说明 |
|---|---|---|
| VECTOR 类型解码 | ✅ | FLOAT16/BFLOAT16/INT8/UINT8 均解码为 float32；**VADD Q8** 存储 + **图内 int8 距离**；**VADD BIN** 存储 + **图内 Hamming** |
| BM25STD.NORM | ✅ | 结果集真 min-max；**TEXT WEIGHT** + **文档长度** + **可测 IDF** + **多字段加权求和**；**BM25STD.TANH** + **`BM25STD_TANH_FACTOR`**；**非**论文级完整 BM25（无 slop 罚分等） |
| GEOSHAPE DIALECT | ✅ | `@geom:[WITHIN $p]` 等强制 **DIALECT ≥ 3**（DIALECT 2+PARAMS 不再静默成功） |
| KNN/DIALECT 错误路径 | ✅ | 非法 DIALECT；缺 PARAMS / 非 VECTOR / dim；`HYBRID_POLICY` 枚举；**`$YIELD_DISTANCE_AS`**；**`$SHARD_K_RATIO`∈(0,1]**；**`BATCH_SIZE`/`EPSILON`/`$EF_RUNTIME` 缺值 ERR**；空预过滤；D1 拒 tag 空格/`@f1\|f2`；比较/`ismissing`/GEOSHAPE 具体 ERR（仍非完整方言） |
| VECTOR_RANGE | ✅ 子集 | `@vec:[VECTOR_RANGE r $q]` + DIALECT≥2；`$YIELD_DISTANCE_AS`/`$EPSILON`；FLAT 暴力；**非** HNSW 近似扫描 |
| FT.SEARCH EXPLAINSCORE | ✅ 子集 | 需 WITHSCORES；BM25STD/DOCSCORE/DISMAX 嵌套线格式；**非**字节级对齐 RediSearch（b=0.09） |
| SORTBY WITHCOUNT | ✅ 语法 | 接受令牌；准确总数本为默认（WITHOUTCOUNT 除外） |
| FT.PROFILE 迭代器细分 | 🔶 | 只报诚实总耗时;细分需 instrument 引擎迭代器 |
| FT.HYBRID RANGE/FILTER/POLICY | 🔶 | 接受但按暴力路径执行 |
| APPLY 日期/geo 函数 | 🔶 | UTC 时间函数族、常用 strftime 格式的 `timefmt`/`parsetime`、`geodistance` 已按 Redis 8.10 实测；完整 strftime 指令仍缺 |
| FT.SEARCH EXPLAINSCORE | ✅ | 需 WITHSCORES；BM25STD/TFIDF/DOCSCORE/DISMAX 嵌套线格式子集（Godis b=0.09 自洽，非字节级对齐）；RESP3 score 嵌套 |
| RDB / AOF rewrite 索引定义持久化 | ✅ | 命令 AOF + 纯 AOF rewrite→FT.CREATE + RDB opaque `ft`（非官方模块格式） |
| ACL @search 类别 | ✅ | `+@search` / ACL CAT `@search` 已生效；非「需重构 ACL」 |
| LVQ/LeanVec 压缩 | ❌ | Intel 专有,OSS Redis 也没有 |
| GEOSHAPE SPHERICAL 数学 | 🔶 | 按 2D 平面处理(Redis 用测地线;影响跨经纬度边界场景) |
| SQLite 后端 | ❌ | `sqlite_backend` 构建标签路径仍只覆盖 FT.CREATE/ADD/SEARCH 文本 |

---

## 8. 提交历史

| 提交 | 内容 |
|---|---|
| `49d3326` | P1~P6 — 正确性/字段选项/VECTOR+KNN/DIALECT 2/聚合/打分/触发面 |
| `ab5c760` | P7 — RESP3 Map 回复(双形类型) |
| `bc78c79` | P8 — 持久化(CONFIG SET/DICT 补 AOF + 端到端验证) |
| `4b92fa0` | P9 — FT.HYBRID(RRF/LINEAR 熔合) |
| `982d56a` | P10 — search-\* 配置命名空间 |
| `3dba0cf` | GEOSHAPE 空间谓词(WKT + 4 谓词) |
| `4da8303` | COLLECT reducer(8.8)+ SVS-VAMANA(8.2) |
| `008988c` | 收尾打磨(FT.EXPLAIN 真实计划树 + MAXAGGREGATERESULTS 钳制) |
