# Redis 8.x RediSearch — Exhaustive Feature Matrix

> Research reference for gap-checking the Godis Go reimplementation.
> Source: redis.io official docs (FT.* command pages, develop/ai/search-and-query/*,
> commands/redis-8.0-commands/, redis-full.conf), plus the RediSearch source
> (`src/notifications.c`, `src/query_parser/v2/parser.y`).
>
> **Scope note:** Redis 8.0 merged the old Redis Stack "RediSearch" module into core
> as the **Redis Query Engine (RQE)**. Many legacy module commands were **removed**
> in 8.0. Where the user's request listed a command that no longer exists in 8.x
> core, that is stated explicitly. Redis 8.4 adds `FT.HYBRID`.

---

## 0. High-level facts (8.x)

| Fact | Value |
|---|---|
| Packaging | Merged into `redis` core in 8.0 (no more `redisearch.so` module load). Config keys renamed to `search-*` (e.g. `search-timeout`, `search-max-search-results`). |
| Data types indexed | HASH (default) and JSON (requires the built-in JSON type). String keys are **not** indexed directly (Upstash-style "string documents" are a hosted offering, not OSS RQE). |
| Write path | No `FT.ADD` / `FT.DEL` / `FT.GET`. Indexing is driven by **keyspace notifications** fired by native Redis write commands. |
| Default dialect | **DIALECT 1** (still). Dialects 1, 3, 4 are deprecated; DIALECT 2 is the recommended target. Redis Software 8.0 docs say DIALECT 2 is enforced, but in OSS `DEFAULT_DIALECT` defaults to 1 and is configurable. |
| Default scorer | **BM25STD** (renamed from `BM25` in 8.4; `BM25` is deprecated alias). |
| Vector algorithms | FLAT, HNSW (since pre-8.0); **SVS-VAMANA** added in 8.2 with compression (LVQ / LeanVec — Intel-optimized variants are proprietary, OSS falls back to 8-bit scalar). |
| Vector numeric types | FLOAT32, FLOAT64 (all versions); FLOAT16, BFLOAT16 (since 2.10); **INT8, UINT8** (since 8.0, part of quantization). |
| ACL categories | `@search`, plus `@read`/`@fast` on read commands. |
| RESP3 | `FT.SEARCH` / `FT.AGGREGATE` return Map replies in RESP3 (with `total_results`, `results`, `attributes`, `format`, `warning`). |

---

## 1. Complete FT.* command list (Redis 8.0 core)

All 24 commands present in the OSS Redis 8.0 command table, plus the 8.4 addition.
"Aliases" column: there are **no non-FT aliases** — every search command uses the
`FT.` prefix.

### 1.1 Commands present in Redis 8.0

| Command | Syntax | Arity | Flags | Since |
|---|---|---|---|---|
| `FT.CREATE` | see §2 | ≥3 (varies) | write | 1.0.0 |
| `FT.ALTER` | `FT.ALTER index [SKIPINITIALSCAN] SCHEMA ADD field options` | ≥5 | write | 1.0.0 |
| `FT.DROPINDEX` | `FT.DROPINDEX index [DD]` | 2 or 3 | write | 1.0.0 |
| `FT.ALIASADD` | `FT.ALIASADD alias index` | 3 | write | 1.4.0 |
| `FT.ALIASDEL` | `FT.ALIASDEL alias` | 2 | write | 1.4.0 |
| `FT.ALIASUPDATE` | `FT.ALIASUPDATE alias index` | 3 | write | 1.4.0 |
| `FT.SEARCH` | see §4 | ≥2 | readonly | 1.0.0 |
| `FT.AGGREGATE` | see §5 | ≥2 | readonly, fast | 1.1.0 |
| `FT.CURSOR READ` | `FT.CURSOR READ index cursor [COUNT read_size]` | ≥3 | readonly | 1.1.0 |
| `FT.CURSOR DEL` | `FT.CURSOR DEL index cursor` | 3 | write | 1.1.0 |
| `FT.INFO` | `FT.INFO index` | 2 | readonly | 1.0.0 |
| `FT.PROFILE` | `FT.PROFILE index <SEARCH\|AGGREGATE> [LIMITED] QUERY query` | ≥5 | readonly | 2.0.0 |
| `FT.EXPLAIN` | `FT.EXPLAIN index query [DIALECT dialect]` | 3 or 4 | readonly | 1.0.0 |
| `FT.EXPLAINCLI` | `FT.EXPLAINCLI index query [DIALECT dialect]` | 3 or 4 | readonly | 1.0.0 |
| `FT.TAGVALS` | `FT.TAGVALS index field_name` | 3 | readonly | 1.0.0 |
| `FT.SPELLCHECK` | `FT.SPELLCHECK index query [DISTANCE distance] [TERMS <INCLUDE\|EXCLUDE> dictionary] [DIALECT dialect]` | ≥3 | readonly | 1.0.0 |
| `FT.SYNUPDATE` | `FT.SYNUPDATE index synonym_group_id [SKIPINITIALSCAN] term [term ...]` | ≥3 | write | 1.2.0 |
| `FT.SYNDUMP` | `FT.SYNDUMP index` | 2 | readonly | 1.2.0 |
| `FT.DICTADD` | `FT.DICTADD dict term [term ...]` | ≥3 | write | 2.0.0 |
| `FT.DICTDEL` | `FT.DICTDEL dict term [term ...]` | ≥3 | write | 2.0.0 |
| `FT.DICTDUMP` | `FT.DICTDUMP dict` | 2 | readonly | 2.0.0 |
| `FT.CONFIG GET` | `FT.CONFIG GET option` | 2 | readonly, admin | 1.0.0 (**deprecated in 8.0**; use `CONFIG GET search-*`) |
| `FT.CONFIG SET` | `FT.CONFIG SET option value` | 3 | admin (**deprecated in 8.0**; use `CONFIG SET`) |
| `FT._LIST` | `FT._LIST` | 1 | readonly | 2.0.0 |

### 1.2 Added in Redis 8.4

| Command | Notes |
|---|---|
| `FT.HYBRID` | Hybrid text + vector search with RRF / LINEAR fusion. See §4.14. |

### 1.3 Commands the user asked about that DO NOT exist in Redis 8.x core

These are either legacy Stack-module commands that were removed, internal debug
helpers that were never part of the public command table, or never existed.

| Asked-for command | Status in 8.x |
|---|---|
| `FT.SUGADD`, `FT.SUGGET`, `FT.SUGDEL`, `FT.SUGLEN` | **Auto-complete suggestions removed in 2.0.** Not in 8.x at all. |
| `FT.SYNADD` | **Removed in 2.0.** Replaced by `FT.SYNUPDATE`. |
| `FT.SYNFORCEUPDATE` | **Internal/removed.** Not in the public command table. Synonyms are re-indexed automatically by `FT.SYNUPDATE` (or via `SKIPINITIALSCAN` to skip). |
| `FT.ADD`, `FT.SAFEADD`, `FT.DEL`, `FT.GET`, `FT.MGET`, `FT.DROPINDEX DD`-only | **Removed in 2.0.** Replaced by native `HSET`/`JSON.SET`/`DEL` and index auto-updates. |
| `FT.MODULE` | Not a public command. Module is identified by `MODULE LIST` (`search` / `ReJSON` modules). |
| `FT._ALLOCLOG` | **Internal debug only**, not in the command table. Use `FT.DEBUG` if exposed, or `MEMORY STATS`. |
| `FT.DEBUG` | Not a top-level command in the 8.0 table; `FT.PROFILE` + `FT.INFO` cover diagnostics. Legacy `FT.DEBUG <subcommand>` was an internal helper. |
| `FT.METRICS` | **Not a public command.** Metrics surfaced via `FT.INFO` (gc_stats, cursor_stats, dialect_stats, index errors) and `INFO` server stats. |
| `FT.COPY` | **Does not exist.** Use native `COPY`. |
| `FT.LIST` (no underscore) | The real name is `FT._LIST` (with leading underscore). |
| `FT.CURSOR GC` | Internal GC; documented as an internal command, not the standard cursor API (`READ`/`DEL` are the public ones). |

---

## 2. FT.CREATE — full option reference

### 2.1 Full syntax (8.x)

```
FT.CREATE index
  [ON <HASH | JSON>]
  [PREFIX count prefix [prefix ...]]
  [FILTER filter]
  [LANGUAGE default_lang]
  [LANGUAGE_FIELD lang_attribute]
  [SCORE default_score]
  [SCORE_FIELD score_attribute]
  [PAYLOAD_FIELD payload_attribute]
  [MAXTEXTFIELDS]
  [TEMPORARY seconds]
  [NOOFFSETS]
  [NOHL]
  [NOFIELDS]
  [NOFREQS]
  [STOPWORDS count [stopword [stopword ...]]]
  [SKIPINITIALSCAN]
  [INDEXALL <ENABLE | DISABLE>]
  [INDEXMISSING]            /* index-wide flag (per-field also exists) */
  SCHEMA field_name [AS alias]
       <TEXT | TAG | NUMERIC | GEO | VECTOR | GEOSHAPE>
       [WITHSUFFIXTRIE]
       [INDEXEMPTY]
       [INDEXMISSING]
       [SORTABLE [UNF]]
       [NOINDEX]
       [ ... more fields ... ]
```

### 2.2 Index-level options (exact semantics)

| Option | Values / type | Notes |
|---|---|---|
| `ON` | `HASH` (default) \| `JSON` | JSON requires the JSON type to be available. |
| `PREFIX count prefix...` | integer + strings | `count` is followed by exactly `count` prefix strings. Default = `*` (all keys). |
| `FILTER expr` | aggregation-expression string | Filter applied per-key; `@__key` exposes the key name. |
| `LANGUAGE default_lang` | language name | Default `english`. Supported: `arabic`, `basque`, `catalan`, `chinese` (uses Friso segmentation), `czech`, `danish`, `dutch`, `english`, `finnish`, `french`, `german`, `greek`, `hungarian`, `indonesian`, `irish`, `italian`, `lithuanian`, `nepali`, `norwegian`, `portuguese`, `romanian`, `russian`, `serbian`, `spanish`, `swedish`, `tamil`, `turkish`, `yiddish`. Unsupported → error. |
| `LANGUAGE_FIELD lang_attribute` | hash/JSON field name | Per-document language override. |
| `SCORE default_score` | float 0.0–1.0 | Default `1.0`. Multiplied into the document score. |
| `SCORE_FIELD score_attribute` | field name | Per-document score source. |
| `PAYLOAD_FIELD payload_attribute` | field name | Binary-safe per-document payload (used by `WITHPAYLOADS`, `HAMMING` scorer, custom scorers). |
| `MAXTEXTFIELDS` | flag | Force-encodes the index as if it had >32 TEXT fields, so `FT.ALTER SCHEMA ADD` can push text-field count past 32. |
| `TEMPORARY seconds` | integer | Lightweight index that expires after `seconds` of inactivity (search or write resets the idle timer). |
| `NOOFFSETS` | flag | Don't store term offsets. Saves memory; disables exact-phrase, SLOP, highlighting. Implies `NOHL`. |
| `NOHL` | flag | Disable highlight support (no offsets stored for this purpose). Implied by `NOOFFSETS`. |
| `NOFIELDS` | flag | Don't store per-term field bits. Saves memory; disables field filtering. |
| `NOFREQS` | flag | Don't store term frequencies. Saves memory; disables frequency-based sorting/scoring. |
| `STOPWORDS count word...` | integer + strings | `count` must equal the number of words that follow. `STOPWORDS 0` = no stopwords. If omitted, uses the built-in default list. |
| `SKIPINITIALSCAN` | flag | Don't backfill existing keys when creating the index. |
| `INDEXALL <ENABLE\|DISABLE>` | enum | Whether to index missing fields as a special "missing" token regardless of per-field `INDEXMISSING`. |
| `INDEXMISSING` (index-wide) | flag | Index-wide missing-value indexing. |

**Stopwords default list** (when `STOPWORDS` is omitted):
`a, an, and, are, as, at, be, but, by, for, if, in, into, is, it, no, not, of, on,
or, such, that, their, then, there, these, they, this, to, was, will, with`.

### 2.3 Field types (SCHEMA)

Six types. Each has its own option set.

| Type | Allowed options |
|---|---|
| `TEXT` | `WEIGHT weight` (float, default 1.0), `NOSTEM`, `PHONETIC matcher`, `SORTABLE [UNF]`, `NOINDEX`, `WITHSUFFIXTRIE`, `INDEXEMPTY`, `INDEXMISSING` |
| `TAG` | `SEPARATOR sep` (single char, default `,`), `CASESENSITIVE`, `SORTABLE [UNF]`, `NOINDEX`, `WITHSUFFIXTRIE`, `INDEXEMPTY`, `INDEXMISSING` |
| `NUMERIC` | `SORTABLE`, `NOINDEX`, `INDEXMISSING` |
| `GEO` | `SORTABLE`, `NOINDEX`, `INDEXMISSING` (value = `"lon,lat"` string) |
| `VECTOR` | `VECTOR algorithm count [attr value ...]` — see §3 |
| `GEOSHAPE` | `[FLAT \| SPHERICAL]` (default `SPHERICAL`), `NOINDEX`, `INDEXMISSING`. **No `SORTABLE`.** Value = WKT (`POLYGON((x y, x y, ...))`, also `POINT`, `MULTIPOINT`, `LINESTRING` per the parser). |

### 2.4 Field options (cross-type)

| Option | Applies to | Meaning |
|---|---|---|
| `AS alias` | all | Logical attribute name; for JSON this is required to give a JSONPath a short name. |
| `SORTABLE` | TEXT, NUMERIC, TAG, GEO (not VECTOR, not GEOSHAPE) | Stores values in a special column for low-latency sort/range. Adds memory overhead. |
| `UNF` | with SORTABLE, hash only | Disable normalization (lowercasing + diacritic stripping). For JSON, `UNF` is implicit with `SORTABLE`. |
| `NOINDEX` | all | Field won't be indexed; useful with `SORTABLE` to make a retrievable-but-not-searchable column. |
| `WITHSUFFIXTRIE` | TEXT, TAG | Maintains a suffix trie for fast suffix (`*foo`) and infix (`*foo*`) queries. |
| `INDEXEMPTY` | TEXT, TAG (v2.10+) | Index empty strings so they can be queried. |
| `INDEXMISSING` | all types (v2.10+) | Index missing values so `ismissing(@field)` works (DIALECT 2+). |
| `NOSTEM` | TEXT | Disable stemming when indexing. Good for proper names. |
| `PHONETIC matcher` | TEXT | Phonetic matching. Matchers: `dm:en`, `dm:fr`, `dm:pt`, `dm:es` (Double Metaphone). |
| `WEIGHT weight` | TEXT | Multiplier for this field's contribution to score. Default 1.0. |
| `SEPARATOR sep` | TAG | Single-char separator for splitting multi-value tag strings. Default `,`. |
| `CASESENSITIVE` | TAG | Don't lowercase tags. |

### 2.5 JSON-specific schema notes

- The identifier is a **JSONPath** (e.g. `$.title`, `$.tags[*]`, `$..reviews[*].score`).
- `AS alias` is essentially mandatory for JSON; queries use the alias, not the path (the parser doesn't fully support raw JSONPaths in queries).
- Multi-value indexing (arrays) is supported for every type — arrays of scalars or arrays of objects matching the path (v2.6+, requires DIALECT 3+ to return all values).
- JSON type mapping: string → TEXT or TAG; number → NUMERIC; array of strings → TAG/TEXT; array of numbers → NUMERIC/VECTOR; boolean/string-encoded geo → GEO/GEOSHAPE.

---

## 3. VECTOR field type — full reference

### 3.1 Syntax

```
field_name VECTOR {FLAT | HNSW | SVS-VAMANA} count
  [TYPE <BFLOAT16 | FLOAT16 | FLOAT32 | FLOAT64 | INT8 | UINT8>]
  [DIM dim]
  [DISTANCE_METRIC <L2 | IP | COSINE>]
  ... algorithm-specific attrs ...
```

`count` = total number of `attribute value` pairs that follow (named-argument style).

### 3.2 Common attributes (all algorithms)

| Attribute | Required | Values | Notes |
|---|---|---|---|
| `TYPE` | yes | `BFLOAT16`, `FLOAT16`, `FLOAT32`, `FLOAT64`, `INT8`, `UINT8` | FLOAT16/BFLOAT16 since 2.10; INT8/UINT8 since 8.0. |
| `DIM` | yes | positive int | Number of components; query vector must match. |
| `DISTANCE_METRIC` | yes | `L2` (Euclidean), `IP` (inner product), `COSINE` | |

### 3.3 FLAT

```
VECTOR FLAT <count>
  TYPE <type>
  DIM <dim>
  DISTANCE_METRIC <L2|IP|COSINE>
  [INITIAL_CAP n]
  [BLOCK_SIZE n]            /* default 1024 */
```

Brute-force exact NN. O(N) query. Use for <1M vectors or when perfect recall is required.

### 3.4 HNSW

```
VECTOR HNSW <count>
  TYPE <type>
  DIM <dim>
  DISTANCE_METRIC <L2|IP|COSINE>
  [M m]                       /* default 16; max outgoing edges per node per layer; layer-0 = 2M */
  [EF_CONSTRUCTION ef]        /* default 200; candidate edges during build */
  [EF_RUNTIME ef]             /* default 10; top candidates held during search; overridable per query */
  [INITIAL_CAP n]
```

Approximate NN via hierarchical small-world graph.

### 3.5 SVS-VAMANA (8.2+)

```
VECTOR SVS-VAMANA <count>
  TYPE <FLOAT32 | FLOAT16>          /* typically */
  DIM <dim>
  DISTANCE_METRIC <COSINE|L2|IP>
  [COMPRESSION <mode>]              /* LVQ4, LVQ8, LeanVec4x8, LeanVec8x16, LVQ4x4, ... */
```

Graph-based NN optimized for compressed storage. **OSS Redis ships only the
8-bit scalar fallback** for `COMPRESSION`; Intel's proprietary LVQ/LeanVec
optimizations are not in OSS. Compression options:
- `LVQ4`, `LVQ8`, `LVQ4x4` — per-vector local scalar quantization (4- or 8-bit).
- `LeanVec4x8`, `LeanVec8x16` — dimensionality reduction + LVQ.

### 3.6 Quantization summary

| Mechanism | Available in OSS 8.x? | Reduction |
|---|---|---|
| INT8 / UINT8 vector type | yes (8.0+) | 4× vs FLOAT32 |
| SVS-VAMANA scalar 8-bit | yes (8.2+) | 4× |
| LVQ / LeanVec (Intel) | **no** (proprietary; needs Intel build) | up to ~60%+ |
| Binary (sign-bit) quant | via Vector Sets (`VADD ... BIN`), **not** via FT.* | 32× |

### 3.7 Empty / missing vector handling

- An empty vector field triggers an indexing failure (counted in `hash_indexing_failures`).
- Missing vector: with `INDEXMISSING` you can query missing via `ismissing(@field)`; otherwise the doc is indexed for its other fields but the vector field is absent.

---

## 4. FT.SEARCH — query language and options

### 4.1 Syntax (8.x)

```
FT.SEARCH index query
  [NOCONTENT]
  [VERBATIM]
  [NOSTOPWORDS]                       /* deprecated in 8.0 */
  [WITHSCORES]
  [WITHPAYLOADS]
  [WITHSORTKEYS]
  [FILTER numeric_field min max ...]   /* deprecated since 2.10; use @field:[min max] */
  [GEOFILTER geo_field lon lat radius <m|km|mi|ft> ...]  /* deprecated since 2.6; use @field:[...] */
  [INKEYS count key [key ...]]
  [INFIELDS count field [field ...]]
  [RETURN count identifier [AS property] ...]
  [SUMMARIZE [FIELDS count field ...] [FRAGS num] [LEN fragsize] [SEPARATOR sep]]
  [HIGHLIGHT [FIELDS count field ...] [TAGS open close]]
  [SLOP slop]
  [TIMEOUT timeout]
  [INORDER]
  [LANGUAGE language]
  [EXPANDER expander]
  [SCORER scorer]
  [EXPLAINSCORE]
  [PAYLOAD payload]
  [SORTBY field [ASC|DESC] [WITHCOUNT]]
  [LIMIT offset num]
  [PARAMS nargs name value ...]
  [DIALECT dialect]
```

### 4.2 Query grammar (full, dialect-agnostic unless noted)

| Construct | Syntax | Meaning |
|---|---|---|
| Multi-word (AND) | `foo bar baz` | Intersection of all terms. |
| Exact phrase | `"hello world"` | Adjacent terms in order. |
| OR | `a \| b \| c` | Union. |
| NOT (negation) | `-term`, `-@field:term`, `-(a b)` | Exclude. |
| Optional | `~term` | Term optional, but boosts docs containing it. |
| Prefix | `pre*` | All dict terms with that prefix. Min length 2 (configurable: `MINPREFIX`). |
| Suffix | `*suffix` | All dict terms with that suffix (brute-force unless `WITHSUFFIXTRIE`). Since 2.6. |
| Infix / contains | `*infix*` | Contains substring (brute-force unless `WITHSUFFIXTRIE`). Since 2.6. |
| Fuzzy | `%term%` | Levenshtein distance 1. `%%term%%` = 2. `%%%term%%%` = 3 (max). |
| Field-scoped | `@field:term` | Restrict to one field. |
| Multi-field scope | `@f1\|f2:term` | (DIALECT 2+) Multiple fields via pipe in modifier. |
| Field-scoped group | `@field:(a b)` \| `@field:(a\|b)` | Parens scope the modifier. |
| Weight boost (query-time) | `(...) => { $weight: 5.0; }` | Per-clause weight. |
| Query attributes | `... => { $k: v; }` | Generic attribute block (`$weight`, `$inorder`, `$slop`, `$SHARD_K_RATIO`, `$YIELD_DISTANCE_AS`). |
| Wildcard (all docs) | `*` | Match every indexed doc. |
| Tag values | `@tag_field:{val1 \| val2}` | Tag set query. Supports spaces inside `{...}` in DIALECT 2+. |
| Numeric range | `@n:[min max]` | Inclusive both ends. |
| Numeric open range | `@n:[(min (max]` | `(` = exclusive. |
| Numeric infinities | `@n:[-inf +inf]` | `-inf`/`+inf` allowed. |
| Comparison ops (DIALECT 2+) | `@n == 5`, `@n != 5`, `@n > 5`, `@n >= 5`, `@n < 5`, `@n <= 5` | |
| Geo radius | `@geo:[lon lat r m\|km\|mi\|ft]` | |
| Geo shape | `@geom:[WITHIN $poly]`, `@geom:[CONTAINS $poly]`, `@geom:[INTERSECTS $poly]`, `@geom:[DISJOINT $poly]` | DIALECT 3+. `$poly` from PARAMS, WKT `POLYGON((...))`. |
| Vector KNN | `*=>[KNN K @vector_field $blob AS score]` | DIALECT 2+. |
| Vector KNN + params | `*=>[KNN 10 @vec $v EF_RUNTIME 50]=>{$YIELD_DISTANCE_AS: dist}` | |
| Hybrid KNN + filter | `@price:[10 100]=>[KNN 10 @vec $v]` | Pre-filter then KNN. |
| Negate field | `-@field:term` | Docs without that field-term. |
| `ismissing` | `ismissing(@field)` | Requires field's `INDEXMISSING` (DIALECT 2+). |

### 4.3 Comparison cheat-sheet (SQL → Redis)

| SQL | Redis Search |
|---|---|
| `WHERE x='foo' AND y='bar'` | `@x:foo @y:bar` |
| `WHERE x='foo' AND y!='bar'` | `@x:foo -@y:bar` |
| `WHERE x='foo' OR y='bar'` | `(@x:foo)|(@y:bar)` |
| `WHERE x IN ('foo','bar','hello world')` | `@x:(foo\|bar\|"hello world")` |
| `WHERE y='foo' AND x NOT IN ('foo','bar')` | `@y:foo (-@x:foo) (-@x:bar)` |
| `WHERE num BETWEEN 10 AND 20` | `@num:[10 20]` |
| `WHERE num >= 10` | `@num:[10 +inf]` |
| `WHERE num > 10` | `@num:[(10 +inf]` |
| `WHERE num < 10 OR num > 20` | `@num:[-inf (10] \| @num:[(20 +inf]` |
| `WHERE name LIKE 'john%'` | `@name:john*` |

### 4.4 DIALECT 1 vs 2 vs 3 vs 4

| Dialect | Introduced | Key behaviors | Status in 8.x |
|---|---|---|---|
| 1 | original | Default. Field modifier applies to full multi-word phrase. `-a b` = `-a -b`. `a b \| c d` = `(a b\|c) d`. | **Default, but deprecated.** |
| 2 | 2.4.3 | `@f:a b` = `@f:a b` (Brown escapes to "any field"). `-a b` = `-a AND b`. `\|` splits the whole query. Supports PARAMS, KNN vector, comparison operators, `ismissing`. | **Recommended target.** |
| 3 | 2.6 | Adds multi-value JSON (returns arrays instead of first scalar). GEOSHAPE queries. | Deprecated. |
| 4 | 2.8 | Sorting optimizations (skip-sorter / partial-range / hybrid / no-opt). Selectable via `DIALECT 4` **or** `WITHOUTCOUNT`. | Deprecated. |

**DIALECT 2 is required for:** PARAMS, vector KNN, comparison operators (`==`,`!=`,`>`,`>=`,`<`,`<=`), `ismissing()`, tag-list spaces.

**Change the default:**
- `redis-server ... --loadmodule redisearch.so DEFAULT_DIALECT 2` (legacy module form)
- `FT.CONFIG SET DEFAULT_DIALECT 2` (deprecated in 8.0)
- `CONFIG SET search-default-dialect 2` (8.0+)

### 4.5 Post-query options

| Option | Effect |
|---|---|
| `LIMIT offset num` | Page results. `LIMIT 0 0` = count only. Default `0 10`. Capped by `search-max-search-results`. |
| `RETURN count id [AS prop] ...` | Project fields. `RETURN 0` ≡ `NOCONTENT`. Identifier may be attribute or JSONPath. |
| `SORTBY field [ASC\|DESC] [WITHCOUNT]` | Sort. `WITHCOUNT` returns accurate total (slower). DIALECT 4 / `WITHOUTCOUNT` enables skip-sorter optimization. |
| `WITHSCORES` | Include document score. |
| `WITHPAYLOADS` | Include payload (requires `PAYLOAD_FIELD`). |
| `WITHSORTKEYS` | Include the sort-key value (for distributed coordination). |
| `NOCONTENT` | IDs only. |
| `VERBATIM` | Disable stemming for query terms. |
| `SLOP n` | Max intermediate terms allowed between phrase terms. |
| `INORDER` | Require terms in document order (used with SLOP). |
| `INFIELDS n f...` | Field allowlist (≡ `@`-modifier but query-wide). |
| `INKEYS n k...` | Document key allowlist. |
| `FILTER field min max` | **Deprecated** legacy numeric filter (use `@field:[min max]`). |
| `GEOFILTER field lon lat r unit` | **Deprecated** legacy geo filter. |
| `SUMMARIZE [FIELDS n f...] [FRAGS n] [LEN n] [SEPARATOR s]` | Return text fragments containing matches. |
| `HIGHLIGHT [FIELDS n f...] [TAGS open close]` | Wrap matches with tags. |
| `SCORER name` | One of §7. |
| `EXPANDER name` | Custom query expander (extension API). |
| `EXPLAINSCORE` | Requires `WITHSCORES`; returns textual score breakdown. |
| `PAYLOAD payload` | Bind payload for custom scoring. |
| `LANGUAGE lang` | Stemmer language for this query. |
| `PARAMS nargs name value...` | Bind `$name` values; DIALECT ≥2 required. |
| `TIMEOUT ms` | Per-query timeout override. |
| `DIALECT n` | 1 (default), 2, 3, or 4. |

### 4.6 Vector KNN clause (full)

```
<base_query>=>[KNN K @vector_field $param [AS alias] [EF_RUNTIME n] [HYBRID_POLICY policy]]
```

- Must be DIALECT ≥ 2.
- `$param` is supplied via `PARAMS`.
- Optional `EF_RUNTIME` overrides the index HNSW runtime param.
- Cluster: add `=>{$SHARD_K_RATIO: 0.6; $YIELD_DISTANCE_AS: dist}` query attributes.
- `*=>[KNN ...]` is the unfiltered form (scans all docs).

### 4.7 Hybrid KNN (pre-filter then vector)

```
FT.SEARCH idx "@price:[10 100]=>[KNN 10 @vec $v AS dist]" PARAMS 2 v <blob> DIALECT 2
```

Pre-filter policies (configured at query level via query attributes, or index level):
- `ADHOC_BF` — evaluate filter on the fly per node.
- `BATCHES` — batch the filter, then traverse.

### 4.8 GEOSHAPE queries (CONTAINS / WITHIN / INTERSECTS / DISJOINT)

```
FT.SEARCH idx '@geom:[WITHIN $poly]'   PARAMS 2 poly 'POLYGON((0 0, 0 100, 100 100, 100 0, 0 0))' DIALECT 3
FT.SEARCH idx '@geom:[CONTAINS $poly]' PARAMS 2 poly 'POLYGON((2 2, 2 50, 50 50, 50 2, 2 2))'    DIALECT 3
FT.SEARCH idx '@geom:[INTERSECTS $p]'  ...
FT.SEARCH idx '@geom:[DISJOINT $p]'    ...
```

- Operators: `WITHIN`, `CONTAINS` (since 2.8); `INTERSECTS`, `DISJOINT` (since 2.10).
- WKT shapes supported: `POINT`, `MULTIPOINT`, `LINESTRING`, `POLYGON`.
- Coordinate system set at index time: `GEOSHAPE FLAT` (Cartesian) or `GEOSHAPE SPHERICAL` (default, geographic).
- No `SORTABLE`, no JSON multi-value for GEOSHAPE.

### 4.9 FT.PROFILE

```
FT.PROFILE index <SEARCH | AGGREGATE> [LIMITED] QUERY <full query including options>
```

Returns the result plus a profile breakdown (iterators, counters, parse time).
`LIMITED` omits per-result details. **Note:** `FT.PROFILE`, `FT.EXPLAIN`, and
`FT.EXPLAINCLI` in 8.4 do **not** include `FT.HYBRID` options.

### 4.10 FT.EXPLAIN / FT.EXPLAINCLI

```
FT.EXPLAIN    index query [DIALECT dialect]   -> bulk string (the plan)
FT.EXPLAINCLI index query [DIALECT dialect]   -> array of lines (the plan, line by line)
```

Both accept `DIALECT` so you can compare plans across dialects.

### 4.11 FT.SPELLCHECK

```
FT.SPELLCHECK index query [DISTANCE distance] [TERMS <INCLUDE|EXCLUDE> dictionary] [DIALECT dialect]
```

- `DISTANCE` = max Levenshtein distance (default 1, max 2).
- `TERMS INCLUDE/EXCLUDE dict` references a dictionary built via `FT.DICTADD`.
- Returns suggested corrections for each misspelled term.

### 4.12 Synonyms (FT.SYNUPDATE / FT.SYNDUMP)

```
FT.SYNUPDATE index group_id [SKIPINITIALSCAN] term [term ...]
FT.SYNDUMP   index
```

`SYNDUMP` returns `term1 -> [group_id1, group_id2, ...], term2 -> [...], ...`.
Synonyms are matched at query time (unless `VERBATIM` is set).

### 4.13 Dictionaries (FT.DICTADD / FT.DICTDEL / FT.DICTDUMP)

```
FT.DICTADD dict term [term ...]
FT.DICTDEL dict term [term ...]
FT.DICTDUMP dict
```

Standalone term lists used by `FT.SPELLCHECK TERMS`.

### 4.14 FT.HYBRID (8.4+)

```
FT.HYBRID index
  SEARCH  "search-expression"           /* FT.SEARCH-style text query */
  SCORER  name [params...]              /* optional, default BM25STD */
  VSIM    @vector_field "vector-data"
    { KNN count K k [EF_RUNTIME ef] [SHARD_K_RATIO r] [YIELD_SCORE_AS name]
    | RANGE count RADIUS r [EPSILON e]  [YIELD_SCORE_AS name] }
    [FILTER "filter-expr"]
    [POLICY <ADHOC_BF|BATCHES> [BATCH_SIZE n]]
  COMBINE
    { RRF    count CONSTANT c? WINDOW w? [YIELD_SCORE_AS name]
    | LINEAR count [ALPHA a BETA b]     WINDOW w? [YIELD_SCORE_AS name] }
  [LIMIT offset num]
  [SORTBY field [ASC|DESC] | NOSORT]
  [PARAMS nargs name value...]
  [TIMEOUT ms]
  [FORMAT fmt]
  [LOAD count field...] | LOAD *
```

- **Default fusion: RRF** (Reciprocal Rank Fusion) with `CONSTANT` default 60 and `WINDOW` default 20.
- **LINEAR** = `alpha * text_score + beta * vector_score`.
- **8.4 limitations**: no `EXPLAINSCORE`, no `WITHCURSOR`, no `SHARD_K_RATIO` metrics in `FT.INFO`, post-`COMBINE` `FILTER` not yet supported.

---

## 5. FT.AGGREGATE — pipeline reference

### 5.1 Syntax (8.x)

```
FT.AGGREGATE index query
  [VERBATIM]
  [LOAD count field [AS name] ...  |  LOAD *]
  [GROUPBY count @field [@field ...]
     REDUCE func nargs arg... [AS name] ... ]
  [SORTBY count @field [ASC|DESC] ... [MAX n]] ...
  [APPLY expr AS name] ...
  [FILTER expr] ...
  [LIMIT offset num] ...
  [WITHCURSOR [COUNT read_size] [MAXIDLE ms]]
  [PARAMS nargs name value ...]
  [SCORER name]
  [ADDSCORES]
  [DIALECT dialect]
  [TIMEOUT ms]
```

Pipeline steps can repeat in any order (e.g. multiple GROUPBYs, multiple
SORTBY/APPLY/FILTER). The pipeline is dynamic and re-entrant.

### 5.2 Steps

| Step | Syntax | Purpose |
|---|---|---|
| `LOAD` | `LOAD count id [AS name] ...` or `LOAD *` | Pull fields from source docs into the pipeline. `@__key` = doc id. Avoid if fields are `SORTABLE`. |
| `GROUPBY` | `GROUPBY nargs @p [@p ...] REDUCE ... ` | Group rows; groups emit one row per distinct key tuple. `GROUPBY 0` = single group across all rows. |
| `REDUCE` | `REDUCE func nargs arg... [AS name]` | Aggregate within a group. See §5.3. |
| `SORTBY` | `SORTBY nargs @p [ASC\|DESC] ... [MAX n]` | Sort current pipeline. `MAX` = keep only top-n. |
| `APPLY` | `APPLY expr AS name` | 1:1 transform; can reference earlier-applied names. |
| `FILTER` | `FILTER expr` | Post-query predicate using APPLY-expression syntax plus `== != < <= > >= && \|\| !`. |
| `LIMIT` | `LIMIT offset num` | Page. Capped by `search-max-aggregate-results`. |
| `WITHCURSOR` | `WITHCURSOR [COUNT n] [MAXIDLE ms]` | Returns a cursor id; consume via `FT.CURSOR READ`. Default COUNT 1000, MAXIDLE 300000 ms (capped). |
| `PARAMS` | `PARAMS nargs name value...` | Bind `$name` (DIALECT ≥2). |
| `ADDSCORES` | flag | Exposes the document's FT score to the pipeline as `@__score`. |
| `SCORER` | name | Built-in scorer name. |

### 5.3 REDUCE functions

| Function | Syntax | Notes |
|---|---|---|
| `COUNT` | `REDUCE COUNT 0` | Row count in group. |
| `COUNT_DISTINCT` | `REDUCE COUNT_DISTINCT 1 @p` | Exact distinct count (hash-set per group). |
| `COUNT_DISTINCTISH` | `REDUCE COUNT_DISTINCTISH 1 @p` | Approx via HyperLogLog (~3% error, 1024 B/group). |
| `SUM` | `REDUCE SUM 1 @p` | Sum of numeric values; non-numeric → 0. |
| `MIN` | `REDUCE MIN 1 @p` | Min (string, number, or NULL). |
| `MAX` | `REDUCE MAX 1 @p` | Max. |
| `AVG` | `REDUCE AVG 1 @p` | Mean of numeric. |
| `STDDEV` | `REDUCE STDDEV 1 @p` | Standard deviation. |
| `QUANTILE` | `REDUCE QUANTILE 2 @p q` | Value at quantile `q` ∈ [0,1]. `0.5` = median. |
| `TOLIST` | `REDUCE TOLIST 1 @p` | All distinct values into one array. |
| `FIRST_VALUE` | `REDUCE FIRST_VALUE nargs @p [BY @p [ASC\|DESC]]` | First/top value, optionally by another field. |
| `RANDOM_SAMPLE` | `REDUCE RANDOM_SAMPLE nargs @p sample_size` | Reservoir sampling. |
| `COLLECT` (8.8+) | `REDUCE COLLECT nargs FIELDS (*\|n @f...) [DISTINCT] [SORTBY ...] [LIMIT o c] [AS name]` | Collect docs per group as array of maps; supports `@__key`/`@__score` projection, dedup, sort, limit. |

### 5.4 APPLY / FILTER expression functions

**Arithmetic operators:** `+ - * / % ^` (no bitwise).

**Comparison / logical (FILTER):** `== != < <= > >= && || !`.

**Field functions:**
- `exists(s)` — whether a field is present.
- `case(cond, if_true, if_false)` — ternary; can be nested.

**Numeric functions:**
- `log(x)`, `log2(x)`, `exp(x)`, `sqrt(x)`, `abs(x)`, `ceil(x)`, `floor(x)`.

**String functions:**
- `upper(s)`, `lower(s)`, `strlen(s)`.
- `startswith(s1, s2)` → 1/0.
- `contains(s1, s2)` → occurrence count (or `len(s1)+1` if `s2==""`).
- `substr(s, offset, count)` (negative offset = from end; `-1` count = "rest").
- `format(fmt, args...)` — only `%s` supported.
- `split(s, [sep=","], [strip=" "])` → array.
- `matched_terms([max_terms=100])` → list of matched query terms.

**Date/time functions:**
- `timefmt(x, [fmt])` — format Unix ts; default `%FT%TZ`.
- `parsetime(s, [fmt])` — inverse of `timefmt`.
- `day(ts)`, `hour(ts)`, `minute(ts)`, `month(ts)` — round down to start of period.
- `dayofweek(ts)` (Sun=0), `dayofmonth(ts)` (1–31), `dayofyear(ts)` (0–365).
- `year(ts)`, `monthofyear(ts)` (0–11).

**Geo functions:**
- `geodistance(...)` — distance in meters. 9 calling conventions:
  - `(field, field)`, `(field, "lon,lat")`, `(field, lon, lat)`,
    `("lon,lat", field)`, `("lon,lat", "lon,lat")`, `("lon,lat", lon, lat)`,
    `(lon, lat, field)`, `(lon, lat, "lon,lat")`, `(lon, lat, lon, lat)`.
  - The GEO field must be preloaded with `LOAD`.

### 5.5 Cursor API

```
FT.AGGREGATE idx q ... WITHCURSOR [COUNT n] [MAXIDLE ms]
FT.CURSOR READ idx cid [COUNT n]
FT.CURSOR DEL  idx cid
```

Default `COUNT` 1000, default `MAXIDLE` 300000 ms (cannot exceed). Lazy GC
every 500 ops or 1 second.

---

## 6. DIALECT differences (consolidated)

| Feature | D1 | D2 | D3 | D4 |
|---|---|---|---|---|
| Default | ✅ | | | |
| Field modifier scope (multi-word) | whole phrase | adjacent only | adjacent only | adjacent only |
| `-a b` | `-(a b)` | `-a AND b` | `-a AND b` | `-a AND b` |
| `\|` precedence | lower than space | higher than space | as D2 | as D2 |
| Tag-list spaces | no | yes | yes | yes |
| `==, !=, >, >=, <, <=` numeric ops | no | yes | yes | yes |
| `PARAMS`, `$var` | no | yes | yes | yes |
| Vector KNN | no | yes | yes | yes |
| `ismissing()` | no | yes | yes | yes |
| GEOSHAPE queries | no | no | yes | yes |
| JSON multi-value returns (arrays) | no (first only) | no (first only) | yes (all values) | yes |
| Sort optimizations (skip/partial/hybrid) | no | no | no | yes (`WITHOUTCOUNT` alias) |

**Deprecated in 8.x:** 1, 3, 4. **Target:** DIALECT 2.

---

## 7. Scoring functions

| Name | Notes |
|---|---|
| `BM25STD` | **Default.** Renamed from `BM25` in 8.4 (`BM25` is a deprecated alias). BM25 with slop penalty and document-score multiplier. |
| `BM25STD.NORM` | BM25STD with min–max normalization across the collection. Slower (depends on global stats). |
| `BM25STD.TANH` | BM25STD normalized via `tanh(x/factor)`. Default factor 4; configurable per-query via `BM25STD_TANH_FACTOR Y`. Faster than `.NORM`. |
| `TFIDF` | Classic TF-IDF with field weights, slop penalty, doc-score multiplier. |
| `TFIDF.DOCNORM` | TF-IDF variant normalizing by weighted document length. |
| `DISMAX` | Simple sum of matched term frequencies; union = max. No penalties. |
| `DOCSCORE` | Returns the document's presumptive score as-is. |
| `HAMMING` | `1/(1+d)` of Hamming distance between document payload and query payload. Requires both payloads present and equal length (multiples of 64 bit are slightly faster). |

Invoke via `SCORER <name>` (and `BM25STD_TANH_FACTOR Y` for the TANH variant).

---

## 8. Indexing triggers (which writes reindex)

Driven by **keyspace notifications** subscribed via
`RedisModule_SubscribeToKeyspaceEvents`. Source: `src/notifications.c` in
RediSearch (the same logic is linked into RQE in 8.x).

### 8.1 Hash-indexed writes that trigger an **update**

| Command | Behavior |
|---|---|
| `HSET` | Update indexed fields (only changed fields are reported via command filter). |
| `HMSET` | Same as HSET. |
| `HSETNX` | Update if the field was newly set (no event fired if hash already exists with the field). |
| `HINCRBY` | Update. |
| `HINCRBYFLOAT` | Update. |
| `HDEL` | Update (fields removed). If the hash becomes empty, the key is deleted → handled as delete. |
| `HEXPIRE` / `HPEXPIRE` / `HEXPIREAT` / `HPEXPIREAT` | Update (field-TTL change). |
| `HPERSIST` | Update. |
| `HEXPIRED` (field-level expiration event) | Update (field has expired). |
| `RESTORE` | Update (restoring a hash key). |
| `COPY` (target) | Update the destination. |
| `RENAME` (from + to) | Update both source and destination keys. |
| `EXPIRE` / `PEXPIRE` / `EXPIREAT` / `PEXPIREAT` | Update (the key's TTL changed — re-evaluated against the FILTER / score). |
| `PERSIST` | Update. |
| `CHANGE` (generic module change event) | Update. |
| `LOADED` (RDB load) | Index the key. |
| `TRIMMED`, `KEY_TRIMMED` (stream-ish trimming notifications) | Treated per key type. |

### 8.2 Deletes (remove from index)

| Command | Behavior |
|---|---|
| `DEL`, `UNLINK` | Remove from index. |
| `EXPIRED` (key expired) | Remove. |
| `EVICTED` (key evicted) | Remove. |
| `SET` (overwriting a hash with a string) | The hash is gone → remove from index. |
| `RENAME` (overwriting an existing destination) | The destination's old value is removed. |
| `RESTORE` (with overwrite of a different type) | Old document removed. |

### 8.3 JSON-indexed writes (module notifications on `json.*` events)

`Indexes_UpdateMatchingWithSchemaRules(..., DocumentType_Json, ...)` is called for:

- `JSON.SET`
- `JSON.MERGE`
- `JSON.MSET`
- `JSON.DEL`
- `JSON.NUMINCRBY`
- `JSON.NUMMULTBY`
- `JSON.STRAPPEND`
- `JSON.ARRAPPEND`
- `JSON.ARRINSERT`
- `JSON.ARRPOP`
- `JSON.ARRTRIM`
- `JSON.TOGGLE`

(`JSON.ARRINDEX` is **not** in the trigger list — it's a read-ish op returning an index, not a mutation.)

### 8.4 SKIPINITIALSCAN

- When set on `FT.CREATE` (or `FT.ALTER SCHEMA ADD`), **does not** backfill
  pre-existing keys. Only future writes trigger indexing.
- Without it, a background scan re-indexes every matching key.

### 8.5 Partial hash-field indexing

The module installs a **command filter** that captures the specific field names
passed to `HSET`/`HMSET`/`HSETNX`/`HINCRBY`/`HINCRBYFLOAT`/`HDEL`. Only those
fields are reprocessed (when the index only references a subset), avoiding a
full reindex of the document.

---

## 9. GEOSHAPE — field type and operators

### 9.1 Schema

```
SCHEMA field GEOSHAPE [FLAT | SPHERICAL] [NOINDEX] [INDEXMISSING]
```

- `SPHERICAL` (default): longitude/latitude geographic coordinates.
- `FLAT`: Cartesian X/Y.
- **No `SORTABLE`.**
- WKT values: `POINT(x y)`, `MULTIPOINT((x y),(x y))`, `LINESTRING(x y, x y, ...)`,
  `POLYGON((x y, x y, ..., x y))`.

### 9.2 Query operators (require DIALECT ≥ 3)

| Operator | Since | Meaning |
|---|---|---|
| `WITHIN` | 2.8 | Doc shape is entirely inside the query shape. |
| `CONTAINS` | 2.8 | Doc shape entirely contains the query shape. |
| `INTERSECTS` | 2.10 | Doc shape and query shape share any area. |
| `DISJOINT` | 2.10 | Doc shape and query shape share no area. |

```
@geom:[WITHIN $poly]   with PARAMS supplying WKT
```

---

## 10. Other 8.x-specific additions and behaviors

### 10.1 Configuration parameters (8.x names)

The 8.0 rename pattern: `ALLCAPS_NAME` → `search-kebab-case-name`. Use
`CONFIG GET search-*` / `CONFIG SET`. `FT.CONFIG` is deprecated.

| 8.x name | Default | Purpose |
|---|---|---|
| `search-timeout` | 500 (ms) | Default FT.SEARCH/FT.AGGREGATE timeout. |
| `search-on-timeout` | `fail` in `redis-full.conf` (module default `return`) | `RETURN` = partial results, `FAIL` = error. |
| `search-max-search-results` | 1000000 | Hard cap on `FT.SEARCH` result count. |
| `search-max-aggregate-results` | 2147483648 | Hard cap on `FT.AGGREGATE` result count. |
| `search-max-doc-tablesize` | — | Max documents per index. |
| `search-max-expansions` | — | Max prefix/fuzzy expansions. |
| `search-max-prefix-expansions` | — | (legacy alias) |
| `search-min-prefix` | 2 | Min length for prefix queries. |
| `search-min-stem-len` | — | Min length for stemming. |
| `search-min-phonetic-term-len` | — | Min length for phonetic matching. |
| `search-min-operation-workers` | — | Min worker threads. |
| `search-multi-text-slop` | — | Slop between multi-text fields. |
| `search-no-mem-pools` | — | Disable memory pools. |
| `search-no-gc` | — | Disable GC. |
| `search-partial-indexed-docs` | — | Partial indexing threshold. |
| `search-raw-docid-encoding` | — | Doc-id encoding optimization. |
| `search-threads` | — | Indexing worker threads. |
| `search-tiered-hnsw-buffer-limit` | — | Tiered HNSW buffer. |
| `search-topology-validation-timeout` | — | Cluster topology validation. |
| `search-union-iterator-heap` | — | Union iterator heap size. |
| `search-vss-max-resize` | 0 | Max vector index resize in bytes. |
| `search-default-dialect` | 1 | Default query dialect. |

`UPGRADE_INDEX` has no matching `CONFIG` key.

### 10.2 BM25 default scoring

- Default scorer is **BM25STD** (8.4 rename). The legacy `BM25` name still works
  but is deprecated.
- New variants: `BM25STD.NORM`, `BM25STD.TANH` (with `BM25STD_TANH_FACTOR Y`).

### 10.3 COLLECT reducer (8.8+)

`REDUCE COLLECT ...` returns per-group arrays of projected doc maps, with
optional `DISTINCT`, `SORTBY`, `LIMIT`. This is the "top-N per group" pattern.
See §5.3.

### 10.4 MULTIPLY / weights

- Per-field `WEIGHT` on TEXT (at FT.CREATE) is the classic multiplier.
- Query-time `(...) => { $weight: N; }` per-clause boost.
- `MULTIPLY` is **not** an FT.* option in the public grammar. (If seen in
  third-party docs, it's referring to internal arithmetic or to non-OSS
  tooling.)

### 10.5 INDEXMISSING / INDEXEMPTY

- `INDEXEMPTY` (TEXT, TAG) — index empty strings so they're searchable.
- `INDEXMISSING` (all types) — index absent fields; queryable via `ismissing(@field)`.
- Both require DIALECT ≥ 2 at query time.

### 10.6 MULTIPOINT

- Supported as a WKT input to GEOSHAPE (`MULTIPOINT((x y), ...))`).
- Not a separate field type.

### 10.7 Multi-value TAG

- TAG fields on JSON arrays are split per element (multi-value).
- DIALECT 2+ allows spaces inside `{...}` tag lists.

### 10.8 NESTED fields

- JSONPath-based schema (`$..child.field`) gives effective nested indexing.
- No `NESTED` keyword in the public FT.CREATE grammar.

### 10.9 JSON.ARRINDEX

- Read-only command, not a mutation → **not** an indexing trigger (see §8.3).

### 10.10 PHONETIC

- Per-TEXT-field option: `PHONETIC <dm:en | dm:fr | dm:pt | dm:es>`.
- Matcher does double-metaphone matching at query time.
- Min term length governed by `search-min-phonetic-term-len`.

### 10.11 Binary indexes / BINKEYS

- Not in the public FT.CREATE grammar. (Vector Sets support `BIN` quantization
  via `VADD ... BIN`; the FT.* engine has no equivalent flag — quantization is
  via vector TYPE INT8/UINT8 or SVS-VAMANA COMPRESSION.)

### 10.12 MAXSEARCHRESULTS / MAXAGGREGATERESULTS

- Hard caps via `search-max-search-results` / `search-max-aggregate-results`.
- A `LIMIT` exceeding the cap is rejected or truncated depending on dialect.

### 10.13 ON_TIMEOUT behavior

- `search-on-timeout`: `RETURN` (return partial) or `FAIL` (return error).
- Per-query `TIMEOUT ms` overrides.

### 10.14 Partial FT.SEARCH filters

- The legacy `FILTER numeric_field min max` and `GEOFILTER ...` options still
  parse but are deprecated since 2.6/2.10; the dialect-2 query syntax
  (`@field:[...]`) is the supported path.
- Hash command filter captures per-field names for partial doc re-indexing.

### 10.15 ADDSCORES / ADDPAYLOADS

- `ADDSCORES` (FT.AGGREGATE) exposes the FT score as `@__score`.
- `WITHPAYLOADS` (FT.SEARCH) returns the document payload. There is no
  `ADDPAYLOADS` keyword in the public grammar.

### 10.16 STRUCTFIELDS / GEO_TYPE

- Not in the public FT.CREATE grammar.

### 10.17 ASYNC / synonym_for

- `ASYNC` is not an FT.CREATE option in 8.x.
- `synonym_for` is not a parameter; synonym linkage is via `FT.SYNUPDATE`.

### 10.18 COVERAGE in aggregations

- No `COVERAGE` keyword in FT.AGGREGATE. Coverage-style reporting is via
  `FT.INFO` statistics.

### 10.19 FT.TAGVALS

```
FT.TAGVALS index field_name
```

Returns the distinct tags indexed in a Tag field. **Note:** can be expensive
on large indexes.

### 10.20 FT._LIST

```
FT._LIST  ->  array of all index names
```

Underscore prefix marks it as internal/undocumented but supported.

### 10.21 FT.INFO return shape (8.x)

Categories returned:
- **General**: `index_name`, `index_options`, `index_definition` (`key_type`, `prefixes`, `default_score`), `attributes` (per-field `identifier`, `attribute`, `type`, options), `num_docs`, `max_doc_id`, `num_terms`, `num_records`.
- **Size stats**: `inverted_sz_mb`, `vector_index_sz_mb`, `total_inverted_index_blocks`, `offset_vectors_sz_mb`, `doc_table_size_mb`, `sortable_values_size_mb`, `key_table_size_mb`, `geoshapes_sz_mb`, `records_per_doc_avg`, `bytes_per_record_avg`, `offsets_per_term_avg`, `offset_bits_per_record_avg`, `tag_overhead_sz_mb`, `text_overhead_sz_mb`, `total_index_memory_sz_mb`.
- **Indexing stats**: `hash_indexing_failures`, `total_indexing_time`, `indexing`, `percent_indexed`, `number_of_uses`, `cleaning`.
- **GC stats**: `bytes_collected`, `total_ms_run`, `total_cycles`, `average_cycle_time_ms`, `last_run_time_ms`, `gc_numeric_trees_missed`, `gc_blocks_denied`.
- **Cursor stats**: `global_idle`, `global_total`, `index_capacity`, `index_total`.
- **Dialect stats**: `dialect_1`, `dialect_2`, `dialect_3`, `dialect_4` usage counts.
- **Index errors**: `indexing failures`, `last indexing error`, `last indexing error key` (index-level and per-field).
- **Field statistics** (since 2.x): per-field error counts.

---

## 11. RESP shape cheatsheet (for the Go reimplementation)

| Command | RESP2 reply | RESP3 reply |
|---|---|---|
| `FT.SEARCH` | Array: `[total, k1, [field,val,...], k2, [...], ...]` | Map: `total_results:int`, `results:[{id,extra,...}]`, `attributes:[...]`, `format:str`, `warning:[...]` |
| `FT.AGGREGATE` | Array: row 1 is total (integer placeholder), then each row is `[k,v,k,v,...]` | Map: `attributes`, `format`, `results:[map,...]`, `total_results:int`, `warning` |
| `FT.AGGREGATE ... WITHCURSOR` | 2-element array `[result_array, cursor_id]` | same |
| `FT.INFO` | Array of `k v` pairs | Map |
| `FT.CREATE` / `FT.ALTER` / etc. | `+OK` | `+OK` |
| `FT.EXPLAIN` | bulk string | bulk string |
| `FT.EXPLAINCLI` | array of strings | array of strings |
| `FT._LIST` | array of strings | array of strings |
| `FT.TAGVALS` | array of strings | array of strings |
| `FT.SYNDUMP` | flat array `[term, [gid,...], term, [...], ...]` | flat array |

---

## 12. Gap-check summary for the Godis reimplementation

When auditing `database/rediSearch.go` / `database/rediSearch_synonym.go` in the
Godis repo against upstream 8.x, the highest-value checks are:

1. **Command surface** — ensure all 24 commands in §1.1 are present (plus
   `FT.HYBRID` if 8.4 parity is targeted). The removed commands in §1.3
   should *not* be implemented as public commands.
2. **FT.CREATE option coverage** — every option in §2.2 and every field option
   in §2.4. Specifically: `INDEXEMPTY`, `INDEXMISSING`, `WITHSUFFIXTRIE`,
   `PHONETIC`, `UNF`, `INDEXALL`, `GEOSHAPE [FLAT|SPHERICAL]`.
3. **VECTOR** — three algorithms (`FLAT`, `HNSW`, `SVS-VAMANA`), six numeric
   types (`BFLOAT16 FLOAT16 FLOAT32 FLOAT64 INT8 UINT8`), all `count`-named-arg
   pairs.
4. **Query language** — DIALECT 2 as default target; comparison operators
   (`== != > >= < <=`), `ismissing()`, PARAMS/KNN, GEOSHAPE operators
   (`WITHIN/CONTAINS/INTERSECTS/DISJOINT`), fuzzy up to 3 levels.
5. **FT.AGGREGATE** — every reducer including `COLLECT` (8.8+), every APPLY
   function family (numeric, string, datetime, geo), `ADDSCORES`, cursors.
6. **Scoring** — `BM25STD` default (and `.NORM` / `.TANH` / `TFIDF` /
   `TFIDF.DOCNORM` / `DISMAX` / `DOCSCORE` / `HAMMING`).
7. **Indexing triggers** — match the `src/notifications.c` list in §8 exactly,
   including hash-field-TTL events (`hexpire`, `hexpired`, `hpersist`) and the
   full `json.*` event list. Partial field re-indexing via a command filter on
   `HSET`/`HDEL`/etc.
8. **Config** — `search-*` kebab-case config keys; `search-on-timeout` =
   `RETURN|FAIL`; `search-max-search-results` / `search-max-aggregate-results`.
9. **Dialect deprecation** — emit deprecation guidance for 1/3/4.
10. **RESP3** — Map replies for SEARCH/AGGREGATE/INFO.

> End of reference.
