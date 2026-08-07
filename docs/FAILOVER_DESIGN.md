# FAILOVER 命令设计(Redis 8 兼容)

> 现状: `database/failover.go` 已实现真实协调切换(Phase 1+2 完成,
> 见下文 §6 实施状态),本设计文档为决策依据。

---

## 1. Redis FAILOVER 语义(目标)

```
FAILOVER [TO host port [FORCE]] [ABORT] [TIMEOUT milliseconds]
```

- **TO**: 指定目标从库;缺省时主库自动选择"数据最接近"的从库
- **FORCE**: 即使目标从库不在线/落后也切换(跳过一致性等待)
- **TIMEOUT**: 等待目标从库同步的最长时间(默认 60s)
- **ABORT**: 中止进行中的 failover

主库视角流程:
1. 校验/选择目标从库
2. 暂停主库写(内部 pause,可配置)
3. 等待目标从库 REPLCONF ACK offset 追上主库
4. 通知目标从库提升(`REPLICAOF NO ONE` 语义)
5. 主库降级为目标从库的从库

---

## 2. godis 复制架构关键点(设计依据)

| 组件 | 位置 | 作用 |
|---|---|---|
| `masterStatus` | replication_master.go:146 | `onlineSlaves map[conn]*slaveClient`、`backlog`、`waitSlaves` |
| `replBacklog` | replication_master.go:67 | 环形缓冲,AOF 监听驱动追加命令字节 |
| 从库 ACK | replication_master.go(REPLCONF GETACK/ack) | 主库获知从库已处理到哪个 offset |
| `slaveStatus` | replication_slave.go:33 | `masterConn`、`masterChan`、`replOffset`、`configVersion` |
| `receiveAOF` | replication_slave.go:385 | 主库流每条命令 `server.Exec(conn, args)` 执行 |
| `slaveOfNone` | replication_slave.go:81 | 从库提升:清 master、role=masterRole |
| `execSlaveOf` | replication_slave.go:60 | 主库降级:设 master、role=slaveRole、setupMaster |

**关键洞察**:从库对主库复制流中的命令**直接执行**(receiveAOF:408),
主库可以往 backlog 注入命令驱动远端从库动作——无需新 TCP 通道。

---

## 3. 设计:状态机

### 状态

```
failoverIdle → failoverWaitingSync → failoverPromoting → failoverDone
                    ↑                       |
                    └──── abort/fail ───────┘
```

| 状态 | 含义 |
|---|---|
| idle | 无进行中的 failover |
| waitingSync | 主库已选目标,等待其 ACK offset 追上 |
| promoting | 已注入提升指令,等待确认 |
| done | 切换完成(或 abort 回退) |

### 全局状态(failover.go 扩展)

```go
type failoverPlan struct {
    mu         sync.Mutex
    state      int32
    targetHost string
    targetPort int
    token      string   // 随机 token,防伪注入
    deadline   time.Time
    abort      atomic.Bool
}
```

---

## 4. 消息流(主库发起 FAILOVER)

```
[新主/原主]                    [目标从库]                      [观察者]
     │ FAILOVER [TO h p]          │                                │
     ├─ 校验: role==master、从库存在(TO 指定或选 offset 最新)
     ├─ 生成 token, state=waitingSync, 记录 deadline
     ├─ 暂停写(可配置; 默认不停, 依赖 TIMEOUT 语义)
     ├─ 轮询 REPLCONF GETACK → 目标 ACK offset >= 主库当前 offset
     │   (force 时跳过等待)
     ├─ 注入: backlog.appendBytes(
     │      REPLCONF FAILOVER <token>        )
     ├─ state=promoting, 等待目标确认(下述)
     │                                  ┌─ receiveAOF 解析到 REPLCONF FAILOVER
     │                                  ├─ 校验 token(与主库协商?见 §5 安全)
     │                                  ├─ slaveOfNone() → 从库变主
     │                                  └─ 回复 +OK(经复制流反向?不行→见 §5)
     ├─ 主库降级: execSlaveOf(targetHost, targetPort)
     │   → setupMaster() 连接新主做增量/全量同步
     └─ state=done
```

### 目标确认问题(核心难点)

从库执行提升后,主库如何知道成功?复制流是单向(主→从),从库回复只能:
1. **REPLCONF ACK 带特殊标记**:主库轮询 GETACK 时,从库在 ack 中附
   `failover:<token>` 表示"已提升"。修改 ack 解析(主库 masterStatus 收到
   REPLCONF ACK <offset> 的地方识别额外参数)。
2. **轮询连接状态**:主库观察目标从库的 slaveMap 条目消失(提升后从库断开
   主库连接),视为成功。

推荐 **方案 2 + 超时兜底**:提升指令注入后,主库轮询目标从库是否断连
(从库 slaveOfNone 会关 masterConn),断连即成功;超时(默认 10s)则 abort
回退。方案 1 作为可选的快速确认增强。

---

## 5. 安全:注入指令防伪

主库向复制流注入 `REPLCONF FAILOVER <token>`。攻击面:任何能写复制流的人
都能让从库提升。缓解:
- 复制流本身受 AOF/backlog 驱动,只有主库能写(内部信任边界)
- token 随机 128-bit,注入前主库通过 REPLCONF GETACK 路径预先告知?——双向
  不可行。**接受复制流信任模型**:与 Redis 相同(Redis 主库向从库注入
  `REPLCONF` 类指令,信任复制链路)。
- 从库收到 REPLCONF FAILOVER 时,仅在**自己确实是该主库的从库**时执行
  (masterConn 身份已由连接建立时确认)。

---

## 6. 分阶段实施

### Phase 1:本地协调(单进程内,无协议扩展)

- `FAILOVER TO <host> <port>`:校验参数、确认从库在线(TO 或选 offset 最新)
- 轮询目标从库 ACK(复用现有 REPLCONF GETACK 机制,`masterStatus` 已跟踪
  ack offset)直到追上或 TIMEOUT
- **不做真实跨进程提升**,仅完成"等待同步"阶段并返回 OK(比空壳多一层
  真实校验)——价值:让运维脚本的 TIMEOUT/等待语义先落地

### Phase 2:复制流注入(核心) ✅ 已完成

- 主库注入 `REPLCONF FAILOVER <token>` 到 backlog
- 从库 `receiveAOF` 识别该命令:`slaveOfNone()` 提升,关闭 masterConn
- 主库轮询目标从库断连(或 ACK 标记)确认,超时 abort
- 主库执行 `execSlaveOf(target)` 降级
- `ABORT`:waitingSync 阶段可中止;promoting 阶段等待超时后回退

### Phase 3:完善(部分完成)

- ✅ `FORCE`:跳过 offset 等待(从库落后也切)——已实现(`TestFailoverForceSwitchesLaggedReplica`)
- ✅ 默认选从策略:offset 最大者(在线从库中)——已实现(pickFailoverTarget)
- ✅ ABORT 状态机:无进行中报 `ERR No failover in progress.`;进行中时置 idle
  并中断等待循环(并发 FAILOVER 报 `ERR A failover is already in progress.`)
- ✅ 错误文本对齐 Redis:`FAILOVER can only be executed by the master`、
  `FAILOVER requires connected replicas.`、`A failover is already in progress.`、
  `No failover in progress.`(注:Redis 实际文本是 "requires connected
  replicas",早期记录 "No connected replicas" 有误)
- ✅ 提升后 ROLE / INFO replication 正确反映:两者均基于 role atomic
  (slaveOfNone 提升 / execSlaveOf 降级已更新),集成测试断言
  新主 role:master、原主 role:slave
- ⬜ 主库写暂停(pauseClients 复用 CLIENT PAUSE 机制,默认关闭)——明确不做

---

## 7. 风险与回退

| 风险 | 缓解 |
|---|---|
| 从库提升时仍有未同步数据(非 force) | Phase 2 强制等待 ACK == 主库 offset |
| 提升指令丢失(backlog 裁剪) | 从库提升前主库先停写(Phase 3);注入后重试一次 |
| 主库降级连接失败 | 原主保留 master 角色?不——Redis 语义:降级失败则原主保持独立主,数据短暂分叉,由运维介入;godis 记录错误并保持原角色 |
| 时钟/offset 漂移 | 完全以 ACK offset 为准,不用墙钟判断同步完成 |
| 测试影响 | 新增测试全部用 `getTestServer` 独立实例 + 两个 Server 实例搭建主从 |

---

## 8. 测试计划

1. **Phase 1 单测**:无从库报错;有从库时 TIMEOUT 语义(从库不 ACK → 超时)
2. **Phase 2 集成** ✅:两 Server 实例(主+从)搭建主从(REPLICAOF)→ 主库
   FAILOVER → 从库提升为新主(ROLE=master)、原主变为从、数据通过全量/
   增量同步一致(`TestFailoverPromotesReplica`)
3. ABORT:启动 failover 后立即 ABORT,状态回 idle,无角色变化
4. FORCE:落后从库强制切换成功
5. 注入防伪:非 REPLCONF FAILOVER 命令不受影响;错误 token 拒绝

---

## 9. 验收标准

- `FAILOVER` 从"假成功"变为真实角色切换,主从数据最终一致
- 与 Redis 行为对齐:参数校验、TIMEOUT/ABORT/FORCE 语义、错误文本
- 不影响正常复制(无 failover 时零开销)

---

## 10. 实施记录(2026-08-07)

- `failover.go` 重写:TO/FORCE/TIMEOUT/ABORT 解析、目标选择(TO 或 offset 最大)、
  waitSlaveInSync(ACK offset 轮询)、注入 `REPLCONF FAILOVER <token>`、
  waitSlaveGone(主动写 ping 检测断连)、主库 `execSlaveOf` 降级
- `replication_slave.go`:receiveAOF 识别 REPLCONF FAILOVER → 异步 slaveOfNone
  提升;**顺带修复既有自死锁 bug**(receiveAOF 持 slaveStatus.mutex 时 Exec,
  GETACK 分支再 Lock 同一把锁 → 复制首次 GETACK 即冻结);Exec 移出锁外
- `replication_master.go`:execReplConf 提前建 slaveClient(REPLCONF announce
  在 PSYNC 之前到达,旧代码丢弃 announce 信息);masterSendUpdatesToSlave
  边界保护(负/溢出 offset 防切片 panic,从库断连于 FAILOVER 时触发);
  **saveForReplication 未关闭 TempFile 句柄**(Windows 上 rename Access
  denied → 全量同步失败,主库无法为新从库提供 RDB)
- 测试:TestFailoverPromotesReplica(真实 TCP 主从两实例,全流程角色互换+数据
  一致)、TestFailoverRequiresReplicas(无从库报错+ABORT 状态机)、
  TestFailoverForceSwitchesLaggedReplica(落后从库 FORCE 切换)
