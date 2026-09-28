# 集群意图代次与命令确认机制 - 产品需求文档

## Overview
- **Summary**: 为 Locust 分布式（master-worker）与本地运行引入单调递增的"集群意图代次（generation）"、逐节点命令确认（ack）与 start/stop 串行化，保证在 Web UI、无头运行、负载形状（LoadTestShape）连续发出调载/停止命令，以及网络延迟、节点重连、RPC 连接重置等情况下，旧命令不能覆盖新状态，每次操作有可确认的集群结果，且 test_start/test_stopping/test_stop 等生命周期在每个测试代次中恰好触发一次。
- **Purpose**: 消除现有 fire-and-forget 命令协议在连续调载与重连场景下的三类缺陷：(1) 仅 `spawn` 携带浮点时间戳、`stop`/`quit` 无序号，过期命令可覆盖新意图；(2) 操作发起方无法确认集群是否真正收敛；(3) `start()`/`stop()` 无串行化，并发调用可重复触发生命周期、产生交叉派发。
- **Target Users**: 在分布式模式下使用 Web UI、`--headless`/`--autostart`、自定义 LoadTestShape 或键盘交互频繁调整负载的 Locust 用户；以及依赖 `test_start`/`test_stop` 事件做环境准备/清理的扩展开发者。

## Goals
- 所有由 master 发往 worker 的状态变更命令（spawn/stop/quit）携带 master 分配的全序单调整数代次；worker 只执行所见最高代次的意图，丢弃任何过期命令。
- worker 对每个应用完成的命令回带代次确认；master 按代次逐节点追踪确认情况，为每次操作提供可等待、可查询的集群结果（已确认/未确认/缺失节点/超时/被取代）。
- worker 重连（含 master 重置 RPC socket、worker 重置连接、重复 `client_ready`）后与 master 当前意图自动对账并收敛，重发幂等，不依赖命令恰好按序到达。
- master 与 worker（含 LocalRunner）的 start/stop 串行化、幂等；同一测试代次内 `test_start`、`test_stopping`、`test_stop` 在每个节点上恰好触发一次；`quit` 为最高水位意图，quit 后旧命令一律无效。
- 所有操作入口（Web `/swarm`、`/stop`、headless autostart/`--run-time`、shape tick、键盘 w/W/s/S）走同一套代次与确认路径。

## Non-Goals
- 不改造 Web UI 前端（React/TS），不新增前端可视化；确认结果以后端等待、事件与日志方式暴露。
- 不保证每一条中间调载命令都被逐字执行：最新意图为准，中间代次允许被跳过/取代。
- 不支持跨 master 重启的意图持久化与重放；master 重启视为新集群会话。
- 不改造 stats 聚合协议、日志上报协议与 `register_message` 自定义消息协议（自定义消息不纳入代次）。
- 不做磁盘级命令审计日志。

## Background & Context
- 现状代码位置：
  - [MasterRunner.start](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/locust/runners.py#L738-L849)：`spawn` 消息 data 含 `timestamp=time.time()`；派发后仅以 worker 周期上报用户数做 1 秒尽力等待（`_wait_for_workers_report_after_ramp_up`）。
  - [WorkerRunner.handle_message "spawn"](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/locust/runners.py#L1374-L1414)：仅按 `last_received_spawn_timestamp` 丢弃更旧/同值 spawn；快速连续调用时间戳相等会误丢合法 spawn；`stop`/`quit` 无任何序号（[stop 分支](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/locust/runners.py#L1415-L1423)）。
  - [MasterRunner.stop](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/locust/runners.py#L875-L908)：仅以 `state not in [INIT, STOPPED, STOPPING]` 为门槛，check-then-set 非原子；靠 3 秒周期 stats 轮询 `user_count != 0` 等待停止。
  - 并发调用 start 的入口至少有：shape_worker（[shape_worker](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/locust/runners.py#L339-L376)）、Web `/swarm` 绿程（[web.py](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/locust/web.py#L345)）、心跳判死后重平衡（[heartbeat_worker](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/locust/runners.py#L944-L964)）、worker 重连/退出触发的再平衡（[handle_message](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/locust/runners.py#L1036-L1050)）、键盘输入（[main.py](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/locust/main.py#L594-L613)）。
  - 重连路径：master [reset_connection](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/locust/runners.py#L966-L974) 重绑 ROUTER；worker [reset_connection](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/locust/runners.py#L1349-L1355) 重建 DEALER 后重发 `client_ready`；master 收到重复 `client_ready` 会以全新 `WorkerNode` 覆盖运行态并可能再次触发 `start()`。
  - 线协议：[Message](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/locust/rpc/protocol.py#L32-L47) 序列化为 `(type, data, node_id)` 的 msgpack 三元组；ZMQ ROUTER/DEALER 仅保证单连接内 FIFO，跨重置/重连无顺序保证。

## Functional Requirements

### 意图代次
- **FR-1**: master 维护一个进程内单调递增整数代次计数器。仅当"意图发生变化"（新的 spawn 目标、stop、quit）时递增；重平衡、重连补发、重发相同目标复用当前代次，不产生新代次。
- **FR-2**: 每条发往 worker 的 `spawn`/`stop`/`quit` 命令在消息 data 中携带该代次；`spawn` 不再依赖浮点 `timestamp` 判定新旧（时间戳字段可保留用于日志/兼容，但排序以代次为准）。
- **FR-3**: worker 记录已见/已应用的最高代次；收到代次低于该水位的任何状态命令（含旧 spawn、旧 stop、旧 quit）直接丢弃，不改变用户数、不改变 worker_state、不触发任何生命周期事件；丢弃时回传一个带"当前水位代次"的过期反馈，master 据此立即向该 worker 补发当前意图（快速对账，不必等待重连）。
- **FR-4**: 不同命令类型共享同一全序代次：旧 stop 不能停止新一轮 spawn 的用户；旧 spawn 不能在 stop/quit 后复活用户。
- **FR-5**: 同一代次的相同命令重复到达时幂等：worker 重算 spawn/stop delta 为 0，不重复触发任何生命周期事件，并正常回确认。

### 确认与集群结果
- **FR-6**: worker 在命令"应用完成"后回传与代次关联的确认（在现有 `spawning`/`spawning_complete`/`client_stopped` 语义上演进携带代次，或统一确认消息；二者择一，以不新增每用户/高频消息为准），内容至少含代次、应用结果（当前各类用户数或 stopped 状态）。
- **FR-7**: master 为每个代次维护"应确认 worker 集合"与"已确认 worker 集合"，并暴露一个操作结果对象，支持：非阻塞查询与有界等待；状态至少包含 `pending`（进行中）、`confirmed`（全部应确认节点已确认最新意图）、`timeout`（超时且存在缺失节点）、`superseded`（被更新代次取代）；结果可枚举已确认与缺失的 worker id。
- **FR-8**: `MasterRunner.start()`/`stop()` 发出命令后在有界超时内等待集群对当前（最新）代次确认；等待期间 worker 重连/判死引起集合变化时正确增删：新连接节点纳入当前代次确认，判死移除的节点从集合移除；缺失节点导致超时时结果为 `timeout` 且日志明确列出未确认节点。
- **FR-9**: 当代次被更新意图取代时，旧代次的等待者被解除阻塞并得到 `superseded` 结果（含取代发生时的确认快照）；集群只需要对最新代次达成 `confirmed`。
- **FR-10**: 新增一个事件（命名沿用以 `test_*`/`worker_*` 风格，如 `cluster_command_completed`），在代次结果进入终态（confirmed/timeout/superseded）时触发一次，载荷含代次、意图摘要、结果状态与缺失节点，供扩展者订阅；不改变现有事件的载荷与触发时机（新增触发保证除外）。
- **FR-11**: 本地模式（LocalRunner）提供同构的结果对象：无网络节点，start/stop 应用即 `confirmed`；其事件订阅者无需区分本地/分布式即可获得一致语义。

### 重连对账
- **FR-12**: worker 发起（重复）`client_ready` 时携带自己已见的最高代次；master 的 `ack` 回复携带 master 当前代次与当前意图类别（运行中目标 / 已停止 / 退出中）。
- **FR-13**: worker 落后于 master 当前代次时，master 将其加入当前代次的应确认集合并幂等重发当前意图；worker 据此收敛：在跑且目标相同则 delta 为 0，已停止而意图为运行则按目标拉起，意图为停止则保持空闲。
- **FR-14**: master 收到重复 `client_ready` 时不得以全新 INIT 节点无条件覆盖正在运行的节点状态：节点身份、代次水位、已确认信息须保留或立即以 worker 上报对账校正；重复就绪消息本身不得触发额外的全集群 `start()` 重平衡（重平衡只由缺失/新增等真实拓扑变化在当前代次下驱动）。
- **FR-15**: master 重置 RPC socket 后，worker 侧可能残留的重置前旧命令在处理时因代次过期被丢弃；集群在重连对账后收敛到重置前的最新意图（运行目标或停止态）。

### 串行化与生命周期幂等
- **FR-16**: master 的意图变更串行化：并发 `start()` 调用按顺序合并——目标与当前意图相同则复用当前代次与同一结果对象；目标更新则创建新代次并取代旧代次；进行中的派发可被安全取代，在途旧代次 spawn 到达 worker 后以新代次为准。串行化不得阻塞 `client_listener` 与心跳绿程处理消息。
- **FR-17**: `stop()` 在 master、worker、LocalRunner 上幂等：已在停止中/已停止时重复或并发调用返回同一结果，不重复发送停止命令，`test_stopping`/`test_stop` 每个测试代次各触发一次。
- **FR-18**: `test_start` 每个"运行代次"在 master 与每个 worker 上恰好触发一次：快速 start→stop→start 序列产生两个运行代次、各一次；同代次重发、重连补发、并发 start 不重复触发；stop 收敛时 `test_stopping`/`test_stop` 恰好一次。
- **FR-19**: `quit` 携带代次且为最高水位意图：worker 执行 quit 后，任何后到的旧 spawn/stop 被丢弃；master 侧 quit 与并发 start/stop 互斥有序，不出现 quit 后重新拉起用户。
- **FR-20**: shape_worker 的阻塞式 `start()` 与外部 stop/swarm 并发时，由同一串行化机制保证最终态为最新调用方意图；shape 返回 None 触发的 stop 与手动 stop 共享幂等结果。

## Non-Functional Requirements
- **NFR-1（并发安全）**: 全部基于 gevent 同步原语实现；命令等待不得死锁，心跳、消息接收、stats 上报绿程在集群有未确认代次时仍保持正常节拍。
- **NFR-2（性能）**: 不引入每用户或亚秒级额外消息；ack 复用现有 spawn/stop 完成消息流；10 worker 规模下连续调载（每 1 秒一次 tick）的消息增量可忽略，现有派发吞吐不回退（以现有测试与 benchmarks/dispatch.py 为佐证）。
- **NFR-3（协议兼容）**: Message 三元组结构与 msgpack 编解码不变，代次置于 data 字典；同集群要求版本一致——不带代次能力的旧版 worker 连接时 master 明确拒绝并输出告警（沿用现有拒绝 pre-2.0 worker 的处理位置）。
- **NFR-4（API 兼容）**: `Environment.create_*_runner`、`runner.start/stop/quit` 签名保持兼容；结果对象与事件为新增能力，不破坏既有 locustfile 与扩展。
- **NFR-5（可观测）**: 代次推进、命令丢弃（收到过期代次）、确认超时与缺失节点、重连对账均有 INFO/DEBUG/WARNING 级日志。
- **NFR-6（可测）**: 核心逻辑可用现有 `mocked_rpc` 做单元测试；端到端行为用现有 loopback ZMQ master+worker 集成测试方式覆盖；遵循 pytest、ruff（行宽 120）、mypy 约束。

## Constraints
- **Technical**: Python 3.11+，gevent 协作式并发；ZMQ ROUTER/DEALER + msgpack；不允许引入新第三方依赖；遵循 [AGENTS.md](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/AGENTS.md) 的代码风格与测试约定。
- **Business**: 开源框架改动需保持向后兼容的公共 API 与事件语义；线协议仅允许在 data 内增量扩展。
- **Dependencies**: 依赖现有 gevent Event/锁、现有消息类型与 [UsersDispatcher](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/locust/dispatch.py)。

## Assumptions
- 用户在澄清问题中选择/默认接受：
  1. 确认结果以后端收敛等待 + 新事件 + 日志呈现，不做 Web 前端改造，但 `/swarm`、`/stop` 等 HTTP 入口的成功语义建立在后端收敛路径之上；
  2. 最新意图为准，中间代次可被跳过，被取代操作仍得到可查询的 `superseded` 结果；
  3. 要求集群同版本升级，不做旧 worker 优雅降级；
  4. 改动覆盖分布式与 LocalRunner 的生命周期串行化/幂等。
- master 为单一权威命令源（现有架构即如此），代次无需跨时钟同步，整数全序替代时间戳。
- 等待集群确认的默认超时可复用现有环境变量风格（如 `LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP`、`stop_timeout + 60`）并允许配置。

## Acceptance Criteria

### AC-1: 过期状态命令在 worker 侧被丢弃
- **Type**: `rule`
- **Given**: worker 已应用代次 N 的意图（spawn 或 stop）
- **When**: worker 收到代次低于 N 的 spawn、stop 或 quit（模拟网络延迟/重连后旧命令到达）
- **Then**: 用户数与 worker_state 不发生变化，不触发 test_start/test_stopping/test_stop，日志记录丢弃，且 worker 向 master 回传其当前水位代次
- **Pass Condition**: 单元测试（mocked rpc 直接投递消息）对三类旧命令均断言无副作用且发出过期反馈
- **Evidence**: `locust/test/test_runners.py` 新增测试通过

### AC-2: 连续不同目标调载后集群收敛到最新意图
- **Type**: `rule`
- **Given**: loopback ZMQ 连接的 1 master + 2 worker
- **When**: 连续调用 start(10)→start(25)→start(7)（不等待全部完成即发下一个），等待最新代次结果
- **Then**: 最新代次结果为 `confirmed`，master user_count 与每 worker 上报用户数最终等于 7 的派发目标，中间代次结果为 `superseded`
- **Pass Condition**: 集成测试断言最终计数与代次结果状态
- **Evidence**: `locust/test/test_runners.py` 新增集成测试通过

### AC-3: 同代次重发幂等
- **Type**: `rule`
- **Given**: worker 已确认代次 N 的 spawn 目标
- **When**: master 因重连/重平衡向其重发代次 N 的相同目标
- **Then**: spawn/stop delta 为 0，用户数不变，test_start/test_stop 不额外触发，worker 再次回传代次 N 确认
- **Pass Condition**: 单元测试断言生命周期计数器不变且确认正常
- **Evidence**: `locust/test/test_runners.py` 新增测试通过

### AC-4: 重连对账使落后节点收敛且不引发重平衡风暴
- **Type**: `rule`
- **Given**: 集群以代次 N 运行中，某 worker 模拟连接重置后以相同 client_id 重发 `client_ready`（携带旧水位）
- **When**: master 完成握手与对账
- **Then**: ack 含当前代次与意图；该 worker 被纳入代次 N 应确认集合并收到幂等补发；重复 client_ready 不创建额外全集群 start 派发、不把运行节点错误重置为 INIT；集群最终 `confirmed`
- **Pass Condition**: 集成测试断言 outbox 中无额外全集群 spawn、节点状态/计数正确、结果 confirmed
- **Evidence**: `locust/test/test_runners.py` 新增测试通过

### AC-5: 每次操作有可等待的集群结果，超时可报告缺失节点
- **Type**: `rule`
- **Given**: master 已连接若干 worker，其中一个被模拟为不回传确认
- **When**: 调用 start() 并等待操作结果（有界超时）
- **Then**: 其他节点确认后结果在超时点变为 `timeout`，结果对象可枚举缺失 worker id 且 master 日志 WARNING 列出；正常全确认时结果为 `confirmed`；结果支持非阻塞查询
- **Pass Condition**: 单元/集成测试分别断言 confirmed、timeout、缺失列表与查询/等待两种用法
- **Evidence**: `locust/test/test_runners.py` 新增测试通过

### AC-6: 生命周期在快速序列与并发调用下每个运行代次恰好一次
- **Type**: `rule`
- **Given**: loopback 集群（事件计数器挂在 master 与每个 worker 的 test_start/test_stopping/test_stop）
- **When**: 多绿程并发 start、随后快速 start→stop→start，以及 shape tick 与 stop 并发
- **Then**: 每个运行代次在每个节点上 test_start 恰好 1 次、对应停止时 test_stopping/test_stop 各恰好 1 次；无重复、无遗漏
- **Pass Condition**: 集成测试按代次与节点断言事件次数
- **Evidence**: `locust/test/test_runners.py` 新增测试通过

### AC-7: stop/quit 幂等且 quit 为最高水位
- **Type**: `rule`
- **Given**: 运行中的集群/worker
- **When**: 并发或重复调用 stop()；以及 worker 应用 quit 后再投递旧代次 spawn/stop
- **Then**: master 仅发出一组当前代次 stop 命令、事件一次、所有调用返回同一结果；quit 后旧命令被丢弃、用户不被重新拉起
- **Pass Condition**: 测试断言 outbox 命令数、事件计数与 quit 后用户数恒为 0
- **Evidence**: `locust/test/test_runners.py` 新增测试通过

### AC-8: 所有操作入口走统一代次/确认路径
- **Type**: `rule`
- **Given**: Web UI、headless（autostart 与 --run-time）、LoadTestShape、键盘输入四类入口
- **When**: 分别发起调载/停止
- **Then**: 代码路径统一经过代次分配、串行化与结果对象；HTTP /swarm 与 /stop 在集群模式下返回前命令已被受理（最新代次已派发），停止路径具备集群确认/超时行为
- **Pass Condition**: 代码审查 + 现有 web/headless/shape 测试通过（必要时新增断言）
- **Evidence**: `locust/test/test_runners.py`、`locust/test/test_web.py`、`locust/test/test_main.py` 测试结果

### AC-9: 协议增量且同版本强制
- **Type**: `rule`
- **Given**: Message 编解码与新旧 worker 握手
- **When**: 检查线协议；模拟无代次能力的旧版 worker 发送 client_ready
- **Then**: Message 仍为 (type, data, node_id)，代次仅在 data 内；master 明确拒绝旧 worker 并输出告警日志
- **Pass Condition**: 协议测试与握手分支测试通过
- **Evidence**: `locust/test/test_runners.py`（含现有 pre-2.0 拒绝测试）通过

### AC-10: LocalRunner 串行化与生命周期幂等
- **Type**: `rule`
- **Given**: LocalRunner 上挂载事件计数
- **When**: 并发/重复 start 与 stop
- **Then**: 用户数最终为最新目标或 0，test_start/test_stopping/test_stop 每个运行代次各一次，结果对象即时 `confirmed`
- **Pass Condition**: `locust/test/test_runners.py` 本地 runner 测试通过
- **Evidence**: 测试输出

### AC-11: 设计与现有架构的融合度与可维护性
- **Type**: `rubric`
- **Dimension**: 代次/确认/串行化抽象与现有 runners.py、dispatch.py、event.py、rpc 协议的契合程度（命名、职责边界、复用现有消息与事件风格）
- **Scale**: 1-5
- **Anchors**: 1 = 另起炉灶、大面积侵入或重复状态机；3 = 可用但抽象边界模糊、存在并行真相源；5 = 抽象最小且内聚，复用现有消息流与事件风格，状态单一真相源，文档/注释充分
- **Pass Threshold**: >= 4
- **Evidence**: 独立代码审查

### AC-12: 并发健壮性与性能不回退
- **Type**: `rubric`
- **Dimension**: 无死锁/无饥饿（心跳与接收不被阻塞）、连续调载下的消息与 CPU 开销
- **Scale**: 1-5
- **Anchors**: 1 = 存在死锁或心跳停摆；3 = 偶发竞争或消息明显增多；5 = 全部并发测试稳定、消息复用现有节奏、benchmarks/dispatch.py 无回退
- **Pass Threshold**: >= 4
- **Evidence**: 压力式集成测试（连续 tick、kill 绿程、重连）重复运行结果与现有测试套件全绿

### AC-13: 质量门禁全绿
- **Type**: `rule`
- **Given**: 改动完成
- **When**: 运行 `uv run pytest locust/test`、`uv run ruff check locust/`、`uv run mypy locust/`
- **Then**: 全部通过，无新增告警
- **Pass Condition**: 三条命令退出码均为 0
- **Evidence**: 命令输出

## Open Questions
- [x] 确认结果的呈现形态：后端收敛等待 + 新事件 + 日志（默认，不做前端）。
- [x] 中间意图语义：最新意图为准，旧代次 `superseded`（默认）。
- [x] 混布兼容：要求同版本，旧 worker 拒绝并告警（默认）。
- [x] 范围：分布式 + LocalRunner 生命周期（默认）。
- [ ] 确认等待默认超时的具体取值与环境变量命名——实现阶段按现有 `stop_timeout + 60` 与 `LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP` 风格确定，属实现细节，不阻塞需求。
