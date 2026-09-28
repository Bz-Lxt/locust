# 集群意图代次与命令确认机制 - 实现计划

实现总体思路（任务内细节，不属于 spec 需求）：

- 在 [runners.py](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/locust/runners.py) 内新增两个内聚原语：
  - `ClusterCommandResult`：一次意图操作的可等待结果（generation、意图摘要、应确认/已确认 worker 集合、pending/confirmed/timeout/superseded 状态、gevent Event、缺失节点快照）。
  - master 侧代次账本：单调整数代次、当前意图（运行目标/停止/退出）、每个代次的结果对象与 worker 水位观测。
- worker 侧以"已应用代次 + 该代次已应用目标指纹"为水位：`gen` 更新必应用；`gen` 相同但目标变化（同代次重平衡再分发）也应用；`gen` 相同且目标相同为幂等 no-op；`gen` 更旧一律丢弃。
- ack 复用现有消息流并携带代次：`spawning_complete`（worker 已完成本次派发）与 `client_stopped`（worker 已停止）携带 `generation`；新增 worker→master 的 `command_stale` 反馈（含 worker 当前水位），master 收到后立即在当前代次下对该 worker 补发当前意图。
- `client_ready` 由裸版本字符串扩展为 `{"version": ..., "generation": <水位>}`；`ack` 携带 `{"index", "generation", "intent", ...}`；不识别该结构的旧 worker 按现有 pre-2.0 拒绝路径拒绝并告警。

## Task 1: 集群代次账本与操作结果原语（ClusterCommandResult / ClusterCommandLedger）+ 单测
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 在 runners.py 中实现 `ClusterCommandResult`（或等价命名）：字段含 generation、意图类型与目标摘要、expected/acked worker 集合、状态机（pending→confirmed/timeout/superseded）、`wait(timeout)` 与非阻塞查询、终态一次性回调/事件钩子、缺失节点快照；终态只允许转换一次。
  - 实现 master 侧代次账本逻辑（与 MasterRunner 解耦的纯 Python 小类，便于单测）：`new_generation(intent)`（仅意图变化时递增，返回新结果；相同意图复用当前结果）、`observe(worker_id, generation, payload)`（归集 ack、识别 stale 反馈、满足全部 expected 时置 confirmed）、`expect_worker/remove_worker`（连接与判死时维护集合）、`supersede(new_result)`（旧结果带确认快照转 superseded 并唤醒等待者）、超时扫描/取消调度。
  - 不引入新第三方依赖；仅用 gevent.event.Event / gevent.lock。
- **Acceptance Criteria Addressed**: AC-5, AC-11
- **Test Requirements**:
  - `rule` TR-1.1: 单测构造账本，模拟全部 ack 后结果为 confirmed 且 wait() 返回；缺少一个 ack 超时时为 timeout 且缺失列表准确；新代次产生后旧结果为 superseded 且快照含当时已确认集合——证据：`locust/test/test_runners.py` 新测试通过。
  - `rule` TR-1.2: 相同意图不递增代次、复用同一结果对象；不同意图严格递增——证据：单测断言 generation 序列与对象身份。
  - `rule` TR-1.3: 终态转换幂等（重复 confirm/timeout 不改变状态、不重复回调）——证据：单测断言。
- **Notes**: 不触碰 ZMQ 与现有 runner 流程；纯逻辑先行。

## Task 2: 线协议扩展（generation 字段、client_ready/ack 握手、command_stale、旧版拒绝）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - `spawn`/`stop`/`quit` 消息 data 统一携带 `generation`（spawn/quit 现有 data 为 dict/None：stop/quit 改为 `{"generation": n}`，接收端兼容读取）。
  - worker `client_ready` 改为 `{"version": __version__, "generation": <水位>}`；master `ack` 改为 `{"index", "generation", "intent", "target"}`（target 为当前各类用户目标或 None）。
  - master 收到不携带 generation 的旧式 client_ready（含裸字符串与无该字段的 dict）时，沿用现有 pre-2.0 拒绝位置输出明确告警，不纳入集群。
  - 定义 worker→master `command_stale` 消息（data: `{"generation": <水位>, "state": <worker_state>}`）及其 master 侧入口（本任务仅登记与日志，补发逻辑在 Task 4/6 接入）。
  - 保留 Message `(type, data, node_id)` 结构与 msgpack 编解码不变。
- **Acceptance Criteria Addressed**: AC-9, AC-1
- **Test Requirements**:
  - `rule` TR-2.1: 新旧 client_ready 形态在 master 握手分支上被正确区分：新形态完成 ack（含代次/意图），旧形态被拒绝且有告警日志，节点不入 clients——证据：`locust/test/test_runners.py` 握手测试（含既有 pre-2.0 用例）通过。
  - `rule` TR-2.2: 携带 generation 的 spawn/stop/quit 可经 Message.serialize/unserialize 往返不丢字段——证据：协议单测。
  - `rule` TR-2.3: `command_stale` 消息可序列化、master 收到后进入定义好的处理入口（本任务允许仅记录日志）——证据：单测断言。
- **Notes**: 同步检查 [test_zmqrpc.py](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/locust/test/test_zmqrpc.py) 无需破坏。

## Task 3: Worker 水位判定、过期丢弃、幂等应用与按代次生命周期
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - WorkerRunner 以"已应用 generation + 该代次已应用目标指纹（user_classes_count 排序快照或 stop/quit 标记）"替代 `last_received_spawn_timestamp` 逻辑：
    - `gen < 水位`：spawn/stop/quit 一律丢弃，不改用户数/worker_state/状态，不触发生命周期，回 `command_stale`（带水位）。
    - `gen == 水位`：目标指纹相同→no-op 但正常回确认（幂等重发/重连补发）；指纹不同（同代次重平衡再分布）→应用差异。
    - `gen > 水位`：正常应用并推进水位。
  - 生命周期以代次内状态迁移为界：idle/stopped→running 仅在代次首次应用 spawn 时 fire 一次 `test_start`；running→idle 仅在代次首次 stop 时 fire 一次 `test_stopping`/`test_stop`（worker 侧 stop 路径与基类 Runner.stop 幂等协同，见 Task 5，本任务先保证消息分支不重复进入）。
  - `spawning_complete`、`client_stopped` 上报携带 generation；quit 应用后进入终止态，后续旧代次消息全部丢弃。
  - worker 连接握手时发送/接收新代次字段，并在握手后按 master 下发的当前意图对账（应用或保持），master 重启导致水位高于 master 时以 master 权威为准重置并对账。
- **Acceptance Criteria Addressed**: AC-1, AC-3, AC-7
- **Test Requirements**:
  - `rule` TR-3.1: 应用 gen=N 后，投递 gen<N 的 spawn/stop/quit 均无副作用（用户数、worker_state、三个生命周期事件计数不变），且 outbox 出现 command_stale 且含水位 N——证据：mocked rpc 单测。
  - `rule` TR-3.2: 同 gen 相同目标重复投递：delta=0、无新增用户、事件不重复、正常回带 gen 的确认——证据：单测。
  - `rule` TR-3.3: 同 gen 不同目标（模拟重平衡）投递时按差异增删用户，且不重复 fire test_start——证据：单测。
  - `rule` TR-3.4: quit 后投递旧 gen spawn/stop，用户数恒 0、无事件——证据：单测。
- **Notes**: 复用现有"kill 旧 spawning_greenlet 再启动新派发"机制承载被取代的在途派发。

## Task 4: Master 代次派发、ack 归集与有界集群等待
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1, Task 2, Task 3
- **Description**:
  - `MasterRunner.start()`：目标变化时经账本创建新代次（取代未完成代次）；spawn data 带 generation；expected 集合=当前 active workers；派发后等待当前代次结果（有界超时，默认值与环境变量沿用 `LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP`/`stop_timeout+60` 风格）；保留现有用户数上报等待作为辅助日志，不再是唯一收敛判据。
  - `MasterRunner.stop()`：创建 stopped 代次，stop data 带 generation；以 client_stopped 携带的 generation 作为确认归集；超时列缺失节点并 WARNING；stop 等待返回结果。
  - `quit()`：创建最高水位 quitting 代次并随 quit 下发；与 start/stop 有序互斥（互斥锁在 Task 5，本任务先接代次）。
  - master 处理 worker 带代次的 `spawning_complete`/`client_stopped`：归入账本 observe；处理 `command_stale`：立即在当前代次下向该 worker 幂等补发当前意图（运行目标经 dispatcher 的单 worker 派发，或 stopped/quitting 对应命令）。
  - worker 后连接/先判死时正确增删 expected 集合（与 Task 6 协同，本任务提供账本维护点）。
- **Acceptance Criteria Addressed**: AC-2, AC-5, AC-8
- **Test Requirements**:
  - `rule` TR-4.1: loopback 集群 start(10)→start(25)→start(7) 快速连发：仅最新代次结果 confirmed，master 与 worker 用户数等于最新派发目标，被取代代次结果为 superseded——证据：新增集成测试。
  - `rule` TR-4.2: 模拟一 worker 不确认：start/stop 结果在超时点为 timeout 且缺失列表准确、日志 WARNING；移除该 worker（判死）后结果可转为 confirmed——证据：单测/集成测试。
  - `rule` TR-4.3: 收到 worker 的 command_stale 后，master 向该 worker 补发当前代次意图并最终收到确认——证据：单测。
  - `rule` TR-4.4: 每条下发的 spawn/stop/quit 均带整数 generation 且同一意图重发使用相同 generation——证据：outbox 断言。
- **Notes**: 保持 start() 对 shape_worker 的阻塞语义不变（阻塞到最新代次确认或超时）。

## Task 5: 意图串行化与生命周期幂等（master / 基类 Runner / LocalRunner）
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 4
- **Description**:
  - 引入 gevent 串行化原语（单锁或单意图绿程队列）：并发 `MasterRunner.start()` 同目标合并（复用进行中结果）、新目标取代旧代次；进行中派发被取代时安全终止/让位于新代次派发（worker 侧以代次水位兜底）。
  - `MasterRunner.stop()` 与基类 `Runner.stop()` 幂等：停止中/已停止的并发/重复调用直接返回同一结果，不重复发命令、不重复 fire `test_stopping`/`test_stop`；补齐对 STATE_CLEANUP/STATE_STOPPING 的重入保护。
  - master/worker 的 `test_start` 仅在新运行代次首次进入运行时触发一次；快速 start→stop→start 产生两个运行代次、各一次；事件触发与代次状态机绑定，消除 check-then-set 竞争。
  - LocalRunner：start/stop 经同一串行化与幂等保护，即时产生 confirmed 结果（结果原语在 Task 7 接通，本任务保证行为与事件次数）。
  - quit 与 start/stop 互斥有序，quit 后任何派发不再产生用户。
- **Acceptance Criteria Addressed**: AC-6, AC-7, AC-10
- **Test Requirements**:
  - `rule` TR-5.1: 多绿程并发 start（同目标/不同目标两组）：同目标合并为同一代次一次派发；不同目标按序产生代次且最终态为最后目标；test_start 次数等于实际运行代数——证据：并发集成测试。
  - `rule` TR-5.2: 快速 start→stop→start 序列：master 与每 worker 上 test_start 各 2 次、test_stopping/test_stop 各 2 次，无重复无遗漏——证据：集成测试事件计数。
  - `rule` TR-5.3: 并发/重复 stop：outbox 仅一组当前代次 stop，事件各 1 次，所有调用返回同一结果——证据：单测。
  - `rule` TR-5.4: LocalRunner 并发 start/stop 后用户数正确、事件每代一次——证据：本地 runner 测试。
- **Notes**: 锁粒度只覆盖意图提交与状态迁移瞬间，派发等待在锁外进行，避免阻塞 client_listener/心跳。

## Task 6: 重连、心跳自愈与重平衡的代次对账
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 4, Task 5
- **Description**:
  - 重复 `client_ready`：保留 WorkerNode 身份/心跳/已观测水位，不以全新 INIT 节点覆盖运行态；依据 worker 上报代次与 master 当前意图对账（纳入当前代次 expected、幂等补发）；重复就绪本身不再触发全集群 start。
  - master `reset_connection` 重绑 ROUTER 后：worker 重连握手即对账，残留旧命令由 worker 水位丢弃；恢复后集群回到重置前意图。
  - worker `reset_connection` 重建 DEALER 后重发带水位 client_ready 的完整流程验证。
  - 心跳判死/自愈、client_stopped、worker quit 等拓扑变化：同步增删账本 expected 集合；仅在真实拓扑变化且运行中时按"同一代次、再分布目标"触发 dispatcher 重平衡（不递增代次、不重复生命周期）。
  - 收紧现有 `# TODO: Test this situation` 的再平衡触发条件，避免重复 client_ready 与心跳自愈叠加产生重复 start。
- **Acceptance Criteria Addressed**: AC-4
- **Test Requirements**:
  - `rule` TR-6.1: 运行中 worker 以同 id 重连（携带旧水位）：outbox 无全集群 spawn，仅对该节点幂等补发当前意图；节点不被重置为 INIT；最终代次 confirmed——证据：集成测试。
  - `rule` TR-6.2: master reset_connection 后 worker 重连，残留旧代次消息被 worker 丢弃且集群收敛到原目标——证据：集成测试。
  - `rule` TR-6.3: worker 判死后移除并在 rebalancing 开启时再分布：代次不变、test_start 不重复、存活 worker 收到同代次新目标并确认；最后一个 worker 判死的 stop 路径仍生效且只触发一次——证据：集成测试。
- **Notes**: 与 Task 4 的 expected 集合维护点成对实现，避免两处真相源。

## Task 7: 新事件、结果对象公共出口与 LocalRunner 同构结果
- **Status**: `pending`
- **Priority**: medium
- **Depends On**: Task 1, Task 5
- **Description**:
  - 在 [event.py](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/locust/event.py) 的 Events 中新增 `cluster_command_completed: EventHook`（载荷：generation、intent、status、expected/acked/missing worker 列表），代次进入终态时恰好触发一次；LocalRunner 操作即时触发 confirmed。
  - runner 暴露结果出口（如 `runner.last_command_result` 与 start/stop 返回结果对象），LocalRunner 返回同构 confirmed 结果；保持 start/stop 既有位置参数签名兼容（返回值为新增）。
  - 补齐 NFR-5 日志：代次推进、过期命令丢弃、对账补发、超时与缺失节点。
  - 在 [docs/extending-locust.rst](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/docs/extending-locust.rst) 事件章节登记新事件与代次/结果语义说明。
- **Acceptance Criteria Addressed**: AC-10, AC-11
- **Test Requirements**:
  - `rule` TR-7.1: confirmed/timeout/superseded 三种终态各恰好触发一次 cluster_command_completed，载荷字段正确；LocalRunner 每次 start/stop 触发 confirmed——证据：单测。
  - `rule` TR-7.2: start/stop 返回结果对象可 wait/查询，且不破坏现有以返回值无关方式调用 start/stop 的代码——证据：测试与既有套件。
  - `rubric` TR-7.3: 抽象与现有事件/runner 风格融合度；scale 1-5；anchors 1=另起炉灶/双真相源，3=可用但边界模糊，5=最小内聚、命名与文档一致；threshold >= 4；证据：独立审查。

## Task 8: 操作入口接线（Web / headless / shape / 键盘）
- **Status**: `pending`
- **Priority**: medium
- **Depends On**: Task 7
- **Description**:
  - [web.py](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/locust/web.py)：`/swarm` 的启动绿程与 `/stop` 均走代次化 start/stop；`/stop` 沿用 stop() 的有界集群等待（端点行为对现有前端保持成功响应）；`_swarm_greenlet` 被 kill 时由串行化机制安全取代而非留下孤儿派发；shape 启动路径不变（内部已走同机制）。
  - [main.py](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/locust/main.py)：headless autostart、`--run-time` 停止、键盘 w/W/s/S 调载均经同一串行化路径（键盘入口可移除粗糙的 state=="spawning" 判断，改由合并/取代语义处理）。
  - shape_worker 阻塞 start() 与外部 stop 并发场景做端到端核对。
- **Acceptance Criteria Addressed**: AC-8
- **Test Requirements**:
  - `rule` TR-8.1: 现有 [test_web.py](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/locust/test/test_web.py) 的 /swarm、/stop 用例在集群/本地模式下全部通过，且停止路径具备代次确认行为（outbox 断言）——证据：测试输出。
  - `rule` TR-8.2: 模拟 shape tick 进行中调用 stop：最终集群用户数为 0、stop 事件一次、无旧代次派发复活——证据：集成测试（复用 test_runners 现有 shape 测试范式）。
  - `rule` TR-8.3: headless autostart/run-time 与键盘调载路径不回归——证据：[test_main.py](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/07876fba-88cd-4556-9031-f5a96087896a/repo/locust/test/test_main.py) 与相关集成测试通过。

## Task 9: 复杂场景压测式集成测试与全量质量门禁
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 1, Task 2, Task 3, Task 4, Task 5, Task 6, Task 7, Task 8
- **Description**:
  - 新增压力式集成测试：多 worker（>=4）下每秒连续 tick 调载、随机 kill 派发绿程、随机 worker reset_connection/判死重连，持续 N 轮后断言：集群计数==最新目标、无节点停留在旧代次、生命周期次数精确、无死锁（心跳节拍持续）。
  - 全量运行 pytest、ruff、mypy；必要时运行 benchmarks/dispatch.py 对比派发开销无回退。
- **Acceptance Criteria Addressed**: AC-12, AC-13（并回归覆盖 AC-1~AC-10）
- **Test Requirements**:
  - `rule` TR-9.1: 压测式集成测试重复运行（至少 5 次）全部稳定通过——证据：命令输出。
  - `rule` TR-9.2: `uv run pytest locust/test`、`uv run ruff check locust/`、`uv run mypy locust/` 退出码均为 0——证据：命令输出。
  - `rubric` TR-9.3: 并发健壮性与性能；scale 1-5；anchors 1=死锁/心跳停摆/消息风暴，3=偶发竞争或开销可见，5=全稳定且消息复用现有节奏、benchmark 无回退；threshold >= 4；证据：压测重复运行结果与 benchmark 对比。
