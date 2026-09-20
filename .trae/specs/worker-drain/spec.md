# 分布式压测节点摘流（Worker Drain） - 产品需求文档

## Overview
- **Summary**: 为 Locust 主控（MasterRunner）增加"单节点摘流/排空（drain）"能力：长期压测中需要逐台维护 worker 节点时，主控可命令指定在线节点优雅退出负载，同时按可配置速率在其余在线节点上**逐类补齐**用户，补齐过程只增不减、不破坏其他节点已有会话；操作可撤销、可重复调用、对断线重连幂等；全局停测可立即打断；未使用该功能时调度行为与现状完全一致。
- **Purpose**: 支撑长期分布式压测中的滚动节点维护（升级、扩容替换、故障处置），在不停压测、不丢目标用户数、不断开其他节点已有用户会话的前提下逐台下线节点。
- **Target Users**: 使用 Locust 分布式模式（master + 多 worker）进行长期压测的平台工程师/SRE，通过主控 HTTP API 或 Python API 操作。

## Goals
- 主控可对指定在线 worker 发起摘流：该节点上全部用户在 `stop_timeout` 期限内优雅停止，节点进程保持在线连接（进入空载的"已摘流"状态）。
- 被摘节点上的各类用户数（per user class）按限速（spawn rate）在其余在线、未摘流的节点上补齐；补齐后各类用户总数与摘流前一致。
- 补齐只向其他节点下发"单调不减"的绝对目标，绝不停掉其他节点上已有的用户（保护会话）。
- 摘流可撤销：撤销后节点立即重新成为可调度成员，后续扩容/形变/再平衡可向其分配用户；撤销本身不停其他节点的用户。
- 幂等：重复摘流同一节点不产生重复停止/补齐；同一 client_id 断线重连（重复 client_ready 或 missing 后心跳自愈）不导致重新纳入或重复补人。
- 全局停测（stop/quit）立即终止摘流补齐流程并通知所有节点（含摘流节点）。
- 不发起任何摘流操作时，所有既有行为（调度、rebalance、--enable-rebalancing、worker_count、消息协议）保持不变。
- 通过 HTTP REST API（`POST /drain`、`POST /undrain`）与 Python API（`MasterRunner.drain_worker()` / `undrain_worker()`）暴露；摘流状态在 `/stats/requests` 的 workers 列表中可见。

## Non-Goals
- 不改动 React Web UI（不新增前端按钮/页面）；前端仅通过现有 workers 数据中的新字段感知状态。
- 不做摘流状态的持久化：worker 进程以**新 client_id** 重启时视为全新节点（既有 client_id 含 uuid 后缀）；同一 client_id 的连接级重连仍需幂等。
- 撤销时不主动把负载"迁回"该节点（避免停止其他节点用户）；节点在下次人数变更时自然分到负载。
- 不新增 worker 侧协议消息类型：摘流/补齐全部复用既有 `spawn`（绝对目标）与 `stop` 消息。
- 不支持 LocalRunner 摘流（单机模式无节点概念）。
- 不新增命令行开关；功能默认"存在但不激活"，激活唯一途径是显式 API 调用。
- 不新增 framework events（可在后续迭代补充 worker_drain/worker_undrain 事件）。

## Background & Context
- 主控 [MasterRunner](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/6943caae-e43c-49e6-8687-dc5cb4cc74b4/repo/locust/runners.py) 通过 [UsersDispatcher](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/6943caae-e43c-49e6-8687-dc5cb4cc74b4/repo/locust/dispatch.py) 计算"每 worker 的每类用户绝对目标数"，并以 `spawn` 消息逐轮下发；worker 的 `start_worker()` 将目标与实际计数 diff，自行 spawn/stop。**绝对目标消息天然幂等**：重复下发相同目标不会重复补人。
- worker 停止用户走 `Runner.stop_users()`：`environment.stop_timeout` 非 0 时先 `user.stop(force=False)` 优雅停止并等待至超时，超时后强杀；`stop_timeout` 由主控在 `spawn` 消息的 `stop_timeout` 字段下发。
- 既有 rebalance 路径（worker 增删、`--enable-rebalancing`、`_prepare_rebalance()`）按"从 0 假爬坡"重算全量分布，会在现存节点间互相挪动用户（停止+重拉），破坏会话——因此摘流不能走该路径，需要独立的"只增不减"增量算法。
- `MasterRunner.start()` 每轮迭代 dispatcher 并向其产出的所有 worker id 下发目标；dispatcher 实例生命周期为一次 dispatch cycle，`stop()` 时置空。
- worker 侧 `spawn` 消息按 `timestamp` 去重（小于等于上次时间戳则丢弃），摘流下发必须使用新的当前时间戳。
- 主控现有并发模型：gevent greenlet（`self.greenlet` Group）；停止时通过 kill spawning/shape greenlet 打断。

## Functional Requirements
- **FR-1（摘流 API）**：`MasterRunner.drain_worker(worker_id: str, spawn_rate: float | None = None) -> None`；将指定节点排入摘流队列并由唯一的摘流 greenlet **串行**处理（契合"逐台维护"）。`spawn_rate` 缺省取当前测试的 `self.spawn_rate`。HTTP：`POST /drain`，表单 `worker_id` 必填、`spawn_rate` 可选。
- **FR-2（优雅退出）**：摘流开始后，主控立即向目标节点下发每类计数为 0 的绝对目标（新时间戳），worker 经既有路径优雅停止全部用户并继续保持连接、随后上报 0 用户；主控不向其发送 quit。
- **FR-3（限速逐类补齐）**：主控对摘流时快照的该节点每类用户数，在其余"在线且未摘流"的承载节点间，以 worker id 排序后 round-robin 分摊为增量；按 `spawn_rate`（每轮 `max(1, floor(rate))` 个、轮间隔 `step/rate` 秒）有节奏地下发绝对目标；先停摘流节点、再限速补其他人。
- **FR-4（会话保护）**：摘流流程中发给所有**非摘流**节点的每类目标数相对其当前目标单调不减；这些节点不会产生 stop diff。
- **FR-5（dispatcher 状态一致）**：摘流在 dispatcher 上做状态手术——将摘流节点移出轮转序列（保留其全 0 记账）、其用户作为增量并入其余节点并同步 `_active_users`、重建 worker 轮转生成器；随后任何常规 `start()`（扩容、形变、rebalance）计算出的 diff 都与现实一致，不会把用户重新派回摘流节点，也不会重复补齐。
- **FR-6（撤销）**：`MasterRunner.undrain_worker(worker_id: str) -> None` / `POST /undrain`：对已完成摘流的节点，将其重新加入 dispatcher 轮转（记账为 0 用户）并清除摘流标记，不触发任何用户停止；对仍在队列中未开始的摘流请求直接取消；对正在摘流中的请求返回明确冲突错误（HTTP 409）。撤销不主动回迁负载。
- **FR-7（幂等-重复操作）**：对已摘流/摘流中/排队中的同一 worker_id 再次 drain 为 no-op（不产生消息、不改变计数）；对未知 id / 非运行态 / 非法 spawn_rate 返回错误且无副作用；对未摘流节点 undrain 为幂等成功。
- **FR-8（幂等-重连）**：摘流节点以同一 client_id 重连（重复 `client_ready`、或 STATE_MISSING 后心跳自愈）时，主控不得将其重新加入 dispatcher、不得触发 `start()`/rebalance、不得重复补人；其摘流标记保留。摘流节点真正被判定死亡并从 clients 移除时，同步清除摘流标记，且不因其移除触发全量 rebalance。
- **FR-9（全局停测打断）**：`MasterRunner.stop()`/`quit()` 立即 kill 摘流 greenlet、清空摘流队列与摘流标记，并向**所有**节点（含摘流节点）发送 stop；停测后重新开始测试时调度为全新状态。
- **FR-10（调度门控）**：测试运行期间，`start()` 的活动 worker 集合与 `worker_count` 均排除摘流节点；摘流节点仍保留在 `clients` 中可上报心跳/统计。
- **FR-11（无承载节点保护）**：若摘流发起时没有任何其他在线、可承载的运行中节点，则拒绝摘流（Python 侧抛错，HTTP 返回错误响应），不牺牲目标总人数。
- **FR-12（可观测性）**：`/stats/requests` 的 `workers` 列表每项增加布尔字段 `drained`。
- **FR-13（文档）**：在 [docs/running-distributed.rst](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/6943caae-e43c-49e6-8687-dc5cb4cc74b4/repo/docs/running-distributed.rst) 增加节点摘流维护章节（Python 与 REST 用法、语义与约束）。

## Non-Functional Requirements
- **NFR-1（零回归）**：未使用摘流时行为逐字节级不变；现有全部测试（`locust/test`）继续通过；新增逻辑以"摘流集合非空/显式调用"为门控。
- **NFR-2（代码质量）**：遵循项目约定——Python 3.11+、行宽 120、ruff、mypy（ignore_missing_imports）、gevent 协作式并发；复用既有消息与停止路径，不引入新依赖。
- **NFR-3（及时打断）**：停测请求发出后，摘流 greenlet 应在一个调度轮间隔上限内（断言用例以 ≤ 1s 有界等待）终止，不得阻塞 test_stop。
- **NFR-4（可测性）**：核心算法在 mock RPC 层可单测（断言下发的目标序列与时间节奏），端到端语义用进程内 master+worker 集成测试覆盖。

## Constraints
- **Technical**: 仅允许修改 Python 主控侧（runners.py、dispatch.py、web.py）、文档与测试；消息协议（msgpack Message）保持兼容；worker 进程不升级也能被摘流（复用 spawn/stop）。
- **Business**: 开源项目惯例，需包含单元/集成测试与用户文档；不引入破坏性 API 变更。
- **Dependencies**: gevent、现有 ZMQ/msgpack RPC、Flask（仅新路由）。

## Assumptions
- 摘流仅在测试处于 RUNNING/SPAWNING（dispatcher 已建立）时有意义；INIT/STOPPED 态调用返回错误。
- worker 以绝对目标 diff 的既有机制保证重发安全；主控以"摘流标记先落库、再异步执行"保证重复调用幂等。
- 逐台串行摘流：一次摘流流程结束后再处理队列中下一台；队列里重复 id 去重。
- 被摘流节点当前上报数作为补齐快照来源（master 侧 WorkerNode.user_classes_count）；dispatcher 内部记账与其一致（dispatch 完成后）。
- "限速"约束主控下发节奏；每轮向接收节点下发的是该节点的累计绝对目标。

## Acceptance Criteria

### AC-1: 指定在线节点可被摘流且优雅退出
- **Type**: `rule`
- **Given**: 主控与 ≥2 个 worker 已连接且测试处于 running（含多 user class）
- **When**: 调用 `drain_worker(worker_id)`（或 POST /drain）
- **Then**: 目标 worker 收到各类计数全 0 的 `spawn` 消息（带新时间戳）；其全部用户经 `stop_users` 路径在 `stop_timeout` 内优雅停止（stop_timeout=0 时立即停止）；节点仍在 master.clients 中、保持心跳，且不会收到 quit
- **Pass Condition**: 集成测试中该 worker `user_count==0` 且仍在 clients；mock 测试断言 outbox 存在全 0 目标 spawn、不存在发给该 id 的 quit
- **Evidence**: `locust/test/test_runners.py` 新增用例

### AC-2: 被摘用户按限速逐类补齐且总数守恒
- **Type**: `rule`
- **Given**: 摘流前各 worker 每类计数已知，摘流节点承载 (C1=n1, C2=n2, ...)
- **When**: 以 spawn_rate=R 摘流该节点
- **Then**: 补齐完成后，其余节点上每类用户合计分别等于摘流前的全集群每类合计；下发节奏为每轮新增不超过 max(1, floor(R)) 个、轮间隔约 step/R 秒；使用默认 R（self.spawn_rate）与显式覆盖均生效
- **Pass Condition**: mock 用例断言每轮消息目标增量受 step 约束且最终逐类合计守恒；集成用例断言摘流前后 master.user_count 及各类合计不变
- **Evidence**: `locust/test/test_runners.py`、`locust/test/test_dispatch.py` 新增用例

### AC-3: 补齐过程保护其他节点已有会话
- **Type**: `rule`
- **Given**: 摘流进行中
- **When**: 检查所有非摘流节点收到的每类绝对目标序列
- **Then**: 每个节点每个 user class 的目标数在整个摘流流程中单调不减；不存在要求任一非摘流节点减少任一类计数的消息
- **Pass Condition**: mock 用例对每个 receiver 断言目标序列单调不减
- **Evidence**: `locust/test/test_runners.py` 新增用例

### AC-4: 摘流可撤销且撤销不迁移会话
- **Type**: `rule`
- **Given**: 节点已完成摘流（0 用户、标记 drained）
- **When**: 调用 `undrain_worker(id)`（或 POST /undrain），随后调用 `start()` 提高目标人数
- **Then**: 节点重新进入 dispatcher 轮转且可分到新用户；撤销动作本身不停任何节点的用户（撤销前后其他节点计数不减少）；排队中未开始的摘流被取消；对摘流进行中的节点撤销返回冲突错误
- **Pass Condition**: 用例断言撤销后扩容能分配到该节点、撤销前后他人计数不变；队列取消与 409 各一用例
- **Evidence**: `locust/test/test_runners.py` 新增用例

### AC-5: 重复摘流/撤销幂等
- **Type**: `rule`
- **Given**: 节点处于排队中/摘流中/已摘流任一状态
- **When**: 以同一 worker_id 再次调用 drain；以及对未摘流节点调用 undrain
- **Then**: 重复 drain 为 no-op：不新增任何 spawn/stop 消息、计数与队列不重复变化；undrain 非摘流节点返回成功且无副作用；未知 id 返回错误且无副作用
- **Pass Condition**: mock 用例比较重复调用前后 outbox 消息数与各节点目标
- **Evidence**: `locust/test/test_runners.py` 新增用例

### AC-6: 摘流节点断线重连不重复补人
- **Type**: `rule`
- **Given**: 节点已摘流（或摘流中）
- **When**: 该节点以同一 client_id 发送重复 client_ready；或先被标记 missing 再以心跳自愈
- **Then**: 主控不将其重新加入 dispatcher、不触发 start()/rebalance、其余节点目标不变；节点仍被标记为摘流
- **Pass Condition**: mock 用例断言两种重连场景后 outbox 无新增 spawn、补齐目标不变、标记仍在
- **Evidence**: `locust/test/test_runners.py` 新增用例

### AC-7: 摘流节点死亡清理不引发全量再平衡
- **Type**: `rule`
- **Given**: 已摘流节点心跳超时进入 missing 并最终达到死亡阈值被移除
- **When**: 主控心跳维护逻辑执行
- **Then**: 同步清除其摘流标记；不因移除一个 0 用户的摘流节点而重置 dispatcher/触发全量 start()（其他节点会话不受影响）
- **Pass Condition**: mock 用例断言未发生 `start` 调用且其他节点未收到变化目标
- **Evidence**: `locust/test/test_runners.py` 新增用例

### AC-8: 全局停测及时打断摘流
- **Type**: `rule`
- **Given**: 以低 spawn_rate 发起摘流、补齐 greenlet 仍在运行
- **When**: 调用 `master.stop()`（或 quit()）
- **Then**: 摘流 greenlet 在 ≤1s 内终止；所有节点（含摘流节点）收到 stop；摘流标记与队列清空；随后重新 start 为全新调度，摘流节点不被特殊对待
- **Pass Condition**: 集成用例在停测后有界等待 greenlet 结束、断言消息与状态；重新 start 后节点可正常承载
- **Evidence**: `locust/test/test_runners.py` 新增用例

### AC-9: REST 接口、错误处理与可观测性
- **Type**: `rule`
- **Given**: 带 Web UI 的 master
- **When**: 调用 POST /drain、POST /undrain、GET /stats/requests
- **Then**: 正常请求返回 success JSON；错误情形（非 master runner、未知 worker_id、非运行态、无其他承载节点、spawn_rate 非正数/非数字）返回 success=false 与恰当 HTTP 状态码（400/404/409）且无副作用；workers 列表含 `drained` 布尔字段
- **Pass Condition**: `locust/test/test_web.py` 新增接口用例覆盖全部分支
- **Evidence**: `locust/test/test_web.py` 新增用例

### AC-10: 默认调度行为零回归
- **Type**: `rule`
- **Given**: 不调用任何摘流 API
- **When**: 运行既有本地/分布式测试（含 --enable-rebalancing、worker 增删、missing、stop 等场景）
- **Then**: 行为与改动前一致
- **Pass Condition**: `uv run pytest locust/test` 全绿；`uv run ruff check locust/` 与 `uv run mypy locust/` 无新增问题
- **Evidence**: 全量测试与静态检查输出

### AC-11: 实现与项目惯例的契合度
- **Type**: `rubric`
- **Dimension**: 代码质量（并发正确性、状态一致性、风格一致性、文档与日志可读性）
- **Scale**: 1-5
- **Anchors**: 1 = 引入新 worker 协议或破坏默认行为/存在竞态；3 = 功能正确但实现绕弯、状态同步有异味；5 = 复用绝对目标消息、greenlet 生命周期清晰、门控严谨、文档示例完整
- **Pass Threshold**: >= 4
- **Evidence**: 代码审查 + 文档渲染检查

## Open Questions
- 无（接口形态、撤销语义、限速来源已由决策者确认采用推荐方案：REST+Python API、撤销仅重新纳入、默认 self.spawn_rate 可覆盖）。
