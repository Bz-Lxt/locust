# 分布式压测节点摘流（Worker Drain） - 实施计划

## Task 1: Dispatcher 摘流/恢复状态手术
- **Status**: `done`（completion evidence：`locust/dispatch.py` 新增 `drain_worker`/`readd_worker`/`has_worker`/`worker_node_ids`/`current_targets`；`locust/test/test_dispatch.py::TestDrainWorker` 6 用例覆盖 TR-1.1~1.4，全量 `test_dispatch.py` 通过）
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 在 [WorkerNode](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/6943caae-e43c-49e6-8687-dc5cb4cc74b4/repo/locust/runners.py#L590-L603) 增加 `drained: bool = False` 标志（仅主控侧记账/可观测用）。
  - 在 [UsersDispatcher](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/6943caae-e43c-49e6-8687-dc5cb4cc74b4/repo/locust/dispatch.py) 新增 `drain_worker(worker_node)`：
    - 快照该节点在 `_users_on_workers` 中的每类计数；将其各项置 0（保留 key 记账，使主控 start() 对其下发 0 目标）。
    - 从 `_worker_nodes` 移除并重排序，重建 `_worker_node_generator`（摘流节点不再参与轮转）。
    - 将其每类用户按"worker id 排序后 round-robin、只增不减"分摊到剩余节点：增加 `_users_on_workers` 计数、向 `_active_users` 追加 `(receiver_node, class_name)` 并移除属于摘流节点的旧 tuple；更新 `_current_user_count`（总数守恒）。
    - 不设置 `_rebalance`（避免触发从零重算）。返回最终每节点绝对目标 map（供主控限速下发）。
  - 新增 `readd_worker(worker_node)`：重新挂回（其记账为全 0）、重排序、重建轮转生成器；不动他人计数、不置 `_rebalance`；幂等（已在列表则 no-op）。
  - 边界：无剩余节点时 `drain_worker` 抛出明确异常（由主控决定拒绝摘流）。
- **Acceptance Criteria Addressed**: AC-2, AC-3, AC-4
- **Test Requirements**:
  - `rule` TR-1.1: 多 user class 场景下 `drain_worker` 后：摘流节点各类为 0；其余节点逐类合计等于摘流前合计（总数守恒）；分摊为 round-robin（3 节点摘 1 节点时增量按 id 序交替）。证据：`locust/test/test_dispatch.py` 新用例断言 `_users_on_workers`。
  - `rule` TR-1.2: `drain_worker` 后 `_worker_nodes` 与轮转生成器不再产出该节点（后续 new_dispatch 爬坡不会派给它）；`_active_users` 中不再含该节点 tuple 且长度守恒。证据：同上断言后续 dispatch 迭代产出。
  - `rule` TR-1.3: `readd_worker` 后节点回到轮转、记账为 0；再次同总人数 new_dispatch 首轮他人目标不减少（不迁移会话）；重复 readd/drain 已摘流节点幂等无异常。证据：test_dispatch.py 新用例。
  - `rule` TR-1.4: 对仅剩单节点的 dispatcher 调用 `drain_worker` 抛出 ValueError。证据：test_dispatch.py 新用例。
- **Notes**: 纯记账层改动，不涉及 gevent/网络；现有 dispatch 测试必须零修改通过（NFR-1）。

## Task 2: MasterRunner 摘流核心（API、串行队列、优雅停止、限速补齐）
- **Status**: `done`（completion evidence：`locust/runners.py` MasterRunner 新增 `drain_worker`/`undrain_worker`、串行队列 greenlet、全 0 spawn、限速 ramp、有界等待；修复 spawn 消息目标 dict 延迟发送别名问题（`_build_spawn_message` 内 copy）。`test_runners.py::TestMasterWorkerDraining` 覆盖 TR-2.1/2.2/2.4/2.5/2.6 + 真实 ZMQ 集成 TR-2.3，共 13 用例通过）
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - 在 [MasterRunner](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/6943caae-e43c-49e6-8687-dc5cb4cc74b4/repo/locust/runners.py#L649) 增加：`drained_client_ids: set[str]`、`_drain_queue: deque[tuple[str, float]]`、`_drain_greenlet: Greenlet | None`、`_draining_client_id: str | None`。
  - `drain_worker(worker_id, spawn_rate=None)`：校验节点存在且在线（ready/running/spawning 均可，但必须在 clients）、状态为 RUNNING/SPAWNING、dispatcher 已建立、存在其他可承载（running/spawning、未摘流）节点、spawn_rate（若给）为正数；校验失败抛 `ValueError`/`LookupError`。已摘流/队列中/进行中的同 id 调用为 no-op。入队（去重）并确保唯一摘流 greenlet 运行。
  - 摘流 greenlet `_drain_workers()` 串行处理队列，单台流程 `_drain_worker(worker_id, spawn_rate)`：
    1. 置 `_draining_client_id`，先将 id 加入 `drained_client_ids` 与 `WorkerNode.drained`（幂等锚点先落库）。
    2. 快照补齐来源以 dispatcher 记账为准（Task 1 返回的最终目标 map：摘流节点 0、receiver 最终目标）。
    3. 立即向摘流节点下发全 0 绝对目标 spawn 消息（新 timestamp、host/stop_timeout/parsed_options 与常规 spawn 一致）。
    4. 限速补齐：将增量按类交织成有序 `(receiver_id, class)` 序列，每轮 `step=max(1, floor(rate))` 累加，向**本轮发生变化**的 receiver 下发累计绝对目标；轮间隔 `step/rate`（与 dispatcher 相同的节流口径），每轮检查主控状态（非 RUNNING/SPAWNING 即退出）。
    5. 等待摘流节点上报 0 与 receiver 达到最终目标（复用 `_wait_for_workers_report_after_ramp_up()` 有界等待）；清空 `_draining_client_id`。
  - `undrain_worker(worker_id)`：未知 id 抛 `LookupError`；在队列未开始 → 从队列移除并清标记（no 消息）；正在摘流（`_draining_client_id==id`）→ 抛冲突异常（映射 HTTP 409）；已摘流 → dispatcher.readd_worker、清集合/节点标记。未摘流节点为幂等 no-op。
  - 下发目标的辅助函数复用 `start()` 中的 spawn 消息构造（timestamp/host/stop_timeout/parsed_options），用 `gevent.spawn_later(0, server.send_to_client, ...)` + Group join。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-3, AC-4, AC-5, AC-11
- **Test Requirements**:
  - `rule` TR-2.1: mock RPC 下 3 worker、多 user class、start 达到稳态后 drain 一个：outbox 出现发给该 id 的全 0 spawn（timestamp 新）且无 quit；其余 worker 收到的目标逐轮单调不减、最终各类合计守恒。证据：test_runners.py mock 用例检查 `MockedRpcServerClient.outbox`。
  - `rule` TR-2.2: 限速：spawn_rate=2 摘流 6 用户时，接收节点至少经历 3 个递增目标轮次，且任意相邻下发间增量 ≤2；显式 spawn_rate 覆盖 self.spawn_rate 生效。证据：mock 用例记录每次相关 spawn 的目标值与时间。
  - `rule` TR-2.3: 进程内集成（真 ZMQ）：3 worker running 后 drain 其一，有界时间内该 worker user_count==0、仍在 master.clients、总 user_count 与各类数不变；stop_timeout 经 spawn 消息传递（stop_timeout 非 0 时用户走优雅停止路径——用会阻塞的 task 配合小 stop_timeout 验证最终能停）。证据：test_runners.py 集成用例。
  - `rule` TR-2.4: 校验分支：未知 id、非运行态、无其他承载节点、spawn_rate<=0/非法分别抛对应异常且 outbox/状态无变化。证据：mock 用例。
  - `rule` TR-2.5: 幂等：对排队中/进行中/已摘流 id 再 drain 不新增消息、队列不重复；对未摘流 id undrain 为 no-op。证据：mock 用例对比 outbox 长度与目标。
  - `rule` TR-2.6: 撤销：已摘流节点 undrain 后再 `start()` 提高总人数，新用户可分配到该节点；undrain 不向任何节点下发减量目标；排队中的 drain 被 undrain 取消（节点最终不被摘流）。证据：mock/集成用例。
- **Notes**: greenlet 加入 `self.greenlet` Group 并挂 `locust_exception_handler`；异常需落日志且不得让 greenlet 静默死亡（finally 中清理 `_draining_client_id` 并继续处理队列）。

## Task 3: 调度门控、重连幂等、死亡清理与全局停测打断
- **Status**: `done`（completion evidence：start() 过滤摘流节点并中止在途摘流、worker_count 排除、client_ready/heartbeat 自愈门控、heartbeat_worker 摘流节点不 rebalance/死亡静默清理、client_stopped/quit 清标记、stop() 重置；TR-3.1~3.6 由 TestMasterWorkerDraining 同名用例覆盖并通过；test_runners.py + test_dispatch.py 全量 184 passed/4 skipped）
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - `MasterRunner.start()`：活动 worker 集合排除 `drained_client_ids`；dispatcher 存在时其节点集合已通过摘流手术保持一致，无需额外改动。
  - `worker_count` 属性排除摘流节点（它们在线但不承载）。
  - `handle_message` 的 `client_ready` 分支：若 id 已摘流——重建 WorkerNode 后恢复其 `drained=True` 与集合标记，**不**调用 dispatcher.add_worker、**不**触发任何 `start()`（含 rebalancing 路径）；常规日志可降为 debug。
  - `heartbeat` 分支 missing→自愈：摘流 id 不 add_worker、不 start。
  - `heartbeat_worker`：摘流节点不因其 heartbeat 衰减进入常规 missing→rebalance 路径（摘流中/已摘流即跳过 remove_worker 与 rebalance start）；达到死亡阈值被移除时，同步从 `drained_client_ids` 删除，且"missing 移除后触发 start"的分支对该情况跳过（dispatcher 未受影响、他人会话不动）。
  - `client_stopped`/`quit` 分支删除节点处：同步清除摘流标记。
  - `stop(send_stop_to_client=True)`：在状态切换后立即 kill 摘流 greenlet、清空队列与摘流集合/节点标志（stop 发送范围仍为 `self.clients.all`，天然包含摘流节点）；`quit()` 沿用 stop。
  - 确保 `start()`（扩容/形变/外部）在摘流进行中被调用时不冲突：摘流 greenlet 每轮以 dispatcher 记账为目标，常规 start 亦收敛到同一记账（绝对目标幂等）；若 start 发生，kill 当前摘流 greenlet 并清队列中的后续项（标记保留），避免双下发。
- **Acceptance Criteria Addressed**: AC-6, AC-7, AC-8, AC-10
- **Test Requirements**:
  - `rule` TR-3.1: 摘流后重复 client_ready：无新 spawn 给任何节点、receiver 目标不变、节点仍被标记 drained 且不在 dispatcher `_worker_nodes`。证据：mock 用例。
  - `rule` TR-3.2: 摘流节点 missing 后心跳自愈：不 add_worker/不 start/不补人。证据：mock 用例。
  - `rule` TR-3.3: 摘流节点达到死亡阈值被删：标记清除、不触发 `start()`（mock patch master.start 断言未调用）、其余节点目标不变。证据：mock 用例（patch HEARTBEAT 间隔/死亡阈值，参考既有 test_worker_missing_after_heartbeat_dead_interval）。
  - `rule` TR-3.4: 低 spawn_rate 摘流进行中调 `stop()`：摘流 greenlet ≤1s 结束；所有 clients（含摘流节点）收到 stop；`drained_client_ids` 与队列为空；随后重新 start 时该节点可正常分到用户。证据：集成/mock 用例。
  - `rule` TR-3.5: `worker_count` 排除摘流节点但节点仍在 clients 且继续心跳上报。证据：mock 用例。
  - `rule` TR-3.6: 摘流进行中外部调用 `start()` 新目标：不出现超过最终目标的重复补人，最终总数收敛到新目标。证据：mock 用例断言各节点目标从不超过 dispatcher 记账。
- **Notes**: 所有新增分支均以 `client_id in self.drained_client_ids` 为门控；门控之外的代码路径零改动。

## Task 4: Web REST 接口与状态可见性
- **Status**: `done`（completion evidence：`locust/web.py` 新增 `POST /drain`、`POST /undrain`（含非 master 400、缺 worker_id 400、非法 spawn_rate 400、未知 id 404、进行中 409、幂等成功），`/stats/requests` workers 项新增 `"drained"` 字段；`locust/test/test_web.py` 新增 `TestWebUIDrain`（6 用例）+ TestWebUI 非 master 用例，TR-4.1/4.2 覆盖并通过；test_web.py 中 15 个失败为既有的 `locust/webui/dist` 未构建导致，与本次改动无关）
- **Priority**: medium
- **Depends On**: Task 2, Task 3
- **Description**:
  - [web.py](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/6943caae-e43c-49e6-8687-dc5cb4cc74b4/repo/locust/web.py) 新增 `POST /drain`：表单 `worker_id`（必填）、`spawn_rate`（可选浮点）；仅 MasterRunner 可用。返回统一 JSON：成功 `{success: true, message, worker_id}`；失败 `{success: false, message}` + 状态码：非 master/非运行态 400、未知 worker 404、摘流进行中冲突 409、参数非法 400。重复摘流返回 success（幂等）。
  - 新增 `POST /undrain`：表单 `worker_id`；未知 worker 404；摘流中冲突 409；成功或本就未摘流返回 success（幂等）。
  - `/stats/requests` 的 workers dict 增加 `"drained": worker.drained`。
  - 两个端点加 `@auth_required_if_enabled`，风格对齐 `/swarm`、`/stop`。
- **Acceptance Criteria Addressed**: AC-9, AC-5
- **Test Requirements**:
  - `rule` TR-4.1: test_web.py 风格用例（Flask test client + mock master）：正常 drain/undrain 200 且调用到 runner 方法；缺 worker_id、非 master、未知 id、409、非法 spawn_rate 的状态码与 success=false；重复 drain 200。证据：`locust/test/test_web.py` 新用例。
  - `rule` TR-4.2: GET /stats/requests（MasterRunner）workers 每项含 drained 字段且与 runner 记账一致。证据：同上。
- **Notes**: 不改动任何前端 TS 代码。

## Task 5: 用户文档
- **Status**: `done`（completion evidence：`docs/running-distributed.rst` 新增 "Draining a worker node (rolling maintenance)" 章节，含场景、语义约束（逐类守恒/限速/串行/幂等/重连/停测打断/最后节点保护/撤销不回迁）、Python API 与 REST curl 示例、错误码说明，与实现一致）
- **Priority**: low
- **Depends On**: Task 4
- **Description**:
  - 在 [docs/running-distributed.rst](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/6943caae-e43c-49e6-8687-dc5cb4cc74b4/repo/docs/running-distributed.rst) 增加"Draining a worker node（滚动维护）"小节：适用场景、Python API（drain_worker/undrain_worker）、REST（curl 示例 /drain、/undrain）、语义说明（逐类守恒、只增不减、stop_timeout、串行逐台、撤销不回迁、重连幂等、停测打断、至少保留一个承载节点）。
- **Acceptance Criteria Addressed**: AC-11
- **Test Requirements**:
  - `rubric` TR-5.1: 文档质量；scale 1-5；anchors 1=缺失或误导 / 3=可用但缺少关键语义或示例 / 5=场景/API/约束/示例完整且与实现一致；threshold >= 4；证据：审阅渲染后的 rst 章节。

## Task 6: 静态检查与全量回归
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 1, Task 2, Task 3, Task 4, Task 5
- **Description**:
  - 运行 `uv run ruff check locust/`、`uv run mypy locust/`、`uv run pytest locust/test/test_dispatch.py locust/test/test_runners.py locust/test/test_web.py`，最后 `uv run pytest locust/test` 全量；修复所有新增问题。
- **Acceptance Criteria Addressed**: AC-10, AC-11
- **Test Requirements**:
  - `rule` TR-6.1: ruff 与 mypy 对改动文件零错误；新增测试全部通过；全量 locust/test 通过（既有用例零修改）。证据：命令输出记录于 Completion Evidence。
  - `rubric` TR-6.2: 实现契合度（并发/状态机/风格/日志）；scale 1-5；anchors 同 AC-11；threshold >= 4；证据：自审记录 + 代码走读。
