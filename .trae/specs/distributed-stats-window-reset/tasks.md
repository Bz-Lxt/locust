# 分布式压测统计重置窗口化 - 实现计划

## Task 1: 窗口 epoch 原语与窗口注册表
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 在 [stats.py](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/a8fdb0d3-ae9f-4db2-afc5-c1b0e3778ed5/repo/locust/stats.py) 引入窗口元数据与窗口注册表（如 `StatsWindow` / `WindowRegistry`）：持有 epoch（首个窗口=0）、窗口起止时间戳、当前窗口的 `RequestStats`、已关闭窗口条目与宽限截止时间。
  - 提供无网络语义的切割原语：关闭当前窗口（记录关闭时间、安排宽限到期回调钩子）→ 创建空 `RequestStats` 的新窗口（epoch+1）。
  - LocalRunner 的重置路径改为调用该原语，保证单机与分布式语义一致（FR-11）。
  - `environment.stats` 始终指向当前窗口的 RequestStats，现有读取方无需改动。
- **Acceptance Criteria Addressed**: AC-7（基础设施部分）、AC-9
- **Test Requirements**:
  - `rule` TR-1.1: 连续调用切割原语两次后，注册表当前 epoch=2；每次切割均产生新的空 RequestStats 对象，旧窗口条目记录了正确 epoch 与关闭时间戳；证据为新增 pytest 断言。
  - `rule` TR-1.2: LocalRunner 经事件触发重置后，`environment.stats` 为新窗口空对象、epoch 递增，且历史窗口可按 epoch 检索；证据为 pytest 输出。
- **Completion Evidence**:
  - TR-1.1/1.2 通过：`uv run pytest locust/test/test_stats.py::TestWindowRegistry` → 6 passed；含连续两次 cut 的 epoch/关闭时间/grace_deadline、close listener、reset 复用 window0、finalize 释放、LocalRunner 切割断言。
  - 回归：test_stats.py + test_web.py 112 passed（17 失败为 webui dist 未构建的预存环境问题，stash 验证一致）；test_runners.py 初次 9 failed 定位为注册表替换 stats 致聚合闭包失效，修复为"初始窗口复用 environment.stats、reset 原地 clear_all"后，原失败 10 项全部 passed。

## Task 2: Worker epoch 标记与无损报告发送
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - WorkerRunner 维护 `current_epoch`（初始 0）；在 `report_to_master` 产出的 data 中增加 `epoch` 字段（FR-1）。
  - 将"序列化即清空"改为双缓冲两阶段：原子换出活动累加器（新样本进入新对象），序列化旧对象快照；RPC 发送失败时保留完整序列化报告于 `pending_stats_reports`，下个周期先重试 pending 再发当前缓冲，发送成功才丢弃（FR-5）。
  - 换出与序列化之间无 `gevent.sleep`/阻塞点，保证与 reset 处理的协作式原子性。
  - `quit` 路径保持发送末次报告；stop/新测试边界复位 epoch 并清理不可送达的 pending 报告，防止跨测试串流。
- **Acceptance Criteria Addressed**: AC-4
- **Test Requirements**:
  - `rule` TR-2.1: monkeypatch `client.send` 首次抛 RPCError、第二次成功时，成功送达报告的计数恰为实际 log 的总量（无重复、无丢失），且首次失败后旧增量完整保留于 pending、新活动缓冲为空；证据为 pytest 断言。
  - `rule` TR-2.2: 正常发送的 stats 消息 data 中包含 `epoch` 字段且等于 worker 当前 epoch；证据为 pytest 断言。
- **Completion Evidence**:
  - TR-2.1/2.2 通过：新增 `TestWorkerRunner::test_failed_stats_send_retains_report_for_retry`（首次 RPCError 后 pending 保留 2 请求/1 错误、active 缓冲为空；期间新增 1 请求；重试后两条 stats 消息分别携带 2 与 1 个请求，无重复无丢失）与 `test_stats_report_includes_epoch`（stats 消息含 epoch=0），2 passed。
  - 回归：TestWorkerRunner + TestMasterRunner + TestMasterWorkerRunners + TestWindowRegistry 共 77 passed, 3 skipped（145s，含真实 in-process 分布式集成）；ruff All checks passed；mypy Success（为 MasterRunner.start 的 window_registry.reset() 补充非空断言）。
  - 实现要点：stats.py `on_report_to_master` 支持 `data["report_stats"]` 注入并做非破坏性 snapshot（移除旧的 errors 清除）；runners.py `_send_stats` 双缓冲 + pending FIFO 重试；stop/spawn 边界复位 epoch=0 与 pending。

## Task 3: Master 重置广播与 Worker 边界切割
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1, Task 2
- **Description**:
  - MasterRunner 新增/改造重置方法：调用窗口切割原语后，经现有 `send_message` 向全部已连接 worker 广播 `reset_stats`，data 含 `{"epoch": 新epoch, "timestamp": 边界时间}`（FR-2）。
  - `/stats/reset` 经 Task 1 已改为调用 `runner.reset_stats()`；MasterRunner 覆写该方法，故重置自动包含广播（保留 web.py 中 `reset_stats` 事件与 exceptions 清理）。
  - WorkerRunner `handle_message` 增加 `reset_stats` 分支：以旧 epoch 立即发送末次增量（含重试 pending）→ `current_epoch` 切换为新 epoch；此后常规报告携带新 epoch（FR-3）。
  - 幂等：收到小于等于当前 epoch 的 reset_stats 时忽略并记录调试日志。
- **Acceptance Criteria Addressed**: AC-2、AC-6（触发路径）
- **Test Requirements**:
  - `rule` TR-3.1: master 重置时，每个已连接 worker 收到恰好一条 `reset_stats` 且 data["epoch"]=新值；证据为捕获消息的 pytest 断言。
  - `rule` TR-3.2: worker 处理 reset_stats 后产生恰好一份旧 epoch 末次报告、累加器归零、后续报告 epoch=新值；重复/过期 reset_stats 不产生额外切割；证据为 pytest 断言。
- **Completion Evidence**:
  - TR-3.1 通过：新增 TestMasterRunner::test_reset_stats_broadcasts_epoch_to_all_workers——2 worker 各收到恰好一条 epoch=1 reset（含 timestamp），registry.current_epoch=1；第二次重置广播 epoch=2。
  - TR-3.2 通过：新增 TestWorkerRunner::test_worker_reset_stats_cuts_boundary——reset 后恰好一份 epoch=0 末次报告（3 请求）、active 缓冲为空、epoch=1；新报告 epoch=1（2 请求）；重复 epoch=1 与过期 epoch=0 消息均幂等忽略。
  - 实现要点：MasterRunner.reset_stats()（cut 时间戳作为边界并广播）；worker reset_stats 分支调用 _send_stats（双缓冲保证末次报告无损，发送失败则 pending 携带旧 epoch 延迟送达）后切换 epoch。
  - 测试确定性：stats_reporter 启动即执行首轮（既有行为，sleep 在循环末尾），三个 worker 测试统一预等待 0.1s 并清理 outbox/sent，连续重复运行结果稳定。
  - 回归：4 个新测试 passed；ruff All checks passed；mypy Success。

## Task 4: Master epoch 路由聚合与兼容降级
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1, Task 2
- **Description**:
  - 改造 master 侧聚合监听器：按 `data["epoch"]` 从窗口注册表取目标窗口 RequestStats 执行既有 extend/error merge 逻辑；窗口已固化/未知 epoch 时不污染当前窗口，原始报告进入迟到归因通道（Task 5 消费）。
  - 无 epoch 字段的报告降级为当前 epoch 处理，不得抛异常（FR-12）。
  - `runners.py` 中 master 自身的 `on_worker_report`（user_classes_count）保持不变。
- **Acceptance Criteria Addressed**: AC-1、AC-8
- **Test Requirements**:
  - `rule` TR-4.1: 构造"epoch=0 在途报告在重置后处理 + epoch=1 新报告"的确定性序列，断言 epoch=1 窗口 total/entries 只含新报告、epoch=0 窗口含在途报告；证据为 pytest 断言。
  - `rule` TR-4.2: 注入不含 epoch 的 stats 消息不抛异常且计入当前窗口；既有 worker_report 自定义监听者仍被调用；证据为 pytest 断言。
- **Completion Evidence**:
  - TR-4.1 通过：新增 TestMasterRunner::test_worker_report_routed_to_its_epoch_window——用独立 RequestStats 模拟 worker 缓冲（避免在 master 窗口合成数据造成双计），epoch0 在途报告在 reset 后处理：window0 仅含 /old（1 请求），window1 仅含 /new（1 请求），互不交叉。
  - TR-4.2 通过：新增 test_worker_report_without_epoch_degrades_to_current_window——无 epoch 字段不抛异常、计入当前窗口，自定义 worker_report 监听者仍被调用。
  - 实现要点：stats.py 新增 StatsRouter/LateStatsSink 类型，setup_distributed_stats_event_listeners 增加 stats_router/late_stats_sink 参数，聚合前先路由；目标缺失时走迟到通道。DistributedRunner 以可后置注入的路由槽位接线（解决 registry 在 super().__init__ 之后才构造的顺序问题），MasterRunner 安装窗口路由与 late_stats_reports 队列。
  - 回归：TestMasterRunner + TestMasterWorkerRunners + TestWorkerRunner 共 75 passed, 3 skipped（147s，含真实分布式集成）；ruff All checks passed；mypy Success（Callable/Any 改为运行时导入）。

## Task 5: 离线/迟到报告归因与 epoch 再同步
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 3, Task 4
- **Description**:
  - Master 对 stale epoch（小于当前 epoch）报告：宽限期内路由到对应旧窗口；窗口已固化时，将该报告按其 epoch 追加写入历史导出（迟到行），不改动已固化快照（FR-6、FR-7 后半）。
  - Epoch 再同步：master 在现有握手（ack）或收到 stale 报告的回执中携带当前 epoch；worker 发现本地 epoch 落后时，先发送旧 epoch 末次增量、清空、切换到新 epoch，再产出新窗口数据（FR-6）。
  - 未观察到边界的离线 worker 回归首批数据整体归入旧 epoch。
- **Acceptance Criteria Addressed**: AC-3
- **Test Requirements**:
  - `rule` TR-5.1: 模拟 worker 断连跨重置、重连发送 stale epoch 报告：旧窗口聚合含该报告、当前窗口不含；worker 经回执完成切割后新报告 epoch=当前值且进入当前窗口；证据为 pytest 断言。
  - `rule` TR-5.2: 窗口已固化后到达的旧 epoch 报告被追加到历史导出且标记其 epoch，master 不抛异常；证据为 pytest 断言。
- **Completion Evidence**:
  - TR-5.1 通过：新增 TestMasterRunner::test_stale_worker_report_attributed_and_worker_resynced——离线 worker 的 epoch=0 报告在 master 已进入 epoch=1 后处理：宽限期内经路由并入 window0（当前 window1 不含）；master 向该 worker 单发 reset_stats（epoch=1），worker 末次旧报告后切换 epoch，新报告进入 window1。
  - TR-5.2 通过：新增 test_report_for_finalized_window_goes_to_late_channel_and_csv——window0 固化（stats=None）后到达的 epoch=0 报告进入 late_stats_reports（原始 data 不丢），CSV writer 经 write_late_stats_report 重建并追加标记 Window=0 的行，当前窗口计数不受影响。
  - 兼容性：test_legacy_report_without_epoch_does_not_trigger_resync——无 epoch 字段（legacy worker）计入当前窗口且不触发再同步，无异常日志。
  - 实现要点：_resync_worker_epoch_if_stale（未知 client/无 epoch 直接返回；stale 则单点补发 reset_stats）；_accept_late_stats_report 入队；write_late_stats_report 用独立 RequestStats + aggregate_worker_report 重建，use_current_percentiles=False（重建对象无 cache），_ensure_history_header 保证表头先写。
  - 回归：3 个新测试 passed；test_stats.py + test_runners.py 191 passed, 3 skipped；4 个全文件顺序失败经 stash 干净代码复现（预存 flaky）；ruff/mypy 通过。

## Task 6: 窗口宽限期固化与内存释放
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 4, Task 7
- **Description**:
  - 窗口关闭时启动宽限计时器（默认 60s，可经环境变量配置；默认值覆盖报告周期与常见抖动）。
  - 到期回调：经 history writer 写入窗口边界行与最终快照行，随后释放该窗口完整聚合（entries/total/errors 引用），仅保留有界元数据（epoch、起止时间、最终 totals 快照）（FR-7）。
  - 固化与迟到路由复用同一写入接口，避免双实现。
- **Acceptance Criteria Addressed**: AC-7
- **Test Requirements**:
  - `rule` TR-6.1: 用短可配置宽限期跑多次重置，到期后旧窗口聚合引用被释放（注册表不再持有其 entries/RequestStats），元数据仍可按 epoch 查询；证据为 pytest 断言（可缩短宽限为秒级）。
  - `rubric` TR-6.2: master 内存对象有界性；scale 1-5；anchors 1=每窗口聚合常驻/线性增长，3=聚合释放但残留明显冗余，5=仅当前+宽限期窗口持聚合、旧窗口仅有界元数据；threshold >= 4；证据为连续超宽限周期多次重置后对象计数检查/代码走查。
- **Completion Evidence**:
  - TR-6.1 通过：新增 test_window_aggregate_finalized_and_released_after_grace_period（grace=0.2s；重置后窗口 0 在宽限期内仍可聚合，到期后 stats 引用释放、final_total 保留且计数=1、close_time 准确，当前窗口不受影响）；test_finalization_emits_boundary_and_final_snapshot_rows（CSV 含 1 条 WINDOW_BOUNDARY 行与 Window=0 的最终快照 old_entry+Aggregated）。
  - TR-6.2 自评 5/5：test_repeated_resets_leave_bounded_number_of_live_aggregates——4 次重置且每次超宽限等待后，持聚合的窗口恰为 1 个（当前），其余 4 个仅有界元数据（is_finalized）。
  - 安全性：test_finalization_waiter_from_previous_generation_is_harmless——宽限期内 registry.reset() 开启新测试后，旧等待者不会固化被回收复用的 window0（generation 代际令牌 + 对象身份双重校验）。
  - 实现要点：Runner._arm_window_finalization/_finalize_window（greenlet 等待 grace_deadline；写入边界行+最终快照；finalize 保留 total.serialize()）；WindowRegistry.generation 每次 reset 递增；StatsCSVFileWriter 构造时挂载到 runner._window_history_writer；write_window_final_snapshot_rows 以全量百分位（use_current_percentiles=False）落最终行；_ensure_history_header 保证任何顺序下表头只写一次。
  - 回归：TestWindowRegistry 10 passed；test_stats.py + test_runners.py 191 passed, 3 skipped；ruff/mypy 通过。

## Task 7: CSV history 新增 Window 列与边界行
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - [stats.py](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/a8fdb0d3-ae9f-4db2-afc5-c1b0e3778ed5/repo/locust/stats.py#L1097-L1249) `StatsCSVFileWriter`：`stats_history_csv_columns` 新增 `Window` 列（建议首列）；`_stats_history_data_rows` 为每行写入其窗口 epoch；提供窗口边界行写入方法（含 epoch、边界时间戳、可辨识 Type/Name 文本）供 Task 6 固化调用（FR-8）。
  - history 文件始终以追加方式保留全部窗口行；覆写式 CSV（stats/failures/exceptions）继续只写当前窗口。
- **Acceptance Criteria Addressed**: AC-5
- **Test Requirements**:
  - `rule` TR-7.1: 经历两次重置、各窗口注入可区分请求量后解析 `_stats_history.csv`：表头含 Window 列；边界行数量=重置次数；数据行 Window 值为所属 epoch；按 Window 分组计数与注入量一一对应；文件保留全部窗口行；证据为 pytest 断言。
- **Completion Evidence**:
  - TR-7.1 通过：TestCsvStats::test_stats_history_window_column_boundary_rows_and_isolation——history 表头首列为 Window；两次重置产生两条 WINDOW_BOUNDARY 行；三窗口数据行分别标记 Window=0/1/2 且请求名与注入量一一对应；覆写式 _stats.csv 只反映当前窗口。
  - 实现要点：_stats_history_data_rows 的 epoch 取自 runner.window_registry.current_epoch（_current_window_epoch duck-typed）；_write_stats_history_row 提取为共用行写入器，被常规行/最终快照/迟到行三处复用；边界行为空占位、Type=WINDOW_BOUNDARY；迟到报告经独立 RequestStats 重建并按原始 epoch 写入，不触碰任何已固化快照。
  - 回归：随 test_stats.py 全套通过；ruff/mypy 通过。

## Task 8: 当前视图与 HTML 报告隔离
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 4
- **Description**:
  - 验证/补齐：`/stats/requests`、failures/exceptions、覆写式 CSV、HTML 报告均以当前窗口 RequestStats 为数据源；新窗口自带空 `history`，HTML 报告种子与 `swarmTemplateArgs.history` 仅当前窗口（FR-9）。
  - 确认缓存（memoize）在重置后不返回旧窗口快照（必要时在重置路径失效缓存）。
- **Acceptance Criteria Addressed**: AC-6（后端部分）
- **Test Requirements**:
  - `rule` TR-8.1: 旧窗口有数据→重置→新窗口独立数据后，/stats/requests 与各覆写式 CSV 的计数只等于新窗口数值；HTML 报告不含旧窗口；证据为 pytest（test_web 模式）断言。
- **Completion Evidence**:
  - TR-8.1 通过：新增 TestWebUI::test_current_views_isolated_after_window_reset——旧窗口 2 个 /old 并预热 memoize 缓存 → /stats/reset → 新窗口 1 个 /new：/stats/requests 返回 total num_requests=1、entries 仅 /new（证明缓存已失效而非返回旧快照）；/stats/requests/csv 覆写式导出仅含 /new；旧窗口对象自身 totals=2 未被改动。
  - 代码走查确认：/stats/requests（web.py）读 environment.runner.stats、get_html_report（html.py）读 environment.runner.stats 及其 history、swarmTemplateArgs.history 均取 environment.stats；cut/reset 经 _sync_environment_stats 使该引用指向当前窗口，新 RequestStats 自带空 history 与 start_time，覆写式 CSV 每周期 truncate 重写当前窗口。
  - 补齐：/stats/reset 中调用 request_stats.clear_cache()（memoize 支持），重置后首个请求强制重算。
  - 回归：2 个相关测试 passed；ruff All checks passed；mypy Success。

## Task 9: 前端图表 Reset 清空与边界标记
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None（可与后端任务并行；联调依赖 Task 3）
- **Description**:
  - [swarm.ts](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/a8fdb0d3-ae9f-4db2-afc5-c1b0e3778ed5/repo/locust/webui/src/redux/api/swarm.ts) resetStats mutation 成功后，dispatch 清空 charts 各时间序列并添加边界 marker（复用/新增 ui.slice action），使当前图表仅累积新窗口点（FR-10）。
  - 更新/新增组件测试与 mock。
- **Acceptance Criteria Addressed**: AC-6（前端部分）
- **Test Requirements**:
  - `rule` TR-9.1: 渲染带非空图表的运行态，触发 resetStats 成功后，charts 各序列被清空（长度归零/仅含新窗口后续点）且 markers 含边界时间；证据为 webui 测试（jest/vitest）断言。
- **Completion Evidence**:
  - TR-9.1 通过：ResetButton.test.tsx 新增 should clear chart series and add a boundary marker——预置非空图表（5 条序列含旧 ISO 时间点）→ 点击 Reset → mutation 成功后 currentRps/currentFailPerSec/totalAvgResponseTime/userCount/time 全部归零，markers 恰含 1 个有效 ISO 边界时间戳且不早于旧数据时间；后续轮询点经 useFetchStats 追加只属于新窗口。
  - 实现要点：ui.slice 新增 resetCharts reducer（清空除 markers 外全部序列并写入边界 marker）；resetStats mutation 的 onQueryStarted 在 queryFulfilled 成功后 dispatch；失败静默保留原图表。为规避 api ↔ ui.slice ↔ store 的循环依赖，ui.slice 改为在回调内动态 import（静态导入会在模块环中失效）。
  - 回归：webui 全量 40 files / 188 tests passed；eslint 无输出（clean）；tsc type-check 通过（markers[0] 加非空断言）。

## Task 10: 文档与 changelog
- **Status**: `pending`
- **Priority**: medium
- **Depends On**: Task 3, Task 6, Task 7
- **Description**:
  - 更新 docs：分布式重置窗口语义、`_stats_history.csv` 的 Window 列与边界行格式、宽限期配置项、混合版本降级说明（NFR-5）。
  - 更新 CHANGELOG.md（含 CSV 格式变更提示）。
- **Acceptance Criteria Addressed**: AC-9
- **Test Requirements**:
  - `rule` TR-10.1: docs 中可检索到窗口/Window 列/边界行/宽限期配置的说明，CHANGELOG 含本次变更与 CSV 格式提示；证据为文档内容检查。
  - `rubric` TR-10.2: 文档质量与一致性；scale 1-5；anchors 1=无文档或与实现矛盾，3=覆盖主要语义但有缺漏，5=语义/格式/配置/兼容性说明完整且与代码一致；threshold >= 4；证据为走查。

## 执行顺序建议
1. Task 1 → Task 2（基础原语与 worker 发送）
2. Task 7、Task 9 可与 Task 3/4 并行（独立信息边界，不同文件）
3. Task 3 → Task 4 → Task 5 → Task 6（协议主链）
4. Task 8 随 Task 4 后验证
5. Task 10 收尾
