# 分布式压测统计重置窗口化 - 产品需求文档

## Overview
- **Summary**: 为 Locust 分布式（master-worker）统计重置建立"窗口（window / epoch）"语义：每次重置形成全节点一致、无损的窗口边界；传输中的旧增量、迟到报告、离线节点回归数据均按 epoch 归入所属窗口；实时图表/当前视图与历史导出相互隔离。
- **Purpose**: 解决长时间压测中连续重置统计时，worker 在途旧增量回流到新结果、离线/迟到数据错配窗口、实时图表与历史 CSV 相互污染的问题。
- **Target Users**: 运行长时间、多次重置统计的分布式压测用户；依赖 `_stats_history.csv` 等历史导出做分窗口分析的用户；基于 `report_to_master` / `worker_report` 事件做自定义聚合的扩展开发者。

## Goals
- 每次重置在 master 与全部已连接 worker 上产生同一 epoch 的边界切割。
- 报告（增量 delta）携带 epoch，master 按 epoch 路由：旧 epoch 的在途/迟到数据永不进入新窗口。
- 重置期间发送失败、worker 短暂离线后的数据不丢失，并归入其所属窗口。
- 当前视图（/stats/requests、表格、实时图表、HTML 报告、覆写式 CSV）只反映当前窗口；历史导出（`_stats_history.csv`）跨窗口无损保留、逐行可归因。
- 已关闭窗口在宽限期后固化到历史导出并释放内存，长时间运行资源有界。

## Non-Goals
- 不实现跨节点物理时钟同步；epoch（而非时间戳）是窗口归因的唯一依据。
- 不改变请求级统计指标的数学定义（百分位、RPS、fail ratio 等计算方式不变）。
- 不提供跨窗口合并分析的查询服务或新 UI 页面（仅保证导出数据可按 Window 列归因）。
- 不保证混合版本集群（旧 worker 不携带 epoch）下的严格窗口语义，仅做安全降级。
- 不改变 master-worker 之间 spawn/stop/heartbeat 的既有协议语义。

## Background & Context
- 现状链路：
  - worker 每 `WORKER_REPORT_INTERVAL`（3s）触发 `report_to_master`，[stats.py](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/a8fdb0d3-ae9f-4db2-afc5-c1b0e3778ed5/repo/locust/stats.py#L818-L851) 中 `get_stripped_report()` 序列化并**立即清空**本地累加器（delta 报告），随后发送；master `on_worker_report` 将 delta `extend` 进唯一的内存 `RequestStats`。
  - [web.py](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/a8fdb0d3-ae9f-4db2-afc5-c1b0e3778ed5/repo/locust/web.py#L373-L380) 的 `/stats/reset` 只在 master 本地 `reset_all()`，**不通知 worker**。
- 由此产生的缺陷：
  1. **在途回流**：reset 时刻已发出、尚未到达的旧 delta，在 reset 后被 `extend` 进新窗口。
  2. **离线错配**：worker 断连期间未送达的增量（且现状"先 strip 后 send"，发送失败时增量直接丢失）在重连后进入当前窗口。
  3. **历史污染**：`_stats_history.csv` 追加写入但无窗口标识与边界行；`stats.history` 被清空后，新旧窗口行在同一文件中无法区分。
  4. **图表拼接**：前端按 2s 轮询在客户端追加图表序列，reset 后旧序列点仍在当前图表中，与新窗口数据连缀。
- 已确认的设计取舍：历史导出采用"新增 Window 列 + 边界行"；一致性采用"最终一致 epoch 路由"（重置不阻塞）；实时图表"清空仅显示新窗口"；旧窗口"宽限期后固化释放"。

## Functional Requirements
- **FR-1 窗口标识**：每次重置分配单调递增 epoch（首个窗口 epoch=0）。worker 的每条 stats 报告携带其所属 epoch；master 记录每个窗口的起止元数据（epoch、开始/关闭时间戳）。
- **FR-2 边界广播**：master 重置时关闭当前窗口、创建新窗口，并向全部已连接 worker 发送 `reset_stats` 消息（携带新 epoch 与边界时间戳）。
- **FR-3 worker 切割**：worker 收到 `reset_stats` 后原子切割：以旧 epoch 发送一份末次增量报告，随后清空累加器；此后所有报告标记新 epoch。
- **FR-4 epoch 路由**：master 收到报告按 epoch 路由到对应窗口聚合；当前视图仅暴露当前（最高）epoch 窗口。
- **FR-5 无损发送**：worker 只在报告发送成功后剥离（清除）已报告增量；发送失败时保留并在下个周期重试；`quit` 前发送末次报告。
- **FR-6 迟到/离线归因**：worker 重连后报告的 stale epoch 增量归入旧窗口；master 告知其当前 epoch，worker 据此完成切割后再产出新窗口数据。worker 离线期间跨越边界、其未观察到边界的增量全部归入旧 epoch（无损优先）。
- **FR-7 窗口宽限与固化**：已关闭窗口在宽限期内保留完整聚合以接收迟到报告；宽限期满后写入历史导出（含边界行与最终快照）并释放聚合内存。固化后才到达的该 epoch 报告，仍以该 epoch 追加写入历史导出，但不再改动已固化快照。
- **FR-8 历史导出格式**：`_stats_history.csv` 新增 `Window` 列（epoch 值）；每次重置写入边界标记行；追加写入，重置不删除、不覆盖既有行。
- **FR-9 当前视图隔离**：`/stats/requests`、failures/exceptions、覆写式 CSV（`_stats.csv`、`_failures.csv`、`_exceptions.csv`）、HTML 报告及其种子 `stats.history` 仅含当前窗口。
- **FR-10 前端图表**：Reset 成功后清空实时图表各序列并从空开始显示新窗口数据，同时添加边界标记；历史序列不与新窗口连缀。
- **FR-11 单机一致**：LocalRunner（非分布式）复用同一窗口切割原语，重置语义与分布式一致（无网络广播步骤）。
- **FR-12 兼容降级**：master 对不含 epoch 的报告按当前 epoch 安全接收，不崩溃；`reset_stats` 事件、`report_to_master` / `worker_report` 事件签名保持兼容。

## Non-Functional Requirements
- **NFR-1 性能**：每报告仅增加一个整数 epoch 字段；epoch 路由为 O(1) 字典查找，对吞吐与延迟无可观测影响。
- **NFR-2 资源有界**：master 内存占用不随窗口数量线性增长；仅当前窗口与宽限期内窗口持有完整聚合，窗口元数据为有界小记录。
- **NFR-3 可维护性**：窗口切割、epoch 路由、固化逻辑集中在共享模块/原语中，master/worker/local 复用，不重复实现。
- **NFR-4 可测试性**：窗口边界、在途、离线、发送失败场景均可用 pytest（现有 fake server / monkeypatch 模式）与前端测试确定性复现。
- **NFR-5 文档**：更新重置语义与 CSV 格式（Window 列、边界行）的用户文档与 changelog。

## Constraints
- **Technical**: Python 3.11+；gevent 并发模型；RPC 经 msgpack + ZMQ；行宽 120，ruff/mypy 检查通过；不引入新的第三方依赖。
- **Business**: 保持事件/API 向后兼容；CSV 新增列需在文档与 changelog 中明确标注。
- **Dependencies**: 仅依赖现有 `locust/rpc`、`locust/stats.py`、`locust/runners.py`、`locust/web.py`、webui 前端模块。

## Assumptions
- epoch 由 master 生成并作为唯一归因依据；不要求 worker 与 master 时钟同步。
- 宽限期默认 60s，可通过环境变量/配置调整；默认值需覆盖一个在途报告周期与常见短时网络抖动。
- 重置主要由 Web UI / HTTP API 触发；headless/library 用户可通过事件与 runner 原语触发同一流程。
- 离线 worker 未观察到边界，其回归首批数据整体归入旧 epoch 是可接受的无损归因。

## Acceptance Criteria

### AC-1: 在途旧增量不回流新窗口
- **Type**: `rule`
- **Given**: master 已有窗口 epoch=0 的聚合，一条携带 epoch=0 的 stats 报告已在传输中（尚未被 master 处理）
- **When**: master 执行重置（进入 epoch=1），随后处理该在途报告，并再收到 epoch=1 的新报告
- **Then**: epoch=1 窗口的 total/entries 不含在途报告的任何计数；在途报告全部计数体现在 epoch=0 窗口中
- **Pass Condition**: 断言新窗口 `num_requests/num_failures/total_response_time` 仅等于新报告数值；旧窗口聚合含在途报告数值
- **Evidence**: 新增 pytest 用例（master handle_message 直接注入 Message("stats", ...) 的确定性序列）输出与断言

### AC-2: 重置在全节点形成同一 epoch 边界
- **Type**: `rule`
- **Given**: 1 个 master 与 N 个已连接且运行中的 worker（fake RPC 或 monkeypatch）
- **When**: 调用 master 重置
- **Then**: master 向每个 worker 发出含新 epoch 的 `reset_stats`；每个 worker 产生恰好一份旧 epoch 末次增量，且此后报告携带新 epoch；worker 累加器已清空
- **Pass Condition**: 检查捕获的消息序列与后续 worker 报告的 epoch 字段全部等于新 epoch，末次报告 epoch 等于旧 epoch
- **Evidence**: pytest 用例对 worker `handle_message("reset_stats")` 行为及消息捕获的断言

### AC-3: 离线 worker 回归数据归入所属窗口
- **Type**: `rule`
- **Given**: worker 在重置前后与 master 断连（未收到 `reset_stats`），期间产生增量，随后重连
- **When**: worker 重连后发送携带 stale epoch 的首批报告
- **Then**: master 将该报告计入旧 epoch 窗口，当前窗口计数不受影响；master 回传当前 epoch，worker 切割后新数据进入新窗口
- **Pass Condition**: 旧窗口聚合含该报告；当前窗口不含；切割后报告 epoch=当前值
- **Evidence**: pytest 用例模拟 stale-epoch 报告与 epoch 再同步握手的断言

### AC-4: 报告发送失败不丢增量
- **Type**: `rule`
- **Given**: worker 已有自上次成功报告以来的增量
- **When**: `_send_stats` 发送阶段抛出 RPCError，并在下个周期恢复正常
- **Then**: 首次报告的增量仍保留在累加器中，恢复后重试送达，master 侧该窗口总计数恰好等于实际发生数（无重复、无丢失）
- **Pass Condition**: 成功送达的报告计数 = 实际 log 的计数；失败后本地累加器未被清除
- **Evidence**: pytest 用例 monkeypatch client.send 首次抛错、统计恢复后聚合值

### AC-5: 历史导出无损、可归因、含边界
- **Type**: `rule`
- **Given**: master 启用 CSV（含 history 文件），经历至少两次重置、每个窗口有可区分的请求量
- **When**: 检查 `_stats_history.csv`
- **Then**: 表头含 `Window` 列；每次重置存在边界标记行；数据行 Window 值为其所属 epoch；按 Window 过滤得到的计数与对应窗口一致；文件保留全部窗口的行（重置未删除/截断历史行）
- **Pass Condition**: 解析 CSV 的列检查、边界行数量=重置次数、按 Window 分组计数与窗口注入量一一对应
- **Evidence**: pytest 用例解析 StatsCSVFileWriter 产出文件的断言

### AC-6: 当前视图仅含当前窗口
- **Type**: `rule`
- **Given**: 旧窗口有数据，已执行重置，新窗口有独立数据
- **When**: 请求 `/stats/requests`、下载 `_stats.csv`/`_failures.csv`/`_exceptions.csv`、生成 HTML 报告
- **Then**: 所有当前视图的计数只反映新窗口；旧窗口计数不出现；前端 Reset 成功后图表序列被清空，仅累积新窗口点且存在边界标记
- **Pass Condition**: 端点与文件断言仅含新窗口数值；前端测试断言图表序列长度重置并含 marker
- **Evidence**: pytest（test_web 模式）与 webui 组件测试断言

### AC-7: 长时间运行资源有界
- **Type**: `rubric`
- **Dimension**: master 内存/对象数量对窗口数的有界性
- **Scale**: 1-5
- **Anchors**: 1 = 每个窗口完整聚合常驻、内存随窗口数线性增长；3 = 旧窗口释放聚合但仍残留明显冗余对象；5 = 仅当前窗口+宽限期窗口持有完整聚合，旧窗口仅留有界元数据
- **Pass Threshold**: >= 4
- **Evidence**: 连续多次重置（超过宽限周期）后对 master 持有的 RequestStats/StatsEntry 数量的检查或代码走查证据

### AC-8: 向后兼容与扩展安全
- **Type**: `rubric`
- **Dimension**: 混合版本与自定义事件用户的兼容程度
- **Scale**: 1-5
- **Anchors**: 1 = 缺 epoch 报告导致 master 异常或事件签名破坏；3 = 不崩溃但自定义用户数据丢失；5 = 缺 epoch 安全降级为当前 epoch，既有事件钩子与自定义消息不受影响
- **Pass Threshold**: >= 4
- **Evidence**: 注入无 epoch 字段报告的 pytest 结果 + 对事件钩子既有测试全部通过的证据

### AC-9: 语义实现一致性与文档完整性
- **Type**: `rubric`
- **Dimension**: 窗口原语复用度与用户文档质量
- **Scale**: 1-5
- **Anchors**: 1 = local/master/worker 各自实现、行为分叉且无文档；3 = 逻辑复用但有轻微分叉或文档不全；5 = 三类 runner 复用同一切割/路由原语，CSV 新列与边界行、重置语义均有文档与 changelog
- **Pass Threshold**: >= 4
- **Evidence**: 代码走查 + docs 与 CHANGELOG 更新证据

## Open Questions
- [x] 历史窗口导出形式：新增 Window 列 + 边界行（用户确认推荐项）
- [x] 一致性语义：最终一致 epoch 路由，重置不阻塞（用户确认推荐项）
- [x] 实时图表：清空仅显示新窗口 + 边界标记（用户确认推荐项）
- [x] 旧窗口保留：宽限期（默认 60s 可配置）后固化释放（用户确认推荐项）
- [ ] 边界行的具体文本/字段布局（实现阶段确定，需在文档中说明）
- [ ] Window 列在表头中的位置（建议首列以利于过滤；以最小破坏现有解析为准）
