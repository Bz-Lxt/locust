# 分布式执行清单一致性校验（Manifest Consistency Gate）- 产品需求文档

## Overview
- **Summary**: 在 Locust 分布式压测中，为 master/worker 引入"完整执行清单（manifest）"指纹校验：worker 在连接、重连与运行中热加入时必须上报其本地加载的脚本与用户定义指纹，由 master 与自身参考清单比对；不一致节点在任何流量产生之前被隔离（quarantine），健康节点继续按一致定义运行。
- **Purpose**: 防止脚本版本、User/Shape 定义或本地加载内容不同的 worker 混入集群，悄然产生异质流量、污染统计数据。
- **Target Users**: 分布式 Locust 压测的执行者与平台维护者（尤其多机、Docker/K8s、滚动升级、运行中扩容场景）。

## Goals
- worker 准入（首次连接、重连、热加入）必须先通过执行清单一致性校验。
- 开跑前确保只有一致节点能收到 spawn 指令；不一致节点零流量。
- 不一致节点被隔离且可观测（日志、API、Web UI），修复后重连可自动恢复准入。
- 健康节点的运行、再平衡与统计不被隔离节点阻断。
- 提供明确的旧版本 worker 兼容策略与 CI/K8s 友好的严格退出开关。

## Non-Goals
- 不校验运行时由 master 权威下发的配置（host、stop_timeout、tags/exclude_tags、parsed_options 自定义参数、每类用户数）——这些已在 `spawn` 消息中下发并由 worker 应用，纳入指纹会造成跨环境误报。
- 不向 worker 推送/同步 locustfile 内容（现有 `-f -` 下载机制保持不变；本特性只做校验，不做修复）。
- 不校验 locustfile 之外第三方依赖包/解释器环境的差异（如 requests 库版本），仅覆盖被加载的 locustfile 源文件。
- 不改变 worker 缺省权重调度、再平衡等既有算法。
- 不做 master 端 UI 上"一键剔除/恢复"操作（本期只做自动隔离与重连恢复）。

## Background & Context
- 现状：`WorkerRunner.connect_to_master()` 发送 `client_ready`（载荷仅为 Locust 版本字符串）；`MasterRunner.handle_message` 对版本差异仅记录 debug/warning 日志即发 `ack` 放行，worker 立即进入 `clients` 池并可被加入 `UsersDispatcher`（[runners.py L1017-L1050](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/83427937-d5b6-4d0c-afbb-1cba317dd11c/repo/locust/runners.py#L1017-L1050)）。
- 重连路径：RPC 异常时 master 发 `reconnect`、worker `reset_connection()` 后重发 `client_ready`；心跳丢失置 `STATE_MISSING`，心跳恢复后 `add_worker` 回到调度；热加入（运行中新 worker 连接）与首连共用同一 `client_ready` 分支。所有路径当前均无脚本一致性校验。
- master 在 `spawn` 消息中下发 timestamp/user_classes_count/host/stop_timeout/parsed_options（[runners.py L791-L806](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/83427937-d5b6-4d0c-afbb-1cba317dd11c/repo/locust/runners.py#L791-L806)），但 User 类任务体、wait_time、Shape 逻辑等代码定义无法被下发覆盖。
- worker 的 locustfile 无论来自本地文件、URL 还是从 master 下载，最终都落盘为普通 .py 并被 import，因此可通过已加载类的模块文件（`inspect.getfile`）稳定取得源码字节。
- 版本策略现状：仅主次版本号前缀（`__version__[0:4]`）不同才 warning，补丁版本差异只 debug。本特性延续该容忍度。

## Functional Requirements

- **FR-1 清单生成**：每个节点能基于本地已加载的 `available_user_classes` 与 `available_shape_classes` 生成 manifest：包含 locustfile 源文件 SHA-256 集合、User 类结构化元数据（类名、weight、fixed_count）、Shape 类名、主次版本号前缀；manifest 体积小（仅哈希与元数据，不含源码）。
- **FR-2 指纹比对与差异原因**：master 持有自身参考 manifest；比对时先比整体指纹，不一致时输出可读的差异原因（文件哈希不同/缺失/多余文件、User 类缺失或 weight/fixed_count 不同、Shape 类不同、版本前缀不同）。
- **FR-3 连接准入**：worker 在 `client_ready` 中携带 manifest；master 仅对匹配节点发 `ack` 并按现有逻辑注册入池/加入调度器；不匹配节点收到新消息 `manifest_rejected`（含差异原因），进入 `quarantined` 状态：保留连接与心跳跟踪，但绝不加入 dispatcher、不计入健康 worker 数。
- **FR-4 无清单旧 worker 策略**：`client_ready` 载荷为裸版本字符串时视为旧 worker；默认按不一致隔离（原因标明"未上报 manifest"），master 命令行开关 `--allow-legacy-workers`（含环境变量）放行但必须记录醒目 warning。
- **FR-5 开跑闸门**：`MasterRunner.start()` 只对 ready/spawning/running 的健康节点派发 spawn；存在被隔离节点时启动日志汇总健康/隔离数量；健康节点数为 0 时拒绝启动（沿用现有 warning+return 行为）。
- **FR-6 热加入校验**：测试运行中（STATE_RUNNING/SPAWNING）新连接的 worker 必须先过校验；不一致者立即隔离、不触发再平衡、不改变目标用户数，已有用户与流量不受影响；一致者沿用现有热加入/再平衡行为。
- **FR-7 重连校验**：`reconnect`/`reset_connection` 后的重复 `client_ready`、以及心跳缺失后恢复，均须重新校验；修复脚本后重连（相同 client_id）可被重新接纳并解除隔离；不一致节点不能借心跳或重复消息复活进入调度。
- **FR-8 worker 隔离语义**：收到 `manifest_rejected` 后 worker 记录显著 error 日志（含原因）并进入隔离态：保持连接与心跳，对任何 `spawn` 消息拒绝执行（防御性二次防线，master 正常不会发送）。命令行开关 `--strict-manifest`（含环境变量）使 worker 在被拒后以非零码退出，便于 CI/K8s 崩溃重启。
- **FR-9 重新接纳**：被隔离 worker 若随后发送匹配的 `client_ready`（重连），master 发 `ack`，worker 清除隔离态恢复正常；master 侧同步清除隔离状态与原因。
- **FR-10 可观测性**：`/stats/requests` 的每个 worker 条目新增 `quarantine_reasons` 字段（健康节点为空），顶层新增 `quarantined_worker_count`；Web UI 分布式监控区域显示被隔离节点数量与（悬停可见的）原因。
- **FR-11 事件钩子**：新增 `events.worker_manifest_rejected`（参数 client_id、reasons、worker_manifest、reference_manifest），供扩展与测试订阅。
- **FR-12 生命周期完整**：master `quit`/`stop` 广播覆盖被隔离节点（保持连接管理完整）；被隔离节点正常参与心跳 liveness 超时与缺失/死亡清理逻辑，但其状态变化不得使其进入调度池。

## Non-Functional Requirements
- **NFR-1 性能**：指纹在节点本地一次性计算（启动/连接路径），源文件读取为少量小文件；不向热路径（心跳、stats 上报、spawn 循环）增加网络往返；新增消息体仅含哈希与元数据。
- **NFR-2 并发安全**：实现遵循 gevent 模型，无线程阻塞；消息经现有 msgpack 序列化，载荷为可 JSON/msgpack 序列化的基本类型。
- **NFR-3 兼容性**：新 worker↔旧 master（旧 master 对 dict 载荷的 client_ready 行为需不崩溃）、旧 worker↔新 master（FR-4 策略）混合时行为明确；现有 `locust/test/test_runners.py` 全部既有用例不回归。
- **NFR-4 代码质量**：通过 `uv run ruff check locust/` 与 `uv run mypy locust/`；Python 3.11+ 语法；行宽 120。
- **NFR-5 文档**：在 `docs/running-distributed.rst` 说明一致性校验行为、两个开关与升级注意事项。

## Constraints
- **Technical**: 只能使用现有 RPC（zmq/msgpack `Message`）通道与 gevent 并发模型；不引入新依赖；前端使用现有 MUI/Redux/RTK Query 栈。
- **Business**: 缺省行为必须安全（默认隔离不一致/旧节点），但不能破坏 master 分发 locustfile（`-f -`）的标准用法——该用法下文件内容天然一致，指纹必然匹配。
- **Dependencies**: 无新增第三方依赖。

## Assumptions
- 用户跳过澄清问题，采用推荐默认：
  1. 指纹范围＝locustfile 源文件内容哈希 + 主次版本号前缀 + 结构化元数据；可下发运行时配置不纳入。
  2. 不一致 worker 默认空闲保活+醒目告警；提供 `--strict-manifest` 使其非零退出。
  3. 旧版无指纹 worker 默认隔离；提供 `--allow-legacy-workers` 显式放行。
  4. 可观测性包含后端 API 与 Web UI 展示。
- master 自身加载的 locustfile 即参考定义（信任 master）。
- locustfile 进程运行期内不会被原地修改；源文件读取在进程启动后可重复（master 参考指纹可缓存）。
- 被加载类的模块均可通过 `inspect.getfile` 定位到常规 .py 文件（本地、下载到 tempdir、URL 下载均成立）；极少数动态/内联定义回退到 `inspect.getsource(类)` 哈希。

## Acceptance Criteria

### AC-1: 清单指纹正确反映定义差异
- **Type**: `rule`
- **Given**: 两个加载了相同 locustfile（相同 User/Shape 定义）的 Environment
- **When**: 分别生成 manifest
- **Then**: 二者整体指纹相同；当且仅当出现以下任一变化时指纹不同：locustfile 源文件字节变化（含同名 task 方法体实现变化）、User 类集合增删、类 weight/fixed_count 不同、Shape 类不同、主次版本前缀不同
- **Pass Condition**: 指纹相等/不等断言全部成立，且比对结果能对每种差异给出对应可读原因（区分文件差异/类差异/版本差异）
- **Evidence**: 新增 `locust/test/test_manifest.py` 单元测试通过

### AC-2: 首次连接按指纹准入与隔离
- **Type**: `rule`
- **Given**: 一个 master 与若干 worker（部分 manifest 匹配、部分不匹配）
- **When**: worker 发送 `client_ready`
- **Then**: 匹配者收到 `ack` 且进入 ready 池；不匹配者收到 `manifest_rejected`（含原因）、节点状态为 `quarantined`、不在 dispatcher 中、不计入 `worker_count`，master 日志与 `events.worker_manifest_rejected` 事件可观察
- **Pass Condition**: 测试断言 ack/manifest_rejected 消息、clients 状态、dispatcher 成员、事件触发均符合
- **Evidence**: `locust/test/test_runners.py` 新增用例通过

### AC-3: 开跑闸门只向健康一致节点派发
- **Type**: `rule`
- **Given**: master 同时连着健康节点与隔离节点
- **When**: 调用 `MasterRunner.start()`
- **Then**: 仅健康节点收到 `spawn` 消息，隔离节点零条 spawn；健康节点能正常完成 spawn 上报；若全部节点均被隔离（健康数为 0），start 拒绝执行并告警，不发送任何 spawn
- **Pass Condition**: mock RPC outbox 中断言隔离节点无 spawn；零健康用例断言无 spawn 且有告警
- **Evidence**: `locust/test/test_runners.py` 新增用例通过

### AC-4: 运行中热加入先校验
- **Type**: `rule`
- **Given**: master 处于 STATE_RUNNING 且已有健康 worker 在跑
- **When**: 一个 manifest 不匹配的新 worker 热加入
- **Then**: 该节点被隔离、未被 add_worker 进 dispatcher、未触发再平衡、目标用户数与既有 worker 的用户数不变；匹配的新 worker 则按现有行为加入
- **Pass Condition**: 测试断言 dispatcher 成员、目标用户数不变、无 spawn 发往隔离节点；匹配热加入行为与既有测试一致
- **Evidence**: `locust/test/test_runners.py` 新增用例通过

### AC-5: 重连与心跳恢复必须重新校验且不可借心跳复活
- **Type**: `rule`
- **Given**: 一个被隔离 worker
- **When**: 其持续发送心跳、或经 `reconnect` 重发 `client_ready`（先不一致、修复后一致）
- **Then**: 心跳不改变其隔离状态、不进入调度；不一致重连仍被拒；修复为一致 manifest 后重连收到 `ack` 并恢复为正常可调度节点
- **Pass Condition**: 心跳后状态仍 quarantined；重连被拒/被接纳两个分支断言成立
- **Evidence**: `locust/test/test_runners.py` 新增用例通过

### AC-6: worker 端隔离语义与 strict 退出
- **Type**: `rule`
- **Given**: worker 收到 `manifest_rejected`
- **When**: 默认模式下随后收到 `spawn`；以及开启 `--strict-manifest` 时收到拒绝
- **Then**: 默认模式 worker 保持连接与心跳、记录 error、拒绝执行 spawn（不创建任何用户）；strict 模式 worker 输出错误后以非零退出码终止
- **Pass Condition**: 默认模式断言无用户产生且 spawn 被忽略并告警；strict 模式断言进程退出码非零
- **Evidence**: `locust/test/test_runners.py` 新增用例通过

### AC-7: 旧版无指纹 worker 兼容策略
- **Type**: `rule`
- **Given**: worker 发送裸版本字符串的旧格式 `client_ready`
- **When**: master 默认配置 / 开启 `--allow-legacy-workers`
- **Then**: 默认被隔离且原因标明未上报 manifest；开关开启时正常收 `ack` 入池且 master 记录醒目 warning
- **Pass Condition**: 两种配置下的状态、消息、日志断言成立
- **Evidence**: `locust/test/test_runners.py` 新增用例通过

### AC-8: 隔离状态可观测（API 与 Web UI）
- **Type**: `rule`
- **Given**: 集群存在被隔离节点
- **When**: 请求 `/stats/requests` 并查看分布式监控 UI
- **Then**: worker 条目含 `state="quarantined"` 与 `quarantine_reasons`，顶层 `quarantined_worker_count` 正确；UI 显示隔离数量，悬停可见原因
- **Pass Condition**: `test_web.py` 断言新字段；前端组件测试/类型定义覆盖隔离徽标
- **Evidence**: 后端与前端测试通过

### AC-9: 通信与性能开销克制
- **Type**: `rubric`
- **Dimension**: 引入机制的通信/计算开销与架构契合度
- **Scale**: 1-5
- **Anchors**: 1 = 在心跳或 stats 热路径增加网络往返或传输完整源码；3 = 仅连接期传元数据但存在冗余重复计算或阻塞 IO；5 = 仅连接/重连期传输一次小型哈希载荷，参考指纹缓存，热路径零新增往返，全程 gevent 友好
- **Pass Threshold**: >= 4
- **Evidence**: 代码审查 + 消息载荷检查（测试中检查 client_ready 载荷仅含基本类型且不含源码字符串）

### AC-10: 工程质量与回归安全
- **Type**: `rubric`
- **Dimension**: 测试覆盖、静态检查、文档与既有行为兼容性
- **Scale**: 1-5
- **Anchors**: 1 = 既有分布式测试回归或缺少新场景测试；3 = 核心路径有测试但 lint/type/文档有缺；5 = 新增场景全覆盖（准入/隔离/热加入/重连/旧版/UI）、ruff 与 mypy 干净、文档更新、`locust/test/test_runners.py` 全绿
- **Pass Threshold**: >= 4
- **Evidence**: ruff/mypy 输出、pytest 结果、docs diff

## Open Questions
- 无（澄清问题被跳过，按 Assumptions 中的推荐默认执行；如审批阶段有不同偏好可在此修订并回到 Specify）。
