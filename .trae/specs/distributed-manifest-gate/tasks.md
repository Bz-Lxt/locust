# 分布式执行清单一致性校验 - 实施计划

实施顺序按依赖关系排列。除特别说明外，测试均使用 pytest，Python 文件遵循 ruff(120) / mypy，前端遵循现有 eslint/prettier。

## Task 1: manifest 生成与比对模块
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Completion Evidence**:
  - TR-1.1/1.2（rule）: `uv run pytest locust/test/test_manifest.py` → 9 passed，覆盖相同指纹、task 方法体变更、类增删、weight/fixed_count、Shape、版本前缀、动态类回退。
  - TR-1.3（rule）: `test_manifest_is_serializable_and_contains_no_source_code` 断言 json/msgpack 可序列化且源码 token 不出现在载荷中，哈希长度 64。
  - TR-1.4（rubric）: 自评 5/5——build 时逐文件读一次并按路径去重，比对为纯函数；证据：[manifest.py](file:///Users/xavskye/workspace/Projects/Phecda/SWEMarkup/data/runs/83427937-d5b6-4d0c-afbb-1cba317dd11c/repo/locust/manifest.py) 代码审查。
  - ruff All checks passed；mypy `Success: no issues found in 1 source file`。
- **Description**:
  - 新建 `locust/manifest.py`：
    - `build_manifest(environment: Environment) -> dict`：收集 `environment.available_user_classes`、`environment.available_shape_classes` 所涉及的去重模块文件（`inspect.getfile`，失败时对类回退 `inspect.getsource`），逐文件计算 SHA-256；User 类元数据（name/weight/fixed_count）按名排序；Shape 类名；版本前缀取 `locust.__version__[0:4]`。
    - 载荷形如 `{"version_prefix": "2.xx", "fingerprint": "<sha256 hex>", "files": [{"filename": str, "sha256": str}], "user_classes": [{"name", "weight", "fixed_count"}], "shape_class": str | None}`；fingerprint 为对规范化（sort_keys、分隔符固定）后的上述内容（不含 fingerprint 自身）的 SHA-256。
    - `compare_manifests(reference, candidate) -> list[str]`：先比 version_prefix，再比文件集合（内容不同/缺失/多余）、User 类集合与 weight/fixed_count、shape_class；返回可读原因字符串列表；空列表即一致。
  - 新建 `locust/test/test_manifest.py`：同定义相等；改 task 方法体/增删类/改 weight/fixed_count/换 Shape/版本前缀不等；原因文本可区分差异类别；动态回退分支可工作（用内联定义类模拟 getfile 失败）；输出可被 msgpack/JSON 序列化且不含文件源码全文。
- **Acceptance Criteria Addressed**: AC-1, AC-9
- **Test Requirements**:
  - `rule` TR-1.1: 相同 locustfile 生成的两个 manifest fingerprint 相等；五类变更（文件字节/类集合/weight/fixed_count/Shape/版本前缀）任一发生时 fingerprint 不等。证据：`uv run pytest locust/test/test_manifest.py` 通过。
  - `rule` TR-1.2: `compare_manifests` 对每类差异返回非空且含对应关键词（文件/类/Shape/版本）的原因，一致时返回空列表。证据：同上。
  - `rule` TR-1.3: manifest 仅含 str/int/None/list/dict 基本类型，可 `json.dumps`/msgpack 序列化，且任何字段不包含源文件源码片段。证据：测试断言。
  - `rubric` TR-1.4: 计算开销与实现简洁性；scale 1-5；1=每次比对重复读文件/递归全环境；3=可工作但有冗余；5=读文件仅在 build 时一次、结构无冗余；threshold >= 4；证据：代码审查。
- **Notes**: master 参考指纹将在 Task 3 缓存；本任务只做纯函数模块，不碰 RPC。

## Task 2: 状态、事件与命令行开关基础设施
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Completion Evidence**:
  - TR-2.1（rule）: `TestManifestInfrastructure`（test_runners.py）3 个用例通过：STATE_QUARANTINED 导入、WorkerNodes.quarantined 过滤、WorkerNode 默认 reasons==[]。
  - TR-2.2（rule）: `test_manifest_consistency_options_default/flags/env` 3 个用例通过，覆盖缺省 False、CLI flag、LOCUST_ALLOW_LEGACY_WORKERS/LOCUST_STRICT_MANIFEST 环境变量。
  - TR-2.3（rule）: `test_worker_manifest_rejected_event` 验证 hook 可监听且四参数齐全。
  - 命令：`uv run pytest locust/test/test_parser.py locust/test/test_runners.py -k "manifest or Manifest"` → 6 passed；ruff All checks passed；mypy 3 个改动源文件 Success。1
- **Description**:
  - `locust/runners.py`：状态常量新增 `STATE_QUARANTINED = "quarantined"`（与现有 STATE_* 同处定义并导出）；`WorkerNode` 新增 `quarantine_reasons: list[str]`（默认空）；`WorkerNodes` 新增 `quarantined` 属性（get_by_state）。
  - `locust/event.py`：新增 `worker_manifest_rejected: EventHook`（文档字符串说明 client_id/reasons/worker_manifest/reference_manifest 参数）并加入 Events 初始化。
  - `locust/argument_parser.py`：新增 `--allow-legacy-workers`（store_true，env_var=`LOCUST_ALLOW_LEGACY_WORKERS`，master 侧语义）与 `--strict-manifest`（store_true，env_var=`LOCUST_STRICT_MANIFEST`，worker 侧语义），help 文案说明用途。
  - `MasterRunner` 增加 `allow_legacy_workers()` 读取方法（仿照现有 `rebalancing_enabled()`）。
- **Acceptance Criteria Addressed**: AC-2, AC-6, AC-7
- **Test Requirements**:
  - `rule` TR-2.1: `STATE_QUARANTINED` 可导入，`WorkerNodes.quarantined` 正确过滤；`WorkerNode` 默认 `quarantine_reasons == []`。证据：test_runners 新用例。
  - `rule` TR-2.2: 两个 CLI 开关可经命令行与环境变量解析为 True，缺省为 False。证据：`locust/test/test_parser.py` 新用例通过。
  - `rule` TR-2.3: `events.worker_manifest_rejected` 可 add_listener/fire 且签名含 client_id/reasons/worker_manifest/reference_manifest。证据：测试断言。

## Task 3: master 连接准入与隔离登记
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - `MasterRunner`：
    - 缓存参考 manifest（`functools.lru_cache` 风格的惰性属性/方法，基于 `self.environment`，进程内一次构建）。
    - 重写 `handle_message` 的 `client_ready` 分支：
      - `msg.data` 为 dict（新协议，含 `manifest`）：比对；匹配 → 现有 ack/入池/fire worker_connect 路径；不匹配 → 发 `manifest_rejected`（data 含 `reasons`、`reference_manifest`、`worker_manifest` 摘要），登记 `WorkerNode(state=STATE_QUARANTINED, quarantine_reasons=...)`，保留在 `self.clients`（替换同名旧条目规则与现状一致），**不**加入 `_users_dispatcher`、**不**触发热加入再平衡；fire `worker_manifest_rejected`；记录 error 级日志。
      - `msg.data` 为裸字符串（旧协议）：版本日志逻辑保留；默认按 quarantine 处理（原因 "worker did not report an execution manifest (legacy worker?)"，不发 manifest_rejected 给老节点以免其困惑——老节点收到未知消息只 warning；仍可发送以便未来 worker，统一发送也可，实施时保持老节点安全：发送消息对老 worker 无副作用，采用统一发送并容忍）；`--allow-legacy-workers` 开启时走准入路径并 fire worker_connect，同时 logger.warning 醒目提示。
      - `msg.data` 为 None/其他：沿用现有 pre-2.0 报错返回。
    - 准入成功时若该 client 此前处于 quarantined（重连恢复），清除隔离原因并按正常节点处理（重新 add_worker 到既有 dispatcher，规则与当前热加入一致）。
    - heartbeat 分支：quarantined 节点只更新 heartbeat liveness/cpu/memory 与事件，**不**用 worker 上报的 state 覆盖 master 侧 STATE_QUARANTINED，**不**触发 self-heal 的 add_worker。
    - `quit()` 广播、心跳死亡清理、`client_stopped`/`quit` 移除逻辑对 quarantined 节点保持完整工作（移除时若不在 dispatcher 则不调用/安全调用 remove_worker）。
- **Acceptance Criteria Addressed**: AC-2, AC-5, AC-7, AC-9
- **Test Requirements**:
  - `rule` TR-3.1: manifest 匹配的 client_ready 收到 ack 且节点出现在 ready 池、worker_count 计入；不匹配的收到 manifest_rejected、状态 quarantined、quarantine_reasons 非空、worker_count 不计入、`worker_manifest_rejected` 事件触发一次。证据：test_runners 新用例（mocked_rpc）。
  - `rule` TR-3.2: 旧格式 client_ready 默认被隔离；开启 allow_legacy_workers 后收 ack 入池且有 warning 日志。证据：test_runners 新用例。
  - `rule` TR-3.3: 被隔离节点持续心跳后仍为 quarantined 且不在 dispatcher；节点死亡清理后从 clients 移除且不引发异常。证据：test_runners 新用例。
  - `rule` TR-3.4: master 参考 manifest 只构建一次（缓存），client_ready 载荷不被写回/修改。证据：测试 spy/断言。
  - `rubric` TR-3.5: 与现有 client_ready 行为的兼容性（匹配路径行为与现状逐行等价，除新增校验外不引入额外观测差异）；scale 1-5；anchors 1=破坏现有热加入/重复 ready 用例；3=可用但分支重复混乱；5=分支清晰且现有用例全绿；threshold >= 4；证据：全量 test_runners 通过 + 代码审查。

## Task 4: 开跑闸门与热加入/重连调度边界
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 3
- **Description**:
  - `MasterRunner.start()`：active_workers 计算天然排除 quarantined（ready/spawning/running）；新增启动日志汇总 `N healthy / M quarantined`；当健康数为 0 时维持现有 "can't start ... before at least one worker" 警告并 return（消息体现全部被隔离）。
  - 心跳 self-heal、missing 清理、`client_stopped`/`quit` 后的再平衡触发点：确保仅健康节点参与 `add_worker`/`remove_worker`；quarantined 节点永不传给 dispatcher。
  - `check_stopped()`：确认 STATE_QUARANTINED 不会误判（其不属于 RUNNING/SPAWNING/INIT，第二分支已覆盖），补充测试锁定行为。
  - 运行中热加入不一致节点：Task 3 已不 add_worker，本任务补端到端断言（目标用户数、既有 worker 用户数、再平衡均无变化）。
  - 重连恢复：被隔离 client_id 以匹配 manifest 重发 client_ready 后恢复为 ready/running 可调度状态，running 中按现有规则触发 add_worker/再平衡。
- **Acceptance Criteria Addressed**: AC-3, AC-4, AC-5
- **Test Requirements**:
  - `rule` TR-4.1: 混合集群 start() 后，mock outbox 中所有 spawn 消息目标仅为健康节点；隔离节点零 spawn；日志含健康/隔离数量。证据：test_runners 新用例。
  - `rule` TR-4.2: 全部节点隔离时 start() 不发任何 spawn 且返回（状态不进入 spawning）。证据：测试断言。
  - `rule` TR-4.3: STATE_RUNNING 下不一致热加入不改变 target_user_count、不触发 dispatcher 新分发；匹配热加入沿用现有行为（与现存热加入用例一致）。证据：test_runners 新用例。
  - `rule` TR-4.4: 被隔离节点修复 manifest 重连后收到 ack、状态恢复正常并能在随后的 start/再平衡中收到 spawn；隔离节点单纯重复发送不匹配 client_ready 不能复活。证据：test_runners 新用例。
  - `rule` TR-4.5: 存在 quarantined 节点时 stop()/check_stopped() 行为正确，不被隔离节点卡住。证据：测试断言。

## Task 5: worker 端上报、隔离与重新接纳
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 3
- **Description**:
  - `WorkerRunner`：
    - `connect_to_master()` 的 `client_ready` 载荷改为 `{"version": __version__, "manifest": build_manifest(environment)}`。
    - 新增 `self.quarantined = False` 与 `self.quarantine_reasons`；`handle_message` 新增 `manifest_rejected` 分支：记录 error 日志（含原因），置 quarantine 状态（`worker_state = STATE_QUARANTINED`），`connection_event.set()`（连接已建立，保持心跳/接收通道，避免无限重连刷屏）；若 `parsed_options.strict_manifest` 为真则打印原因后以非零码退出（走 `sys.exit(1)`/Environment quitting 路径，确保日志 flush）。
    - `ack` 分支：清除 quarantined 与原因、恢复 `worker_state = STATE_INIT`（重新接纳）。
    - `spawn` 分支防御：quarantined 时忽略并 warning，不回 spawning、不创建用户。
    - heartbeat 上报 state 时允许发送 STATE_QUARANTINED（master 不会用其覆盖隔离态）。
  - 注意新 worker↔旧 master：旧 master 不校验 dict 载荷（Python `dict != version string` 会走 warning 分支且仍发 ack——核对旧逻辑 `msg.data != __version__` 对 dict 仅触发 warning，不阻断），无需 worker 侧额外处理，补一个兼容性测试锁定。
- **Acceptance Criteria Addressed**: AC-5, AC-6, AC-9
- **Test Requirements**:
  - `rule` TR-5.1: worker 启动发送的 client_ready 载荷包含 version 与 manifest（manifest 通过 build_manifest 生成），不含源码全文。证据：test_runners 新用例。
  - `rule` TR-5.2: 收到 manifest_rejected（非 strict）后 worker_state=quarantined、connected 保持 True、随后 spawn 被忽略（无 spawning 消息回 master、无用户启动）并产生 warning；收到 ack 后隔离清除。证据：test_runners 新用例。
  - `rule` TR-5.3: strict 模式下收到 manifest_rejected 进程以非零码退出（mock sys.exit/断言调用与退出码）。证据：test_runners 新用例。
  - `rule` TR-5.4: 新 worker（dict 载荷）连旧风格 master（只回 ack）时可正常进入连接完成。证据：测试用例。
  - `rubric` TR-5.5: worker 隔离期资源行为（心跳/统计上报继续但不产生用户流量，无异常重试风暴）；scale 1-5；anchors 1=被拒后无限重连刷日志；3=保活但有明显冗余日志；5=静默保活+周期性有限提示；threshold >= 4；证据：测试中对连接重试次数的断言 + 代码审查。

## Task 6: API 与 Web UI 可观测性
- **Status**: `pending`
- **Priority**: medium
- **Depends On**: Task 4
- **Description**:
  - `locust/web.py`：`/stats/requests` worker 条目增加 `"quarantine_reasons"`（健康节点为 `[]`），报告顶层增加 `quarantined_worker_count`；`/worker-count` 语义保持为健康节点数（SwarmForm 用其判断可开跑），必要时在 `/swarm` 状态或另一字段提供隔离数（优先复用 stats 轮询，不新增轮询）。
  - 前端：`ui.types.ts` 的 `ISwarmWorker` 增加 quarantine_reasons、stats 响应增加 quarantinedWorkerCount；`useFetchStats.ts`/ui slice 透传；`SwarmMonitor.tsx` 在 Workers 旁显示红色 "N quarantined" 徽标，Tooltip 列出各节点原因；更新 `SwarmMonitor.test.tsx` 与 mock。
- **Acceptance Criteria Addressed**: AC-8
- **Test Requirements**:
  - `rule` TR-6.1: test_web.py 中断言 worker 条目含 quarantine_reasons、顶层 quarantined_worker_count 计数正确（0/正值两种）。证据：`uv run pytest locust/test/test_web.py -k ...` 通过。
  - `rule` TR-6.2: SwarmMonitor 在存在隔离节点时渲染徽标且 Tooltip 含原因文本；无隔离时不渲染。证据：前端组件测试通过（yarn test 或项目既有测试命令）。
  - `rule` TR-6.3: worker-count 与 SwarmForm 的可开跑判断不把隔离节点计为可用 worker。证据：后端测试断言健康计数。

## Task 7: 文档与全量验证
- **Status**: `pending`
- **Priority**: medium
- **Depends On**: Task 6
- **Description**:
  - `docs/running-distributed.rst`：新增"执行清单一致性校验"小节：说明校验内容、quarantine 行为、热加入/重连语义、`--allow-legacy-workers` 与 `--strict-manifest` 用途及滚动升级建议。
  - 运行并修复：`uv run ruff check locust/`、`uv run mypy locust/`、`uv run pytest locust/test/test_manifest.py locust/test/test_runners.py locust/test/test_web.py locust/test/test_parser.py`，以及前端 lint/test。
  - CHANGELOG.md 是否需要条目按仓库惯例判断（仅在有明确惯例时添加）。
- **Acceptance Criteria Addressed**: AC-9, AC-10
- **Test Requirements**:
  - `rule` TR-7.1: ruff 与 mypy 对新增/修改文件零错误。证据：命令输出。
  - `rule` TR-7.2: 上述后端测试与前端组件测试全部通过；既有 test_runners/test_web 无回归。证据：pytest/yarn 输出。
  - `rubric` TR-7.3: 文档准确性与完整性（行为、开关、升级注意事项与最终实现一致）；scale 1-5；anchors 1=无文档；3=有说明但与实现有出入；5=准确覆盖行为/开关/运维含义；threshold >= 4；证据：docs diff 审查。
