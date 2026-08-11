# Vane 集成 Lance：实现、执行模型与并发语义

本文描述 Vane 当前工作区中的 Lance 集成，包括源码引入方式、公开接口、
DuckDB/FTE/Ray 适配、快照与 mutation 协调、线程和资源模型，以及已经完成和
尚未完成的验证。

这里严格区分三类结论：

- **已实现**：能从当前源码直接确认的行为；
- **已验证**：当前工作区已经运行测试覆盖的行为；
- **边界或限制**：尚未验证，或者当前实现没有提供的保证。

本文描述的是 2026-08-11 的工作区状态。后续修改并发协议、Lance 版本或 Ray
调度方式时，应同步更新本文。

## 1. 集成目标与范围

第一版支持 Linux x86-64、本地文件系统、S3 和 S3-compatible 存储，以及目录
namespace 和 REST namespace。集成提供以下能力：

- 普通数据集读取、投影、过滤、聚合和部分算子下推；
- `create`、`append`、`overwrite` 写入；
- namespace 的建表、删表、`INSERT`、`UPDATE`、`DELETE`、`TRUNCATE`、
  `MERGE` 和列变更；
- vector search、FTS 和 hybrid search；
- scalar/vector/FTS 索引的创建、查看、使用和删除；
- `OPTIMIZE` 和 `VACUUM LANCE`；
- 本地 FTE 和 Ray FTE 执行，其中普通 fragment scan 可以按 fragment 分布到
  多个 Ray 节点。

当前集成没有同时构建两套 DuckDB，也不依赖运行时 `INSTALL` 或加载外部 Lance
extension。

## 2. 源码、构建与供应链改动

### 2.1 直接 vendoring

[`external/lance-duckdb`](external/lance-duckdb) 直接导入自
`lance-format/lance-duckdb` 的 commit
`63c2446f7d9c8a59fd73a49fededb0c3725cc192`。导入时间、归档哈希和本地差异记录在
[`external/lance-duckdb/VENDORING.md`](external/lance-duckdb/VENDORING.md)，总的
来源记录在 [`SOURCE_PROVENANCE.md`](SOURCE_PROVENANCE.md)。

导入时保留了 Lance 自有的 C++、Rust、CMake、`Cargo.lock`、测试、文档、bench、
许可证和辅助脚本，排除了：

- `.gitmodules`；
- 指向官方 `duckdb/duckdb` 的 `duckdb` gitlink；
- `extension-ci-tools` gitlink。

因此 Vane 仍然只有 [`external/duckdb`](external/duckdb) 这一份经过分布式改造的
DuckDB 1.5 fork。Lance extension 直接使用它的头文件和 ABI。

### 2.2 静态链接

[`cmake/lance_extension_config.cmake`](cmake/lance_extension_config.cmake) 将 Lance
注册为 in-tree static extension；构建只生成静态 `lance_extension` 并链接进
`vane._native`。wheel 运行时不会：

- 构建或下载另一份 DuckDB；
- 构建 loadable Lance extension；
- 执行 `INSTALL lance`；
- 从用户目录动态加载 Lance 二进制。

开启 `BUILD_UNITTESTS` 时，vendored SQLLogicTest 会先复制到构建目录，避免
mutation 测试修改 Git 跟踪的 fixture。

### 2.3 Rust feature、锁文件和安全检查

[`external/lance-duckdb/Cargo.toml`](external/lance-duckdb/Cargo.toml) 关闭 Lance
默认 feature，只保留本地数据路径、AWS/S3 和 REST namespace 所需依赖；Cargo
始终使用已提交的 `Cargo.lock`。为修复传递依赖中的 `quick-xml` 安全问题，仓库
保留了窄范围的 `object_store`/OpenDAL source patch，并记录原始归档哈希和 delta。

同时增加了：

- [`LICENSES/lance-rust-dependencies.txt`](LICENSES/lance-rust-dependencies.txt)；
- [`scripts/sync_lance_cargo_licenses.py`](scripts/sync_lance_cargo_licenses.py)；
- Security workflow 中的 `cargo audit --file external/lance-duckdb/Cargo.lock`；
- Cargo Dependabot 配置；
- sdist、wheel 和许可证清单检查。

## 3. 对外接口

新增 [`vane.lance`](vane/lance/__init__.py)，主要对象和方法如下。

| 接口 | 作用 |
| --- | --- |
| `vane.read_lance(uri)` | 读取一个 bind 时固定版本的 Lance relation |
| `connection.read_lance(uri)` | connection 级读取接口，并附加 snapshot lease |
| `relation.write_lance(uri, ...)` / `to_lance` | 将 relation 写入 Lance，支持 `create`、`append`、`overwrite` |
| `LanceDataset` | scan、三类 search、写入、索引、optimize、vacuum |
| `LanceNamespace` | attach/detach、获取 table、建表和删表 |
| `LanceTable` | scan、完整 DML、列 DDL、索引和 maintenance |
| `LanceMergeBuilder` | 构造 matched update/delete 和 not-matched insert |

所有由 Python builder 生成的对象名和 option 名都经过引用或白名单校验，索引名等
裸标识符不允许注入任意 SQL。

一个典型 Ray 用法是：

```python
import vane
from vane.lance import LanceDataset

vane.set_runner_ray(address="auto")
conn = vane.connect()
dataset = LanceDataset("s3://warehouse/events.lance", conn)

# 普通 scan 可以按 Lance fragment 分布到多个 Ray 节点。
rows = dataset.scan().filter("event_date >= DATE '2026-08-01'")

# 上游 relation 分布式计算，最后由一个事务 owner 提交。
rows.write_lance("s3://warehouse/events_copy.lance", mode="overwrite")

# Search 是一个全局 Lance source task，不会按 fragment 做局部 top-k。
nearest = dataset.vector_search("embedding", [0.1, 0.2, 0.3], k=10)
```

## 4. 分布式执行适配

### 4.1 Opaque scan split

Vane 的 DuckDB fork 新增了
[`ExtensionScanSplitProvider`](external/duckdb/src/include/duckdb/function/extension_scan_split_provider.hpp)。
extension 可以向分布式 planner 返回不透明、可序列化的工作单元，而不需要把它们
伪装成文件路径。

Lance 普通 scan 的 split payload 只包含 fragment ID；URI、固定版本、投影、过滤
和其他 bind 状态在物理计划中单独序列化。worker 收到任务后重新打开指定版本，
不会传输以下进程内对象：

- Rust `Dataset`/scanner/writer handle；
- C++ cache entry 或裸指针；
- Arrow stream handle；
- connection 内的 secret 对象。

这使同一计划可以安全地在不同 Ray worker 进程、不同 Ray 节点上反序列化。

### 4.2 普通 fragment scan

bind 时先读取 dataset version，并列出该版本的 fragment 及其估算字节数。分布式
planner 按估算字节尽量均衡地分组。目标任务数大致为：

```text
target = distributed_worker_slots
         或 distributed_node_count（没有 slot 信息时）
target = max(target, scan_task_min_partition_num)
task_count = min(fragment_count, target)
```

没有以上配置时，最多生成每 fragment 一个任务。提交 backlog 和 worker admission
会限制同时处于运行状态的任务数，所以“计划中有多少 task”不等于“同时运行多少
task”。

每个 worker 使用 URI 和 bind-time version 重新打开 dataset，再只扫描分配给它的
fragment。新提交的 append/index/optimize 不会改变已经 bind 的 relation。

以下路径会退化为一个 global split，而不是 fragment 分布式 scan：

- REST namespace 的 `query_table`；
- sampling；
- row-id point lookup；
- 已下推的全局 limit/offset；
- 要求全局 dataset scanner 的路径；
- optimizer 生成的 Lance ExecIR 全局下推。

### 4.3 Search

vector、FTS 和 hybrid 都只生成一个 `lance-search-global-v1` split。这样可以保证
top-k、`_score` 和 `_hybrid_score` 在完整数据集范围计算，避免各 fragment 先算
top-k 再由 Vane 错误拼接。

这意味着：

- 一次 search 的 Lance source 阶段只运行在一个 Ray worker 节点；
- Lance/DataFusion 可以在该进程内部使用线程和异步 I/O；
- search 输出后的 join、projection、shuffle 等普通算子仍可进入 Vane 分布式
  pipeline；
- 多个独立 search 查询可以在不同 worker 上并发，但单次 search 不会横跨多个
  Ray 节点。

hybrid search 当前不是两个 Ray 查询。它在同一个全局 task、同一个 dataset handle
上先执行 vector candidate scan，再执行 FTS candidate scan，然后在 Rust 中归一化、
合并 row id、计算 `_hybrid_score` 并取最终 top-k。两个底层 scan 当前是顺序执行，
不是两个并行 Ray 分支。

对于目录/local/S3 数据集，search bind data 包含固定 dataset version，worker 会按
该版本重开。对于 REST namespace，search/query_table 由远端 namespace 服务执行，
当前序列化状态没有本地 Lance version；其跨请求快照保证取决于远端服务协议，Vane
不能声明和目录数据集完全相同的 numeric-version 语义。

### 4.4 单事务 writer

Lance COPY function 返回 `SINGLE_COMMIT_WRITER` 执行模式。分布式写入分成两段：

1. 上游 scan、join、projection、UDF 和 repartition 仍可在多个进程和节点并行；
2. gather 收集所有上游 exchange handle，合并成一个 writer task，由一个 Lance
   transaction owner 消费所有输入并提交。

“单 writer”指只有一个事务和 commit owner，并不表示整个写入只有一个 OS 线程：

- DuckDB COPY sink 本身不是 parallel sink，按 batch 调用同一个 writer；
- Rust writer 使用一个专用 background thread；
- C++ 到 Rust 之间使用容量为 2 的同步 channel，形成 batch 级背压；
- Lance 编码和 object-store I/O 可以使用该进程共享的 Tokio runtime。

因此不会出现多个 Ray task 各自提交一次 append/overwrite，但编码和 S3 I/O 仍有
受限的进程内并发。

### 4.5 `writer_started` 失败边界

在 writer task 对目标数据集产生影响之前，driver 会在 `VANE_SESSION_DIR` 下原子
持久化 `writer_started` marker。marker 写入发生在所有上游输入就绪之后、唯一 writer
task 暴露给调度器之前。

- marker 之前失败：writer 尚未启动，FTE 可以按普通失败策略重试；
- marker 之后失败：worker task 的 `max_attempts` 被收紧为 1，失败被标成不可重试；
- commit 成功但 ACK 丢失：driver 返回 `LanceCommitOutcomeUnknownError`，不会自动再
  提交一次；
- marker 之后但实际未 commit 也可能返回 outcome unknown，这是 fail-closed 的代价。

调用方收到 outcome unknown 后必须根据 operation id 和数据集当前 version/transaction
记录做人工或业务级判定，不能直接重复 append。

## 5. Snapshot 与 mutation coordinator

[`vane/lance/_coordinator.py`](vane/lance/_coordinator.py) 按规范化 dataset URI 管理
三类 lease：snapshot、mutation 和 vacuum。

URI 规范化会移除 userinfo、query 和 fragment，并对 scheme/hostname 做规范化，避免
凭证进入 actor name 或日志。Ray 模式使用名为
`vane-lance-<sha256(uri)>` 的 detached actor；同一 Ray cluster 内、相同 URI 的多个
driver 会找到同一个 actor。非 Ray 模式使用当前 Python 进程内的
`threading.Condition` coordinator。

当前状态机提供以下规则：

- 多个 snapshot 可以同时持有；
- snapshot 与 mutation 可以重叠；
- 同一 dataset 的 mutation 按进入队列的顺序 FIFO，并且一次只有一个 owner；
- 不同 dataset 使用不同 coordinator，不互相阻塞；
- vacuum 等待 active mutation 和全部已登记 snapshot 结束；
- 有 vacuum waiter 时拒绝新 snapshot，避免持续新读饿死 vacuum；
- active vacuum 期间 snapshot 和 mutation 都不能进入。

等待中的 vacuum 当前没有和 mutation 共用一个统一 FIFO：新的 mutation 不会因为
`vacuum_waiters > 0` 自动停止。因此持续到来的 mutation 仍可能延迟 vacuum，当前
实现不承诺 mutation 与 vacuum 之间的严格公平性。

### 5.1 Lease 生命周期

`connection.read_lance()` 在 bind 之前获取 snapshot lease，并把 lease 作为 external
dependency 附着在 relation 上。派生 relation 会保留依赖；Ray runner 在结果 stream
关闭前保留源 relation。计划序列化只传一个不拥有 lease 的 placeholder，所以不会
发生某个 worker 反序列化后提前释放 driver lease 的问题。

这也意味着 relation 或结果 stream 持有得越久，`VACUUM` 等待得越久。长查询应及时
关闭结果 stream 并释放不再使用的 relation。

### 5.2 Coordinator 保证的作用域

FIFO 和 snapshot/vacuum 保护只覆盖通过以下公共 Python 路径发起的操作：

- `vane.read_lance` / `connection.read_lance`；
- `LanceDataset`、`LanceNamespace`、`LanceTable`；
- `relation.write_lance` / `relation.to_lance`。

用户直接执行原始 Lance SQL，例如直接 `COPY ... (FORMAT LANCE)`、`INSERT`、
`CREATE INDEX` 或直接扫描 attached table，会绕过 Python coordinator。目录数据集的
scan 仍会在 bind 时固定 version，但绕过 lease 的读不会阻止 Vane `VACUUM`；绕过
mutation lease 的写也不享受 Vane FIFO。此时只能依赖 Lance 自身的 transaction
冲突检测。

Coordinator 也不是跨 Ray cluster 的分布式锁。另一个 Ray cluster、普通 Python
进程或外部 Lance writer 不会加入这个队列；Vane 不做隐式冲突重试。

## 6. 多线程、多进程和多节点模型

### 6.1 多线程

每个进程只有一个由 `OnceLock` 创建的共享 Tokio multi-thread runtime。线程数按以下
顺序确定，并且至少为 1：

1. `VANE_LANCE_WORKER_CPUS`；
2. `OMP_NUM_THREADS`；
3. 当前进程可见的 host parallelism。

Ray worker 创建时把 `VANE_LANCE_WORKER_CPUS` 设为该节点分配给 Vane worker 的 CPU
数；local runner 设为 `host_cpu // local_worker_count`。runtime 一旦初始化不会在
环境变量变化后动态 resize。

Ray worker 内多个 task 共用一个 DuckDB `DatabaseInstance`、TaskScheduler 和 Tokio
runtime，每个 task 使用独立 cursor/ClientContext。FTE worker 按 `task_cpu_slots` 和
内存预算做 admission：

- 普通 fragment scan task 的 admission weight 为 1；
- search、Lance ExecIR 和 writer 请求 bind connection 的 DuckDB `threads` 数；
- worker 会把请求 clamp 到自身 CPU capacity；
- 请求全部 node CPU 的全局 task 会在该 worker 上排队阻止其他 CPU task 同时进入，
  但其他节点仍可运行查询。

CPU slot 是调度权重，不是 cgroup 或硬件级 CPU 限额。普通 scan task 如果分到多个
fragment，DuckDB 内部 `MaxThreads` 仍可能达到
`min(threads, task_fragment_count)`；共享 TaskScheduler 会限制进程总线程池，但 admission
账面上的 1 slot 可能低估该 task 的瞬时 CPU 使用。

另外，index、DML、DDL、optimize 和 vacuum 的 Python API 当前直接在调用方 connection
进程执行，不进入 Ray FTE task admission。它们会使用调用方进程的 Tokio runtime。
Ray driver 没有像 worker actor 一样自动设置 `VANE_LANCE_WORKER_CPUS`，所以若需要严格
限制 driver-side Lance 操作，部署方必须在该进程第一次使用 Lance 前设置
`VANE_LANCE_WORKER_CPUS` 或 `OMP_NUM_THREADS`。这是当前资源治理的一个限制。

### 6.2 多进程

local FTE 的 `num_workers > 1` 当前仍是 **单进程 in-process** worker，不是多个 Lance
进程。它们共享当前进程的 coordinator 和 Tokio runtime；每个执行线程按需创建自己的
Vane/DuckDB connection 和 cursor。

Ray 模式才是主要多进程路径：

- client/driver 进程负责 Python API、bind、计划和 snapshot/mutation lease；
- 每个有 CPU 的 Ray node 创建一个持久 `RayWorkerActor` 进程；
- 每个 worker 进程各有自己的 DuckDB instance、dataset cache、temporary secret 和
  Tokio runtime；
- dataset coordinator 是独立 Ray actor；
- worker 之间只传物理计划、opaque split、Arrow/exchange 数据和状态，不传 Lance
  handle。

同一个 Ray worker actor 可以接收多个异步 RPC，但实际 native fragment 并发由 FTE
CPU/memory admission 和共享 DuckDB scheduler 控制，而不是由 actor 的
`max_concurrency` 直接决定。

多个不连接 Ray 的 Python 进程各有独立 local coordinator，因此不会互相 FIFO。多个
连接同一 Ray cluster 的 driver 才能通过 named coordinator actor 共享队列。

### 6.3 多节点

普通 fragment scan 的 task 可以分布到所有可用 Ray worker 节点。每个节点必须能用
同一 URI 打开相同 dataset version：

- S3/MinIO 是推荐的共享存储路径；
- 本地路径只适合同机 Ray 节点，或者所有物理机器挂载了语义一致的共享文件系统；
- 不能假设不同机器上的同名本地目录包含相同 Lance transaction 文件。

单次 search、REST `query_table`、全局 ExecIR 和最终 writer 都只运行在一个节点。
writer 的上游可以横跨多节点，但 commit owner 只有一个。不同 search 查询、不同
dataset mutation 和普通 scan 可以被调度到不同节点并行。

Dataset coordinator 的一致性范围是一个 Ray control plane。跨集群或外部 writer 的
冲突由 Lance optimistic transaction 机制兜底，Vane 返回冲突，不自动重放 mutation。

## 7. 各功能的并发性

下表描述通过 Vane 公共 Python API 发起操作时的当前行为。

| 功能 | 单次操作内部并发 | 同一 dataset 上的多操作 | 可见性与一致性 |
| --- | --- | --- | --- |
| 普通 fragment scan | 多个 FTE task 可跨进程、跨节点；每个 task admission weight 为 1；同一 task 可包含多个 fragment | 多读并发；可与 mutation/index build 并发 | 目录/local/S3 在 bind 时固定 numeric version，整个查询不会混读新旧版本 |
| sampling、point lookup、limit/offset、REST `query_table`、全局 ExecIR | 一个 global source task；ExecIR 可在 Lance/DataFusion 内部并行 | 多查询受 worker admission 限制 | 目录数据仍固定 version；REST 快照语义依赖远端服务 |
| vector search | 一个全局 FTE task，在单个 worker 内由 Lance 执行 ANN/brute-force | 多查询可跨 worker 并发，也可与 mutation 并发 | 固定目录 dataset version；只看到该版本已提交的索引 |
| FTS | 一个全局 FTE task | 与 vector 相同 | `_score` 在完整 query result 范围计算，不做 fragment 局部合并 |
| hybrid search | 一个全局 task；内部顺序运行 vector 和 FTS candidate scan，再在 Rust 合并 | 多查询可跨 worker 并发 | `_distance`、`_score`、`_hybrid_score` 来自同一 dataset handle/version |
| `relation.write_lance` | 上游可多节点并行；一个 writer task、一个 transaction；后台线程和 Tokio 可并行编码/I/O | 公共 API 对同 dataset FIFO；不同 dataset 可并发 | commit 前不可见；barrier 后失败不重试，返回 outcome unknown |
| table DML、DDL、MERGE | 在调用方 connection 进程执行；Lance 内部可使用 Tokio/I/O 并发 | 公共 API 对同 dataset FIFO；实际同时执行不同 dataset 需要独立 connection/进程 | 每个 mutation commit 后产生新可见版本 |
| 创建、优化、删除索引 | 一个 mutation owner；当前不分成多个 Ray task | 与同 dataset 的其他公共 mutation 串行，读/search 不被 coordinator 阻塞 | 已开始的查询继续使用旧版本；新 bind 才看到新索引 |
| 使用索引 | 属于 read/search，不获取 mutation lease | 多查询可同时使用；可与索引创建重叠 | 不会看到未 commit 的半成品索引 |
| `VACUUM LANCE` | 在调用方进程执行 | 等待已登记 snapshot 和 active mutation；active vacuum 独占 | 不删除公共 API 中仍持有 snapshot lease 的旧版本文件 |

还需考虑 connection 自身的并发限制：不同 dataset 的 coordinator 虽然互不阻塞，但
不要从多个线程同时复用同一个 DuckDB connection。需要真正并行时应使用独立
connection，并让 Ray/FTE admission 统一控制 worker 资源。

## 8. 凭证与连接隔离

S3 配置来自 Vane session snapshot/AWS credential chain。driver planning connection 和
每个 worker session connection 都会创建 connection-local、temporary 的
`TYPE LANCE` secret，支持：

- access key 和 secret key；
- session token；
- region；
- HTTP/HTTPS endpoint；
- MinIO/S3-compatible 的 path-style request；
- credential-chain 刷新路径。

Lance bind data 和 opaque split 不包含明文凭证；worker 从会话控制面收到的配置重建
secret。计划文本和 actor name 会脱敏，secret/token/password key 会登记为 redacted。
需要注意：Vane 仍然必须通过受保护的 session 控制面把凭证交给 worker；“计划中不
带凭证”不代表凭证无需跨进程传输。

temporary secret 不会出现在另一个独立 connection 的 `duckdb_secrets()` 中。Ray 会在
每个 session/query connection 上重新应用配置，而不依赖全局持久 secret。

## 9. 已完成的验证

### 9.1 本地和单 Ray cluster

当前工作区已经运行以下定向覆盖：

- Lance/coordinator 非 external-service 集合：`17 passed, 3 deselected`；
- 两个 Ray Lance 定向测试通过：fragment scan、单 writer、bind-time snapshot、
  vacuum lease、vector/FTS/hybrid，以及 index commit 后的新查询使用索引；
- local in-process 写入、snapshot、多线程 read/append、namespace DML/DDL/MERGE、
  index、optimize、vacuum 和 secret connection isolation；
- local coordinator 和真实 Ray coordinator 的 mutation FIFO、不同 dataset 并发、
  read/write overlap、vacuum 等待和 lease plan-transport ownership。

仓库还保留并注册了 58 个 vendored SQLLogicTest 文件，覆盖 scan、pushdown、类型、
DML、MERGE、namespace、search、index、maintenance 和 S3。它们会在 native DuckDB
test build 中从构建目录副本运行；上面的 `17 passed` 仅指 Python 定向集合，不等同于
“本轮重新执行了全部 58 个 native SQL 文件”。

### 9.2 真实 MinIO 和同机双 Ray 节点

已经使用本机实际启动的 MinIO 服务完成两组 external-service 测试：

- 单 Ray cluster + MinIO：`1 passed`；
- 同一台物理机上启动两个 Ray node + MinIO：`1 passed`，约 17 秒。

双节点测试不是只检查 `ray.nodes()` 数量。它完成了以下端到端验证：

- 两个 node 具有不同 Ray `NodeID`，每个 node 4 CPU；
- 65,536 行输入写入 S3-compatible Lance dataset；
- scan 计划至少产生 8 个 fragment descriptor；
- 从 Ray GCS `TaskInfo` 读取实际 `fte_create_task -> NodeID` 映射，确认两个 node 都执行
  了 Lance fragment task；
- 多节点 aggregate 的 count/sum 正确；
- 运行中的旧 snapshot 与并发 append 隔离，新查询看到新 version；
- 单 writer 写入、目录 namespace discovery、vector/FTS index、hybrid search、
  optimize 和 vacuum；
- S3 凭证没有进入 physical-plan 文本。

当次 native 归属检查还确认测试使用的是已安装包中的
`vane/_native.cpython-312-x86_64-linux-gnu.so`；`PRAGMA version` 为
`v1.5.0-vane.b1c745e9c4-dirty`，SourceID 为 `ba5076f4c8`。这两个标识是当时 dirty
工作区的快照，提交或修改 `external/duckdb` 后会正常变化。

### 9.3 这些验证没有证明什么

同机双 Ray node 验证了 Ray 的多 node 调度、多 raylet/worker 进程和共享 MinIO 数据
路径，但它不等于两台物理机器。当前仍未完成：

- 两台或更多物理机器上的真实网络、独立磁盘和跨机故障验证；
- node kill、raylet kill、网络分区、S3 超时和长时间压力测试；
- 真实 Lance commit 成功但 ACK 丢失的端到端故障注入；当前只验证了 barrier/retry
  协议和错误结果契约；
- 使用真实外部 REST namespace endpoint 的测试；对应测试存在，但没有配置服务时会
  skip；
- Lance 操作进行中的真实临时凭证轮换；
- 多个独立 Ray cluster 或外部 writer 的并发冲突矩阵；
- 性能、吞吐量、扩展效率和公平性 benchmark。

因此目前可以确认“同机两个逻辑 Ray 节点确实共同执行了 Lance fragment scan”，不应
把它表述成“已经验证真实多机生产部署”。

## 10. 当前风险和后续工作

### 10.1 Lease 的进程崩溃恢复

snapshot/mutation token 当前没有 TTL 或 owner-liveness 检测。正常 close、异常栈展开
和 Python finalizer 会释放 lease；但持有 lease 的 driver 被 `kill -9`、机器断电或与
coordinator 永久失联时，detached coordinator actor 可能保留孤儿 token，导致后续
vacuum 或 mutation 等待。生产化前需要增加 owner heartbeat/epoch、lease TTL 或
driver-death recovery，并做故障注入。

### 10.2 公共 API 与原始 SQL 的一致性缺口

当前 coordinator 位于 Python API 层，不是 DuckDB catalog/transaction 层的强制锁。
若要让所有 SQL 都满足相同 FIFO/lease 语义，需要把 dataset identity 和 coordinator
acquire/release 下沉到 native bind/execute 生命周期，或者明确禁止绕过公共 API。

### 10.3 Driver-side 资源控制

search 的执行 task 和分布式 writer 有 CPU-slot admission，但 DML/DDL/index/maintenance
仍在 driver connection 直接执行。后续可以把它们变成显式单 mutation task，或为
driver 增加和 Ray worker 一致的 CPU permit/runtime 初始化策略。

### 10.4 REST snapshot 语义

REST `query_table` 是一个全局 source，但当前没有序列化 numeric dataset version。
应根据目标 namespace 服务能力增加 version/token contract，或在 API 文档中明确它只
提供该服务自身定义的一致性。

### 10.5 真正多机验收

下一阶段至少应在两个独立主机上使用共享 S3/MinIO，重复同机双节点场景，并增加：

- 查询期间 kill 一个 worker，验证只重试安全的 read task；
- writer barrier 前/后分别 kill worker；
- commit 成功后丢弃 ACK，验证没有第二次 append；
- snapshot 期间运行 vacuum，随后 kill driver，验证 lease recovery；
- 同 dataset mutation FIFO、不同 dataset 并发和跨 cluster conflict；
- REST namespace live endpoint 的 query/search/credential isolation。

## 11. 维护和回归检查

修改 Lance C++、Rust、DuckDB distributed adapter 或 Python API 后，应按
[`DEVELOPMENT.md`](DEVELOPMENT.md) 使用增量 native build，不能使用 editable
install：

```bash
export SKBUILD_BUILD_DIR="$PWD/build/python-release"
export SKBUILD_CMAKE_BUILD_TYPE=Release
uv pip install . --no-build-isolation
```

先运行定向测试，再运行 release/fast gate。外部服务和双节点测试需要显式选择
`external_service`；直接调用 `ray.init()` 的测试必须同时标记 `real_ray` 和
`ray_cluster_owner`。

```bash
scripts/run_installed_pytest.sh tests/fast/test_lance.py tests/fast/test_lance_coordinator.py
scripts/run_installed_pytest.sh -m external_service tests/fast/test_lance.py
scripts/run_release_tests.sh
scripts/run_fast_tests.sh
```

同时检查 Cargo 锁文件、RustSec、许可证 bundle、sdist/wheel 内容以及 native
`__file__`、`PRAGMA version` 和 SourceID，防止测试误用旧 wheel 或另一份 DuckDB。
