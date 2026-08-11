# Vane 对 lance-duckdb 的上游差异说明

本文说明 Vane 集成 Lance 时，相对原始 `lance-duckdb` 做了哪些修改，并区分以下三类内容：

1. 直接修改的 `lance-duckdb` 源码；
2. 为承载这些修改而增加的 Vane DuckDB、Python 和 Ray 适配；
3. 继续沿用上游、没有被 Vane 重写的 Lance 能力。

完整的集成架构、并发模型和验证范围见
[`LANCE_INTEGRATION.md`](LANCE_INTEGRATION.md)。

## 1. 比较基线与结论

比较基线是 `lance-format/lance-duckdb` 的固定提交
[`63c2446f7d9c8a59fd73a49fededb0c3725cc192`](https://github.com/lance-format/lance-duckdb/commit/63c2446f7d9c8a59fd73a49fededb0c3725cc192)。
导入来源、归档 SHA-256 和本地修改摘要记录在
[`external/lance-duckdb/VENDORING.md`](external/lance-duckdb/VENDORING.md)。

以该提交的干净 checkout 与当前 `external/lance-duckdb` 逐文件比较，排除格式差异、
Rust `target/`、SQLLogicTest 临时数据和其他构建产物后：

- 15 个上游文件发生了修改；
- 新增 `VENDORING.md`；
- 新增 3 个 Rust 依赖的安全回补源码目录；
- 导入时删除 `.gitmodules`、`duckdb` 和 `extension-ci-tools` gitlink；
- 没有导入或编译第二套 DuckDB。

这些修改没有创建新的 Lance 文件格式或事务实现。它们主要把原有
`lance-duckdb` 适配成可以被 Vane 静态链接、序列化并交给 Ray worker 执行的扩展。

## 2. 修改总览

| 范围 | 原始 lance-duckdb | Vane 中的修改 | 主要目的 |
| --- | --- | --- | --- |
| 源码组织 | DuckDB 和 extension 工具为 gitlink | 不导入两个 gitlink，直接使用 Vane DuckDB fork | 避免两套 DuckDB 和 ABI 冲突 |
| 扩展构建 | 同时构建静态和 loadable extension | 只构建 PIC 静态扩展并链接进 `vane._native` | 不允许运行时加载不匹配的扩展 |
| Rust feature | AWS、Azure、GCP、OSS、Hugging Face 等 | 第一版只保留本地、AWS/S3 和 REST namespace | 缩小依赖面和发布产物 |
| Tokio runtime | 按宿主机可用 CPU 创建 | 按 Vane/Ray worker CPU 配额创建 | 防止每个 worker 占满整机 CPU |
| Dataset handle | 进程内 handle，不提供固定版本重开接口 | 增加 version 查询和 checkout-version FFI | worker 重开同一 snapshot |
| 普通 scan | 单个 DuckDB 进程内部扫描 fragments | 输出 opaque fragment split，由 Vane 分组到 Ray tasks | 支持跨进程、跨节点扫描 |
| Search | 本地 table function | 序列化为一个全局 search split | 保持全局 top-k 和评分语义 |
| COPY 写入 | 可选择 parallel copy | 强制 `SINGLE_COMMIT_WRITER` | 保证只有一个事务 owner |
| Dataset cache | 可持续复用 latest handle | 新 bind 重新解析 latest，固定版本单独缓存 | 新查询看见新版本，旧查询保持旧版本 |
| Storage secret | 主要依赖当前 connection 状态 | worker connection 重建临时 `TYPE LANCE` secret | 物理计划不携带明文凭证 |

## 3. Vendoring 和构建修改

### 3.1 删除嵌套 gitlink

上游仓库的 `.gitmodules` 指向：

- 官方 `duckdb/duckdb`；
- DuckDB `extension-ci-tools`。

Vane 导入时排除了 `.gitmodules` 和两个 gitlink。`lance-duckdb` 并没有维护一份
Lance 特有的 DuckDB fork，因此 Vane 继续使用自身已经加入分布式执行能力的
[`external/duckdb`](external/duckdb)。

### 3.2 只构建静态扩展

[`external/lance-duckdb/CMakeLists.txt`](external/lance-duckdb/CMakeLists.txt)
删除了 `build_loadable_extension` 路径，只保留 `build_static_extension`，并为静态目标
启用 position-independent code。

最终构建关系是：

```text
external/lance-duckdb C++
        +
lance_duckdb_ffi Rust staticlib
        +
external/duckdb
        |
        v
vane._native
```

因此 Vane 不需要执行 `INSTALL lance` 或 `LOAD lance`，也不会生成可被另一套 DuckDB
动态加载的 Lance 扩展。

Vane 根构建通过
[`cmake/lance_extension_config.cmake`](cmake/lance_extension_config.cmake)
注册该静态扩展。SQLLogicTest 使用构建目录中的测试副本，测试过程中产生的数据不会
修改导入的上游 fixture，测试文件也不会进入 wheel。

### 3.3 可重复的 Cargo 构建

构建命令增加：

```text
cargo build --locked --no-default-features
```

同时：

- `.gitignore` 不再忽略根 `Cargo.lock`；
- `Cargo.lock` 被固定并随 feature 变化重新生成；
- CMake 将安全补丁目录加入构建依赖；
- CMake 会查找仓库管理的 `protoc`，找不到时明确失败。

## 4. Rust feature 与安全补丁

上游 [`Cargo.toml`](external/lance-duckdb/Cargo.toml) 为 `lance` 启用了 AWS、Azure、
GCP、OSS 和 Hugging Face 等 feature。Vane 第一版改为：

```toml
lance = { version = "9.0.0", default-features = false, features = ["aws"] }
lance-namespace-impls = { version = "9.0.0", default-features = false, features = ["rest"] }
```

其他 Lance crate 也显式设置 `default-features = false`。当前支持面是：

- 本地文件系统；
- AWS S3；
- S3-compatible 服务，例如 MinIO；
- REST namespace。

Vane 还为以下 crates.io 发布版本增加了本地 path patch：

- `object_store 0.13.2`；
- `opendal-core 0.57.0`；
- `opendal-service-s3 0.57.0`。

这些目录保留原始源码和许可证，只把其 `quick-xml` 依赖约束提升到安全修复后的
`0.41` 系列。完整 hash 和改动范围见
[`vendor-patches/README.md`](external/lance-duckdb/vendor-patches/README.md)。

## 5. 有界 Tokio runtime

上游 [`rust/runtime.rs`](external/lance-duckdb/rust/runtime.rs) 使用
`Runtime::new()` 创建进程全局 runtime。Vane 保留“一进程一个共享 runtime”的模式，
但改为显式设置 worker thread 数：

1. 优先读取 `VANE_LANCE_WORKER_CPUS`；
2. 其次读取 `OMP_NUM_THREADS`；
3. 否则使用当前进程可用并行度；
4. 所有情况至少为 1。

Ray worker 在第一次 Lance 调用前设置 `VANE_LANCE_WORKER_CPUS`。这避免一台机器上的
多个 Ray worker 各自按照整机核心数创建 Tokio 线程。

runtime 仍由 `OnceLock` 初始化，首次创建后不会动态 resize。部署时必须在该进程首次
使用 Lance 之前完成 CPU 环境配置。

## 6. 固定 snapshot 和 Dataset cache

### 6.1 新增 version FFI

[`rust/ffi/dataset.rs`](external/lance-duckdb/rust/ffi/dataset.rs) 和
[`src/include/lance_ffi.hpp`](external/lance-duckdb/src/include/lance_ffi.hpp)
新增：

```text
lance_dataset_version(dataset)
lance_dataset_checkout_version(dataset, version)
```

URI 和目录 namespace 扫描在 bind 时读取具体 version。物理计划传给另一个 worker
后，不传递原进程的 Rust/C++ handle，而是按照 URI 和 version checkout 同一个
snapshot。

### 6.2 cache 区分 latest 与固定版本

[`src/lance_dataset_cache.cpp`](external/lance-duckdb/src/lance_dataset_cache.cpp)
增加两类行为：

- latest 查询在每次 bind 时重新打开数据集并比较 version；
- 固定版本使用包含 version 的 cache key，可安全复用不可变 handle。

效果是：

- 其他 connection、进程或外部 writer 提交后，新 bind 可以看见更新后的版本；
- 已经开始的查询继续使用原来的固定版本；
- 一个查询的不同 Ray tasks 不会分别看到新旧版本。

REST `query_table` 和远程 namespace search 不在 worker 端 checkout 本地数字版本，其
snapshot 语义仍取决于远端 namespace 服务。这一点没有被描述成与本地 URI 完全等价。

## 7. 普通 scan 的分布式适配

### 7.1 Opaque fragment split

[`src/include/lance_scan_bind_data.hpp`](external/lance-duckdb/src/include/lance_scan_bind_data.hpp)
让 `LanceScanBindData` 实现 Vane 新增的 `ExtensionScanSplitProvider`。

普通扫描按 Lance fragment 产生：

```text
lance-fragment-v1:<fragment_id>
```

每个 split 同时提供估算行数和磁盘字节数。Vane 按 worker slot、目标 task 数和 split
大小进行分组，而不是要求一个 fragment 固定对应一个 Ray task。

以下操作不会按 fragment 拆分，而是产生一个 `lance-global-v1` split：

- REST namespace `query_table`；
- pushed-down sampling；
- row-id take；
- 需要全局语义的 limit/offset；
- 显式标记为 global 的执行计划。

### 7.2 只序列化纯数据

[`src/lance_scan.cpp`](external/lance-duckdb/src/lance_scan.cpp) 为普通 scan 和 ExecIR
增加了 serialize/deserialize，包括：

- URI；
- 固定 dataset version；
- schema、列名和类型；
- projection 和 filter IR；
- sampling、take、limit/offset 参数；
- fragment ID；
- 不含凭证的 namespace 身份。

以下对象不会进入计划：

- Rust `Dataset` handle；
- C++ cache entry 或裸指针；
- Arrow stream handle；
- connection secret。

worker 反序列化后，根据自己的 connection 和 secret 重开固定 snapshot，再应用被分配
的 fragment IDs。

### 7.3 分片情况下的执行修正

为避免拆分后改变结果，scan 还做了以下修正：

- `count(*)` 只统计当前 task 拥有的 fragments；
- fragment 行数未知时退化为扫描一个轻量物理列；
- 已应用 fragment split 时，不再选择会重新覆盖 fragment 范围的全局 scanner；
- DuckDB 内部 scan 线程数限制为当前 task 所选 fragment 数和 `threads` 的最小值。

## 8. Vector、FTS 与 Hybrid search

[`src/lance_search.cpp`](external/lance-duckdb/src/lance_search.cpp) 为 vector、FTS 和
hybrid bind data 增加：

- `Copy()`；
- serialize/deserialize；
- 固定 dataset version；
- task CPU slot 信息；
- `ExtensionScanSplitProvider`。

每次搜索只产生一个：

```text
lance-search-global-v1
```

这是有意设计。Vane 不会让多个 Ray tasks 分别对 fragments 做局部 top-k，再自行拼接
结果，因为那会改变：

- vector top-k；
- 全局 FTS `_score`；
- hybrid `_hybrid_score`；
- filter 和 index 的执行语义。

搜索计算仍调用上游 Lance 实现，并在一个 Ray worker 内使用 Lance/Tokio 并行能力。
Vane 的修改只负责 snapshot、计划传输、资源声明和全局 source 边界。

## 9. COPY 写入和单事务 owner

上游 [`src/lance_write.cpp`](external/lance-duckdb/src/lance_write.cpp) 在不需要保持输入
顺序时可以选择 `PARALLEL_COPY_TO_FILE`。Vane 将 Lance COPY 固定为：

```text
SINGLE_COMMIT_WRITER
```

同时为 COPY bind data 增加序列化，包含：

- write mode；
- data storage version；
- 行组和文件大小限制；
- 输出列名与类型。

这里的“single writer”表示只有一个 Lance 事务 owner 和 commit 权限，不表示整个查询
退化为单线程：

- writer 上游的 relation 计算仍可在多个 Ray nodes 上执行；
- 所有分区最终通过 gather 交给一个 writer task；
- Lance 的 Arrow 编码和 S3 异步 I/O 仍可使用 writer 进程内并发。

真正的 gather、task retry 边界和 `writer_started` 持久屏障位于 Vane DuckDB fork，而
不是 `lance-duckdb` 上游本体。

## 10. Storage secret 修改

[`src/lance_common.cpp`](external/lance-duckdb/src/lance_common.cpp) 修改了 directory
namespace 的 storage option 解析：

- 如果 namespace 已显式提供 storage options，则使用显式值；
- 否则从当前 worker connection 的临时 `TYPE LANCE` secret 解析；
- 使用本地持有的 key/value vectors 保证传给 FFI 的指针在调用期间有效。

scan/search 序列化只保留 URI、endpoint、table ID、delimiter 等身份信息，不序列化：

- access key；
- secret key；
- session token；
- bearer token；
- API key；
- 任意认证 header。

Ray 仍然需要通过受保护的 session 控制面把凭证交给 worker。这里保证的是凭证不出现
在物理计划、opaque split、actor name 和普通计划日志中，不代表凭证完全不跨进程。

## 11. Vane DuckDB fork 的配套修改

以下修改不位于 `external/lance-duckdb`，但它们是上述接口真正生效的承载层。

### 11.1 通用 extension scan split

[`extension_scan_split_provider.hpp`](external/duckdb/src/include/duckdb/function/extension_scan_split_provider.hpp)
新增通用接口：

- `GetScanSplits()`；
- `SetScanSplits()`；
- `GetTaskCpuSlots()`。

[`translator_scan.cpp`](external/duckdb/src/execution/distributed/pipeline_node/translator_scan.cpp)
识别该接口，按估算字节对 opaque splits 分组，并生成 Vane scan tasks。

这个接口不是 Lance 专用协议；其他扩展以后也可以使用同一种机制提供不可解释但可序列化
的 scan split。

### 11.2 单提交 COPY

Vane DuckDB 新增 `CopyFunctionExecutionMode::SINGLE_COMMIT_WRITER`：

- translator 为上游结果增加 gather；
- 只生成一个事务 owner task；
- writer task 被提交前写入 `writer_started` 屏障；
- 屏障前失败仍可按普通 task 策略重试；
- 屏障后失败不再提交第二次写入，而是返回 commit outcome unknown。

主要实现位于：

- [`copy_function.hpp`](external/duckdb/src/include/duckdb/function/copy_function.hpp)；
- [`translator_copy.cpp`](external/duckdb/src/execution/distributed/pipeline_node/translator_copy.cpp)；
- [`runner.hpp`](external/duckdb/src/include/duckdb/execution/distributed/plan/runner.hpp)。

## 12. Vane Python 与 Ray 配套修改

这些也不是上游 `lance-duckdb` 的修改：

- [`vane/lance`](vane/lance) 新增 `LanceDataset`、`LanceNamespace`、`LanceTable` 和
  `LanceMergeBuilder`；
- `vane.read_lance()` 和 `connection.read_lance()` 在 bind 前获取 snapshot lease；
- `relation.write_lance()` 在写入期间获取 mutation lease；
- [`vane/lance/_coordinator.py`](vane/lance/_coordinator.py) 在本地使用进程内
  coordinator，在 Ray 模式使用 detached coordinator actor；
- 同一 dataset 的 mutation 按 FIFO 串行；
- snapshot 可并发，也可与 mutation 重叠；
- VACUUM 等待 mutation 和全部 snapshot leases；
- [`vane/runners/ray/worker.py`](vane/runners/ray/worker.py) 设置 Lance CPU 环境并在每个
  worker connection 创建临时 secret。

这些并发保证只覆盖 `vane.lance`、`read_lance()` 和 `write_lance()` 等公共入口。直接
执行原始 Lance SQL 可以绕过 Python coordinator，底层仍只剩 Lance optimistic
transaction conflict detection 兜底。

## 13. 明确没有修改的 Lance 核心能力

以下实现继续使用上游版本，没有被 Vane 重写：

- Lance 文件、manifest 和 transaction 格式；
- optimistic transaction conflict detection；
- Arrow batch 到 Lance 文件的编码逻辑；
- INSERT、UPDATE、DELETE、TRUNCATE 和 MERGE 的 Lance 执行算法；
- vector、FTS 和 hybrid 的搜索、评分与 rerank 算法；
- index 创建、删除和查询算法；
- optimize 和 vacuum 的底层实现；
- S3 object-store I/O 语义。

对应的 `lance_insert.cpp`、`lance_update.cpp`、`lance_delete.cpp`、
`lance_merge.cpp`、`lance_index.cpp`、`lance_maintenance.cpp` 和 Rust writer 实现均未
出现在上游逐文件差异中。

因此更准确的描述是：

> Vane 在保留上游 Lance 数据与事务语义的前提下，增加了分布式计划协议、固定
> snapshot、资源控制、单 commit owner 和集群内协调层。

## 14. 直接修改的上游文件清单

构建和依赖：

```text
external/lance-duckdb/.gitignore
external/lance-duckdb/CMakeLists.txt
external/lance-duckdb/Cargo.toml
external/lance-duckdb/Cargo.lock
```

Rust：

```text
external/lance-duckdb/rust/runtime.rs
external/lance-duckdb/rust/ffi/dataset.rs
```

C++ headers：

```text
external/lance-duckdb/src/include/lance_common.hpp
external/lance-duckdb/src/include/lance_dataset_cache.hpp
external/lance-duckdb/src/include/lance_ffi.hpp
external/lance-duckdb/src/include/lance_scan_bind_data.hpp
```

C++ implementation：

```text
external/lance-duckdb/src/lance_common.cpp
external/lance-duckdb/src/lance_dataset_cache.cpp
external/lance-duckdb/src/lance_scan.cpp
external/lance-duckdb/src/lance_search.cpp
external/lance-duckdb/src/lance_write.cpp
```

新增的本地记录和补丁：

```text
external/lance-duckdb/VENDORING.md
external/lance-duckdb/vendor-patches/object_store-0.13.2/
external/lance-duckdb/vendor-patches/opendal-core-0.57.0/
external/lance-duckdb/vendor-patches/opendal-service-s3-0.57.0/
```

## 15. 后续升级上游时的审查重点

升级 `lance-duckdb` 或 Lance crate 时，不能简单覆盖当前目录。至少需要重新确认：

1. 上游 DuckDB 基线和 Vane DuckDB ABI 是否仍兼容；
2. scan/search/write bind data 的字段是否发生变化；
3. serialize/deserialize 是否覆盖所有影响执行结果的字段；
4. fragment split 是否仍对应一个固定 dataset version；
5. search 是否仍必须保持单个全局 source；
6. writer 是否仍只有一个 transaction owner；
7. `writer_started` 后是否仍禁止任务级重试；
8. latest/version-qualified dataset cache 是否仍保持正确可见性；
9. Rust runtime 是否仍受 worker CPU 配额限制；
10. secret、token 和 header 是否仍不会进入物理计划或日志；
11. 本地 Rust 安全补丁是否已经被新版本上游吸收；
12. Cargo license、security audit、sdist 和 wheel 检查是否通过。
