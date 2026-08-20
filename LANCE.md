# 在 Vane 中完整使用 Lance

本文给出一套可执行的 Vane + Lance 示例，覆盖当前固定版本
[`lance-duckdb`](https://github.com/hubgeter/lance-duckdb) 的全部公开能力，以及 Vane 在这些能力之上增加的分布式扫描、全局搜索和单事务写入。

本文对应的 `lance-duckdb` revision 是
`856203ca15bdf21e1f6c2962038ccbd4598573b0`。仓库中的示例不是伪代码：每个脚本都包含断言，本文末尾记录了实际执行过的命令和边界。以下内部函数不属于公开 API，因此不作为用户入口：
`__lance_scan`、`__lance_namespace_scan`、`__lance_exec`、`__lance_compact_files` 等所有
`__lance_*` 函数。

集成改动、端到端调用链以及多线程、多进程、多节点并发模型见
[`VANE_LANCE_ARCHITECTURE.md`](VANE_LANCE_ARCHITECTURE.md)。

## 1. 示例入口

完整代码在以下五个脚本中：

| 脚本 | 覆盖范围 | 真实验证环境 |
|---|---|---|
| [`examples/lance_complete.py`](examples/lance_complete.py) | 本地读写、快照、三种搜索、三种索引、维护、目录 namespace、DML/DDL | 临时本地文件系统，local runner |
| [`examples/lance_distributed.py`](examples/lance_distributed.py) | 分布式 fragment scan、全局搜索、create/append/overwrite、空数据集 | 单机 Ray 集群，4 个 CPU worker slot |
| [`examples/lance_s3.py`](examples/lance_s3.py) | S3 secret、分布式对象存储 I/O、S3 namespace、`s3/s3a/s3n` | 本地 MinIO，单机 Ray 集群 |
| [`examples/lance_secrets.py`](examples/lance_secrets.py) | 三种 secret provider、五类对象存储配置、透传与脱敏 | DuckDB Secret Manager；除 S3 外不发起云 I/O |
| [`examples/lance_rest_namespace.py`](examples/lance_rest_namespace.py) | REST catalog、`query_table` 下推、DML、搜索、索引、维护 | 本地 `lance-namespace` 9.0.1 REST adapter |

安装好当前分支后可以直接运行不依赖外部服务的三个例子：

```bash
VANE_PROGRESS=0 python examples/lance_complete.py
VANE_PROGRESS=0 python examples/lance_distributed.py
python examples/lance_secrets.py
```

`lance_s3.py` 和 `lance_rest_namespace.py` 分别需要一个可访问的 S3-compatible endpoint 和 Lance REST Namespace endpoint，配置方法见后文。

开发者不要做 editable install。修改过 C++ 后按仓库约定复用增量构建目录：

```bash
export SKBUILD_BUILD_DIR="$PWD/build/python-release"
export SKBUILD_CMAKE_BUILD_TYPE=Release
uv pip install . --no-build-isolation
```

## 2. 扩展加载和 runner

Vane 把 Lance 扩展静态链接进 native engine，不需要也不应该执行 `INSTALL lance` 或
`LOAD lance`。下面的检查包含在完整本地示例中：

```python
import vane

vane.set_runner_local()
con = vane.connect()
row = con.execute(
    """
    SELECT installed, loaded, extension_version
    FROM duckdb_extensions()
    WHERE extension_name = 'lance'
      AND install_mode = 'STATICALLY_LINKED'
    """
).fetchone()
assert row == (
    True,
    True,
    "856203ca15bdf21e1f6c2962038ccbd4598573b0",
)
```

可选设置 `lance_deferred_materialization` 默认是 `true`。它允许 scan 尽量推迟大列的物化；必要时可通过普通 DuckDB `SET` 修改：

```sql
SET lance_deferred_materialization = false;
SET lance_deferred_materialization = true;
```

选择执行模式时遵循以下规则：

- `vane.set_runner_local()`：一个进程中的完整 FTE local runner，适合调试、DDL、catalog 和小数据。
- `vane.set_runner_ray()`：Vane 的分布式执行路径，适合并行 scan 和分布式写入。
- namespace DDL、DML、索引和维护命令由协调端执行；普通 dataset scan 和 Relation 写入才会进入分布式数据面。

## 3. Dataset 写入、扫描和固定快照

### 3.1 SQL `COPY`

支持 `create`、`append` 和 `overwrite` 三种 mode：

```sql
COPY (
  SELECT 1::BIGINT AS id, 'duck'::VARCHAR AS label
) TO '/mnt/shared/documents.lance' (
  FORMAT LANCE,
  MODE 'create',
  DATA_STORAGE_VERSION '2.2',
  MAX_ROWS_PER_FILE 1048576,
  MAX_ROWS_PER_GROUP 1024,
  MAX_BYTES_PER_FILE 96636764160
);

COPY (SELECT 2::BIGINT AS id, 'horse'::VARCHAR AS label)
TO '/mnt/shared/documents.lance' (FORMAT LANCE, MODE 'append');

COPY (SELECT 3::BIGINT AS id, 'dragon'::VARCHAR AS label)
TO '/mnt/shared/documents.lance' (FORMAT LANCE, MODE 'overwrite');
```

`DATA_STORAGE_VERSION` 在 create/overwrite 时默认是 `2.2`，append 保留已有数据集的版本。当前 Vane surface 不暴露 upstream DuckDB COPY planner 的 `WRITE_EMPTY_FILE` 开关；本地 COPY 和分布式 `write_lance()` 都固定保证零行输入也提交一个保留输入 schema 的空数据集。

### 3.2 Python Relation API

```python
import vane
from vane.lance import LanceDataset

con = vane.connect()
source = con.sql(
    "SELECT * FROM (VALUES (1, 'one'), (2, 'two')) AS t(id, label)"
)
source.write_lance(
    "/mnt/shared/items.lance",
    mode="create",
    data_storage_version="2.2",
    max_rows_per_file=1_048_576,
    max_rows_per_group=1_024,
    max_bytes_per_file=96_636_764_160,
)

dataset = LanceDataset("/mnt/shared/items.lance", con)
dataset.write(
    con.sql("SELECT 3 AS id, 'three' AS label"),
    mode="append",
)
```

`relation.to_lance()` 是 `relation.write_lance()` 的兼容别名。推荐使用后者以便代码含义更清晰。

### 3.3 三种扫描入口

```python
# 模块级 API
relation_a = vane.read_lance("/mnt/shared/items.lance", connection=con)

# connection API
relation_b = con.read_lance("/mnt/shared/items.lance")

# 高层 Dataset API
relation_c = LanceDataset("/mnt/shared/items.lance", con).scan()
```

SQL 也支持 replacement scan：

```sql
SELECT id, label
FROM '/mnt/shared/items.lance'
WHERE id >= 2
LIMIT 10;
```

本地相对路径按创建 relation/dataset 时的进程工作目录解析，并固定成绝对路径；Lance 不使用 DuckDB 的
`file_search_path` 搜索数据集。不要在构造 `LanceDataset` 后切换工作目录再用另一个相对字符串访问同一
数据集。Ray 多节点部署应直接使用所有节点语义一致的绝对共享路径或对象存储 URI。

投影、可表达的过滤和 limit 会下推到 Lance。`LanceDataset.snapshot()` 在 relation 的整个生命周期持有一个固定快照租约，适合需要和同进程或同 Ray runner 中 mutation/vacuum 协调的查询：

```python
dataset = LanceDataset("/mnt/shared/items.lance", con)
with dataset.snapshot() as snapshot:
    rows = snapshot.filter("id BETWEEN 10 AND 20").fetchall()
```

## 4. Vector、FTS 和 hybrid search

三个公开 table function 都可以直接写 SQL，也可以通过 `LanceDataset`/`LanceTable` 调用。

### 4.1 Vector search

```python
nearest = dataset.vector_search(
    "vec",
    [0.1, 0.0, 0.0, 0.0],
    k=10,
    nprobes=4,
    refine_factor=2,
    prefilter=True,
    use_index=True,
)
rows = nearest.project("id, _distance").fetchall()
```

对应 SQL 参数名是历史拼写 `nprobs`，Python API 使用常见拼写 `nprobes`：

```sql
SELECT id, _distance
FROM lance_vector_search(
  '/mnt/shared/documents.lance',
  'vec',
  [0.1, 0.0, 0.0, 0.0]::FLOAT[4],
  k = 10,
  nprobs = 4,
  refine_factor = 2,
  prefilter = true,
  use_index = true,
  explain_verbose = true
);
```

结果的 `_distance` 越小越相似。`use_index=false` 强制 exact KNN。`filter` 参数只用于 attached namespace table 的 `query_table` 过滤；普通 dataset path 应使用 relation 的 `.filter(...)` 或 SQL `WHERE`。

### 4.2 Full-text search

```python
matches = dataset.fts(
    "text",
    "puppy",
    k=10,
    prefilter=True,
)
rows = matches.project("id, _score").fetchall()
```

```sql
SELECT id, _score
FROM lance_fts(
  '/mnt/shared/documents.lance',
  'text',
  'puppy',
  k = 10,
  prefilter = true
);
```

结果的 `_score` 越大越相关。namespace table 同样可以通过显式 `filter='category = ...'` 让 REST `query_table` 在 top-k 之前过滤。

### 4.3 Hybrid search

```python
hybrid = dataset.hybrid_search(
    "vec",
    [0.1, 0.0, 0.0, 0.0],
    "text",
    "puppy",
    k=10,
    nprobes=4,
    refine_factor=2,
    prefilter=False,
    use_index=True,
    alpha=0.6,
    oversample_factor=4,
)
rows = hybrid.project(
    "id, _hybrid_score, _distance, _score"
).fetchall()
```

`alpha` 越大越偏向 vector 分支；`oversample_factor` 控制候选扩展。hybrid 返回
`_hybrid_score`、`_distance` 和 `_score`。普通路径、目录 namespace 和 S3 路径均已验证；当前 revision 对 REST namespace table 明确返回 `NotImplementedException`，因为 REST Namespace API 还没有 hybrid query contract。

### 4.4 分布式排名语义

在 Ray runner 中，普通 scan 按不可变 Lance fragment 拆分；vector、FTS、hybrid search 必须保持一个全局 search task。不能先在每个 fragment 上分别做 top-k 再合并，否则结果不是全局 top-k。

## 5. 索引和维护

### 5.1 三类索引

```python
dataset.create_index(
    "documents_vec_idx",
    "vec",
    index_type="IVF_FLAT",
    num_partitions=1,
    metric_type="l2",
)
dataset.create_index(
    "documents_text_idx",
    "text",
    index_type="INVERTED",
)
dataset.create_index(
    "documents_category_idx",
    "category",
    index_type="BTREE",
)

print(dataset.show_indexes().fetchall())
dataset.drop_index("documents_category_idx")
```

SQL 形式如下：

```sql
CREATE INDEX documents_vec_idx
ON '/mnt/shared/documents.lance' (vec)
USING IVF_FLAT WITH (num_partitions = 1, metric_type = 'l2');

CREATE INDEX documents_text_idx
ON '/mnt/shared/documents.lance' (text)
USING INVERTED;

CREATE INDEX documents_category_idx
ON '/mnt/shared/documents.lance' (category)
USING BTREE;

SHOW INDEXES ON '/mnt/shared/documents.lance';
DROP INDEX documents_category_idx ON '/mnt/shared/documents.lance';
```

Vector index 要求固定长度的 `FLOAT[N]` 或 `DOUBLE[N]` 列。索引增量维护支持全部三种 mode：

```sql
ALTER INDEX documents_vec_idx ON '/mnt/shared/documents.lance'
OPTIMIZE WITH (mode = 'append');

ALTER INDEX documents_vec_idx ON '/mnt/shared/documents.lance'
OPTIMIZE WITH (mode = 'merge', num_indices_to_merge = 1);

ALTER INDEX documents_vec_idx ON '/mnt/shared/documents.lance'
OPTIMIZE WITH (mode = 'retrain');
```

### 5.2 Compaction、vacuum 和 auto cleanup

```python
dataset.optimize(
    target_rows_per_fragment=1_048_576,
    max_rows_per_group=1_024,
    max_bytes_per_file=0,
    materialize_deletions=True,
    materialize_deletions_threshold=0.1,
    num_threads=1,
    batch_size=2,
    defer_index_remap=False,
)

dataset.vacuum(
    older_than_seconds=1_209_600,
    delete_unverified=False,
    error_if_tagged_old_versions=True,
    retain_n_versions=3,
)
```

```sql
ALTER TABLE '/mnt/shared/documents.lance'
SET AUTO_CLEANUP WITH (
  interval = 1,
  older_than = '1h',
  retain_versions = 3
);

SHOW MAINTENANCE ON '/mnt/shared/documents.lance';
ALTER TABLE '/mnt/shared/documents.lance' UNSET AUTO_CLEANUP;
```

`OPTIMIZE`、`VACUUM LANCE` 和 index optimize 都返回 `Operation`、`Target`、`MetricsJSON`。
通过 `LanceDataset.vacuum()` 发起的 vacuum 会等待同一 Vane coordinator 中的固定快照和 mutation 结束，
并在排队后阻止新的 reader/writer 插队。直接执行裸 SQL `VACUUM LANCE ...` 不经过 Python coordinator；
跨进程、跨 Ray cluster 或混用其他 Lance client 时，调用方仍须在更高层协调旧快照生命周期。

## 6. Directory namespace、DML 和 DDL

目录可以作为一个 DuckDB catalog 挂载；目录下的 `items.lance` 映射成
`demo_lance.main.items`：

```python
from vane.lance import LanceNamespace

namespace = LanceNamespace(
    "/mnt/shared/lance_catalog",
    "demo_lance",
    connection=con,
)
table = namespace.create_table(
    "items",
    "SELECT 1::BIGINT AS id, 'one'::VARCHAR AS label",
)
print(table.scan().fetchall())
```

alias、schema 和 table name 会逐段引用，因此名称中可以包含点、连字符或 SQL 关键字；索引和维护命令也
保留同样的限定名语义。

也可以完全使用 SQL：

```sql
ATTACH '/mnt/shared/lance_catalog' AS demo_lance (TYPE LANCE);

CREATE TABLE demo_lance.main.schema_only (
  id BIGINT,
  label VARCHAR
) WITH (data_storage_version = '2.2');

CREATE TABLE demo_lance.main.items AS
SELECT 1::BIGINT AS id, 'one'::VARCHAR AS label, 10::INTEGER AS quantity;

SHOW TABLES FROM demo_lance.main;
```

### 6.1 DML

Python API 覆盖 INSERT、UPDATE、DELETE、TRUNCATE 和 MERGE builder：

```python
table.insert(
    "SELECT 2::BIGINT AS id, 'two'::VARCHAR AS label, "
    "20::INTEGER AS quantity"
)
table.update({"quantity": "quantity + 1"}, where="id = 2")
table.delete(where="id = 1")

table.merge(source_sql, "target.id = source.id").when_matched_update(
    {"label": "source.label", "quantity": "source.quantity"}
).when_not_matched_insert(
    {
        "id": "source.id",
        "label": "source.label",
        "quantity": "source.quantity",
    }
).execute()

table.truncate()
```

原生 SQL `MERGE INTO` 还支持 matched DELETE/DO NOTHING/ERROR、not matched by source，以及 `RETURNING merge_action`。完整本地例子真实执行了 update/insert 和 delete/returning 路径；REST 例子也执行了 Python builder 的 matched delete。

### 6.2 Schema evolution 和 metadata

```sql
ALTER TABLE demo_lance.main.items ADD COLUMN quantity_plus_one BIGINT;
UPDATE demo_lance.main.items
SET quantity_plus_one = quantity + 1;

ALTER TABLE demo_lance.main.items
ADD COLUMN constant_value INTEGER DEFAULT 42;

COMMENT ON TABLE demo_lance.main.items IS 'Lance example items';
COMMENT ON COLUMN demo_lance.main.items.quantity_plus_one
IS 'derived quantity';

ALTER TABLE demo_lance.main.items RENAME COLUMN label TO display_label;
ALTER TABLE demo_lance.main.items ALTER COLUMN quantity TYPE BIGINT;
ALTER TABLE demo_lance.main.items ALTER COLUMN quantity SET NOT NULL;
ALTER TABLE demo_lance.main.items ALTER COLUMN quantity DROP NOT NULL;
ALTER TABLE demo_lance.main.items DROP COLUMN quantity_plus_one;
ALTER TABLE demo_lance.main.items DROP COLUMN constant_value;
```

当前 revision 已实测 constant default。需要引用其他列的派生值时，使用“先 ADD nullable column，再 UPDATE”的两步方式；`ADD COLUMN ... DEFAULT (quantity + 1)` 在当前 DuckDB binder 中不可用。

最后可以执行：

```sql
TRUNCATE TABLE demo_lance.main.items;
DROP TABLE demo_lance.main.items;
DETACH demo_lance;
```

`LanceNamespace(..., read_only=True)` 会创建只读 attachment；DuckDB catalog 层拒绝所有经该 attachment
发起的 DML/DDL，Python table write API 还会做提前检查。直接按物理 URI 调用 `write_lance(uri)` 不属于
attachment，因此不会继承 attachment 的只读策略。

`LanceTable` 的 INSERT/CTAS/UPDATE/DELETE/MERGE/维护 helper 在调用方 connection 中执行，不进入
`relation.write_lance()` 的分布式 staging/driver-commit 数据面。Python mutation helper 要求 autocommit，
避免把已经外部提交的 Lance 变更伪装成可由 DuckDB rollback 撤销。

`namespace.detach()` 会先为当前 attachment 下每张表获取 vacuum lease，等待这些表的 scan relation
释放后才 DETACH；默认超时是 30 秒，超时不会移除 attachment。若当前线程还保留着 `table.scan()` 的
relation，应先消费并释放它，或明确传入适合调用方的 `timeout`。

## 7. Ray 分布式 scan 和单事务写入

运行完整分布式例子：

```bash
VANE_PROGRESS=0 python examples/lance_distributed.py
```

例子生成 8 个 Parquet 分片，然后真实执行以下行为：

- create 32 行，按 Lance fragments 分布式读取；
- vector/FTS/hybrid 都保持全局排名；
- append 16 行，数据集变成 48 行；
- overwrite 为 8 行；
- 零行 Parquet 输入仍创建带完整 schema 的 Lance dataset；
- 每次 write 只有一个已提交 transaction，结束后没有残留 `_vane_staging`。

分布式写入不是让每个 worker 直接 append 目标数据集。每个 worker 只生成 uncommitted staging transaction；driver 上的 coordinator 检查选中的全部 task attempt 后发布一个 Lance transaction。这个模型避免 worker 重试导致重复 append，也让 create/overwrite 对外只出现一个提交点。

注意以下部署约束：

- 普通本地路径在 bind 时转成绝对路径；多节点时每个 Ray 节点必须把这个路径解析到同一个共享文件系统。
- 多节点部署优先使用 S3-compatible object store。
- distributed append 要求输入与现有 Lance schema 精确一致，包括 field identity 和 metadata。
- 本文真实执行的是“一台机器上的多 worker Ray 集群”，验证了 Vane 的分布式计划和 transaction 协议；它不是多台物理机器的网络/共享存储压测。

## 8. Object store secret 和真实 S3/MinIO I/O

### 8.1 Secret provider

`TYPE LANCE` 支持三种 provider：

- `config`：在 SQL 中显式配置；
- `credential_chain`：使用 AWS SDK 等上游 credential chain；
- `env`：由对应 provider 从环境变量解析。

生产环境推荐 scoped secret，不要把凭证放进 URI：

```sql
CREATE SECRET lance_prod_s3 (
  TYPE LANCE,
  PROVIDER credential_chain,
  SCOPE 's3://production-bucket/',
  REGION 'us-east-1'
);
```

MinIO 或其他 S3-compatible 服务可以显式配置：

```sql
CREATE SECRET lance_minio (
  TYPE LANCE,
  PROVIDER config,
  SCOPE 's3://vane-lance-example/',
  ACCESS_KEY_ID 'minioadmin',
  SECRET_ACCESS_KEY 'minioadmin',
  REGION 'us-east-1',
  ENDPOINT 'http://127.0.0.1:19000',
  VIRTUAL_HOSTED_STYLE_REQUEST false,
  ALLOW_HTTP true
);
```

`STORAGE_OPTIONS map([...], [...])` 可以透传当前 allowlist 中还没有的 Lance option。Secret 展示会对包含 `secret`、`password`、`token` 的键脱敏。

### 8.2 支持的 URI family

| Family | URI | 典型配置 | 本文验证程度 |
|---|---|---|---|
| S3 / compatible | `s3://`、`s3a://`、`s3n://` | access key、region、endpoint、path style | MinIO 真实分布式读写、搜索、namespace/drop |
| Google Cloud Storage | `gs://` | token/service account、OpenDAL | secret bind、scope、透传、脱敏 |
| Azure Blob | `az://` | account name/key、SAS、OpenDAL | secret bind、scope、透传、脱敏 |
| Alibaba OSS | `oss://` | endpoint、access key、region | secret bind、scope、透传、脱敏 |
| Hugging Face Hub | `hf://` | token、revision、root | secret bind、scope、透传、脱敏 |

没有真实云账号时，`examples/lance_secrets.py` 只验证 GCS/Azure/OSS/HF 的配置路径，不声称完成这些服务的真实 I/O。

### 8.3 运行 S3 例子

先启动 MinIO 或提供一个已有 endpoint 和 bucket，然后执行：

```bash
export LANCE_S3_ENDPOINT=http://127.0.0.1:19000
export LANCE_S3_BUCKET=vane-lance-example
export LANCE_S3_REGION=us-east-1
export LANCE_S3_ACCESS_KEY_ID=minioadmin
export LANCE_S3_SECRET_ACCESS_KEY=minioadmin
VANE_PROGRESS=0 python examples/lance_s3.py
```

脚本使用唯一前缀，执行分布式 create/append/fragment scan/vector/FTS/hybrid，验证三种 S3 scheme alias，挂载 S3 目录 namespace，最后精确 drop 自己创建的 table。

## 9. REST Namespace

已有 Lance REST Namespace endpoint 时运行：

```bash
export LANCE_REST_ENDPOINT=http://127.0.0.1:12333
export LANCE_REST_NAMESPACE=vane_example
VANE_PROGRESS=0 python examples/lance_rest_namespace.py
```

可选认证通过环境变量传入：

```bash
export LANCE_REST_HEADER='x-lancedb-database=my_db;x-custom-header=value'
export LANCE_REST_BEARER_TOKEN='...'
export LANCE_REST_API_KEY='...'
export LANCE_REST_DELIMITER='$'
```

对应 SQL surface 是：

```sql
ATTACH 'vane_example' AS rest_lance (
  TYPE LANCE,
  ENDPOINT 'http://127.0.0.1:12333',
  DELIMITER '$',
  HEADER 'x-vane-example=executed'
);
```

REST example 真实验证了：

- 在先执行 `SHOW TABLES` 的同一连接里 CTAS，并立即发现新表；
- `query_table` 的 projection、filter、`LIMIT/OFFSET` 下推；
- INSERT、UPDATE、DELETE、MERGE 和 DROP；
- vector/FTS 显式 namespace filter、IVF_FLAT index；
- OPTIMIZE、VACUUM、SET/SHOW/UNSET AUTO_CLEANUP；
- hybrid search 当前返回清晰的 unsupported error。

REST scan 通过 Namespace `query_table` 获取 Arrow IPC，不要求 Vane client 直接拥有底层对象存储凭证。
bind 会先解析并固定具体 table version；执行阶段的分页请求携带该 version，并校验每个 IPC 响应的 schema
和列顺序。因此已创建的 relation 可与普通 mutation 并发而保持旧快照，vacuum/drop 仍会等待它释放。
维护和 mutation 由 endpoint 返回的 location/temporary credential 完成。

## 10. 并发、重试和故障语义

Vane 以规范化、去凭证的 dataset URI 作为 coordinator identity，并提供四类租约：

- snapshot lease：允许并发 reader；等待 active vacuum；
- consistent-snapshot lease：用于需要避开 mutation 的控制面读；等待 active mutation；
- mutation lease：FIFO 串行 mutation，并让位于已排队的 vacuum/consistent reader；
- vacuum lease：等待两类 snapshot 和 mutation 全部结束；一旦排队就阻止新的 reader/writer 插队。

POSIX local 模式除进程内状态机外，还使用按 identity 建立的 advisory file lock，覆盖同一机器、同一用户、
使用同一 coordinator 目录的多个 Vane 进程；进程退出时内核释放锁。Windows、不同主机/用户、其他 Ray
cluster 和外部 Lance writer 不在这个锁域内。Ray 模式的 named coordinator actor 覆盖同一 Ray control
plane 中 identity 相同的 driver/process。coordinator backend 会按 identity 固定，持有 lease 时切换
local/Ray runner 会 fail closed。

所有 acquire 都支持有限、非负 timeout，超时信息包含当前 token；Ray 状态还包含 job/actor/node/pid 等
owner 诊断。Ray actor 重建或 incarnation 丢失会 fail closed，而不会用空状态继续放行。运维恢复必须先
确认实际 reader/writer 已停止，再精确释放 token 或显式重置 coordinator：

```python
from vane.lance import DatasetCoordinator, recover_ray_dataset_coordinator

coordinator = DatasetCoordinator(uri)
status = coordinator.lease_status()
# 只有已证明该 owner 不再运行时：
token = status["outcome_unknown_mutations"][0]
coordinator.force_release(token)

recover_ray_dataset_coordinator(
    uri,
    expected_identity=coordinator.identity,
    force=False,
)
```

系统不会用 TTL 自动释放 active mutation：远端 commit 仍可能运行时这样做会破坏单写者不变量。

分布式 write 有两个必须区分的失败结果：

1. 明确未提交：可以按业务策略重新提交；Vane 会清理 staging 和未提交目标文件。
2. outcome unknown，或已提交但 lease cleanup 失败：可能已经写入，**不要把它作为一个新 write 自动重试**。Vane 分别用 `LanceCommitOutcomeUnknownError` 和 `LanceCommitCleanupError` 表达这种状态，并保留 operation/cleanup 信息供人工核对。

## 11. 完整能力矩阵

| 公开能力 | SQL/Python 入口 | 已执行示例 |
|---|---|---|
| 静态扩展与 setting | `duckdb_extensions()`、`lance_deferred_materialization` | complete |
| URI replacement scan | `SELECT FROM '...lance'` | complete |
| Python scan 与 snapshot | `read_lance`、`LanceDataset.scan/snapshot` | complete |
| filter/projection/limit pushdown | Relation/SQL | complete、distributed、REST |
| create/append/overwrite | `COPY FORMAT LANCE`、`write_lance`、`LanceDataset.write` | complete、distributed、S3 |
| 空 schema dataset | zero-row COPY/distributed write | complete、distributed |
| Vector search | `lance_vector_search`、`vector_search` | complete、distributed、S3、REST |
| Full-text search | `lance_fts`、`fts` | complete、distributed、S3、REST |
| Hybrid search | `lance_hybrid_search`、`hybrid_search` | complete、distributed、S3；REST unsupported contract |
| IVF_FLAT/BTREE/INVERTED | CREATE/SHOW/DROP INDEX | complete；REST 另验 IVF_FLAT |
| index optimize | append/merge/retrain | complete |
| compaction/vacuum | `OPTIMIZE`、`VACUUM LANCE` | complete、REST |
| automatic cleanup | SET/SHOW/UNSET AUTO_CLEANUP | complete、REST |
| directory namespace | `ATTACH ... TYPE LANCE`、`LanceNamespace` | complete、S3 |
| REST namespace/options | ENDPOINT/HEADER/DELIMITER/auth options | REST |
| schema-only/CTAS | CREATE TABLE/CREATE TABLE AS | complete、REST |
| INSERT/UPDATE/DELETE | SQL、`LanceTable` | complete、REST |
| MERGE/RETURNING | SQL、`LanceMergeBuilder` | complete、REST |
| TRUNCATE/DROP | SQL、`LanceTable/LanceNamespace` | complete、REST |
| add/rename/type/drop column | ALTER TABLE、Python helpers | complete |
| not-null/comments/default metadata | ALTER/COMMENT | complete |
| read-only attachment | READ_ONLY、`read_only=True` | complete |
| config/chain/env secrets | CREATE SECRET TYPE LANCE | secrets、S3 |
| S3/GCS/Azure/OSS/HF family | scoped storage options | secrets；S3 有真实 I/O |
| `s3a`/`s3n` normalization | URI aliases | S3 |
| Ray fragment scan/global search | Vane Ray runner | distributed、S3 |
| Ray single-transaction write | distributed `write_lance` | distributed、S3 |

## 12. 本文的实际验证记录

2026-08-14 清理 `lance-duckdb` 的独立 DuckDB/extension-ci-tools
submodule 和 standalone extension 工具链后，使用
`DUCKDB_LANCE_DIRECTORY` 指向 revision
`856203ca15bdf21e1f6c2962038ccbd4598573b0` 的本地 checkout，重新增量编译并执行了：

```text
VANE_PROGRESS=0 python examples/lance_complete.py
  -> ALL LOCAL LANCE EXAMPLES PASSED

VANE_PROGRESS=0 python examples/lance_distributed.py
  -> ALL DISTRIBUTED LANCE EXAMPLES PASSED

python examples/lance_secrets.py
  -> ALL LANCE SECRET EXAMPLES PASSED

VANE_PROGRESS=0 python examples/lance_s3.py
  -> ALL S3 LANCE EXAMPLES PASSED

VANE_PROGRESS=0 python examples/lance_rest_namespace.py
  -> ALL REST LANCE EXAMPLES PASSED

scripts/run_installed_pytest.sh -m 'not real_ray' \
  tests/fast/test_local_fte_runner_entrypoint.py \
  tests/fast/test_lance.py \
  tests/fast/test_lance_coordinator.py \
  tests/fast/test_package_metadata.py
  -> 93 passed, 7 deselected

scripts/run_installed_pytest.sh -m 'real_ray and not ray_cluster_owner' \
  tests/fast/test_local_fte_runner_entrypoint.py \
  tests/fast/test_lance.py \
  tests/fast/test_lance_coordinator.py \
  tests/fast/test_package_metadata.py
  -> 7 passed, 93 deselected

scripts/run_release_tests.sh
  -> 564 passed, 21 deselected
  -> 17 passed, 568 deselected

scripts/run_fast_tests.sh non-ray
  -> 6283 passed, 602 skipped, 130 deselected, 7 xfailed, 1 xpassed

scripts/run_fast_tests.sh shared-ray
  -> 110 passed, 2 skipped, 6911 deselected

scripts/run_fast_tests.sh owner-ray
  -> 7 cluster-owner cases passed in their test-owned Ray processes
```

focused tests 按 marker 分成独立 pytest 进程，避免把 non-Ray、共享 Ray cluster 和 test-owned Ray cluster
混在一个长生命周期进程里。首次完整 fast run 的 shared-Ray shard 在无 Python traceback 的 native crash 后
退出；该用例单独通过，清理残留 Ray 状态后完整重跑 shared-Ray shard 得到上面的通过结果。

S3 验证使用 MinIO `RELEASE.2025-01-20T14-49-07Z`，监听
`127.0.0.1:19000`。REST 验证使用与扩展依赖一致的
`lance-namespace-impls 9.0.1` `RestAdapter`，监听 `127.0.0.1:12333`，底层是临时 directory namespace。

验证范围的诚实边界是：Ray 测试是单机多 worker；MinIO 是真实对象存储协议但不是 AWS S3；REST 是真实 HTTP/Arrow IPC adapter；GCS、Azure、OSS、Hugging Face 因无账号只验证了配置、scope、option forwarding 和 redaction。最终 MERGE planner 修复后的本地完整示例、focused tests 和 release shards 已重跑；当时没有可用 MinIO/REST 服务，因此没有在最后一次 native rebuild 后再次执行这两个外部服务数据面示例。
