# Vane 使用 Lance：完整、可执行、已验证的示例

本文给出 Vane 当前 Lance 集成的完整使用示例，并说明每一类功能的实际验证范围。
配套程序是 [`examples/lance_complete.py`](examples/lance_complete.py)：示例和断言写在
同一个文件中，任何结果不符合预期都会以非零状态退出。

本文中的“覆盖全部 lance-duckdb 功能”采用一个可审计的边界：覆盖 Vane 当前构建启用
的 lance-duckdb 公共 SQL、Python 和分布式功能，而不是声称覆盖任意 Lance crate 内部
函数或未编译的可选后端。功能清单以 vendored 版本的
[`docs/sql.md`](external/lance-duckdb/docs/sql.md)、
[`docs/cloud.md`](external/lance-duckdb/docs/cloud.md)、
[`docs/rest.md`](external/lance-duckdb/docs/rest.md) 和实际注册代码为准。

## 1. 当前支持边界

Vane 当前构建支持：

- Linux x86-64；
- 本地文件系统；
- AWS S3 和 S3-compatible 服务，例如 MinIO；
- 目录 namespace；
- REST namespace；
- 普通扫描、写入、完整目录 namespace DML/DDL、三类搜索、索引和 maintenance；
- local FTE 和 Ray FTE，其中普通 fragment scan 可以跨 Ray 节点执行。

Vane 将 Lance 静态链接进 `vane._native`，所以不要执行 `INSTALL lance` 或
`LOAD lance`。下面的检查就是加载验证：

```python
import vane

connection = vane.connect()
assert connection.execute(
    """
    SELECT loaded, installed
    FROM duckdb_extensions()
    WHERE extension_name = 'lance'
    """
).fetchone() == (True, True)
```

在最终安装产物上查询 `duckdb_functions()`，当前对用户注册的 `lance%` table function
恰好是 `lance_vector_search`、`lance_fts` 和 `lance_hybrid_search`，本文三者均覆盖。
以 `__lance_` 开头的函数是这些 SQL/DDL 路径使用的内部执行入口，不作为用户 API
单独示范。

vendored lance-duckdb 的云文档还描述了 GCS、Azure、OSS 和 Hugging Face，但这些是由
Cargo feature 决定的条件能力。Vane 当前关闭 Lance 默认 feature，只启用 AWS 和 REST
所需 feature，因此这四种后端不属于当前 Vane 功能面，也没有在本文中伪造“已验证”
示例。

## 2. 运行完整示例

按照 [`DEVELOPMENT.md`](DEVELOPMENT.md) 安装 Vane，不要使用 editable install。只需
运行本地示例时：

```bash
cd /tmp
VANE_RUNNER=local-fast \
  /home/yuwei/vane/.venv/bin/python \
  /home/yuwei/vane/examples/lance_complete.py local
```

默认使用一个新的 `/tmp/vane-lance-complete-*` 目录，并在每个步骤输出 `PASS`。也可以
保留到指定目录：

```bash
VANE_RUNNER=local-fast .venv/bin/python examples/lance_complete.py \
  local --root /tmp/my-vane-lance-example
```

`--root` 必须是新的空目录；示例会在其中创建固定命名的数据集。默认临时目录最适合
重复执行，也避免示例替用户删除已有文件。

## 3. Python API 总览

| 接口 | 示例中的覆盖 |
| --- | --- |
| `vane.read_lance(uri)` | 固定版本 relation、filter/project/aggregate |
| `connection.read_lance(uri)` | connection 级扫描 |
| `relation.write_lance()` / `relation.to_lance()` | `create`、`append`、`overwrite` 和文件/row-group 参数 |
| `LanceDataset.scan()` / `snapshot()` | 扫描、snapshot lease、运行中查询固定版本 |
| `LanceDataset.vector_search()` | exact/indexed、`k`、`nprobes`、`refine_factor`、`prefilter`、`use_index`、`filter` |
| `LanceDataset.fts()` | FTS、`k`、`prefilter`、REST 显式 `filter` |
| `LanceDataset.hybrid_search()` | vector + FTS、`alpha`、`oversample_factor` 和 indexed/exact 路径 |
| `LanceDataset.write()` | 单事务 writer convenience API |
| `create_index()` / `show_indexes()` / `drop_index()` | 所有已注册 scalar/vector index type |
| `optimize()` / `vacuum()` | compact 和旧版本清理 |
| `LanceNamespace` | 目录/REST attach、read-only、table/create/drop/detach |
| `LanceTable` | scan、insert/update/delete/truncate、列 DDL、merge |
| `LanceMergeBuilder` | matched update/delete、not-matched insert |

Python builder 覆盖常用且安全的接口；更完整的 SQL 动作，例如 `MERGE ... RETURNING`、
`WHEN NOT MATCHED BY SOURCE`、comments 和 auto-cleanup，在同一示例中使用原始 SQL。

## 4. 写入、读取和固定快照

### 4.1 Create、append 和 overwrite

下面是配套程序实际执行的核心写法：

```python
from vane.lance import LanceDataset

dataset = LanceDataset("/tmp/example/search.lance", connection)

source.to_lance(
    dataset.uri,
    mode="create",
    max_rows_per_file=2,
    max_rows_per_group=1,
    max_bytes_per_file=1_048_576,
    data_storage_version="2.2",
)
dataset.write(more_rows, mode="append")
replacement.write_lance(dataset.uri, mode="overwrite")
```

验证同时确认：第二次对已存在路径使用 `mode="create"` 会失败，不会静默覆盖。

`COPY` 的低层 SQL 接口也已执行：

```sql
COPY (SELECT 1::BIGINT AS id, 'x'::VARCHAR AS value LIMIT 0)
TO '/tmp/example/empty.lance'
(FORMAT lance, mode 'overwrite', write_empty_file true);
```

示例逐一创建并读取了 `data_storage_version` 为 `2.0`、`2.1`、`2.2`、`stable` 和
`next` 的数据集。append 保留原数据集版本格式。

### 4.2 三种读取入口和普通 SQL

```python
relation1 = vane.read_lance(uri, connection=connection)
relation2 = connection.read_lance(uri)
relation3 = LanceDataset(uri, connection).scan()

assert relation1.aggregate("count(*), sum(id)").fetchone() == (6, 21)
```

也可以直接把 URI 当成 DuckDB 表：

```sql
SELECT id, metadata.rank
FROM '/tmp/example/search.lance'
WHERE label BETWEEN 2 AND 4
ORDER BY id;
```

完整示例执行并断言了：

- projection、filter、aggregate；
- `LIMIT` + `OFFSET` 下推；
- `TABLESAMPLE SYSTEM`；
- LIST/STRUCT 表达式；
- 单 fragment 的 `rowid IN (...)` 点查和 `_rowid` 一致性；
- `lance_deferred_materialization` extension setting 的 false/true round trip；
- `EXPLAIN (FORMAT JSON)` 中的 Lance pushdown 标记。

### 4.3 查询开始后保持同一版本

仅仅构造一个 relation 不代表它已经进入执行期。示例使用两个 connection 和一个
确定性 Arrow barrier：

1. reader 启动查询并在 bind 后的执行屏障等待；
2. writer append 新版本；
3. 释放 reader；
4. 运行中的 reader 仍只返回旧版本 4 行；
5. commit 后新启动的查询返回 5 行。

这与“先 append、后第一次执行一个惰性 relation”不同；后者当然可以 bind 到新版本。

## 5. Vector、FTS 和 hybrid search

### 5.1 Vector search

```python
nearest = dataset.vector_search(
    "vec",
    [0.0, 0.0, 0.0, 0.0],
    k=3,
    nprobes=2,
    refine_factor=2,
    prefilter=False,
    use_index=False,
)

rows = nearest.order("_distance").project("id, _distance").fetchall()
```

返回的 `_distance` 越小越近。示例还实际执行了：

- `FLOAT[N]` 查询向量；
- `DOUBLE[]` 到 float32 的查询向量转换；
- exact KNN；
- IVF index 查询；
- `prefilter=True` 后再计算 top-k；
- verbose `EXPLAIN` 中出现实际使用的 `ANNSubIndex`。

### 5.2 FTS

```python
hits = (
    dataset.fts("text", "puppy", k=10, prefilter=True)
    .filter("label >= 2")
    .order("_score DESC")
)
```

`_score` 越大越相关。示例在没有索引和创建 `INVERTED` 索引后都执行了查询。

### 5.3 Hybrid

```python
hits = dataset.hybrid_search(
    "vec",
    [0.0, 0.0, 0.0, 0.0],
    "text",
    "puppy",
    k=3,
    nprobes=1,
    refine_factor=2,
    use_index=True,
    alpha=0.5,
    oversample_factor=4,
)
```

结果包含 `_hybrid_score`、`_distance` 和 `_score`。在 Ray 模式下，三类 search 都是
一个全局 Lance source task；Vane 不会按 fragment 分别计算局部 top-k 后拼接。

## 6. Directory namespace、DML、MERGE 和 DDL

```python
from vane.lance import LanceNamespace

namespace = LanceNamespace("/tmp/lance-root", "demo", connection=connection)
table = namespace.create_table(
    "items",
    "SELECT * FROM (VALUES (1::BIGINT, 'one')) AS input(id, value)",
)

table.insert("SELECT 2::BIGINT AS id, 'two'::VARCHAR AS value")
table.update({"value": "upper(value)"}, where="id = 2")
table.delete(where="id = 1")
table.truncate()
namespace.drop_table("items")
namespace.detach()
```

除了 Python API，完整示例还执行了以下 SQL 功能：

- schema-only `CREATE TABLE`；
- CTAS 和 `WITH (data_storage_version = ...)`；
- `INSERT VALUES` 和 `INSERT SELECT`；
- 条件/全表 `UPDATE`、表达式更新和事务 rollback；
- 条件/全表 `DELETE`；
- `TRUNCATE TABLE`；
- `DROP TABLE` 和 `DROP TABLE IF EXISTS`；
- table/column comments；
- `ADD COLUMN ... DEFAULT`；
- `SET NOT NULL` 和 `DROP NOT NULL`；
- `RENAME COLUMN`、`ALTER COLUMN ... TYPE`、`DROP COLUMN`；
- namespace table 的动态发现。
- read-only namespace 可以扫描，并拒绝 mutation。

`read_only=True` 不只是 Python 侧约定：attach 后的 catalog 保持 DuckDB read-only
database 身份，查询可用，而 `INSERT` 等 mutation 在执行前由 DuckDB 拒绝。本轮审计
修复了 read-only Lance catalog 的内部 `:memory:` backing store 无法初始化的问题，并
用本地 directory namespace 与 REST + Ray 两条路径实际验证。

当前 vendored SQL 文档仍写着 `SET NOT NULL` 不支持，但实际 vendored 测试和本示例都已
确认当前代码支持；本文以实际构建结果为准。

### 6.1 完整 MERGE 动作

Python builder 示例：

```python
table.merge(source_sql, "target.id = source.id").when_matched_update(
    {"value": "source.value"}
).when_not_matched_insert(
    {"id": "source.id", "value": "source.value"}
).execute()

table.merge(
    "SELECT 4::BIGINT AS id", "target.id = source.id"
).when_matched_delete().execute()
```

配套程序还逐一执行并断言了所有已支持 action：

- `WHEN MATCHED THEN UPDATE`；
- `WHEN MATCHED THEN DELETE`；
- `WHEN MATCHED THEN DO NOTHING`；
- `WHEN MATCHED THEN ERROR`，作为预期错误校验；
- `WHEN NOT MATCHED THEN INSERT`；
- `WHEN NOT MATCHED THEN DO NOTHING`；
- `WHEN NOT MATCHED BY SOURCE THEN UPDATE`；
- `WHEN NOT MATCHED BY SOURCE THEN DELETE`；
- `WHEN NOT MATCHED BY SOURCE THEN DO NOTHING`；
- `RETURNING merge_action, ...`；
- `BEGIN` + `ROLLBACK` 的无提交语义。

## 7. 全部索引类型和生命周期

示例不是只验证 SQL 能被 parser 接受，而是为每一种类型创建真实索引、执行查询或检查
元数据，然后执行 `DROP INDEX`。

| 类别 | 已实建的类型 | 示例列 |
| --- | --- | --- |
| Scalar | `BTREE` | integer、nested `metadata.rank` |
| Scalar | `BITMAP` | 低基数 integer |
| Scalar | `ZONEMAP` | ordered integer |
| Scalar | `BLOOMFILTER` | integer |
| Full text | `INVERTED` | text |
| Text scalar | `NGRAM` | text |
| List scalar | `LABELLIST` | `VARCHAR[]` |
| Vector | `IVF_FLAT` | `FLOAT[4]` / `FLOAT[8]` |
| Vector | `IVF_PQ` | `FLOAT[8]`，PQ sub-vector 参数 |
| Vector | `IVF_SQ` | `FLOAT[8]`，scalar quantization 参数 |
| Vector | `IVF_RQ` | `FLOAT[8]`，residual quantization 参数 |
| Vector | `IVF_HNSW_FLAT` | IVF + HNSW + flat |
| Vector | `IVF_HNSW_PQ` | IVF + HNSW + PQ |
| Vector | `IVF_HNSW_SQ` | IVF + HNSW + SQ |

典型写法：

```python
dataset.create_index(
    "vec_idx",
    "vec",
    index_type="IVF_HNSW_PQ",
    num_partitions=1,
    metric_type="l2",
    num_sub_vectors=2,
    num_bits=4,
    max_iterations=2,
    hnsw_m=8,
    hnsw_ef_construction=20,
)

assert dataset.show_indexes().fetchall()
dataset.drop_index("vec_idx")
```

PQ 示例的 8 维向量使用 2 个 sub-vectors；`num_bits=4` 时把它错误设置为 1 会被 Lance
正确拒绝。生产参数应按数据规模和召回率基准选择，本文的小参数只用于快速、确定性的
功能验证。

## 8. Index maintenance、OPTIMIZE、VACUUM 和 auto-cleanup

示例在 index 建成后 append 新 fragment，并实际执行三种 index optimize mode：

```sql
ALTER INDEX vec_idx ON '/tmp/example/items.lance'
OPTIMIZE WITH (mode = 'append');

ALTER INDEX vec_idx ON '/tmp/example/items.lance'
OPTIMIZE WITH (mode = 'merge', num_indices_to_merge = 1);

ALTER INDEX vec_idx ON '/tmp/example/items.lance'
OPTIMIZE WITH (mode = 'retrain');
```

文件 compact 和旧版本清理：

```python
dataset.optimize(
    target_rows_per_fragment=1024,
    max_rows_per_group=128,
    max_bytes_per_file=0,
    materialize_deletions=True,
    materialize_deletions_threshold=0.1,
    num_threads=1,
    batch_size=256,
    defer_index_remap=False,
)

dataset.vacuum(
    older_than_seconds=0,
    delete_unverified=False,
    error_if_tagged_old_versions=True,
    retain_n_versions=1,
)
```

auto-cleanup 也做了 set/show/unset round trip：

```sql
ALTER TABLE '/tmp/example/items.lance'
SET AUTO_CLEANUP WITH (interval = 1, older_than = '1s', retain_versions = 2);

SHOW MAINTENANCE ON '/tmp/example/items.lance';

ALTER TABLE '/tmp/example/items.lance' UNSET AUTO_CLEANUP;
```

## 9. MinIO / S3-compatible 完整示例

先创建 bucket，再提供环境变量。不要把真实凭证写进脚本、物理计划或文档：

```bash
export TEST_MINIO_ENDPOINT=http://127.0.0.1:9000
export TEST_MINIO_ACCESS_KEY='<access-key>'
export TEST_MINIO_SECRET_KEY='<secret-key>'
export TEST_MINIO_REGION=us-east-1
export TEST_MINIO_BUCKET=lance-example

VANE_RUNNER=local-fast .venv/bin/python examples/lance_complete.py \
  minio --prefix "vane-lance-example/$(date +%s)"
```

此模式实际验证：

- temporary、connection-local、按 URI scope 匹配的 `TYPE LANCE` secret；
- `PROVIDER config`；
- `PROVIDER credential_chain`；
- `PROVIDER env`；
- `STORAGE_OPTIONS` map；
- endpoint、region、path-style、HTTP MinIO 设置；
- S3 写入、读取和目录 namespace；
- vector/FTS index 和 indexed hybrid search；
- S3 上的 optimize 和 vacuum；
- 另一个 connection 看不到当前 connection 的 temporary secret。

示例会把 `TEST_MINIO_*` 映射为当前进程的 AWS credential-chain 环境变量。若使用临时
AWS 凭证，可以额外设置 `AWS_SESSION_TOKEN`；代码会把它加入 config secret。本文的
本地 MinIO 实跑使用长期测试 key，没有伪造一个无法由 MinIO 校验的 session token，
因此“真实 STS token 的服务端鉴权”不标为已验证。

secret scope 要覆盖 namespace 根本身。若 scope 写成
`s3://bucket/root/`，但 attach 的 URI 是不带尾斜杠的 `s3://bucket/root`，该 secret
不会匹配；完整示例使用 `SCOPE 's3://bucket/root'`。

## 10. REST namespace 完整示例

需要一个可写的 Lance REST Namespace 服务和已经存在的 namespace ID：

```bash
VANE_RUNNER=local-fast .venv/bin/python examples/lance_complete.py rest \
  --endpoint http://127.0.0.1:2333 \
  --namespace-id demo \
  --table vane_complete_example
```

示例执行的 attach 同时覆盖 `ENDPOINT`、`DELIMITER`、`HEADER`、`BEARER_TOKEN` 和
`API_KEY` 参数。示例 token 是本地无鉴权适配器接受的非敏感占位值；生产服务必须换成
真实认证信息。

该模式实际验证：

- table list、create、insert、update、delete 和 drop；
- read-only REST attach 可以 scan，并在执行前拒绝 mutation；
- `query_table` 的 projection、filter、`LIMIT` + `OFFSET` 下推；
- `EXPLAIN` 中 `namespace_query_table` backend 标记；
- vector/FTS 的 REST 显式 `filter` + prefilter；
- hybrid search；
- REST 表的 index create/show 和 indexed search；
- REST 表的 optimize、vacuum 和 auto-cleanup round trip。

普通 schema evolution `ALTER TABLE ... ADD/RENAME/TYPE/DROP COLUMN` 当前只支持目录
namespace；REST table 会明确返回 `ALTER TABLE operation not supported for Lance
tables`。maintenance 使用的 `ALTER TABLE ... SET/UNSET AUTO_CLEANUP` 是单独注册的
Lance maintenance 语法，REST 路径支持。

这轮实际 REST 验证使用 `lance-namespace-impls 9.0.1` 的 `RestAdapter` 包装可写的
manifest directory backend。验证过程中发现并修复了相对 table list 结果丢失
namespace 前缀、导致 REST `DROP TABLE` 失败的问题；最终示例确认 drop 后 table list
不再包含该表。

## 11. Ray 双节点示例

生产集群已经启动时，用户侧执行方式是：

```python
import vane
from vane import runners
from vane.lance import LanceDataset

vane.set_runner_ray(address="auto")
connection = vane.connect()
dataset = LanceDataset("s3://warehouse/events.lance", connection)

relation = dataset.scan().filter("event_date >= DATE '2026-08-01'")
tables = list(runners.get_or_create_runner().run_iter_tables(relation))

# 上游 relation 可以跨节点执行，最后只有一个 Lance transaction owner。
relation.write_lance("s3://warehouse/events-copy.lance", mode="overwrite")
```

仓库中的真实双节点验收不是 mock。它在一台机器上用 `ray.cluster_utils.Cluster` 创建两个
Ray node，每个 node 配置 4 CPU、8 GiB worker memory 和 1 GiB object store，然后连接
真实 MinIO：

```bash
TEST_MINIO_ENDPOINT=http://127.0.0.1:9000 \
TEST_MINIO_ACCESS_KEY='<access-key>' \
TEST_MINIO_SECRET_KEY='<secret-key>' \
TEST_MINIO_REGION=us-east-1 \
TEST_MINIO_BUCKET=lance-example \
scripts/run_installed_pytest.sh \
  -o addopts= \
  tests/fast/test_lance.py::test_ray_lance_two_node_minio_fragment_scan_snapshot_and_search \
  -vv
```

该测试检查的不只是两个节点处于 Alive：

- 65,536 行输入被写成至少 8 个 fragment scan descriptors；
- Ray GCS task events 中，本次查询产生的 FTE task NodeID 集合必须等于两个 live
  node 的 NodeID 集合；
- 两节点聚合的 count/sum 与单机数学结果一致；
- scan 启动后 append，新旧查询分别看到旧/新版本；
- S3 session 配置能到达 worker，但物理计划没有 explicit credential；
- vector 和 FTS index commit 后，hybrid search 仍是一个全局 source，并返回单机相同
  顺序；
- MinIO directory namespace、optimize 和 vacuum 可用；
- fixture 最后关闭 Ray cluster 并删除本次 bucket prefix。

REST namespace 的 Ray 路径另用真实 HTTP 服务执行了
`test_ray_lance_rest_namespace_query_table_is_global_source`：计划只有一个 partition，
并携带恰好一个 `lance-global-v1` descriptor，随后由 Ray worker 通过 `query_table`
读回正确结果；最终为 `1 passed in 9.96s`。

单次 search 不跨两个 node 做局部 top-k，这是正确性设计，不是“没有分布式”。普通
fragment scan 跨节点；search 在一个 worker 内由 Lance 并发，并且多个 search 查询
可以被调度到不同 worker。

## 12. 功能覆盖矩阵

| lance-duckdb/Vane 公共功能 | 本地 | MinIO | REST | 双节点 Ray |
| --- | --- | --- | --- | --- |
| 静态加载检查 | 已执行 | 已执行 | 已执行 | worker 实际加载 |
| URI scan、projection/filter | 已执行 | 已执行 | `query_table` 已执行 | fragment scan 已执行 |
| limit/offset/sampling/rowid/deferred materialization setting | 已执行 | scan 已执行 | limit/offset 已执行 | global/fragment 计划测试 |
| create/append/overwrite、空表、storage version | 已执行 | overwrite 已执行 | CTAS/insert 已执行 | single writer 已执行 |
| directory namespace（含 read-only） | 已执行 | 已执行 | 不适用 | MinIO namespace 已执行 |
| REST namespace + auth/header 参数 | 不适用 | 不适用 | 已执行 | REST global-source 另有定向测试 |
| INSERT/UPDATE/DELETE/TRUNCATE | 已执行 | 同一引擎路径 | REST 前三项已执行 | mutation owner 路径已执行 |
| 全部 MERGE action + RETURNING + rollback | 已执行 | 同一引擎路径 | 未声明 REST MERGE | coordinator/Ray 定向测试 |
| comments/列 schema evolution | 已执行 | 同一目录路径 | generic ALTER 不支持 | 不拆成 Ray task |
| vector/FTS/hybrid | exact/indexed 已执行 | indexed 已执行 | exact/indexed 已执行 | global source 已执行 |
| 7 种 scalar/text/list index | 全部实建 | IVF/INVERTED 实建 | IVF/INVERTED 实建 | index commit 已执行 |
| 7 种 vector index | 全部实建并查询 | IVF_FLAT 实建 | IVF_FLAT 实建 | IVF_FLAT 实建 |
| show/drop index | 已执行 | index 使用已执行 | show 已执行 | commit 可见性已执行 |
| index optimize append/merge/retrain | 三种均已执行 | 引擎路径相同 | 未重复三种 mode | 不拆成 Ray task |
| optimize/vacuum/auto-cleanup | 已执行 | optimize/vacuum | 三项已执行 | optimize/vacuum 已执行 |
| TYPE LANCE config/chain/env secret | 已执行创建 | 三种 provider 实际读写 | namespace auth 参数 | worker session/脱敏已执行 |

矩阵中的“同一引擎路径”表示该功能没有为每种存储后端机械重复所有组合，但至少在本地
完成完整语义验证，并在 MinIO/REST 上完成该后端的关键读写、搜索、索引和 maintenance
闭环。它不等于声称所有后端组合都已穷举。

## 13. 本轮真实验证记录

验证日期：2026-08-11。

| 环境 | 实际执行 | 结果 |
| --- | --- | --- |
| 安装后的 native | 从 `/tmp` import，检查 `vane._native.__file__`、`PRAGMA version`、SourceID、extension 状态 | 通过 |
| Local | `examples/lance_complete.py local` | 所有断言通过 |
| MinIO | 隔离的真实 MinIO server，`examples/lance_complete.py minio` | config/chain/env、读写、namespace、search/index、maintenance 全部通过 |
| REST | 真实 HTTP `RestAdapter`，`examples/lance_complete.py rest` | query_table、search、DML、index、maintenance、修复后的 drop 全部通过 |
| Ray 双节点 + MinIO | 显式断言 fragment scan 任务分布到两个 NodeID | `1 passed in 17.43s` |
| Ray + REST | 真实 HTTP `query_table` 全局 source；一个 partition、一个 global descriptor | `1 passed in 9.96s` |
| Lance 定向回归 | `tests/fast/test_lance.py` 的非外部服务用例 | `11 passed, 3 deselected` |
| Release 回归 | `scripts/run_release_tests.sh` 的 non-Ray / real-Ray 分片 | `536 passed` / `14 passed` |
| Fast non-Ray | `scripts/run_fast_tests.sh` 的 non-Ray 分片 | `6200 passed, 602 skipped` |
| Fast shared-Ray | 共享真实 Ray cluster 的完整分片重跑 | `104 passed, 6 skipped` |
| Fast cluster-owner Ray | 每个用例独立拥有 Ray cluster | `7 passed, 1 skipped` |

第一次全量 fast 的 shared-Ray 分片中，
`test_real_ray_dataset_coordinator_orders_mutations_and_vacuum` 有一次
`ray.get(first, timeout=5)` 超时；当轮其余结果是 `103 passed, 6 skipped`。
该用例随后隔离复跑为 `1 passed in 7.53s`，再按相同顺序重跑整个
shared-Ray 分片为 `104 passed, 6 skipped`。因此记录为一次未重现的 5 秒
Ray actor 等待超时，而不是删掉或隐藏首轮失败。

最终安装检查使用：

```text
vane._native = /home/yuwei/vane/.venv/lib/python3.12/site-packages/vane/_native.cpython-312-x86_64-linux-gnu.so
PRAGMA version = v1.5.0-vane.c7a90b9699
DuckDB SourceID = ba5076f4c8
lance loaded/installed = true/true
```

`PRAGMA version` 中的 Vane revision 是最近一次提交，DuckDB SourceID 只描述
`external/duckdb` 内容；本轮未提交的文档/example 和 lance-duckdb 修复不会改变
DuckDB SourceID。提交本轮修改并重建后，Vane revision 会更新为新 commit。

## 14. 已确认的限制和使用注意事项

1. **REST generic ALTER 不支持。** 目录 namespace 已覆盖完整列 DDL；REST 只声明并
   验证当前实现实际支持的 DML、索引、search 和 maintenance。
2. **多 fragment 的 rowid point lookup 有已复现问题。** 单 fragment `rowid IN` 已通过；
   对 append 后的多 fragment 数据集做相同点查，当前会把全局 rowid 错当成 fragment
   内偏移并报 `Invalid read params Indices(...)`。普通 fragment scan 不受影响。本文不把
   这个失败路径标成已支持。
3. **REST 快照由远端服务负责。** 本地/S3 数据集由 Vane 序列化 numeric version；REST
   `query_table` 的跨请求一致性取决于 REST 服务实现。
4. **原始 SQL 会绕过 Python coordinator。** 需要同一数据集 mutation FIFO、snapshot
   lease 或 vacuum 保护时，优先使用 `LanceDataset`、`LanceNamespace`、`LanceTable`
   和 `relation.write_lance`。
5. **不同 Ray cluster 不共享 coordinator。** 外部 writer 和另一集群由 Lance optimistic
   transaction conflict 检测兜底，Vane 不做隐式 retry。
6. **共享存储必须对所有节点可见。** 真正多机器 Ray 推荐 S3；本地路径只有在所有节点
   挂载同一共享文件系统时才安全。
7. **Local runner 不是多进程替代品。** 当前 local FTE worker 在同一进程；多进程、
   多节点语义以 Ray 验证为准。

更完整的实现、线程池、CPU admission、single writer、snapshot lease 和多节点并发分析
见 [`LANCE_INTEGRATION.md`](LANCE_INTEGRATION.md)；相对原始 lance-duckdb 的源码差异见
[`LANCE_DUCKDB_UPSTREAM_DELTA.md`](LANCE_DUCKDB_UPSTREAM_DELTA.md)。
