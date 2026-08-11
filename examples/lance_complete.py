# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

"""Executable, assertion-backed examples for Vane's Lance integration.

Run this file with the installed Vane package, not an editable install. See
LANCE_COMPLETE_EXAMPLES.md for the commands used to verify every mode.
"""

from __future__ import annotations

import argparse
import os
import re
import tempfile
import threading
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa

import vane
from vane.lance import LanceDataset, LanceNamespace


def _literal(value: str | Path) -> str:
    return "'" + os.fspath(value).replace("'", "''") + "'"


def _check(label: str, actual: Any, expected: Any) -> None:
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")
    print(f"PASS {label}: {actual!r}")


def _expect_error(label: str, action: Callable[[], Any], message: str) -> None:
    try:
        action()
    except Exception as exc:
        if message not in str(exc):
            raise AssertionError(
                f"{label}: expected error containing {message!r}, got {type(exc).__name__}: {exc}"
            ) from exc
        print(f"PASS {label}: expected {type(exc).__name__}")
        return
    raise AssertionError(f"{label}: expected an error containing {message!r}")


def _search_source_sql(start: int = 1) -> str:
    rows = [
        (start, "puppy plays in the park", 1, "[0.0, 0.0, 0.0, 0.0]", "['pet', 'red']"),
        (start + 1, "kitten sleeps on the couch", 2, "[2.0, 0.0, 0.0, 0.0]", "['pet', 'blue']"),
        (start + 2, "puppy eats food", 3, "[0.0, 3.0, 0.0, 0.0]", "['pet', 'food']"),
        (start + 3, "politics news update", 4, "[0.0, 0.0, 4.0, 0.0]", "['news']"),
        (start + 4, "sports news today", 5, "[0.0, 0.0, 0.0, 5.0]", "['news', 'sport']"),
    ]
    values = ",\n".join(
        f"({row_id}::BIGINT, '{text}'::VARCHAR, {label}::INTEGER, "
        f"{vector}::FLOAT[4], {tags}::VARCHAR[], "
        f"{{'rank': {label}::INTEGER, 'active': true}})"
        for row_id, text, label, vector, tags in rows
    )
    return "SELECT * FROM (VALUES\n" + values + ") AS input(id, text, label, vec, tags, metadata)"


def _verify_extension(connection: Any) -> None:
    extension = connection.execute(
        "SELECT loaded, installed FROM duckdb_extensions() WHERE extension_name = 'lance'"
    ).fetchone()
    _check("statically linked Lance extension", extension, (True, True))
    version = connection.execute("PRAGMA version").fetchone()
    print(f"INFO DuckDB version={version[0]} source_id={version[1]} codename={version[2]}")
    print(f"INFO native={vane._native.__file__}")
    connection.execute("SET lance_deferred_materialization = false")
    _check(
        "lance_deferred_materialization=false",
        connection.execute("SELECT current_setting('lance_deferred_materialization')").fetchone(),
        (False,),
    )
    connection.execute("SET lance_deferred_materialization = true")
    _check(
        "lance_deferred_materialization=true",
        connection.execute("SELECT current_setting('lance_deferred_materialization')").fetchone(),
        (True,),
    )


def _verify_write_scan_and_snapshot(connection: Any, root: Path) -> Path:
    dataset_path = root / "search.lance"
    dataset = LanceDataset(dataset_path, connection)

    # Both relation aliases and the dataset convenience method own one Lance
    # transaction. Small files make fragment-level scans visible to Vane.
    connection.sql(_search_source_sql()).to_lance(
        str(dataset_path),
        mode="create",
        max_rows_per_file=2,
        max_rows_per_group=1,
        max_bytes_per_file=1_048_576,
        data_storage_version="2.2",
    )
    _check("relation.to_lance(create)", dataset.scan().aggregate("count(*)").fetchone(), (5,))

    append = connection.sql(
        """
        SELECT 6::BIGINT AS id,
               'archived note'::VARCHAR AS text,
               6::INTEGER AS label,
               [9.0, 9.0, 9.0, 9.0]::FLOAT[4] AS vec,
               ['archive']::VARCHAR[] AS tags,
               {'rank': 6::INTEGER, 'active': false} AS metadata
        """
    )
    dataset.write(append, mode="append", max_rows_per_file=2, max_rows_per_group=1)
    _check("fresh scan sees committed append", dataset.scan().aggregate("count(*)").fetchone(), (6,))

    # The three public scan entry points all bind a fixed Lance version.
    expected = (6, 21)
    _check(
        "vane.read_lance",
        vane.read_lance(str(dataset_path), connection=connection)
        .aggregate("count(*)::BIGINT, sum(id)::BIGINT")
        .fetchone(),
        expected,
    )
    _check(
        "connection.read_lance",
        connection.read_lance(str(dataset_path)).aggregate("count(*)::BIGINT, sum(id)::BIGINT").fetchone(),
        expected,
    )
    _check(
        "LanceDataset.scan",
        dataset.scan().aggregate("count(*)::BIGINT, sum(id)::BIGINT").fetchone(),
        expected,
    )

    quoted = _literal(dataset_path)
    _check(
        "URI scan projection/filter",
        connection.execute(
            f"SELECT id, metadata.rank FROM {quoted} WHERE label BETWEEN 2 AND 4 ORDER BY id"
        ).fetchall(),
        [(2, 2), (3, 3), (4, 4)],
    )
    _check(
        "LIMIT/OFFSET",
        connection.execute(f"SELECT id FROM {quoted} LIMIT 2 OFFSET 1").fetchall(),
        [(2,), (3,)],
    )
    _check(
        "TABLESAMPLE SYSTEM",
        connection.execute(f"SELECT count(*) FROM {quoted} TABLESAMPLE SYSTEM (100 PERCENT)").fetchone(),
        (6,),
    )
    rowid_path = root / "rowid-single-fragment.lance"
    connection.sql("SELECT i::BIGINT AS id FROM range(5) AS input(i)").write_lance(str(rowid_path), mode="overwrite")
    _check(
        "native rowid point lookup order (single fragment)",
        connection.execute(
            f"""
            SELECT id, CAST(rowid AS BIGINT), _rowid
            FROM {_literal(rowid_path)} WHERE rowid IN (2, 0, 4)
            """
        ).fetchall(),
        [(2, 2, 2), (0, 0, 0), (4, 4, 4)],
    )
    _check(
        "nested LIST/STRUCT expressions",
        connection.execute(
            f"SELECT id FROM {quoted} WHERE list_contains(tags, 'pet') AND metadata.rank >= 2 ORDER BY id"
        ).fetchall(),
        [(2,), (3,)],
    )
    explain = "\n".join(
        str(value)
        for row in connection.execute(
            f"EXPLAIN (FORMAT JSON) SELECT id FROM {quoted} WHERE label >= 2 LIMIT 2 OFFSET 1"
        ).fetchall()
        for value in row
    )
    if '"Lance Limit Offset Pushdown": "true"' not in explain:
        raise AssertionError("Lance LIMIT/OFFSET pushdown was not present in EXPLAIN")
    print("PASS scan pushdown diagnostics")

    with dataset.snapshot() as snapshot:
        _check(
            "LanceDataset.snapshot lease",
            snapshot.filter("id >= 5").order("id").project("id").fetchall(),
            [(5,), (6,)],
        )

    # COPY exposes options that are intentionally lower-level than write_lance.
    empty_path = root / "empty.lance"
    connection.execute(
        f"""
        COPY (SELECT 1::BIGINT AS id, 'x'::VARCHAR AS value LIMIT 0)
        TO {_literal(empty_path)}
        (FORMAT lance, mode 'overwrite', write_empty_file true)
        """
    )
    _check("COPY write_empty_file", connection.execute(f"SELECT count(*) FROM {_literal(empty_path)}").fetchone(), (0,))

    for storage_version in ("2.0", "2.1", "2.2", "stable", "next"):
        version_path = root / f"storage-{storage_version.replace('.', '-')}.lance"
        connection.execute(
            f"""
            COPY (SELECT 1::BIGINT AS id)
            TO {_literal(version_path)}
            (FORMAT lance, mode 'overwrite', data_storage_version '{storage_version}')
            """
        )
        _check(
            f"COPY data_storage_version={storage_version}",
            connection.execute(f"SELECT count(*) FROM {_literal(version_path)}").fetchone(),
            (1,),
        )

    create_mode_path = root / "create-mode.lance"
    connection.sql("SELECT 1::BIGINT AS id").write_lance(str(create_mode_path), mode="create")
    _expect_error(
        "write_lance(create) refuses replacement",
        lambda: connection.sql("SELECT 2::BIGINT AS id").write_lance(str(create_mode_path), mode="create"),
        "already exists",
    )
    connection.sql("SELECT 2::BIGINT AS id").write_lance(str(create_mode_path), mode="overwrite")
    _check(
        "relation.write_lance(overwrite)",
        connection.execute(f"SELECT id FROM {_literal(create_mode_path)}").fetchone(),
        (2,),
    )
    return dataset_path


def _verify_bind_time_snapshot(root: Path) -> None:
    dataset_path = root / "concurrent-snapshot.lance"
    reader = vane.connect()
    writer = vane.connect()
    entered = threading.Event()
    release = threading.Event()
    result: list[tuple[int, int]] = []
    errors: list[BaseException] = []
    thread: threading.Thread | None = None
    try:
        reader.sql("SELECT i::BIGINT AS id FROM range(4) AS input(i)").write_lance(str(dataset_path), mode="overwrite")
        barrier_batch = pa.record_batch({"token": pa.array([1], type=pa.int64())})

        def barrier_batches():
            entered.set()
            if not release.wait(timeout=30):
                raise TimeoutError("snapshot example timed out at its execution barrier")
            yield barrier_batch

        reader.register(
            "lance_snapshot_barrier",
            pa.RecordBatchReader.from_batches(barrier_batch.schema, barrier_batches()),
        )
        dataset = LanceDataset(dataset_path, reader)
        query = (
            dataset.scan()
            .set_alias("lance_rows")
            .join(reader.table("lance_snapshot_barrier").set_alias("barrier"), "TRUE")
            .project("lance_rows.id, barrier.token")
            .order("id")
        )

        def run_query() -> None:
            try:
                result.extend(query.fetchall())
            except BaseException as exc:
                errors.append(exc)

        thread = threading.Thread(target=run_query, daemon=True)
        thread.start()
        if not entered.wait(timeout=30):
            raise TimeoutError("snapshot query did not reach its post-bind execution barrier")

        writer.sql("SELECT 4::BIGINT AS id").write_lance(str(dataset_path), mode="append")
        release.set()
        thread.join(timeout=30)
        if thread.is_alive():
            raise TimeoutError("snapshot query did not finish")
        if errors:
            raise errors[0]
        _check(
            "running scan remains on its bind-time version",
            result,
            [(0, 1), (1, 1), (2, 1), (3, 1)],
        )
        _check(
            "query started after commit sees the new version",
            dataset.scan().order("id").fetchall(),
            [(0,), (1,), (2,), (3,), (4,)],
        )
    finally:
        release.set()
        if thread is not None:
            thread.join(timeout=30)
        reader.close()
        writer.close()


def _verify_search_and_indexes(connection: Any, root: Path, dataset_path: Path) -> None:
    dataset = LanceDataset(dataset_path, connection)
    vector = (
        dataset.vector_search(
            "vec",
            [0.0, 0.0, 0.0, 0.0],
            k=3,
            nprobes=2,
            refine_factor=2,
            prefilter=False,
            use_index=False,
        )
        .order("_distance")
        .project("id")
        .fetchall()
    )
    _check("exact vector search", vector, [(1,), (2,), (3,)])
    _check(
        "vector prefilter",
        dataset.vector_search("vec", [0.0, 0.0, 0.0, 0.0], k=10, prefilter=True, use_index=False)
        .filter("label >= 4")
        .order("_distance")
        .project("id")
        .fetchall(),
        [(4,), (5,), (6,)],
    )
    _check(
        "full-text search",
        dataset.fts("text", "puppy", k=10, prefilter=True).filter("label >= 2").order("id").project("id").fetchall(),
        [(3,)],
    )
    _check(
        "hybrid search",
        dataset.hybrid_search(
            "vec",
            [0.0, 0.0, 0.0, 0.0],
            "text",
            "puppy",
            k=3,
            nprobes=2,
            refine_factor=2,
            prefilter=False,
            use_index=False,
            alpha=0.5,
            oversample_factor=4,
        )
        .order("_hybrid_score DESC")
        .project("id")
        .fetchall(),
        [(3,), (1,), (2,)],
    )

    quoted = _literal(dataset_path)
    _check(
        "DOUBLE[] query vector coercion",
        connection.execute(
            f"""
            SELECT id FROM lance_vector_search(
              {quoted}, 'vec', [0.0, 0.0, 0.0, 0.0]::DOUBLE[], k = 1, use_index = false
            ) ORDER BY _distance
            """
        ).fetchall(),
        [(1,)],
    )

    scalar_indexes = [
        ("id_btree", "id", "BTREE"),
        ("label_bitmap", "label", "BITMAP"),
        ("id_zonemap", "id", "ZONEMAP"),
        ("id_bloom", "id", "BLOOMFILTER"),
        ("text_inverted", "text", "INVERTED"),
        ("text_ngram", "text", "NGRAM"),
        ("tags_label_list", "tags", "LABELLIST"),
        ("metadata_rank", "metadata.rank", "BTREE"),
    ]
    for name, column, index_type in scalar_indexes:
        dataset.create_index(name, column, index_type=index_type)
    dataset.create_index(
        "vec_ivf_flat",
        "vec",
        index_type="IVF_FLAT",
        num_partitions=1,
        metric_type="l2",
    )
    names = {row[0] for row in dataset.show_indexes().fetchall()}
    _check("all scalar index families plus IVF_FLAT", names, {item[0] for item in scalar_indexes} | {"vec_ivf_flat"})

    explain = "\n".join(
        str(value)
        for row in connection.execute(
            f"""
            EXPLAIN (FORMAT JSON)
            SELECT id FROM lance_vector_search(
              {quoted}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4],
              k = 3, nprobs = 1, refine_factor = 2,
              use_index = true, explain_verbose = true
            )
            """
        ).fetchall()
        for value in row
    )
    if "ANNSubIndex: name=vec_ivf_flat" not in explain:
        raise AssertionError("indexed vector search did not select vec_ivf_flat")
    print("PASS indexed vector search plan")
    _check(
        "indexed full-text search",
        dataset.fts("text", "puppy", k=10).order("id").project("id").fetchall(),
        [(1,), (3,)],
    )

    dataset.drop_index("id_zonemap")
    if "id_zonemap" in {row[0] for row in dataset.show_indexes().fetchall()}:
        raise AssertionError("DROP INDEX did not remove id_zonemap")
    print("PASS DROP INDEX")

    # Every vector index implementation accepted by the vendored FFI is built
    # against its own dataset so SHOW/DROP and search can be asserted exactly.
    vector_index_options: dict[str, dict[str, Any]] = {
        "IVF_FLAT": {},
        "IVF_PQ": {"num_sub_vectors": 2, "num_bits": 4, "max_iterations": 2},
        "IVF_SQ": {"num_bits": 8, "sample_rate": 32},
        "IVF_RQ": {"num_bits": 4},
        "IVF_HNSW_FLAT": {"hnsw_m": 8, "hnsw_ef_construction": 20},
        "IVF_HNSW_PQ": {
            "num_sub_vectors": 2,
            "num_bits": 4,
            "max_iterations": 2,
            "hnsw_m": 8,
            "hnsw_ef_construction": 20,
        },
        "IVF_HNSW_SQ": {
            "num_bits": 8,
            "sample_rate": 32,
            "hnsw_m": 8,
            "hnsw_ef_construction": 20,
        },
    }
    vector_source = """
        SELECT i::BIGINT AS id, [
          ((i * 3) % 97)::FLOAT / 97,
          ((i * 5) % 89)::FLOAT / 89,
          ((i * 7) % 83)::FLOAT / 83,
          ((i * 11) % 79)::FLOAT / 79,
          ((i * 13) % 73)::FLOAT / 73,
          ((i * 17) % 71)::FLOAT / 71,
          ((i * 19) % 67)::FLOAT / 67,
          ((i * 23) % 61)::FLOAT / 61
        ]::FLOAT[8] AS vec
        FROM range(512) AS input(i)
    """
    for index_type, options in vector_index_options.items():
        index_path = root / f"index-{index_type.lower()}.lance"
        connection.sql(vector_source).write_lance(str(index_path), mode="overwrite")
        index_dataset = LanceDataset(index_path, connection)
        index_dataset.create_index(
            "vec_idx",
            "vec",
            index_type=index_type,
            num_partitions=1,
            metric_type="l2",
            **options,
        )
        rows = (
            index_dataset.vector_search("vec", [0.0] * 8, k=3, nprobes=1, refine_factor=2, use_index=True)
            .project("id")
            .fetchall()
        )
        _check(f"{index_type} build and indexed search count", len(rows), 3)
        _check(f"{index_type} SHOW INDEXES", [row[0] for row in index_dataset.show_indexes().fetchall()], ["vec_idx"])
        index_dataset.drop_index("vec_idx")
        _check(f"{index_type} DROP INDEX", index_dataset.show_indexes().fetchall(), [])


def _verify_namespace_dml_and_ddl(connection: Any, root: Path) -> None:
    namespace = LanceNamespace(root, "lance_demo", connection=connection)
    try:
        connection.execute(
            """
            CREATE OR REPLACE TABLE lance_demo.main.schema_only
              (id BIGINT, value VARCHAR DEFAULT 'unset')
              WITH (data_storage_version = '2.0')
            """
        )
        _check(
            "schema-only CREATE TABLE",
            connection.execute("SELECT count(*) FROM lance_demo.main.schema_only").fetchone(),
            (0,),
        )
        connection.execute(
            """
            CREATE OR REPLACE TABLE lance_demo.main.ctas_versioned
              WITH (data_storage_version = '2.1')
            AS SELECT 7::BIGINT AS id, 'ctas'::VARCHAR AS value
            """
        )
        _check(
            "namespace CTAS",
            connection.execute("SELECT * FROM lance_demo.main.ctas_versioned").fetchall(),
            [(7, "ctas")],
        )

        table = namespace.create_table(
            "items",
            "SELECT * FROM (VALUES (1::BIGINT, 'one'), (2::BIGINT, 'two')) AS input(id, value)",
        )
        table.insert("SELECT 3::BIGINT AS id, 'three'::VARCHAR AS value")
        connection.execute("INSERT INTO lance_demo.main.items VALUES (4, 'four')")
        table.update({"value": "upper(value)"}, where="id = 2")
        connection.execute("UPDATE lance_demo.main.items SET value = 'all' WHERE id = 4")
        _check(
            "INSERT and UPDATE",
            table.scan().order("id").fetchall(),
            [(1, "one"), (2, "TWO"), (3, "three"), (4, "all")],
        )

        connection.execute("BEGIN TRANSACTION")
        connection.execute("UPDATE lance_demo.main.items SET value = 'rolled-back'")
        connection.execute("ROLLBACK")
        _check(
            "DML rollback",
            table.scan().filter("value = 'rolled-back'").aggregate("count(*)").fetchone(),
            (0,),
        )

        table.delete(where="id = 1")
        _check("Python DELETE", table.scan().order("id").project("id").fetchall(), [(2,), (3,), (4,)])

        table.merge(
            "SELECT * FROM (VALUES (2::BIGINT, 'two-again'), (5::BIGINT, 'five')) AS input(id, value)",
            "target.id = source.id",
        ).when_matched_update({"value": "source.value"}).when_not_matched_insert(
            {"id": "source.id", "value": "source.value"}
        ).execute()
        _check(
            "LanceMergeBuilder update/insert",
            table.scan().order("id").fetchall(),
            [(2, "two-again"), (3, "three"), (4, "all"), (5, "five")],
        )
        table.merge(
            "SELECT 4::BIGINT AS id",
            "target.id = source.id",
        ).when_matched_delete().execute()
        _check(
            "LanceMergeBuilder delete",
            table.scan().order("id").fetchall(),
            [(2, "two-again"), (3, "three"), (5, "five")],
        )

        connection.execute(
            """
            CREATE OR REPLACE TABLE lance_demo.main.merge_actions AS
            SELECT * FROM (VALUES
              (1::BIGINT, 'one'::VARCHAR),
              (2::BIGINT, 'two'::VARCHAR),
              (3::BIGINT, 'three'::VARCHAR)
            ) AS input(id, value)
            """
        )
        changed = connection.execute(
            """
            MERGE INTO lance_demo.main.merge_actions AS target
            USING (SELECT 1::BIGINT AS id, 'ONE'::VARCHAR AS value) AS source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET value = source.value
            WHEN NOT MATCHED BY SOURCE AND target.id = 2 THEN UPDATE SET value = 'orphan'
            WHEN NOT MATCHED BY SOURCE THEN DO NOTHING
            RETURNING merge_action, id, value
            """
        ).fetchall()
        _check(
            "MERGE matched/by-source update and by-source do-nothing",
            sorted(changed),
            [("UPDATE", 1, "ONE"), ("UPDATE", 2, "orphan")],
        )
        _check(
            "MERGE matched DO NOTHING",
            connection.execute(
                """
                MERGE INTO lance_demo.main.merge_actions AS target
                USING (SELECT 1::BIGINT AS id) AS source ON target.id = source.id
                WHEN MATCHED THEN DO NOTHING
                """
            ).fetchone(),
            (0,),
        )
        _check(
            "MERGE not-matched DO NOTHING",
            connection.execute(
                """
                MERGE INTO lance_demo.main.merge_actions AS target
                USING (SELECT 99::BIGINT AS id) AS source ON target.id = source.id
                WHEN NOT MATCHED THEN DO NOTHING
                """
            ).fetchone(),
            (0,),
        )
        _expect_error(
            "MERGE matched ERROR",
            lambda: connection.execute(
                """
                MERGE INTO lance_demo.main.merge_actions AS target
                USING (SELECT 1::BIGINT AS id) AS source ON target.id = source.id
                WHEN MATCHED THEN ERROR 'intentional example error'
                """
            ),
            "intentional example error",
        )
        _check(
            "MERGE matched DELETE RETURNING",
            connection.execute(
                """
                MERGE INTO lance_demo.main.merge_actions AS target
                USING (SELECT 3::BIGINT AS id) AS source ON target.id = source.id
                WHEN MATCHED THEN DELETE
                RETURNING merge_action, id, value
                """
            ).fetchall(),
            [("DELETE", 3, "three")],
        )

        connection.execute(
            """
            CREATE OR REPLACE TABLE lance_demo.main.merge_by_source_delete AS
            SELECT * FROM (VALUES (1::BIGINT), (2::BIGINT), (3::BIGINT)) AS input(id)
            """
        )
        _check(
            "MERGE not-matched-by-source DELETE",
            connection.execute(
                """
                MERGE INTO lance_demo.main.merge_by_source_delete AS target
                USING (SELECT 2::BIGINT AS id) AS source ON target.id = source.id
                WHEN MATCHED THEN DO NOTHING
                WHEN NOT MATCHED BY SOURCE THEN DELETE
                """
            ).fetchone(),
            (2,),
        )

        connection.execute("BEGIN TRANSACTION")
        connection.execute(
            """
            MERGE INTO lance_demo.main.items AS target
            USING (SELECT 100::BIGINT AS id, 'rollback'::VARCHAR AS value) AS source
            ON target.id = source.id
            WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)
            """
        )
        connection.execute("ROLLBACK")
        _check(
            "MERGE transaction rollback",
            table.scan().filter("id = 100").aggregate("count(*)").fetchone(),
            (0,),
        )

        # Schema evolution, defaults and comments use the same table object.
        table.add_column("score", "INTEGER", default="42")
        connection.execute("COMMENT ON TABLE lance_demo.main.items IS 'Lance example table'")
        connection.execute("COMMENT ON COLUMN lance_demo.main.items.score IS 'example score'")
        _check(
            "table comment",
            connection.execute(
                """
                SELECT comment FROM duckdb_tables()
                WHERE database_name = 'lance_demo' AND table_name = 'items'
                """
            ).fetchone(),
            ("Lance example table",),
        )
        _check(
            "column comment",
            connection.execute(
                """
                SELECT comment FROM duckdb_columns()
                WHERE database_name = 'lance_demo' AND table_name = 'items' AND column_name = 'score'
                """
            ).fetchone(),
            ("example score",),
        )
        connection.execute("ALTER TABLE lance_demo.main.items ALTER COLUMN score SET NOT NULL")
        connection.execute("ALTER TABLE lance_demo.main.items ALTER COLUMN score DROP NOT NULL")
        table.rename_column("score", "score_old")
        connection.execute("ALTER TABLE lance_demo.main.items ALTER COLUMN score_old TYPE BIGINT")
        table.drop_column("score_old")
        _check(
            "ALTER ADD/NOT NULL/RENAME/TYPE/DROP",
            table.scan().columns,
            ["id", "value"],
        )

        truncate_table = namespace.table("schema_only")
        truncate_table.insert(
            "SELECT * FROM (VALUES (1::BIGINT, 'one'::VARCHAR), (2::BIGINT, 'two'::VARCHAR)) AS input(id, value)"
        )
        truncate_table.truncate()
        _check("TRUNCATE", truncate_table.scan().aggregate("count(*)").fetchone(), (0,))
        namespace.drop_table("schema_only")
        namespace.drop_table("does_not_exist", if_exists=True)
        if ("schema_only",) in connection.execute("SHOW TABLES FROM lance_demo.main").fetchall():
            raise AssertionError("DROP TABLE did not remove schema_only")
        print("PASS DROP TABLE and dynamic namespace discovery")
    finally:
        namespace.detach()

    read_only_namespace = LanceNamespace(root, "lance_read_only", read_only=True, connection=connection)
    try:
        _check(
            "read-only namespace scan",
            read_only_namespace.table("ctas_versioned").scan().fetchall(),
            [(7, "ctas")],
        )
        _expect_error(
            "read-only namespace rejects mutation",
            lambda: read_only_namespace.table("ctas_versioned").insert(
                "SELECT 8::BIGINT AS id, 'forbidden'::VARCHAR AS value"
            ),
            "read-only mode",
        )
    finally:
        read_only_namespace.detach()


def _verify_maintenance(connection: Any, dataset_path: Path) -> None:
    dataset = LanceDataset(dataset_path, connection)
    quoted = _literal(dataset_path)

    # Add unindexed data, then exercise all three index optimize modes.
    connection.sql(
        """
        SELECT 7::BIGINT AS id,
               'maintenance row'::VARCHAR AS text,
               7::INTEGER AS label,
               [8.0, 8.0, 8.0, 8.0]::FLOAT[4] AS vec,
               ['maintenance']::VARCHAR[] AS tags,
               {'rank': 7::INTEGER, 'active': true} AS metadata
        """
    ).write_lance(str(dataset_path), mode="append")
    for mode, extra in (
        ("append", ""),
        ("merge", ", num_indices_to_merge = 1"),
        ("retrain", ""),
    ):
        result = connection.execute(
            f"""
            ALTER INDEX vec_ivf_flat ON {quoted}
            OPTIMIZE WITH (mode = '{mode}'{extra})
            """
        ).fetchone()
        _check(f"ALTER INDEX OPTIMIZE {mode}", result[0], "optimize_index")

    compact = dataset.optimize(
        target_rows_per_fragment=1024,
        max_rows_per_group=128,
        max_bytes_per_file=0,
        materialize_deletions=True,
        materialize_deletions_threshold=0.1,
        num_threads=1,
        batch_size=256,
        defer_index_remap=False,
    )
    _check("OPTIMIZE", compact[0][0], "compact")

    connection.execute(
        f"""
        ALTER TABLE {quoted}
        SET AUTO_CLEANUP WITH (interval = 1, older_than = '1s', retain_versions = 2)
        """
    )
    maintenance = dict(connection.execute(f"SHOW MAINTENANCE ON {quoted}").fetchall())
    _check(
        "SET/SHOW AUTO_CLEANUP",
        maintenance,
        {"enabled": "true", "interval": "1", "older_than": "1s", "retain_versions": "2"},
    )
    connection.execute(f"ALTER TABLE {quoted} UNSET AUTO_CLEANUP")
    _check(
        "UNSET AUTO_CLEANUP",
        connection.execute(f"SHOW MAINTENANCE ON {quoted}").fetchall(),
        [("enabled", "false")],
    )

    cleanup = dataset.vacuum(
        older_than_seconds=0,
        delete_unverified=False,
        error_if_tagged_old_versions=True,
        retain_n_versions=1,
    )
    _check("VACUUM LANCE", cleanup[0][0], "cleanup")


def run_local(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    connection = vane.connect()
    try:
        connection.execute("PRAGMA threads = 4")
        _verify_extension(connection)
        dataset_path = _verify_write_scan_and_snapshot(connection, root)
        _verify_bind_time_snapshot(root)
        _verify_search_and_indexes(connection, root, dataset_path)
        _verify_namespace_dml_and_ddl(connection, root)
        _verify_maintenance(connection, dataset_path)
    finally:
        connection.close()
    print(f"PASS local complete example root={root}")


def _s3_settings() -> dict[str, str]:
    names = {
        "endpoint": "TEST_MINIO_ENDPOINT",
        "access_key_id": "TEST_MINIO_ACCESS_KEY",
        "secret_access_key": "TEST_MINIO_SECRET_KEY",
        "region": "TEST_MINIO_REGION",
        "bucket": "TEST_MINIO_BUCKET",
    }
    settings = {key: os.getenv(name, "").strip() for key, name in names.items()}
    settings["region"] = settings["region"] or "us-east-1"
    missing = [names[key] for key, value in settings.items() if not value and key != "region"]
    if missing:
        raise RuntimeError("missing MinIO settings: " + ", ".join(missing))
    return settings


def _create_lance_secret(connection: Any, settings: dict[str, str], scope: str) -> None:
    options = {
        "access_key_id": settings["access_key_id"],
        "secret_access_key": settings["secret_access_key"],
        "region": settings["region"],
        "endpoint": settings["endpoint"],
        "virtual_hosted_style_request": "false",
        "allow_http": "true" if settings["endpoint"].lower().startswith("http://") else "false",
    }
    session_token = os.getenv("AWS_SESSION_TOKEN", "").strip()
    if session_token:
        options["session_token"] = session_token
    connection.execute(
        """
        CREATE OR REPLACE TEMPORARY SECRET vane_lance_example
        (TYPE LANCE, PROVIDER config, SCOPE ?, STORAGE_OPTIONS ?)
        """,
        [scope, options],
    )


def run_minio(prefix: str) -> None:
    settings = _s3_settings()
    os.environ["AWS_ENDPOINT_URL"] = settings["endpoint"]
    os.environ["AWS_ACCESS_KEY_ID"] = settings["access_key_id"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = settings["secret_access_key"]
    os.environ["AWS_REGION"] = settings["region"]
    os.environ["AWS_DEFAULT_REGION"] = settings["region"]
    prefix = prefix.strip("/") + "/"
    root_uri = f"s3://{settings['bucket']}/{prefix.rstrip('/')}"
    dataset_uri = f"{root_uri}/items.lance"

    connection = vane.connect()
    other_connection = vane.connect()
    namespace: LanceNamespace | None = None
    try:
        _verify_extension(connection)
        _create_lance_secret(connection, settings, root_uri)
        secret_rows = connection.execute(
            """
            SELECT name, type, provider, scope
            FROM duckdb_secrets() WHERE name = 'vane_lance_example'
            """
        ).fetchall()
        _check(
            "temporary scoped TYPE LANCE config secret",
            secret_rows,
            [("vane_lance_example", "lance", "config", [root_uri])],
        )
        _check(
            "secret is connection-local",
            other_connection.execute("SELECT name FROM duckdb_secrets() WHERE name = 'vane_lance_example'").fetchall(),
            [],
        )

        implicit_options = {
            "region": settings["region"],
            "endpoint": settings["endpoint"],
            "virtual_hosted_style_request": "false",
            "allow_http": "true" if settings["endpoint"].lower().startswith("http://") else "false",
        }
        for provider in ("credential_chain", "env"):
            provider_scope = f"{root_uri}/{provider}"
            connection.execute(
                f"""
                CREATE OR REPLACE TEMPORARY SECRET vane_lance_{provider}
                (TYPE LANCE, PROVIDER {provider}, SCOPE ?, STORAGE_OPTIONS ?)
                """,
                [provider_scope, implicit_options],
            )
            provider_dataset = LanceDataset(f"{provider_scope}/items.lance", connection)
            provider_dataset.write(connection.sql("SELECT 1::BIGINT AS id"), mode="overwrite")
            _check(
                f"MinIO PROVIDER {provider}",
                provider_dataset.scan().fetchall(),
                [(1,)],
            )

        dataset = LanceDataset(dataset_uri, connection)
        dataset.write(
            connection.sql(_search_source_sql()),
            mode="overwrite",
            max_rows_per_file=2,
            max_rows_per_group=1,
        )
        _check("MinIO read/write", dataset.scan().aggregate("count(*)").fetchone(), (5,))

        namespace = LanceNamespace(root_uri, "minio_lance", connection=connection)
        _check(
            "MinIO directory namespace",
            namespace.table("items").scan().order("id").project("id").fetchall(),
            [(1,), (2,), (3,), (4,), (5,)],
        )
        dataset.create_index("vec_idx", "vec", index_type="IVF_FLAT", num_partitions=1, metric_type="l2")
        dataset.create_index("text_idx", "text", index_type="INVERTED")
        _check(
            "MinIO indexed hybrid search",
            dataset.hybrid_search(
                "vec",
                [0.0, 0.0, 0.0, 0.0],
                "text",
                "puppy",
                k=3,
                nprobes=1,
                refine_factor=2,
                use_index=True,
            )
            .order("_hybrid_score DESC")
            .project("id")
            .fetchall(),
            [(1,), (3,), (2,)],
        )
        _check("MinIO OPTIMIZE", dataset.optimize()[0][0], "compact")
        _check(
            "MinIO VACUUM",
            dataset.vacuum(
                older_than_seconds=0,
                delete_unverified=False,
                error_if_tagged_old_versions=True,
            )[0][0],
            "cleanup",
        )
    finally:
        if namespace is not None:
            namespace.detach()
        other_connection.close()
        connection.close()
    print(f"PASS MinIO complete example uri={root_uri}")


def run_rest(endpoint: str, namespace_id: str, table_name: str) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
        raise ValueError("REST example table must be a bare SQL identifier")
    connection = vane.connect()
    _verify_extension(connection)
    connection.execute(
        f"""
        ATTACH {_literal(namespace_id)} AS rest_lance (
          TYPE LANCE,
          ENDPOINT {_literal(endpoint)},
          DELIMITER '$',
          HEADER 'x-vane-example=executed',
          BEARER_TOKEN 'example-bearer-token',
          API_KEY 'example-api-key'
        )
        """
    )
    namespace = LanceNamespace(
        namespace_id,
        "rest_lance",
        endpoint=endpoint,
        connection=connection,
        attach=False,
    )
    table = namespace.table(table_name)
    qualified = f'rest_lance.main."{table_name.replace(chr(34), chr(34) * 2)}"'
    maintenance_target = f"rest_lance.main.{table_name}"
    read_only_namespace: LanceNamespace | None = None
    try:
        connection.execute(f"DROP TABLE IF EXISTS {qualified}")
        connection.execute(
            f"""
            CREATE TABLE {qualified} AS
            SELECT * FROM (VALUES
              (1::BIGINT, 'Alice'::VARCHAR, 'puppy plays in the park'::VARCHAR,
               1::INTEGER, [0.0, 0.0, 0.0, 0.0]::FLOAT[4]),
              (2::BIGINT, 'Bob'::VARCHAR, 'kitten sleeps on the couch'::VARCHAR,
               2::INTEGER, [2.0, 0.0, 0.0, 0.0]::FLOAT[4]),
              (3::BIGINT, 'Charlie'::VARCHAR, 'puppy eats food'::VARCHAR,
               3::INTEGER, [0.0, 3.0, 0.0, 0.0]::FLOAT[4]),
              (4::BIGINT, 'Dora'::VARCHAR, 'politics news update'::VARCHAR,
               4::INTEGER, [0.0, 0.0, 4.0, 0.0]::FLOAT[4]),
              (5::BIGINT, 'Eve'::VARCHAR, 'sports news today'::VARCHAR,
               5::INTEGER, [0.0, 0.0, 0.0, 5.0]::FLOAT[4])
            ) AS input(id, name, text, label, vec)
            """
        )
        connection.execute(
            f"""
            INSERT INTO {qualified} VALUES
              (6, 'Frank', 'archived note', 6, [9.0, 9.0, 9.0, 9.0]::FLOAT[4])
            """
        )
        _check(
            "REST list tables",
            (table_name,) in connection.execute("SHOW TABLES FROM rest_lance.main").fetchall(),
            True,
        )
        connection.execute(
            f"""
            ATTACH {_literal(namespace_id)} AS rest_lance_read_only (
              TYPE LANCE,
              READ_ONLY true,
              ENDPOINT {_literal(endpoint)},
              DELIMITER '$',
              HEADER 'x-vane-example=executed',
              BEARER_TOKEN 'example-bearer-token',
              API_KEY 'example-api-key'
            )
            """
        )
        read_only_namespace = LanceNamespace(
            namespace_id,
            "rest_lance_read_only",
            endpoint=endpoint,
            connection=connection,
            attach=False,
        )
        _check(
            "REST read-only namespace scan",
            read_only_namespace.table(table_name).scan().aggregate("count(*)").fetchone(),
            (6,),
        )
        _expect_error(
            "REST read-only namespace rejects mutation",
            lambda: read_only_namespace.table(table_name).insert(
                "SELECT 7::BIGINT AS id, 'Grace'::VARCHAR AS name, "
                "'forbidden'::VARCHAR AS text, 7::INTEGER AS label, "
                "[7.0, 7.0, 7.0, 7.0]::FLOAT[4] AS vec"
            ),
            "read-only mode",
        )
        read_only_namespace.detach()
        read_only_namespace = None
        _check(
            "REST query_table projection/filter/limit/offset",
            connection.execute(f"SELECT name FROM {qualified} WHERE id >= 2 ORDER BY id LIMIT 2 OFFSET 1").fetchall(),
            [("Charlie",), ("Dora",)],
        )
        explain = "\n".join(
            str(value)
            for row in connection.execute(
                f"EXPLAIN (FORMAT JSON) SELECT name FROM {qualified} WHERE id >= 2 LIMIT 2 OFFSET 1"
            ).fetchall()
            for value in row
        )
        for marker in (
            '"Lance Scan Backend": "namespace_query_table"',
            '"Lance Limit Offset Pushdown": "true"',
        ):
            if marker not in explain:
                raise AssertionError(f"REST EXPLAIN did not contain {marker}")
        print("PASS REST query_table pushdown diagnostics")

        _check(
            "REST vector search explicit prefilter",
            table.vector_search(
                "vec",
                [0.0, 0.0, 0.0, 0.0],
                k=2,
                prefilter=True,
                use_index=False,
                filter="label >= 3",
            )
            .order("_distance")
            .project("id")
            .fetchall(),
            [(3,), (4,)],
        )
        _check(
            "REST full-text search explicit prefilter",
            table.fts("text", "puppy", k=10, prefilter=True, filter="label >= 2").order("id").project("id").fetchall(),
            [(3,)],
        )
        _check(
            "REST hybrid search",
            table.hybrid_search(
                "vec",
                [0.0, 0.0, 0.0, 0.0],
                "text",
                "puppy",
                k=3,
                use_index=False,
            )
            .order("_hybrid_score DESC")
            .project("id")
            .fetchall(),
            [(3,), (1,), (2,)],
        )

        table.create_index("rest_vec_idx", "vec", index_type="IVF_FLAT", num_partitions=1, metric_type="l2")
        table.create_index("rest_text_idx", "text", index_type="INVERTED")
        _check(
            "REST index DDL",
            [row[0] for row in table.show_indexes().fetchall()],
            ["rest_text_idx", "rest_vec_idx"],
        )
        _check(
            "REST indexed vector search count",
            table.vector_search(
                "vec",
                [0.0, 0.0, 0.0, 0.0],
                k=3,
                nprobes=1,
                refine_factor=2,
                use_index=True,
            )
            .aggregate("count(*)")
            .fetchone(),
            (3,),
        )

        connection.execute(f"UPDATE {qualified} SET name = 'BOB' WHERE id = 2")
        connection.execute(f"DELETE FROM {qualified} WHERE id = 1")
        _check(
            "REST DML",
            connection.execute(f"SELECT id, name FROM {qualified} ORDER BY id").fetchall(),
            [
                (2, "BOB"),
                (3, "Charlie"),
                (4, "Dora"),
                (5, "Eve"),
                (6, "Frank"),
            ],
        )

        _check("REST OPTIMIZE", table.optimize()[0][0], "compact")
        connection.execute(
            f"""
            ALTER TABLE {maintenance_target}
            SET AUTO_CLEANUP WITH (interval = 1, older_than = '1s', retain_versions = 2)
            """
        )
        _check(
            "REST SHOW MAINTENANCE",
            dict(connection.execute(f"SHOW MAINTENANCE ON {maintenance_target}").fetchall()),
            {"enabled": "true", "interval": "1", "older_than": "1s", "retain_versions": "2"},
        )
        connection.execute(f"ALTER TABLE {maintenance_target} UNSET AUTO_CLEANUP")
        _check(
            "REST VACUUM",
            table.vacuum(
                older_than_seconds=0,
                delete_unverified=False,
                error_if_tagged_old_versions=True,
            )[0][0],
            "cleanup",
        )
        _expect_error(
            "REST ALTER TABLE is intentionally unsupported",
            lambda: connection.execute(f"ALTER TABLE {qualified} ADD COLUMN active BOOLEAN DEFAULT true"),
            "ALTER TABLE operation not supported for Lance tables",
        )
        connection.execute(f"DROP TABLE {qualified}")
        _check(
            "REST DROP TABLE",
            (table_name,) in connection.execute("SHOW TABLES FROM rest_lance.main").fetchall(),
            False,
        )
    finally:
        if read_only_namespace is not None:
            read_only_namespace.detach()
        namespace.detach()
        connection.close()
    print(f"PASS REST complete example endpoint={endpoint} namespace={namespace_id}")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)

    local = subparsers.add_parser("local", help="run all local SQL/Python examples")
    local.add_argument(
        "--root",
        type=Path,
        default=None,
        help="empty directory for example datasets; a temporary directory is used by default",
    )

    minio = subparsers.add_parser("minio", help="run S3-compatible examples using TEST_MINIO_* env")
    minio.add_argument(
        "--prefix",
        default=f"vane-lance-example/{uuid.uuid4()}",
        help="bucket prefix owned by this example run",
    )

    rest = subparsers.add_parser("rest", help="run REST namespace examples")
    rest.add_argument("--endpoint", required=True)
    rest.add_argument("--namespace-id", required=True)
    rest.add_argument("--table", default="vane_complete_example")
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.mode == "local":
        root = args.root or Path(tempfile.mkdtemp(prefix="vane-lance-complete-"))
        run_local(root.resolve())
    elif args.mode == "minio":
        run_minio(args.prefix)
    else:
        run_rest(args.endpoint, args.namespace_id, args.table)


if __name__ == "__main__":
    main()
