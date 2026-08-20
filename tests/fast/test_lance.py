# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import gc
import os
import subprocess
import sys
import textwrap
import threading
from pathlib import Path

import pytest

import vane
import vane.lance._coordinator as coordinator_module
from vane import runners
from vane.lance import LanceDataset, LanceNamespace, _option_sql
from vane.lance._coordinator import _SnapshotLease
from vane.runners.ray import set_runner_ray


def _sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _write_dataset(connection, path: Path, *, empty: bool = False) -> None:
    predicate = " WHERE false" if empty else ""
    connection.execute(
        "COPY (SELECT i::BIGINT AS id, ('value-' || i::VARCHAR) AS value "
        f"FROM range(12) AS source(i){predicate}) TO {_sql_literal(path)} "
        "(FORMAT LANCE, MODE 'create', MAX_ROWS_PER_FILE 3)"
    )


def _write_partitioned_parquet(
    connection,
    path: Path,
    *,
    start: int,
    count: int,
    file_count: int,
    value_expression: str = "('value-' || i::VARCHAR)::VARCHAR",
) -> None:
    path.mkdir()
    end = start + count
    for file_id in range(file_count):
        connection.execute(
            "COPY (SELECT i::BIGINT AS id, "
            f"{value_expression} AS value FROM range({start}, {end}) AS source(i) "
            f"WHERE i % {file_count} = {file_id}) TO "
            f"{_sql_literal(path / f'part-{file_id}.parquet')} (FORMAT PARQUET)"
        )


def _lance_transaction_count(path: Path) -> int:
    return sum(entry.is_file() for entry in (path / "_transactions").iterdir())


def _lance_data_files(path: Path) -> set[Path]:
    data = path / "data"
    if not data.exists():
        return set()
    return {entry.relative_to(path) for entry in data.rglob("*") if entry.is_file()}


def _distributed_task_prefixes(files: set[Path]) -> set[str]:
    prefixes: set[str] = set()
    for path in files:
        parts = path.name.split("_", 3)
        if len(parts) == 4 and parts[0] == "vane":
            prefixes.add("_".join(parts[:3]))
    return prefixes


def _assert_staging_empty(path: Path) -> None:
    staging = path / "_vane_staging"
    assert not staging.exists() or not any(staging.rglob("*"))


def _run_distributed(runner, relation) -> list[dict[str, object]]:
    return [row for table in runner.run_iter_tables(relation) for row in table.to_pylist()]


@pytest.fixture
def ray_runner(ray_local):
    del ray_local
    vane.teardown_runner()
    set_runner_ray(noop_if_initialized=True)
    try:
        yield runners.get_or_create_runner()
    finally:
        vane.teardown_runner()


@pytest.fixture
def ray_write_runner(ray_local, monkeypatch):
    del ray_local
    monkeypatch.setenv("VANE_DISTRIBUTED_NODE_COUNT", "1")
    monkeypatch.setenv("VANE_DISTRIBUTED_WORKER_SLOTS", "4")
    monkeypatch.setenv("VANE_RAY_SCAN_TASK_MIN_PARTITION_NUM", "4")
    monkeypatch.setenv("VANE_RAY_SCAN_TASK_SIZE_GROUPING", "0")
    monkeypatch.setenv("VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION", "1")
    vane.teardown_runner()
    set_runner_ray(noop_if_initialized=True)
    try:
        yield runners.get_or_create_runner()
    finally:
        vane.teardown_runner()


def test_lance_python_api_writes_and_reads_local_dataset(tmp_path: Path) -> None:
    connection = vane.connect()
    path = tmp_path / "python-api.lance"

    try:
        relation = connection.sql("SELECT i::BIGINT AS id, i * 10 AS value FROM range(5) AS source(i)")
        LanceDataset(path, connection).write(relation)

        assert vane.read_lance(str(path), connection=connection).order("id").fetchall() == [
            (0, 0),
            (1, 10),
            (2, 20),
            (3, 30),
            (4, 40),
        ]
    finally:
        connection.close()


def test_lance_dataset_write_rejects_unknown_options(tmp_path: Path) -> None:
    connection = vane.connect()

    try:
        dataset = LanceDataset(tmp_path / "unknown-option.lance", connection)
        with pytest.raises(ValueError, match="unsupported_option"):
            dataset.write(connection.sql("SELECT 1::BIGINT AS id"), unsupported_option=True)
        assert not Path(dataset.uri).exists()
    finally:
        connection.close()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_lance_runtime_fails_closed_in_a_forked_child(tmp_path: Path) -> None:
    connection = vane.connect()
    path = tmp_path / "fork-runtime.lance"

    try:
        _write_dataset(connection, path)
    finally:
        connection.close()

    script = textwrap.dedent(
        r"""
        import os
        import signal
        import sys
        import time

        import vane

        path = sys.argv[1]
        connection = vane.connect()
        assert connection.read_lance(path).aggregate("count(*)").fetchone() == (12,)

        read_fd, write_fd = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(read_fd)
            try:
                child_connection = vane.connect()
                child_connection.read_lance(path).fetchall()
            except BaseException as exc:
                message = f"{type(exc).__name__}: {exc}".encode("utf-8", errors="replace")
                os.write(write_fd, message)
                os.close(write_fd)
                os._exit(0)
            os.write(write_fd, b"child unexpectedly used the inherited Lance runtime")
            os.close(write_fd)
            os._exit(1)

        os.close(write_fd)
        deadline = time.monotonic() + 5
        while True:
            waited_pid, status = os.waitpid(pid, os.WNOHANG)
            if waited_pid == pid:
                break
            if time.monotonic() >= deadline:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
                raise RuntimeError("forked child hung while entering the inherited Lance runtime")
            time.sleep(0.01)

        message = os.read(read_fd, 65_536).decode("utf-8", errors="replace")
        os.close(read_fd)
        connection.close()
        print(message)
        if os.waitstatus_to_exitcode(status) != 0:
            raise RuntimeError(message)
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script, str(path)],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "initialized in process" in result.stdout
    assert "before fork" in result.stdout
    assert "spawn/exec" in result.stdout


def test_lance_writes_reject_relations_from_another_connection(tmp_path: Path) -> None:
    target = vane.connect()
    source = vane.connect()
    namespace = LanceNamespace(tmp_path / "cross-connection", "target_ns", connection=target)

    try:
        source.execute("CREATE TEMP TABLE source_values AS SELECT 7::BIGINT AS id")
        relation = source.table("source_values")

        with pytest.raises(ValueError, match="different connection"):
            LanceDataset(tmp_path / "direct.lance", target).write(relation)
        with pytest.raises(ValueError, match="different connection"):
            namespace.create_table("created", relation)

        table = namespace.create_table("items", "SELECT 1::BIGINT AS id")
        with pytest.raises(ValueError, match="different connection"):
            table.insert(relation)
        with pytest.raises(ValueError, match="different connection"):
            table.write(relation, mode="overwrite")
        with pytest.raises(ValueError, match="different connection"):
            table.merge(relation, "target.id = source.id").when_not_matched_insert({"id": "source.id"}).execute()

        assert table.scan().fetchall() == [(1,)]
    finally:
        namespace.detach()
        source.close()
        target.close()


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_lance_options_reject_non_finite_floats(value: float) -> None:
    with pytest.raises(ValueError, match="must be finite"):
        _option_sql({"threshold": value})


def test_lance_dataset_resolves_relative_path_once(tmp_path: Path, monkeypatch) -> None:
    original_directory = tmp_path / "original"
    other_directory = tmp_path / "other"
    original_directory.mkdir()
    other_directory.mkdir()
    seen: list[str] = []

    class FakeConnection:
        def read_lance(self, uri: str) -> object:
            seen.append(uri)
            return object()

    monkeypatch.chdir(original_directory)
    dataset = LanceDataset("relative.lance", FakeConnection())
    monkeypatch.chdir(other_directory)

    dataset.scan()

    expected = str((original_directory / "relative.lance").resolve())
    assert dataset.uri == expected
    assert dataset.coordinator.identity == expected
    assert seen == [expected]


def test_lance_relative_paths_ignore_duckdb_file_search_path(tmp_path: Path, monkeypatch) -> None:
    working_directory = tmp_path / "working"
    search_directory = tmp_path / "search"
    working_directory.mkdir()
    search_directory.mkdir()
    monkeypatch.chdir(working_directory)
    connection = vane.connect()

    try:
        connection.execute(f"SET file_search_path = {_sql_literal(search_directory)}")
        dataset = LanceDataset("relative.lance", connection)
        connection.execute("COPY (SELECT 7::BIGINT AS id) TO 'relative.lance' (FORMAT LANCE, MODE 'create')")

        assert dataset.uri == str((working_directory / "relative.lance").resolve())
        assert dataset.scan().fetchall() == [(7,)]
        assert (working_directory / "relative.lance").is_dir()
        assert not (search_directory / "relative.lance").exists()
    finally:
        connection.close()


def test_lance_snapshot_context_keeps_escaped_relation_lease(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(coordinator_module, "_configured_runner_type", lambda: "local-fast")

    class FakeRelation:
        def _attach_lance_snapshot_lease(self, lease: object) -> None:
            self.lease = lease

    class FakeConnection:
        def read_lance(self, uri: str) -> FakeRelation:
            relation = FakeRelation()
            relation._attach_lance_snapshot_lease(_SnapshotLease(uri))
            return relation

        def table_function(self, name: str, parameters: list[str]) -> FakeRelation:
            assert name == "__lance_scan"
            assert parameters == [str(tmp_path / "snapshot.lance")]
            return FakeRelation()

    dataset = LanceDataset(tmp_path / "snapshot.lance", FakeConnection())
    local = dataset.coordinator._local

    with dataset.snapshot() as relation:
        assert len(local._active_snapshots) == 2

    assert relation is not None
    assert len(local._active_snapshots) == 1
    del relation
    gc.collect()
    assert local._active_snapshots == set()


def test_lance_directory_namespace_table_api(tmp_path: Path) -> None:
    connection = vane.connect()
    namespace = LanceNamespace(tmp_path / "namespace", "lance_ns", connection=connection)

    try:
        assert connection.execute("SHOW TABLES FROM lance_ns.main").fetchall() == []
        table = namespace.create_table("items", "SELECT 1::BIGINT AS id, 'one'::VARCHAR AS label")
        table.insert("SELECT 2::BIGINT AS id, 'two'::VARCHAR AS label")
        (
            table.merge(
                "SELECT * FROM (VALUES (2::BIGINT, 'TWO'::VARCHAR), (3::BIGINT, 'three'::VARCHAR)) source(id, label)",
                "target.id = source.id",
            )
            .when_matched_update({"label": "source.label"})
            .when_not_matched_insert({"id": "source.id", "label": "source.label"})
            .execute()
        )
        assert table.scan().order("id").fetchall() == [(1, "one"), (2, "TWO"), (3, "three")]
        assert connection.execute("SHOW TABLES FROM lance_ns.main").fetchall() == [("items",)]
    finally:
        namespace.detach()
        connection.close()


def test_lance_namespace_rejects_empty_rest_endpoint() -> None:
    with pytest.raises(ValueError, match="endpoint cannot be empty"):
        LanceNamespace("catalog", "rest_ns", endpoint="  ")


def test_lance_namespace_api_quotes_catalog_and_table_names(tmp_path: Path) -> None:
    connection = vane.connect()
    namespace = LanceNamespace(tmp_path / "quoted-namespace", "team.data-x", connection=connection)

    try:
        table = namespace.create_table("items.v1-x", "SELECT 1::BIGINT AS id")
        assert table.scan().fetchall() == [(1,)]

        table.create_index("items_idx", "id", index_type="BTREE")
        assert table.show_indexes().filter("index_name = 'items_idx'").count("*").fetchone() == (1,)
        assert table.optimize()[0][0] == "compact"
        assert table.vacuum(older_than_seconds=0, delete_unverified=False)[0][0] == "cleanup"
        table.drop_index("items_idx")
    finally:
        gc.collect()
        namespace.detach()
        connection.close()


def test_lance_attached_table_overwrite_rejects_schema_change_without_catalog_damage(tmp_path: Path) -> None:
    connection = vane.connect()
    namespace = LanceNamespace(tmp_path / "overwrite-namespace", "overwrite_ns", connection=connection)

    try:
        table = namespace.create_table("items", "SELECT 1::BIGINT AS old_id")
        with pytest.raises(NotImplementedError, match="does not support schema changes"):
            table.write(connection.sql("SELECT 'new'::VARCHAR AS new_label"), mode="overwrite")

        relation = table.scan()
        assert relation.columns == ["old_id"]
        assert relation.fetchall() == [(1,)]
        del relation
        gc.collect()

        table.write(connection.sql("SELECT 2::BIGINT AS old_id"), mode="overwrite")
        assert table.scan().fetchall() == [(2,)]
    finally:
        namespace.detach()
        connection.close()


def test_lance_namespace_detach_timeout_preserves_attachment(tmp_path: Path) -> None:
    connection = vane.connect()
    namespace = LanceNamespace(tmp_path / "detach-namespace", "detach_ns", connection=connection)
    relation = None
    detached = False

    try:
        table = namespace.create_table("items", "SELECT 1::BIGINT AS id")
        relation = table.scan()
        with pytest.raises(TimeoutError, match=r"active_snapshots.*pid="):
            namespace.detach(timeout=0.01)
        assert connection.execute("SHOW TABLES FROM detach_ns.main").fetchall() == [("items",)]

        del relation
        relation = None
        gc.collect()
        namespace.detach()
        detached = True
        assert connection.execute(
            "SELECT count(*) FROM duckdb_databases() WHERE lower(database_name) = 'detach_ns'"
        ).fetchone() == (0,)
    finally:
        if relation is not None:
            del relation
            gc.collect()
        if not detached:
            namespace.detach(timeout=0)
        connection.close()


def test_lance_namespace_helpers_reject_explicit_transactions_before_side_effects(tmp_path: Path) -> None:
    connection = vane.connect()
    namespace = LanceNamespace(tmp_path / "transaction-namespace", "transaction_ns", connection=connection)

    try:
        connection.begin()
        with pytest.raises(RuntimeError, match="does not support explicit transactions"):
            namespace.create_table("items", "SELECT 1::BIGINT AS id")
        connection.rollback()
        assert connection.execute("SHOW TABLES FROM transaction_ns.main").fetchall() == []

        table = namespace.create_table("items", "SELECT 1::BIGINT AS id")
        connection.begin()
        with pytest.raises(RuntimeError, match="does not support explicit transactions"):
            table.write(connection.sql("SELECT 2::BIGINT AS id"), mode="overwrite")
        with pytest.raises(RuntimeError, match="does not support explicit transactions"):
            table.insert("SELECT 2::BIGINT AS id")
        with pytest.raises(RuntimeError, match="does not support explicit transactions"):
            namespace.drop_table("items")
        connection.rollback()

        assert table.scan().fetchall() == [(1,)]
    finally:
        namespace.detach()
        connection.close()


def test_lance_native_write_rejects_explicit_transactions_before_creating_dataset(tmp_path: Path) -> None:
    connection = vane.connect()
    path = tmp_path / "transaction-write.lance"

    try:
        connection.begin()
        with pytest.raises(vane.InvalidInputException, match="does not support explicit transactions"):
            connection.sql("SELECT 1::BIGINT AS id").write_lance(str(path))
        connection.rollback()

        assert not path.exists()
    finally:
        connection.close()


def test_lance_rest_scan_is_version_pinned_without_blocking_mutations(monkeypatch) -> None:
    monkeypatch.setattr(coordinator_module, "_configured_runner_type", lambda: "local-fast")
    executed: list[str] = []

    class FakeRelation:
        columns = ["id"]
        types = ["BIGINT"]

        def _attach_lance_snapshot_lease(self, lease: object) -> None:
            self.lease = lease

    class FakeResult:
        def fetchone(self) -> tuple[int]:
            return (1,)

    class FakeConnection:
        def _is_auto_commit(self) -> bool:
            return True

        def table(self, name: str) -> FakeRelation:
            assert name == '"rest_ns"."main"."items"'
            return FakeRelation()

        def sql(self, sql: str) -> FakeRelation:
            assert sql in {"SELECT 2::BIGINT AS id", "SELECT 3::BIGINT AS id"}
            return FakeRelation()

        def execute(self, sql: str, *args: object) -> FakeConnection | FakeResult:
            if "duckdb_tables()" in sql:
                return FakeResult()
            del args
            executed.append(sql)
            return self

    connection = FakeConnection()
    namespace = LanceNamespace(
        "catalog",
        "rest_ns",
        endpoint="https://namespace.example.test",
        connection=connection,
        attach=False,
    )
    monkeypatch.setattr(namespace, "_attachment_is_read_only", lambda: False)
    table = namespace.table("items")
    relation = table.scan()
    local = table.coordinator._local
    assert len(local._active_snapshots) == 1
    assert local._active_consistent_snapshots == set()

    mutation_finished = threading.Event()

    def mutate() -> None:
        table.insert("SELECT 2::BIGINT AS id")
        mutation_finished.set()

    thread = threading.Thread(target=mutate, daemon=True)
    thread.start()
    assert mutation_finished.wait(timeout=5)
    thread.join(timeout=5)
    assert not thread.is_alive()

    table.write("SELECT 3::BIGINT AS id", mode="overwrite")
    assert executed == [
        'INSERT INTO "rest_ns"."main"."items" SELECT 2::BIGINT AS id',
        'CREATE OR REPLACE TABLE "rest_ns"."main"."items" AS SELECT 3::BIGINT AS id',
    ]
    assert all("namespace.example.test" not in sql for sql in executed)

    del relation
    gc.collect()
    assert local._active_snapshots == set()


def test_lance_fragment_scan_uses_distributed_scan_contract(tmp_path: Path, ray_runner, monkeypatch) -> None:
    connection = vane.connect()
    monkeypatch.chdir(tmp_path)
    path = Path("fragmented.lance")

    try:
        _write_dataset(connection, path)
        rows = _run_distributed(
            ray_runner,
            vane.read_lance(str(path), connection=connection).project("id, value"),
        )
        assert sorted(tuple(row.values()) for row in rows) == [(index, f"value-{index}") for index in range(12)]
    finally:
        connection.close()


def test_lance_sql_write_rejects_zero_limits(tmp_path: Path) -> None:
    connection = vane.connect()

    try:
        with pytest.raises(Exception, match="limits must be greater than zero"):
            connection.execute(
                "COPY (SELECT 1::BIGINT AS id) TO "
                f"{_sql_literal(tmp_path / 'invalid.lance')} "
                "(FORMAT LANCE, MAX_ROWS_PER_FILE 0)"
            )
    finally:
        connection.close()


def test_lance_sql_write_rejects_empty_target() -> None:
    connection = vane.connect()

    try:
        with pytest.raises(Exception, match="Lance COPY target cannot be empty"):
            connection.execute("COPY (SELECT 1::BIGINT AS id) TO '' (FORMAT LANCE)")
    finally:
        connection.close()


def test_lance_empty_dataset_is_an_explicit_distributed_empty_scan(tmp_path: Path, ray_runner) -> None:
    connection = vane.connect()
    path = tmp_path / "empty.lance"

    try:
        _write_dataset(connection, path, empty=True)
        assert _run_distributed(ray_runner, vane.read_lance(str(path), connection=connection)) == []
    finally:
        connection.close()


def test_lance_vector_search_runs_as_one_global_distributed_task(tmp_path: Path, ray_runner) -> None:
    connection = vane.connect()
    path = tmp_path / "vectors.lance"

    try:
        connection.execute(
            "COPY (SELECT * FROM (VALUES "
            "(1::BIGINT, [0.0, 0.0]::FLOAT[2]), "
            "(2::BIGINT, [1.0, 0.0]::FLOAT[2]), "
            "(3::BIGINT, [4.0, 0.0]::FLOAT[2])) AS source(id, vec)) "
            f"TO {_sql_literal(path)} (FORMAT LANCE, MODE 'create')"
        )
        relation = LanceDataset(path, connection).vector_search("vec", [0.0, 0.0], k=2).project("id")
        assert [next(iter(row.values())) for row in _run_distributed(ray_runner, relation)] == [1, 2]
    finally:
        connection.close()


def test_lance_distributed_write_commits_create_append_and_overwrite_once(tmp_path: Path, ray_write_runner) -> None:
    del ray_write_runner
    connection = vane.connect()
    path = tmp_path / "distributed-write.lance"
    create_input = tmp_path / "create-input"
    append_input = tmp_path / "append-input"
    overwrite_input = tmp_path / "overwrite-input"

    try:
        _write_partitioned_parquet(connection, create_input, start=0, count=32, file_count=8)
        _write_partitioned_parquet(connection, append_input, start=32, count=16, file_count=8)
        _write_partitioned_parquet(connection, overwrite_input, start=100, count=8, file_count=4)

        connection.read_parquet(str(create_input / "*.parquet")).write_lance(str(path), mode="create")
        create_files = _lance_data_files(path)
        assert len(_distributed_task_prefixes(create_files)) > 1
        assert connection.read_lance(str(path)).aggregate("count(*), min(id), max(id)").fetchone() == (32, 0, 31)
        assert _lance_transaction_count(path) == 1
        _assert_staging_empty(path)

        connection.read_parquet(str(append_input / "*.parquet")).write_lance(str(path), mode="append")
        append_files = _lance_data_files(path) - create_files
        assert len(_distributed_task_prefixes(append_files)) > 1
        assert connection.read_lance(str(path)).aggregate("count(*), min(id), max(id)").fetchone() == (48, 0, 47)
        assert _lance_transaction_count(path) == 2
        _assert_staging_empty(path)

        files_before_overwrite = _lance_data_files(path)
        connection.read_parquet(str(overwrite_input / "*.parquet")).write_lance(str(path), mode="overwrite")
        overwrite_files = _lance_data_files(path) - files_before_overwrite
        assert len(_distributed_task_prefixes(overwrite_files)) > 1
        assert connection.read_lance(str(path)).aggregate("count(*), min(id), max(id)").fetchone() == (8, 100, 107)
        assert _lance_transaction_count(path) == 3
        _assert_staging_empty(path)
    finally:
        connection.close()


def test_lance_distributed_write_preserves_empty_schema(tmp_path: Path, ray_write_runner) -> None:
    del ray_write_runner
    connection = vane.connect()
    path = tmp_path / "distributed-empty.lance"
    source = tmp_path / "empty-input"

    try:
        _write_partitioned_parquet(connection, source, start=0, count=0, file_count=1)
        connection.read_parquet(str(source / "*.parquet")).write_lance(str(path), mode="create")

        relation = connection.read_lance(str(path))
        assert relation.aggregate("count(*)").fetchone() == (0,)
        assert relation.columns == ["id", "value"]
        assert [str(logical_type) for logical_type in relation.types] == ["BIGINT", "VARCHAR"]
        assert _lance_transaction_count(path) == 1
        _assert_staging_empty(path)
    finally:
        connection.close()


def test_lance_distributed_write_aborts_copied_files_after_commit_failure(tmp_path: Path, ray_write_runner) -> None:
    del ray_write_runner
    connection = vane.connect()
    path = tmp_path / "distributed-failure.lance"
    create_input = tmp_path / "failure-create-input"
    invalid_input = tmp_path / "failure-append-input"

    try:
        _write_partitioned_parquet(connection, create_input, start=0, count=16, file_count=4)
        _write_partitioned_parquet(
            connection,
            invalid_input,
            start=16,
            count=16,
            file_count=4,
            value_expression="i::BIGINT",
        )
        connection.read_parquet(str(create_input / "*.parquet")).write_lance(str(path), mode="create")
        files_before = _lance_data_files(path)

        with pytest.raises(Exception, match=r"(?i)(schema|field|type)"):
            connection.read_parquet(str(invalid_input / "*.parquet")).write_lance(str(path), mode="append")

        assert connection.read_lance(str(path)).aggregate("count(*), min(id), max(id)").fetchone() == (16, 0, 15)
        assert _lance_transaction_count(path) == 1
        assert _lance_data_files(path) == files_before
        _assert_staging_empty(path)
    finally:
        connection.close()
