#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

"""Exercise Vane's distributed Lance scan, search, and write contracts."""

from __future__ import annotations

import argparse
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import ray

import vane
from vane import runners
from vane.lance import LanceDataset


def _sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


@contextmanager
def _example_root(requested_root: Path | None) -> Iterator[Path]:
    if requested_root is None:
        with tempfile.TemporaryDirectory(prefix="vane-lance-distributed-") as temp_dir:
            yield Path(temp_dir)
        return

    root = requested_root.expanduser().resolve()
    if root.exists() and any(root.iterdir()):
        raise ValueError(f"--root must be absent or empty: {root}")
    root.mkdir(parents=True, exist_ok=True)
    yield root


def _write_parquet_parts(
    connection: Any,
    root: Path,
    *,
    start: int,
    count: int,
    file_count: int,
) -> None:
    root.mkdir()
    end = start + count
    for file_id in range(file_count):
        connection.execute(
            "COPY (SELECT i::BIGINT AS id, "
            "('token' || i::VARCHAR)::VARCHAR AS text, "
            "CASE WHEN i % 2 = 0 THEN 'even' ELSE 'odd' END::VARCHAR AS category, "
            "[i::FLOAT, 0.0::FLOAT, 0.0::FLOAT, 0.0::FLOAT]::FLOAT[4] AS vec "
            f"FROM range({start}, {end}) AS source(i) WHERE i % {file_count} = {file_id}) "
            f"TO {_sql_literal(root / f'part-{file_id}.parquet')} (FORMAT PARQUET)"
        )


def _collect(runner: Any, relation: Any) -> list[tuple[Any, ...]]:
    return [tuple(row.values()) for table in runner.run_iter_tables(relation) for row in table.to_pylist()]


def _transaction_count(dataset_path: Path) -> int:
    return sum(path.is_file() for path in (dataset_path / "_transactions").iterdir())


def run(root: Path) -> None:
    # This example owns its Ray process; do not inherit a shell-level cluster.
    os.environ.pop("RAY_ADDRESS", None)
    os.environ["VANE_DISTRIBUTED_NODE_COUNT"] = "1"
    os.environ["VANE_DISTRIBUTED_WORKER_SLOTS"] = "4"
    os.environ["VANE_RAY_SCAN_TASK_MIN_PARTITION_NUM"] = "4"
    os.environ["VANE_RAY_SCAN_TASK_SIZE_GROUPING"] = "0"
    os.environ["VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION"] = "1"

    # An installed native package is required. Avoid making the source checkout
    # shadow it when Ray imports Vane in fresh worker processes.
    os.chdir(root)
    # Force an isolated local cluster even when RAY_ADDRESS is set by the shell.
    ray.init(address="local", num_cpus=4, include_dashboard=False)
    vane.set_runner_ray(noop_if_initialized=True)
    runner = runners.get_or_create_runner()
    connection = vane.connect()

    try:
        create_input = root / "create-input"
        append_input = root / "append-input"
        overwrite_input = root / "overwrite-input"
        empty_input = root / "empty-input"
        dataset_path = root / "distributed.lance"
        empty_path = root / "distributed-empty.lance"

        _write_parquet_parts(connection, create_input, start=0, count=32, file_count=8)
        _write_parquet_parts(connection, append_input, start=32, count=16, file_count=8)
        _write_parquet_parts(connection, overwrite_input, start=100, count=8, file_count=4)
        _write_parquet_parts(connection, empty_input, start=0, count=0, file_count=1)

        connection.read_parquet(str(create_input / "*.parquet")).write_lance(
            str(dataset_path),
            mode="create",
            data_storage_version="2.2",
            max_rows_per_file=8,
            max_rows_per_group=4,
            max_bytes_per_file=1_048_576,
        )
        assert _transaction_count(dataset_path) == 1

        dataset = LanceDataset(dataset_path, connection)
        rows = _collect(runner, dataset.scan().project("id, text").filter("id % 7 = 0"))
        assert sorted(rows) == [
            (0, "token0"),
            (7, "token7"),
            (14, "token14"),
            (21, "token21"),
            (28, "token28"),
        ]

        vector_rows = _collect(
            runner,
            dataset.vector_search("vec", [0.1, 0.0, 0.0, 0.0], k=3, use_index=False).project("id, _distance"),
        )
        assert [row[0] for row in vector_rows] == [0, 1, 2], vector_rows

        fts_rows = _collect(runner, dataset.fts("text", "token17", k=3).project("id, _score"))
        assert fts_rows and fts_rows[0][0] == 17, fts_rows

        hybrid_rows = _collect(
            runner,
            dataset.hybrid_search(
                "vec",
                [17.1, 0.0, 0.0, 0.0],
                "text",
                "token17",
                k=3,
                use_index=False,
                alpha=0.5,
            ).project("id, _hybrid_score"),
        )
        assert hybrid_rows and hybrid_rows[0][0] == 17, hybrid_rows

        connection.read_parquet(str(append_input / "*.parquet")).write_lance(str(dataset_path), mode="append")
        assert _transaction_count(dataset_path) == 2
        assert dataset.scan().aggregate("count(*), min(id), max(id)").fetchone() == (48, 0, 47)

        connection.read_parquet(str(overwrite_input / "*.parquet")).write_lance(str(dataset_path), mode="overwrite")
        assert _transaction_count(dataset_path) == 3
        assert dataset.scan().aggregate("count(*), min(id), max(id)").fetchone() == (8, 100, 107)

        connection.read_parquet(str(empty_input / "*.parquet")).write_lance(str(empty_path), mode="create")
        empty = LanceDataset(empty_path, connection).scan()
        assert empty.fetchall() == []
        assert empty.columns == ["id", "text", "category", "vec"]
        assert _transaction_count(empty_path) == 1

        for path in (dataset_path, empty_path):
            staging = path / "_vane_staging"
            assert not staging.exists() or not any(staging.rglob("*"))

        print("distributed fragment scan: passed")
        print("global vector/FTS/hybrid ranking: passed")
        print("single-transaction create/append/overwrite and empty schema: passed")
        print("ALL DISTRIBUTED LANCE EXAMPLES PASSED")
    finally:
        connection.close()
        vane.teardown_runner()
        ray.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        help="keep generated datasets in this absent or empty directory",
    )
    args = parser.parse_args()
    with _example_root(args.root) as root:
        print(f"working directory: {root}")
        run(root)


if __name__ == "__main__":
    main()
