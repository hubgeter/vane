#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

"""Run a real distributed Lance round trip against S3 or MinIO."""

from __future__ import annotations

import argparse
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any

import ray
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.session import Session

import vane
from vane import runners
from vane.lance import LanceDataset, LanceNamespace


def _sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _ensure_bucket(args: argparse.Namespace) -> None:
    client = Session().create_client(
        "s3",
        endpoint_url=args.endpoint,
        aws_access_key_id=args.access_key_id,
        aws_secret_access_key=args.secret_access_key,
        region_name=args.region,
        config=Config(s3={"addressing_style": "path"}),
    )
    try:
        client.create_bucket(Bucket=args.bucket)
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code not in {"BucketAlreadyExists", "BucketAlreadyOwnedByYou"}:
            raise


def _write_parquet_parts(connection: Any, root: Path, *, start: int, count: int) -> None:
    root.mkdir()
    end = start + count
    for file_id in range(4):
        connection.execute(
            "COPY (SELECT i::BIGINT AS id, ('token' || i::VARCHAR)::VARCHAR AS text, "
            "[i::FLOAT, 0.0::FLOAT, 0.0::FLOAT, 0.0::FLOAT]::FLOAT[4] AS vec "
            f"FROM range({start}, {end}) AS source(i) WHERE i % 4 = {file_id}) "
            f"TO {_sql_literal(root / f'part-{file_id}.parquet')} (FORMAT PARQUET)"
        )


def _collect(runner: Any, relation: Any) -> list[tuple[Any, ...]]:
    return [tuple(row.values()) for table in runner.run_iter_tables(relation) for row in table.to_pylist()]


def run(args: argparse.Namespace) -> None:
    _ensure_bucket(args)
    run_id = "run_" + uuid.uuid4().hex
    table_name = "items"
    namespace_uri = f"s3://{args.bucket}/{run_id}"
    dataset_uri = f"{namespace_uri}/{table_name}.lance"
    secret_name = "lance_minio_" + uuid.uuid4().hex

    os.environ.pop("RAY_ADDRESS", None)
    os.environ["VANE_DISTRIBUTED_NODE_COUNT"] = "1"
    os.environ["VANE_DISTRIBUTED_WORKER_SLOTS"] = "4"
    os.environ["VANE_RAY_SCAN_TASK_MIN_PARTITION_NUM"] = "4"
    os.environ["VANE_RAY_SCAN_TASK_SIZE_GROUPING"] = "0"
    os.environ["VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION"] = "1"

    with tempfile.TemporaryDirectory(prefix="vane-lance-s3-") as temp_dir:
        root = Path(temp_dir)
        os.chdir(root)
        create_input = root / "create-input"
        append_input = root / "append-input"

        ray.init(address="local", num_cpus=4, include_dashboard=False)
        vane.set_runner_ray(noop_if_initialized=True)
        runner = runners.get_or_create_runner()
        connection = vane.connect()
        namespace: LanceNamespace | None = None
        table_created = False

        try:
            connection.execute(
                f"""
                CREATE SECRET {secret_name} (
                  TYPE LANCE,
                  PROVIDER config,
                  SCOPE 's3://{args.bucket}/',
                  ACCESS_KEY_ID {_sql_literal(args.access_key_id)},
                  SECRET_ACCESS_KEY {_sql_literal(args.secret_access_key)},
                  REGION {_sql_literal(args.region)},
                  ENDPOINT {_sql_literal(args.endpoint)},
                  VIRTUAL_HOSTED_STYLE_REQUEST false,
                  ALLOW_HTTP true
                )
                """
            )
            secret = connection.execute(
                "SELECT type, provider, scope FROM duckdb_secrets() WHERE name = " + _sql_literal(secret_name)
            ).fetchone()
            assert secret == ("lance", "config", [f"s3://{args.bucket}/"]), secret

            _write_parquet_parts(connection, create_input, start=0, count=24)
            _write_parquet_parts(connection, append_input, start=24, count=8)
            connection.read_parquet(str(create_input / "*.parquet")).write_lance(
                dataset_uri,
                mode="create",
                max_rows_per_file=6,
                max_rows_per_group=3,
                max_bytes_per_file=1_048_576,
            )
            table_created = True
            connection.read_parquet(str(append_input / "*.parquet")).write_lance(
                dataset_uri,
                mode="append",
            )

            dataset = LanceDataset(dataset_uri, connection)
            distributed_rows = _collect(
                runner,
                dataset.scan().filter("id % 9 = 0").project("id, text"),
            )
            assert sorted(distributed_rows) == [
                (0, "token0"),
                (9, "token9"),
                (18, "token18"),
                (27, "token27"),
            ]

            vector_rows = _collect(
                runner,
                dataset.vector_search("vec", [11.1, 0.0, 0.0, 0.0], k=3, use_index=False).project("id"),
            )
            assert [row[0] for row in vector_rows] == [11, 12, 10], vector_rows
            fts_rows = _collect(runner, dataset.fts("text", "token11", k=3).project("id"))
            assert fts_rows == [(11,)], fts_rows
            hybrid_rows = _collect(
                runner,
                dataset.hybrid_search(
                    "vec",
                    [11.1, 0.0, 0.0, 0.0],
                    "text",
                    "token11",
                    k=3,
                    use_index=False,
                ).project("id"),
            )
            assert hybrid_rows and hybrid_rows[0] == (11,), hybrid_rows

            for scheme in ("s3", "s3a", "s3n"):
                alias_uri = dataset_uri.replace("s3://", f"{scheme}://", 1)
                assert connection.read_lance(alias_uri).aggregate("count(*)").fetchone() == (32,)

            namespace = LanceNamespace(namespace_uri, "s3_lance", connection=connection)
            assert connection.execute("SHOW TABLES FROM s3_lance.main").fetchall() == [(table_name,)]
            assert namespace.table(table_name).scan().aggregate("min(id), max(id)").fetchone() == (0, 31)
            namespace.drop_table(table_name)
            table_created = False
            assert connection.execute("SHOW TABLES FROM s3_lance.main").fetchall() == []

            print(f"S3 dataset: {dataset_uri}")
            print("TYPE LANCE config secret and s3/s3a/s3n resolution: passed")
            print("distributed S3 create/append/scan/vector/FTS/hybrid: passed")
            print("S3 directory namespace discovery and DROP TABLE: passed")
            print("ALL S3 LANCE EXAMPLES PASSED")
        finally:
            if namespace is not None:
                if table_created:
                    try:
                        namespace.drop_table(table_name)
                    except Exception:
                        pass
                namespace.detach()
            vane.teardown_runner()
            try:
                connection.execute(f"DROP SECRET {secret_name}")
            finally:
                connection.close()
                ray.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default=os.getenv("LANCE_S3_ENDPOINT", "http://127.0.0.1:19000"))
    parser.add_argument("--bucket", default=os.getenv("LANCE_S3_BUCKET", "vane-lance-example"))
    parser.add_argument("--region", default=os.getenv("LANCE_S3_REGION", "us-east-1"))
    parser.add_argument("--access-key-id", default=os.getenv("LANCE_S3_ACCESS_KEY_ID", "minioadmin"))
    parser.add_argument(
        "--secret-access-key",
        default=os.getenv("LANCE_S3_SECRET_ACCESS_KEY", "minioadmin"),
    )
    run(parser.parse_args())


if __name__ == "__main__":
    main()
