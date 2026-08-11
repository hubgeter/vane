# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import gc
import os
import shutil
import threading
import time
import uuid
import warnings
from contextlib import suppress
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import vane
import vane.lance._coordinator as coordinator_module
from vane import runners as _runners
from vane.lance import LanceCommitCleanupError, LanceDataset, LanceNamespace
from vane.runners.local import set_runner_local
from vane.runners.ray.worker import _configure_duckdb_s3

SEARCH_DATASET = Path(__file__).parents[2] / "external" / "lance-duckdb" / "test" / "data" / "search_test_data.lance"


def _teardown_runner() -> None:
    if hasattr(vane, "teardown_runner"):
        vane.teardown_runner()


@pytest.fixture
def local_runner():
    _teardown_runner()
    try:
        set_runner_local(num_workers=2, max_running_tasks=2)
        runner = _runners.get_or_create_runner()
    except Exception:
        pytest.skip("duckdb local FTE runner API not available in this environment")
    if getattr(runner, "name", None) != "local":
        pytest.skip(f"Local runner not active, got runner={getattr(runner, 'name', None)!r}")
    try:
        yield runner
    finally:
        _teardown_runner()


@pytest.fixture
def ray_lance_runner(ray_local, monkeypatch):
    monkeypatch.setenv("VANE_RAY_SCAN_TASK_MIN_PARTITION_NUM", "4")
    monkeypatch.setenv("VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION", "1")
    _teardown_runner()
    _runners.set_runner_ray(noop_if_initialized=True)
    runner = _runners.get_or_create_runner()
    assert getattr(runner, "name", None) == "ray"
    try:
        yield runner
    finally:
        _teardown_runner()


def _rows_from_ray_tables(tables, expected_arity: int) -> list[tuple[object, ...]]:
    assert tables
    assert all(table.num_columns == expected_arity for table in tables), [table.schema for table in tables]
    return [tuple(row[column] for column in table.column_names) for table in tables for row in table.to_pylist()]


def _ray_rows(runner, relation) -> list[tuple[object, ...]]:
    expected_arity = len(relation.columns)
    return _rows_from_ray_tables(list(runner.run_iter_tables(relation)), expected_arity)


def _ray_scan_descriptor_counts(relation, connection, query_id: str) -> list[int]:
    logical_plan = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(relation, query_id)
    planning_connection = connection.cursor()
    physical_plan = None
    try:
        physical_plan = logical_plan.to_physical_plan(planning_connection)
        return sorted(len(descriptors) for descriptors in physical_plan.scan_task_descriptor_map().values())
    finally:
        physical_plan = None
        planning_connection.close()


def _ray_scan_cpu_slots(relation, connection, query_id: str) -> list[int]:
    logical_plan = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(relation, query_id)
    planning_connection = connection.cursor()
    physical_plan = None
    try:
        physical_plan = logical_plan.to_physical_plan(planning_connection)
        descriptors = [
            descriptor
            for task_descriptors in physical_plan.scan_task_descriptor_map().values()
            for descriptor in task_descriptors
        ]
        return [vane.ray_cxx.scan_task_cpu_slots(descriptor) for descriptor in descriptors]
    finally:
        physical_plan = None
        planning_connection.close()


def _assert_ray_global_source(relation, connection, query_id: str) -> None:
    logical_plan = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(relation, query_id)
    planning_connection = connection.cursor()
    physical_plan = None
    try:
        physical_plan = logical_plan.to_physical_plan(planning_connection)
        assert physical_plan.num_partitions() == 1
        descriptor_map = physical_plan.scan_task_descriptor_map()
        if descriptor_map:
            descriptors = [
                descriptor for task_descriptors in descriptor_map.values() for descriptor in task_descriptors
            ]
            assert len(descriptors) == 1
            assert b"lance-global-v1" in descriptors[0]
        assert "ScanTaskSource" in physical_plan.repr_ascii(False)
    finally:
        physical_plan = None
        planning_connection.close()


def _write_search_parquet(path: Path) -> None:
    pq.write_table(
        pa.table(
            {
                "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
                "label": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
                "text": pa.array(
                    [
                        "puppy plays in the park",
                        "kitten sleeps on the couch",
                        "puppy eats food",
                        "politics news update",
                        "sports news today",
                    ]
                ),
                "keywords": pa.array(["dog pet", "cat pet", "dog food", "news politics", "news sports"]),
                "vec": pa.array(
                    [
                        [0.0, 0.0, 0.0, 0.0],
                        [2.0, 0.0, 0.0, 0.0],
                        [0.0, 3.0, 0.0, 0.0],
                        [0.0, 0.0, 4.0, 0.0],
                        [0.0, 0.0, 0.0, 5.0],
                    ],
                    type=pa.list_(pa.float32(), 4),
                ),
            }
        ),
        path,
        row_group_size=2,
    )


def _minio_lance_config() -> tuple[str, str, str, str, str, str | None]:
    endpoint = os.getenv("TEST_MINIO_ENDPOINT") or os.getenv("AWS_ENDPOINT_URL") or "http://127.0.0.1:9000"
    access_key = os.getenv("TEST_MINIO_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID") or ""
    secret_key = os.getenv("TEST_MINIO_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY") or ""
    region = os.getenv("TEST_MINIO_REGION") or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
    bucket = os.getenv("TEST_MINIO_BUCKET") or "lance-test"
    session_token = os.getenv("AWS_SESSION_TOKEN")
    if not access_key or not secret_key:
        pytest.skip("MinIO credentials are not configured (TEST_MINIO_ACCESS_KEY/TEST_MINIO_SECRET_KEY)")
    return endpoint, access_key, secret_key, region, bucket, session_token


def _install_lance_minio_env(monkeypatch) -> tuple[str, str, str, str, str, str | None]:
    endpoint, access_key, secret_key, region, bucket, session_token = _minio_lance_config()
    config = {
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_REGION": region,
        "AWS_DEFAULT_REGION": region,
    }
    if session_token:
        config["AWS_SESSION_TOKEN"] = session_token
    for name, value in config.items():
        monkeypatch.setenv(name, value)
    return endpoint, access_key, secret_key, region, bucket, session_token


def _delete_minio_prefix(minio_config: tuple[str, str, str, str, str, str | None], prefix: str) -> None:
    from botocore.config import Config
    from botocore.session import Session

    endpoint, access_key, secret_key, region, bucket, session_token = minio_config
    client = Session().create_client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
        region_name=region,
        config=Config(s3={"addressing_style": "path"}),
    )
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
            if objects:
                client.delete_objects(Bucket=bucket, Delete={"Objects": objects, "Quiet": True})
    finally:
        client.close()


def _configure_lance_minio(connection, monkeypatch) -> tuple[str, str, str, str, str, str | None]:
    minio_config = _install_lance_minio_env(monkeypatch)
    endpoint, access_key, secret_key, region, bucket, session_token = minio_config
    config = {
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_REGION": region,
        "AWS_DEFAULT_REGION": region,
    }
    if session_token:
        config["AWS_SESSION_TOKEN"] = session_token
    _configure_duckdb_s3(connection, config)
    probe_prefix = f"vane-lance-preflight/{uuid.uuid4()}/"
    probe_path = f"s3://{bucket}/{probe_prefix}probe.parquet"
    try:
        connection.execute(f"COPY (SELECT 1::BIGINT AS value) TO '{probe_path}' (FORMAT PARQUET)")
        assert connection.execute(f"SELECT value FROM read_parquet('{probe_path}')").fetchone() == (1,)
    except Exception as exc:
        with suppress(Exception):
            _delete_minio_prefix(minio_config, probe_prefix)
        pytest.skip(f"MinIO/S3-compatible endpoint is not writable ({type(exc).__name__})")
    _delete_minio_prefix(minio_config, probe_prefix)
    return minio_config


def _create_lance_minio_secret(connection, minio_config: tuple[str, str, str, str, str, str | None]) -> None:
    endpoint, access_key, secret_key, region, _, session_token = minio_config
    lance_options = {
        "access_key_id": access_key,
        "secret_access_key": secret_key,
        "region": region,
        "endpoint": endpoint,
        "virtual_hosted_style_request": "false",
        "allow_http": "true" if not endpoint.lower().startswith("https://") else "false",
    }
    if session_token:
        lance_options["session_token"] = session_token
    connection.execute(
        "CREATE OR REPLACE TEMPORARY SECRET vane_lance_session "
        "(TYPE LANCE, PROVIDER config, SCOPE 's3://', STORAGE_OPTIONS ?)",
        [lance_options],
    )


@pytest.fixture
def ray_lance_minio_runner(ray_local, monkeypatch):
    minio_config = _install_lance_minio_env(monkeypatch)
    monkeypatch.setenv("VANE_RAY_SCAN_TASK_MIN_PARTITION_NUM", "4")
    monkeypatch.setenv("VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION", "1")
    _teardown_runner()
    _runners.set_runner_ray(noop_if_initialized=True)
    runner = _runners.get_or_create_runner()
    assert getattr(runner, "name", None) == "ray"
    try:
        yield runner, minio_config
    finally:
        _teardown_runner()


@pytest.fixture
def ray_lance_two_node_minio_runner(monkeypatch):
    try:
        import ray
    except ModuleNotFoundError as exc:
        if exc.name != "ray":
            raise
        pytest.skip("ray not installed")

    from ray.cluster_utils import Cluster

    from vane.runners.ray import driver as ray_driver

    minio_config = _install_lance_minio_env(monkeypatch)
    monkeypatch.setenv("VANE_RAY_SCAN_TASK_MIN_PARTITION_NUM", "8")
    monkeypatch.setenv("VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION", "1")
    monkeypatch.setenv("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")
    monkeypatch.setenv("RAY_task_events_report_interval_ms", "100")

    vane_package_parent = str(Path(vane.__file__).resolve().parent.parent)
    pythonpath_entries = [vane_package_parent]
    existing_pythonpath = os.environ.get("PYTHONPATH")
    if existing_pythonpath:
        pythonpath_entries.append(existing_pythonpath)
    runtime_env_vars = {
        "PYTHONPATH": os.pathsep.join(dict.fromkeys(pythonpath_entries)),
        "PYTHONWARNINGS": os.environ.get("PYTHONWARNINGS", ""),
        "RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO": "0",
    }

    cluster = Cluster(shutdown_at_exit=False)
    try:
        _teardown_runner()
        ray_driver.shutdown_background_event_loop()
        if ray.is_initialized():
            ray.shutdown()

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=r"Tip: In future versions of Ray")
            for node_index in range(2):
                cluster.add_node(
                    include_dashboard=False,
                    node_name=f"vane-lance-test-node-{node_index}",
                    num_cpus=4,
                    num_gpus=0,
                    memory=8 * 1024**3,
                    object_store_memory=1024**3,
                )
            ray.init(
                address=cluster.address,
                ignore_reinit_error=True,
                logging_level="info",
                log_to_driver=True,
                runtime_env={"env_vars": runtime_env_vars},
            )

        live_node_ids = {
            str(node["NodeID"])
            for node in ray.nodes()
            if node.get("Alive") and float(node.get("Resources", {}).get("CPU", 0)) > 0
        }
        assert len(live_node_ids) == 2, ray.nodes()

        _runners.set_runner_ray(noop_if_initialized=True)
        runner = _runners.get_or_create_runner()
        assert getattr(runner, "name", None) == "ray"
        yield runner, minio_config, frozenset(live_node_ids)
    finally:
        try:
            _teardown_runner()
        finally:
            try:
                ray_driver.shutdown_background_event_loop()
            finally:
                try:
                    if ray.is_initialized():
                        ray.shutdown()
                finally:
                    cluster.shutdown()


def _ray_fte_create_task_locations() -> dict[str, str]:
    import ray
    from ray._private import ray_constants
    from ray._private.grpc_utils import init_grpc_channel
    from ray.core.generated import gcs_service_pb2_grpc
    from ray.core.generated.gcs_service_pb2 import GetTaskEventsRequest

    channel = init_grpc_channel(
        ray.get_runtime_context().gcs_address,
        ray_constants.GLOBAL_GRPC_OPTIONS,
        asynchronous=False,
    )
    try:
        reply = gcs_service_pb2_grpc.TaskInfoGcsServiceStub(channel).GetTaskEvents(
            GetTaskEventsRequest(limit=10000),
            timeout=10,
        )
    finally:
        channel.close()
    if int(reply.status.code) != 0:
        raise RuntimeError(f"Ray GCS task event query failed: {reply.status.message}")
    locations: dict[str, str] = {}
    for event in reply.events_by_task:
        if not event.task_info.name.endswith(".fte_create_task"):
            continue
        node_id = event.state_updates.node_id or event.task_info.node_id
        if node_id:
            locations[event.task_id.hex()] = node_id.hex()
    return locations


def _settled_ray_fte_create_task_locations() -> dict[str, str]:
    deadline = time.monotonic() + 5
    previous: dict[str, str] | None = None
    stable_observations = 0
    while time.monotonic() < deadline:
        current = _ray_fte_create_task_locations()
        if current == previous:
            stable_observations += 1
            if stable_observations >= 3:
                return current
        else:
            previous = current
            stable_observations = 0
        time.sleep(0.1)
    return previous or {}


def _ray_fte_create_task_node_ids(baseline_task_ids: set[str], expected_count: int) -> set[str]:
    deadline = time.monotonic() + 15
    observed_node_ids: set[str] = set()
    while time.monotonic() < deadline:
        locations = _ray_fte_create_task_locations()
        observed_node_ids = {node_id for task_id, node_id in locations.items() if task_id not in baseline_task_ids}
        if len(observed_node_ids) >= expected_count:
            return observed_node_ids
        time.sleep(0.1)
    return observed_node_ids


def test_ray_lance_fragment_scan_single_writer_snapshot_and_search(ray_lance_runner, tmp_path) -> None:
    connection = vane.connect()
    dataset_path = tmp_path / "ray_public_api.lance"
    source_path = tmp_path / "ray_lance_source.parquet"
    append_path = tmp_path / "ray_lance_append.parquet"
    pq.write_table(
        pa.table(
            {
                "id": pa.array(range(32), type=pa.int64()),
                "bucket": pa.array((value % 3 for value in range(32)), type=pa.int32()),
            }
        ),
        source_path,
        row_group_size=8,
    )
    pq.write_table(
        pa.table(
            {
                "id": pa.array([32], type=pa.int64()),
                "bucket": pa.array([2], type=pa.int32()),
            }
        ),
        append_path,
    )
    dataset = LanceDataset(dataset_path, connection)
    pinned_scan = None
    pinned_stream = None
    vacuum_thread = None
    vacuum_finished = threading.Event()
    vacuum_result: list[tuple[object, ...]] = []
    vacuum_error: list[BaseException] = []
    try:
        source = connection.read_parquet(str(source_path)).repartition(4)
        dataset.write(
            source,
            mode="overwrite",
            max_rows_per_file=8,
            max_rows_per_group=4,
        )

        pinned_scan = dataset.scan().project("id, bucket")
        assert _ray_scan_descriptor_counts(pinned_scan, connection, "ray-lance-fragment-scan") == [4]

        pinned_stream = ray_lance_runner.run_iter_tables(pinned_scan)
        first_pinned_table = next(pinned_stream)
        dataset.write(connection.read_parquet(str(append_path)), mode="append")

        def run_vacuum() -> None:
            vacuum_connection = vane.connect()
            try:
                vacuum_result.extend(
                    LanceDataset(dataset_path, vacuum_connection).vacuum(
                        older_than_seconds=0,
                        delete_unverified=False,
                        error_if_tagged_old_versions=True,
                    )
                )
            except BaseException as exc:
                vacuum_error.append(exc)
            finally:
                vacuum_connection.close()
                vacuum_finished.set()

        vacuum_thread = threading.Thread(target=run_vacuum, daemon=True)
        vacuum_thread.start()
        assert not vacuum_finished.wait(timeout=0.25), "VACUUM passed an active distributed snapshot lease"

        pinned_rows = _rows_from_ray_tables([first_pinned_table, *pinned_stream], expected_arity=2)
        assert sorted(row[0] for row in pinned_rows) == list(range(32))
        pinned_stream = None
        pinned_scan = None
        gc.collect()
        assert vacuum_finished.wait(timeout=30), "VACUUM did not continue after the Ray snapshot was released"
        vacuum_thread.join(timeout=5)
        assert not vacuum_thread.is_alive()
        assert not vacuum_error
        assert vacuum_result and vacuum_result[0][0] == "cleanup"

        fresh_rows = _ray_rows(ray_lance_runner, dataset.scan().project("id, bucket"))
        assert sorted(row[0] for row in fresh_rows) == list(range(33))
        assert sum(int(row[0]) for row in fresh_rows) == 528

        search_dataset = LanceDataset(SEARCH_DATASET, connection)
        vector = search_dataset.vector_search("vec", [0.0, 0.0, 0.0, 0.0], k=3, use_index=False)
        fts = search_dataset.fts("text", "puppy", k=10)
        hybrid = search_dataset.hybrid_search(
            "vec",
            [0.0, 0.0, 0.0, 0.0],
            "text",
            "puppy",
            k=3,
            use_index=False,
        )

        _assert_ray_global_source(vector, connection, "ray-lance-vector-search")
        _assert_ray_global_source(fts, connection, "ray-lance-fts")
        _assert_ray_global_source(hybrid, connection, "ray-lance-hybrid-search")

        vector_rows = _ray_rows(ray_lance_runner, vector.order("_distance").project("id"))
        fts_rows = _ray_rows(ray_lance_runner, fts.order("id").project("id"))
        hybrid_rows = _ray_rows(ray_lance_runner, hybrid.order("_hybrid_score DESC").project("id"))
        assert [row[0] for row in vector_rows] == [1, 2, 3]
        assert [row[0] for row in fts_rows] == [1, 3]
        assert [row[0] for row in hybrid_rows] == [1, 3, 2]
    finally:
        if pinned_stream is not None:
            pinned_stream.close()
        pinned_stream = None
        pinned_scan = None
        gc.collect()
        if vacuum_thread is not None:
            vacuum_thread.join(timeout=30)
        connection.close()


def test_ray_lance_index_commit_and_indexed_search(ray_lance_runner, tmp_path) -> None:
    connection = vane.connect()
    dataset_path = tmp_path / "ray_indexed_search.lance"
    source_path = tmp_path / "ray_indexed_search.parquet"
    _write_search_parquet(source_path)
    dataset = LanceDataset(dataset_path, connection)
    scan = None
    stream = None
    try:
        dataset.write(
            connection.read_parquet(str(source_path)).repartition(3),
            mode="overwrite",
            max_rows_per_file=2,
            max_rows_per_group=1,
        )

        scan = dataset.scan().project("id")
        assert _ray_scan_descriptor_counts(scan, connection, "ray-lance-pre-index-scan") == [3]
        stream = ray_lance_runner.run_iter_tables(scan)
        first_table = next(stream)

        dataset.create_index("vec_idx", "vec", index_type="IVF_FLAT", num_partitions=1, metric_type="l2")
        dataset.create_index("text_idx", "text", index_type="INVERTED")

        old_snapshot_rows = _rows_from_ray_tables([first_table, *stream], expected_arity=1)
        assert sorted(row[0] for row in old_snapshot_rows) == [1, 2, 3, 4, 5]
        stream = None
        scan = None
        gc.collect()

        indexes = dataset.show_indexes().fetchall()
        assert [row[0] for row in indexes] == ["text_idx", "vec_idx"]

        explain_rows = connection.execute(
            f"""
            EXPLAIN (FORMAT JSON)
            SELECT id
            FROM lance_vector_search(
                '{dataset_path}',
                'vec',
                [0.0, 0.0, 0.0, 0.0]::FLOAT[4],
                k = 3,
                nprobs = 1,
                refine_factor = 2,
                use_index = true,
                explain_verbose = true
            )
            """
        ).fetchall()
        explain_text = "\n".join(str(value) for row in explain_rows for value in row)
        assert "ANNSubIndex: name=vec_idx" in explain_text

        vector = dataset.vector_search(
            "vec",
            [0.0, 0.0, 0.0, 0.0],
            k=3,
            nprobes=1,
            refine_factor=2,
            use_index=True,
        )
        fts = dataset.fts("text", "puppy", k=10)
        hybrid = dataset.hybrid_search(
            "vec",
            [0.0, 0.0, 0.0, 0.0],
            "text",
            "puppy",
            k=3,
            nprobes=1,
            refine_factor=2,
            use_index=True,
        )
        _assert_ray_global_source(vector, connection, "ray-lance-indexed-vector-search")
        _assert_ray_global_source(fts, connection, "ray-lance-indexed-fts")
        _assert_ray_global_source(hybrid, connection, "ray-lance-indexed-hybrid-search")

        assert [row[0] for row in _ray_rows(ray_lance_runner, vector.order("_distance").project("id"))] == [1, 2, 3]
        assert [row[0] for row in _ray_rows(ray_lance_runner, fts.order("id").project("id"))] == [1, 3]
        assert [row[0] for row in _ray_rows(ray_lance_runner, hybrid.order("_hybrid_score DESC").project("id"))] == [
            1,
            3,
            2,
        ]

        vector = None
        fts = None
        hybrid = None
        gc.collect()
        optimize_result = dataset.optimize()
        vacuum_result = dataset.vacuum(
            older_than_seconds=0,
            delete_unverified=False,
            error_if_tagged_old_versions=True,
        )
        assert optimize_result and optimize_result[0][0] == "compact"
        assert vacuum_result and vacuum_result[0][0] == "cleanup"
    finally:
        if stream is not None:
            stream.close()
        stream = None
        scan = None
        gc.collect()
        connection.close()


@pytest.mark.external_service
@pytest.mark.real_ray
@pytest.mark.ray_cluster_owner
def test_ray_lance_two_node_minio_fragment_scan_snapshot_and_search(
    ray_lance_two_node_minio_runner, tmp_path, monkeypatch
) -> None:
    ray_runner, minio_config, cluster_node_ids = ray_lance_two_node_minio_runner
    probe_connection = vane.connect()
    try:
        _configure_lance_minio(probe_connection, monkeypatch)
    finally:
        probe_connection.close()

    row_count = 65536
    source_path = tmp_path / "ray_lance_two_node_source.parquet"
    append_path = tmp_path / "ray_lance_two_node_append.parquet"
    search_path = tmp_path / "ray_lance_two_node_search.parquet"
    pq.write_table(
        pa.table(
            {
                "id": pa.array(range(row_count), type=pa.int64()),
                "bucket": pa.array((value % 17 for value in range(row_count)), type=pa.int32()),
            }
        ),
        source_path,
        row_group_size=4096,
    )
    pq.write_table(
        pa.table(
            {
                "id": pa.array([row_count], type=pa.int64()),
                "bucket": pa.array([row_count % 17], type=pa.int32()),
            }
        ),
        append_path,
    )
    _write_search_parquet(search_path)

    connection = vane.connect()
    namespace = None
    scan = None
    pinned_scan = None
    pinned_stream = None
    root_prefix = f"vane-lance-two-node/{uuid.uuid4()}/"
    _, _, _, _, bucket, _ = minio_config
    root_uri = f"s3://{bucket}/{root_prefix.rstrip('/')}"
    dataset = LanceDataset(f"{root_uri}/items.lance", connection)
    search_dataset = LanceDataset(f"{root_uri}/search.lance", connection)
    try:
        _create_lance_minio_secret(connection, minio_config)
        session_probe = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            connection.sql("SELECT 1"), "ray-lance-two-node-session-probe"
        )
        _, access_key, secret_key, _, _, _ = minio_config
        assert session_probe.has_explicit_s3_credentials() is False
        assert session_probe.session_config()["AWS_ACCESS_KEY_ID"] == access_key
        assert session_probe.session_config()["AWS_SECRET_ACCESS_KEY"] == secret_key
        dataset.write(
            connection.read_parquet(str(source_path)).repartition(8),
            mode="overwrite",
            max_rows_per_file=4096,
            max_rows_per_group=2048,
        )

        scan = dataset.scan().project("id, bucket")
        descriptor_counts = _ray_scan_descriptor_counts(scan, connection, "ray-lance-two-node-scan-plan")
        assert len(descriptor_counts) == 1
        assert descriptor_counts[0] >= 8
        post_planning_probe = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            connection.sql("SELECT 1"), "ray-lance-two-node-post-planning-probe"
        )
        assert post_planning_probe.has_explicit_s3_credentials() is False

        baseline_task_ids = set(_settled_ray_fte_create_task_locations())
        aggregate_rows = _ray_rows(
            ray_runner,
            scan.aggregate("count(*)::BIGINT AS rows, sum(id)::HUGEINT AS total"),
        )
        assert aggregate_rows == [(row_count, row_count * (row_count - 1) // 2)]
        execution_node_ids = _ray_fte_create_task_node_ids(baseline_task_ids, expected_count=2)
        assert execution_node_ids == set(cluster_node_ids)
        scan = None
        gc.collect()

        pinned_scan = dataset.scan().project("id")
        pinned_stream = ray_runner.run_iter_tables(pinned_scan)
        first_pinned_table = next(pinned_stream)
        dataset.write(connection.read_parquet(str(append_path)), mode="append")
        pinned_ids = [
            int(row[0]) for row in _rows_from_ray_tables([first_pinned_table, *pinned_stream], expected_arity=1)
        ]
        assert len(pinned_ids) == row_count
        assert min(pinned_ids) == 0
        assert max(pinned_ids) == row_count - 1
        pinned_stream = None
        pinned_scan = None
        gc.collect()

        fresh_rows = _ray_rows(
            ray_runner,
            dataset.scan().aggregate("count(*)::BIGINT AS rows, sum(id)::HUGEINT AS total"),
        )
        assert fresh_rows == [(row_count + 1, row_count * (row_count + 1) // 2)]

        search_dataset.write(
            connection.read_parquet(str(search_path)).repartition(3),
            mode="overwrite",
            max_rows_per_file=2,
            max_rows_per_group=1,
        )
        search_dataset.create_index(
            "vec_idx",
            "vec",
            index_type="IVF_FLAT",
            num_partitions=1,
            metric_type="l2",
        )
        search_dataset.create_index("text_idx", "text", index_type="INVERTED")
        hybrid = search_dataset.hybrid_search(
            "vec",
            [0.0, 0.0, 0.0, 0.0],
            "text",
            "puppy",
            k=3,
            nprobes=1,
            refine_factor=2,
            use_index=True,
        )
        _assert_ray_global_source(hybrid, connection, "ray-lance-two-node-hybrid")
        assert [row[0] for row in _ray_rows(ray_runner, hybrid.order("_hybrid_score DESC").project("id"))] == [
            1,
            3,
            2,
        ]

        namespace = LanceNamespace(root_uri, "ray_two_node_ns", connection=connection)
        namespace_tables = set(connection.execute("SHOW TABLES FROM ray_two_node_ns.main").fetchall())
        assert {("items",), ("search",)} <= namespace_tables

        optimize_result = dataset.optimize(target_rows_per_fragment=131072)
        vacuum_result = dataset.vacuum(
            older_than_seconds=0,
            delete_unverified=False,
            error_if_tagged_old_versions=True,
        )
        assert optimize_result and optimize_result[0][0] == "compact"
        assert vacuum_result and vacuum_result[0][0] == "cleanup"
    finally:
        if pinned_stream is not None:
            pinned_stream.close()
        scan = None
        pinned_stream = None
        pinned_scan = None
        gc.collect()
        if namespace is not None:
            namespace.detach()
        connection.close()
        _delete_minio_prefix(minio_config, root_prefix)


@pytest.mark.external_service
def test_ray_lance_minio_write_namespace_index_and_search(ray_lance_minio_runner, tmp_path, monkeypatch) -> None:
    ray_runner, minio_config = ray_lance_minio_runner
    probe_connection = vane.connect()
    try:
        _configure_lance_minio(probe_connection, monkeypatch)
    finally:
        probe_connection.close()

    connection = vane.connect()
    other_connection = vane.connect()
    namespace = None
    hybrid = None
    logical_plan = None
    physical_plan = None
    root_prefix = None
    source_path = tmp_path / "ray_lance_minio.parquet"
    _write_search_parquet(source_path)
    try:
        _, access_key, secret_key, _, bucket, session_token = minio_config
        _create_lance_minio_secret(connection, minio_config)
        session_probe = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            connection.sql("SELECT 1"), "ray-lance-minio-session-probe"
        )
        assert session_probe.has_explicit_s3_credentials() is False
        assert session_probe.session_config()["AWS_ACCESS_KEY_ID"] == access_key
        assert session_probe.session_config()["AWS_SECRET_ACCESS_KEY"] == secret_key
        root_prefix = f"vane-lance-e2e/{uuid.uuid4()}/"
        root_uri = f"s3://{bucket}/{root_prefix.rstrip('/')}"
        dataset_uri = f"{root_uri}/items.lance"
        dataset = LanceDataset(dataset_uri, connection)
        dataset.write(
            connection.read_parquet(str(source_path)).repartition(3),
            mode="overwrite",
            max_rows_per_file=2,
            max_rows_per_group=1,
        )

        namespace = LanceNamespace(root_uri, "ray_minio_ns", connection=connection)
        assert ("items",) in connection.execute("SHOW TABLES FROM ray_minio_ns.main").fetchall()
        assert sorted(row[0] for row in _ray_rows(ray_runner, namespace.table("items").scan().project("id"))) == [
            1,
            2,
            3,
            4,
            5,
        ]

        dataset.create_index("vec_idx", "vec", index_type="IVF_FLAT", num_partitions=1, metric_type="l2")
        dataset.create_index("text_idx", "text", index_type="INVERTED")
        hybrid = dataset.hybrid_search(
            "vec",
            [0.0, 0.0, 0.0, 0.0],
            "text",
            "puppy",
            k=3,
            nprobes=1,
            refine_factor=2,
            use_index=True,
        )
        logical_plan = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(hybrid, "ray-lance-minio-hybrid")
        physical_plan = logical_plan.to_physical_plan(connection)
        plan_text = physical_plan.repr_ascii(False)
        assert physical_plan.num_partitions() == 1
        assert physical_plan.scan_task_descriptor_map() == {}
        assert [row[0] for row in _ray_rows(ray_runner, hybrid.order("_hybrid_score DESC").project("id"))] == [
            1,
            3,
            2,
        ]
        for credential in (access_key, secret_key, session_token):
            if credential:
                assert credential not in plan_text

        assert (
            other_connection.execute("SELECT name FROM duckdb_secrets() WHERE name = 'vane_lance_session'").fetchall()
            == []
        )
    finally:
        physical_plan = None
        logical_plan = None
        hybrid = None
        gc.collect()
        if namespace is not None:
            try:
                namespace.drop_table("items", if_exists=True)
            finally:
                namespace.detach()
        other_connection.close()
        connection.close()
        if root_prefix is not None:
            _delete_minio_prefix(minio_config, root_prefix)


@pytest.mark.external_service
def test_ray_lance_rest_namespace_query_table_is_global_source(ray_lance_runner) -> None:
    endpoint = os.getenv("LANCE_NAMESPACE_ENDPOINT", "").strip()
    namespace_id = os.getenv("LANCE_NAMESPACE_ID", "").strip()
    table_name = os.getenv("LANCE_NAMESPACE_TABLE", "").strip()
    if not endpoint or not namespace_id or not table_name:
        pytest.skip("LANCE_NAMESPACE_ENDPOINT, LANCE_NAMESPACE_ID, and LANCE_NAMESPACE_TABLE are required")
    if '"' in table_name:
        pytest.skip("LANCE_NAMESPACE_TABLE must not contain a double quote")

    bearer_token = os.getenv("LANCE_NAMESPACE_BEARER_TOKEN", "").strip()
    api_key = os.getenv("LANCE_NAMESPACE_API_KEY", "").strip()
    header = os.getenv("LANCE_NAMESPACE_HEADER", "").strip()

    connection = vane.connect()
    namespace = None
    try:
        if bearer_token or api_key or header:

            def quote(value: str) -> str:
                return "'" + value.replace("'", "''") + "'"

            attach_options = ["TYPE LANCE", "READ_ONLY true", "ENDPOINT " + quote(endpoint)]
            if bearer_token:
                attach_options.append("BEARER_TOKEN " + quote(bearer_token))
            if api_key:
                attach_options.append("API_KEY " + quote(api_key))
            if header:
                attach_options.append("HEADER " + quote(header))
            connection.execute("ATTACH " + quote(namespace_id) + " AS ray_rest_ns (" + ", ".join(attach_options) + ")")
            namespace = LanceNamespace(
                namespace_id, "ray_rest_ns", endpoint=endpoint, read_only=True, connection=connection, attach=False
            )
        else:
            namespace = LanceNamespace(
                namespace_id, "ray_rest_ns", endpoint=endpoint, read_only=True, connection=connection
            )
        assert (table_name,) in connection.execute("SHOW TABLES FROM ray_rest_ns.main").fetchall()
        relation = connection.sql(f'SELECT * FROM ray_rest_ns.main."{table_name}"')
        _assert_ray_global_source(relation, connection, "ray-lance-rest-query-table")
        rows = _ray_rows(ray_lance_runner, relation.aggregate("count(*)::BIGINT"))
        assert len(rows) == 1
        assert int(rows[0][0]) >= 0
        explain = connection.execute(
            f'EXPLAIN (FORMAT JSON) SELECT * FROM ray_rest_ns.main."{table_name}" LIMIT 1'
        ).fetchall()
        explain_text = "\n".join(str(value) for row in explain for value in row)
        assert '"Lance Scan Backend": "namespace_query_table"' in explain_text
        assert '"Lance Limit Offset Pushdown": "true"' in explain_text
    finally:
        if namespace is not None:
            namespace.detach()
        connection.close()


def test_public_lance_write_and_fragment_scan(local_runner, tmp_path, monkeypatch) -> None:
    source_path = tmp_path / "lance_input.parquet"
    dataset_path = tmp_path / "public_api.lance"

    monkeypatch.setenv("VANE_RUNNER", "local-fast")
    setup_connection = vane.connect()
    try:
        setup_connection.sql(
            "SELECT i::BIGINT AS id, (i % 3)::INTEGER AS bucket FROM range(32) AS input(i)"
        ).write_parquet(str(source_path))
    finally:
        setup_connection.close()

    monkeypatch.setenv("VANE_RUNNER", "local")
    connection = vane.connect()
    try:
        dataset = LanceDataset(dataset_path, connection)
        dataset.write(connection.read_parquet(str(source_path)).repartition(4), mode="overwrite")

        assert vane.read_lance(str(dataset_path), connection=connection).aggregate(
            "count(*) AS rows, sum(id) AS total, min(bucket) AS lo, max(bucket) AS hi"
        ).fetchone() == (32, 496, 0, 2)
        with dataset.snapshot() as snapshot:
            assert snapshot.filter("id >= 28").order("id").project("id").fetchall() == [
                (28,),
                (29,),
                (30,),
                (31,),
            ]
    finally:
        connection.close()


def test_distributed_empty_lance_scan_returns_no_rows(local_runner, tmp_path) -> None:
    connection = vane.connect()
    dataset_path = tmp_path / "empty_scan.lance"
    try:
        quoted_path = str(dataset_path).replace("'", "''")
        connection.execute(
            "COPY (SELECT 1::BIGINT AS id LIMIT 0) "
            f"TO '{quoted_path}' (FORMAT lance, MODE 'overwrite', WRITE_EMPTY_FILE true)"
        )
        assert vane.read_lance(str(dataset_path), connection=connection).fetchall() == []
    finally:
        connection.close()


def test_successful_distributed_lance_write_removes_writer_barrier(tmp_path, monkeypatch) -> None:
    _teardown_runner()
    session_dir = tmp_path / "session"
    monkeypatch.setenv("VANE_SESSION_DIR", str(session_dir))
    connection = None
    try:
        set_runner_local(num_workers=2, max_running_tasks=2)
        connection = vane.connect()
        connection.sql("SELECT 1::BIGINT AS id").write_lance(str(tmp_path / "barrier_cleanup.lance"), mode="create")
        barrier_dir = session_dir / "lance_writer_barriers"
        assert barrier_dir.is_dir()
        assert list(barrier_dir.glob("*.writer_started")) == []
    finally:
        if connection is not None:
            connection.close()
        _teardown_runner()


def test_lance_write_reports_committed_when_mutation_lease_cleanup_fails(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("VANE_RUNNER", "local-fast")
    dataset_path = tmp_path / "committed_cleanup_failure.lance"
    real_mutation_lease = coordinator_module._MutationLease

    class FailingReleaseMutationLease(real_mutation_lease):
        def close(self, *, wait: bool = True) -> None:
            super().close(wait=wait)
            raise RuntimeError("planned mutation lease release failure")

    monkeypatch.setattr(coordinator_module, "_MutationLease", FailingReleaseMutationLease)
    connection = vane.connect()
    try:
        with pytest.raises(LanceCommitCleanupError, match="Do not retry the write") as error:
            connection.sql("SELECT 42::BIGINT AS id").write_lance(str(dataset_path), mode="overwrite")

        assert error.value.committed is True
        assert error.value.safe_to_retry is False
        assert error.value.uri == str(dataset_path)
        assert "planned mutation lease release failure" in error.value.detail
        assert vane.read_lance(str(dataset_path), connection=connection).fetchall() == [(42,)]
    finally:
        connection.close()


def test_running_lance_scan_remains_on_bind_time_version_during_append(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("VANE_RUNNER", "local-fast")
    dataset_path = tmp_path / "snapshot.lance"
    reader = vane.connect()
    writer = vane.connect()
    query_result: list[tuple[int, int]] = []
    query_error: list[BaseException] = []
    query_entered = threading.Event()
    release_query = threading.Event()
    query_thread: threading.Thread | None = None

    try:
        reader.sql("SELECT i::BIGINT AS id FROM range(4) AS input(i)").write_lance(str(dataset_path), mode="overwrite")

        barrier_batch = pa.record_batch({"token": pa.array([1], type=pa.int64())})

        def barrier_batches():
            query_entered.set()
            if not release_query.wait(timeout=30):
                raise TimeoutError("timed out waiting to release the Lance snapshot query")
            yield barrier_batch

        barrier_reader = pa.RecordBatchReader.from_batches(
            barrier_batch.schema,
            barrier_batches(),
        )
        reader.register("lance_snapshot_query_barrier", barrier_reader)
        dataset = LanceDataset(dataset_path, reader)
        query = (
            dataset.scan()
            .set_alias("lance_rows")
            .join(reader.table("lance_snapshot_query_barrier").set_alias("barrier"), "TRUE")
            .project("lance_rows.id, barrier.token")
            .order("id")
        )

        def run_query() -> None:
            try:
                query_result.extend(query.fetchall())
            except BaseException as exc:
                query_error.append(exc)

        query_thread = threading.Thread(target=run_query, daemon=True)
        query_thread.start()
        assert query_entered.wait(timeout=30), "Lance query never reached the post-bind execution barrier"

        writer.sql("SELECT 4::BIGINT AS id").write_lance(str(dataset_path), mode="append")
        release_query.set()
        query_thread.join(timeout=30)

        assert not query_thread.is_alive()
        assert not query_error
        assert query_result == [(0, 1), (1, 1), (2, 1), (3, 1)]
        assert dataset.scan().order("id").fetchall() == [(0,), (1,), (2,), (3,), (4,)]
    finally:
        release_query.set()
        if query_thread is not None:
            query_thread.join(timeout=30)
        reader.close()
        writer.close()


def test_lance_dataset_cache_distinguishes_recreated_dataset(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("VANE_RUNNER", "local-fast")
    dataset_path = tmp_path / "recreated_cache.lance"
    reader = vane.connect()
    first_writer = vane.connect()
    second_writer = None
    try:
        first_writer.sql("SELECT 1::BIGINT AS id").write_lance(str(dataset_path), mode="create")
        assert vane.read_lance(str(dataset_path), connection=reader).fetchall() == [(1,)]

        first_writer.close()
        shutil.rmtree(dataset_path)
        second_writer = vane.connect()
        second_writer.sql("SELECT 2::BIGINT AS id").write_lance(str(dataset_path), mode="create")

        assert vane.read_lance(str(dataset_path), connection=reader).fetchall() == [(2,)]
    finally:
        reader.close()
        with suppress(Exception):
            first_writer.close()
        if second_writer is not None:
            second_writer.close()


def test_lance_dataset_cache_generation_survives_vacuum(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("VANE_RUNNER", "local-fast")
    dataset_path = tmp_path / "vacuumed_generation_cache.lance"
    connection = vane.connect()
    try:
        dataset = LanceDataset(dataset_path, connection)
        dataset.write(connection.sql("SELECT 1::BIGINT AS id"), mode="overwrite")
        dataset.write(connection.sql("SELECT 2::BIGINT AS id"), mode="append")
        assert dataset.scan().order("id").fetchall() == [(1,), (2,)]

        vacuum_result = dataset.vacuum(
            older_than_seconds=0,
            delete_unverified=False,
            error_if_tagged_old_versions=True,
        )
        assert vacuum_result and vacuum_result[0][0] == "cleanup"
        assert dataset.scan().order("id").fetchall() == [(1,), (2,)]
    finally:
        connection.close()


def test_public_lance_search_uses_global_results(monkeypatch) -> None:
    monkeypatch.setenv("VANE_RUNNER", "local-fast")
    connection = vane.connect()
    try:
        dataset = LanceDataset(SEARCH_DATASET, connection)
        vector_ids = (
            dataset.vector_search("vec", [0.0, 0.0, 0.0, 0.0], k=3, use_index=False)
            .order("_distance")
            .project("id")
            .fetchall()
        )
        fts_ids = dataset.fts("text", "puppy", k=10).order("id").project("id").fetchall()
        hybrid_ids = (
            dataset.hybrid_search(
                "vec",
                [0.0, 0.0, 0.0, 0.0],
                "text",
                "puppy",
                k=3,
                use_index=False,
            )
            .order("_hybrid_score DESC")
            .project("id")
            .fetchall()
        )

        assert vector_ids == [(1,), (2,), (3,)]
        assert fts_ids == [(1,), (3,)]
        assert hybrid_ids == [(1,), (3,), (2,)]
    finally:
        connection.close()


def test_distributed_lance_search_restores_cpu_slots_after_deserialization(monkeypatch) -> None:
    monkeypatch.setenv("VANE_RUNNER", "ray")
    connection = vane.connect()
    try:
        connection.execute("SET threads = 3")
        search_path = str(SEARCH_DATASET).replace("'", "''")
        searches = {
            "vector": connection.sql(
                "SELECT * FROM lance_vector_search("
                f"'{search_path}', 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], k = 3, use_index = false)"
            ),
            "fts": connection.sql(f"SELECT * FROM lance_fts('{search_path}', 'text', 'puppy', k = 3)"),
            "hybrid": connection.sql(
                "SELECT * FROM lance_hybrid_search("
                f"'{search_path}', 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
                "'text', 'puppy', k = 3, use_index = false)"
            ),
        }

        for name, relation in searches.items():
            assert _ray_scan_cpu_slots(relation, connection, f"lance-{name}-cpu-slots") == [3]
    finally:
        connection.close()


def test_lance_session_secret_is_temporary_and_connection_local(monkeypatch) -> None:
    monkeypatch.setenv("VANE_RUNNER", "local-fast")
    connection = vane.connect()
    other_connection = vane.connect()
    try:
        _configure_duckdb_s3(connection, {}, use_session_credentials=False)

        assert connection.sql(
            "SELECT name, type, provider, scope FROM duckdb_secrets() WHERE name = 'vane_lance_session'"
        ).fetchall() == [
            ("vane_lance_session", "lance", "credential_chain", ["s3://"]),
        ]
        assert (
            other_connection.sql("SELECT name FROM duckdb_secrets() WHERE name = 'vane_lance_session'").fetchall() == []
        )
    finally:
        connection.close()
        other_connection.close()


def test_lance_secret_redacts_key_valued_storage_options(monkeypatch) -> None:
    monkeypatch.setenv("VANE_RUNNER", "local-fast")
    connection = vane.connect()
    sentinels = {
        "API_KEY": "api-key-plaintext-sentinel",
        "ACCOUNT_KEY": "account-key-plaintext-sentinel",
        "SAS_KEY": "sas-key-plaintext-sentinel",
        "SERVICE_ACCOUNT_KEY": "service-account-key-plaintext-sentinel",
    }
    try:
        connection.execute(
            "CREATE OR REPLACE TEMPORARY SECRET lance_redaction_test "
            "(TYPE LANCE, PROVIDER config, SCOPE 's3://', STORAGE_OPTIONS ?)",
            [sentinels],
        )
        secret_string = connection.execute(
            "SELECT secret_string FROM duckdb_secrets() WHERE name = 'lance_redaction_test'"
        ).fetchone()[0]
        for sentinel in sentinels.values():
            assert sentinel not in secret_string
    finally:
        connection.close()


def test_namespace_dml_index_and_maintenance_api(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("VANE_RUNNER", "local-fast")
    connection = vane.connect()
    namespace = LanceNamespace(tmp_path, "lance_api", connection=connection)
    try:
        table = namespace.create_table(
            "items",
            "SELECT * FROM (VALUES (1::BIGINT, 'one'), (2::BIGINT, 'two')) AS input(id, value)",
        )
        table.insert("SELECT 3::BIGINT AS id, 'three'::VARCHAR AS value")
        table.update({"value": "'TWO'"}, where="id = 2")
        table.delete(where="id = 1")
        table.merge(
            "SELECT * FROM (VALUES (2::BIGINT, 'two-again'), (4::BIGINT, 'four')) AS input(id, value)",
            "target.id = source.id",
        ).when_matched_update({"value": "source.value"}).when_not_matched_insert(
            {"id": "source.id", "value": "source.value"}
        ).execute()

        assert table.scan().order("id").fetchall() == [
            (2, "two-again"),
            (3, "three"),
            (4, "four"),
        ]

        table.create_index("items_id", "id", index_type="BTREE")
        assert [row[0] for row in table.show_indexes().fetchall()] == ["items_id"]
        table.drop_index("items_id")
        assert table.show_indexes().fetchall() == []

        optimize_result = table.optimize()
        vacuum_result = table.vacuum(
            older_than_seconds=0,
            delete_unverified=False,
            error_if_tagged_old_versions=True,
        )
        assert optimize_result[0][0] == "compact"
        assert vacuum_result[0][0] == "cleanup"
    finally:
        namespace.drop_table("items", if_exists=True)
        namespace.detach()
        connection.close()


def test_lance_namespace_read_only_scans_and_rejects_mutation(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("VANE_RUNNER", "local-fast")
    connection = vane.connect()
    writable = LanceNamespace(tmp_path, "lance_writable", connection=connection)
    try:
        writable.create_table("items", "SELECT 1::BIGINT AS id, 'one'::VARCHAR AS value")
    finally:
        writable.detach()

    read_only = LanceNamespace(tmp_path, "lance_read_only", read_only=True, connection=connection)
    try:
        assert read_only.table("items").scan().fetchall() == [(1, "one")]
        with pytest.raises(vane.InvalidInputException, match="read-only mode"):
            read_only.table("items").insert("SELECT 2::BIGINT AS id, 'two'::VARCHAR AS value")
    finally:
        read_only.detach()
        connection.close()


def test_rest_namespace_hybrid_search_is_rejected_before_opening_backing_uri(monkeypatch) -> None:
    import json
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread

    monkeypatch.setenv("VANE_RUNNER", "local-fast")
    requests: list[str] = []

    class RestNamespaceHandler(BaseHTTPRequestHandler):
        def _send_json(self, body: dict[str, object]) -> None:
            payload = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self) -> None:
            requests.append(self.path)
            self._send_json({"tables": ["search"]})

        def do_POST(self) -> None:
            requests.append(self.path)
            content_length = int(self.headers.get("Content-Length", "0"))
            self.rfile.read(content_length)
            self._send_json(
                {
                    "table": "search",
                    "namespace": ["mock"],
                    "table_uri": SEARCH_DATASET.resolve().as_uri(),
                    "schema": {
                        "fields": [
                            {
                                "name": "id",
                                "nullable": False,
                                "type": {"type": "int64"},
                            }
                        ]
                    },
                }
            )

        def log_message(self, format, *args) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), RestNamespaceHandler)
    server_thread = Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    connection = vane.connect()
    namespace = None
    try:
        endpoint = f"http://127.0.0.1:{server.server_port}"
        namespace = LanceNamespace("mock", "rest_hybrid", endpoint=endpoint, read_only=True, connection=connection)
        with pytest.raises(vane.NotImplementedException, match="hybrid search is not supported for REST namespace"):
            namespace.table("search").hybrid_search(
                "vec",
                [0.0, 0.0, 0.0, 0.0],
                "text",
                "puppy",
            )
        assert any("/v1/table/" in request and "/describe" in request for request in requests)
    finally:
        if namespace is not None:
            namespace.detach()
        connection.close()
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)


@pytest.mark.parametrize("bad_name", ["idx; DROP TABLE items", 'idx"quoted', "a.b"])
def test_lance_index_name_rejects_custom_command_injection(tmp_path, monkeypatch, bad_name) -> None:
    monkeypatch.setenv("VANE_RUNNER", "local-fast")
    connection = vane.connect()
    dataset = LanceDataset(tmp_path / "does-not-need-to-exist.lance", connection)
    try:
        with pytest.raises(ValueError, match="invalid Lance index name"):
            dataset.create_index(bad_name, "id", index_type="BTREE")
    finally:
        connection.close()
