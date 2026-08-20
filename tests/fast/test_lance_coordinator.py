# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import gc
import pickle
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest

import vane.lance._coordinator as coordinator_module
from vane.lance._coordinator import (
    DatasetCoordinator,
    LanceCommitCleanupError,
    _LocalDatasetCoordinator,
    _MutationLease,
    _SnapshotLease,
    normalize_dataset_uri,
)


def test_normalize_dataset_uri_removes_credentials_and_query() -> None:
    assert (
        normalize_dataset_uri("S3://access:secret@Bucket/path/data.lance/?token=hidden#fragment")
        == "s3://bucket/path/data.lance"
    )


def test_normalize_dataset_uri_canonicalizes_s3_aliases() -> None:
    expected = "s3://bucket/path/data.lance"
    assert normalize_dataset_uri("s3://bucket/path/data.lance") == expected
    assert normalize_dataset_uri("s3a://bucket/path/data.lance") == expected
    assert normalize_dataset_uri("s3n://bucket/path/data.lance") == expected


def test_normalize_dataset_uri_canonicalizes_local_file_uri(tmp_path) -> None:
    dataset_path = tmp_path / "space in name.lance"
    plain = DatasetCoordinator(dataset_path)
    file_uri = DatasetCoordinator(dataset_path.as_uri())
    localhost_uri = DatasetCoordinator(f"file://localhost{dataset_path.as_posix()}")

    assert plain.identity == str(dataset_path.resolve())
    assert file_uri.identity == plain.identity
    assert localhost_uri.identity == plain.identity
    assert file_uri._local is plain._local
    assert localhost_uri._local is plain._local


def test_dataset_coordinator_initializes_configured_ray_before_acquiring_lease(monkeypatch, tmp_path) -> None:
    calls: list[tuple[str, str]] = []
    events: list[str] = []

    class _RemoteMethod:
        def __init__(self, name: str) -> None:
            self._name = name

        def remote(self, token: str) -> None:
            calls.append((self._name, token))

    actor = SimpleNamespace(
        acquire_snapshot=_RemoteMethod("acquire_snapshot"),
        release_snapshot=_RemoteMethod("release_snapshot"),
    )
    fake_ray = SimpleNamespace(
        get_actor=lambda name, namespace: events.append("get_actor") or actor,
        get=lambda result: result,
    )
    monkeypatch.setattr(coordinator_module, "_configured_runner_type", lambda: "ray")
    monkeypatch.setattr(
        coordinator_module,
        "_ray_runtime_for_coordinator",
        lambda: events.append("initialize_ray") or fake_ray,
    )
    monkeypatch.setattr(coordinator_module, "_ray_coordinator_class", lambda ray: object())

    coordinator = DatasetCoordinator(tmp_path / "late-ray.lance")
    assert coordinator._ray is None

    lease = _SnapshotLease(coordinator)
    token = lease._token
    assert coordinator._ray is actor
    assert events == ["initialize_ray", "get_actor"]

    lease.close()
    assert calls == [("acquire_snapshot", token), ("release_snapshot", token)]


def test_dataset_coordinator_keeps_configured_local_backend_when_ray_is_running(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(coordinator_module, "_configured_runner_type", lambda: "local")
    monkeypatch.setattr(
        coordinator_module,
        "_ray_runtime_for_coordinator",
        lambda: pytest.fail("local coordination must not initialize or use Ray"),
    )

    coordinator = DatasetCoordinator(tmp_path / "local.lance")
    lease = _SnapshotLease(coordinator)
    assert coordinator._backend_kind == "local"
    assert coordinator._local._active_snapshots == {lease._token}
    lease.close()


def test_dataset_coordinator_fails_closed_if_runner_backend_changes(monkeypatch, tmp_path) -> None:
    runner_type = "local"
    monkeypatch.setattr(coordinator_module, "_configured_runner_type", lambda: runner_type)

    coordinator = DatasetCoordinator(tmp_path / "changed-runner.lance")
    lease = _SnapshotLease(coordinator)
    lease.close()

    runner_type = "ray"
    with pytest.raises(RuntimeError, match="runner changed after the Lance coordinator backend was selected"):
        _SnapshotLease(coordinator)


def test_dataset_coordinator_backend_binding_is_shared_by_identity(monkeypatch, tmp_path) -> None:
    runner_type = "local"
    monkeypatch.setattr(coordinator_module, "_configured_runner_type", lambda: runner_type)
    identity = tmp_path / "shared-backend.lance"

    lease = _SnapshotLease(DatasetCoordinator(identity))
    runner_type = "ray"
    with pytest.raises(RuntimeError, match="runner changed after the Lance coordinator backend was selected"):
        _SnapshotLease(DatasetCoordinator(identity))
    lease.close()


def test_mutations_are_fifo() -> None:
    coordinator = _LocalDatasetCoordinator()
    coordinator.acquire_mutation("owner")
    acquired: list[str] = []
    release_first = threading.Event()
    first_acquired = threading.Event()
    second_acquired = threading.Event()

    def run(token: str, release: threading.Event | None = None) -> None:
        coordinator.acquire_mutation(token)
        acquired.append(token)
        (first_acquired if token == "first" else second_acquired).set()
        if release is not None:
            assert release.wait(timeout=30)
        coordinator.release_mutation(token)

    first = threading.Thread(target=run, args=("first", release_first), daemon=True)
    second = threading.Thread(target=run, args=("second",), daemon=True)
    first.start()
    with coordinator._condition:
        assert coordinator._condition.wait_for(lambda: list(coordinator._mutation_queue) == ["first"], timeout=5)
    second.start()
    with coordinator._condition:
        assert coordinator._condition.wait_for(
            lambda: list(coordinator._mutation_queue) == ["first", "second"], timeout=5
        )

    coordinator.release_mutation("owner")
    assert first_acquired.wait(timeout=5)
    assert not second_acquired.is_set()
    release_first.set()
    assert second_acquired.wait(timeout=5)
    first.join(timeout=5)
    second.join(timeout=5)

    assert not first.is_alive()
    assert not second.is_alive()
    assert acquired == ["first", "second"]


def test_consistent_snapshot_blocks_mutations_until_relation_release() -> None:
    coordinator = _LocalDatasetCoordinator()
    coordinator.acquire_consistent_snapshot("reader")
    mutation_acquired = threading.Event()

    def run_mutation() -> None:
        coordinator.acquire_mutation("writer")
        mutation_acquired.set()
        coordinator.release_mutation("writer")

    mutation = threading.Thread(target=run_mutation, daemon=True)
    mutation.start()
    with coordinator._condition:
        assert coordinator._condition.wait_for(lambda: list(coordinator._mutation_queue) == ["writer"], timeout=5)
    assert not mutation_acquired.is_set()

    coordinator.release_consistent_snapshot("reader")
    assert mutation_acquired.wait(timeout=5)
    mutation.join(timeout=5)
    assert not mutation.is_alive()


def test_waiting_consistent_snapshot_gets_a_turn_between_mutations() -> None:
    coordinator = _LocalDatasetCoordinator()
    coordinator.acquire_mutation("owner")
    reader_acquired = threading.Event()
    reader_release = threading.Event()
    writer_acquired = threading.Event()

    def read() -> None:
        coordinator.acquire_consistent_snapshot("reader")
        reader_acquired.set()
        assert reader_release.wait(timeout=5)
        coordinator.release_consistent_snapshot("reader")

    def write() -> None:
        coordinator.acquire_mutation("next-writer")
        writer_acquired.set()
        coordinator.release_mutation("next-writer")

    writer = threading.Thread(target=write, daemon=True)
    reader = threading.Thread(target=read, daemon=True)
    writer.start()
    with coordinator._condition:
        assert coordinator._condition.wait_for(lambda: list(coordinator._mutation_queue) == ["next-writer"], timeout=5)
    reader.start()
    with coordinator._condition:
        assert coordinator._condition.wait_for(lambda: coordinator._consistent_snapshot_waiters == 1, timeout=5)

    coordinator.release_mutation("owner")
    assert reader_acquired.wait(timeout=5)
    assert not writer_acquired.is_set()
    reader_release.set()
    assert writer_acquired.wait(timeout=5)
    reader.join(timeout=5)
    writer.join(timeout=5)


def test_waiting_vacuum_gets_a_turn_before_queued_mutation() -> None:
    coordinator = _LocalDatasetCoordinator()
    coordinator.acquire_mutation("owner")
    vacuum_acquired = threading.Event()
    vacuum_release = threading.Event()
    writer_acquired = threading.Event()

    def vacuum() -> None:
        coordinator.acquire_vacuum("vacuum")
        vacuum_acquired.set()
        assert vacuum_release.wait(timeout=5)
        coordinator.release_vacuum("vacuum")

    def write() -> None:
        coordinator.acquire_mutation("next-writer")
        writer_acquired.set()
        coordinator.release_mutation("next-writer")

    vacuum_thread = threading.Thread(target=vacuum, daemon=True)
    writer = threading.Thread(target=write, daemon=True)
    vacuum_thread.start()
    with coordinator._condition:
        assert coordinator._condition.wait_for(lambda: coordinator._vacuum_waiters == 1, timeout=5)
    writer.start()
    with coordinator._condition:
        assert coordinator._condition.wait_for(lambda: list(coordinator._mutation_queue) == ["next-writer"], timeout=5)

    coordinator.release_mutation("owner")
    assert vacuum_acquired.wait(timeout=5)
    assert not writer_acquired.is_set()
    vacuum_release.set()
    assert writer_acquired.wait(timeout=5)
    vacuum_thread.join(timeout=5)
    writer.join(timeout=5)


def test_consistent_snapshot_and_mutation_do_not_deadlock_after_vacuum() -> None:
    coordinator = _LocalDatasetCoordinator()
    coordinator.acquire_vacuum("owner")
    reader_acquired = threading.Event()
    reader_release = threading.Event()
    writer_acquired = threading.Event()

    def write() -> None:
        coordinator.acquire_mutation("writer")
        writer_acquired.set()
        coordinator.release_mutation("writer")

    def read() -> None:
        coordinator.acquire_consistent_snapshot("reader")
        reader_acquired.set()
        assert reader_release.wait(timeout=5)
        coordinator.release_consistent_snapshot("reader")

    writer = threading.Thread(target=write, daemon=True)
    reader = threading.Thread(target=read, daemon=True)
    writer.start()
    with coordinator._condition:
        assert coordinator._condition.wait_for(lambda: list(coordinator._mutation_queue) == ["writer"], timeout=5)
    reader.start()
    with coordinator._condition:
        assert coordinator._condition.wait_for(lambda: coordinator._consistent_snapshot_waiters == 1, timeout=5)

    coordinator.release_vacuum("owner")
    assert reader_acquired.wait(timeout=5)
    assert not writer_acquired.is_set()
    reader_release.set()
    assert writer_acquired.wait(timeout=5)
    reader.join(timeout=5)
    writer.join(timeout=5)


def test_lease_acquire_timeout_reports_current_owner() -> None:
    coordinator = _LocalDatasetCoordinator("diagnostic-identity")
    coordinator.acquire_snapshot("reader|pid=1|thread=1")
    try:
        with pytest.raises(TimeoutError, match=r"active_snapshots.*reader"):
            coordinator.acquire_vacuum("vacuum|pid=1|thread=1", timeout=0.01)
    finally:
        coordinator.release_snapshot("reader|pid=1|thread=1")


@pytest.mark.skipif(coordinator_module.fcntl is None, reason="requires POSIX flock")
def test_local_process_lock_recovers_after_owner_is_killed(tmp_path) -> None:
    identity = str(tmp_path / "cross-process.lance")
    child_code = """
import os
import sys
import time
from vane.lance._coordinator import _LocalDatasetCoordinator

coordinator = _LocalDatasetCoordinator(sys.argv[1])
token = f"child|pid={os.getpid()}|thread=0"
coordinator.acquire_mutation(token)
print(token, flush=True)
time.sleep(60)
"""
    child = subprocess.Popen(
        [sys.executable, "-c", child_code, identity],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout is not None
        child_token = child.stdout.readline().strip()
        assert child_token.startswith("child|pid=")

        coordinator = _LocalDatasetCoordinator(identity)
        parent_token = f"parent|pid={coordinator_module.os.getpid()}|thread=0"
        with pytest.raises(TimeoutError, match="current cross-process tokens") as error:
            coordinator.acquire_mutation(parent_token, timeout=0.2)
        assert child_token in str(error.value)

        child.kill()
        child.wait(timeout=5)
        coordinator.acquire_mutation(parent_token, timeout=5)
        coordinator.release_mutation(parent_token)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)


def test_ray_coordinator_reserves_a_release_concurrency_slot(monkeypatch) -> None:
    class FakeRay:
        def __init__(self) -> None:
            self.remote_options: dict[str, object] = {}

        def method(self, *, concurrency_group: str):
            def decorate(method):
                method.test_concurrency_group = concurrency_group
                return method

            return decorate

        def remote(self, **options: object):
            self.remote_options = dict(options)

            def decorate(actor_class):
                return actor_class

            return decorate

    fake_ray = FakeRay()
    monkeypatch.setattr(coordinator_module, "_RAY_COORDINATOR_CLASS", None)

    actor_class = coordinator_module._ray_coordinator_class(fake_ray)

    assert fake_ray.remote_options == {
        "max_concurrency": 128,
        "concurrency_groups": {"acquire": 127, "release": 1},
    }
    for method_name in (
        "acquire_snapshot",
        "acquire_consistent_snapshot",
        "acquire_mutation",
        "acquire_vacuum",
    ):
        assert getattr(actor_class, method_name).test_concurrency_group == "acquire"
    for method_name in (
        "release_snapshot",
        "release_consistent_snapshot",
        "release_mutation",
        "release_vacuum",
    ):
        assert getattr(actor_class, method_name).test_concurrency_group == "release"


def test_vacuum_waits_for_snapshot_and_writer_but_reads_and_writes_overlap() -> None:
    coordinator = _LocalDatasetCoordinator()
    coordinator.acquire_snapshot("reader")
    coordinator.acquire_mutation("writer")

    vacuum_acquired = threading.Event()
    vacuum_released = threading.Event()

    def run_vacuum() -> None:
        coordinator.acquire_vacuum("vacuum")
        vacuum_acquired.set()
        assert vacuum_released.wait(timeout=5)
        coordinator.release_vacuum("vacuum")

    vacuum = threading.Thread(target=run_vacuum)
    vacuum.start()
    with coordinator._condition:
        assert coordinator._condition.wait_for(lambda: coordinator._vacuum_waiters == 1, timeout=5)
    assert not vacuum_acquired.is_set()

    coordinator.release_mutation("writer")
    assert not vacuum_acquired.is_set()
    coordinator.release_snapshot("reader")
    assert vacuum_acquired.wait(timeout=5)

    vacuum_released.set()
    vacuum.join(timeout=5)
    assert not vacuum.is_alive()


def test_different_dataset_coordinators_do_not_block_each_other() -> None:
    left = _LocalDatasetCoordinator()
    right = _LocalDatasetCoordinator()
    left.acquire_mutation("left")
    right.acquire_mutation("right")
    right.release_mutation("right")
    left.release_mutation("left")


def test_snapshot_lease_is_idempotent_and_blocks_vacuum(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(coordinator_module, "_configured_runner_type", lambda: "local-fast")
    coordinator = DatasetCoordinator(tmp_path / "lease.lance")
    local = coordinator._local
    lease = _SnapshotLease(coordinator)

    vacuum_acquired = threading.Event()

    def run_vacuum() -> None:
        local.acquire_vacuum("vacuum")
        vacuum_acquired.set()
        local.release_vacuum("vacuum")

    vacuum = threading.Thread(target=run_vacuum, daemon=True)
    vacuum.start()
    with local._condition:
        assert local._condition.wait_for(lambda: local._vacuum_waiters == 1, timeout=5)
    assert not vacuum_acquired.is_set()

    lease.close()
    lease.close()
    assert vacuum_acquired.wait(timeout=5)
    vacuum.join(timeout=5)
    assert not vacuum.is_alive()


def test_snapshot_lease_plan_transport_does_not_transfer_ownership(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(coordinator_module, "_configured_runner_type", lambda: "local-fast")
    coordinator = DatasetCoordinator(tmp_path / "serialized-lease.lance")
    local = coordinator._local
    lease = _SnapshotLease(coordinator)
    token = lease._token

    transferred = pickle.loads(pickle.dumps(lease))
    del transferred
    gc.collect()

    assert local._active_snapshots == {token}
    lease.close()
    assert local._active_snapshots == set()


def test_outcome_unknown_mutation_lease_is_retained_for_operator_reconciliation(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(coordinator_module, "_configured_runner_type", lambda: "local-fast")
    coordinator = DatasetCoordinator(tmp_path / "unknown-write.lance")
    lease = _MutationLease(coordinator)
    token = lease._token

    lease.retain_after_outcome_unknown()
    del lease
    gc.collect()

    status = coordinator.lease_status()
    assert status["active_mutation"] == token
    assert status["outcome_unknown_mutations"] == [token]
    assert coordinator.force_release(token) is True
    assert coordinator.lease_status()["active_mutation"] is None


def test_lance_commit_cleanup_error_pickles_round_trip() -> None:
    original = LanceCommitCleanupError("s3://bucket/items.lance", "coordinator unavailable")
    restored = pickle.loads(pickle.dumps(original))

    assert isinstance(restored, LanceCommitCleanupError)
    assert restored.uri == original.uri
    assert restored.detail == original.detail
    assert restored.committed is True
    assert restored.safe_to_retry is False


def test_real_ray_dataset_coordinator_orders_mutations_and_vacuum(ray_local, request, tmp_path) -> None:
    import ray

    import vane
    from vane import runners

    vane.teardown_runner()
    runners.set_runner_ray(noop_if_initialized=True)
    request.addfinalizer(vane.teardown_runner)

    left = DatasetCoordinator(tmp_path / "ray-left.lance")
    same_left = DatasetCoordinator(tmp_path / "ray-left.lance")
    right = DatasetCoordinator(tmp_path / "ray-right.lance")
    with left.snapshot(), same_left.snapshot(), right.snapshot():
        pass
    assert left._ray is not None
    assert same_left._ray is not None
    assert right._ray is not None
    assert left._ray._actor_id == same_left._ray._actor_id
    assert left._ray._actor_id != right._ray._actor_id

    actor = left._ray
    ray.get(actor.acquire_mutation.remote("owner"))
    first = actor.acquire_mutation.remote("first")
    second = actor.acquire_mutation.remote("second")
    ray.get(actor.release_mutation.remote("owner"))
    # A max-concurrency async actor may start these two submitted coroutines in
    # either order. Its deque preserves the order in which they enter the actor;
    # verify that exactly one wins and the other remains blocked until release.
    ready, _ = ray.wait([first, second], num_returns=1, timeout=5)
    assert len(ready) == 1
    if ready[0] == first:
        active_token, waiting_token, waiting = "first", "second", second
    else:
        active_token, waiting_token, waiting = "second", "first", first
    assert ray.wait([waiting], timeout=0.1)[0] == []
    ray.get(actor.release_mutation.remote(active_token))
    ray.get(waiting, timeout=5)

    # A different normalized dataset has a different coordinator and can hold
    # its mutation lease while the left dataset remains occupied.
    ray.get(right._ray.acquire_mutation.remote("right"), timeout=5)
    ray.get(right._ray.release_mutation.remote("right"), timeout=5)

    ray.get(actor.release_mutation.remote(waiting_token))
    ray.get(actor.acquire_snapshot.remote("reader"))
    ray.get(actor.acquire_mutation.remote("writer"))
    vacuum = actor.acquire_vacuum.remote("vacuum")
    assert ray.wait([vacuum], timeout=0.1)[0] == []
    ray.get(actor.release_mutation.remote("writer"))
    assert ray.wait([vacuum], timeout=0.1)[0] == []
    ray.get(actor.release_snapshot.remote("reader"))
    ray.get(vacuum, timeout=5)
    ray.get(actor.release_vacuum.remote("vacuum"))

    lease = _SnapshotLease(left)
    lease.__del__()
    vacuum_after_finalizer = actor.acquire_vacuum.remote("vacuum-after-finalizer")
    ray.get(vacuum_after_finalizer, timeout=5)
    ray.get(actor.release_vacuum.remote("vacuum-after-finalizer"))
