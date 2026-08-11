# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import asyncio
import hashlib
import os
import threading
import uuid
from collections import deque
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, SupportsIndex
from urllib.parse import SplitResult, urlsplit, urlunsplit


def normalize_dataset_uri(uri: str | os.PathLike[str]) -> str:
    """Return a credential-free identity suitable for coordinator naming."""
    value = os.fspath(uri).strip()
    if not value:
        raise ValueError("Lance dataset URI cannot be empty")
    parsed = urlsplit(value)
    if not parsed.scheme or (len(parsed.scheme) == 1 and value[1:2] == ":"):
        return str(Path(value).expanduser().resolve(strict=False))
    hostname = (parsed.hostname or "").lower()
    port = f":{parsed.port}" if parsed.port is not None else ""
    # Deliberately omit userinfo, query, and fragment so credentials cannot
    # enter actor names, task context, or logs.
    scheme = parsed.scheme.lower()
    if scheme in {"s3a", "s3n"}:
        scheme = "s3"
    clean = SplitResult(scheme, hostname + port, parsed.path.rstrip("/"), "", "")
    return urlunsplit(clean)


class _LocalDatasetCoordinator:
    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._mutation_queue: deque[str] = deque()
        self._active_mutation: str | None = None
        self._active_snapshots: set[str] = set()
        self._vacuum_waiters = 0
        self._active_vacuum: str | None = None

    def acquire_snapshot(self, token: str) -> None:
        with self._condition:
            self._condition.wait_for(lambda: self._active_vacuum is None and self._vacuum_waiters == 0)
            self._active_snapshots.add(token)

    def release_snapshot(self, token: str) -> None:
        with self._condition:
            self._active_snapshots.discard(token)
            self._condition.notify_all()

    def acquire_mutation(self, token: str) -> None:
        with self._condition:
            self._mutation_queue.append(token)
            try:
                self._condition.wait_for(
                    lambda: (
                        self._active_vacuum is None
                        and self._active_mutation is None
                        and self._mutation_queue
                        and self._mutation_queue[0] == token
                    )
                )
            except BaseException:
                try:
                    self._mutation_queue.remove(token)
                except ValueError:
                    pass
                self._condition.notify_all()
                raise
            self._mutation_queue.popleft()
            self._active_mutation = token

    def release_mutation(self, token: str) -> None:
        with self._condition:
            if self._active_mutation != token:
                raise RuntimeError("Lance mutation lease owner mismatch")
            self._active_mutation = None
            self._condition.notify_all()

    def acquire_vacuum(self, token: str) -> None:
        with self._condition:
            self._vacuum_waiters += 1
            try:
                self._condition.wait_for(
                    lambda: self._active_vacuum is None and self._active_mutation is None and not self._active_snapshots
                )
                self._active_vacuum = token
            finally:
                self._vacuum_waiters -= 1
                self._condition.notify_all()

    def release_vacuum(self, token: str) -> None:
        with self._condition:
            if self._active_vacuum != token:
                raise RuntimeError("Lance vacuum lease owner mismatch")
            self._active_vacuum = None
            self._condition.notify_all()


_LOCAL_COORDINATORS: dict[str, _LocalDatasetCoordinator] = {}
_LOCAL_COORDINATORS_LOCK = threading.Lock()
_RAY_COORDINATOR_CLASS: Any | None = None


class LanceCommitCleanupError(RuntimeError):
    """A Lance write committed, but releasing its mutation lease failed."""

    committed = True
    safe_to_retry = False

    def __init__(self, uri: str | os.PathLike[str], detail: str) -> None:
        self.uri = os.fspath(uri)
        self.detail = detail
        self.cleanup_warnings = (detail,)
        super().__init__(
            f"Lance write to {self.uri!r} committed, but mutation lease cleanup failed: {detail}. "
            "Do not retry the write."
        )


def _local_coordinator(identity: str) -> _LocalDatasetCoordinator:
    with _LOCAL_COORDINATORS_LOCK:
        return _LOCAL_COORDINATORS.setdefault(identity, _LocalDatasetCoordinator())


def _ray_coordinator_class(ray: Any) -> Any:
    global _RAY_COORDINATOR_CLASS
    if _RAY_COORDINATOR_CLASS is not None:
        return _RAY_COORDINATOR_CLASS

    @ray.remote(max_concurrency=128)
    class RayLanceDatasetCoordinator:
        def __init__(self) -> None:
            self.condition = asyncio.Condition()
            self.mutation_queue: deque[str] = deque()
            self.active_mutation: str | None = None
            self.active_snapshots: set[str] = set()
            self.vacuum_waiters = 0
            self.active_vacuum: str | None = None

        async def acquire_snapshot(self, token: str) -> None:
            async with self.condition:
                await self.condition.wait_for(lambda: self.active_vacuum is None and self.vacuum_waiters == 0)
                self.active_snapshots.add(token)

        async def release_snapshot(self, token: str) -> None:
            async with self.condition:
                self.active_snapshots.discard(token)
                self.condition.notify_all()

        async def acquire_mutation(self, token: str) -> None:
            async with self.condition:
                self.mutation_queue.append(token)
                try:
                    await self.condition.wait_for(
                        lambda: (
                            self.active_vacuum is None
                            and self.active_mutation is None
                            and bool(self.mutation_queue)
                            and self.mutation_queue[0] == token
                        )
                    )
                except BaseException:
                    try:
                        self.mutation_queue.remove(token)
                    except ValueError:
                        pass
                    self.condition.notify_all()
                    raise
                self.mutation_queue.popleft()
                self.active_mutation = token

        async def release_mutation(self, token: str) -> None:
            async with self.condition:
                if self.active_mutation != token:
                    raise RuntimeError("Lance mutation lease owner mismatch")
                self.active_mutation = None
                self.condition.notify_all()

        async def acquire_vacuum(self, token: str) -> None:
            async with self.condition:
                self.vacuum_waiters += 1
                try:
                    await self.condition.wait_for(
                        lambda: (
                            self.active_vacuum is None and self.active_mutation is None and not self.active_snapshots
                        )
                    )
                    self.active_vacuum = token
                finally:
                    self.vacuum_waiters -= 1
                    self.condition.notify_all()

        async def release_vacuum(self, token: str) -> None:
            async with self.condition:
                if self.active_vacuum != token:
                    raise RuntimeError("Lance vacuum lease owner mismatch")
                self.active_vacuum = None
                self.condition.notify_all()

    _RAY_COORDINATOR_CLASS = RayLanceDatasetCoordinator
    return _RAY_COORDINATOR_CLASS


def _configured_runner_type() -> str:
    from vane.runners import get_or_infer_runner_type

    runner_type = get_or_infer_runner_type().strip().lower()
    if runner_type not in {"local", "local-fast", "ray"}:
        raise RuntimeError(f"Unsupported Vane runner type for Lance coordination: {runner_type!r}")
    return runner_type


def _ray_runtime_for_coordinator() -> Any:
    from vane.runners import get_or_create_runner

    runner = get_or_create_runner()
    if getattr(runner, "name", None) != "ray":
        raise RuntimeError("Lance coordinator expected the configured Vane runner to initialize Ray")

    import ray

    if not ray.is_initialized():
        raise RuntimeError("The configured Vane Ray runner did not initialize Ray")
    return ray


class DatasetCoordinator:
    def __init__(self, uri: str | os.PathLike[str]) -> None:
        self.identity = normalize_dataset_uri(uri)
        self._local = _local_coordinator(self.identity)
        self._ray: Any | None = None
        self._ray_runtime: Any | None = None
        self._backend_kind: str | None = None
        self._backend_lock = threading.Lock()

    def _resolve_backend(self) -> Any:
        runner_type = _configured_runner_type()
        requested_kind = "ray" if runner_type == "ray" else "local"
        with self._backend_lock:
            if self._backend_kind is not None:
                if self._backend_kind != requested_kind:
                    raise RuntimeError(
                        "Vane runner changed after the Lance coordinator backend was selected "
                        f"for {self.identity!r}: {self._backend_kind} -> {requested_kind}"
                    )
                if self._backend_kind == "local":
                    return self._local
                if self._ray is None or self._ray_runtime is None:
                    raise RuntimeError("Lance Ray coordinator backend is incomplete")
                return self._ray

            if requested_kind == "local":
                self._backend_kind = "local"
                return self._local

            ray = _ray_runtime_for_coordinator()
            name = "vane-lance-" + hashlib.sha256(self.identity.encode()).hexdigest()[:32]
            actor_class = _ray_coordinator_class(ray)
            try:
                self._ray = ray.get_actor(name, namespace="vane-lance")
            except ValueError:
                self._ray = actor_class.options(
                    name=name,
                    namespace="vane-lance",
                    lifetime="detached",
                    get_if_exists=True,
                ).remote()
            self._ray_runtime = ray
            self._backend_kind = "ray"
            return self._ray

    def _call(self, method: str, token: str, *, backend: Any | None = None) -> Any:
        resolved_backend = self._resolve_backend() if backend is None else backend
        if resolved_backend is self._local:
            getattr(self._local, method)(token)
            return resolved_backend
        if self._ray_runtime is None:
            raise RuntimeError("Lance Ray coordinator runtime is unavailable")
        self._ray_runtime.get(getattr(resolved_backend, method).remote(token))
        return resolved_backend

    def _call_no_wait(self, method: str, token: str, *, backend: Any | None = None) -> None:
        resolved_backend = self._resolve_backend() if backend is None else backend
        if resolved_backend is self._local:
            getattr(self._local, method)(token)
            return
        # Ray actor calls are submitted before remote() returns. Dropping the
        # result reference does not cancel the actor task. This path is used by
        # finalizers, which may run inside Ray's own deserialization thread and
        # therefore must never recursively call ray.get().
        getattr(resolved_backend, method).remote(token)

    @contextmanager
    def snapshot(self) -> Iterator[None]:
        token = str(uuid.uuid4())
        backend = self._call("acquire_snapshot", token)
        try:
            yield
        finally:
            self._call("release_snapshot", token, backend=backend)

    @contextmanager
    def mutation(self) -> Iterator[None]:
        token = str(uuid.uuid4())
        backend = self._call("acquire_mutation", token)
        try:
            yield
        finally:
            self._call("release_mutation", token, backend=backend)

    @contextmanager
    def vacuum(self) -> Iterator[None]:
        token = str(uuid.uuid4())
        backend = self._call("acquire_vacuum", token)
        try:
            yield
        finally:
            self._call("release_vacuum", token, backend=backend)


class _DatasetLease:
    """An idempotent coordinator lease that can be owned by a native relation."""

    def __init__(self, coordinator: DatasetCoordinator | str | os.PathLike[str], kind: str) -> None:
        if kind not in {"snapshot", "mutation"}:
            raise ValueError(f"unsupported Lance dataset lease kind: {kind!r}")
        self._coordinator = (
            coordinator if isinstance(coordinator, DatasetCoordinator) else DatasetCoordinator(coordinator)
        )
        self._kind = kind
        self._token = str(uuid.uuid4())
        self._closed = True
        self._close_lock = threading.Lock()
        self._backend = self._coordinator._call(f"acquire_{kind}", self._token)
        self._closed = False

    def close(self, *, wait: bool = True) -> None:
        with self._close_lock:
            if self._closed:
                return
            method = f"release_{self._kind}"
            if wait:
                self._coordinator._call(method, self._token, backend=self._backend)
            else:
                self._coordinator._call_no_wait(method, self._token, backend=self._backend)
            self._closed = True

    def close_after_commit(self, uri: str | os.PathLike[str]) -> None:
        try:
            self.close()
        except BaseException as exc:
            detail = f"{type(exc).__name__}: {exc}"
            raise LanceCommitCleanupError(uri, detail) from exc

    def __del__(self) -> None:
        try:
            self.close(wait=False)
        except Exception:
            # Interpreter shutdown or a lost Ray coordinator must not turn
            # relation destruction into an unraisable user-visible failure.
            pass


class _TransferredDatasetLease:
    """A plan-transport placeholder that never owns a coordinator lease."""


def _restore_transferred_dataset_lease() -> _TransferredDatasetLease:
    return _TransferredDatasetLease()


class _SnapshotLease(_DatasetLease):
    def __init__(self, coordinator: DatasetCoordinator | str | os.PathLike[str]) -> None:
        super().__init__(coordinator, "snapshot")

    def __reduce_ex__(self, protocol: SupportsIndex) -> tuple[Any, tuple[()]]:
        del protocol
        # The runner keeps the source relation alive until its result stream is
        # closed, so the original driver-side lease remains the sole owner.
        # A copied lease could release the token early; worse, its finalizer can
        # call ray.get() recursively from a Ray deserialization thread.
        return _restore_transferred_dataset_lease, ()


class _MutationLease(_DatasetLease):
    def __init__(self, coordinator: DatasetCoordinator | str | os.PathLike[str]) -> None:
        super().__init__(coordinator, "mutation")
