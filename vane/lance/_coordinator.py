# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import asyncio
import hashlib
import math
import os
import tempfile
import threading
import time
import uuid
from collections import deque
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, SupportsIndex
from urllib.parse import SplitResult, unquote, urlsplit, urlunsplit

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows uses the in-process guard
    fcntl = None  # type: ignore[assignment]


def normalize_dataset_uri(uri: str | os.PathLike[str]) -> str:
    """Return a credential-free identity suitable for coordinator naming."""
    value = os.fspath(uri).strip()
    if not value:
        raise ValueError("Lance dataset URI cannot be empty")
    parsed = urlsplit(value)
    if not parsed.scheme or (len(parsed.scheme) == 1 and value[1:2] == ":"):
        return str(Path(value).expanduser().resolve(strict=False))
    scheme = parsed.scheme.lower()
    hostname = (parsed.hostname or "").lower()
    if scheme == "file" and hostname in {"", "localhost"}:
        return str(Path(unquote(parsed.path)).expanduser().resolve(strict=False))
    port = f":{parsed.port}" if parsed.port is not None else ""
    # Deliberately omit userinfo, query, and fragment so credentials cannot
    # enter actor names, task context, or logs.
    if scheme in {"s3a", "s3n"}:
        scheme = "s3"
    clean = SplitResult(scheme, hostname + port, parsed.path.rstrip("/"), "", "")
    return urlunsplit(clean)


def canonicalize_dataset_uri(uri: str | os.PathLike[str]) -> str:
    """Resolve a local dataset URI once while preserving remote access details."""
    value = os.fspath(uri).strip()
    if not value:
        raise ValueError("Lance dataset URI cannot be empty")
    parsed = urlsplit(value)
    if not parsed.scheme or (len(parsed.scheme) == 1 and value[1:2] == ":"):
        return str(Path(value).expanduser().resolve(strict=False))
    if parsed.scheme.lower() == "file" and (parsed.hostname or "").lower() in {"", "localhost"}:
        return str(Path(unquote(parsed.path)).expanduser().resolve(strict=False))
    return value


class _LocalProcessLocks:
    """Crash-recoverable advisory locks shared by local Vane processes."""

    def __init__(self, identity: str | None) -> None:
        self._identity = identity
        self._handles: dict[str, tuple[list[Any], Path]] = {}
        self._lock = threading.Lock()
        self._directory: Path | None = None
        if identity is not None and fcntl is not None:
            uid = getattr(os, "getuid", lambda: os.getpid())()
            root = Path(
                os.environ.get(
                    "VANE_LANCE_COORDINATOR_DIR",
                    str(Path(tempfile.gettempdir()) / f"vane-lance-coordinator-{uid}"),
                )
            )
            root.mkdir(mode=0o700, parents=True, exist_ok=True)
            self._directory = root / hashlib.sha256(identity.encode()).hexdigest()
            self._directory.mkdir(mode=0o700, exist_ok=True)

    @property
    def supported(self) -> bool:
        return self._directory is not None and fcntl is not None

    def _specs(self, kind: str) -> tuple[tuple[str, int], ...]:
        assert fcntl is not None
        shared = fcntl.LOCK_SH
        exclusive = fcntl.LOCK_EX
        if kind == "snapshot":
            return (("vacuum", shared),)
        if kind == "consistent_snapshot":
            return (("vacuum", shared), ("consistent", shared))
        if kind == "mutation":
            return (("vacuum", shared), ("consistent", exclusive), ("mutation", exclusive))
        if kind == "vacuum":
            return (("vacuum", exclusive),)
        raise ValueError(f"unsupported Lance process lock kind: {kind!r}")

    def acquire(self, kind: str, token: str, timeout: float | None = None) -> None:
        if not self.supported:
            return
        assert self._directory is not None
        assert fcntl is not None
        deadline = None if timeout is None else time.monotonic() + timeout
        handles: list[Any] = []
        try:
            for name, mode in self._specs(kind):
                handle = (self._directory / f"{name}.lock").open("a+b")
                handles.append(handle)
                while True:
                    try:
                        fcntl.flock(handle.fileno(), mode | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        if deadline is not None and time.monotonic() >= deadline:
                            raise TimeoutError(
                                f"timed out acquiring cross-process Lance {kind} lease for {self._identity!r}; "
                                f"current cross-process tokens: {self.active_tokens()}"
                            )
                        time.sleep(0.05)
            with self._lock:
                if token in self._handles:
                    raise RuntimeError("duplicate Lance process lock token")
                record_path = self._directory / f"lease-{hashlib.sha256(token.encode()).hexdigest()}.txt"
                record_path.write_text(token, encoding="utf-8")
                self._handles[token] = (handles, record_path)
        except BaseException:
            for handle in reversed(handles):
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                finally:
                    handle.close()
            raise

    def release(self, token: str, *, require_owner: bool) -> bool:
        if not self.supported:
            return False
        assert fcntl is not None
        with self._lock:
            owned = self._handles.pop(token, None)
        if owned is None:
            if require_owner:
                raise RuntimeError("Lance cross-process lease owner mismatch")
            return False
        handles, record_path = owned
        for handle in reversed(handles):
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()
        record_path.unlink(missing_ok=True)
        return True

    def active_tokens(self) -> list[str]:
        if not self.supported:
            return []
        assert self._directory is not None
        tokens: list[str] = []
        for record_path in self._directory.glob("lease-*.txt"):
            try:
                token = record_path.read_text(encoding="utf-8")
                pid_text = token.rsplit("|pid=", 1)[1].split("|", 1)[0]
                pid = int(pid_text)
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    record_path.unlink(missing_ok=True)
                    continue
                except PermissionError:
                    pass
                tokens.append(token)
            except (OSError, ValueError, IndexError):
                continue
        return sorted(tokens)


def _timeout_deadline(timeout: float | None) -> float | None:
    if timeout is None:
        return None
    value = float(timeout)
    if not math.isfinite(value) or value < 0:
        raise ValueError("Lance coordinator timeout must be a finite non-negative number")
    return time.monotonic() + value


def _remaining_timeout(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    return max(0.0, deadline - time.monotonic())


def _new_lease_token() -> str:
    return f"{uuid.uuid4()}|pid={os.getpid()}|thread={threading.get_ident()}"


def _ray_owner_metadata(ray: Any) -> dict[str, str] | None:
    get_runtime_context = getattr(ray, "get_runtime_context", None)
    if get_runtime_context is None:
        return None
    try:
        context = get_runtime_context()
        owner: dict[str, str] = {
            "pid": str(os.getpid()),
            "thread": str(threading.get_ident()),
        }
        for field, getter_name in (
            ("job_id", "get_job_id"),
            ("actor_id", "get_actor_id"),
            ("node_id", "get_node_id"),
        ):
            getter = getattr(context, getter_name, None)
            if getter is None:
                continue
            value = getter()
            hex_value = getattr(value, "hex", None)
            rendered = hex_value() if callable(hex_value) else str(value)
            if rendered and rendered != "nil":
                owner[field] = rendered
        return owner
    except Exception:
        # Owner metadata is diagnostic. An unavailable Ray context must not
        # weaken or prevent the lease itself.
        return None


class _LocalDatasetCoordinator:
    def __init__(self, identity: str | None = None) -> None:
        self._condition = threading.Condition()
        self._identity = identity
        self._process_locks = _LocalProcessLocks(identity)
        self._mutation_queue: deque[str] = deque()
        self._active_mutation: str | None = None
        self._active_snapshots: set[str] = set()
        self._active_consistent_snapshots: set[str] = set()
        self._consistent_snapshot_waiters = 0
        self._prefer_consistent_snapshots = False
        self._vacuum_waiters = 0
        self._active_vacuum: str | None = None
        self._outcome_unknown_mutations: set[str] = set()
        self._pending_process_locks: set[str] = set()

    def acquire_snapshot(self, token: str, timeout: float | None = None) -> None:
        deadline = _timeout_deadline(timeout)
        with self._condition:
            if not self._condition.wait_for(
                lambda: self._active_vacuum is None and self._vacuum_waiters == 0,
                timeout=_remaining_timeout(deadline),
            ):
                raise TimeoutError(self._timeout_message("snapshot"))
            self._active_snapshots.add(token)
            self._pending_process_locks.add(token)
        try:
            self._process_locks.acquire("snapshot", token, _remaining_timeout(deadline))
        except BaseException:
            with self._condition:
                self._active_snapshots.discard(token)
                self._pending_process_locks.discard(token)
                self._condition.notify_all()
            raise
        with self._condition:
            self._pending_process_locks.discard(token)

    def release_snapshot(self, token: str) -> None:
        with self._condition:
            self._process_locks.release(token, require_owner=False)
            self._active_snapshots.discard(token)
            self._condition.notify_all()

    def acquire_consistent_snapshot(self, token: str, timeout: float | None = None) -> None:
        deadline = _timeout_deadline(timeout)
        with self._condition:
            self._consistent_snapshot_waiters += 1
            self._prefer_consistent_snapshots = True
            try:
                acquired = self._condition.wait_for(
                    lambda: (
                        self._active_vacuum is None
                        and self._vacuum_waiters == 0
                        and self._active_mutation is None
                        and (not self._mutation_queue or self._prefer_consistent_snapshots)
                    ),
                    timeout=_remaining_timeout(deadline),
                )
                if not acquired:
                    raise TimeoutError(self._timeout_message("consistent snapshot"))
                self._active_consistent_snapshots.add(token)
                self._pending_process_locks.add(token)
            finally:
                self._consistent_snapshot_waiters -= 1
                if self._consistent_snapshot_waiters == 0:
                    self._prefer_consistent_snapshots = False
                self._condition.notify_all()
        try:
            self._process_locks.acquire("consistent_snapshot", token, _remaining_timeout(deadline))
        except BaseException:
            with self._condition:
                self._active_consistent_snapshots.discard(token)
                self._pending_process_locks.discard(token)
                self._condition.notify_all()
            raise
        with self._condition:
            self._pending_process_locks.discard(token)

    def release_consistent_snapshot(self, token: str) -> None:
        with self._condition:
            self._process_locks.release(token, require_owner=False)
            self._active_consistent_snapshots.discard(token)
            self._condition.notify_all()

    def acquire_mutation(self, token: str, timeout: float | None = None) -> None:
        deadline = _timeout_deadline(timeout)
        with self._condition:
            self._mutation_queue.append(token)
            try:
                acquired = self._condition.wait_for(
                    lambda: (
                        self._active_vacuum is None
                        and self._vacuum_waiters == 0
                        and self._active_mutation is None
                        and not self._active_consistent_snapshots
                        and self._consistent_snapshot_waiters == 0
                        and self._mutation_queue
                        and self._mutation_queue[0] == token
                    ),
                    timeout=_remaining_timeout(deadline),
                )
                if not acquired:
                    raise TimeoutError(self._timeout_message("mutation"))
            except BaseException:
                try:
                    self._mutation_queue.remove(token)
                except ValueError:
                    pass
                self._condition.notify_all()
                raise
            self._mutation_queue.popleft()
            self._active_mutation = token
            self._pending_process_locks.add(token)
        try:
            self._process_locks.acquire("mutation", token, _remaining_timeout(deadline))
        except BaseException:
            with self._condition:
                if self._active_mutation == token:
                    self._active_mutation = None
                self._pending_process_locks.discard(token)
                if self._consistent_snapshot_waiters:
                    self._prefer_consistent_snapshots = True
                self._condition.notify_all()
            raise
        with self._condition:
            self._pending_process_locks.discard(token)

    def release_mutation(self, token: str) -> None:
        with self._condition:
            if self._active_mutation != token:
                raise RuntimeError("Lance mutation lease owner mismatch")
            self._process_locks.release(token, require_owner=False)
            self._active_mutation = None
            self._outcome_unknown_mutations.discard(token)
            if self._consistent_snapshot_waiters:
                self._prefer_consistent_snapshots = True
            self._condition.notify_all()

    def mark_mutation_outcome_unknown(self, token: str) -> None:
        with self._condition:
            if self._active_mutation != token:
                raise RuntimeError("Lance mutation lease owner mismatch")
            self._outcome_unknown_mutations.add(token)

    def lease_status(self) -> dict[str, Any]:
        with self._condition:
            return {
                "active_mutation": self._active_mutation,
                "outcome_unknown_mutations": sorted(self._outcome_unknown_mutations),
                "active_snapshots": sorted(self._active_snapshots),
                "active_consistent_snapshots": sorted(self._active_consistent_snapshots),
                "active_vacuum": self._active_vacuum,
                "mutation_queue": list(self._mutation_queue),
                "cross_process_locking": self._process_locks.supported,
                "cross_process_tokens": self._process_locks.active_tokens(),
            }

    def _timeout_message(self, kind: str) -> str:
        status = self.lease_status()
        return f"timed out acquiring Lance {kind} lease for {self._identity!r}; current leases: {status}"

    def force_release(self, token: str) -> bool:
        with self._condition:
            if token in self._pending_process_locks:
                raise RuntimeError("cannot force-release a Lance lease while its cross-process lock is pending")
            changed = False
            if self._active_mutation == token:
                self._active_mutation = None
                changed = True
            if self._active_vacuum == token:
                self._active_vacuum = None
                changed = True
            if token in self._active_snapshots:
                self._active_snapshots.remove(token)
                changed = True
            if token in self._active_consistent_snapshots:
                self._active_consistent_snapshots.remove(token)
                changed = True
            try:
                self._mutation_queue.remove(token)
                changed = True
            except ValueError:
                pass
            self._outcome_unknown_mutations.discard(token)
            self._process_locks.release(token, require_owner=False)
            if changed:
                self._condition.notify_all()
            return changed

    def acquire_vacuum(self, token: str, timeout: float | None = None) -> None:
        deadline = _timeout_deadline(timeout)
        with self._condition:
            self._vacuum_waiters += 1
            try:
                acquired = self._condition.wait_for(
                    lambda: (
                        self._active_vacuum is None
                        and self._active_mutation is None
                        and not self._active_snapshots
                        and not self._active_consistent_snapshots
                    ),
                    timeout=_remaining_timeout(deadline),
                )
                if not acquired:
                    raise TimeoutError(self._timeout_message("vacuum"))
                self._active_vacuum = token
                self._pending_process_locks.add(token)
            finally:
                self._vacuum_waiters -= 1
                self._condition.notify_all()
        try:
            self._process_locks.acquire("vacuum", token, _remaining_timeout(deadline))
        except BaseException:
            with self._condition:
                if self._active_vacuum == token:
                    self._active_vacuum = None
                self._pending_process_locks.discard(token)
                self._condition.notify_all()
            raise
        with self._condition:
            self._pending_process_locks.discard(token)

    def release_vacuum(self, token: str) -> None:
        with self._condition:
            if self._active_vacuum != token:
                raise RuntimeError("Lance vacuum lease owner mismatch")
            self._process_locks.release(token, require_owner=False)
            self._active_vacuum = None
            self._condition.notify_all()


_LOCAL_COORDINATORS: dict[str, _LocalDatasetCoordinator] = {}
_LOCAL_COORDINATORS_LOCK = threading.Lock()
_RAY_COORDINATOR_CLASS: Any | None = None


@dataclass(frozen=True)
class _BackendBinding:
    kind: str
    backend: Any
    ray_runtime: Any | None = None


_BACKEND_BINDINGS: dict[str, _BackendBinding] = {}
_BACKEND_BINDINGS_LOCK = threading.Lock()
_RAY_COORDINATOR_KV_NAMESPACE = b"vane-lance"


def _ray_coordinator_marker_key(identity: str) -> bytes:
    digest = hashlib.sha256(identity.encode()).hexdigest()
    return f"coordinator-incarnation/{digest}".encode()


def _ray_coordinator_marker() -> Any:
    from ray.experimental import internal_kv

    return internal_kv


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

    def __reduce__(self) -> tuple[Any, tuple[str, str]]:
        return LanceCommitCleanupError, (self.uri, self.detail)


def _local_coordinator(identity: str) -> _LocalDatasetCoordinator:
    with _LOCAL_COORDINATORS_LOCK:
        return _LOCAL_COORDINATORS.setdefault(identity, _LocalDatasetCoordinator(identity))


def _ray_coordinator_class(ray: Any) -> Any:
    global _RAY_COORDINATOR_CLASS
    if _RAY_COORDINATOR_CLASS is not None:
        return _RAY_COORDINATOR_CLASS

    @ray.remote(
        max_concurrency=128,
        concurrency_groups={"acquire": 127, "release": 1},
    )
    class RayLanceDatasetCoordinator:
        def __init__(self, marker_key: bytes, incarnation: str) -> None:
            self.condition = asyncio.Condition()
            self.acquire_loop: asyncio.AbstractEventLoop | None = None
            self.incarnation = incarnation
            self.unsafe_reason: str | None = None
            self.mutation_queue: deque[str] = deque()
            self.active_mutation: str | None = None
            self.active_snapshots: set[str] = set()
            self.active_consistent_snapshots: set[str] = set()
            self.consistent_snapshot_waiters = 0
            self.prefer_consistent_snapshots = False
            self.vacuum_waiters = 0
            self.active_vacuum: str | None = None
            self.outcome_unknown_mutations: set[str] = set()
            self.lease_owners: dict[str, dict[str, str]] = {}

            runtime_context = ray.get_runtime_context()
            if runtime_context.was_current_actor_reconstructed:
                self.unsafe_reason = (
                    "Lance coordinator actor restarted and lost in-memory lease state; "
                    "coordination is fail-closed until an operator explicitly recovers it"
                )
            marker = _ray_coordinator_marker()
            existing = marker._internal_kv_get(marker_key, namespace=_RAY_COORDINATOR_KV_NAMESPACE)
            if existing is None:
                marker._internal_kv_put(
                    marker_key,
                    incarnation.encode(),
                    overwrite=False,
                    namespace=_RAY_COORDINATOR_KV_NAMESPACE,
                )
                existing = marker._internal_kv_get(marker_key, namespace=_RAY_COORDINATOR_KV_NAMESPACE)
            if existing is None or existing.decode() != incarnation:
                self.unsafe_reason = (
                    "Lance coordinator incarnation changed after prior state existed; "
                    "coordination is fail-closed until an operator explicitly recovers it"
                )

        def _ensure_safe(self) -> None:
            if self.unsafe_reason is not None:
                raise RuntimeError(self.unsafe_reason)

        def _capture_acquire_loop(self) -> None:
            loop = asyncio.get_running_loop()
            if self.acquire_loop is None:
                self.acquire_loop = loop
            elif self.acquire_loop is not loop:
                raise RuntimeError("Lance coordinator acquire methods changed event loops")

        async def _run_on_acquire_loop(self, operation: Any) -> None:
            if self.acquire_loop is None:
                raise RuntimeError("Lance coordinator release has no matching acquire loop")
            if self.acquire_loop is asyncio.get_running_loop():
                await operation
                return
            future = asyncio.run_coroutine_threadsafe(operation, self.acquire_loop)
            await asyncio.wrap_future(future)

        def _status_unlocked(self) -> dict[str, Any]:
            return {
                "incarnation": self.incarnation,
                "unsafe_reason": self.unsafe_reason,
                "active_mutation": self.active_mutation,
                "outcome_unknown_mutations": sorted(self.outcome_unknown_mutations),
                "active_snapshots": sorted(self.active_snapshots),
                "active_consistent_snapshots": sorted(self.active_consistent_snapshots),
                "active_vacuum": self.active_vacuum,
                "mutation_queue": list(self.mutation_queue),
                "lease_owners": dict(self.lease_owners),
            }

        async def _wait_for(self, predicate: Any, timeout: float | None, kind: str) -> None:
            try:
                if timeout is None:
                    await self.condition.wait_for(predicate)
                else:
                    await asyncio.wait_for(self.condition.wait_for(predicate), timeout)
            except asyncio.TimeoutError as exc:
                raise TimeoutError(
                    f"timed out acquiring Lance {kind} lease; current leases: {self._status_unlocked()}"
                ) from exc

        @ray.method(concurrency_group="acquire")
        async def acquire_snapshot(
            self,
            token: str,
            timeout: float | None = None,
            owner: dict[str, str] | None = None,
        ) -> None:
            self._ensure_safe()
            self._capture_acquire_loop()
            async with self.condition:
                await self._wait_for(
                    lambda: self.active_vacuum is None and self.vacuum_waiters == 0,
                    timeout,
                    "snapshot",
                )
                self.active_snapshots.add(token)
                if owner is not None:
                    self.lease_owners[token] = owner

        @ray.method(concurrency_group="release")
        async def release_snapshot(self, token: str) -> None:
            self._ensure_safe()
            await self._run_on_acquire_loop(self._release_snapshot(token))

        async def _release_snapshot(self, token: str) -> None:
            async with self.condition:
                self.active_snapshots.discard(token)
                self.lease_owners.pop(token, None)
                self.condition.notify_all()

        @ray.method(concurrency_group="acquire")
        async def acquire_consistent_snapshot(
            self,
            token: str,
            timeout: float | None = None,
            owner: dict[str, str] | None = None,
        ) -> None:
            self._ensure_safe()
            self._capture_acquire_loop()
            async with self.condition:
                self.consistent_snapshot_waiters += 1
                self.prefer_consistent_snapshots = True
                try:
                    await self._wait_for(
                        lambda: (
                            self.active_vacuum is None
                            and self.vacuum_waiters == 0
                            and self.active_mutation is None
                            and (not self.mutation_queue or self.prefer_consistent_snapshots)
                        ),
                        timeout,
                        "consistent snapshot",
                    )
                    self.active_consistent_snapshots.add(token)
                    if owner is not None:
                        self.lease_owners[token] = owner
                finally:
                    self.consistent_snapshot_waiters -= 1
                    if self.consistent_snapshot_waiters == 0:
                        self.prefer_consistent_snapshots = False
                    self.condition.notify_all()

        @ray.method(concurrency_group="release")
        async def release_consistent_snapshot(self, token: str) -> None:
            self._ensure_safe()
            await self._run_on_acquire_loop(self._release_consistent_snapshot(token))

        async def _release_consistent_snapshot(self, token: str) -> None:
            async with self.condition:
                self.active_consistent_snapshots.discard(token)
                self.lease_owners.pop(token, None)
                self.condition.notify_all()

        @ray.method(concurrency_group="acquire")
        async def acquire_mutation(
            self,
            token: str,
            timeout: float | None = None,
            owner: dict[str, str] | None = None,
        ) -> None:
            self._ensure_safe()
            self._capture_acquire_loop()
            async with self.condition:
                self.mutation_queue.append(token)
                try:
                    await self._wait_for(
                        lambda: (
                            self.active_vacuum is None
                            and self.vacuum_waiters == 0
                            and self.active_mutation is None
                            and not self.active_consistent_snapshots
                            and self.consistent_snapshot_waiters == 0
                            and bool(self.mutation_queue)
                            and self.mutation_queue[0] == token
                        ),
                        timeout,
                        "mutation",
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
                if owner is not None:
                    self.lease_owners[token] = owner

        @ray.method(concurrency_group="release")
        async def release_mutation(self, token: str) -> None:
            self._ensure_safe()
            await self._run_on_acquire_loop(self._release_mutation(token))

        async def _release_mutation(self, token: str) -> None:
            async with self.condition:
                if self.active_mutation != token:
                    raise RuntimeError("Lance mutation lease owner mismatch")
                self.active_mutation = None
                self.outcome_unknown_mutations.discard(token)
                self.lease_owners.pop(token, None)
                if self.consistent_snapshot_waiters:
                    self.prefer_consistent_snapshots = True
                self.condition.notify_all()

        @ray.method(concurrency_group="release")
        async def mark_mutation_outcome_unknown(self, token: str) -> None:
            self._ensure_safe()
            await self._run_on_acquire_loop(self._mark_mutation_outcome_unknown(token))

        async def _mark_mutation_outcome_unknown(self, token: str) -> None:
            async with self.condition:
                if self.active_mutation != token:
                    raise RuntimeError("Lance mutation lease owner mismatch")
                self.outcome_unknown_mutations.add(token)

        @ray.method(concurrency_group="release")
        async def lease_status(self) -> dict[str, Any]:
            async def read_status() -> dict[str, Any]:
                async with self.condition:
                    return self._status_unlocked()

            if self.acquire_loop is None:
                return {
                    "incarnation": self.incarnation,
                    "unsafe_reason": self.unsafe_reason,
                    "active_mutation": None,
                    "outcome_unknown_mutations": [],
                    "active_snapshots": [],
                    "active_consistent_snapshots": [],
                    "active_vacuum": None,
                    "mutation_queue": [],
                    "lease_owners": {},
                }
            if self.acquire_loop is asyncio.get_running_loop():
                return await read_status()
            future = asyncio.run_coroutine_threadsafe(read_status(), self.acquire_loop)
            return await asyncio.wrap_future(future)

        @ray.method(concurrency_group="release")
        async def force_release(self, token: str) -> bool:
            async def release() -> bool:
                async with self.condition:
                    changed = False
                    if self.active_mutation == token:
                        self.active_mutation = None
                        changed = True
                    if self.active_vacuum == token:
                        self.active_vacuum = None
                        changed = True
                    if token in self.active_snapshots:
                        self.active_snapshots.remove(token)
                        changed = True
                    if token in self.active_consistent_snapshots:
                        self.active_consistent_snapshots.remove(token)
                        changed = True
                    try:
                        self.mutation_queue.remove(token)
                        changed = True
                    except ValueError:
                        pass
                    self.outcome_unknown_mutations.discard(token)
                    self.lease_owners.pop(token, None)
                    if changed:
                        self.condition.notify_all()
                    return changed

            if self.acquire_loop is None:
                return False
            if self.acquire_loop is asyncio.get_running_loop():
                return await release()
            future = asyncio.run_coroutine_threadsafe(release(), self.acquire_loop)
            return await asyncio.wrap_future(future)

        @ray.method(concurrency_group="acquire")
        async def acquire_vacuum(
            self,
            token: str,
            timeout: float | None = None,
            owner: dict[str, str] | None = None,
        ) -> None:
            self._ensure_safe()
            self._capture_acquire_loop()
            async with self.condition:
                self.vacuum_waiters += 1
                try:
                    await self._wait_for(
                        lambda: (
                            self.active_vacuum is None
                            and self.active_mutation is None
                            and not self.active_snapshots
                            and not self.active_consistent_snapshots
                        ),
                        timeout,
                        "vacuum",
                    )
                    self.active_vacuum = token
                    if owner is not None:
                        self.lease_owners[token] = owner
                finally:
                    self.vacuum_waiters -= 1
                    self.condition.notify_all()

        @ray.method(concurrency_group="release")
        async def release_vacuum(self, token: str) -> None:
            self._ensure_safe()
            await self._run_on_acquire_loop(self._release_vacuum(token))

        async def _release_vacuum(self, token: str) -> None:
            async with self.condition:
                if self.active_vacuum != token:
                    raise RuntimeError("Lance vacuum lease owner mismatch")
                self.active_vacuum = None
                self.lease_owners.pop(token, None)
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
        with self._backend_lock, _BACKEND_BINDINGS_LOCK:
            binding = _BACKEND_BINDINGS.get(self.identity)
            if binding is not None:
                if binding.kind != requested_kind:
                    raise RuntimeError(
                        "Vane runner changed after the Lance coordinator backend was selected "
                        f"for {self.identity!r}: {binding.kind} -> {requested_kind}. "
                        "Release all Lance relations and leases before changing runners."
                    )
                self._backend_kind = binding.kind
                if binding.kind == "local":
                    return binding.backend
                self._ray = binding.backend
                self._ray_runtime = binding.ray_runtime
                if self._ray_runtime is None:
                    raise RuntimeError("Lance Ray coordinator backend is incomplete")
                return self._ray

            if requested_kind == "local":
                binding = _BackendBinding("local", self._local)
                _BACKEND_BINDINGS[self.identity] = binding
                self._backend_kind = "local"
                return binding.backend

            ray = _ray_runtime_for_coordinator()
            name = "vane-lance-" + hashlib.sha256(self.identity.encode()).hexdigest()[:32]
            actor_class = _ray_coordinator_class(ray)
            try:
                actor = ray.get_actor(name, namespace="vane-lance")
            except ValueError:
                marker_key = _ray_coordinator_marker_key(self.identity)
                marker = _ray_coordinator_marker()._internal_kv_get(marker_key, namespace=_RAY_COORDINATOR_KV_NAMESPACE)
                if marker is not None:
                    raise RuntimeError(
                        "The Lance coordinator actor disappeared while a prior incarnation marker still exists for "
                        f"{self.identity!r}. Coordination is fail-closed; inspect outstanding work and call "
                        "recover_ray_dataset_coordinator() explicitly before reopening this dataset."
                    )
                incarnation = str(uuid.uuid4())
                actor = actor_class.options(
                    name=name,
                    namespace="vane-lance",
                    lifetime="detached",
                    get_if_exists=True,
                    max_restarts=-1,
                    max_task_retries=-1,
                ).remote(marker_key, incarnation)
            binding = _BackendBinding("ray", actor, ray)
            _BACKEND_BINDINGS[self.identity] = binding
            self._ray = actor
            self._ray_runtime = ray
            self._backend_kind = "ray"
            return actor

    def _call(
        self,
        method: str,
        token: str,
        *,
        backend: Any | None = None,
        timeout: float | None = None,
    ) -> Any:
        if method.startswith("acquire_"):
            _timeout_deadline(timeout)
        resolved_backend = self._resolve_backend() if backend is None else backend
        if resolved_backend is self._local:
            if method.startswith("acquire_"):
                if timeout is None:
                    getattr(self._local, method)(token)
                else:
                    getattr(self._local, method)(token, timeout)
            else:
                getattr(self._local, method)(token)
            return resolved_backend
        if self._ray_runtime is None:
            raise RuntimeError("Lance Ray coordinator runtime is unavailable")
        remote_method = getattr(resolved_backend, method)
        owner = _ray_owner_metadata(self._ray_runtime) if method.startswith("acquire_") else None
        if owner is not None:
            reference = remote_method.remote(token, timeout, owner)
        elif method.startswith("acquire_") and timeout is not None:
            reference = remote_method.remote(token, timeout)
        else:
            reference = remote_method.remote(token)
        self._ray_runtime.get(reference)
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

    def lease_status(self) -> dict[str, Any]:
        backend = self._resolve_backend()
        if backend is self._local:
            return self._local.lease_status()
        if self._ray_runtime is None:
            raise RuntimeError("Lance Ray coordinator runtime is unavailable")
        return self._ray_runtime.get(backend.lease_status.remote())

    def force_release(self, token: str) -> bool:
        """Release one exact token selected from :meth:`lease_status`."""
        backend = self._resolve_backend()
        if backend is self._local:
            return self._local.force_release(token)
        if self._ray_runtime is None:
            raise RuntimeError("Lance Ray coordinator runtime is unavailable")
        return bool(self._ray_runtime.get(backend.force_release.remote(token)))

    @contextmanager
    def snapshot(self, *, timeout: float | None = None) -> Iterator[None]:
        token = _new_lease_token()
        backend = self._call("acquire_snapshot", token, timeout=timeout)
        try:
            yield
        finally:
            self._call("release_snapshot", token, backend=backend)

    @contextmanager
    def consistent_snapshot(self, *, timeout: float | None = None) -> Iterator[None]:
        token = _new_lease_token()
        backend = self._call("acquire_consistent_snapshot", token, timeout=timeout)
        try:
            yield
        finally:
            self._call("release_consistent_snapshot", token, backend=backend)

    @contextmanager
    def mutation(self, *, timeout: float | None = None) -> Iterator[None]:
        token = _new_lease_token()
        backend = self._call("acquire_mutation", token, timeout=timeout)
        try:
            yield
        finally:
            self._call("release_mutation", token, backend=backend)

    @contextmanager
    def vacuum(self, *, timeout: float | None = None) -> Iterator[None]:
        token = _new_lease_token()
        backend = self._call("acquire_vacuum", token, timeout=timeout)
        try:
            yield
        finally:
            self._call("release_vacuum", token, backend=backend)


def recover_ray_dataset_coordinator(
    uri: str | os.PathLike[str], *, expected_identity: str, force: bool = False
) -> dict[str, Any]:
    """Explicitly reset a lost Ray coordinator after operator reconciliation.

    ``expected_identity`` must exactly match the normalized identity so a
    caller cannot accidentally reset the wrong dataset. An actor with live
    leases is never reset unless ``force=True``.
    """
    identity = normalize_dataset_uri(uri)
    if expected_identity != identity:
        raise ValueError(
            "expected_identity does not match the normalized Lance dataset identity: "
            f"{expected_identity!r} != {identity!r}"
        )
    ray = _ray_runtime_for_coordinator()
    name = "vane-lance-" + hashlib.sha256(identity.encode()).hexdigest()[:32]
    actor: Any | None
    status: dict[str, Any]
    try:
        actor = ray.get_actor(name, namespace="vane-lance")
    except ValueError:
        actor = None
        status = {"actor_found": False}
    else:
        status = dict(ray.get(actor.lease_status.remote()))
        status["actor_found"] = True
        has_live_state = any(
            (
                status.get("active_mutation"),
                status.get("active_snapshots"),
                status.get("active_consistent_snapshots"),
                status.get("active_vacuum"),
                status.get("mutation_queue"),
            )
        )
        if has_live_state and not force:
            raise RuntimeError(
                "The Lance coordinator still reports live leases. Reconcile their owners first, release an exact "
                "token with DatasetCoordinator.force_release(), or repeat with force=True only after proving no "
                "writer or reader remains active."
            )
        ray.kill(actor, no_restart=True)

    marker_key = _ray_coordinator_marker_key(identity)
    _ray_coordinator_marker()._internal_kv_del(marker_key, namespace=_RAY_COORDINATOR_KV_NAMESPACE)
    with _BACKEND_BINDINGS_LOCK:
        _BACKEND_BINDINGS.pop(identity, None)
    status["recovered_identity"] = identity
    return status


class _DatasetLease:
    """An idempotent coordinator lease that can be owned by a native relation."""

    def __init__(self, coordinator: DatasetCoordinator | str | os.PathLike[str], kind: str) -> None:
        if kind not in {"snapshot", "consistent_snapshot", "mutation"}:
            raise ValueError(f"unsupported Lance dataset lease kind: {kind!r}")
        self._coordinator = (
            coordinator if isinstance(coordinator, DatasetCoordinator) else DatasetCoordinator(coordinator)
        )
        self._kind = kind
        self._token = _new_lease_token()
        self._closed = True
        self._retained_after_outcome_unknown = False
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

    def retain_after_outcome_unknown(self) -> None:
        """Leave a mutation lease held when the writer may still be committing."""
        if self._kind != "mutation":
            raise RuntimeError("only a mutation lease can be retained after an outcome-unknown write")
        with self._close_lock:
            if self._closed:
                return
            # Mark local ownership closed before the best-effort diagnostic RPC:
            # neither an RPC failure nor object finalization may release this
            # lease while the remote commit can still be running.
            self._closed = True
            self._retained_after_outcome_unknown = True
            try:
                self._coordinator._call("mark_mutation_outcome_unknown", self._token, backend=self._backend)
            except BaseException:
                pass

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


class _ConsistentSnapshotLease(_DatasetLease):
    def __init__(self, coordinator: DatasetCoordinator | str | os.PathLike[str]) -> None:
        super().__init__(coordinator, "consistent_snapshot")

    def __reduce_ex__(self, protocol: SupportsIndex) -> tuple[Any, tuple[()]]:
        del protocol
        return _restore_transferred_dataset_lease, ()


class _MutationLease(_DatasetLease):
    def __init__(self, coordinator: DatasetCoordinator | str | os.PathLike[str]) -> None:
        super().__init__(coordinator, "mutation")
