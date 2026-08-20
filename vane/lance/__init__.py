# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import math
import os
import re
from collections.abc import Iterator, Mapping, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from typing import Any

from vane.lance._coordinator import (
    DatasetCoordinator,
    LanceCommitCleanupError,
    _ConsistentSnapshotLease,
    _MutationLease,
    _SnapshotLease,
    canonicalize_dataset_uri,
    normalize_dataset_uri,
    recover_ray_dataset_coordinator,
)
from vane.runners.copy_outcome import CopyOutcomeUnknownError


class LanceCommitOutcomeUnknownError(CopyOutcomeUnknownError):
    """A Lance writer may have committed; repeating it is unsafe."""


def _retained_lease_detail(exc: CopyOutcomeUnknownError, identity: str) -> str:
    suffix = (
        f"The mutation lease for {identity!r} remains held while the commit is reconciled; "
        "inspect DatasetCoordinator(identity).lease_status() and release only the exact token after proving the "
        "writer stopped."
    )
    return f"{exc.detail} {suffix}".strip()


def _connection(connection: Any | None) -> Any:
    if connection is not None:
        return connection
    import vane

    return vane.default_connection()


def _quote_identifier(value: str) -> str:
    parts = value.split(".")
    if any(not part for part in parts):
        raise ValueError(f"invalid SQL identifier: {value!r}")
    return ".".join('"' + part.replace('"', '""') + '"' for part in parts)


def _quote_identifier_part(value: str) -> str:
    if not value:
        raise ValueError("SQL identifier cannot be empty")
    return '"' + value.replace('"', '""') + '"'


def _bare_identifier(
    value: str,
    *,
    label: str = "SQL identifier",
    allow_qualified: bool = True,
) -> str:
    parts = value.split(".")
    if (
        not parts
        or (not allow_qualified and len(parts) != 1)
        or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part) for part in parts)
    ):
        raise ValueError(f"invalid {label}: {value!r}")
    return ".".join(parts)


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _option_sql(options: Mapping[str, Any]) -> str:
    rendered: list[str] = []
    for key, value in options.items():
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise ValueError(f"invalid Lance option name: {key!r}")
        if isinstance(value, bool):
            literal = "true" if value else "false"
        elif isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError(f"Lance option {key!r} must be finite")
            literal = str(value)
        elif isinstance(value, int):
            literal = str(value)
        else:
            literal = _quote_literal(str(value))
        rendered.append(f"{key} = {literal}")
    return ", ".join(rendered)


def _require_autocommit(connection: Any, operation: str) -> None:
    if not connection._is_auto_commit():
        raise RuntimeError(
            f"{operation} does not support explicit transactions; commit or roll back the transaction first"
        )


def _relation_sql(source: Any, connection: Any, operation: str) -> str:
    if isinstance(source, str):
        if not source:
            raise ValueError(f"{operation} source SQL cannot be empty")
        return source
    shares_connection = getattr(source, "_shares_connection", None)
    if shares_connection is None or not callable(shares_connection):
        raise TypeError(f"{operation} source must be a SQL string or Vane relation")
    if not shares_connection(connection):
        raise ValueError(
            f"{operation} source relation belongs to a different connection; "
            "materialize it or recreate it on the target Lance connection"
        )
    source_sql = source.sql_query()
    if not source_sql:
        raise ValueError(f"{operation} source relation is not serializable; provide a SELECT SQL string")
    return source_sql


class LanceDataset:
    def __init__(self, uri: str | os.PathLike[str], connection: Any | None = None) -> None:
        self.uri = canonicalize_dataset_uri(uri)
        self.connection = _connection(connection)
        self.coordinator = DatasetCoordinator(self.uri)

    def scan(self) -> Any:
        return self.connection.read_lance(self.uri)

    def _scan_without_lease(self) -> Any:
        return self.connection.table_function("__lance_scan", [self.uri])

    def _new_snapshot_lease(self) -> _SnapshotLease | _ConsistentSnapshotLease:
        return _SnapshotLease(self.coordinator)

    def _snapshot_context(self) -> Any:
        return self.coordinator.snapshot()

    def _snapshot_relation(self, factory: Any) -> Any:
        lease = self._new_snapshot_lease()
        try:
            relation = factory()
            relation._attach_lance_snapshot_lease(lease)
        except BaseException:
            lease.close()
            raise
        return relation

    def _scan_reference(self) -> str:
        return self.uri

    def _sql_target(self) -> str:
        return _quote_literal(self.uri)

    def _lance_command_target(self) -> str:
        return self._sql_target()

    @contextmanager
    def _mutation(self, operation: str) -> Iterator[None]:
        _require_autocommit(self.connection, operation)
        lease = _MutationLease(self.coordinator)
        try:
            yield
        except CopyOutcomeUnknownError:
            lease.retain_after_outcome_unknown()
            raise
        except BaseException:
            lease.close()
            raise
        else:
            lease.close_after_commit(self.uri)

    @contextmanager
    def _vacuum(self, operation: str, *, timeout: float | None = None) -> Iterator[None]:
        _require_autocommit(self.connection, operation)
        with self.coordinator.vacuum(timeout=timeout):
            yield

    @contextmanager
    def snapshot(self) -> Iterator[Any]:
        """Return a relation whose lease remains valid if it escapes this block."""
        with self._snapshot_context():
            yield self._snapshot_relation(self._scan_without_lease)

    def vector_search(
        self,
        vector_column: str,
        query: Sequence[float],
        *,
        k: int = 10,
        nprobes: int | None = None,
        refine_factor: int | None = None,
        prefilter: bool = False,
        use_index: bool = True,
        filter: str | None = None,
    ) -> Any:
        params: list[Any] = [self._scan_reference(), vector_column, list(query)]
        named: dict[str, Any] = {"k": k, "prefilter": prefilter, "use_index": use_index}
        if nprobes is not None:
            named["nprobs"] = nprobes
        if refine_factor is not None:
            named["refine_factor"] = refine_factor
        if filter is not None:
            named["filter"] = filter
        return self._snapshot_relation(
            lambda: self.connection.table_function("lance_vector_search", params, named_parameters=named)
        )

    def fts(
        self,
        text_column: str,
        query: str,
        *,
        k: int = 10,
        prefilter: bool = False,
        filter: str | None = None,
    ) -> Any:
        params: list[Any] = [self._scan_reference(), text_column, query]
        named: dict[str, Any] = {"k": k, "prefilter": prefilter}
        if filter is not None:
            named["filter"] = filter
        return self._snapshot_relation(
            lambda: self.connection.table_function("lance_fts", params, named_parameters=named)
        )

    def hybrid_search(
        self,
        vector_column: str,
        vector_query: Sequence[float],
        text_column: str,
        text_query: str,
        *,
        k: int = 10,
        nprobes: int | None = None,
        refine_factor: int | None = None,
        prefilter: bool = False,
        use_index: bool = True,
        alpha: float = 0.5,
        oversample_factor: int = 4,
    ) -> Any:
        named: dict[str, Any] = {
            "k": k,
            "prefilter": prefilter,
            "use_index": use_index,
            "alpha": alpha,
            "oversample_factor": oversample_factor,
        }
        params: list[Any] = [
            self._scan_reference(),
            vector_column,
            list(vector_query),
            text_column,
            text_query,
        ]
        if nprobes is not None:
            named["nprobs"] = nprobes
        if refine_factor is not None:
            named["refine_factor"] = refine_factor
        return self._snapshot_relation(
            lambda: self.connection.table_function("lance_hybrid_search", params, named_parameters=named)
        )

    def write(self, relation: Any, *, mode: str = "create", **options: Any) -> None:
        if isinstance(relation, str):
            raise TypeError("Lance dataset writes require a Vane relation, not SQL text")
        supported_options = {
            "data_storage_version",
            "max_bytes_per_file",
            "max_rows_per_file",
            "max_rows_per_group",
        }
        unsupported_options = set(options).difference(supported_options)
        if unsupported_options:
            rendered = ", ".join(sorted(unsupported_options))
            raise ValueError(f"Lance dataset writes do not support these writer options: {rendered}")
        _relation_sql(relation, self.connection, "Lance dataset write")
        _require_autocommit(self.connection, "Lance dataset writes")
        try:
            # The native public method owns the dataset mutation lease so direct
            # relation.write_lance() calls and this convenience API share FIFO.
            relation.write_lance(self.uri, mode=mode, **options)
        except CopyOutcomeUnknownError as exc:
            raise LanceCommitOutcomeUnknownError(
                exc.operation_id,
                exc.base_path,
                exc.run_id,
                exc.manifest_path,
                exc.committed_marker_path,
                _retained_lease_detail(exc, self.coordinator.identity),
                exc.cleanup_warnings,
            ) from exc

    def create_index(self, name: str, column: str, *, index_type: str, **options: Any) -> None:
        sql = (
            f"CREATE INDEX {_bare_identifier(name, label='Lance index name', allow_qualified=False)} "
            f"ON {self._lance_command_target()} "
            f"({_bare_identifier(column, label='Lance index column')}) "
            f"USING {_bare_identifier(index_type, label='Lance index type', allow_qualified=False)}"
        )
        if options:
            sql += " WITH (" + _option_sql(options) + ")"
        with self._mutation("Lance index creation"):
            self.connection.execute(sql)

    def show_indexes(self) -> Any:
        return self.connection.sql(f"SHOW INDEXES ON {self._lance_command_target()}")

    def drop_index(self, name: str) -> None:
        with self._mutation("Lance index removal"):
            self.connection.execute(
                f"DROP INDEX {_bare_identifier(name, label='Lance index name', allow_qualified=False)} "
                f"ON {self._lance_command_target()}"
            )

    def optimize(self, **options: Any) -> Any:
        sql = f"OPTIMIZE {self._lance_command_target()}"
        if options:
            sql += " WITH (" + _option_sql(options) + ")"
        with self._mutation("Lance optimization"):
            return self.connection.sql(sql).fetchall()

    def vacuum(self, **options: Any) -> Any:
        sql = f"VACUUM LANCE {self._lance_command_target()}"
        if options:
            sql += " WITH (" + _option_sql(options) + ")"
        with self._vacuum("Lance vacuum"):
            return self.connection.sql(sql).fetchall()


class LanceNamespace:
    def __init__(
        self,
        namespace_id: str | os.PathLike[str],
        alias: str,
        *,
        endpoint: str | None = None,
        read_only: bool = False,
        connection: Any | None = None,
        attach: bool = True,
    ) -> None:
        raw_namespace_id = os.fspath(namespace_id)
        if endpoint is not None:
            endpoint = endpoint.strip()
            if not endpoint:
                raise ValueError("Lance namespace endpoint cannot be empty")
        self.namespace_id = raw_namespace_id if endpoint is not None else canonicalize_dataset_uri(raw_namespace_id)
        self.alias = alias
        self.endpoint = endpoint
        self.read_only = read_only
        self.connection = _connection(connection)
        if attach:
            options = ["TYPE LANCE", f"READ_ONLY {'true' if read_only else 'false'}"]
            if endpoint is not None:
                options.append("ENDPOINT " + _quote_literal(endpoint))
            self.connection.execute(
                f"ATTACH {_quote_literal(self.namespace_id)} AS {_quote_identifier_part(alias)} ({', '.join(options)})"
            )

    def table(self, name: str, schema: str = "main") -> LanceTable:
        return LanceTable(self, name, schema=schema)

    def _attachment_is_read_only(self) -> bool:
        row = self.connection.execute(
            "SELECT readonly FROM duckdb_databases() WHERE lower(database_name) = lower(?)",
            [self.alias],
        ).fetchone()
        return bool(row[0]) if row is not None else self.read_only

    def create_table(
        self,
        name: str,
        source: Any,
        *,
        schema: str = "main",
        if_not_exists: bool = False,
    ) -> LanceTable:
        table = self.table(name, schema=schema)
        source_sql = _relation_sql(source, self.connection, "CREATE TABLE")
        guard = " IF NOT EXISTS" if if_not_exists else ""
        with table._mutation("Lance table creation"):
            self.connection.execute(f"CREATE TABLE{guard} {table._sql_target()} AS {source_sql}")
        return table

    def drop_table(
        self,
        name: str,
        *,
        schema: str = "main",
        if_exists: bool = False,
    ) -> None:
        table = self.table(name, schema=schema)
        guard = " IF EXISTS" if if_exists else ""
        # A directory table drop removes files used by active fixed snapshots.
        # The vacuum lease waits for those readers and blocks new ones.
        with table._vacuum("Lance table removal"):
            self.connection.execute(f"DROP TABLE{guard} {table._sql_target()}")

    def detach(self, *, timeout: float | None = 30.0) -> None:
        """Detach after all attached-table readers finish.

        A bounded default prevents a relation retained by the caller from
        turning a same-thread detach into an unobservable permanent wait.
        Pass ``timeout=None`` only when another thread or process is known to
        release every outstanding scan.
        """
        _require_autocommit(self.connection, "Lance namespace detach")
        rows = self.connection.execute(
            """
            SELECT schema_name, table_name
            FROM duckdb_tables()
            WHERE lower(database_name) = lower(?)
            ORDER BY lower(schema_name), lower(table_name), schema_name, table_name
            """,
            [self.alias],
        ).fetchall()
        with ExitStack() as leases:
            for schema, name in rows:
                leases.enter_context(self.table(name, schema=schema)._vacuum("Lance namespace detach", timeout=timeout))
            self.connection.execute(f"DETACH {_quote_identifier_part(self.alias)}")


class LanceTable(LanceDataset):
    def __init__(self, namespace: LanceNamespace, name: str, *, schema: str = "main") -> None:
        self.namespace = namespace
        self.name = name
        self.schema = schema
        self.qualified_name = f"{namespace.alias}.{schema}.{name}"
        self._quoted_name = ".".join(_quote_identifier_part(part) for part in (namespace.alias, schema, name))
        if namespace.endpoint is not None:
            identity = (
                namespace.endpoint.rstrip("/")
                + "/"
                + namespace.namespace_id.strip("/")
                + "/"
                + schema.casefold()
                + "/"
                + name.casefold()
            )
        else:
            identity = (
                normalize_dataset_uri(namespace.namespace_id).rstrip("/")
                + "/"
                + schema.casefold()
                + "/"
                + name.casefold()
                + ".lance"
            )
        super().__init__(identity, namespace.connection)

    def scan(self) -> Any:
        return self._snapshot_relation(self._scan_without_lease)

    def _scan_without_lease(self) -> Any:
        return self.connection.table(self._quoted_name)

    def _new_snapshot_lease(self) -> _SnapshotLease | _ConsistentSnapshotLease:
        # The extension pins REST query_table requests to a concrete table
        # version, so REST reads can use the same snapshot/vacuum exclusion as
        # directory datasets without blocking ordinary mutations.
        return super()._new_snapshot_lease()

    def _snapshot_context(self) -> Any:
        return super()._snapshot_context()

    def _scan_reference(self) -> str:
        return self._quoted_name

    def _sql_target(self) -> str:
        return self._quoted_name

    def _lance_command_target(self) -> str:
        return self._quoted_name

    def _existing_schema(self) -> tuple[list[str], list[Any]] | None:
        exists = self.connection.execute(
            """
            SELECT 1
            FROM duckdb_tables()
            WHERE lower(database_name) = lower(?)
              AND lower(schema_name) = lower(?)
              AND lower(table_name) = lower(?)
            LIMIT 1
            """,
            [self.namespace.alias, self.schema, self.name],
        ).fetchone()
        if exists is None:
            return None
        relation = self.connection.table(self._quoted_name)
        return relation.columns, relation.types

    def _reject_unsupported_schema_overwrite(self, source_sql: str) -> None:
        existing_schema = self._existing_schema()
        if existing_schema is None:
            return
        source = self.connection.sql(source_sql)
        source_schema = (source.columns, source.types)
        if source_schema != existing_schema:
            raise NotImplementedError(
                "Lance attached-table overwrite does not support schema changes; "
                "drop and recreate the table through LanceNamespace instead"
            )

    def write(self, relation: Any, *, mode: str = "create", **options: Any) -> None:
        if self.namespace._attachment_is_read_only():
            raise PermissionError(f"cannot write Lance table {self.qualified_name!r}: attachment is read-only")
        normalized_mode = mode.strip().lower()
        if normalized_mode not in {"create", "append", "overwrite"}:
            raise ValueError("Lance table write mode must be one of create, append, or overwrite")
        source_sql = _relation_sql(relation, self.connection, "Lance table write")
        if normalized_mode == "append" and options:
            raise ValueError("Lance table append does not support writer options")
        unsupported_options = set(options).difference({"data_storage_version"})
        if unsupported_options:
            rendered = ", ".join(sorted(unsupported_options))
            raise ValueError(f"Lance attached-table writes do not support these writer options: {rendered}")

        if normalized_mode == "append":
            sql = f"INSERT INTO {self._sql_target()} {source_sql}"
        else:
            replace = " OR REPLACE" if normalized_mode == "overwrite" else ""
            option_sql = " WITH (" + _option_sql(options) + ")" if options else ""
            sql = f"CREATE{replace} TABLE {self._sql_target()}{option_sql} AS {source_sql}"
        try:
            with self._mutation("Lance table writes"):
                if normalized_mode == "overwrite":
                    self._reject_unsupported_schema_overwrite(source_sql)
                self.connection.execute(sql)
        except CopyOutcomeUnknownError as exc:
            raise LanceCommitOutcomeUnknownError(
                exc.operation_id,
                exc.base_path,
                exc.run_id,
                exc.manifest_path,
                exc.committed_marker_path,
                _retained_lease_detail(exc, self.coordinator.identity),
                exc.cleanup_warnings,
            ) from exc

    def insert(self, source: Any, *, columns: Sequence[str] | None = None) -> None:
        source_sql = _relation_sql(source, self.connection, "INSERT")
        column_sql = ""
        if columns:
            column_sql = " (" + ", ".join(_quote_identifier(column) for column in columns) + ")"
        with self._mutation("Lance table inserts"):
            self.connection.execute(f"INSERT INTO {self._sql_target()}{column_sql} {source_sql}")

    def update(self, assignments: Mapping[str, str], *, where: str | None = None) -> None:
        if not assignments:
            raise ValueError("UPDATE requires at least one assignment")
        set_sql = ", ".join(f"{_quote_identifier(column)} = {expression}" for column, expression in assignments.items())
        where_sql = f" WHERE {where}" if where else ""
        with self._mutation("Lance table updates"):
            self.connection.execute(f"UPDATE {self._sql_target()} SET {set_sql}{where_sql}")

    def delete(self, *, where: str | None = None) -> None:
        where_sql = f" WHERE {where}" if where else ""
        with self._mutation("Lance table deletes"):
            self.connection.execute(f"DELETE FROM {self._sql_target()}{where_sql}")

    def truncate(self) -> None:
        with self._mutation("Lance table truncation"):
            self.connection.execute(f"TRUNCATE TABLE {self._sql_target()}")

    def add_column(self, name: str, sql_type: str, *, default: str | None = None) -> None:
        default_sql = f" DEFAULT {default}" if default is not None else ""
        with self._mutation("Lance table schema changes"):
            self.connection.execute(
                f"ALTER TABLE {self._sql_target()} ADD COLUMN {_quote_identifier(name)} {sql_type}{default_sql}"
            )

    def drop_column(self, name: str) -> None:
        with self._mutation("Lance table schema changes"):
            self.connection.execute(f"ALTER TABLE {self._sql_target()} DROP COLUMN {_quote_identifier(name)}")

    def rename_column(self, old_name: str, new_name: str) -> None:
        with self._mutation("Lance table schema changes"):
            self.connection.execute(
                f"ALTER TABLE {self._sql_target()} RENAME COLUMN "
                f"{_quote_identifier(old_name)} TO {_quote_identifier(new_name)}"
            )

    def merge(self, source: Any, on: str) -> LanceMergeBuilder:
        return LanceMergeBuilder(self, source, on)


@dataclass
class LanceMergeBuilder:
    table: LanceTable
    source: Any
    on: str
    _clauses: list[str] = field(default_factory=list)

    def when_matched_update(self, assignments: Mapping[str, str], *, condition: str | None = None) -> LanceMergeBuilder:
        prefix = "WHEN MATCHED" + (f" AND {condition}" if condition else "")
        values = ", ".join(f"{_quote_identifier(key)} = {value}" for key, value in assignments.items())
        self._clauses.append(f"{prefix} THEN UPDATE SET {values}")
        return self

    def when_matched_delete(self, *, condition: str | None = None) -> LanceMergeBuilder:
        prefix = "WHEN MATCHED" + (f" AND {condition}" if condition else "")
        self._clauses.append(f"{prefix} THEN DELETE")
        return self

    def when_not_matched_insert(self, values: Mapping[str, str], *, condition: str | None = None) -> LanceMergeBuilder:
        prefix = "WHEN NOT MATCHED" + (f" AND {condition}" if condition else "")
        columns = ", ".join(_quote_identifier(key) for key in values)
        expressions = ", ".join(values.values())
        self._clauses.append(f"{prefix} THEN INSERT ({columns}) VALUES ({expressions})")
        return self

    def execute(self) -> None:
        if not self._clauses:
            raise ValueError("MERGE requires at least one action")
        source_sql = _relation_sql(self.source, self.table.connection, "MERGE")
        sql = (
            f"MERGE INTO {_quote_identifier(self.table.qualified_name)} AS target "
            f"USING ({source_sql}) AS source ON {self.on} " + " ".join(self._clauses)
        )
        with self.table._mutation("Lance table merges"):
            self.table.connection.execute(sql)


__all__ = [
    "DatasetCoordinator",
    "LanceCommitCleanupError",
    "LanceCommitOutcomeUnknownError",
    "LanceDataset",
    "LanceMergeBuilder",
    "LanceNamespace",
    "LanceTable",
    "normalize_dataset_uri",
    "recover_ray_dataset_coordinator",
]
