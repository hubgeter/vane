# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
import re
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from vane.lance._coordinator import DatasetCoordinator, LanceCommitCleanupError, _SnapshotLease, normalize_dataset_uri
from vane.runners.copy_outcome import CopyOutcomeUnknownError


class LanceCommitOutcomeUnknownError(CopyOutcomeUnknownError):
    """A Lance writer may have committed; repeating it is unsafe."""


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
        elif isinstance(value, (int, float)):
            literal = str(value)
        else:
            literal = _quote_literal(str(value))
        rendered.append(f"{key} = {literal}")
    return ", ".join(rendered)


class LanceDataset:
    def __init__(self, uri: str | os.PathLike[str], connection: Any | None = None) -> None:
        self.uri = os.fspath(uri)
        self.connection = _connection(connection)
        self.coordinator = DatasetCoordinator(self.uri)

    def scan(self) -> Any:
        return self.connection.read_lance(self.uri)

    def _snapshot_relation(self, factory: Any) -> Any:
        lease = _SnapshotLease(self.coordinator)
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
    def snapshot(self) -> Iterator[Any]:
        """Hold a distributed read lease for the lifetime of a query."""
        with self.coordinator.snapshot():
            yield self.scan()

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
        named = ["k := ?", "prefilter := ?", "use_index := ?"]
        params: list[Any] = [self._scan_reference(), vector_column, list(query), k, prefilter, use_index]
        if nprobes is not None:
            named.append("nprobs := ?")
            params.append(nprobes)
        if refine_factor is not None:
            named.append("refine_factor := ?")
            params.append(refine_factor)
        if filter is not None:
            named.append("filter := ?")
            params.append(filter)
        sql = "SELECT * FROM lance_vector_search(?, ?, ?, " + ", ".join(named) + ")"
        return self._snapshot_relation(lambda: self.connection.sql(sql, params=params))

    def fts(
        self,
        text_column: str,
        query: str,
        *,
        k: int = 10,
        prefilter: bool = False,
        filter: str | None = None,
    ) -> Any:
        named = ["k := ?", "prefilter := ?"]
        params: list[Any] = [self._scan_reference(), text_column, query, k, prefilter]
        if filter is not None:
            named.append("filter := ?")
            params.append(filter)
        sql = "SELECT * FROM lance_fts(?, ?, ?, " + ", ".join(named) + ")"
        return self._snapshot_relation(lambda: self.connection.sql(sql, params=params))

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
        named = ["k := ?", "prefilter := ?", "use_index := ?", "alpha := ?", "oversample_factor := ?"]
        params: list[Any] = [
            self._scan_reference(),
            vector_column,
            list(vector_query),
            text_column,
            text_query,
            k,
            prefilter,
            use_index,
            alpha,
            oversample_factor,
        ]
        if nprobes is not None:
            named.append("nprobs := ?")
            params.append(nprobes)
        if refine_factor is not None:
            named.append("refine_factor := ?")
            params.append(refine_factor)
        sql = "SELECT * FROM lance_hybrid_search(?, ?, ?, ?, ?, " + ", ".join(named) + ")"
        return self._snapshot_relation(lambda: self.connection.sql(sql, params=params))

    def write(self, relation: Any, *, mode: str = "create", **options: Any) -> None:
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
                exc.detail,
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
        with self.coordinator.mutation():
            self.connection.execute(sql)

    def show_indexes(self) -> Any:
        return self.connection.sql(f"SHOW INDEXES ON {self._lance_command_target()}")

    def drop_index(self, name: str) -> None:
        with self.coordinator.mutation():
            self.connection.execute(
                f"DROP INDEX {_bare_identifier(name, label='Lance index name', allow_qualified=False)} "
                f"ON {self._lance_command_target()}"
            )

    def optimize(self, **options: Any) -> Any:
        sql = f"OPTIMIZE {self._lance_command_target()}"
        if options:
            sql += " WITH (" + _option_sql(options) + ")"
        with self.coordinator.mutation():
            return self.connection.sql(sql).fetchall()

    def vacuum(self, **options: Any) -> Any:
        sql = f"VACUUM LANCE {self._lance_command_target()}"
        if options:
            sql += " WITH (" + _option_sql(options) + ")"
        with self.coordinator.vacuum():
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
        self.namespace_id = os.fspath(namespace_id)
        self.alias = alias
        self.endpoint = endpoint
        self.connection = _connection(connection)
        if attach:
            options = ["TYPE LANCE", f"READ_ONLY {'true' if read_only else 'false'}"]
            if endpoint is not None:
                options.append("ENDPOINT " + _quote_literal(endpoint))
            self.connection.execute(
                f"ATTACH {_quote_literal(self.namespace_id)} AS {_quote_identifier(alias)} ({', '.join(options)})"
            )

    def table(self, name: str, schema: str = "main") -> LanceTable:
        return LanceTable(self, name, schema=schema)

    def create_table(
        self,
        name: str,
        source: Any,
        *,
        schema: str = "main",
        if_not_exists: bool = False,
    ) -> LanceTable:
        table = self.table(name, schema=schema)
        source_sql = source if isinstance(source, str) else source.sql_query()
        if not source_sql:
            raise ValueError("CREATE TABLE source relation is not serializable; provide a SELECT SQL string")
        guard = " IF NOT EXISTS" if if_not_exists else ""
        with table.coordinator.mutation():
            self.connection.execute(f"CREATE TABLE{guard} {_quote_identifier(table.qualified_name)} AS {source_sql}")
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
        with table.coordinator.mutation():
            self.connection.execute(f"DROP TABLE{guard} {_quote_identifier(table.qualified_name)}")

    def detach(self) -> None:
        self.connection.execute(f"DETACH {_quote_identifier(self.alias)}")


class LanceTable(LanceDataset):
    def __init__(self, namespace: LanceNamespace, name: str, *, schema: str = "main") -> None:
        self.namespace = namespace
        self.name = name
        self.schema = schema
        self.qualified_name = f"{namespace.alias}.{schema}.{name}"
        if namespace.endpoint:
            identity = (
                namespace.endpoint.rstrip("/") + "/" + namespace.namespace_id.strip("/") + "/" + schema + "/" + name
            )
        else:
            identity = normalize_dataset_uri(namespace.namespace_id).rstrip("/") + "/" + name + ".lance"
        super().__init__(identity, namespace.connection)

    def scan(self) -> Any:
        return self._snapshot_relation(lambda: self.connection.table(self.qualified_name))

    def _scan_reference(self) -> str:
        return self.qualified_name

    def _sql_target(self) -> str:
        return _quote_identifier(self.qualified_name)

    def _lance_command_target(self) -> str:
        return _bare_identifier(self.qualified_name, label="Lance table name")

    def insert(self, source: Any, *, columns: Sequence[str] | None = None) -> None:
        source_sql = source if isinstance(source, str) else source.sql_query()
        if not source_sql:
            raise ValueError("INSERT source relation is not serializable; provide a SELECT SQL string")
        column_sql = ""
        if columns:
            column_sql = " (" + ", ".join(_quote_identifier(column) for column in columns) + ")"
        with self.coordinator.mutation():
            self.connection.execute(f"INSERT INTO {self._sql_target()}{column_sql} {source_sql}")

    def update(self, assignments: Mapping[str, str], *, where: str | None = None) -> None:
        if not assignments:
            raise ValueError("UPDATE requires at least one assignment")
        set_sql = ", ".join(f"{_quote_identifier(column)} = {expression}" for column, expression in assignments.items())
        where_sql = f" WHERE {where}" if where else ""
        with self.coordinator.mutation():
            self.connection.execute(f"UPDATE {self._sql_target()} SET {set_sql}{where_sql}")

    def delete(self, *, where: str | None = None) -> None:
        where_sql = f" WHERE {where}" if where else ""
        with self.coordinator.mutation():
            self.connection.execute(f"DELETE FROM {self._sql_target()}{where_sql}")

    def truncate(self) -> None:
        with self.coordinator.mutation():
            self.connection.execute(f"TRUNCATE TABLE {self._sql_target()}")

    def add_column(self, name: str, sql_type: str, *, default: str | None = None) -> None:
        default_sql = f" DEFAULT {default}" if default is not None else ""
        with self.coordinator.mutation():
            self.connection.execute(
                f"ALTER TABLE {self._sql_target()} ADD COLUMN {_quote_identifier(name)} {sql_type}{default_sql}"
            )

    def drop_column(self, name: str) -> None:
        with self.coordinator.mutation():
            self.connection.execute(f"ALTER TABLE {self._sql_target()} DROP COLUMN {_quote_identifier(name)}")

    def rename_column(self, old_name: str, new_name: str) -> None:
        with self.coordinator.mutation():
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
        source_sql = self.source if isinstance(self.source, str) else self.source.sql_query()
        if not source_sql:
            raise ValueError("MERGE source relation is not serializable; provide a SELECT SQL string")
        sql = (
            f"MERGE INTO {_quote_identifier(self.table.qualified_name)} AS target "
            f"USING ({source_sql}) AS source ON {self.on} " + " ".join(self._clauses)
        )
        with self.table.coordinator.mutation():
            self.table.connection.execute(sql)


__all__ = [
    "LanceCommitCleanupError",
    "LanceCommitOutcomeUnknownError",
    "LanceDataset",
    "LanceMergeBuilder",
    "LanceNamespace",
    "LanceTable",
    "normalize_dataset_uri",
]
