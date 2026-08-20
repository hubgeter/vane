#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

"""Generate the license bundle for Rust crates linked into Lance support."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
OUTPUT = REPOSITORY_ROOT / "LICENSES" / "lance-rust-dependencies.txt"
DEFAULT_BUILD_MANIFEST = (
    REPOSITORY_ROOT / "build" / "python-release" / "_deps" / "lance_extension_fc-src" / "Cargo.toml"
)
LICENSE_PREFIXES = ("license", "copying", "notice", "copyright")
FORBIDDEN_LICENSES = (
    "AGPL-1.0",
    "AGPL-1.0-only",
    "AGPL-1.0-or-later",
    "AGPL-3.0",
    "AGPL-3.0-only",
    "AGPL-3.0-or-later",
    "GPL-1.0",
    "GPL-1.0-only",
    "GPL-1.0-or-later",
    "GPL-2.0",
    "GPL-2.0-only",
    "GPL-2.0-or-later",
    "GPL-3.0",
    "GPL-3.0-only",
    "GPL-3.0-or-later",
    "LGPL-2.0",
    "LGPL-2.0-only",
    "LGPL-2.0-or-later",
    "LGPL-2.1",
    "LGPL-2.1-only",
    "LGPL-2.1-or-later",
    "LGPL-3.0",
    "LGPL-3.0-only",
    "LGPL-3.0-or-later",
    "SSPL-1.0",
)
FORBIDDEN_LICENSE_KEYS = {license_id.casefold() for license_id in FORBIDDEN_LICENSES}


class _SpdxChoiceParser:
    """Evaluate whether an SPDX expression has a non-forbidden choice.

    ``OR`` represents a selectable licensing alternative, while every branch
    of ``AND`` must be acceptable. ``WITH`` never makes a forbidden base
    license acceptable; this is intentionally conservative.
    """

    TOKEN = re.compile(r"\s*(\(|\)|/|AND\b|OR\b|WITH\b|[A-Za-z0-9][A-Za-z0-9.+-]*)", re.IGNORECASE)

    def __init__(self, expression: str) -> None:
        self.tokens: list[str] = []
        offset = 0
        while offset < len(expression):
            match = self.TOKEN.match(expression, offset)
            if match is None:
                raise ValueError(f"unsupported SPDX syntax near {expression[offset:]!r}")
            self.tokens.append(match.group(1))
            offset = match.end()
        self.offset = 0

    def parse(self) -> bool:
        if not self.tokens:
            raise ValueError("empty SPDX expression")
        result = self._parse_or()
        if self.offset != len(self.tokens):
            raise ValueError(f"unexpected SPDX token {self.tokens[self.offset]!r}")
        return result

    def _peek(self, value: str) -> bool:
        return self.offset < len(self.tokens) and self.tokens[self.offset].upper() == value

    def _consume(self, value: str) -> None:
        if not self._peek(value):
            raise ValueError(f"expected SPDX token {value!r}")
        self.offset += 1

    def _parse_or(self) -> bool:
        result = self._parse_and()
        while self._peek("OR") or self._peek("/"):
            self.offset += 1
            alternative = self._parse_and()
            result = result or alternative
        return result

    def _parse_and(self) -> bool:
        result = self._parse_with()
        while self._peek("AND"):
            self.offset += 1
            requirement = self._parse_with()
            result = result and requirement
        return result

    def _parse_with(self) -> bool:
        result = self._parse_atom()
        if self._peek("WITH"):
            self.offset += 1
            self._parse_identifier("SPDX exception")
        return result

    def _parse_atom(self) -> bool:
        if self._peek("("):
            self.offset += 1
            result = self._parse_or()
            self._consume(")")
            return result
        identifier = self._parse_identifier("license identifier")
        return identifier.casefold() not in FORBIDDEN_LICENSE_KEYS

    def _parse_identifier(self, label: str) -> str:
        if self.offset >= len(self.tokens):
            raise ValueError(f"expected SPDX {label}")
        token = self.tokens[self.offset]
        if token.upper() in {"AND", "OR", "WITH"} or token in {"(", ")", "/"}:
            raise ValueError(f"expected SPDX {label}, got {token!r}")
        self.offset += 1
        return token


def license_expression_has_permitted_choice(expression: str) -> bool:
    return _SpdxChoiceParser(expression).parse()


def resolve_manifest(requested: Path | None) -> Path:
    if requested is None:
        override = os.environ.get("DUCKDB_LANCE_DIRECTORY")
        requested = Path(override) if override else DEFAULT_BUILD_MANIFEST
    requested = requested.expanduser()
    manifest = requested / "Cargo.toml" if requested.is_dir() else requested
    if not manifest.is_file():
        raise RuntimeError(
            "lance-duckdb Cargo.toml was not found; pass --manifest PATH or "
            "set DUCKDB_LANCE_DIRECTORY to the pinned checkout"
        )
    lock_file = manifest.with_name("Cargo.lock")
    if not lock_file.is_file():
        raise RuntimeError(f"lance-duckdb lock file was not found: {lock_file}")
    return manifest.resolve()


def cargo_metadata(manifest: Path) -> dict[str, object]:
    command = [
        "cargo",
        "metadata",
        "--locked",
        "--format-version",
        "1",
        "--manifest-path",
        str(manifest),
    ]
    try:
        output = subprocess.check_output(command, cwd=REPOSITORY_ROOT, text=True)
    except FileNotFoundError as exc:
        raise RuntimeError("cargo is required to inspect Lance dependencies") from exc
    return json.loads(output)


def package_license_files(package: dict[str, object]) -> list[Path]:
    manifest_path = Path(str(package["manifest_path"]))
    package_root = manifest_path.parent
    candidates: set[Path] = set()
    declared = package.get("license_file")
    if declared:
        declared_path = Path(str(declared))
        candidates.add(declared_path if declared_path.is_absolute() else package_root / declared_path)
    for child in package_root.rglob("*"):
        if child.is_file() and child.name.lower().startswith(LICENSE_PREFIXES):
            candidates.add(child)
    return sorted(path for path in candidates if path.is_file())


def normalized_text(path: Path) -> str:
    content = path.read_text(encoding="utf-8", errors="replace").replace("\r\n", "\n")
    return "\n".join(line.rstrip() for line in content.splitlines()).rstrip() + "\n"


def build_bundle(metadata: dict[str, object], lock_file: Path) -> str:
    resolve = metadata.get("resolve")
    root_package_id = resolve.get("root") if isinstance(resolve, dict) else None
    if not root_package_id:
        raise RuntimeError("Cargo metadata is missing the root package id")
    packages = [
        package
        for package in metadata["packages"]  # type: ignore[index]
        if package.get("id") != root_package_id
    ]
    packages.sort(key=lambda package: (str(package["name"]).lower(), str(package["version"])))
    workspace_root = Path(str(metadata.get("workspace_root") or lock_file.parent)).resolve()

    text_by_hash: dict[str, str] = {}
    text_users: dict[str, list[str]] = defaultdict(list)
    inventory: list[str] = []
    errors: list[str] = []

    for package in packages:
        name = str(package["name"])
        version = str(package["version"])
        component = f"{name} {version}"
        expression = str(package.get("license") or "").strip()
        files = package_license_files(package)
        if not expression and not package.get("license_file"):
            errors.append(f"{component}: missing Cargo license metadata")
        if expression:
            try:
                permitted = license_expression_has_permitted_choice(expression)
            except ValueError as exc:
                errors.append(f"{component}: invalid SPDX expression {expression!r}: {exc}")
            else:
                if not permitted:
                    errors.append(f"{component}: no permitted license choice in {expression!r}")

        hashes: list[str] = []
        for path in files:
            content = normalized_text(path)
            digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
            text_by_hash.setdefault(digest, content)
            text_users[digest].append(f"{component} ({path.name})")
            hashes.append(digest)
        digest_list = ",".join(sorted(set(hashes))) if hashes else "metadata-only"
        source = package.get("source")
        if source is None:
            package_root = Path(str(package["manifest_path"])).parent.resolve()
            try:
                relative_root = package_root.relative_to(workspace_root)
            except ValueError:
                source = f"path+external/{name}-{version}"
            else:
                source = f"path+workspace/{relative_root.as_posix()}"
        inventory.append(f"{component} | {expression or 'license-file'} | {source} | {digest_list}")

    if errors:
        raise RuntimeError("\n".join(errors))

    lock_digest = hashlib.sha256(lock_file.read_bytes()).hexdigest()
    lines = [
        "Vane Lance Rust dependency licenses",
        "====================================",
        "",
        "Generated by scripts/sync_lance_cargo_licenses.py.",
        "Do not edit this file manually.",
        f"Cargo.lock SHA-256: {lock_digest}",
        "",
        "Inventory",
        "---------",
        "name version | SPDX/license expression | Cargo source | license text SHA-256",
        *inventory,
        "",
        "Preserved license and notice texts",
        "----------------------------------",
    ]
    for digest in sorted(text_by_hash):
        lines.extend(
            (
                "",
                f"SHA-256: {digest}",
                "Used by: " + "; ".join(sorted(text_users[digest], key=str.lower)),
                "",
                text_by_hash[digest].rstrip(),
            )
        )
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        help="Path to the pinned lance-duckdb Cargo.toml or its source directory",
    )
    parser.add_argument("--check", action="store_true", help="Fail instead of rewriting an out-of-date bundle")
    args = parser.parse_args()

    try:
        manifest = resolve_manifest(args.manifest)
        generated = build_bundle(cargo_metadata(manifest), manifest.with_name("Cargo.lock"))
    except (OSError, RuntimeError, subprocess.CalledProcessError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    existing = OUTPUT.read_text(encoding="utf-8") if OUTPUT.exists() else ""
    if args.check:
        if existing != generated:
            print(
                f"error: {OUTPUT.relative_to(REPOSITORY_ROOT)} is stale; run scripts/sync_lance_cargo_licenses.py",
                file=sys.stderr,
            )
            return 1
        print(f"verified {OUTPUT.relative_to(REPOSITORY_ROOT)}")
        return 0

    OUTPUT.write_text(generated, encoding="utf-8")
    print(f"wrote {OUTPUT.relative_to(REPOSITORY_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
