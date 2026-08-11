# lance-duckdb vendoring provenance

- Upstream: `https://github.com/lance-format/lance-duckdb`
- Imported revision: `63c2446f7d9c8a59fd73a49fededb0c3725cc192`
- Import date: 2026-08-10
- Canonical `git archive --format=tar` SHA-256:
  `dc9159b9c8e9cf3e4362e1f62fc01c7a555aa2840469f09ab931bfde1c162eb1`
- GitHub source archive SHA-256 at import time:
  `af11b51aa4e9b8d15d92e4a6b53932cdf11e52cca6eda2526fda380d49f79088`

The import contains every file owned by lance-duckdb at that revision. It
intentionally excludes `.gitmodules` and the `duckdb` and
`extension-ci-tools` gitlinks. Vane builds the extension against its directly
vendored DuckDB fork at `external/duckdb`; no second DuckDB source tree is
present or built.

Vane-local changes after import:

- build only the static `lance_extension` target and link it into
  `vane._native`; do not build a loadable DuckDB extension;
- register a build-directory copy of the vendored SQLLogicTest suite with
  DuckDB test builds so mutation cases cannot alter imported fixtures, while
  keeping those tests out of the runtime wheel;
- restrict Lance storage features to local files and AWS/S3 plus the REST
  namespace implementation;
- build Cargo dependencies with the committed lock file and regenerate that
  lock file after removing unused cloud feature graphs;
- vendor narrow source patches for `object_store 0.13.2`, `opendal-core
  0.57.0`, and `opendal-service-s3 0.57.0` that raise `quick-xml` to the
  security-fixed `0.41` line; archive hashes and the exact delta are recorded
  in `vendor-patches/README.md`;
- discover the repository-managed `protoc` for reproducible PEP 517 builds;
- capture a dataset version at bind time and expose opaque fragment/global
  splits to Vane without serializing Rust or C++ handles;
- refresh unversioned dataset cache entries at bind time so commits from other
  connections become visible to new queries while already-running queries keep
  their immutable versioned snapshot;
- route writes through Vane's single-commit writer and its durable
  `writer_started` retry barrier;
- resolve storage credentials from connection-scoped `TYPE LANCE` secrets so
  serialized plans and logs remain credential-free; and
- size the shared Tokio runtime from Vane/Ray worker CPU capacity instead of
  blindly using every host CPU, with weighted CPU permits for global search
  and writer tasks.

To reproduce the canonical archive hash:

```bash
git clone --filter=blob:none --no-checkout \
  https://github.com/lance-format/lance-duckdb.git
git -C lance-duckdb archive --format=tar \
  63c2446f7d9c8a59fd73a49fededb0c3725cc192 | sha256sum
```
