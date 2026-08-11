# Vendored Rust security backports

These directories are exact source imports of the listed crates.io releases,
except that their `quick-xml` dependency has been raised to `0.41.0`.  The
released crates constrain the dependency to `0.39.x`, which is affected by
RUSTSEC-2026-0194 and RUSTSEC-2026-0195.  Vane patches the consumers instead of
waiving either advisory because XML parsing is reachable from the supported S3
storage path.

| Crate | Version | crates.io archive SHA-256 | Local change |
| --- | --- | --- | --- |
| `object_store` | 0.13.2 | `622acbc9100d3c10e2ee15804b0caa40e55c933d5aa53814cd520805b7958a49` | Require `quick-xml >=0.41.0, <0.42.0` |
| `opendal-core` | 0.57.0 | `c4f8607c90e2c963a91467f50fb49fbc7fb3d573f88cea219ca59ccd3740b309` | Require `quick-xml >=0.41.0, <0.42.0` |
| `opendal-service-s3` | 0.57.0 | `313d46c9f5ae70bca26b7c3e3fbb9b639292625f28af73aa016f47e788af9deb` | Require `quick-xml >=0.41.0, <0.42.0` |

The original normalized and author manifests, licenses, notices, source, and
tests are retained.  `Cargo.toml` is the manifest Cargo consumes for each path
patch; `Cargo.toml.orig` remains as upstream provenance where its workspace
dependencies cannot be evaluated outside the original repository.
