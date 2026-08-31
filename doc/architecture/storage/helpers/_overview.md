# Helpers — Overview

The helpers subsystem encompasses everything related to C++ storage
helpers: how they are configured, how credentials are prepared for
them, and how they execute I/O operations. This subdirectory groups
all helper-related documentation in one place.

> **Parent documentation:** [Storage Configuration Architecture
> Overview](../_overview.md)

## Documents

- **[Helper Spec](helper-spec.md)** — How typed Erlang
  contract records are translated into flat binary maps
  (`#helper_spec{}`) for the C++ NIF layer. Covers the build,
  update, and describe flows, admin context vs user context, capability
  queries, credential redaction, and the special handling required for
  [OAuth2-supporting storages](helper-spec.md#oauth2-supporting-storages-http-webdav)
  (HTTP, WebDAV).

- **[Helper Operations](helper-operations.md)** — The runtime I/O
  engine: how file operations flow from Erlang business logic through
  a layered handle system (`sd_handle` → `helper_handle` →
  `file_handle`) down to the async C++ NIF. Covers handle lifecycle,
  caching, the async NIF call pattern, error handling (including
  `EKEYEXPIRED` for OAuth2 token expiration), and fallback strategies.

- **[C++ Helper Caching](cpp-helper-caching.md)** — How the C++
  `CachingStorageHelperCreator` deduplicates storage helper instances
  at the NIF level. When multiple Erlang handles resolve to the same
  storage arguments and LUMA credentials, they share a single C++
  helper. Covers cache key generation, the `VersionedStorageHelper`
  update proxy, eviction policy, sharing scenarios, and implications
  for Erlang-level refactoring.

## Key Relationships

```
                    ┌──────────────────────┐
                    │  Storage CRUD        │
                    │  (create/update)     │
                    └──────┬───────────────┘
                           │ builds
                           ▼
                    ┌──────────────────────┐
                    │  Helper Spec       │ ◄── contracts (op-panel-contracts)
                    │  (helper-spec.md)  │
                    └──────┬───────────────┘
                           │ persisted in storage_config
                           │ used at runtime by
                           ▼
                    ┌──────────────────────┐
                    │  LUMA                │
                    │  (credential         │ ◄── resolves user-specific
                    │   resolution)        │     credentials incl. OAuth2
                    └──────┬───────────────┘
                           │ credentials
                           ▼
                    ┌─────────────────────────┐
                    │  Helper Operations      │
                    │  (helper-operations.md) │ ◄── validates credentials,
                    │  sd_handle →            │     merges with configuration,
                    │  helper_handle →        │     calls C++ NIF
                    │  file_handle → NIF      │
                    └──────────┬──────────────┘
                               │
                               ▼
                    ┌─────────────────────────┐
                    │  C++ Helper Caching     │
                    │  (cpp-helper-caching.md)│ ◄── deduplicates C++ helpers
                    │  CachingStorageHelper-  │     by args + credentials;
                    │  Creator → Versioned-   │     VersionedStorageHelper
                    │  StorageHelper → NIF    │     enables in-place updates
                    └─────────────────────────┘
```

Helper configuration is built at storage creation/update time and
persisted. At runtime, LUMA resolves per-user credentials (potentially
including [OAuth2 token acquisition](../luma/credential-resolution.md#oauth2-credential-lifecycle)),
which are then validated by the helper spec module and merged with
storage configuration before being passed to the C++ helper via the NIF.

## Related Documentation

- [Storage Configuration Architecture Overview](../_overview.md)
- [Storage Data Contracts](../storage-contracts.md)
- [Storage CRUD Operations](../storage-crud-operations.md)
- [LUMA — Local User Mapping](../luma/_overview.md)
- [LUMA Credential Resolution](../luma/credential-resolution.md)
