# Helpers — Overview

The helpers subsystem encompasses everything related to C++ storage
helpers: how they are configured, how credentials are prepared for
them, and how they execute I/O operations. This subdirectory groups
all helper-related documentation in one place.

> **Parent documentation:** [Storage Configuration Architecture
> Overview](../_overview.md)

## Documents

- **[Helper Configuration](helper-config.md)** — How typed Erlang
  contract records are translated into flat binary maps
  (`#helper_config{}`) for the C++ NIF layer. Covers the build,
  update, and describe flows, admin context vs user context, capability
  queries, credential redaction, and the special handling required for
  [OAuth2-supporting storages](helper-config.md#oauth2-supporting-storages-http-webdav)
  (HTTP, WebDAV).

- **[Helper Operations](helper-operations.md)** — The runtime I/O
  engine: how file operations flow from Erlang business logic through
  a layered handle system (`sd_handle` → `helper_handle` →
  `file_handle`) down to the async C++ NIF. Covers handle lifecycle,
  caching, the async NIF call pattern, error handling (including
  `EKEYEXPIRED` for OAuth2 token expiration), and fallback strategies.

## Key Relationships

```
                    ┌──────────────────────┐
                    │  Storage CRUD        │
                    │  (create/update)     │
                    └──────┬───────────────┘
                           │ builds
                           ▼
                    ┌──────────────────────┐
                    │  Helper Config       │ ◄── contracts (op-panel-contracts)
                    │  (helper-config.md)  │
                    └──────┬───────────────┘
                           │ persisted in storage_config
                           │ used at runtime by
                           ▼
                    ┌──────────────────────┐
                    │  LUMA                │
                    │  (credential         │ ◄── resolves user-specific
                    │   resolution)        │     credentials incl. OAuth2
                    └──────┬───────────────┘
                           │ user_ctx
                           ▼
                    ┌─────────────────────────┐
                    │  Helper Operations      │
                    │  (helper-operations.md) │ ◄── validates user_ctx,
                    │  sd_handle →            │     merges with args,
                    │  helper_handle →        │     calls C++ NIF
                    │  file_handle → NIF      │
                    └─────────────────────────┘
```

Helper configuration is built at storage creation/update time and
persisted. At runtime, LUMA resolves per-user credentials (potentially
including [OAuth2 token acquisition](../luma/credential-resolution.md#oauth2-credential-lifecycle)),
which are then validated by the helper config module and merged with
storage args before being passed to the C++ helper via the NIF.

## Related Documentation

- [Storage Configuration Architecture Overview](../_overview.md)
- [Storage Data Contracts](../storage-contracts.md)
- [Storage CRUD Operations](../storage-crud-operations.md)
- [LUMA — Local User Mapping](../luma/_overview.md)
- [LUMA Credential Resolution](../luma/credential-resolution.md)
