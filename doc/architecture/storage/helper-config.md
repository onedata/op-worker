# Helper Configuration

The helper configuration system bridges the gap between typed Erlang
storage contracts and the C++ NIF helpers that perform actual I/O.
Contract records (`#storage_create_spec{}`, `#storage_update_spec{}`)
use rich Erlang types (records, atoms, integers), but the C++ NIF
layer expects flat binary maps. The helper_config subsystem translates
between these two worlds: it builds, updates, and describes storage
configurations as NIF-compatible key-value maps while preserving
type-safety on the Erlang side.

> **Complementary documentation**
>
> - [Storage Configuration Architecture Overview](_overview.md) — key
>   concepts, component roles, architecture diagram
> - [Storage Data Contracts](storage-contracts.md) — `#storage_create_spec{}`,
>   `#storage_update_spec{}`, `#storage_description{}` definitions

---

## What Is a Helper Config

A **helper config** is an Erlang record that holds all configuration
and credentials required by a C++ helper in a format the NIF can
consume. It is defined as:

```erlang
-record(helper_config, {
    name :: binary(),
    args = #{} :: #{binary() => binary()},
    admin_ctx = #{} :: #{binary() => binary()}
}).
```

- **`name`** — Identifies the C++ helper implementation. Examples:
  `<<"s3">>`, `<<"posix">>`, `<<"ceph">>`. The dispatcher uses this
  to route calls to the correct per-storage module.

- **`args`** — Storage-specific settings: hostname, bucket name, mount
  point, block size, timeout, storage path type, etc. All values are
  binary strings.

- **`admin_ctx`** — Storage-level credentials set during configuration:
  access keys, secret keys, passwords, uid/gid. These are stored
  persistently with the storage. All values are binary strings.

**All values must be binary strings.** The C++ NIF interface cannot
work with Erlang records or complex types (atoms, integers, tuples).
Everything must be serialized to string key-value pairs. Integers and
atoms are converted via `integer_to_binary/1`, `atom_to_binary/1`, and
similar helpers.

**Keys use camelCase.** Examples: `<<"bucketName">>`, `<<"accessKey">>`,
`<<"storagePathType">>`. This convention matches the C++ helper
expectations and keeps JSON-like semantics for the NIF layer.

**Why flat binary maps?** The C++ NIF layer is a separate compiled
component. It receives a single map of strings and does not understand
Erlang records or type descriptors. A flat `#{binary() => binary()}`
map is the lowest common denominator: easy to serialize, pass across
the NIF boundary, and parse in C++.

---

## Architecture

The helper_config system uses a **strategy / dispatcher pattern**. Each
storage type has different configuration fields, credentials, and
capabilities. The behaviour ensures a consistent interface while
allowing per-type customization.

```
helper_config.erl (dispatcher)
        |
        +-- get_module(Type) → routes by storage type
        |
        +-- helper_config_behaviour.erl (callback interface)
        |
        +-- helper_config_utils.erl (shared utilities)
        |
        +-- 11 per-storage modules:
                s3_helper_config
                posix_helper_config
                ceph_helper_config
                cephrados_helper_config
                swift_helper_config
                glusterfs_helper_config
                http_helper_config
                webdav_helper_config
                xrootd_helper_config
                nfs_helper_config
                nulldevice_helper_config
```

- **`helper_config.erl`** — Central dispatcher. Exposes `build/1`,
  `update/2`, `describe/1`, `build_helper_nif_args/2`, and capability
  queries. Routes each call to the appropriate per-storage module via
  `get_module/1` based on `name` or storage type.

- **`helper_config_behaviour.erl`** — Defines the callback interface:
  `build/1`, `validate_user_ctx/1`, `build_args_diff/2`,
  `build_admin_ctx_diff/2`, `describe/1`, plus capability and
  redaction callbacks.

- **`helper_config_utils.erl`** — Shared utilities: optional-arg
  handling, diff building, storage path type conversion, user context
  validation.

- **Per-storage modules** — One module per storage type. Each
  implements the behaviour and knows how to map its contract records
  to and from flat binary maps.

**Why this pattern?** Each storage type has different configuration
fields (S3 has bucket and region; POSIX has mount point; HTTP has
URL). Credentials differ (S3 uses access/secret keys; POSIX uses
uid/gid; Swift uses password). Capabilities differ (HTTP is read-only;
S3 does not support rename). A single monolithic module would become
unmaintainable. The behaviour ensures a uniform API while isolating
type-specific logic in dedicated modules.

---

## Building from Contracts

The **create** flow converts a typed create spec into a helper config:

```
#storage_create_spec{} → helper_config:build/1 → Module:build/1 → #helper_config{}
```

1. `helper_config:build/1` receives `#storage_create_spec{}` with
   `type`, `configuration`, `credentials`, and `timeout`.

2. The dispatcher calls `get_module(Type)` to select the per-storage
   module (e.g. `s3_helper_config` for `<<"s3">>`).

3. The module's `build/1` callback:
   - Maps configuration record fields → `args` (with type conversions)
   - Maps credentials record fields → `admin_ctx`
   - Adds `timeout` from the create spec to `args` as `<<"timeout">>`
   - Omits optional fields that are `undefined` via
     `add_optional_args_if_defined/2`

4. Returns `#helper_config{name, args, admin_ctx}`.

**Type conversions:** Integers (e.g. `block_size`, `timeout`) use
`integer_to_binary/1`. Atoms (e.g. `verify_server_certificate`) use
`atom_to_binary/1`. Storage path type (`flat` | `canonical`) is
converted via `storage_path_type_to_binary/1` to `<<"flat">>` |
`<<"canonical">>`.

### S3 Mapping Example

| Contract field                         | NIF key              | Conversion                  |
|----------------------------------------|----------------------|-----------------------------|
| `s3_configuration.hostname`             | `<<"hostname">>`     | as-is                       |
| `s3_configuration.bucket_name`          | `<<"bucketName">>`   | as-is                       |
| `s3_configuration.block_size`           | `<<"blockSize">>`    | `integer_to_binary`         |
| `s3_configuration.storage_path_type`    | `<<"storagePathType">>` | `storage_path_type_to_binary` |
| `s3_configuration.scheme`               | `<<"scheme">>`       | as-is                       |
| `s3_configuration.signature_version`    | `<<"signatureVersion">>` | `integer_to_binary`      |
| `s3_configuration.verify_server_certificate` | `<<"verifyServerCertificate">>` | `atom_to_binary` |
| `s3_configuration.region`              | `<<"region">>`       | as-is (optional)           |
| `s3_credentials.access_key`            | `<<"accessKey">>`    | as-is                      |
| `s3_credentials.secret_key`             | `<<"secretKey">>`    | as-is                      |
| `timeout` (from create spec)            | `<<"timeout">>`      | `integer_to_binary`        |

---

## Updating Helper Config

The **update** flow applies diffs from an update spec to an existing
helper config:

```
#helper_config{} + #storage_update_spec{} → helper_config:update/2 → {ok, NewHelperConfig} | {error, no_change}
```

1. `helper_config:update/2` receives the current `#helper_config{}`
   and `#storage_update_spec{}`.

2. The dispatcher routes to the per-storage module.

3. `Module:build_args_diff/2` compares the update spec's
   `*_configuration_diff` fields to the current `args`. Only changed
   or new values are included; unchanged values are filtered out.

4. `Module:build_admin_ctx_diff/2` does the same for credentials
   (`*_credentials_diff` → `admin_ctx`).

5. If both diffs are empty, the function returns `{error, no_change}`.
   The caller can use this to avoid emitting unnecessary events or
   persisting no-op updates.

6. Otherwise, the diffs are merged: `maps:merge(CurrentArgs, ArgsDiff)`
   and `maps:merge(CurrentAdminCtx, AdminCtxDiff)`. Returns
   `{ok, NewHelperConfig}`.

**Why return `no_change`?** The updater and orchestration layer can
short-circuit: no need to persist, emit events, or reload helpers when
nothing actually changed. This avoids unnecessary churn and keeps
logs clean.

---

## Describing Helper Config

The **describe** flow is the inverse of build: it reconstructs typed
records from flat binary maps.

```
#helper_config{} → helper_config:describe/1 → Module:describe/1 → #helper_config_description{}
```

`helper_config:describe/1` returns `#helper_config_description{}`
with `type`, `configuration`, `credentials`, and `timeout`. Each
per-storage module implements `describe/1` to map `args` and
`admin_ctx` back to Erlang records (`#s3_configuration{}`,
`#s3_credentials{}`, etc.).

**Use case:** `storage_describer` calls `helper_config:describe/1` when
handling a GET request. The result is embedded in
`#storage_description{}` and returned to the REST client. The client
expects typed configuration and credentials; the flat maps are
internal and never exposed.

---

## Admin Context vs User Context

**Admin context** — Storage-level credentials set during storage
configuration. Stored as part of `#helper_config{}` in `admin_ctx`.
Examples: S3 access key and secret key, POSIX root uid/gid, Swift
password. These credentials apply to the storage as a whole and are
persisted with the storage configuration.

**User context** — Per-session credentials provided at runtime when
performing I/O. Examples: user-specific access tokens, delegated
credentials. Not stored in `helper_config`. User context is passed
separately when e.g. opening a file.

**At NIF call time:** `helper_config:build_helper_nif_args/2` merges
`args` and `user_ctx` into a single map. The user context is validated
first via `validate_user_ctx/2`; if validation fails, the function
returns `{error, Reason}`. If validation succeeds, the result is
`maps:merge(HelperConfig#helper_config.args, UserCtx)`. The merged
map is what the C++ helper receives.

**Why separate?** Admin credentials are storage-wide and typically
static until an admin updates the storage. User credentials are
per-session and may change with each request (e.g. different users
accessing the same storage). Storing user credentials in helper_config
would pollute the persisted configuration and complicate multi-user
scenarios. Merging at call time keeps the separation clean.

---

## Capability Queries

Each per-storage module declares its capabilities via behaviour
callbacks. The dispatcher exposes these as `helper_config:is_*`
functions. CRUD verification uses them to validate configuration
constraints (e.g. rejecting readwrite for HTTP storage).

| Capability                    | What it means                     | Example                         |
|------------------------------|-----------------------------------|---------------------------------|
| `is_posix_compatible`        | Supports POSIX filesystem semantics | POSIX, GlusterFS, NFS          |
| `is_object_storage`          | Object-based; no rename, no POSIX semantics | S3, Swift, Ceph               |
| `is_storage_access_type_supported` | Supports readonly or readwrite | HTTP: readonly only            |
| `is_auto_import_supported`   | Can auto-import existing data     | POSIX, S3 (with canonical + block_size 0) |
| `is_file_registration_supported` | Can register existing files   | S3 (with canonical + block_size 0) |
| `is_import_supported`        | Either auto-import or file registration | Logical OR of the above   |
| `is_rename_supported`        | Supports atomic rename            | POSIX: yes; S3: no              |
| `is_nfs4_acl_supported`      | Supports NFSv4 ACLs                | NFS, POSIX on some backends    |
| `is_oauth2_supported`        | Supports OAuth2 credentials       | WebDAV, HTTP (with token)      |
| `is_getting_size_supported`  | Can report storage size           | S3 (block_size 0), POSIX       |

`is_import_supported/1` is defined as `is_auto_import_supported(HelperConfig)
orelse is_file_registration_supported(HelperConfig)` — it answers
whether the storage can import existing data in any form.

---

## Credential Redaction

Before logging or returning credentials to clients, confidential
fields are replaced with `<<"*****">>`. This prevents secrets from
appearing in logs or API responses.

**Functions:**
- `redact_confidential_credentials/2` — masks secrets in a full
  credentials record
- `redact_confidential_credentials_diff/2` — masks secrets in a
  credentials diff

Each per-storage module declares which fields to redact via
`helper_config_utils:redact_record_fields_if_defined/2`. The redacted
value is `?CONFIDENTIAL_MASK` (`<<"*****">>`).

**Per-type redacted fields:**

| Storage   | Redacted fields                          |
|-----------|------------------------------------------|
| S3        | `secret_key`                             |
| Ceph      | `key`                                    |
| CephRados | `key`                                    |
| Swift     | `password`                               |
| HTTP      | `credentials`, `onedata_access_token`    |
| WebDAV    | `credentials`, `onedata_access_token`   |
| XRootD    | `credentials`                            |

POSIX, GlusterFS, NFS, and nulldevice have no confidential credential
fields (uid/gid are not considered secrets in the same way).

---

## Related Documentation

- [Storage Configuration Architecture Overview](_overview.md)
- [Storage Data Contracts](storage-contracts.md)
- [Storage CRUD Operations](storage-crud-operations.md)
