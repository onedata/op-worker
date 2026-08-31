# Storage Data Contracts

Storage Data Contracts define the typed interface between Onepanel and
op-worker for storage CRUD operations. They consist of Erlang records
in the op-panel-contracts library, shared by both components to ensure
consistent request and response shapes at compile time. This document
describes the data model: the three main records, the per-storage-type
pattern, and the create vs update distinction.

> [!NOTE]
> This document covers **storage contracts only**. The op-panel-contracts
> repository is designed to eventually hold all typed contracts between
> Onepanel and op-worker (not just storage-related ones). As the
> refactoring progresses, more RPC interfaces will adopt the same
> contract-based approach.

> This document is part of the storage configuration documentation set.
> See [Storage Configuration Overview](_overview.md) for the high-level
> architecture and [Storage CRUD Operations](storage-crud-operations.md)
> for end-to-end flows including REST-to-contract translation.

## Contract Structure

The op-panel-contracts library defines three main records in
`include/storage/common.hrl`:

### Main Records

```erlang
-record(storage_create_spec, {
    type :: onedata_storage:type(),
    name :: onedata_storage:name(),
    timeout = undefined :: undefined | onedata_storage:operation_timeout(),
    readonly = false :: onedata_storage:readonly(),
    imported = false :: onedata_storage:imported(),
    luma :: onedata_storage:luma_spec(),
    qos_parameters = #{} :: onedata_storage:qos_parameters(),
    credentials :: onedata_storage:credentials(),
    configuration :: onedata_storage:configuration()
}).

-record(storage_update_spec, {
    type :: onedata_storage:type(),
    name :: undefined | onedata_storage:name(),
    timeout :: undefined | onedata_storage:operation_timeout(),
    readonly :: undefined | onedata_storage:readonly(),
    imported :: undefined | onedata_storage:imported(),
    luma :: undefined | onedata_storage:luma_spec(),
    qos_parameters :: undefined | onedata_storage:qos_parameters(),
    credentials :: undefined | onedata_storage:credentials_diff(),
    configuration :: undefined | onedata_storage:configuration_diff()
}).

-record(storage_description, {
    id :: onedata_storage:id(),
    type :: onedata_storage:type(),
    name :: onedata_storage:name(),
    timeout :: undefined | onedata_storage:operation_timeout(),
    readonly :: onedata_storage:readonly(),
    imported :: onedata_storage:imported(),
    luma :: onedata_storage:luma_spec(),
    qos_parameters :: onedata_storage:qos_parameters(),
    credentials :: onedata_storage:credentials(),
    configuration :: onedata_storage:configuration()
}).
```

| Field | Type | Purpose |
|-------|------|---------|
| `type` | `binary()` | Storage type (e.g. `<<"s3">>`, `<<"posix">>`). Determines which credential and configuration records apply. |
| `name` | `binary()` | Human-readable storage name. Required on create; optional on update. |
| `timeout` | `integer()` | Operation timeout in milliseconds. Optional. |
| `readonly` | `boolean()` | If true, blocks write operations. Must be used with imported storage. |
| `imported` | `boolean()` | Indicates storage contains existing data to be imported. |
| `luma` | `#luma_spec{}` | Local User Mapping source: feed type, URL, API key. |
| `qos_parameters` | `map()` | Key-value pairs for QoS. |
| `credentials` | `credentials()` | Storage-specific auth data (union of all `*_credentials` records). |
| `configuration` | `configuration()` | Storage-specific settings (union of all `*_configuration` records). |
| `id` | `binary()` | Storage identifier. Present only in `#storage_description{}`. |

### LUMA Spec

The **LUMA spec** is embedded in create, update, and description records.
It configures the Local User Mapping database feed (out of scope for this
doc set). The record is:

```erlang
-record(luma_spec, {
    feed :: auto | local | external,
    url :: undefined | binary(),
    api_key :: undefined | binary()
}).
```

| Field | Purpose |
|-------|---------|
| `feed` | `auto` (automatic), `local` (stored in op-worker), or `external` (HTTP service). |
| `url` | URL of external feed. Required when `feed = external`. |
| `api_key` | API key for external feed. Optional when `feed = external`. |

## Create vs Update Pattern

**Create** uses full records: `credentials()` and `configuration()`.
All required fields must be present. The system builds a complete
helper spec from scratch.

**Update** uses diff records: `credentials_diff()` and
`configuration_diff()`. Every field is optional; `undefined` means "no
change". The updater merges only non-undefined fields into the existing
config.

### Immutable `storage_path_type`

The `storage_path_type` field (flat vs canonical) is immutable after
creation. It appears in `*_configuration` records but not in
`*_configuration_diff` records. A storage cannot switch from flat to
canonical or vice versa during its lifetime.

### Rationale

Updates carry only what changed. The caller does not need to resend the
entire credentials or configuration map. This reduces payload size,
avoids accidental overwrites, and keeps the update contract simple.

## Per-Storage Type Records

Each of the 11 storage types defines four records:

| Record | Purpose |
|--------|---------|
| `{type}_credentials` | Full credentials for create |
| `{type}_credentials_diff` | Optional credentials for update |
| `{type}_configuration` | Full configuration for create |
| `{type}_configuration_diff` | Mutable configuration for update |

### Summary Table

| Type | Credentials (key fields) | Configuration (key fields) |
|------|--------------------------|---------------------------|
| ceph | username, key | monitor_hostname, cluster_name, pool_name |
| cephrados | username, key | monitor_hostname, cluster_name, pool_name, block_size |
| glusterfs | uid, gid | volume, hostname, port, transport, mount_point |
| http | credentials_type, credentials, oauth2_idp, onedata_access_token | endpoint, verify_server_certificate |
| nfs | uid, gid | version, host, volume, read_ahead, dir_cache |
| nulldevice | uid, gid | latency_min/max, timeout_probability, filter |
| posix | uid, gid | mount_point |
| s3 | access_key, secret_key | scheme, hostname, bucket_name, region |
| swift | username, password, project_name | auth_url, container_name |
| webdav | credentials_type, credentials, oauth2_idp, onedata_access_token | endpoint, range_write_support |
| xrootd | credentials_type, credentials | url, file_mode_mask, dir_mode_mask |

S3 represents the **object storage pattern**: access_key/secret_key,
bucket/container, scheme and hostname. POSIX represents the
**filesystem pattern**: uid/gid and mount_point.

### S3 Example (Full Fields)

| Record | Fields |
|--------|--------|
| `#s3_credentials{}` | `access_key`, `secret_key` |
| `#s3_credentials_diff{}` | `access_key`, `secret_key` (all optional) |
| `#s3_configuration{}` | `scheme`, `hostname`, `bucket_name`, `signature_version`, `verify_server_certificate`, `region`, `block_size`, `file_mode`, `dir_mode`, `storage_path_type` |
| `#s3_configuration_diff{}` | `scheme`, `hostname`, `bucket_name`, `signature_version`, `verify_server_certificate`, `region`, `file_mode`, `dir_mode` — no `storage_path_type` |

### POSIX Example (Full Fields)

| Record | Fields |
|--------|--------|
| `#posix_credentials{}` | `uid`, `gid` |
| `#posix_credentials_diff{}` | `uid`, `gid` (all optional) |
| `#posix_configuration{}` | `mount_point`, `storage_path_type` (always `canonical`) |
| `#posix_configuration_diff{}` | `mount_point` only — no `storage_path_type` |

## How Contracts Are Consumed

### REST to Contract (Onepanel)

Onepanel's spec builders convert camelCase JSON maps into contract
records. This is a mechanical translation with no business logic —
see [Storage CRUD Operations](storage-crud-operations.md) for the
full flow and spec builder details.

### Contract to Helper Spec (op-worker)

On the op-worker side, contracts are translated into `#helper_spec{}`:

- `configuration` → `#helper_spec.configuration` (flat binary map)
- `credentials` → `#helper_spec.credentials` (flat binary map)

The helper spec uses camelCase keys for the C++ NIF layer. See
[Helper Spec](helpers/helper-spec.md) for details.

## Type Safety

The `credentials()` and `configuration()` types are unions of all
type-specific records:

```erlang
-type credentials() ::
    ceph_credentials() | cephrados_credentials() | glusterfs_credentials() |
    http_credentials() | nfs_credentials() | nulldevice_credentials() |
    posix_credentials() | s3_credentials() | swift_credentials() |
    webdav_credentials() | xrootd_credentials().
```

The `type` field in the spec determines which credential and
configuration record to expect. Dialyzer can catch type mismatches at
compile time: passing S3 credentials to a POSIX storage builder, or
wrong configuration shape, will fail static analysis. This is a key
improvement over the old JSON-based approach, where errors surfaced only
at runtime.

## Related Documentation

- [Storage Configuration Overview](_overview.md)
- [Storage CRUD Operations](storage-crud-operations.md)
- [Helper Spec](helpers/helper-spec.md)
- [Adding a New Storage Type](adding-new-storage-type.md)
