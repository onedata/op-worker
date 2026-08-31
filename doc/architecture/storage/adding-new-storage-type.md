# Adding a New Storage Type

This guide walks you through adding support for a new storage backend type
to the Onedata storage configuration system. You will define typed contracts
in op-panel-contracts, add REST translation in onepanel, and implement
helper config logic in op-worker. Each storage type follows a consistent
pattern; use POSIX or S3 as templates depending on whether your storage is
filesystem-like or object-storage-like.

> This document is part of the storage configuration documentation set.
> See [Storage Configuration Overview](_overview.md) for the high-level
> architecture and data flow.

## Prerequisites

Before starting, ensure you have:

- **Familiarity with the storage configuration architecture** — read
  [overview](_overview.md) to understand how contracts, spec builders,
  and helper configs fit together.

- **A working C++ helper implementation** — the NIF side that performs
  actual I/O is out of scope. The new storage type must already have a
  C++ helper that accepts args and admin_ctx as flat binary maps.

- **Understanding of configuration and credentials** — know what fields
  and credentials the new storage needs. This determines the record
  definitions you will add.

## Step 0: Define REST API in Swagger (onepanel-swagger)

The REST API is defined in the **onepanel-swagger** repository. Swagger
definitions drive code generation — the REST model, validators, and
route definitions in Onepanel are auto-generated from these YAML files.

### 0.1 Create storage type definitions

In `definitions/storage/`, create a subdirectory for your storage type
(e.g. `definitions/storage/newstorage/`). Each existing storage type
has up to six YAML files:

| File | Purpose |
|------|---------|
| `common.yaml` | Shared fields (used by create, get, modify) |
| `credentials.yaml` | Credential fields (access keys, tokens, etc.) |
| `create.yaml` | Create model (allOf: common + credentials + type-specific) |
| `get.yaml` | GET response model |
| `modify.yaml` | PATCH model (all fields optional) |

Use an existing type as a template — `s3/` for object storage,
`posix/` for filesystem-like storage. Define the fields that match
your contract records from Step 1.

### 0.2 Register in the index

Update `definitions/storage/create_request.yaml`,
`definitions/storage/create_details.yaml`,
`definitions/storage/get_details.yaml`, and
`definitions/storage/modify_details.yaml` to include the new type
in the polymorphic discriminator list.

### 0.3 Generate Erlang code

Run the code generator:

```bash
make cowboy-server
```

This produces Erlang modules in the generated output directory. Copy
the updated `rest_model` and related files into the Onepanel source
tree (`src/http/`). The generated code includes the polymorphic
dispatch for the new storage type.

## Step 1: Define Contracts (op-panel-contracts)

### 1.1 Create the storage header

Create a new header file `include/storage/newstorage.hrl` (replace
`newstorage` with your type name, e.g. `minio`, `wasabi`). Define four
records:

```erlang
-record(newstorage_credentials, {
    %% Authentication fields required at create time.
    access_key :: binary(),
    secret_key :: binary()
}).

-record(newstorage_credentials_diff, {
    %% Same fields as credentials, all optional (undefined = no change).
    access_key :: undefined | binary(),
    secret_key :: undefined | binary()
}).

-record(newstorage_configuration, {
    %% Configuration fields plus storage_path_type.
    scheme :: binary(),
    hostname :: binary(),
    bucket_name :: binary(),
    %% ... other required/optional fields ...
    storage_path_type :: flat | canonical
}).

-record(newstorage_configuration_diff, {
    %% Mutable config fields only — no storage_path_type (immutable).
    scheme :: undefined | binary(),
    hostname :: undefined | binary(),
    bucket_name :: undefined | binary()
    %% ... other mutable fields ...
}).
```

**Templates:**

- **Filesystem-like** (e.g. POSIX, NFS): Copy from `posix.hrl`.
  Credentials typically have `uid`, `gid`. Configuration has `mount_point`
  and `storage_path_type` (often `canonical` only for POSIX).

- **Object-storage-like** (e.g. S3, Swift): Copy from `s3.hrl`.
  Credentials typically have `access_key`, `secret_key`. Configuration
  has `scheme`, `hostname`, `bucket_name`, optional `region`, `block_size`,
  and `storage_path_type`.

### 1.2 Update onedata_storage.erl

In `src/onedata_storage.erl`:

1. Add the include:

   ```erlang
   -include("storage/newstorage.hrl").
   ```

2. Add type aliases for the four records (follow the same pattern as
   existing types).

3. Add the new type to the union types: `credentials()`, `credentials_diff()`,
   `configuration()`, `configuration_diff()`.

4. Add the new storage type binary to the `type()` comment list (e.g.
   `<<"newstorage">>`).

5. Add the new types to `-export_type/1` in the same order as other types.

## Step 2: Add Spec Builder (onepanel)

### 2.1 Create the type-specific builder

Create `src/services/helpers/storage_spec/newstorage_storage_spec_builder.erl`.

Implement six functions:

| Function | Purpose |
|----------|---------|
| `build_credentials/1` | `#{camelKey => ...}` → `#newstorage_credentials{}` |
| `build_credentials_diff/1` | Same with `maps:get(Key, Params, undefined)` for optional fields |
| `build_configuration/1` | Map → `#newstorage_configuration{}` (include `storage_path_type`) |
| `build_configuration_diff/1` | Map → `#newstorage_configuration_diff{}` (no `storage_path_type`) |
| `credentials_to_map/1` | `#newstorage_credentials{}` → map (camelCase keys) for GET |
| `configuration_to_map/1` | `#newstorage_configuration{}` → map (camelCase keys) for GET |

**Key conventions:**

- REST uses **camelCase** (`accessKey`, `bucketName`); records use
  **snake_case** (`access_key`, `bucket_name`).

- For diff builders, use `maps:get(Key, Params, undefined)` so missing
  keys mean "no change".

- For `storage_path_type`, use
  `storage_spec_builder_utils:binary_to_storage_path_type/1` to convert
  `<<"flat">>` / `<<"canonical">>` to atoms.

### 2.2 Update storage_spec_builder.erl

In the main dispatcher `src/services/helpers/storage_spec/storage_spec_builder.erl`:

Add a clause to each of these functions that dispatches to your new module
when `Type =:= <<"newstorage">>`:

- `build_credentials/2`
- `build_credentials_diff/2`
- `build_configuration/2`
- `build_configuration_diff/2`
- `credentials_to_map/2`
- `configuration_to_map/2`

### 2.3 Update REST model

Add the new type to the polymorphic storage model. The REST API typically
uses a discriminator (e.g. `type` or `storageType`) to select the schema.
Ensure the new storage type is a valid value in the model definition so
that create/update requests are accepted.

## Step 3: Add Helper Config Module (op-worker)

### 3.1 Create the helper config module

Create
`src/modules/storage/helpers/config/storage/newstorage_helper_config.erl`.

Implement `helper_config_behaviour`:

**Core callbacks:**

- `build/1` — Translate `#newstorage_configuration{}` → args map,
  `#newstorage_credentials{}` → admin_ctx map. Use
  `helper_config_utils:add_optional_args_if_defined/2` for optional
  fields; include `storagePathType` via
  `helper_config_utils:storage_path_type_to_binary/1`.

- `validate_user_ctx/1` — Define required and optional user context keys.
  Use `helper_config_utils:validate_user_ctx(UserCtx, Required, Optional)`.

- `build_args_diff/2` — Compute args diff from
  `#newstorage_configuration_diff{}`. Use
  `helper_config_utils:build_args_diff_from_specs/2`. If
  `configuration =:= undefined` in the update spec, substitute an empty
  diff record.

- `build_admin_ctx_diff/2` — Compute admin_ctx diff from
  `#newstorage_credentials_diff{}`. Return `#{}` when
  `credentials =:= undefined`.

- `describe/1` — Reverse the translation: flat maps → typed records for
  `#helper_config_description{}`. Use
  `helper_config_utils:storage_path_type_from_binary/1` and
  `helper_config_utils:set_optional_record_fields_if_defined/3` for
  optional fields.

**Capability callbacks:**

- `is_posix_compatible/0`, `is_object_storage/0`, `is_rename_supported/0`
- `is_nfs4_acl_supported/0`, `is_oauth2_supported/0`
- `is_storage_access_type_supported/1`, `is_auto_import_supported/1`,
  `is_file_registration_supported/1`, `is_getting_size_supported/1`

Implement according to your storage semantics. POSIX: `is_posix_compatible`
and `is_rename_supported` true. S3: `is_object_storage` true,
`is_rename_supported` false.

**Other callbacks:**

- `get_block_size/1` — Return block size for object storage; `undefined`
  for filesystem storage.

- `redact_confidential_credentials/1` — Redact sensitive fields (e.g.
  `secret_key`) before logging. Use
  `helper_config_utils:redact_record_fields_if_defined/2` for S3-like
  credentials.

- `redact_confidential_credentials_diff/1` — Same for diff records.

**Args keys:** Use camelCase for the C++ NIF layer (e.g. `<<"accessKey">>`,
`<<"bucketName">>`, `<<"storagePathType">>`).

### 3.2 Update helpers.hrl

In `include/modules/storage/helpers/helpers.hrl`:

Add the helper name macro:

```erlang
-define(NEWSTORAGE_HELPER_NAME, <<"newstorage">>).
```

Update `POSIX_COMPATIBLE_HELPERS`, `OBJECT_HELPERS`, or `AUTO_IMPORT_HELPERS`
if the new type belongs there.

### 3.3 Update helper_config.erl

In `src/modules/storage/helpers/config/helper_config.erl`:

Add a clause to `get_module/1`:

```erlang
get_module(?NEWSTORAGE_HELPER_NAME) -> newstorage_helper_config:module_info(module).
```

### 3.4 Update storage_crud_utils.erl

In `src/modules/storage/crud/storage_crud_utils.erl`:

1. Add the include:

   ```erlang
   -include_lib("opw_panel_contracts/include/storage/newstorage.hrl").
   ```

2. Add `get_record_def/2` clauses for the four records:

   ```erlang
   get_record_def(newstorage_credentials, N) ->
       case record_info(size, newstorage_credentials) - 1 of
           N -> record_info(fields, newstorage_credentials);
           _ -> no
       end;
   %% ... same for credentials_diff, configuration, configuration_diff ...
   ```

   This enables pretty-printing of specs in logs.

## Step 4: Register and Test

1. **Compile and run Dialyzer** — Ensure the new type is included in
   union types so Dialyzer does not report type mismatches. Fix any
   new warnings.

2. **Test create/update/describe via REST API** — Create a storage of
   the new type, patch it with a configuration diff, and GET the
   description. Verify the round-trip returns correct data.

3. **Verify diagnostics** — Run storage diagnostics on the new storage
   to ensure the helper can connect and perform read/write tests.

4. **Check credential redaction** — Confirm that
   `redact_confidential_credentials` and
   `redact_confidential_credentials_diff` mask secrets in logs and
   API responses.

5. **Add a distributed test suite** — Onepanel has a test framework
   with per-storage-type suites in
   `test_distributed/suites/api/oneprovider/storages/`. Create a new
   suite `api_op_storage_newstorage_test_SUITE.erl` following the
   pattern of existing suites (e.g.
   `api_op_storage_s3_test_SUITE.erl` for object storage or
   `api_op_storage_posix_test_SUITE.erl` for filesystem). The base
   module `api_op_storages_test_base.erl` provides shared test
   helpers.

## Checklist

Use this checklist to ensure all required changes are made:

```
onepanel-swagger:
[ ] definitions/storage/newstorage/ (create directory with
    common.yaml, credentials.yaml, create.yaml, get.yaml, modify.yaml)
[ ] definitions/storage/create_request.yaml,
    create_details.yaml, get_details.yaml, modify_details.yaml
    (add new type to discriminator)
[ ] make cowboy-server → copy generated rest_model to onepanel

op-panel-contracts:
[ ] include/storage/newstorage.hrl (create)
[ ] src/onedata_storage.erl (include, type aliases, union types,
    export_type)

onepanel:
[ ] src/services/helpers/storage_spec/
    newstorage_storage_spec_builder.erl (create)
[ ] src/services/helpers/storage_spec/storage_spec_builder.erl
    (add dispatch clauses for build_credentials,
    build_credentials_diff, build_configuration,
    build_configuration_diff, credentials_to_map,
    configuration_to_map)
[ ] src/http/ (update generated REST model files)

op-worker:
[ ] src/modules/storage/helpers/config/storage/
    newstorage_helper_config.erl (create)
[ ] include/modules/storage/helpers/helpers.hrl
    (add helper name macro)
[ ] src/modules/storage/helpers/config/helper_config.erl
    (add get_module clause)
[ ] src/modules/storage/crud/storage_crud_utils.erl
    (add include, get_record_def clauses for pretty-print)

tests:
[ ] onepanel/test_distributed/suites/api/oneprovider/storages/
    api_op_storage_newstorage_test_SUITE.erl (create)
```

## Related Documentation

- [Storage Configuration Overview](_overview.md)
- [Storage Data Contracts](storage-contracts.md)
- [Storage CRUD Operations](storage-crud-operations.md)
- [Helper Configuration](helpers/helper-config.md)
- [Helper Operations](helpers/helper-operations.md)
