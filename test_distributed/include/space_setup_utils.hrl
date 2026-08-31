%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions concerning space setup in tests.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(STORAGE_IMPORT_HRL).

-include_lib("onenv_ct/include/chart_values.hrl").

-record(s3_storage_params, {
    storage_path_type :: binary(),
    imported_storage = false :: boolean(),
    hostname :: binary(),
    bucket_name = ?S3_BUCKET_NAME :: binary(),
    access_key = ?S3_KEY_ID :: binary(),
    secret_key = ?S3_ACCESS_KEY :: binary(),
    block_size = ?S3_DEFAULT_BLOCK_SIZE :: integer()
}).

-record(posix_storage_params, {
    mount_point :: binary(),
    imported_storage = false :: boolean()
}).

-record(http_storage_params, {
    endpoint :: binary(),
    readonly = true :: boolean(),
    imported_storage = true :: boolean(),
    verify_server_certificate = false :: boolean(),
    emulate_range_read = true :: boolean(),
    % max file size (in bytes) eligible for emulated range reads; undefined leaves the
    % storage default. Only relevant when emulate_range_read = true - files larger than
    % this cannot be read from a server lacking native range read support.
    max_emulated_range_read_file_size = undefined :: undefined | non_neg_integer()
}).

-record(support_spec, {
    provider :: oct_background:entity_selector(),
    storage_spec = any ::
        space_setup_utils:posix_storage_params() |
        space_setup_utils:s3_storage_params() |
        storage:id() |
        any, % uses randomly selected storage of given provider
    size = 123454321 :: integer(),
    % optional storage import config applied during support, e.g.
    % #{mode => <<"manual">>}; only honoured for imported storages
    storage_import = #{} :: map()
}).

-record(space_spec, {
    name :: atom() | binary() | undefined,
    owner = space_owner :: oct_background:entity_selector(),
    users = [] :: [oct_background:entity_selector()],
    supports :: [space_setup_utils:support_spec()]
}).

-endif.