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

-record(support_spec, {
    provider :: oct_background:entity_selector(),
    storage_spec = any ::
        space_setup_utils:posix_storage_params() |
        space_setup_utils:s3_storage_params() |
        storage:id() |
        any, % uses randomly selected storage of given provider
    size = 123454321 :: integer()
}).

-record(space_spec, {
    name :: atom() | binary() | undefined,
    owner = space_owner :: oct_background:entity_selector(),
    users = [] :: [oct_background:entity_selector()],
    supports :: [space_setup_utils:support_spec()]
}).

-endif.