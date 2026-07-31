%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions concerning space setup in tests.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(SPACE_SETUP_UTILS_HRL).
-define(SPACE_SETUP_UTILS_HRL, 1).

-include_lib("onenv_ct/include/chart_values.hrl").

% LUMA DB populated with mappings fetched over HTTP from an external feed service
% under the given url (see luma_test_server for a test implementation of such
% a service).
-record(external_feed_luma, {
    url :: binary(),
    api_key = undefined :: undefined | binary()
}).

-record(s3_storage_params, {
    storage_path_type :: binary(),
    imported_storage = false :: boolean(),
    hostname :: binary(),
    bucket_name = ?S3_BUCKET_NAME :: binary(),
    access_key = ?S3_KEY_ID :: binary(),
    secret_key = ?S3_ACCESS_KEY :: binary(),
    block_size = ?S3_DEFAULT_BLOCK_SIZE :: integer(),
    luma_feed = auto :: space_setup_utils:luma_feed_spec()
}).

-record(posix_storage_params, {
    mount_point :: binary(),
    imported_storage = false :: boolean(),
    luma_feed = auto :: space_setup_utils:luma_feed_spec()
}).

-record(nulldevice_storage_params, {
    imported_storage = false :: boolean(),
    % simulation of latency and timeouts is disabled by default, so that the storage
    % adds virtually no overhead to file operations
    latency_min = 0 :: non_neg_integer(),
    latency_max = 0 :: non_neg_integer(),
    timeout_probability = 0.0 :: float(),
    % file operations affected by the simulated latency and timeouts ("*" - all of them)
    filter = <<"*">> :: binary()
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
        space_setup_utils:storage_params() |
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