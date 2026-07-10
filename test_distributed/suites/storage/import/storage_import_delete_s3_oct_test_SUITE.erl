%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests how storage import detects and propagates deletions made
%%% directly on S3 storage; see storage_import_delete_oct_test_base for the shared
%%% bodies.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_delete_s3_oct_test_SUITE).
-author("Bartosz Walkowicz").

-include("storage_import_oct_test.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    non_empty_directory_deletion_test/1,
    import_continues_after_deletion_test/1,
    file_deletion_purges_metadata_test/1,
    nested_file_deletion_test/1,
    bulk_deletion_test/1
]).

all() -> [
    non_empty_directory_deletion_test,
    file_deletion_purges_metadata_test,
    nested_file_deletion_test,
    bulk_deletion_test,
    import_continues_after_deletion_test
].

-define(IMPORTING_PROVIDER_SELECTOR, krakow).

-define(SUITE_CTX, #storage_import_test_suite_ctx{
    storage_type = s3,
    importing_provider_selector = ?IMPORTING_PROVIDER_SELECTOR,
    non_importing_provider_selector = paris,
    space_owner_selector = space_owner
}).
-define(run_test(), storage_import_delete_oct_test_base:?FUNCTION_NAME(?SUITE_CTX)).


%%%==================================================================
%%% Test functions
%%%===================================================================


non_empty_directory_deletion_test(_Config) -> ?run_test().
import_continues_after_deletion_test(_Config) -> ?run_test().
file_deletion_purges_metadata_test(_Config) -> ?run_test().
nested_file_deletion_test(_Config) -> ?run_test().
bulk_deletion_test(_Config) -> ?run_test().


%%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [
        ?MODULE, sd_test_utils, storage_file_setup_utils,
        storage_import_test_utils, storage_import_delete_oct_test_base
    ],
    opt:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "2op_s3",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {datastore_links_tree_order, 100},
            {cache_to_disk_delay_ms, timer:seconds(1)},
            {cache_to_disk_force_delay_ms, timer:seconds(2)}
        ]}],
        posthook = fun(NewConfig) ->
            storage_import_test_utils:clean_up_after_previous_run(all(), ?SUITE_CTX),
            storage_import_test_utils:mock_space_dir_statbuf_on_flat_storage(?IMPORTING_PROVIDER_SELECTOR),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    storage_import_test_utils:unmock_space_dir_statbuf_on_flat_storage(?IMPORTING_PROVIDER_SELECTOR),
    oct_background:end_per_suite().


init_per_testcase(Case, Config) ->
    storage_import_delete_oct_test_base:init_per_testcase(Case, ?SUITE_CTX, Config).


end_per_testcase(Case, Config) ->
    storage_import_delete_oct_test_base:end_per_testcase(Case, ?SUITE_CTX, Config).
