%%%--------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests storage import on POSIX storage.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_posix_oct_test_SUITE).
-author("Katarzyna Such").

-include("storage_import_oct_test.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    import_empty_storage_test/1,
    import_empty_directory_test/1,
    import_directory_error_test/1,
    import_directory_check_user_id_test/1,

    import_empty_file_test/1,
    import_file_with_content_test/1
]).

all() -> [
    import_empty_storage_test,
    import_empty_directory_test,
    import_directory_error_test,
    import_directory_check_user_id_test,
    import_empty_file_test,
    import_file_with_content_test
].

-define(SUITE_CTX, #storage_import_test_suite_ctx{
    storage_type = posix,
    importing_provider_selector = krakow,
    non_importing_provider_selector = paris,
    space_owner_selector = user1
}).
-define(run_test(), storage_import_oct_test_base:?FUNCTION_NAME(?SUITE_CTX)).


%%%==================================================================
%%% Test functions
%%%===================================================================


import_empty_storage_test(_Config) ->
    ?run_test().


import_empty_directory_test(_Config) ->
    ?run_test().


import_directory_error_test(_Config) ->
    ?run_test().


import_directory_check_user_id_test(_Config) ->
    ?run_test().


import_empty_file_test(_Config) ->
    ?run_test().


import_file_with_content_test(_Config) ->
    ?run_test().


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [?MODULE, sd_test_utils, storage_import_oct_test_base],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {dbsync_changes_broadcast_interval, timer:seconds(1)},
            {datastore_links_tree_order, 100},
            {cache_to_disk_delay_ms, timer:seconds(1)},
            {cache_to_disk_force_delay_ms, timer:seconds(2)}
        ]}],
        posthook = fun(NewConfig) ->
            storage_import_oct_test_base:clean_up_after_previous_run(all(), ?SUITE_CTX),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).


end_per_testcase(_Case, _Config) ->
    ok.
