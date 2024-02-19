%%%--------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests storage import.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_oct_test_SUITE).
-author("Katarzyna Such").

-include_lib("onenv_ct/include/oct_background.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("space_setup_utils.hrl").
-include_lib("storage_import_oct_test.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    empty_import_test/1,
    create_directory_import_test/1
]).

-define(TEST_CASES, [
    empty_import_test,
    create_directory_import_test
]).

-define(RANDOM_PROVIDER(), ?RAND_ELEMENT([krakow, paris])).

all() -> ?ALL(?TEST_CASES).

-define(RUN_TEST(),
    try
        storage_import_oct_test_base:?FUNCTION_NAME(#storage_import_test_config{
            p1_selector = krakow,
            p2_selector = paris,
            space_selector = space_krk_par_p})
    catch __TYPE:__REASON:__STACKTRACE ->
        ct:pal("Test failed due to ~p:~p.~nStacktrace: ~p", [__TYPE, __REASON, __STACKTRACE]),
        error(test_failed)
    end
).

%%%==================================================================
%%% Test functions
%%%===================================================================

empty_import_test(_Config) ->
    ?RUN_TEST().


create_directory_import_test(_Config) ->
    ?RUN_TEST().

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    ModulesToLoad = [?MODULE, initializer, sd_test_utils, storage_import_oct_test_base],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {dbsync_changes_broadcast_interval, timer:seconds(1)},
            {datastore_links_tree_order, 100},
            {cache_to_disk_delay_ms, timer:seconds(1)},
            {cache_to_disk_force_delay_ms, timer:seconds(2)}
        ]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
