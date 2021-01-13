%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of db_sync changes requesting in multi provider environment with simulated apply errors
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_changes_requesting_with_errors_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    test_with_simulation_of_apply_errors/1,
    handling_changes_separately_test_with_simulation_of_apply_errors/1,
    handling_changes_separately_test_with_simulation_of_apply_errors_and_delays/1
]).

all() -> [
    test_with_simulation_of_apply_errors,
    handling_changes_separately_test_with_simulation_of_apply_errors,
    handling_changes_separately_test_with_simulation_of_apply_errors_and_delays
].

%%%===================================================================
%%% Test functions
%%%
%%% Note: some preparation (e.g. setting environment variables) is done in init_per_testcase 
%%% as information needed to revert changes in end_per_testcase/2 (e.g. set original values of 
%%% environment variables) has to be added to Config.
%%% Tests that seem to be identical with test from dbsync_changes_requesting_test_SUITE
%%% differ in init_per_testcase/2
%%%===================================================================

test_with_simulation_of_apply_errors(Config) ->
    dbsync_changes_requesting_test_base:generic_test_skeleton(Config, medium).

handling_changes_separately_test_with_simulation_of_apply_errors(Config) ->
    dbsync_changes_requesting_test_base:handle_each_correlation_in_out_stream_separattely(Config),
    dbsync_changes_requesting_test_base:generic_test_skeleton(Config, medium).

handling_changes_separately_test_with_simulation_of_apply_errors_and_delays(Config) ->
    dbsync_changes_requesting_test_base:handle_each_correlation_in_out_stream_separattely(Config),
    dbsync_changes_requesting_test_base:add_delay_to_in_stream_handler(Config),
    dbsync_changes_requesting_test_base:generic_test_skeleton(Config, small).

%%%===================================================================
%%% SetUp and TearDown functions
%%% Note: mocking of dbsync streams can result in crash logs as
%%% meck kills streams creating mock - ignore these errors
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> multi_provider_file_ops_test_base:init_env(NewConfig) end,
    [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base, dbsync_changes_requesting_test_base]},
        {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    multi_provider_file_ops_test_base:teardown_env(Config).

%%%===================================================================

init_per_testcase(test_with_simulation_of_apply_errors = Case, Config) ->
    Workers =  ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [dbsync_changes]),
    % Note: dbsync_communicator mock is initialized here:
    MockedConfig = multi_provider_file_ops_test_base:mock_sync_and_rtransfer_errors(Config),
    init_per_testcase(?DEFAULT_CASE(Case), MockedConfig);
init_per_testcase(Case, Config) when
    Case =:= handling_changes_separately_test_with_simulation_of_apply_errors orelse
        Case =:= handling_changes_separately_test_with_simulation_of_apply_errors_and_delays ->
    Workers =  ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [dbsync_changes, dbsync_out_stream]),
    Config2 = dbsync_changes_requesting_test_base:use_single_doc_broadcast_batch(Config),
    % Note: dbsync_in_stream_worker and dbsync_communicator mocks are initialized here:
    Config3 = multi_provider_file_ops_test_base:mock_sync_and_rtransfer_errors(Config2, false),
    init_per_testcase(?DEFAULT_CASE(Case), Config3);
init_per_testcase(_Case, Config) ->
    Workers =  ?config(op_worker_nodes, Config),
    test_utils:set_env(Workers, ?APP_NAME, seq_history_interval, 1),
    test_utils:set_env(Workers, ?APP_NAME, dbsync_handler_spawn_size, 100000),
    ct:timetrap({minutes, 60}),
    lfm_proxy:init(Config).

%%%===================================================================

end_per_testcase(test_with_simulation_of_apply_errors = Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [dbsync_changes]),
    % Note: dbsync_communicator is unmocked here
    multi_provider_file_ops_test_base:unmock_sync_and_rtransfer_errors(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(Case, Config) when 
    Case =:= handling_changes_separately_test_with_simulation_of_apply_errors orelse 
        Case =:= handling_changes_separately_test_with_simulation_of_apply_errors_and_delays ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [dbsync_changes, dbsync_out_stream]),
    dbsync_changes_requesting_test_base:use_default_broadcast_batch_size(Config),
    % Note: dbsync_in_stream_worker and dbsync_communicator are unmocked here
    multi_provider_file_ops_test_base:unmock_sync_and_rtransfer_errors(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
