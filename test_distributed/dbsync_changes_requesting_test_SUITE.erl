%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Basic tests of db_sync changes requesting in multi provider environment
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_changes_requesting_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    basic_changes_requests_test/1,
    handling_changes_separately_test/1,
    test_with_simulated_apply_delays/1
]).

all() -> [
    basic_changes_requests_test,
    handling_changes_separately_test,
    test_with_simulated_apply_delays
].

%%%===================================================================
%%% Test functions
%%%
%%% Note: some preparation (e.g. setting environment variables) is done in init_per_testcase 
%%% as information needed to revert changes in end_per_testcase/2 (e.g. to set original values of
%%% environment variables) has to be added to Config.
%%% Tests that seem to be identical with tests from dbsync_changes_requesting_with_errors_test_SUITE
%%% differ in init_per_testcase/2
%%%===================================================================

basic_changes_requests_test(Config) ->
    dbsync_changes_requesting_test_base:generic_test_skeleton(Config, large).

handling_changes_separately_test(Config) ->
    dbsync_changes_requesting_test_base:handle_each_correlation_in_out_stream_separattely(Config),
    dbsync_changes_requesting_test_base:generic_test_skeleton(Config, medium).

test_with_simulated_apply_delays(Config) ->
    dbsync_changes_requesting_test_base:add_delay_to_in_stream_handler(Config),
    dbsync_changes_requesting_test_base:generic_test_skeleton(Config, medium).

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

init_per_testcase(basic_changes_requests_test = Case, Config) ->
    Workers =  ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [dbsync_communicator, dbsync_changes]),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(handling_changes_separately_test = Case, Config) ->
    Workers =  ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [dbsync_communicator, dbsync_changes, dbsync_out_stream]),
    Config2 = dbsync_changes_requesting_test_base:use_single_doc_broadcast_batch(Config),
    init_per_testcase(?DEFAULT_CASE(Case), Config2);
init_per_testcase(test_with_simulated_apply_delays = Case, Config) ->
    Workers =  ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [dbsync_communicator, dbsync_changes, dbsync_in_stream_worker]),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(_Case, Config) ->
    Workers =  ?config(op_worker_nodes, Config),
    test_utils:set_env(Workers, ?APP_NAME, seq_history_interval, 1),
    test_utils:set_env(Workers, ?APP_NAME, dbsync_handler_spawn_size, 100000),
    ct:timetrap({minutes, 60}),
    lfm_proxy:init(Config).

%%%===================================================================

end_per_testcase(basic_changes_requests_test = Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [dbsync_communicator, dbsync_changes]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(handling_changes_separately_test = Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [dbsync_communicator, dbsync_changes, dbsync_out_stream]),
    dbsync_changes_requesting_test_base:use_default_broadcast_batch_size(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(test_with_simulated_apply_delays = Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [dbsync_communicator, dbsync_changes, dbsync_in_stream_worker]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
