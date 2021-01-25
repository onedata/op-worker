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
-include("proto/oneprovider/dbsync_messages2.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    basic_changes_requests_test/1,
    handling_changes_separately_test/1,
    test_with_simulated_apply_delays/1,
    requesting_from_custom_provider/1
]).

all() -> [
    basic_changes_requests_test,
    handling_changes_separately_test,
    test_with_simulated_apply_delays,
    requesting_from_custom_provider
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
    dbsync_changes_requesting_test_base:handle_each_correlation_in_out_stream_separately(Config),
    dbsync_changes_requesting_test_base:generic_test_skeleton(Config, medium).

test_with_simulated_apply_delays(Config) ->
    dbsync_changes_requesting_test_base:add_delay_to_in_stream_handler(Config),
    dbsync_changes_requesting_test_base:generic_test_skeleton(Config, medium).

requesting_from_custom_provider(Config0) ->
    % Prepare variables used during test
    User = <<"user1">>,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, User, {6, 0, 0, 1}, 180),
    SpaceName = <<"space1">>,
    SpaceId = SpaceName,
    [Worker1, Worker2 | _] = Workers = ?config(op_worker_nodes, Config),
    WorkerToProviderId = maps:from_list(lists:map(fun(Worker) ->
        {Worker, rpc:call(Worker, oneprovider, get_id_or_undefined, [])}
    end, Workers)),
    W2Id = maps:get(Worker2, WorkerToProviderId),

    test_utils:mock_expect(Worker1, dbsync_in_stream, handle_cast, fun
        ({changes_batch, MsgId, ReferenceProvId, #internal_changes_batch{sender_id = SenderId} = Batch}, State)
            when SenderId =:= W2Id ->
            case get(requesting_test) of
                true ->
                    {noreply, State};
                _ ->
                    put(requesting_test, true),
                    MockedBatch = Batch#internal_changes_batch{since = 0, until = 0, timestamp = 0, docs = []},
                    meck:passthrough([{changes_batch, MsgId, ReferenceProvId, MockedBatch}, State])
            end;
        (Msg, State) ->
            meck:passthrough([Msg, State])
    end),

    ok = rpc:call(Worker1, internal_services_manager, stop_service,
        [dbsync_worker_sup, <<"dbsync_in_stream", SpaceId/binary>>, SpaceId]),
    ok = rpc:call(Worker1, internal_services_manager, start_service,
        [dbsync_worker_sup, <<"dbsync_in_stream", SpaceId/binary>>, start_in_stream, stop_in_stream, [SpaceId], SpaceId]),

    DirSizes = [10, 20, 1, 5, 2, 1],
    % Create dirs with sleeps to better mix outgoing changes from different providers
    DirsList1 = dbsync_changes_requesting_test_base:create_dirs_batch(Config, SpaceName, Workers, DirSizes),
    timer:sleep(10000),
    DirsList2 = dbsync_changes_requesting_test_base:create_dirs_batch(Config, SpaceName, lists:reverse(Workers), DirSizes),
    timer:sleep(5000),
    DirsList3 = dbsync_changes_requesting_test_base:create_dirs_batch(Config, SpaceName, Workers, DirSizes),

    dbsync_changes_requesting_test_base:verify_dirs_batch(Config, DirsList1),
    dbsync_changes_requesting_test_base:verify_dirs_batch(Config, DirsList2),
    dbsync_changes_requesting_test_base:verify_dirs_batch(Config, DirsList3),

    % TODO VFS-7205 - why dbsync_changes_requesting_test_base:verify_sequences_correlation
    % can be called here when it is expected to fail?
    ?assertEqual(true, catch dbsync_test_utils:are_all_seqs_equal(Workers, SpaceName), 60),

    test_utils:mock_expect(Worker1, dbsync_in_stream, handle_cast, fun(Msg, State) ->
        meck:passthrough([Msg, State])
    end),
    Timestamp0 = rpc:call(Worker1, global_clock, timestamp_seconds, []),

    DirsList4 = dbsync_changes_requesting_test_base:create_dirs_batch(Config, SpaceName, Workers, DirSizes),
    dbsync_changes_requesting_test_base:verify_dirs_batch(Config, DirsList4),

    ?assertEqual(true, catch dbsync_test_utils:are_all_seqs_and_timestamps_equal(Workers, SpaceName, Timestamp0), 60),
    dbsync_changes_requesting_test_base:verify_sequences_correlation(WorkerToProviderId, SpaceId).

%%%===================================================================
%%% SetUp and TearDown functions
%%% Note: mocking of dbsync streams can result in crash logs as
%%% meck kills streams creating mock - ignore these errors
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig2 = multi_provider_file_ops_test_base:init_env(NewConfig),
        dbsync_changes_requesting_test_base:create_tester_session(NewConfig2),
        Workers =  ?config(op_worker_nodes, NewConfig2),
        % space_logic is already mocked by initializer
        test_utils:mock_expect(Workers, space_logic, get_latest_emitted_seq, fun(_, _) ->
            {ok, 1000000000}
        end),
        NewConfig2
    end,
    [{?LOAD_MODULES, [initializer, dbsync_test_utils, dbsync_changes_requesting_test_base]},
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
init_per_testcase(requesting_from_custom_provider = Case, Config) ->
    [Worker1 | _] = Workers =  ?config(op_worker_nodes, Config),
    ok = test_utils:set_env(Workers, ?APP_NAME, dbsync_out_stream_handling_interval, timer:seconds(1)),
    ok = test_utils:set_env(Workers, ?APP_NAME, dbsync_zone_check_base_interval, timer:seconds(5)),
    test_utils:mock_new(Worker1, dbsync_in_stream),
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
    [Worker1 | _ ] = Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [dbsync_communicator, dbsync_changes, dbsync_in_stream_worker]),
    % Restart stream as unmocking kills stream process
    SpaceId = <<"space1">>,
    ok = rpc:call(Worker1, internal_services_manager, stop_service,
        [dbsync_worker_sup, <<"dbsync_in_stream", SpaceId/binary>>, SpaceId]),
    ok = rpc:call(Worker1, internal_services_manager, start_service,
        [dbsync_worker_sup, <<"dbsync_in_stream", SpaceId/binary>>, start_in_stream, stop_in_stream, [SpaceId], SpaceId]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(requesting_from_custom_provider = Case, Config) ->
    [Worker1 | _] =  ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Worker1, dbsync_in_stream),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
