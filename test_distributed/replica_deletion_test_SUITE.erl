%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests of replica_deletion mechanism
%%% @end
%%%-------------------------------------------------------------------
-module(replica_deletion_test_SUITE).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/logging.hrl").

-export([init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1, all/0]).

%% tests
-export([
    successful_replica_deletion_test/1,
    failed_replica_deletion_test/1,
    canceled_replica_deletion_test/1,
    throttling_test/1
]).

all() -> [
    successful_replica_deletion_test,
    failed_replica_deletion_test,
    canceled_replica_deletion_test,
    throttling_test
].

-define(SPACE_ID, <<"space1">>).
-define(DELETION_TYPE, test_deletion_type).
-define(JOB_ID(N), <<"job-", ?CASE/binary, (integer_to_binary(N))/binary>>).
-define(UUID(N, Ref), <<"uuid-", ?CASE/binary, (integer_to_binary(N))/binary, "_", Ref/binary>>).
-define(CASE, (atom_to_binary(?FUNCTION_NAME, utf8))).
-define(BLOCK(Offset, Size), #file_block{offset = Offset, size = Size}).
-define(VV, #{}).
-define(RUN_TEST(Config, TestFun, Setups),
    run_test(Config, TestFun, ?FUNCTION_NAME, Setups)).

-define(SETUPS, [
    % Tests are run with 2 parameters: FilesNum and JobsNum.
    % Number of total replica_deletion requests is equal to FilesNum * JobsNum.
    % {FileNums, JobNums}
    {1, 1},
    {10, 1},
    {100, 1},
    {1000, 1},
    {10000, 1},
    {1000, 10},
    {100, 100},
    {10, 1000},
    {1, 10000}
]).

-define(COUNTER_ID(Uuid, JobId), {Uuid, JobId}).

%%%===================================================================
%%% API
%%%===================================================================

successful_replica_deletion_test(Config) ->
    ?RUN_TEST(Config, fun successful_replica_deletion_test_base/3, ?SETUPS).

failed_replica_deletion_test(Config) ->
    ?RUN_TEST(Config, fun failed_replica_deletion_test_base/3, ?SETUPS).

canceled_replica_deletion_test(Config) ->
    ?RUN_TEST(Config, fun canceled_replica_deletion_test_base/3, ?SETUPS).

throttling_test(Config) ->
    % This test checks whether throttling works properly in replica_deletion_master process
    % It ensures whether no more requests are handled than
    % replica_deletion_max_parallel_requests environment variable allows.
    % NOTE!!! replica_deletion_max_parallel_requests is decreased to 10 in init_per_testcase
    [W1 | _] = ?config(op_worker_nodes, Config),
    {ok, MaxParallelRequests} = test_utils:get_env(W1, op_worker, replica_deletion_max_parallel_requests),
    ?RUN_TEST(Config, fun throttling_test_base/3, [
        % {FilesNum, JobsNum}
        % test with number of requests less than MaxParallelRequests
        {MaxParallelRequests - 1, 1},
        % test with number of requests equal to MaxParallelRequests
        {MaxParallelRequests, 1},
        % test with number of requests greater than MaxParallelRequests
        {MaxParallelRequests + 1, 1},
        % test with number of requests 10 times greater than MaxParallelRequests
        {MaxParallelRequests, 10}
    ]).

%%%===================================================================
%%% Test base
%%%===================================================================

successful_replica_deletion_test_base(Config, FilesNum, JobsNum) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    ProviderId2 = ?GET_DOMAIN_BIN(W2),
    TestFileSize = 10,
    JobIds = [?JOB_ID(N) || N <- lists:seq(1, JobsNum)],
    UuidsAndJobIds = [{?UUID(N, JobId), JobId} || N <- lists:seq(1, FilesNum), JobId <- JobIds],
    {ok, CSPid} = countdown_server:start_link(self(), W1),

    mock_replica_deletion_request_confirmation(W2),
    mock_deletion_predicate(W1, true),
    mock_delete_blocks(W1),
    mock_process_result_success(W1, CSPid, TestFileSize),

    Stopwatch0 = stopwatch:start(),
    CounterIds = lists:foldl(fun({Uuid, JobId}, CounterIdsAcc) ->
        Request = prepare_deletion_request(W1, Uuid, ProviderId2, [?BLOCK(0, TestFileSize)], ?VV),
        CounterId = countdown_server:init_counter(W1, 1, ?COUNTER_ID(Uuid, JobId)),
        request_deletion(W1, Request, ?SPACE_ID, JobId, ?DELETION_TYPE),
        [CounterId | CounterIdsAcc]
    end, [], UuidsAndJobIds),
    ct:pal("Scheduled in: ~p s.", [stopwatch:read_seconds(Stopwatch0, float)]),

    Stopwatch = stopwatch:start(),
    countdown_server:await_all(W1, CounterIds, timer:seconds(600)),
    ct:pal("Finished in: ~p s.", [stopwatch:read_seconds(Stopwatch, float)]),

    ?assertEqual(false, is_replica_deletion_master_alive(W1, ?SPACE_ID), 10).

failed_replica_deletion_test_base(Config, FilesNum, JobsNum) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    ProviderId2 = ?GET_DOMAIN_BIN(W2),
    TestFileSize = 10,
    JobIds = [?JOB_ID(N) || N <- lists:seq(1, JobsNum)],
    UuidsAndJobIds = [{?UUID(N, JobId), JobId} || N <- lists:seq(1, FilesNum), JobId <- JobIds],
    {ok, CSPid} = countdown_server:start_link(self(), W1),

    mock_replica_deletion_request_refusal(W2),
    mock_process_result_failure(W1, CSPid),

    Stopwatch0 = stopwatch:start(),
    CounterIds = lists:foldl(fun({Uuid, JobId}, CounterIdsAcc) ->
        Request = prepare_deletion_request(W1, Uuid, ProviderId2, [?BLOCK(0, TestFileSize)], ?VV),
        CounterId = countdown_server:init_counter(W1, 1, ?COUNTER_ID(Uuid, JobId)),
        request_deletion(W1, Request, ?SPACE_ID, JobId, ?DELETION_TYPE),
        [CounterId | CounterIdsAcc]
    end, [], UuidsAndJobIds),
    ct:pal("Scheduled in: ~p s.", [stopwatch:read_seconds(Stopwatch0, float)]),

    Stopwatch = stopwatch:start(),
    countdown_server:await_all(W1, CounterIds, timer:seconds(600)),
    ct:pal("Finished in: ~p s.", [stopwatch:read_seconds(Stopwatch, float)]),

    ?assertEqual(false, is_replica_deletion_master_alive(W1, ?SPACE_ID), 10).

canceled_replica_deletion_test_base(Config, FilesNum, JobsNum) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    ProviderId2 = ?GET_DOMAIN_BIN(W2),
    JobIds = [?JOB_ID(N) || N <- lists:seq(1, JobsNum)],
    UuidsAndJobIds = [{?UUID(N, JobId), JobId} || N <- lists:seq(1, FilesNum), JobId <- JobIds],
    TestFileSize = 10,
    {ok, CSPid} = countdown_server:start_link(self(), W1),

    mock_replica_deletion_request_confirmation(W2),
    mock_process_result_cancel(W1, CSPid),
    mock_deletion_predicate(W1, false),

    Stopwatch0 = stopwatch:start(),
    CounterIds = lists:foldl(fun({Uuid, JobId}, CounterIdsAcc) ->
        Request = prepare_deletion_request(W1, Uuid, ProviderId2, [?BLOCK(0, TestFileSize)], ?VV),
        CounterId = countdown_server:init_counter(W1, 1, ?COUNTER_ID(Uuid, JobId)),
        request_deletion(W1, Request, ?SPACE_ID, JobId, ?DELETION_TYPE),
        [CounterId | CounterIdsAcc]
    end, [], UuidsAndJobIds),
    ct:pal("Scheduled in: ~p s.", [stopwatch:read_seconds(Stopwatch0, float)]),

    Stopwatch = stopwatch:start(),
    countdown_server:await_all(W1, CounterIds, timer:seconds(600)),
    ct:pal("Finished in: ~p s.", [stopwatch:read_seconds(Stopwatch, float)]),

    ?assertEqual(false, is_replica_deletion_master_alive(W1, ?SPACE_ID), 10).

throttling_test_base(Config, FilesNum, JobsNum) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    ProviderId2 = ?GET_DOMAIN_BIN(W2),
    TestFileSize = 10,
    {ok, MaxParallelRequests} = test_utils:get_env(W1, op_worker, replica_deletion_max_parallel_requests),
    JobIds = [?JOB_ID(N) || N <- lists:seq(1, JobsNum)],
    UuidsAndJobIds = [{?UUID(N, JobId), JobId} || N <- lists:seq(1, FilesNum), JobId <- JobIds],
    {ok, CSPid} = countdown_server:start_link(self(), W1),

    mock_replica_deletion_request_confirmation(W2),
    capture_replica_deletion_changes_handle_confirmation(W1, CSPid),
    mock_deletion_predicate(W1, true),
    mock_delete_blocks(W1),

    Stopwatch0 = stopwatch:start(),
    ScheduledCounters = lists:foldl(fun({Uuid, JobId}, ScheduledCountersAcc) ->
        Request = prepare_deletion_request(W1, Uuid, ProviderId2, [?BLOCK(0, TestFileSize)], ?VV),
        % this counter will be decreased when request is scheduled
        ScheduledCounterId = countdown_server:init_counter(W1, 1),
        % this counter will be decreased when request is processed
        ProcessedCounterId = countdown_server:init_counter(W1, 1, ?COUNTER_ID(Uuid, JobId)),
        spawn(fun() ->
            request_deletion(W1, Request, ?SPACE_ID, JobId, ?DELETION_TYPE),
            % ScheduledCounterId will be decreased immediately after scheduling process returns
            % (is released from blocking call to replica_deletion_master).
            % Return ProcessedCounterId as counter data, after decreasing ScheduledCounterId.
            countdown_server:decrease(CSPid, ScheduledCounterId, ProcessedCounterId)
        end),
        [ScheduledCounterId | ScheduledCountersAcc]
    end, [], UuidsAndJobIds),
    ct:pal("Scheduled in: ~p s.", [stopwatch:read_seconds(Stopwatch0, float)]),

    Stopwatch = stopwatch:start(),
    throttle_test_loop(W1, #{W1 => ScheduledCounters}, MaxParallelRequests),
    ct:pal("Finished in: ~p s.", [stopwatch:read_seconds(Stopwatch, float)]),

    ?assertEqual(false, is_replica_deletion_master_alive(W1, ?SPACE_ID), 10).

throttle_test_loop(_Worker, ScheduledCounters, _MaxParallelRequests) when map_size(ScheduledCounters) == 0 ->
    ok;
throttle_test_loop(Worker, ScheduledCounters, MaxParallelRequests) ->
    % MaxParallelRequests will return immediately, the rest will be blocked
    {ScheduledCountersData, RestScheduledCounters} =
        countdown_server:await_many(ScheduledCounters, timer:seconds(20), MaxParallelRequests),

    % Ensure that no more requests were scheduled.
    % Scheduling processes should be blocked on calls to replica_deletion_master.
    countdown_server:not_received_any(Worker, RestScheduledCounters, timer:seconds(20)),

    % Extract ids of counters responsible for counting number of processed requests.
    % ProcessedCounterId is passed to countdown_server when decreasing ScheduledCounter.
    ProcessedCountersPerScheduledCounters = maps:get(Worker, ScheduledCountersData, #{}),
    ProcessedCounters = lists:flatten(maps:values(ProcessedCountersPerScheduledCounters)),

    % await for all requests to be processed
    ProcessedCountersData = countdown_server:await_all(Worker, ProcessedCounters, timer:seconds(60)),

    % Extract Args for notifying replica_deletion_master about handled requests.
    % Args are passed to countdown_server when ProcessedCounter is decreased in
    % replica_deletion_changes:handle_confirmation mock.
    NotifyArgsPerProcessedCounters = maps:get(Worker, ProcessedCountersData, #{}),

    % notify replica_deletion_master about processed requests, so that it will start other requests
    lists:foreach(fun([Args]) ->
        ok = rpc:call(Worker, replica_deletion_master, notify_handled_request, Args)
    end, maps:values(NotifyArgsPerProcessedCounters)),

    % continue test for RestScheduledCounters
    throttle_test_loop(Worker, RestScheduledCounters, MaxParallelRequests).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        lists:foreach(fun(Worker) ->
            test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_stream_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
            test_utils:set_env(Worker, ?APP_NAME, prefetching, off),
            test_utils:set_env(Worker, ?APP_NAME, rerun_transfers, false),
            test_utils:set_env(Worker, op_worker, max_file_replication_retries_per_file, 5),
            test_utils:set_env(Worker, op_worker, max_eviction_retries_per_file_replica, 5)
        end, ?config(op_worker_nodes, NewConfig2)),

        application:start(ssl),
        hackney:start(),
        initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig2, "env_desc.json"), NewConfig2)
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, countdown_server, ?MODULE]}
        | Config
    ].

init_per_testcase(throttling_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    {ok, OldValue} = test_utils:get_env(hd(Workers), op_worker, replica_deletion_max_parallel_requests),
    ok = test_utils:set_env(Workers, op_worker, replica_deletion_max_parallel_requests, 10),
    init_per_testcase(default, [{old_replica_deletion_max_parallel_requests, OldValue} | Config]);
init_per_testcase(_Case, Config) ->
    Config2 = sort_workers(Config),
    ct:timetrap(timer:minutes(20)),
    lfm_proxy:init(Config2).

end_per_testcase(throttling_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    OldValue = ?config(old_replica_deletion_max_parallel_requests, Config),
    ok = test_utils:set_env(Workers, op_worker, replica_deletion_max_parallel_requests, OldValue),
    end_per_testcase(default, Config);
end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).

end_per_suite(Config) ->
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    application:stop(ssl),
    initializer:teardown_storage(Config).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

prepare_deletion_request(Worker, FileUuid, Requestee, Blocks, VersionVector) ->
    rpc:call(Worker, replica_deletion_master, prepare_deletion_request, [FileUuid, Requestee, Blocks, VersionVector]).

request_deletion(Worker, Req, SpaceId, JobId, JobType) ->
    rpc:call(Worker, replica_deletion_master, request_deletion, [SpaceId, Req, JobId, JobType], infinity).

mock_replica_deletion_request_confirmation(Worker) ->
    ok = test_utils:mock_new(Worker, replica_deletion_changes),
    ok = test_utils:mock_expect(Worker, replica_deletion_changes, can_support_deletion, fun(RD) ->
        {true, RD#replica_deletion.requested_blocks}
    end).

mock_replica_deletion_request_refusal(Worker) ->
    ok = test_utils:mock_new(Worker, replica_deletion_changes),
    ok = test_utils:mock_expect(Worker, replica_deletion_changes, can_support_deletion, fun(_RD) -> false end).

mock_delete_blocks(Worker) ->
    ok = test_utils:mock_new(Worker, replica_deletion_req),
    ok = test_utils:mock_expect(Worker, replica_deletion_req,  delete_blocks, fun(_, _, _) -> ok end).

mock_process_result_success(Worker, CountdownServer, FileSize) ->
    mock_process_result(Worker, CountdownServer, {ok, FileSize}).

mock_process_result_failure(Worker, CountdownServer) ->
    mock_process_result(Worker, CountdownServer, {error, replica_deletion_refused}).

mock_process_result_cancel(Worker, CountdownServer) ->
    mock_process_result(Worker, CountdownServer,
        [{error, precondition_not_satisfied}, {error, canceled}]).

mock_process_result(Worker, CountdownServer, AcceptedResults) ->
    AcceptedResults2 = utils:ensure_list(AcceptedResults),
    ok = test_utils:mock_new(Worker, replica_deletion_master),
    ok = test_utils:mock_expect(Worker, replica_deletion_master,  process_result,
        fun(_SpaceId, FileUuid, Result, JobId, _JobType) ->
            case lists:member(Result, AcceptedResults2) of
                true ->
                    countdown_server:decrease(CountdownServer, ?COUNTER_ID(FileUuid, JobId), {FileUuid, Result});
                false ->
                    ok
            end
        end
    ).

mock_deletion_predicate(Worker, Result) ->
    ok = test_utils:mock_new(Worker, replica_deletion_worker),
    ok = test_utils:mock_expect(Worker, replica_deletion_worker, custom_predicate,
        fun(_SpaceId, _JobType, _JobId) -> Result end
    ).

capture_replica_deletion_changes_handle_confirmation(Worker, CountdownServer) ->
    ok = test_utils:mock_new(Worker, replica_deletion_changes),
    ok = test_utils:mock_expect(Worker, replica_deletion_changes, handle_confirmation,
        fun(#document{value = #replica_deletion{
            file_uuid = FileUuid,
            space_id = SpaceId,
            job_id = JobId,
            job_type = JobType
        }}) ->
            % Pass args for notifying replica_deletion_master about processed request to countdown_server
            Args = [SpaceId, JobId, JobType],
            ok = countdown_server:decrease(CountdownServer, ?COUNTER_ID(FileUuid, JobId), [Args])
        end
    ).

is_replica_deletion_master_alive(Worker, SpaceId) ->
    is_pid(rpc:call(Worker, global, whereis_name, [{replica_deletion_master, SpaceId}])).


sort_workers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:keyreplace(op_worker_nodes, 1, Config, {op_worker_nodes, lists:sort(Workers)}).

run_test(Config, Testcase, TestcaseName, FilesAndJobsNums) ->
    Results = lists:map(fun({FilesNum, JobsNum}) ->
        try
            ct:print("Starting testcase ~p with FilesNum = ~p and JobsNum = ~p.", [TestcaseName, FilesNum, JobsNum]),
            Testcase(Config, FilesNum, JobsNum),
            ok
        catch
            E:R ->
                ct:print("Testcase ~p failed due to ~p for FilesNum = ~p, JobsNum = ~p.~nStacktrace: ~n~p",
                    [TestcaseName, {E, R}, FilesNum, JobsNum, erlang:get_stacktrace()]),
                error
        after
            cleanup(Config)
        end
    end, FilesAndJobsNums),
    ?assertEqual(true, lists:all(fun(E) -> E =:= ok end, Results)).

cleanup(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(W) -> catch countdown_server:stop(W) end, Workers),
    test_utils:mock_unload(Workers, [
        replica_deletion_changes, replica_deletion_req, replica_deletion_master, replica_deletion_worker
    ]).