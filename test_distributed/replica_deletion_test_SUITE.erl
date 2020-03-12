%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
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

-export([init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1, all/0]).

%% tests
-export([
    successful_replica_deletion_test/1,
    failed_replica_deletion_test/1,
    canceled_replica_deletion_test/1
]).

all() -> [
    successful_replica_deletion_test,
    failed_replica_deletion_test,
    canceled_replica_deletion_test
].

-define(SPACE_ID, <<"space1">>).
-define(DELETION_TYPE, test_deletion_type).
-define(JOB_ID(N), <<"job-", ?CASE/binary, (integer_to_binary(N))/binary>>).
-define(UUID(N, Ref), <<"uuid-", ?CASE/binary, (integer_to_binary(N))/binary, "_", Ref/binary>>).
-define(CASE, (atom_to_binary(?FUNCTION_NAME, utf8))).
-define(BLOCK(Offset, Size), #file_block{offset = Offset, size = Size}).
-define(VV, #{}).

-define(SETUPS, [
%   {FileNums, JobNums}
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

%%%===================================================================
%%% API
%%%===================================================================

successful_replica_deletion_test(Config) ->
    run_test(Config, fun successful_replica_deletion_test_base/3, ?SETUPS).

failed_replica_deletion_test(Config) ->
    run_test(Config, fun failed_replica_deletion_test_base/3, ?SETUPS).

canceled_replica_deletion_test(Config) ->
    run_test(Config, fun canceled_replica_deletion_test_base/3, ?SETUPS).

%%%===================================================================
%%% Test base
%%%===================================================================

successful_replica_deletion_test_base(Config, FilesNum, JobsNum) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    ProviderId2 = ?GET_DOMAIN_BIN(W2),
    TestFileSize = 10,
    JobIds = [?JOB_ID(N) || N <- lists:seq(1, JobsNum)],
    UuidsAndJobIds = [{?UUID(N, JobId), JobId} || N <- lists:seq(1, FilesNum), JobId <- JobIds],
    ReqsAndJobIds = [{prepare_deletion_request(W1, U, ProviderId2, [?BLOCK(0, TestFileSize)], ?VV), J} || {U, J} <- UuidsAndJobIds],
    {ok, CSPid} = countdown_server:start_link(self(), W1),
    mock_replica_deletion_request_confirmation(W2),
    CounterRefs = [countdown_server:init_counter(W1, FilesNum) || _ <- JobIds],
    JobIdsToCounters = maps:from_list(lists:zip(JobIds, CounterRefs)),

    mock_delete_blocks(W1),
    mock_process_result_success(W1, CSPid, JobIdsToCounters, TestFileSize),
    mock_deletion_predicate(W1, true),

    StartTime0 = time_utils:system_time_millis(),
    lists:foreach(fun({Req, JobId}) ->
        request_deletion(W1, Req, ?SPACE_ID, JobId, ?DELETION_TYPE)
    end, ReqsAndJobIds),
    EndTime0 = time_utils:system_time_millis(),
    ct:pal("Scheduled in: ~p s.", [(EndTime0 - StartTime0) / 1000]),

    StartTime = time_utils:system_time_millis(),
    countdown_server:await_many(W1, CounterRefs, timer:seconds(600)),
    EndTime = time_utils:system_time_millis(),
    ct:pal("Finished in: ~p s.", [(EndTime - StartTime) / 1000]),
    ?assertEqual(false, is_replica_deletion_master_alive(W1, ?SPACE_ID), 10).

failed_replica_deletion_test_base(Config, FilesNum, JobsNum) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    ProviderId2 = ?GET_DOMAIN_BIN(W2),
    JobIds = [?JOB_ID(N) || N <- lists:seq(1, JobsNum)],
    UuidsAndJobIds = [{?UUID(N, JobId), JobId} || N <- lists:seq(1, FilesNum), JobId <- JobIds],
    ReqsAndJobIds = [{prepare_deletion_request(W1, U, ProviderId2, [?BLOCK(0, 10)], ?VV), R} || {U, R} <- UuidsAndJobIds],
    {ok, CSPid} = countdown_server:start_link(self(), W1),
    mock_replica_deletion_request_refusal(W2),
    CounterRefs = [countdown_server:init_counter(W1, FilesNum) || _ <- JobIds],
    JobIdsToCounters = maps:from_list(lists:zip(JobIds, CounterRefs)),

    mock_process_result_failure(W1, CSPid, JobIdsToCounters),

    StartTime0 = time_utils:system_time_millis(),
    lists:foreach(fun({Req, JobId}) ->
        request_deletion(W1, Req, ?SPACE_ID, JobId, ?DELETION_TYPE)
    end, ReqsAndJobIds),
    EndTime0 = time_utils:system_time_millis(),
    ct:pal("Scheduled in: ~p s.", [(EndTime0 - StartTime0) / 1000]),

    StartTime = time_utils:system_time_millis(),
    countdown_server:await_many(W1, CounterRefs, timer:seconds(600)),
    EndTime = time_utils:system_time_millis(),
    ct:pal("Finished in: ~p s.", [(EndTime - StartTime) / 1000]),
    ?assertEqual(false, is_replica_deletion_master_alive(W1, ?SPACE_ID), 10).

canceled_replica_deletion_test_base(Config, FilesNum, JobsNum) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    ProviderId2 = ?GET_DOMAIN_BIN(W2),
    JobIds = [?JOB_ID(N) || N <- lists:seq(1, JobsNum)],
    UuidsAndJobIds = [{?UUID(N, JobId), JobId} || N <- lists:seq(1, FilesNum), JobId <- JobIds],
    ReqsAndJobIds = [{prepare_deletion_request(W1, U, ProviderId2, [?BLOCK(0, 10)], ?VV), R} || {U, R} <- UuidsAndJobIds],
    {ok, CSPid} = countdown_server:start_link(self(), W1),
    mock_replica_deletion_request_confirmation(W2),
    CounterRefs = [countdown_server:init_counter(W1, FilesNum) || _ <- JobIds],
    JobIdsToCounters = maps:from_list(lists:zip(JobIds, CounterRefs)),

    mock_process_result_cancel(W1, CSPid, JobIdsToCounters),
    mock_deletion_predicate(W1, false),

    StartTime0 = time_utils:system_time_millis(),
    lists:foreach(fun({Req, JobId}) ->
        request_deletion(W1, Req, ?SPACE_ID, JobId, ?DELETION_TYPE)
    end, ReqsAndJobIds),
    EndTime0 = time_utils:system_time_millis(),
    ct:pal("Scheduled in: ~p s.", [(EndTime0 - StartTime0) / 1000]),

    StartTime = time_utils:system_time_millis(),
    countdown_server:await_many(W1, CounterRefs, timer:seconds(600)),
    EndTime = time_utils:system_time_millis(),
    ct:pal("Finished in: ~p s.", [(EndTime - StartTime) / 1000]),
    ?assertEqual(false, is_replica_deletion_master_alive(W1, ?SPACE_ID), 10).

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
            test_utils:set_env(Worker, ?APP_NAME, rerun_transfers, false)
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

init_per_testcase(_Case, Config) ->
    Config2 = sort_workers(Config),
    ct:timetrap(timer:minutes(60)),
    lfm_proxy:init(Config2).

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

mock_process_result_success(Worker, CountdownServer, JobIdsToCounters, FileSize) ->
    mock_process_result(Worker, CountdownServer, JobIdsToCounters, {ok, FileSize}).

mock_process_result_failure(Worker, CountdownServer, JobIdsToCounters) ->
    mock_process_result(Worker, CountdownServer, JobIdsToCounters, {error, replica_deletion_refused}).

mock_process_result_cancel(Worker, CountdownServer, JobIdsToCounters) ->
    mock_process_result(Worker, CountdownServer, JobIdsToCounters,
        [{error, precondition_not_satisfied}, {error, canceled}]).

%%ok = test_utils:mock_new(Worker, replica_deletion_master),
%%    ok = test_utils:mock_expect(Worker, replica_deletion_master,  process_result, fun
%%        (_SpaceId, FileUuid, Result = {error, Reason}, JobId, _JobType) when Reason =:= precondition_not_satisfied orelse Reason =:= canceled ->
%%            countdown_server:decrease(CountdownServer, maps:get(JobId, JobIdsToCounters), {FileUuid, Result})
%%    end).

mock_process_result(Worker, CountdownServer, JobIdsToCounters, AcceptedResults) ->
    AcceptedResults2 = utils:ensure_list(AcceptedResults),
    ok = test_utils:mock_new(Worker, replica_deletion_master),
    ok = test_utils:mock_expect(Worker, replica_deletion_master,  process_result,
        fun(_SpaceId, FileUuid, Result, JobId, _JobType) ->
            case lists:member(Result, AcceptedResults2) of
                true ->
                    countdown_server:decrease(CountdownServer, maps:get(JobId, JobIdsToCounters), {FileUuid, Result});
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

is_replica_deletion_master_alive(Worker, SpaceId) ->
    is_pid(rpc:call(Worker, global, whereis_name, [{replica_deletion_master, SpaceId}])).


sort_workers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:keyreplace(op_worker_nodes, 1, Config, {op_worker_nodes, lists:sort(Workers)}).

run_test(Config, Testcase, FilesAndJobsNums) ->
    Results = lists:map(fun({FilesNum, JobsNum}) ->
        try
            Testcase(Config, FilesNum, JobsNum),
            ok
        catch
            E:R ->
                ct:print("Testcase ~p failed due to ~p for FilesNum = ~p, JobsNum = ~p.~nStacktrace: ~n~p",
                    [Testcase, {E, R}, FilesNum, JobsNum, erlang:get_stacktrace()]),
                error
        after
            cleanup(Config)
        end
    end, FilesAndJobsNums),
    ?assertEqual(true, lists:all(fun(E) -> E =:= ok end, Results)).

cleanup(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(W) -> catch countdown_server:stop(W) end, Workers),
    test_utils:mock_unload(Workers, [replica_deletion_changes, replica_deletion_req, replica_deletion_master]).