%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This test checks requests routing inside OP cluster.
%%% @end
%%%--------------------------------------------------------------------
-module(stress_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("cluster_worker/include/elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([stress_test/1, file_meta_basic_operations_test/1, file_meta_basic_operations_test_base/1, stress_test_base/1,
    many_files_creation_test/1, many_files_creation_test_base/1]).

-define(STRESS_CASES, [
    file_meta_basic_operations_test
    %% TODO add simmilar test without mocks within cluster
    %% sequencer_manager_multiple_streams_messages_ordering_test, connection_multi_ping_pong_test,
    %% event_stream_different_file_id_aggregation_test,
    %% event_manager_multiple_subscription_test, event_manager_multiple_clients_test
]).
-define(STRESS_NO_CLEARING_CASES, [
    many_files_creation_test
    %% TODO add no clearing option to other tests
]).

all() ->
    ?STRESS_ALL(?STRESS_CASES, ?STRESS_NO_CLEARING_CASES).

-define(REQUEST_TIMEOUT, timer:seconds(30)).
-define(TIMEOUT, timer:seconds(60)).
-define(call_store(Fun, Args), erlang:apply(file_meta, Fun, Args)).
-define(call(N, M, A), ?call(N, file_meta, M, A)).
-define(call(N, Mod, M, A), rpc:call(N, Mod, M, A)).

%%%===================================================================
%%% Test functions
%%%===================================================================

stress_test(Config) ->
    ?STRESS(Config,[
            {description, "Main stress test function. Links together all cases to be done multiple times as one continous test."},
            {success_rate, 99.9},
            {config, [{name, stress}, {description, "Basic config for stress test"}]}
        ]
    ).
stress_test_base(Config) ->
  performance:stress_test(Config).

%%%===================================================================

file_meta_basic_operations_test(Config) ->
    ?PERFORMANCE(Config, [
        {description, "Performs operations on file meta model"}
      ]
    ).
file_meta_basic_operations_test_base(Config) ->
  model_file_meta_test_SUITE:basic_operations_test_core(Config).

%%%===================================================================

many_files_creation_test(Config) ->
    ?PERFORMANCE(Config, [
        {parameters, [
            [{name, threads_num}, {value, 20}, {description, "Number of threads used during the test."}],
            [{name, files_per_thead}, {value, 10}, {description, "Number of files used by single threads."}]
        ]},
        {description, "Performs multiple datastore operations using many threads. Level - database."}
    ]).
many_files_creation_test_base(Config) ->
    LastFails = ?config(last_fails, Config),
    RepNum = ?config(rep_num, Config),
    case LastFails of
        0 ->
            ok;
        _ ->
            ct:print("many_files_creation_test_base: Sleep because of failures: ~p sek", [2 * LastFails]),
            timer:sleep(timer:seconds(2 * LastFails))
    end,

    [Worker1, Worker2] = Workers = ?config(op_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    FilesPerThead = ?config(files_per_thead, Config),

    % TODO - check why this does not work (function clause in resolve_path):
%%     put(file_beg, binary_to_list(term_to_binary(os:timestamp()))),
    put(file_beg, get_random_string()),
    Master = self(),
    AnswerDesc = get(file_beg),

    case RepNum of
        1 ->
            ?assertMatch({ok, _}, ?call(Worker1, create,
                [{path, <<"/">>}, #file_meta{name = <<"spaces">>, is_scope = true}]));
        _ ->
            ok
    end,

    SpaceNameString = "Space " ++ AnswerDesc,
    SpaceName = list_to_binary(SpaceNameString),
    FullSpaceNameString = "/spaces/" ++ SpaceNameString,
    FullSpaceName = list_to_binary(FullSpaceNameString),
    ?assertMatch({ok, _}, ?call(Worker2, create,
        [{path, <<"/spaces">>}, #file_meta{name = SpaceName, is_scope = true}])),

    CreateFiles = fun(DocsSet) ->
        for(1, FilesPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(create, [{path, FullSpaceName}, #file_meta{name = list_to_binary(DocsSet ++ integer_to_list(I))}]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, 1, CreateFiles),
    OpsNum = ThreadsNum * FilesPerThead,
    {OkNumCL, OkTimeCL, _ErrorNumCL, _ErrorTimeCL, _ErrorsListCL} = count_answers(OpsNum),
    ?assertEqual(OpsNum, OkNumCL),

    Get = fun(DocsSet) ->
        for(1, FilesPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(get, [{path, list_to_binary(FullSpaceNameString ++ "/" ++ DocsSet ++ integer_to_list(I))}]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, 1, Get),
    {OkNum2, OkTime2, _ErrorNum2, _ErrorTime2, ErrorsList2} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList2),
    ?assertEqual(OpsNum, OkNum2),

    ClearRatio = 2,
    NewTN = round(ThreadsNum / ClearRatio),
    NewCT = 1,
    DelOpsNum = FilesPerThead * NewTN,

    ClearMany = fun(DocsSet) ->
        for(1, FilesPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(delete, [{path, list_to_binary(FullSpaceNameString ++ "/" ++ DocsSet ++ integer_to_list(I))}]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, AnswerDesc, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    spawn_at_nodes(Workers, NewTN, NewCT, ClearMany),
    {DelLinkOkNum, DelLinkTime, _DelLinkErrorNum, _DelLinkErrorTime, DelLinkErrorsList} =
        count_answers(DelOpsNum),
    ?assertEqual([], DelLinkErrorsList),
    ?assertEqual(DelOpsNum, DelLinkOkNum),

    FailedNum = ?config(failed_num, Config),
    DocsInDB = (ThreadsNum - NewTN) * FilesPerThead,
    ct:print("Files in system: ~p", [DocsInDB * (RepNum - FailedNum)]),

    [
        #parameter{name = files_in_datastore, value = DocsInDB,
            description = "Files in datastore after test"},
        #parameter{name = create_time, value = OkTimeCL / OkNumCL, unit = "us",
            description = "Average time of creating file"},
        #parameter{name = get_time, value = OkTime2 / OkNum2, unit = "us",
            description = "Average time of get operation"},
        #parameter{name = del_time, value = DelLinkTime / DelOpsNum, unit = "us",
            description = "Average time of delete operation"}
    ].


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

for(N, N, F) ->
    F(N);
for(I, N, F) ->
    F(I),
    for(I + 1, N, F).

spawn_at_nodes(Nodes, Threads, ConflictedThreads, Fun) ->
    spawn_at_nodes(Nodes, [], Threads, 1, 0, ConflictedThreads, Fun, []).

spawn_at_nodes(_Nodes, _Nodes2, 0, _DocsSetNum, _DocNumInSet, _ConflictedThreads, _Fun, Pids) ->
    lists:foreach(fun(Pid) -> Pid ! start end, Pids);
spawn_at_nodes(Nodes, Nodes2, Threads, DocsSet, ConflictedThreads, ConflictedThreads, Fun, Pids) ->
    spawn_at_nodes(Nodes, Nodes2, Threads, DocsSet + 1, 0, ConflictedThreads, Fun, Pids);
spawn_at_nodes([], Nodes2, Threads, DocsSetNum, DocNumInSet, ConflictedThreads, Fun, Pids) ->
    spawn_at_nodes(Nodes2, [], Threads, DocsSetNum, DocNumInSet, ConflictedThreads, Fun, Pids);
spawn_at_nodes([N | Nodes], Nodes2, Threads, DocsSetNum, DocNumInSet, ConflictedThreads, Fun, Pids) ->
    Master = self(),
    AnswerDesc = get(file_beg),
    FileBeg = "_" ++ AnswerDesc ++ "_",
    Pid = spawn(N, fun() ->
        try
            receive start -> ok end,
            Fun(integer_to_list(DocsSetNum) ++ FileBeg)
        catch
            E1:E2 ->
                Master ! {store_ans, AnswerDesc, {uncatched_error, E1, E2, erlang:get_stacktrace()}, 0}
        end
    end),
    spawn_at_nodes(Nodes, [N | Nodes2], Threads - 1, DocsSetNum, DocNumInSet + 1, ConflictedThreads, Fun, [Pid | Pids]).

count_answers(Exp) ->
    count_answers(Exp, {0, 0, 0, 0, []}). %{OkNum, OkTime, ErrorNum, ErrorTime, ErrorsList}

count_answers(0, TmpAns) ->
    TmpAns;

count_answers(Num, {OkNum, OkTime, ErrorNum, ErrorTime, ErrorsList}) ->
    AnswerDesc = get(file_beg),
    NewAns = receive
                 {store_ans, AnswerDesc, Ans, Time} ->
                     case Ans of
                         ok ->
                             {OkNum + 1, OkTime + Time, ErrorNum, ErrorTime, ErrorsList};
                         {ok, _} ->
                             {OkNum + 1, OkTime + Time, ErrorNum, ErrorTime, ErrorsList};
                         {uncatched_error, E1, E2, ST} ->
                             ?assertEqual({ok, ok, ok}, {E1, E2, ST}),
                             error;
                         E ->
                             {OkNum, OkTime, ErrorNum + 1, ErrorTime + Time, [E | ErrorsList]}
                     end
             after ?REQUEST_TIMEOUT ->
                 {error, timeout}
             end,
    case NewAns of
        {error, timeout} ->
            {OkNum, OkTime, ErrorNum, ErrorTime, ErrorsList};
        _ ->
            count_answers(Num - 1, NewAns)
    end.

get_random_string() ->
    get_random_string(10, "abcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ").

get_random_string(Length, AllowedChars) ->
    lists:foldl(fun(_, Acc) ->
        [lists:nth(random:uniform(length(AllowedChars)),
            AllowedChars)]
        ++ Acc
    end, [], lists:seq(1, Length)).