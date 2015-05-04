%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Tests datastore basic operations at all levels.
%%% It is utils module - it contains test functions but it is not
%%% test suite. These functions are included by suites that do tests
%%% using various environments.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_basic_ops_utils).
-author("Michal Wrzeszcz").

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("annotations/include/annotations.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/test/performance.hrl").

-define(REQUEST_TIMEOUT, timer:seconds(10)).

-export([create_delete_test/2, save_test/2, update_test/2, get_test/2, exists_test/2]).

-define(call_store(Fun, Level, CustomArgs), erlang:apply(datastore, Fun, [Level] ++ CustomArgs)).

%%%===================================================================
%%% Test function
%% ====================================================================


create_delete_test(Config, Level) ->
    Workers = ?config(op_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),

    ok = test_node_starter:load_modules(Workers, [?MODULE]),
    Master = self(),

    TestFun = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(create, Level, [
                    #document{
                        key = list_to_binary(DocsSet++integer_to_list(I)),
                        value = #some_record{field1 = I, field2 = <<"abc">>, field3 = {test, tuple}}
                    }]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, TestFun),
    OpsNum = ThreadsNum * DocsPerThead * OpsPerDoc,
    {OkNum, OkTime, ErrorNum, ErrorTime, ErrorsList} = count_answers(OpsNum),
    ?assertEqual(OpsNum, OkNum+ErrorNum),
    % TODO change when datastore behavior will be coherent
    ct:print("Create ok num: ~p, error num ~p:, level ~p", [OkNum, ErrorNum, Level]),
    ?assert((ThreadsNum * DocsPerThead >= OkNum) and (DocsPerThead * trunc(ThreadsNum/ConflictedThreads) =< OkNum)),

    TestFun2 = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(delete, Level, [
                    some_record, list_to_binary(DocsSet++integer_to_list(I))]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, TestFun2),
    {OkNum2, OkTime2, _ErrorNum2, _ErrorTime2, ErrorsList2} = count_answers(OpsNum),
    ?assertEqual(OpsNum, OkNum2),
    ?assertEqual([], ErrorsList2),

    [
        #parameter{name = create_ok_time, value = OkTime/OkNum, unit = "microsek"},
        #parameter{name = create_error_time, value = ErrorTime/ErrorNum, unit = "microsek"},
        #parameter{name = delete_time, value = OkTime2/OkNum2, unit = "microsek"}
    ].

save_test(Config, Level) ->
    Workers = ?config(op_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),

    ok = test_node_starter:load_modules(Workers, [?MODULE]),
    Master = self(),

    TestFun = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(save, Level, [
                    #document{
                        key = list_to_binary(DocsSet++integer_to_list(I)),
                        value = #some_record{field1 = I, field2 = <<"abc">>, field3 = {test, tuple}}
                    }]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, TestFun),
    OpsNum = ThreadsNum * DocsPerThead * OpsPerDoc,
    {OkNum, OkTime, _ErrorNum, _ErrorTime, ErrorsList} = count_answers(OpsNum),
    ?assertEqual(OpsNum, OkNum),
    ?assertEqual([], ErrorsList),

    TestFun2 = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(delete, Level, [
                some_record, list_to_binary(DocsSet++integer_to_list(I))]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, 1, TestFun2),
    OpsNum2 = DocsPerThead * ThreadsNum,
    {OkNum2, _OkTime2, _ErrorNum2, _ErrorTime2, ErrorsList2} = count_answers(OpsNum2),
    ?assertEqual([], ErrorsList2),
    ?assertEqual(OpsNum2, OkNum2),

    #parameter{name = save_time, value = OkTime/OkNum, unit = "microsek"}.

update_test(Config, Level) ->
    Workers = ?config(op_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),

    ok = test_node_starter:load_modules(Workers, [?MODULE]),
    Master = self(),

    TestFun = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(1, OpsPerDoc, fun(J) ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(update, Level, [
                    some_record, list_to_binary(DocsSet++integer_to_list(I)),
                    #{field1 => I+J}
                ]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, TestFun),
    OpsNum = ThreadsNum * DocsPerThead * OpsPerDoc,
    {OkNum, _OkTime, ErrorNum, ErrorTime, _ErrorsList} = count_answers(OpsNum),
    % TODO change when datastore behavior will be coherent
    ct:print("Update ok num: ~p, error num ~p:, level ~p", [OkNum, ErrorNum, Level]),
%%     ?assertEqual(0, OkNum),
%%     ?assertEqual(OpsNum, ErrorNum),
    ?assertEqual(OpsNum, OkNum+ErrorNum),

    TestFun2 = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(save, Level, [
                #document{
                    key = list_to_binary(DocsSet++integer_to_list(I)),
                    value = #some_record{field1 = I, field2 = <<"abc">>, field3 = {test, tuple}}
                }]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, TestFun2),
    OpsNum2 = DocsPerThead * ThreadsNum,
    {OkNum2, _OkTime2, _ErrorNum2, _ErrorTime2, ErrorsList2} = count_answers(OpsNum2),
    ?assertEqual([], ErrorsList2),
    ?assertEqual(OpsNum2, OkNum2),

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, TestFun),
    {OkNum3, OkTime3, _ErrorNum3, _ErrorTime3, ErrorsList3} = count_answers(OpsNum),
    ?assertEqual(OpsNum, OkNum3),
    ?assertEqual([], ErrorsList3),

    TestFun3 = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(delete, Level, [
                some_record, list_to_binary(DocsSet++integer_to_list(I))]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, TestFun3),
    {OkNum4, _OkTime4, _ErrorNum4, _ErrorTime4, ErrorsList4} = count_answers(OpsNum2),
    ?assertEqual([], ErrorsList4),
    ?assertEqual(OpsNum2, OkNum4),

    [
        #parameter{name = update_ok_time, value = OkTime3/OkNum3, unit = "microsek"},
        #parameter{name = update_error_time, value = ErrorTime/ErrorNum, unit = "microsek"}
    ].

get_test(Config, Level) ->
    Workers = ?config(op_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),

    ok = test_node_starter:load_modules(Workers, [?MODULE]),
    Master = self(),

    TestFun = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(1, OpsPerDoc, fun(J) ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(get, Level, [
                    some_record, list_to_binary(DocsSet++integer_to_list(I))
                ]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, TestFun),
    OpsNum = ThreadsNum * DocsPerThead * OpsPerDoc,
    {OkNum, _OkTime, ErrorNum, ErrorTime, _ErrorsList} = count_answers(OpsNum),
    % TODO change when datastore behavior will be coherent
    ct:print("Get ok num: ~p, error num ~p:, level ~p", [OkNum, ErrorNum, Level]),
%%     ?assertEqual(0, OkNum),
%%     ?assertEqual(OpsNum, ErrorNum),
    ?assertEqual(OpsNum, OkNum+ErrorNum),

    TestFun2 = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(save, Level, [
                #document{
                    key = list_to_binary(DocsSet++integer_to_list(I)),
                    value = #some_record{field1 = I, field2 = <<"abc">>, field3 = {test, tuple}}
                }]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, TestFun2),
    OpsNum2 = DocsPerThead * ThreadsNum,
    {OkNum2, _OkTime2, _ErrorNum2, _ErrorTime2, ErrorsList2} = count_answers(OpsNum2),
    ?assertEqual([], ErrorsList2),
    ?assertEqual(OpsNum2, OkNum2),

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, TestFun),
    {OkNum3, OkTime3, _ErrorNum3, _ErrorTime3, ErrorsList3} = count_answers(OpsNum),
    ?assertEqual(OpsNum, OkNum3),
    ?assertEqual([], ErrorsList3),

    TestFun3 = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(delete, Level, [
                some_record, list_to_binary(DocsSet++integer_to_list(I))]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, TestFun3),
    {OkNum4, _OkTime4, _ErrorNum4, _ErrorTime4, ErrorsList4} = count_answers(OpsNum2),
    ?assertEqual([], ErrorsList4),
    ?assertEqual(OpsNum2, OkNum4),

    [
        #parameter{name = get_ok_time, value = OkTime3/OkNum3, unit = "microsek"},
        #parameter{name = get_error_time, value = ErrorTime/ErrorNum, unit = "microsek"}
    ].

exists_test(Config, Level) ->
    Workers = ?config(op_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),

    ok = test_node_starter:load_modules(Workers, [?MODULE]),
    Master = self(),

    TestFun = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(1, OpsPerDoc, fun(J) ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(exists, Level, [
                    some_record, list_to_binary(DocsSet++integer_to_list(I))
                ]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, TestFun),
    OpsNum = ThreadsNum * DocsPerThead * OpsPerDoc,
    {OkNum, OkTime, ErrorNum, _ErrorTime, _ErrorsList} = count_answers(OpsNum),
    ?assertEqual(OpsNum, OkNum),
    ?assertEqual(0, ErrorNum),

    TestFun2 = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(save, Level, [
                #document{
                    key = list_to_binary(DocsSet++integer_to_list(I)),
                    value = #some_record{field1 = I, field2 = <<"abc">>, field3 = {test, tuple}}
                }]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, TestFun2),
    OpsNum2 = DocsPerThead * ThreadsNum,
    {OkNum2, _OkTime2, _ErrorNum2, _ErrorTime2, ErrorsList2} = count_answers(OpsNum2),
    ?assertEqual([], ErrorsList2),
    ?assertEqual(OpsNum2, OkNum2),

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, TestFun),
    {OkNum3, OkTime3, _ErrorNum3, _ErrorTime3, ErrorsList3} = count_answers(OpsNum),
    ?assertEqual(OpsNum, OkNum3),
    ?assertEqual([], ErrorsList3),

    TestFun3 = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(delete, Level, [
                some_record, list_to_binary(DocsSet++integer_to_list(I))]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, TestFun3),
    {OkNum4, _OkTime4, _ErrorNum4, _ErrorTime4, ErrorsList4} = count_answers(OpsNum2),
    ?assertEqual([], ErrorsList4),
    ?assertEqual(OpsNum2, OkNum4),

    [
        #parameter{name = exists_true_time, value = OkTime3/OkNum3, unit = "microsek"},
        #parameter{name = exists_false_time, value = OkTime/OkNum, unit = "microsek"}
    ].


%%%===================================================================
%%% Internal functions
%%%===================================================================

for(1, F) ->
    F();
for(N, F) ->
    F(),
    for(N - 1, F).

for(N, N, F) ->
    F(N);
for(I, N, F) ->
    F(I),
    for(I + 1, N, F).

spawn_at_nodes(Nodes, Threads, ConflictedThreads, Fun) ->
    spawn_at_nodes(Nodes, [], Threads, 1, 0, ConflictedThreads, Fun).

spawn_at_nodes(_Nodes, _Nodes2, 0, _DocsSetNum, _DocNumInSet, _ConflictedThreads, _Fun) ->
    ok;
spawn_at_nodes(Nodes, Nodes2, Threads, DocsSet, ConflictedThreads, ConflictedThreads, Fun) ->
    spawn_at_nodes(Nodes, Nodes2, Threads, DocsSet+1, 0, ConflictedThreads, Fun);
spawn_at_nodes([], Nodes2, Threads, DocsSetNum, DocNumInSet, ConflictedThreads, Fun) ->
    spawn_at_nodes(Nodes2, [], Threads, DocsSetNum, DocNumInSet, ConflictedThreads, Fun);
spawn_at_nodes([N | Nodes], Nodes2, Threads, DocsSetNum, DocNumInSet, ConflictedThreads, Fun) ->
    Master = self(),
    spawn(N, fun() ->
        try
            Fun(integer_to_list(DocsSetNum) ++ "_")
        catch
            E1:E2 ->
                Master ! {store_ans, {uncatched_error, E1, E2, erlang:get_stacktrace()}, 0}
        end
    end),
    spawn_at_nodes(Nodes, [N | Nodes2], Threads - 1, DocsSetNum, DocNumInSet+1, ConflictedThreads, Fun).

count_answers(Exp) ->
    count_answers(Exp, {0,0,0,0, []}). %{OkNum, OkTime, ErrorNum, ErrorTime, ErrorsList}

count_answers(0, TmpAns) ->
    TmpAns;

count_answers(Num, {OkNum, OkTime, ErrorNum, ErrorTime, ErrorsList}) ->
    NewAns = receive
              {store_ans, Ans, Time} ->
                  case Ans of
                      ok ->
                          {OkNum + 1, OkTime + Time, ErrorNum, ErrorTime, ErrorsList};
                      {ok, _} ->
                          {OkNum + 1, OkTime + Time, ErrorNum, ErrorTime, ErrorsList};
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