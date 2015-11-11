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
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("cluster/worker/modules/datastore/datastore.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/global_definitions.hrl").

-define(REQUEST_TIMEOUT, timer:seconds(30)).

-export([create_delete_test/2, create_sync_delete_test/2, save_test/2, save_sync_test/2, update_test/2,
    update_sync_test/2, get_test/2, exists_test/2, mixed_test/2, set_hooks/2, unset_hooks/2]).

-define(call_store(Fun, Level, CustomArgs), erlang:apply(datastore, Fun, [Level] ++ CustomArgs)).

%%%===================================================================
%%% API
%%%===================================================================

create_delete_test(Config, Level) ->
    create_delete_test_base(Config, Level, create, delete).

create_sync_delete_test(Config, Level) ->
    create_delete_test_base(Config, Level, create_sync, delete_sync).

create_delete_test_base(Config, Level, Fun, Fun2) ->
    Workers = ?config(op_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),

    disable_cache_control(Workers),
    Master = self(),

    TestFun = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(Fun, Level, [
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
    case ((ThreadsNum * DocsPerThead < OkNum) or (DocsPerThead * trunc(ThreadsNum/ConflictedThreads) > OkNum)) of
        true -> ct:print("Create errors list: ~p", [ErrorsList]);
        _ -> ok
    end,
    ?assert((ThreadsNum * DocsPerThead >= OkNum) and (DocsPerThead * trunc(ThreadsNum/ConflictedThreads) =< OkNum)),

    TestFun2 = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(Fun2, Level, [
                    some_record, list_to_binary(DocsSet++integer_to_list(I))]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, TestFun2),
    {OkNum2, OkTime2, _ErrorNum2, _ErrorTime2, ErrorsList2} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList2),
    ?assertEqual(OpsNum, OkNum2),

    CreateErrorTime = case ErrorNum of
                          0 ->
                              0;
                          _ ->
                              ErrorTime/ErrorNum
                      end,

    [
        #parameter{name = create_ok_time, value = OkTime/OkNum, unit = "us",
            description = "Average time of create operation that ended successfully"},
        #parameter{name = create_error_time, value = CreateErrorTime, unit = "us",
            description = "Average time of create operation that filed (e.g. file exists)"},
        #parameter{name = create_error_num, value = ErrorNum, unit = "-",
            description = "Average numer of create operation that filed (e.g. file exists)"},
        #parameter{name = delete_time, value = OkTime2/OkNum2, unit = "us",
            description = "Average time of delete operation"}
    ].

save_test(Config, Level) ->
    save_test_base(Config, Level, save, delete).

save_sync_test(Config, Level) ->
    save_test_base(Config, Level, save_sync, delete_sync).

save_test_base(Config, Level, Fun, Fun2) ->
    Workers = ?config(op_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),

    disable_cache_control(Workers),
    Master = self(),

    SaveMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(OpsPerDoc, fun() ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(Fun, Level, [
                    #document{
                        key = list_to_binary(DocsSet++integer_to_list(I)),
                        value = #some_record{field1 = I, field2 = <<"abc">>, field3 = {test, tuple}}
                    }]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, SaveMany),
    OpsNum = ThreadsNum * DocsPerThead * OpsPerDoc,
    {OkNum, OkTime, _ErrorNum, _ErrorTime, ErrorsList} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList),
    ?assertEqual(OpsNum, OkNum),

    DelMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(Fun2, Level, [
                some_record, list_to_binary(DocsSet++integer_to_list(I))]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, 1, DelMany),
    OpsNum2 = DocsPerThead * ThreadsNum,
    {OkNum2, _OkTime2, _ErrorNum2, _ErrorTime2, ErrorsList2} = count_answers(OpsNum2),
    ?assertEqual([], ErrorsList2),
    ?assertEqual(OpsNum2, OkNum2),

    #parameter{name = save_time, value = OkTime/OkNum, unit = "us",
        description = "Average time of save operation"}.

update_test(Config, Level) ->
    update_test_base(Config, Level, update, save, delete).

update_sync_test(Config, Level) ->
    update_test_base(Config, Level, update_sync, save_sync, delete_sync).

update_test_base(Config, Level, Fun, Fun2, Fun3) ->
    Workers = ?config(op_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),

    disable_cache_control(Workers),
    Master = self(),

    UpdateMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(1, OpsPerDoc, fun(J) ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(Fun, Level, [
                    some_record, list_to_binary(DocsSet++integer_to_list(I)),
                    #{field1 => I+J}
                ]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, UpdateMany),
    OpsNum = ThreadsNum * DocsPerThead * OpsPerDoc,
    {OkNum, _OkTime, ErrorNum, ErrorTime, _ErrorsList} = count_answers(OpsNum),
    % TODO change when datastore behavior will be coherent
    ct:print("Update ok num: ~p, error num ~p:, level ~p", [OkNum, ErrorNum, Level]),
%%     ?assertEqual(0, OkNum),
%%     ?assertEqual(OpsNum, ErrorNum),
    ?assertEqual(OpsNum, OkNum+ErrorNum),

    SaveMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(Fun2, Level, [
                #document{
                    key = list_to_binary(DocsSet++integer_to_list(I)),
                    value = #some_record{field1 = I, field2 = <<"abc">>, field3 = {test, tuple}}
                }]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, SaveMany),
    OpsNum2 = DocsPerThead * ThreadsNum,
    {OkNum2, _OkTime2, _ErrorNum2, _ErrorTime2, ErrorsList2} = count_answers(OpsNum2),
    ?assertEqual([], ErrorsList2),
    ?assertEqual(OpsNum2, OkNum2),

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, UpdateMany),
    {OkNum3, OkTime3, _ErrorNum3, _ErrorTime3, ErrorsList3} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList3),
    ?assertEqual(OpsNum, OkNum3),

    ClearFun = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(Fun3, Level, [
                some_record, list_to_binary(DocsSet++integer_to_list(I))]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, ClearFun),
    {OkNum4, _OkTime4, _ErrorNum4, _ErrorTime4, ErrorsList4} = count_answers(OpsNum2),
    ?assertEqual([], ErrorsList4),
    ?assertEqual(OpsNum2, OkNum4),

    UpdateErrorTime = case ErrorNum of
                          0 ->
                              0;
                          _ ->
                              ErrorTime/ErrorNum
                      end,

    [
        #parameter{name = update_ok_time, value = OkTime3/OkNum3, unit = "us",
            description = "Average time of update operation that ended successfully"},
        #parameter{name = update_error_time, value = UpdateErrorTime, unit = "us",
            description = "Average time of update operation that failed (e.g. file does not exist)"},
        #parameter{name = update_error_num, value = ErrorNum, unit = "-",
            description = "Average number of update operation that failed (e.g. file does not exist)"}
    ].

get_test(Config, Level) ->
    Workers = ?config(op_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),

    disable_cache_control(Workers),
    Master = self(),

    GetMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(1, OpsPerDoc, fun(_J) ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(get, Level, [
                    some_record, list_to_binary(DocsSet++integer_to_list(I))
                ]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, GetMany),
    OpsNum = ThreadsNum * DocsPerThead * OpsPerDoc,
    {OkNum, _OkTime, ErrorNum, ErrorTime, _ErrorsList} = count_answers(OpsNum),
    % TODO change when datastore behavior will be coherent
    ct:print("Get ok num: ~p, error num ~p:, level ~p", [OkNum, ErrorNum, Level]),
%%     ?assertEqual(0, OkNum),
%%     ?assertEqual(OpsNum, ErrorNum),
    ?assertEqual(OpsNum, OkNum+ErrorNum),

    SaveMany = fun(DocsSet) ->
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

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, SaveMany),
    OpsNum2 = DocsPerThead * ThreadsNum,
    {OkNum2, _OkTime2, _ErrorNum2, _ErrorTime2, ErrorsList2} = count_answers(OpsNum2),
    ?assertEqual([], ErrorsList2),
    ?assertEqual(OpsNum2, OkNum2),

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, GetMany),
    {OkNum3, OkTime3, _ErrorNum3, _ErrorTime3, ErrorsList3} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList3),
    ?assertEqual(OpsNum, OkNum3),

    ClearFun = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(delete, Level, [
                some_record, list_to_binary(DocsSet++integer_to_list(I))]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, ClearFun),
    {OkNum4, _OkTime4, _ErrorNum4, _ErrorTime4, ErrorsList4} = count_answers(OpsNum2),
    ?assertEqual([], ErrorsList4),
    ?assertEqual(OpsNum2, OkNum4),

    GetErrorTime = case ErrorNum of
                          0 ->
                              0;
                          _ ->
                              ErrorTime/ErrorNum
                      end,

    [
        #parameter{name = get_ok_time, value = OkTime3/OkNum3, unit = "us",
            description = "Average time of get operation that ended successfully"},
        #parameter{name = get_error_time, value = GetErrorTime, unit = "us",
            description = "Average time of get operation that failed (e.g. file does not exist)"},
        #parameter{name = update_error_num, value = ErrorNum, unit = "-",
            description = "Average number of update operation that failed (e.g. file does not exist)"}
    ].

exists_test(Config, Level) ->
    Workers = ?config(op_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),

    disable_cache_control(Workers),
    Master = self(),

    ExistMultiCheck = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(1, OpsPerDoc, fun(_J) ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(exists, Level, [
                    some_record, list_to_binary(DocsSet++integer_to_list(I))
                ]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, ExistMultiCheck),
    OpsNum = ThreadsNum * DocsPerThead * OpsPerDoc,
    {OkNum, OkTime, _ErrorNum, _ErrorTime, ErrorsList} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList),
    ?assertEqual(OpsNum, OkNum),

    SaveMany = fun(DocsSet) ->
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

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, SaveMany),
    OpsNum2 = DocsPerThead * ThreadsNum,
    {OkNum2, _OkTime2, _ErrorNum2, _ErrorTime2, ErrorsList2} = count_answers(OpsNum2),
    ?assertEqual([], ErrorsList2),
    ?assertEqual(OpsNum2, OkNum2),

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, ExistMultiCheck),
    {OkNum3, OkTime3, _ErrorNum3, _ErrorTime3, ErrorsList3} = count_answers(OpsNum),
    ?assertEqual([], ErrorsList3),
    ?assertEqual(OpsNum, OkNum3),

    ClearFun = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            BeforeProcessing = os:timestamp(),
            Ans = ?call_store(delete, Level, [
                some_record, list_to_binary(DocsSet++integer_to_list(I))]),
            AfterProcessing = os:timestamp(),
            Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, ClearFun),
    {OkNum4, _OkTime4, _ErrorNum4, _ErrorTime4, ErrorsList4} = count_answers(OpsNum2),
    ?assertEqual([], ErrorsList4),
    ?assertEqual(OpsNum2, OkNum4),

    [
        #parameter{name = exists_true_time, value = OkTime3/OkNum3, unit = "us",
            description = "Average time of exists operation that returned true"},
        #parameter{name = exists_false_time, value = OkTime/OkNum, unit = "us",
            description = "Average time of exists operation that returned false"}
    ].

mixed_test(Config, Level) ->
    Workers = ?config(op_worker_nodes, Config),
    ThreadsNum = ?config(threads_num, Config),
    DocsPerThead = ?config(docs_per_thead, Config),
    OpsPerDoc = ?config(ops_per_doc, Config),
    ConflictedThreads = ?config(conflicted_threads, Config),

    case performance:is_stress_test() of
        true ->
            put(file_beg, binary_to_list(term_to_binary(os:timestamp())));
        _ ->
            disable_cache_control(Workers)
    end,
    Master = self(),

    CreateMany = fun(DocsSet) ->
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

    SaveMany = fun(DocsSet) ->
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

    UpdateMany = fun(DocsSet) ->
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

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, CreateMany),
    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, SaveMany),
    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, UpdateMany),
    OpsNum = ThreadsNum * DocsPerThead * OpsPerDoc,

    {OkNum, OkTime, ErrorNum, ErrorTime, _ErrorsList} = count_answers(3*OpsNum),
    ?assertEqual(3*OpsNum, OkNum+ErrorNum),





    GetMany = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(1, OpsPerDoc, fun(_J) ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(get, Level, [
                    some_record, list_to_binary(DocsSet++integer_to_list(I))
                ]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    ExistMultiCheck = fun(DocsSet) ->
        for(1, DocsPerThead, fun(I) ->
            for(1, OpsPerDoc, fun(_J) ->
                BeforeProcessing = os:timestamp(),
                Ans = ?call_store(exists, Level, [
                    some_record, list_to_binary(DocsSet++integer_to_list(I))
                ]),
                AfterProcessing = os:timestamp(),
                Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
            end)
        end)
    end,

    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, GetMany),
    spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, ExistMultiCheck),
    {OkNum2, OkTime2, _ErrorNum2, _ErrorTime2, ErrorsList2} = count_answers(2*OpsNum),
    ?assertEqual([], ErrorsList2),
    ?assertEqual(2*OpsNum, OkNum2),

    case performance:should_clear(Config) of
        true ->
            ClearFun = fun(DocsSet) ->
                for(1, DocsPerThead, fun(I) ->
                    BeforeProcessing = os:timestamp(),
                    Ans = ?call_store(delete, Level, [
                        some_record, list_to_binary(DocsSet++integer_to_list(I))]),
                    AfterProcessing = os:timestamp(),
                    Master ! {store_ans, Ans, timer:now_diff(AfterProcessing, BeforeProcessing)}
                end)
            end,

            spawn_at_nodes(Workers, ThreadsNum, ConflictedThreads, ClearFun),
            OpsNum2 = DocsPerThead * ThreadsNum,
            {OkNum3, _OkTime3, _ErrorNum3, _ErrorTime3, ErrorsList3} = count_answers(OpsNum2),
            ?assertEqual([], ErrorsList3),
            ?assertEqual(OpsNum2, OkNum3);
        false ->
            ok
    end,

    [
        #parameter{name = create_save_update_time, value = (OkTime+ErrorTime)/(OkNum+ErrorNum), unit = "us",
            description = "Average time of create/save/update"},
        #parameter{name = get_exist_time, value = OkTime2/OkNum2, unit = "us",
            description = "Average time of get/exist"}
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
    FileBeg = case performance:is_stress_test() of
        true ->
            "_" ++ get(file_beg) ++ "_";
        _ ->
            "_"
    end,
    spawn(N, fun() ->
        try
            timer:sleep(timer:seconds(1)), % sleep to allow all threads start
            Fun(integer_to_list(DocsSetNum) ++ FileBeg)
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

disable_cache_control(Workers) ->
    lists:foreach(fun(W) ->
        ?assertEqual(ok, gen_server:call({?NODE_MANAGER_NAME, W}, disable_cache_control))
    end, Workers).

set_hooks(Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_node_starter:load_modules(Workers, [?MODULE]),

    Methods = [save, get, exists, delete, update, create, fetch_link, delete_links],
    ModelConfig = lists:map(fun(Method) ->
        {some_record, Method}
    end, Methods),

    case check_config_name(Case) of
        global ->
            ok;
        local ->
            test_utils:mock_new(Workers, caches_controller),
            test_utils:mock_expect(Workers, caches_controller, cache_to_datastore_level, fun(ModelName) ->
                case lists:member(ModelName, datastore_config:global_caches() -- [some_record]) of
                    true -> global_only;
                    _ -> local_only
                end
            end),
            test_utils:mock_expect(Workers, caches_controller, cache_to_task_level, fun(ModelName) ->
                case lists:member(ModelName, datastore_config:global_caches() -- [some_record]) of
                    true -> cluster;
                    _ -> node
                end
            end);
        _ ->
            lists:foreach(fun(W) ->
                lists:foreach(fun(MC) ->
                    ?assert(rpc:call(W, ets, delete_object, [datastore_local_state, {MC, cache_controller}]))
                end, ModelConfig)
            end, Workers)
    end,
    Config.

unset_hooks(Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    [W | _] = Workers,

    Methods = [save, get, exists, delete, update, create, fetch_link, delete_links],
    ModelConfig = lists:map(fun(Method) ->
        {some_record, Method}
    end, Methods),

    case check_config_name(Case) of
        global ->
            ?assertMatch(ok, rpc:call(W, caches_controller, wait_for_cache_dump, [])),
            ?assertMatch(ok, gen_server:call({?NODE_MANAGER_NAME, W}, clear_mem_synch, 60000));
        local ->
            lists:foreach(fun(Wr) ->
                ?assertMatch(ok, rpc:call(Wr, caches_controller, wait_for_cache_dump, [])),
                ?assertMatch(ok, gen_server:call({?NODE_MANAGER_NAME, Wr}, clear_mem_synch, 60000))
            end, Workers),
            test_utils:mock_unload(Workers, [caches_controller]);
        _ ->
            lists:foreach(fun(Wr) ->
                lists:foreach(fun(MC) ->
                    ?assert(rpc:call(Wr, ets, insert, [datastore_local_state, {MC, cache_controller}]))
                end, ModelConfig)
            end, Workers)
    end.

check_config_name(Case) ->
    CStr = atom_to_list(Case),
    case (string:str(CStr, "cache") > 0) and (string:str(CStr, "sync") == 0) of
        true ->
            case string:str(CStr, "global") > 0 of
                true ->
                    global;
                _ ->
                    local
            end;
        _ ->
            no_hooks
    end.
