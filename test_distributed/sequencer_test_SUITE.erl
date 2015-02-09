%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Tests sequencer API.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_test_SUITE).
-author("Krzysztof Trzepla").

-include("test_utils.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([sequencer_dispatcher_test/1]).

all() -> [sequencer_dispatcher_test].

%%%===================================================================
%%% Test function
%% ====================================================================

sequencer_dispatcher_test(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    FuseId1 = <<"fuse_id_1">>,
    FuseId2 = <<"fuse_id_2">>,
    Connection = self(),

    [SeqMan1, SeqMan2] = lists:map(fun(FuseId) ->
        CreateOrGetSeqManAnswers = utils:pmap(fun(Worker) ->
            rpc:call(Worker, sequencer_dispatcher,
                create_or_get_sequencer_manager, [FuseId, Connection])
        end, lists:duplicate(2, Worker1) ++ lists:duplicate(2, Worker2)),

        lists:foreach(fun(CreateOrGetSeqManAnswer) ->
            ?assertMatch({ok, _}, CreateOrGetSeqManAnswer)
        end, CreateOrGetSeqManAnswers),

        {_, [FirstSeqMan | SeqMans]} = lists:unzip(CreateOrGetSeqManAnswers),

        lists:foreach(fun(SeqMan) ->
            ?assertEqual(FirstSeqMan, SeqMan)
        end, SeqMans),

        FirstSeqMan
    end, [FuseId1, FuseId2]),

    ?assertNotEqual(SeqMan1, SeqMan2),

    utils:pforeach(fun(FuseId) ->
        ProcessesBeforeRemoval = processes([Worker1, Worker2]),
        ?assertMatch([_], lists:filter(fun(P) ->
            P =:= SeqMan1
        end, ProcessesBeforeRemoval)),

        RemoveSeqManAnswer1 = rpc:call(Worker1, sequencer_dispatcher,
            remove_sequencer_manager, [FuseId]),
        ?assertMatch(ok, RemoveSeqManAnswer1),
        RemoveSeqManAnswer2 = rpc:call(Worker1, sequencer_dispatcher,
            remove_sequencer_manager, [FuseId]),
        ?assertMatch({error, _}, RemoveSeqManAnswer2),

        ProcessesAfterRemoval = processes([Worker1, Worker2]),
        ?assertMatch([], lists:filter(fun(P) ->
            P =:= SeqMan1
        end, ProcessesAfterRemoval))
    end, [FuseId1, FuseId2]),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    try
        test_node_starter:prepare_test_environment(Config, ?TEST_FILE(Config, "env_desc.json"))
    catch
        A:B -> ct:print("~p:~p~n~p", [A, B, erlang:get_stacktrace()])
    end.

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

processes(Workers) ->
    lists:foldl(fun(Worker, Processes) ->
        Processes ++ rpc:call(Worker, erlang, processes, [])
    end, [], Workers).