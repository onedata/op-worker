%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests of event manager API.
%%% @end
%%%-------------------------------------------------------------------
-module(event_manager_test_SUITE).
-author("Krzysztof Trzepla").

-include("workers/event_manager/write_event.hrl").
-include("workers/event_manager/event_stream.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("annotations/include/annotations.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    event_manager_creation_and_removal_test/1,
    event_manager_subscription_and_emission_test/1
]).

-perf_test({perf_cases, []}).
all() -> [
    event_manager_creation_and_removal_test,
    event_manager_subscription_and_emission_test
].

-define(TIMEOUT, timer:seconds(5)).

%%%====================================================================
%%% Test function
%%%====================================================================

%% Test creation and removal of event dispatcher using event manager.
event_manager_creation_and_removal_test(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    SessionId1 = <<"session_id_1">>,
    SessionId2 = <<"session_id_2">>,

    % Check whether event worker returns the same event dispatcher
    % for given session dispite of node on which request is processed.
    [EvtDisp1, EvtDisp2] = lists:map(fun({SessionId, Workers}) ->
        CreateOrGetEvtDispAnswers = utils:pmap(fun(Worker) ->
            rpc:call(Worker, event_manager,
                get_or_create_event_dispatcher, [SessionId])
        end, Workers),

        lists:foreach(fun(CreateOrGetEvtDispAnswer) ->
            ?assertMatch({ok, _}, CreateOrGetEvtDispAnswer)
        end, CreateOrGetEvtDispAnswers),

        {_, [FirstEvtDisp | EvtDisps]} = lists:unzip(CreateOrGetEvtDispAnswers),

        lists:foreach(fun(EvtDisp) ->
            ?assertEqual(FirstEvtDisp, EvtDisp)
        end, EvtDisps),

        FirstEvtDisp
    end, [
        {SessionId1, lists:duplicate(2, Worker1) ++ lists:duplicate(2, Worker2)},
        {SessionId2, lists:duplicate(2, Worker2) ++ lists:duplicate(2, Worker1)}
    ]),

    % Check whether event worker returns different event dispatchers for
    % different sessions.
    ?assertNotEqual(EvtDisp1, EvtDisp2),

    % Check whether event worker can remove event manager
    % for given session dispite of node on which request is processed.
    utils:pforeach(fun({SessionId, Worker}) ->
        ProcessesBeforeRemoval = processes([Worker1, Worker2]),
        ?assertMatch([_], lists:filter(fun(Process) ->
            Process =:= EvtDisp1
        end, ProcessesBeforeRemoval)),

        RemoveEvtDispAnswer1 = rpc:call(Worker, event_manager,
            remove_event_dispatcher, [SessionId]),
        ?assertMatch(ok, RemoveEvtDispAnswer1),
        RemoveEvtDispAnswer2 = rpc:call(Worker, event_manager,
            remove_event_dispatcher, [SessionId]),
        ?assertMatch({error, _}, RemoveEvtDispAnswer2),

        ProcessesAfterRemoval = processes([Worker1, Worker2]),
        ?assertMatch([], lists:filter(fun(Proces) ->
            Proces =:= EvtDisp1
        end, ProcessesAfterRemoval))
    end, [
        {SessionId1, Worker2},
        {SessionId2, Worker1}
    ]),

    ok.

event_manager_subscription_and_emission_test(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    Self = self(),
    SessionId1 = <<"session_id_1">>,
    SessionId2 = <<"session_id_2">>,
    Sub = #write_event_subscription{
        producer = gui,
        event_stream_spec = #event_stream{
            metadata = 0,
            admission_rule = fun(#write_event{}) -> true; (_) -> false end,
            aggregation_rule = fun
                (#write_event{file_id = FileId} = Evt1, #write_event{file_id = FileId} = Evt2) ->
                    {ok, #write_event{
                        file_id = Evt1#write_event.file_id,
                        counter = Evt1#write_event.counter + Evt2#write_event.counter,
                        size = Evt1#write_event.size + Evt2#write_event.size,
                        file_size = Evt2#write_event.file_size,
                        blocks = Evt1#write_event.blocks ++ Evt2#write_event.blocks
                    }};
                (_, _) -> {error, different}
            end,
            transition_rule = fun(Meta, #write_event{counter = Counter}) ->
                Meta + Counter
            end,
            emission_rule = fun(Meta) -> Meta >= 6 end,
            handlers = [fun(Evts) -> Self ! {handler, Evts} end]
        }
    },

    {ok, _} = rpc:call(Worker1, event_manager,
        get_or_create_event_dispatcher, [SessionId1]),

    SubAnswer = rpc:call(Worker2, event_manager, subscribe, [Sub]),
    ?assertMatch({ok, _}, SubAnswer),
    {ok, SubId} = SubAnswer,

    {ok, _} = rpc:call(Worker2, event_manager,
        get_or_create_event_dispatcher, [SessionId2]),

    lists:foldl(fun(Evt, N) ->
        EmitAns = rpc:call(Worker1, event_manager, emit, [Evt#write_event{blocks = [{N, N}]}, SessionId1]),
        ?assertEqual(ok, EmitAns),
        N + 1
    end, 1, lists:duplicate(6, #write_event{counter = 1, size = 1, file_size = 1})),

    ?assertMatch({ok, _}, test_utils:receive_msg({handler, [#write_event{counter = 6,
        size = 6, file_size = 1, blocks = [{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}]}]}, ?TIMEOUT)),
    ?assertMatch({error, timeout}, test_utils:receive_any()),

    FileId1 = <<"file_id_1">>,
    FileId2 = <<"file_id_2">>,

    lists:foreach(fun(FileId) ->
        lists:foreach(fun(Evt) ->
            EmitAns = rpc:call(Worker1, event_manager, emit, [Evt, SessionId2]),
            ?assertEqual(ok, EmitAns)
        end, lists:duplicate(3, #write_event{file_id = FileId, counter = 1, size = 1, file_size = 1}))
    end, [FileId1, FileId2]),

    ?assertMatch({ok, _}, test_utils:receive_msg({handler, [
        #write_event{file_id = FileId2, counter = 3, size = 3, file_size = 1},
        #write_event{file_id = FileId1, counter = 3, size = 3, file_size = 1}
    ]}, ?TIMEOUT)),
    ?assertMatch({error, timeout}, test_utils:receive_any()),

    UnsubAnswer = rpc:call(Worker1, event_manager, unsubscribe, [SubId]),
    ?assertEqual(ok, UnsubAnswer),

    ok = rpc:call(Worker1, event_manager, remove_event_dispatcher, [SessionId2]),
    ok = rpc:call(Worker2, event_manager, remove_event_dispatcher, [SessionId1]),

    ok.

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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns list of processes running on given nodes.
%% @end
%%--------------------------------------------------------------------
-spec processes(Nodes :: [node()]) -> [Pid :: pid()].
processes(Nodes) ->
    lists:foldl(fun(Node, Processes) ->
        Processes ++ rpc:call(Node, erlang, processes, [])
    end, [], Nodes).