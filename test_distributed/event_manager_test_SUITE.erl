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

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([event_manager_test/1]).

all() -> [event_manager_test].

-define(TIMEOUT, timer:seconds(5)).

%%%====================================================================
%%% Test function
%%%====================================================================

%% Test creation and removal of event dispatcher using event manager.
event_manager_test(Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    FuseId1 = <<"fuse_id_1">>,
    FuseId2 = <<"fuse_id_2">>,

    % Check whether event worker returns the same event dispatcher
    % for given session dispite of node on which request is processed.
    [EvtDisp1, EvtDisp2] = lists:map(fun({FuseId, Workers}) ->
        CreateOrGetEvtDispAnswers = utils:pmap(fun(Worker) ->
            rpc:call(Worker, event_manager,
                get_or_create_event_dispatcher, [FuseId])
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
        {FuseId1, lists:duplicate(2, Worker1) ++ lists:duplicate(2, Worker2)},
        {FuseId2, lists:duplicate(2, Worker2) ++ lists:duplicate(2, Worker1)}
    ]),

    % Check whether event worker returns different event dispatchers for
    % different sessions.
    ?assertNotEqual(EvtDisp1, EvtDisp2),

    % Check whether event worker can remove event manager
    % for given session dispite of node on which request is processed.
    utils:pforeach(fun({FuseId, Worker}) ->
        ProcessesBeforeRemoval = processes([Worker1, Worker2]),
        ?assertMatch([_], lists:filter(fun(Process) ->
            Process =:= EvtDisp1
        end, ProcessesBeforeRemoval)),

        RemoveEvtDispAnswer1 = rpc:call(Worker, event_manager,
            remove_event_dispatcher, [FuseId]),
        ?assertMatch(ok, RemoveEvtDispAnswer1),
        RemoveEvtDispAnswer2 = rpc:call(Worker, event_manager,
            remove_event_dispatcher, [FuseId]),
        ?assertMatch({error, _}, RemoveEvtDispAnswer2),

        ProcessesAfterRemoval = processes([Worker1, Worker2]),
        ?assertMatch([], lists:filter(fun(Proces) ->
            Proces =:= EvtDisp1
        end, ProcessesAfterRemoval))
    end, [
        {FuseId1, Worker2},
        {FuseId2, Worker1}
    ]),

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