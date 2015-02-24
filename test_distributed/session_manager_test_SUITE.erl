%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests of session manager API.
%%% @end
%%%-------------------------------------------------------------------
-module(session_manager_test_SUITE).
-author("Krzysztof Trzepla").

-include("workers/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([session_manager_test/1, session_getters_test/1,
    session_supervisor_child_crash_test/1]).

all() -> [
    session_manager_test,
    session_getters_test,
    session_supervisor_child_crash_test
].

-define(TIMEOUT, timer:seconds(5)).

%%%====================================================================
%%% Test function
%%%====================================================================

%% Test creation and removal of session using session manager.
session_manager_test(Config) ->
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    Self = self(),
    SessId1 = <<"session_id_1">>,
    SessId2 = <<"session_id_2">>,
    Iden1 = #identity{user_id = <<"user_id_1">>},
    Iden2 = #identity{user_id = <<"user_id_2">>},

    % Check whether session manager creates session for the first time and then
    % reuses it dispite of node on which request is processed.
    [
        {Node1, [SessSup1, EvtMan1, SeqMan1, Comm1] = Pids1},
        {Node2, [SessSup2, EvtMan2, SeqMan2, Comm2] = Pids2}
    ] = lists:map(fun({SessId, Iden, Workers}) ->
        Answers = utils:pmap(fun(Worker) ->
            rpc:call(Worker, session_manager,
                reuse_or_create_session, [SessId, Iden, Self])
        end, Workers),

        AnswersWithoutCreatedAnswer = lists:delete({ok, created}, Answers),
        ?assertNotEqual(Answers, AnswersWithoutCreatedAnswer),
        ?assert(lists:all(fun(Answer) ->
            Answer =:= {ok, reused}
        end, AnswersWithoutCreatedAnswer)),

        % Check connections have been added to communicator associated with
        % reused session.
        lists:foreach(fun(_) ->
            ?assertMatch({ok, _}, test_utils:receive_msg(add_connection, ?TIMEOUT))
        end, AnswersWithoutCreatedAnswer),
        ?assertEqual({error, timeout}, test_utils:receive_any()),

        Doc = rpc:call(Worker1, session, get, [SessId]),
        ?assertMatch({ok, #document{key = SessId, value = #session{}}}, Doc),
        {ok, #document{value = #session{
            identity = Iden,
            node = Node,
            session_sup = SessSup,
            event_manager = EvtMan,
            sequencer_manager = SeqMan,
            communicator = Comm
        }}} = Doc,

        % Check session infrastructure is running.
        lists:foreach(fun(Pid) ->
            ?assert(is_pid(Pid)),
            ?assertNotEqual(undefined, rpc:call(Node, erlang, process_info, [Pid]))
        end, [SessSup, EvtMan, SeqMan, Comm]),

        {Node, [SessSup, EvtMan, SeqMan, Comm]}
    end, [
        {SessId1, Iden1, lists:duplicate(2, Worker1) ++ lists:duplicate(2, Worker2)},
        {SessId2, Iden2, lists:duplicate(2, Worker2) ++ lists:duplicate(2, Worker1)}
    ]),

    % Check whether session manager returns different session supervisors, event
    % managers, sequencer managers and communicators for different sessions.
    lists:foreach(fun({Pid1, Pid2}) ->
        ?assertNotEqual(Pid1, Pid2)
    end, lists:zip(Pids1, Pids2)),

    % Check supervision tree structure.
    ?assert(is_child({session_manager_worker_sup, Node1}, SessSup1)),
    ?assert(is_child({session_manager_worker_sup, Node2}, SessSup2)),
    [
        [EvtManSup1, SeqManSup1, Comm1],
        [EvtManSup2, SeqManSup2, Comm2]
    ] = lists:map(fun(SessSup) ->
        lists:map(fun(ChildId) ->
            Answer = get_child(SessSup, ChildId),
            ?assertMatch({ok, _}, Answer),
            {ok, Child} = Answer,
            Child
        end, [event_manager_sup, sequencer_manager_sup, communicator])
    end, [SessSup1, SessSup2]),
    lists:foreach(fun({Sup, Child}) ->
        ?assert(is_child(Sup, Child))
    end, [{EvtManSup1, EvtMan1}, {SeqManSup1, SeqMan1},
        {EvtManSup2, EvtMan2}, {SeqManSup2, SeqMan2}]),

    % Check whether session manager can remove session dispite of node on which
    % request is processed.
    utils:pforeach(fun({SessId, Node, Pids, Worker}) ->
        ?assertEqual(ok, rpc:call(Worker, session_manager,
            remove_session, [SessId])),
        ?assertMatch({error, _}, rpc:call(Worker, session_manager,
            remove_session, [SessId])),

        % Check whether session has been removed from cache.
        ?assertMatch({error, {not_found, _}}, rpc:call(Worker,
            session, get, [SessId])),

        % Check session infrastructure has been stopped.
        lists:foreach(fun(Pid) ->
            ?assertEqual(undefined, rpc:call(Node, erlang, process_info, [Pid]))
        end, Pids)
    end, [
        {SessId1, Node1, Pids1, Worker2},
        {SessId2, Node2, Pids2, Worker1}
    ]),

    % Check session manager supervisors have no children.
    ?assertEqual([], supervisor:which_children({session_manager_worker_sup, Node1})),
    ?assertEqual([], supervisor:which_children({session_manager_worker_sup, Node2})),

    ok.

%% Test of session getters.
session_getters_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),

    lists:foreach(fun(GetterName) ->
        Answer = rpc:call(Worker, session, GetterName, [SessId]),
        ?assertMatch({ok, _}, Answer),
        {ok, Pid} = Answer,
        ?assert(is_pid(Pid))
    end, [get_event_manager, get_sequencer_manager, get_communicator]),

    Answer = rpc:call(Worker, session, get_session_supervisor_and_node, [SessId]),
    ?assertMatch({ok, {_, _}}, Answer),
    {ok, {Pid, Node}} = Answer,
    ?assert(is_pid(Pid)),
    ?assert(is_atom(Node)),

    ok.

%% Test removal of session in case of crash of event manager supervisor, sequencer
%% manager supervisor or communicator.
session_supervisor_child_crash_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    SessId = <<"session_id">>,
    Iden = #identity{user_id = <<"user_id">>},

    lists:foreach(fun({ChildId, Fun, Args}) ->
        ?assertEqual({ok, created}, rpc:call(Worker, session_manager,
            reuse_or_create_session, [SessId, Iden, Self])),

        {ok, {SessSup, Node}} = rpc:call(Worker, session,
            get_session_supervisor_and_node, [SessId]),

        {ok, Child} = get_child(SessSup, ChildId),

        apply(Fun, [Child | Args]),

        timer:sleep(?TIMEOUT),

        ?assertMatch({error, {not_found, _}}, rpc:call(Worker, session, get, [SessId])),
        ?assertEqual([], supervisor:which_children({session_manager_worker_sup, Node}))
    end, [
        {event_manager_sup, fun erlang:exit/2, [kill]},
        {sequencer_manager_sup, fun erlang:exit/2, [kill]},
        {communicator, fun gen_server:cast/2, [kill]}
    ]),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(session_manager_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    Self = self(),
    
    test_utils:mock_new(Workers, communicator),
    test_utils:mock_expect(Workers, communicator, send, fun
        (_, _) -> ok
    end),
    test_utils:mock_expect(Workers, communicator, add_connection,
        fun(_Comm, _Con) ->
            Self ! add_connection, ok
        end
    ),

    Config;

init_per_testcase(session_getters_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    SessId = <<"session_id">>,
    Iden = #identity{user_id = <<"user_id">>},

    ?assertEqual({ok, created}, rpc:call(Worker, session_manager,
        reuse_or_create_session, [SessId, Iden, Self])),

    [{session_id, SessId} | Config];

init_per_testcase(session_supervisor_child_crash_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    test_utils:mock_new(Worker, logger),
    test_utils:mock_expect(Worker, logger, dispatch_log, fun
        (_, _, _, [_, _, kill], _) -> meck:exception(throw, crash);
        (A, B, C, D, E) -> meck:passthrough([A, B, C, D, E])
    end),

    Config.

end_per_testcase(session_manager_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),

    test_utils:mock_validate(Workers, communicator),
    test_utils:mock_unload(Workers, communicator),

    Config;

end_per_testcase(session_getters_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),

    ?assertEqual(ok, rpc:call(Worker, session_manager,
        remove_session, [SessId])),

    proplists:delete(session_id, Config);

end_per_testcase(session_supervisor_child_crash_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    test_utils:mock_validate(Worker, logger),
    test_utils:mock_unload(Worker, logger),

    Config.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether supervisor has given child.
%% @end
%%--------------------------------------------------------------------
-spec is_child(Sup :: pid() | {Name :: atom(), Node :: node()}, Child :: pid()) ->
    true | false.
is_child(Sup, Child) ->
    Children = supervisor:which_children(Sup),
    case lists:keyfind(Child, 2, Children) of
        false -> false;
        _ -> true
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns supervisor child.
%% @end
%%--------------------------------------------------------------------
-spec get_child(Sup :: pid(), ChildId :: term()) ->
    {ok, Child :: pid()} | {error, not_found}.
get_child(Sup, ChildId) ->
    Children = supervisor:which_children(Sup),
    case lists:keyfind(ChildId, 1, Children) of
        {ChildId, Child, _, _} -> {ok, Child};
        false -> {error, not_found}
    end.