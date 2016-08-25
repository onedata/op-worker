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

-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("annotations/include/annotations.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    session_manager_session_creation_and_reuse_test/1,
    session_manager_session_components_running_test/1,
    session_manager_supervision_tree_structure_test/1,
    session_manager_session_removal_test/1,
    session_getters_test/1,
    session_supervisor_child_crash_test/1]).

all() ->
    ?ALL([
        session_manager_session_creation_and_reuse_test,
        session_manager_session_components_running_test,
        session_manager_supervision_tree_structure_test,
        session_manager_session_removal_test,
        session_getters_test,
        session_supervisor_child_crash_test
    ]).

-define(TIMEOUT, timer:seconds(5)).

%%%===================================================================
%%% Test functions
%%%===================================================================

%% Check whether session manager creates session for the first time and then
%% reuses it despite of node on which request is processed.
session_manager_session_creation_and_reuse_test(Config) ->
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    Self = self(),
    SessId1 = <<"session_id_1">>,
    SessId2 = <<"session_id_2">>,
    Iden1 = #user_identity{user_id = <<"user_id_1">>},
    Iden2 = #user_identity{user_id = <<"user_id_2">>},

    lists:foreach(fun({SessId, Iden, Workers}) ->
        Answers = utils:pmap(fun(Worker) ->
            ?assertMatch({ok, _}, rpc:call(Worker, session_manager,
                reuse_or_create_fuse_session, [SessId, Iden, Self]))
        end, Workers),

        % Check connections have been added to session
        {ok, Cons} = ?assertMatch({ok, _},
            rpc:call(Worker1, session, get_connections, [SessId]), 10),
        ?assertEqual(length(Answers), length(Cons))

    end, [
        {SessId1, Iden1, lists:duplicate(2, Worker1) ++ lists:duplicate(2, Worker2)},
        {SessId2, Iden2, lists:duplicate(2, Worker2) ++ lists:duplicate(2, Worker1)}
    ]),

    ?assertEqual(ok, rpc:call(Worker1, session_manager, remove_session, [SessId1])),
    ?assertEqual(ok, rpc:call(Worker2, session_manager, remove_session, [SessId2])),

    ok.

%% Check whether after creation of session all session components are running.
session_manager_session_components_running_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessIds = ?config(session_ids, Config),

    [Pids1, Pids2] = lists:map(fun(SessId) ->
        Doc = rpc:call(Worker, session, get, [SessId]),

        ?assertMatch({ok, #document{key = SessId, value = #session{}}}, Doc),
        {ok, #document{value = #session{
            node = Node,
            supervisor = SessSup,
            event_manager = EvtMan,
            sequencer_manager = SeqMan
        }}} = Doc,

        % Check session infrastructure is running.
        lists:foreach(fun(Pid) ->
            ?assert(is_pid(Pid)),
            ?assertNotEqual(undefined, rpc:call(Node, erlang, process_info, [Pid]))
        end, [SessSup, EvtMan, SeqMan]),

        [SessSup, EvtMan, SeqMan]
    end, SessIds),

    % Check whether session manager returns different session supervisors, event
    % managers and sequencer managers for different sessions.
    lists:foreach(fun({Pid1, Pid2}) ->
        ?assertNotEqual(Pid1, Pid2)
    end, lists:zip(Pids1, Pids2)),

    ok.

%% Check whether session supervision tree structure is valid.
session_manager_supervision_tree_structure_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessIds = ?config(session_ids, Config),
    Idents = ?config(identities, Config),

    [
        {Node1, [SessSup1, EvtMan1, SeqMan1]},
        {Node2, [SessSup2, EvtMan2, SeqMan2]}
    ] = lists:map(fun({SessId, Iden}) ->
        Doc = rpc:call(Worker, session, get, [SessId]),

        ?assertMatch({ok, #document{key = SessId, value = #session{}}}, Doc),
        {ok, #document{value = #session{
            identity = Iden,
            node = Node,
            supervisor = SessSup,
            event_manager = EvtMan,
            sequencer_manager = SeqMan
        }}} = Doc,

        {Node, [SessSup, EvtMan, SeqMan]}
    end, lists:zip(SessIds, Idents)),

    % Check supervision tree structure.
    ?assertEqual(true, is_child({session_manager_worker_sup, Node1}, SessSup1)),
    ?assertEqual(true, is_child({session_manager_worker_sup, Node2}, SessSup2)),
    [
        [EvtManSup1, SeqManSup1],
        [EvtManSup2, SeqManSup2]
    ] = lists:map(fun(SessSup) ->
        lists:map(fun(ChildId) ->
            Answer = get_child(SessSup, ChildId),
            ?assertMatch({ok, _}, Answer),
            {ok, Child} = Answer,
            Child
        end, [event_manager_sup, sequencer_manager_sup])
    end, [SessSup1, SessSup2]),
    lists:foreach(fun({Sup, Child}) ->
        ?assert(is_child(Sup, Child))
    end, [{EvtManSup1, EvtMan1}, {SeqManSup1, SeqMan1},
        {EvtManSup2, EvtMan2}, {SeqManSup2, SeqMan2}]),

    ok.

%% Check whether session manager can remove session dispite of node on which
%% request is processed.
session_manager_session_removal_test(Config) ->
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),
    [SessId1, SessId2] = SessIds = ?config(session_ids, Config),
    Idents = ?config(identities, Config),

    [{Node1, Pids1}, {Node2, Pids2}] = lists:map(fun({SessId, Iden}) ->
        Doc = rpc:call(Worker1, session, get, [SessId]),

        ?assertMatch({ok, #document{key = SessId, value = #session{}}}, Doc),
        {ok, #document{value = #session{
            identity = Iden,
            node = Node,
            supervisor = SessSup,
            event_manager = EvtMan,
            sequencer_manager = SeqMan
        }}} = Doc,

        {Node, [SessSup, EvtMan, SeqMan]}
    end, lists:zip(SessIds, Idents)),

    utils:pforeach(fun({SessId, Node, Pids, Worker}) ->
        ?assertEqual(ok, rpc:call(Worker, session_manager,
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

%% Check whether session getters returns valid session components.
session_getters_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?config(session_id, Config),

    lists:foreach(fun(GetterName) ->
        {ok, Result} = ?assertMatch({ok, _},
            rpc:call(Worker, session, GetterName, [SessId])),
        case Result of
            [Inner] -> ?assert(is_pid(Inner));
            _ -> ?assert(is_pid(Result))
        end
    end, [get_event_manager, get_sequencer_manager, get_connections]),

    Answer = rpc:call(Worker, session, get_session_supervisor_and_node, [SessId]),
    ?assertMatch({ok, {_, _}}, Answer),
    {ok, {Pid, Node}} = Answer,
    ?assert(is_pid(Pid)),
    ?assert(is_atom(Node)),

    ok.

%% Check whether session is removed in case of crash of event manager supervisor,
%% sequencer manager supervisor or session watcher.
session_supervisor_child_crash_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    SessId = <<"session_id">>,
    Iden = #user_identity{user_id = <<"user_id">>},

    lists:foreach(fun({ChildId, Fun, Args}) ->
        ?assertMatch({ok, _}, rpc:call(Worker, session_manager,
            reuse_or_create_fuse_session, [SessId, Iden, Self])),

        {ok, {SessSup, Node}} = rpc:call(Worker, session,
            get_session_supervisor_and_node, [SessId]),

        {ok, Child} = get_child(SessSup, ChildId),

        apply(Fun, [Child | Args]),

        ?assertMatch({error, {not_found, _}}, rpc:call(Worker, session, get, [SessId]), 10),
        ?assertEqual([], supervisor:which_children({session_manager_worker_sup, Node}))
    end, [
        {event_manager_sup, fun erlang:exit/2, [kill]},
        {sequencer_manager_sup, fun erlang:exit/2, [kill]},
        {session_watcher, fun gen_server:cast/2, [kill]}
    ]),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    NewConfig = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]),
    [Worker | _] = ?config(op_worker_nodes, NewConfig),
    initializer:clear_models(Worker, [subscription]),
    NewConfig.

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(session_manager_session_creation_and_reuse_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Workers),
    Config;

init_per_testcase(session_getters_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Self = self(),
    SessId = <<"session_id">>,
    Iden = #user_identity{user_id = <<"user_id">>},
    initializer:communicator_mock(Worker),
    initializer:basic_session_setup(Worker, SessId, Iden, Self, Config);

init_per_testcase(session_supervisor_child_crash_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    initializer:communicator_mock(Worker),
    test_utils:mock_new(Worker, logger),
    test_utils:mock_expect(Worker, logger, dispatch_log, fun
        (_, _, _, [_, _, kill], _) -> meck:exception(throw, crash);
        (A, B, C, D, E) -> meck:passthrough([A, B, C, D, E])
    end),

    Config;

init_per_testcase(Case, Config) when
    Case =:= session_manager_session_components_running_test;
    Case =:= session_manager_supervision_tree_structure_test;
    Case =:= session_manager_session_removal_test ->
    Workers = ?config(op_worker_nodes, Config),
    Self = self(),
    SessId1 = <<"session_id_1">>,
    SessId2 = <<"session_id_2">>,
    Iden1 = #user_identity{user_id = <<"user_id_1">>},
    Iden2 = #user_identity{user_id = <<"user_id_2">>},

    initializer:communicator_mock(Workers),
    ?assertMatch({ok, _}, rpc:call(hd(Workers), session_manager,
        reuse_or_create_fuse_session, [SessId1, Iden1, Self])),
    ?assertMatch({ok, _}, rpc:call(hd(Workers), session_manager,
        reuse_or_create_fuse_session, [SessId2, Iden2, Self])),
    ?assertEqual(ok, rpc:call(hd(Workers), session_manager,
        remove_session, [?ROOT_SESS_ID])),

    [{session_ids, [SessId1, SessId2]}, {identities, [Iden1, Iden2]} | Config].

end_per_testcase(session_getters_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    initializer:basic_session_teardown(Worker, Config),
    test_utils:mock_validate_and_unload(Worker, communicator);

end_per_testcase(session_supervisor_child_crash_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Worker, logger);

end_per_testcase(Case, Config) when
    Case =:= session_manager_session_creation_and_reuse_test;
    Case =:= session_manager_session_removal_test ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, communicator);

end_per_testcase(Case, Config) when
    Case =:= session_manager_session_components_running_test;
    Case =:= session_manager_supervision_tree_structure_test ->
    Workers = ?config(op_worker_nodes, Config),
    SessIds = ?config(session_ids, Config),
    lists:foreach(fun(SessId) ->
        ?assertEqual(ok, rpc:call(hd(Workers), session_manager,
            remove_session, [SessId]))
    end, SessIds),
    test_utils:mock_validate_and_unload(Workers, communicator).

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