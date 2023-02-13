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

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    session_creation_reuse_and_cleaning_test/1,
    session_manager_session_components_running_test/1,
    session_manager_supervision_tree_structure_test/1,
    session_manager_session_removal_test/1,
    session_getters_test/1,
    session_supervisor_child_crash_test/1,
    session_create_terminate_test/1]).

all() ->
    ?ALL([
        session_creation_reuse_and_cleaning_test,
        session_manager_session_components_running_test,
        session_manager_supervision_tree_structure_test,
        session_manager_session_removal_test,
        session_getters_test,
        session_supervisor_child_crash_test,
        session_create_terminate_test
    ]).

-define(TIMEOUT, timer:seconds(5)).

%%%===================================================================
%%% Test functions
%%%===================================================================

%% Check whether session manager creates session for the first time and then
%% reuses it despite of node on which request is processed.
session_creation_reuse_and_cleaning_test(Config) ->
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),
    Self = self(),

    Nonce1 = <<"nonce_1">>,
    Nonce2 = <<"nonce_2">>,
    User1Id = <<"user_id_1">>,
    User2Id = <<"user_id_2">>,
    AccessToken1 = initializer:create_access_token(User1Id),
    AccessToken2 = initializer:create_access_token(User2Id),

    [SessId1, SessId2] = lists:map(fun({Nonce, UserId, AccessToken, Workers}) ->
        Answers = [{ok, SessId} | _] = lists_utils:pmap(fun(Worker) ->
            fuse_test_utils:setup_fuse_session(Worker, UserId, Nonce, AccessToken, Self)
        end, Workers),

        % Check if all operations succeeded and all returned the same session id
        ?assertEqual([], lists:filter(fun(Ans) -> {ok, SessId} =/= Ans end, Answers)),

        % Check connections have been added to session
        {ok, _EffSessId, Cons} = ?assertMatch({ok, _, _},
            rpc:call(Worker1, session_connections, list, [SessId]), 10),
        ?assertEqual(length(Answers), length(Cons)),
        SessId
    end, [
        {Nonce1, User1Id, AccessToken1, lists:duplicate(2, Worker1) ++ lists:duplicate(2, Worker2)},
        {Nonce2, User2Id, AccessToken2, lists:duplicate(2, Worker2) ++ lists:duplicate(2, Worker1)}
    ]),

    lists:foreach(fun({Worker, SessId}) ->
        {Supervisor, Node} = get_supervisor_and_node(Worker, SessId),
        ?assert(is_supervisor_alive(Worker, Supervisor, Node)),

        % Stop and check if session is cleared
        ?assertEqual(ok, rpc:call(Worker, session_manager, terminate_session, [SessId])),
        ?assertEqual({error, not_found}, rpc:call(Worker, session, get, [SessId]), 10),
        ?assertEqual(false, is_supervisor_alive(Worker, Supervisor, Node), 10)
    end, [{Worker1, SessId1}, {Worker2, SessId2}]).

session_create_terminate_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    UserId = <<"user_id">>,
    Nonce = <<"nonce">>,
    AccessToken = initializer:create_access_token(UserId),

    {ok, SessId} = fuse_test_utils:setup_fuse_session(Worker, UserId, Nonce, AccessToken),

    % Mock session_manager to force race between session creating and stopping
    % (process that is cleaning session is sleeping for 5 seconds to ensure that
    % spawned process will be trying to initialize session during clearing)
    Master = self(),
    test_utils:mock_expect(Worker, session_manager, clean_terminated_session, fun(SessIdArg) ->
        Ans = meck:passthrough([SessIdArg]),
        spawn(fun() ->
            AnsToVerify = fuse_test_utils:setup_fuse_session(Worker, UserId, Nonce, AccessToken),
            Master ! {reuse_ans, AnsToVerify}
        end),
        timer:sleep(timer:seconds(5)),
        Ans
    end),

    % Check connection has been added to session
    ?assertMatch({ok, _, [_]}, rpc:call(Worker, session_connections, list, [SessId]), 10),

    {Supervisor1, Node1} = get_supervisor_and_node(Worker, SessId),
    ?assert(is_supervisor_alive(Worker, Supervisor1, Node1)),

    % Stop and check if it has been renewed
    ?assertEqual(ok, rpc:call(Worker, session_manager, terminate_session, [SessId])),
    ReuseAns = receive
        {reuse_ans, ReceivedAns} -> ReceivedAns
    after
        timer:seconds(10) -> timeout
    end,
    ?assertEqual({ok, SessId}, ReuseAns),
    {Supervisor2, Node2} = get_supervisor_and_node(Worker, SessId),
    ?assertEqual(false, is_supervisor_alive(Worker, Supervisor1, Node1), 10),
    ?assertNotEqual(Supervisor1, Supervisor2),
    ?assert(is_supervisor_alive(Worker, Supervisor2, Node2)),

    % Allow session usual clearing
    test_utils:mock_expect(Worker, session_manager, clean_terminated_session, fun(SessIdArg) ->
        meck:passthrough([SessIdArg])
    end),

    % Stop and check if session is cleared
    ?assertEqual(ok, rpc:call(Worker, session_manager, terminate_session, [SessId])),
    ?assertEqual({error, not_found}, rpc:call(Worker, session, get, [SessId]), 10),
    ?assertEqual(false, is_supervisor_alive(Worker, Supervisor2, Node2), 10),

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
        {Node1, [SessSupervisor1, EvtMan1, SeqMan1]},
        {Node2, [SessSupervisor2, EvtMan2, SeqMan2]}
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
    ?assertEqual(true, is_child({session_manager_worker_sup, Node1}, SessSupervisor1)),
    ?assertEqual(true, is_child({session_manager_worker_sup, Node2}, SessSupervisor2)),
    [
        [EvtManSupervisor1, SeqManSupervisor1],
        [EvtManSupervisor2, SeqManSupervisor2]
    ] = lists:map(fun(SessSup) ->
        lists:map(fun(ChildId) ->
            Answer = get_child(SessSup, ChildId),
            ?assertMatch({ok, _}, Answer),
            {ok, Child} = Answer,
            Child
        end, [event_manager_sup, sequencer_manager_sup])
    end, [SessSupervisor1, SessSupervisor2]),
    lists:foreach(fun({Supervisor, Child}) ->
        ?assert(is_child(Supervisor, Child))
    end, [{EvtManSupervisor1, EvtMan1}, {SeqManSupervisor1, SeqMan1},
        {EvtManSupervisor2, EvtMan2}, {SeqManSupervisor2, SeqMan2}]),

    ok.

%% Check whether session manager can remove session despite of node on which
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

    lists_utils:pforeach(fun({SessId, Node, Pids, Worker}) ->
        ?assertEqual(ok, rpc:call(Worker, session_manager,
            terminate_session, [SessId])),

        % Check whether session has been removed from cache.
        ?assertMatch({error, not_found}, rpc:call(Worker,
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

    lists:foreach(fun({GetterModule, GetterName}) ->
        {ok, Result} = ?assertMatch({ok, _},
            rpc:call(Worker, GetterModule, GetterName, [SessId])),
        case Result of
            [Inner] -> ?assert(is_pid(Inner));
            _ -> ?assert(is_pid(Result))
        end
    end, [{session, get_event_manager}, {session, get_sequencer_manager}]),

    {ok, _, [Conn]} = ?assertMatch(
        {ok, SessId, [_]},
        rpc:call(Worker, session_connections, list, [SessId])
    ),
    ?assert(is_pid(Conn)),

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
    UserId = <<"user_id">>,
    Nonce = <<"nonce">>,

    lists:foreach(fun({ChildId, Fun, Args}) ->
        {ok, SessId} = fuse_test_utils:setup_fuse_session(Worker, UserId, Nonce),

        {ok, {SessSup, Node}} = rpc:call(Worker, session,
            get_session_supervisor_and_node, [SessId]),

        {ok, Child} = get_child(SessSup, ChildId),

        apply(Fun, [Child | Args]),

        ?assertMatch({error, not_found}, rpc:call(Worker, session, get, [SessId]), 10),
        ?assertEqual([], supervisor:which_children({session_manager_worker_sup, Node}))
    end, [
        {event_manager_sup, fun erlang:exit/2, [kill]},
        {sequencer_manager_sup, fun erlang:exit/2, [kill]},
        {incoming_session_watcher, fun gen_server:cast/2, [kill]}
    ]),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        [Worker | _] = ?config(op_worker_nodes, NewConfig),
        initializer:clear_subscriptions(Worker),
        initializer:mock_auth_manager(NewConfig),
        NewConfig
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, fuse_test_utils]} | Config].


init_per_testcase(session_creation_reuse_and_cleaning_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Workers),
    Config;

init_per_testcase(session_create_terminate_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Workers),
    test_utils:mock_new(Workers, session_manager),
    Config;

init_per_testcase(session_getters_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    UserId = <<"user_id">>,
    Nonce = <<"nonce">>,
    initializer:communicator_mock(Worker),
    {ok, SessId} = fuse_test_utils:setup_fuse_session(Worker, UserId, Nonce),
    [{session_id, SessId}, {identity, ?SUB(user, <<"user_id">>)} | Config];

init_per_testcase(session_supervisor_child_crash_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    initializer:communicator_mock(Worker),
    test_utils:mock_new(Worker, onedata_logger, [passthrough]),
    test_utils:mock_expect(Worker, onedata_logger, log, fun
        (A, B, C) -> meck:passthrough([A, B, C])
    end),

    Config;

init_per_testcase(Case, Config) when
    Case =:= session_manager_session_components_running_test;
    Case =:= session_manager_supervision_tree_structure_test;
    Case =:= session_manager_session_removal_test
->
    Workers = ?config(op_worker_nodes, Config),
    Nonce1 = <<"nonce_1">>,
    Nonce2 = <<"nonce_2">>,
    User1Id = <<"user_id_1">>,
    User2Id = <<"user_id_2">>,

    initializer:communicator_mock(Workers),
    {ok, SessId1} = fuse_test_utils:setup_fuse_session(hd(Workers), User1Id, Nonce1),
    {ok, SessId2} = fuse_test_utils:setup_fuse_session(hd(Workers), User2Id, Nonce2),
    ?assertEqual(ok, rpc:call(hd(Workers), session_manager,
        terminate_session, [?ROOT_SESS_ID])),
    ?assertEqual(ok, rpc:call(hd(Workers), session_manager,
        terminate_session, [?GUEST_SESS_ID])),

    [
        {session_ids, [SessId1, SessId2]},
        {identities, [?SUB(user, User1Id), ?SUB(user, User2Id)]}
        | Config
    ].

end_per_suite(_Config) ->
    ok.

end_per_testcase(session_getters_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    basic_session_teardown(Worker, Config),
    test_utils:mock_validate_and_unload(Worker, communicator);

end_per_testcase(session_supervisor_child_crash_test, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Worker, onedata_logger);

end_per_testcase(Case, Config) when
    Case =:= session_creation_reuse_and_cleaning_test;
    Case =:= session_manager_session_removal_test ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, communicator);

end_per_testcase(session_create_terminate_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, session_manager),
    test_utils:mock_validate_and_unload(Workers, communicator);

end_per_testcase(Case, Config) when
    Case =:= session_manager_session_components_running_test;
    Case =:= session_manager_supervision_tree_structure_test ->
    Workers = ?config(op_worker_nodes, Config),
    SessIds = ?config(session_ids, Config),
    lists:foreach(fun(SessId) ->
        ?assertEqual(ok, rpc:call(hd(Workers), session_manager,
            terminate_session, [SessId]))
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

%%--------------------------------------------------------------------
%% @doc
%% Removes basic test session.
%% @end
%%--------------------------------------------------------------------
-spec basic_session_teardown(Worker :: node(), Config :: term()) -> NewConfig :: term().
basic_session_teardown(Worker, Config) ->
    SessId = proplists:get_value(session_id, Config),
    ?assertEqual(ok, rpc:call(Worker, session_manager, terminate_session, [SessId])).

%% @private
-spec is_supervisor_alive(Worker :: node(), Sup :: pid(), Node :: node()) -> boolean().
is_supervisor_alive(Worker, Sup, Node) ->
    SupChildren = rpc:call(Worker, supervisor, which_children, [{?SESSION_MANAGER_WORKER_SUP, Node}]),
    length(lists:filter(fun({_, Child, _, _}) -> Child =:= Sup end, SupChildren)) =:= 1.

%% @private
-spec get_supervisor_and_node(Worker :: node(), SessId :: session:id()) ->
    {Supervisor :: pid(), Node :: node()}.
get_supervisor_and_node(Worker, SessId) ->
    {ok, #document{value = #session{supervisor = Supervisor, node = Node}}} =
        ?assertMatch({ok, _}, rpc:call(Worker, session, get, [SessId])),
    {Supervisor, Node}.