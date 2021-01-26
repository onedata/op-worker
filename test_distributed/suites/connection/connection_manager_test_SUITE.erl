%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc

%%% This test suite verifies correct behaviour of outgoing_connection_manager
%%% that should (re)start connections to peer providers until backoff limit
%%% is reached.
%%%
%%% NOTE !!!
%%% Providers do not support even one common space so connection is not
%%% made automatically and must be initiated manually in tests
%%% (helps with mocking and testing backoff).
%%% @end
%%%-------------------------------------------------------------------
-module(connection_manager_test_SUITE).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    consecutive_failures_to_verify_peer_should_terminate_session_test/1,
    consecutive_failures_to_perform_handshake_should_terminate_session_test/1,
    if_at_least_one_handshake_succeeds_session_should_not_be_terminated_test/1,
    crushed_connection_should_be_restarted_test/1
]).

all() -> [
    consecutive_failures_to_verify_peer_should_terminate_session_test,
    consecutive_failures_to_perform_handshake_should_terminate_session_test,
    if_at_least_one_handshake_succeeds_session_should_not_be_terminated_test,
    crushed_connection_should_be_restarted_test
].

-define(ATTEMPTS, 40).


%%%===================================================================
%%% Test functions
%%%===================================================================


consecutive_failures_to_verify_peer_should_terminate_session_test(Config) ->
    [Worker1P2, Worker1P1, _Worker2P1] = ?config(op_worker_nodes, Config),
    P1Id = op_test_rpc:get_provider_id(Worker1P1),

    set_conn_manager_backoff_parameters(Worker1P2),

    SessId = get_outgoing_provider_session(P1Id),

    start_outgoing_provider_session(Worker1P2, SessId),
    ?assertEqual(true, outgoing_provider_session_exists(Worker1P2, SessId)),

    % After reaching max backoff period session should be terminated
    ?assertEqual(false, outgoing_provider_session_exists(Worker1P2, SessId), ?ATTEMPTS),

    ok.


consecutive_failures_to_perform_handshake_should_terminate_session_test(Config) ->
    [Worker1P2, Worker1P1, _Worker2P1] = ?config(op_worker_nodes, Config),
    P1Id = op_test_rpc:get_provider_id(Worker1P1),

    set_conn_manager_backoff_parameters(Worker1P2),

    SessId = session_utils:get_provider_session_id(outgoing, P1Id),

    start_outgoing_provider_session(Worker1P2, SessId),
    ?assertEqual(true, outgoing_provider_session_exists(Worker1P2, SessId)),

    % After reaching max backoff period session should be terminated
    ?assertEqual(false, outgoing_provider_session_exists(Worker1P2, SessId), ?ATTEMPTS),

    ok.


if_at_least_one_handshake_succeeds_session_should_not_be_terminated_test(Config) ->
    [Worker1P2, Worker1P1, _Worker2P1] = ?config(op_worker_nodes, Config),
    P1Id = op_test_rpc:get_provider_id(Worker1P1),

    set_conn_manager_backoff_parameters(Worker1P2),

    SessId = session_utils:get_provider_session_id(outgoing, P1Id),

    start_outgoing_provider_session(Worker1P2, SessId),
    ?assertEqual(true, outgoing_provider_session_exists(Worker1P2, SessId)),

    % Even after reaching max backoff period session should not be terminated
    % as there exists connection to one of the peer nodes
    timer:sleep(timer:seconds(?ATTEMPTS)),
    ?assertEqual(true, outgoing_provider_session_exists(Worker1P2, SessId)),
    ?assertMatch([_], get_outgoing_provider_session_connections(Worker1P2, SessId)),

    ok.


crushed_connection_should_be_restarted_test(Config) ->
    [Worker1P2, Worker1P1, _Worker2P1] = ?config(op_worker_nodes, Config),
    P1Id = op_test_rpc:get_provider_id(Worker1P1),

    set_conn_manager_backoff_parameters(Worker1P2),

    SessId = session_utils:get_provider_session_id(outgoing, P1Id),

    start_outgoing_provider_session(Worker1P2, SessId),
    ?assertEqual(true, outgoing_provider_session_exists(Worker1P2, SessId)),
    [Conn1, Conn2] = ?assertMatch(
        [_, _], get_outgoing_provider_session_connections(Worker1P2, SessId), ?ATTEMPTS
    ),
    Cons1 = lists:sort([Conn1, Conn2]),

    connection:close(Conn2),
    timer:sleep(timer:seconds(5)),

    Cons2 = lists:sort(?assertMatch(
        [_, _], get_outgoing_provider_session_connections(Worker1P2, SessId), ?ATTEMPTS
    )),

    ?assert(lists:member(Conn1, Cons2)),
    ?assertNotEqual(Cons1, Cons2),

    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        NewConfig3 = initializer:create_test_users_and_spaces(
            ?TEST_FILE(NewConfig2, "env_desc.json"), NewConfig2
        ),
        initializer:mock_auth_manager(NewConfig3),
        NewConfig3
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, ?MODULE]} | Config].


end_per_suite(Config) ->
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:teardown_storage(Config).


init_per_testcase(consecutive_failures_to_verify_peer_should_terminate_session_test = Case, Config) ->
    [Worker1P2 | _] = ?config(op_worker_nodes, Config),
    mock_provider_identity_verification(Worker1P2),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(consecutive_failures_to_perform_handshake_should_terminate_session_test = Case, Config) ->
    [_Worker1P2, Worker1P1, Worker2P1] = ?config(op_worker_nodes, Config),
    mock_provider_handshake([Worker1P1, Worker2P1]),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(if_at_least_one_handshake_succeeds_session_should_not_be_terminated_test = Case, Config) ->
    [_Worker1P2, Worker1P1, _Worker2P1] = ?config(op_worker_nodes, Config),
    mock_provider_handshake([Worker1P1]),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(consecutive_failures_to_verify_peer_should_terminate_session_test = Case, Config) ->
    [Worker1P2 | _] = ?config(op_worker_nodes, Config),
    unmock_provider_identity_verification(Worker1P2),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(consecutive_failures_to_perform_handshake_should_terminate_session_test = Case, Config) ->
    [_Worker1P2, Worker1P1, Worker2P1] = ?config(op_worker_nodes, Config),
    unmock_provider_handshake([Worker1P1, Worker2P1]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(if_at_least_one_handshake_succeeds_session_should_not_be_terminated_test = Case, Config) ->
    [_Worker1P2, Worker1P1, _Worker2P1] = ?config(op_worker_nodes, Config),
    unmock_provider_handshake([Worker1P1]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_Case, _Config) ->
    ok.


%%%===================================================================
%%% Helper functions
%%%===================================================================


%% @private
-spec set_conn_manager_backoff_parameters(node()) -> ok.
set_conn_manager_backoff_parameters(Node) ->
    lists:foreach(fun({Env, Val}) ->
        ok = rpc:call(Node, application, set_env, [op_worker, Env, Val])
    end, [
        {conn_manager_initial_backoff_interval, 2000},
        {conn_manager_backoff_interval_rate, 2},
        {conn_manager_max_backoff_interval, 10000}
    ]).


%% @private
-spec get_outgoing_provider_session(od_provider:id()) -> session:id().
get_outgoing_provider_session(PeerId) ->
    session_utils:get_provider_session_id(outgoing, PeerId).


%% @private
-spec start_outgoing_provider_session(node(), session:id()) -> ok.
start_outgoing_provider_session(Node, SessionId) ->
    ?assertEqual(
        {ok, SessionId},
        rpc:call(Node, session_connections, ensure_connected, [SessionId])
    ),
    ok.


%% @private
-spec outgoing_provider_session_exists(node(), session:id()) -> boolean().
outgoing_provider_session_exists(Node, SessionId) ->
    rpc:call(Node, session, exists, [SessionId]).


%% @private
-spec get_outgoing_provider_session_connections(node(), session:id()) -> [pid()].
get_outgoing_provider_session_connections(Node, SessionId) ->
    {ok, #document{value = #session{
        connections = Cons
    }}} = ?assertMatch({ok, _}, rpc:call(Node, session, get, [SessionId])),

    Cons.


%% @private
-spec mock_provider_identity_verification(node()) -> ok.
mock_provider_identity_verification(Node) ->
    ok = test_utils:mock_new(Node, provider_logic, [passthrough]),
    ok = test_utils:mock_expect(Node, provider_logic, verify_provider_identity, fun(_) ->
        {error, unverified_provider}
    end).


%% @private
-spec unmock_provider_identity_verification(node()) -> ok.
unmock_provider_identity_verification(Node) ->
    ok = test_utils:mock_unload(Node, auth_manager).


%% @private
-spec mock_provider_handshake([node()]) -> ok.
mock_provider_handshake(Nodes) ->
    test_utils:mock_new(Nodes, connection_auth, [passthrough]),
    test_utils:mock_expect(Nodes, connection_auth, handle_handshake, fun(_, _) ->
        throw(invalid_token)
    end).


%% @private
-spec unmock_provider_handshake([node()]) -> ok.
unmock_provider_handshake(Nodes) ->
    ok = test_utils:mock_unload(Nodes, connection_auth).
