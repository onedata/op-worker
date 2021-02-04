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

-include("api_file_test_utils.hrl").
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
    reconnects_should_be_performed_with_backoff_test/1,
    crashed_connection_should_be_restarted_test/1
]).

all() -> [
    consecutive_failures_to_verify_peer_should_terminate_session_test,
    consecutive_failures_to_perform_handshake_should_terminate_session_test,
    reconnects_should_be_performed_with_backoff_test,
    crashed_connection_should_be_restarted_test
].

-define(ATTEMPTS, 40).


%%%===================================================================
%%% Test functions
%%%===================================================================


% identity verification is mocked in init_per_testcase to always fail
consecutive_failures_to_verify_peer_should_terminate_session_test(_Config) ->
    ParisId = oct_background:get_provider_id(paris),
    [KrakowNode] = oct_background:get_provider_nodes(krakow),

    SessId = get_outgoing_provider_session_id(ParisId),

    ?assertEqual(false, session_exists(KrakowNode, SessId)),

    start_outgoing_provider_session(KrakowNode, SessId),
    ?assertEqual(true, session_exists(KrakowNode, SessId)),

    % After reaching max backoff period session should be terminated
    ?assertEqual(false, session_exists(KrakowNode, SessId), ?ATTEMPTS),

    ok.


% handshake is mocked in init_per_testcase to always fail
consecutive_failures_to_perform_handshake_should_terminate_session_test(_Config) ->
    ParisId = oct_background:get_provider_id(paris),
    [KrakowNode] = oct_background:get_provider_nodes(krakow),

    SessId = session_utils:get_provider_session_id(outgoing, ParisId),

    ?assertEqual(false, session_exists(KrakowNode, SessId)),

    start_outgoing_provider_session(KrakowNode, SessId),
    ?assertEqual(true, session_exists(KrakowNode, SessId)),

    % After reaching max backoff period session should be terminated
    ?assertEqual(false, session_exists(KrakowNode, SessId), ?ATTEMPTS),

    ok.


% handshake is mocked in init_per_testcase to succeed only after 2 retries
reconnects_should_be_performed_with_backoff_test(_Config) ->
    ParisId = oct_background:get_provider_id(paris),
    [KrakowNode] = oct_background:get_provider_nodes(krakow),

    SessId = session_utils:get_provider_session_id(outgoing, ParisId),

    ?assertEqual(false, session_exists(KrakowNode, SessId)),

    start_outgoing_provider_session(KrakowNode, SessId),
    ?assertEqual(true, session_exists(KrakowNode, SessId)),

    % Normally session exists entire time while next connection retries are
    % performed and terminates only after last failed attempt. Only if any
    % connection attempt succeeded it will persist. So to check if backoff
    % worked properly checking whether session exists must be performed after
    % time the session could die
    timer:sleep(timer:seconds(?ATTEMPTS)),

    ?assertEqual(true, session_exists(KrakowNode, SessId)),

    ok.


crashed_connection_should_be_restarted_test(_Config) ->
    ParisId = oct_background:get_provider_id(paris),
    [KrakowNode] = oct_background:get_provider_nodes(krakow),

    SessId = session_utils:get_provider_session_id(outgoing, ParisId),

    ?assertEqual(false, session_exists(KrakowNode, SessId)),

    start_outgoing_provider_session(KrakowNode, SessId),
    ?assertEqual(true, session_exists(KrakowNode, SessId)),
    [Conn1] = ?assertMatch([_], get_session_connections(KrakowNode, SessId), ?ATTEMPTS),

    % Close connection and wait some time after so that it would be unregistered
    % from session connections before waiting for new connection to be made.
    % Unfortunately, it can't be made without sleep as this would introduce races
    % (e.g. Conn1 has not been unregistered and was fetched once again)
    connection:close(Conn1),
    timer:sleep(timer:seconds(5)),
    [Conn2] = ?assertMatch([_], get_session_connections(KrakowNode, SessId), ?ATTEMPTS),

    ?assertNotEqual(Conn1, Conn2),

    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    ssl:start(),
    hackney:start(),
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "2op_no_common_spaces",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    hackney:stop(),
    ssl:stop().


init_per_testcase(consecutive_failures_to_verify_peer_should_terminate_session_test = Case, Config) ->
    [Node] = oct_background:get_provider_nodes(krakow),
    mock_provider_identity_verification_to_always_fail(Node),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(consecutive_failures_to_perform_handshake_should_terminate_session_test = Case, Config) ->
    Nodes = oct_background:get_provider_nodes(paris),
    mock_handshake_to_succeed_after_n_retries(Nodes, infinity),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(reconnects_should_be_performed_with_backoff_test = Case, Config) ->
    Nodes = oct_background:get_provider_nodes(paris),
    mock_handshake_to_succeed_after_n_retries(Nodes, 2),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(_Case, Config) ->
    [KrakowNode] = oct_background:get_provider_nodes(krakow),
    ParisId = oct_background:get_provider_id(paris),
    SessId = session_utils:get_provider_session_id(outgoing, ParisId),

    terminate_session(KrakowNode, SessId),
    ?assertEqual(false, session_exists(KrakowNode, SessId), ?ATTEMPTS),

    set_conn_manager_backoff_parameters(KrakowNode),

    Config.


end_per_testcase(consecutive_failures_to_verify_peer_should_terminate_session_test = Case, Config) ->
    [Node] = oct_background:get_provider_nodes(krakow),
    unmock_provider_identity_verification(Node),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(consecutive_failures_to_perform_handshake_should_terminate_session_test = Case, Config) ->
    Nodes = oct_background:get_provider_nodes(paris),
    unmock_provider_handshake(Nodes),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(reconnects_should_be_performed_with_backoff_test = Case, Config) ->
    Nodes = oct_background:get_provider_nodes(paris),
    unmock_provider_handshake(Nodes),
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
        {conn_manager_min_backoff_interval, 2000},
        {conn_manager_max_backoff_interval, 10000},
        {conn_manager_backoff_interval_rate, 2}
    ]).


%% @private
-spec get_outgoing_provider_session_id(od_provider:id()) -> session:id().
get_outgoing_provider_session_id(PeerId) ->
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
-spec terminate_session(node(), session:id()) -> ok.
terminate_session(Node, SessionId) ->
    ?assertEqual(ok, rpc:call(Node, session_manager, terminate_session, [SessionId])).


%% @private
-spec session_exists(node(), session:id()) -> boolean().
session_exists(Node, SessionId) ->
    rpc:call(Node, session, exists, [SessionId]).


%% @private
-spec get_session_connections(node(), session:id()) -> [pid()].
get_session_connections(Node, SessionId) ->
    {ok, #document{value = #session{
        connections = Cons
    }}} = ?assertMatch({ok, _}, rpc:call(Node, session, get, [SessionId])),

    Cons.


%% @private
-spec mock_provider_identity_verification_to_always_fail(node()) -> ok.
mock_provider_identity_verification_to_always_fail(Node) ->
    ok = test_utils:mock_new(Node, provider_logic, [passthrough]),
    ok = test_utils:mock_expect(Node, provider_logic, verify_provider_identity, fun(_) ->
        {error, unverified_provider}
    end).


%% @private
-spec unmock_provider_identity_verification(node()) -> ok.
unmock_provider_identity_verification(Node) ->
    ok = test_utils:mock_unload(Node, provider_logic).


%% @private
-spec mock_handshake_to_succeed_after_n_retries([node()], infinity | non_neg_integer()) -> ok.
mock_handshake_to_succeed_after_n_retries(Nodes, MaxFailedAttempts) ->
    {_, []} = rpc:multicall(Nodes, application, set_env, [
        ?APP_NAME, test_handshake_failed_attempts, MaxFailedAttempts
    ]),

    test_utils:mock_new(Nodes, connection_auth, [passthrough]),
    test_utils:mock_expect(Nodes, connection_auth, handle_handshake, fun(Request, IpAddress) ->
        case application:get_env(?APP_NAME, test_handshake_failed_attempts, 0) of
            infinity ->
                throw(invalid_token);
            0 ->
                meck:passthrough([Request, IpAddress]);
            Num ->
                application:set_env(?APP_NAME, test_handshake_failed_attempts, Num - 1),
                throw(invalid_token)
        end
    end).


%% @private
-spec unmock_provider_handshake([node()]) -> ok.
unmock_provider_handshake(Nodes) ->
    ok = test_utils:mock_unload(Nodes, connection_auth).
