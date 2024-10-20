%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests user auth cache and token authentication.
%%% @end
%%%--------------------------------------------------------------------
-module(user_auth_test_SUITE).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("graph_sync/provider_graph_sync.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([
    all/0,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    auth_cache_expiration_test/1,
    auth_cache_expiration_with_time_warps_test/1,
    auth_cache_size_test/1,
    auth_cache_user_access_blocked_event_test/1,
    auth_cache_named_token_events_test/1,
    auth_cache_temporary_token_events_test/1,
    auth_cache_oz_conn_status_test/1,
    token_authentication/1,
    token_expiration/1
]).

all() -> ?ALL([
    auth_cache_expiration_test,
    auth_cache_expiration_with_time_warps_test,
    auth_cache_size_test,
    auth_cache_user_access_blocked_event_test,
    auth_cache_named_token_events_test,
    auth_cache_temporary_token_events_test,
    auth_cache_oz_conn_status_test,
    token_authentication,
    token_expiration
]).


-define(DEFAULT_AUTH_CACHE_SIZE_LIMIT, 5000).

-define(USER_ID_1, <<"test_id_1">>).
-define(USER_ID_2, <<"test_id_2">>).
-define(USER_FULL_NAME, <<"test_name">>).

-define(NOW(), global_clock:timestamp_seconds()).

-define(ATTEMPTS, 60).

%%%===================================================================
%%% Test functions
%%%===================================================================


auth_cache_expiration_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    Mod = token_logic,
    Fun = verify_access_token,

    clear_auth_caches(Config),

    AccessToken1 = initializer:create_access_token(?USER_ID_1, [], temporary),
    AccessToken2 = initializer:create_access_token(?USER_ID_1, [#cv_time{
        valid_until = ?NOW() + 15
    }], named),
    AccessToken3 = initializer:create_access_token(?USER_ID_2, [], named),

    TokenCredentials1 = create_token_credentials(AccessToken1, rest),
    TokenCredentials2 = create_token_credentials(AccessToken2, graphsync),
    TokenCredentials3 = create_token_credentials(
        AccessToken3, oneclient, allow_data_access_caveats, AccessToken1
    ),

    set_auth_cache_default_ttl(Worker1, 2),
    ?assertEqual(0, rpc:call(Worker1, meck, num_calls, [Mod, Fun, '_'])),

    lists:foreach(fun({TokenCredentials, ExpUserId}) ->
        ?assertMatch({ok, ?USER(ExpUserId), _}, verify_credentials(Worker1, TokenCredentials))
    end, [
        {TokenCredentials1, ?USER_ID_1},
        {TokenCredentials2, ?USER_ID_1},
        {TokenCredentials3, ?USER_ID_2}
    ]),
    ?assertEqual(3, get_auth_cache_size(Worker1)),

    % Assert proper number of calls and access/consumer token arguments
    ?assertEqual(3, rpc:call(Worker1, meck, num_calls, [Mod, Fun, '_'])),
    ?assertMatch(AccessToken1, rpc:call(Worker1, meck, capture, [1, Mod, Fun, '_', 1])),
    ?assertMatch(undefined, rpc:call(Worker1, meck, capture, [1, Mod, Fun, '_', 2])),
    ?assertMatch(AccessToken2, rpc:call(Worker1, meck, capture, [2, Mod, Fun, '_', 1])),
    ?assertMatch(undefined, rpc:call(Worker1, meck, capture, [2, Mod, Fun, '_', 2])),
    ?assertMatch(AccessToken3, rpc:call(Worker1, meck, capture, [3, Mod, Fun, '_', 1])),
    ?assertMatch(AccessToken1, rpc:call(Worker1, meck, capture, [3, Mod, Fun, '_', 2])),

    % TokenCredentials without consumer token should be cached for as long as token allows
    % (time caveats). On the other hand TokenCredentials with consumer token should be cached
    % for only limited amount of time as provider can't subscribe to and monitor consumer tokens
    lists:foreach(fun(ZoneCallsNum) ->
        timer:sleep(timer:seconds(3)),

        ?assertMatch({ok, ?USER(?USER_ID_1), _}, verify_credentials(Worker1, TokenCredentials1)),
        ?assertEqual(ZoneCallsNum, rpc:call(Worker1, meck, num_calls, [Mod, Fun, '_'])),

        ?assertMatch({ok, ?USER(?USER_ID_1), _}, verify_credentials(Worker1, TokenCredentials2)),
        ?assertEqual(ZoneCallsNum, rpc:call(Worker1, meck, num_calls, [Mod, Fun, '_'])),

        ?assertMatch({ok, ?USER(?USER_ID_2), _}, verify_credentials(Worker1, TokenCredentials3)),
        ?assertEqual(ZoneCallsNum + 1, rpc:call(Worker1, meck, num_calls, [Mod, Fun, '_'])),
        ?assertMatch(AccessToken3, rpc:call(Worker1, meck, capture, [ZoneCallsNum + 1, Mod, Fun, '_', 1]))

    end, lists:seq(3, 6)),

    timer:sleep(timer:seconds(4)),

    % AccessToken1 without time caveats is still cached (for eternity or until token event is received)
    ?assertMatch({ok, ?USER(?USER_ID_1), undefined}, verify_credentials(Worker1, TokenCredentials1)),
    ?assertEqual(7, rpc:call(Worker1, meck, num_calls, [Mod, Fun, '_'])),

    % AccessToken2 with passed time limit is no longer cached
    ?assertMatch(?ERROR_UNAUTHORIZED, verify_credentials(Worker1, TokenCredentials2)),
    ?assertEqual(8, rpc:call(Worker1, meck, num_calls, [Mod, Fun, '_'])),
    ?assertMatch(AccessToken2, rpc:call(Worker1, meck, capture, [last, Mod, Fun, '_', 1])),

    clear_auth_caches(Config).


auth_cache_expiration_with_time_warps_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    Mod = token_logic,
    Fun = verify_access_token,

    clear_auth_caches(Config),

    AccessToken1 = initializer:create_access_token(?USER_ID_1, [], temporary),
    TokenCredentials1 = create_token_credentials(AccessToken1, rest),

    Token2TTL = ?NOW() + 1500,
    AccessToken2 = initializer:create_access_token(?USER_ID_1, [#cv_time{
        valid_until = Token2TTL
    }], named),
    TokenCredentials2 = create_token_credentials(AccessToken2, graphsync),

    set_auth_cache_default_ttl(Worker1, 5),

    ?assertMatch({ok, ?USER(?USER_ID_1), undefined}, verify_credentials(Worker1, TokenCredentials1)),
    ?assertMatch({ok, ?USER(?USER_ID_1), Token2TTL}, verify_credentials(Worker1, TokenCredentials2)),
    ?assertEqual(2, rpc:call(Worker1, meck, num_calls, [Mod, Fun, '_'])),

    simulate_gs_temporary_tokens_revocation(Worker1, ?USER_ID_1),

    % After temporary token revocation cache entries for such tokens should be changed to
    % ?ERROR_TOKEN_INVALID and kept for cache entry ttl independent of global time
    ?assertMatch(?ERROR_TOKEN_REVOKED, verify_credentials(Worker1, TokenCredentials1)),
    ?assertMatch({ok, ?USER(?USER_ID_1), Token2TTL}, verify_credentials(Worker1, TokenCredentials2)),
    ?assertEqual(2, rpc:call(Worker1, meck, num_calls, [Mod, Fun, '_'])),

    timer:sleep(timer:seconds(3)),
    time_test_utils:simulate_seconds_passing(2000),

    % Forward time warp should invalidate entries for tokens with expired ttl
    % and leave unchanged cached errors
    ?assertMatch(?ERROR_TOKEN_REVOKED, verify_credentials(Worker1, TokenCredentials1)),
    ?assertEqual(2, rpc:call(Worker1, meck, num_calls, [Mod, Fun, '_'])),
    ?assertMatch(?ERROR_UNAUTHORIZED, verify_credentials(Worker1, TokenCredentials2)),
    ?assertEqual(3, rpc:call(Worker1, meck, num_calls, [Mod, Fun, '_'])),

    % After passage of local time greater than cache default ttl concrete error entries
    % are invalidated and tokens can be verified once again
    timer:sleep(timer:seconds(3)),

    ?assertMatch({ok, ?USER(?USER_ID_1), undefined}, verify_credentials(Worker1, TokenCredentials1)),
    ?assertEqual(4, rpc:call(Worker1, meck, num_calls, [Mod, Fun, '_'])),
    ?assertMatch(?ERROR_UNAUTHORIZED, verify_credentials(Worker1, TokenCredentials2)),
    ?assertEqual(4, rpc:call(Worker1, meck, num_calls, [Mod, Fun, '_'])),

    % Backward time warp should make tokens with ttl usable once again but only after cached
    % error entries for those tokens expires
    time_test_utils:simulate_seconds_passing(-1000),

    ?assertMatch({ok, ?USER(?USER_ID_1), undefined}, verify_credentials(Worker1, TokenCredentials1)),
    ?assertMatch(?ERROR_UNAUTHORIZED, verify_credentials(Worker1, TokenCredentials2)),
    ?assertEqual(4, rpc:call(Worker1, meck, num_calls, [Mod, Fun, '_'])),

    timer:sleep(timer:seconds(3)),

    ?assertMatch({ok, ?USER(?USER_ID_1), undefined}, verify_credentials(Worker1, TokenCredentials1)),
    ?assertEqual(4, rpc:call(Worker1, meck, num_calls, [Mod, Fun, '_'])),
    ?assertMatch({ok, ?USER(?USER_ID_1), Token2TTL}, verify_credentials(Worker1, TokenCredentials2)),
    ?assertEqual(5, rpc:call(Worker1, meck, num_calls, [Mod, Fun, '_'])),

    clear_auth_caches(Config).


auth_cache_size_test(Config) ->
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    clear_auth_caches(Config),

    AccessToken = initializer:create_access_token(?USER_ID_1),

    TokenCredentials1 = create_token_credentials(AccessToken, undefined),
    TokenCredentials2 = create_token_credentials(AccessToken, graphsync),
    TokenCredentials3 = create_token_credentials(AccessToken, rest),
    TokenCredentials4 = create_token_credentials(AccessToken, oneclient, allow_data_access_caveats),
    TokenCredentials5 = create_token_credentials(AccessToken, oneclient, disallow_data_access_caveats),

    lists:foreach(fun({TokenCredentials, Worker, ExpCacheSize}) ->
        ?assertMatch(
            {ok, ?USER(?USER_ID_1), undefined},
            verify_credentials(Worker, TokenCredentials)
        ),
        ?assertEqual(ExpCacheSize, get_auth_cache_size(Worker), ?ATTEMPTS)
    end, [
        {TokenCredentials1, Worker1, 1},
        {TokenCredentials2, Worker2, 1},
        {TokenCredentials3, Worker1, 2},
        {TokenCredentials4, Worker2, 2},
        {TokenCredentials5, Worker1, 3}
    ]),

    % Default cache size limit is big enough so that 2 or 3 entries will not be purged
    timer:sleep(timer:seconds(5)),
    ?assertEqual(3, get_auth_cache_size(Worker1)),
    ?assertEqual(2, get_auth_cache_size(Worker2)),

    % Setting auth cache size limit to 2 should cause cache purge on Worker1
    % (cache entries exceed limit) but not on Worker2
    set_auth_cache_size_limit(Worker1, 2),
    set_auth_cache_size_limit(Worker2, 2),
    timer:sleep(timer:seconds(5)),
    ?assertEqual(0, get_auth_cache_size(Worker1)),
    ?assertEqual(2, get_auth_cache_size(Worker2)),

    clear_auth_caches(Config).


auth_cache_user_access_blocked_event_test(Config) ->
    [Worker1, Worker2 | _] = Nodes = ?config(op_worker_nodes, Config),

    % this will populate the cache with user docs that have 'blocked' set to false
    simulate_user_update_with_blocked_value(Nodes, ?USER_ID_1, false, 1),
    simulate_user_update_with_blocked_value(Nodes, ?USER_ID_2, false, 1),

    clear_auth_caches(Config),
    CacheItemTTL = 2,
    set_auth_cache_default_ttl(Worker1, CacheItemTTL),

    AccessToken1A = initializer:create_access_token(?USER_ID_1, [], named),
    AccessToken1B = initializer:create_access_token(?USER_ID_1, [], temporary),
    AccessToken2A = initializer:create_access_token(?USER_ID_2, [], named),
    AccessToken2B = initializer:create_access_token(?USER_ID_2, [], temporary),

    TokenCredentials1A = create_token_credentials(AccessToken1A),
    TokenCredentials1B = create_token_credentials(AccessToken1B),
    TokenCredentials2A = create_token_credentials(AccessToken2A),
    TokenCredentials2B = create_token_credentials(AccessToken2B),

    lists:foreach(fun({TokenCredentials, Worker, ExpUserId}) ->
        ?assertMatch({ok, ?USER(ExpUserId), undefined}, verify_credentials(Worker, TokenCredentials))
    end, [
        {TokenCredentials1A, Worker1, ?USER_ID_1},
        {TokenCredentials1B, Worker2, ?USER_ID_1},
        {TokenCredentials2A, Worker2, ?USER_ID_2},
        {TokenCredentials2B, Worker1, ?USER_ID_2}
    ]),

    ?assertEqual(2, get_auth_cache_size(Worker1)),
    ?assertEqual(2, get_auth_cache_size(Worker2)),

    InitialBlockChangeEvents = total_block_change_events(Nodes),

    simulate_user_update_with_blocked_value(Nodes, ?USER_ID_1, true, 2),
    ?assertEqual(InitialBlockChangeEvents + 1, total_block_change_events(Nodes)),
    ?assertMatch(?ERROR_USER_BLOCKED, verify_credentials(Worker1, TokenCredentials1A)),
    ?assertMatch(?ERROR_USER_BLOCKED, verify_credentials(Worker2, TokenCredentials1B)),
    ?assertMatch({ok, ?USER(?USER_ID_2), undefined}, verify_credentials(Worker2, TokenCredentials2A)),
    ?assertMatch({ok, ?USER(?USER_ID_2), undefined}, verify_credentials(Worker1, TokenCredentials2B)),
    % no new entries should be added (verification was done on the same nodes as previously)
    ?assertEqual(2, get_auth_cache_size(Worker1)),
    ?assertEqual(2, get_auth_cache_size(Worker2)),
    % verify on different nodes, which should cause new entries to be added to the cache
    ?assertMatch(?ERROR_USER_BLOCKED, verify_credentials(Worker2, TokenCredentials1A)),
    ?assertMatch(?ERROR_USER_BLOCKED, verify_credentials(Worker1, TokenCredentials1B)),
    ?assertMatch({ok, ?USER(?USER_ID_2), undefined}, verify_credentials(Worker1, TokenCredentials2A)),
    ?assertMatch({ok, ?USER(?USER_ID_2), undefined}, verify_credentials(Worker2, TokenCredentials2B)),
    ?assertEqual(4, get_auth_cache_size(Worker1)),
    ?assertEqual(4, get_auth_cache_size(Worker2)),

    % another update of the user document when the blocked value does not change
    % should not cause another block change event
    simulate_user_update_with_blocked_value(Nodes, ?USER_ID_1, true, 3),
    ?assertEqual(InitialBlockChangeEvents + 1, total_block_change_events(Nodes)),

    % while the user is blocked, verification of new tokens should be blocked too
    % (this is checked in token_logic mock)
    AccessToken1C = initializer:create_access_token(?USER_ID_1, [], named),
    TokenCredentials1C = create_token_credentials(AccessToken1C),
    ?assertMatch(?ERROR_USER_BLOCKED, verify_credentials(Worker1, TokenCredentials1C)),
    ?assertEqual(5, get_auth_cache_size(Worker1)),

    simulate_user_update_with_blocked_value(Nodes, ?USER_ID_2, true, 2),
    ?assertEqual(InitialBlockChangeEvents + 2, total_block_change_events(Nodes)),
    ?assertMatch(?ERROR_USER_BLOCKED, verify_credentials(Worker2, TokenCredentials1A)),
    ?assertMatch(?ERROR_USER_BLOCKED, verify_credentials(Worker1, TokenCredentials1B)),
    ?assertMatch(?ERROR_USER_BLOCKED, verify_credentials(Worker1, TokenCredentials2A)),
    ?assertMatch(?ERROR_USER_BLOCKED, verify_credentials(Worker2, TokenCredentials2B)),
    ?assertEqual(5, get_auth_cache_size(Worker1)),
    ?assertEqual(4, get_auth_cache_size(Worker2)),

    % user doc revision must be higher than the previous known one, otherwise
    % the change should not be taken into account (the user should be still blocked)
    simulate_user_update_with_blocked_value(Nodes, ?USER_ID_1, false, 2),
    ?assertEqual(InitialBlockChangeEvents + 2, total_block_change_events(Nodes)),
    ?assertMatch(?ERROR_USER_BLOCKED, verify_credentials(Worker1, TokenCredentials1A)),
    ?assertMatch(?ERROR_USER_BLOCKED, verify_credentials(Worker2, TokenCredentials1B)),
    ?assertMatch(?ERROR_USER_BLOCKED, verify_credentials(Worker1, TokenCredentials1C)),

    % newer revision should cause unblocking, cached and new tokens should be verifiable immediately
    simulate_user_update_with_blocked_value(Nodes, ?USER_ID_1, false, 4),
    ?assertEqual(InitialBlockChangeEvents + 3, total_block_change_events(Nodes)),
    ?assertMatch(?ERROR_USER_BLOCKED, verify_credentials(Worker1, TokenCredentials2A)),
    ?assertMatch(?ERROR_USER_BLOCKED, verify_credentials(Worker2, TokenCredentials2B)),
    ?assertMatch({ok, ?USER(?USER_ID_1), undefined}, verify_credentials(Worker1, TokenCredentials1A)),
    ?assertMatch({ok, ?USER(?USER_ID_1), undefined}, verify_credentials(Worker2, TokenCredentials1B)),
    ?assertMatch({ok, ?USER(?USER_ID_1), undefined}, verify_credentials(Worker2, TokenCredentials1C)),
    ?assertEqual(5, get_auth_cache_size(Worker1)),
    % TokenCredentials1C were verified on the second worker, which should cause new entry to be added
    ?assertEqual(5, get_auth_cache_size(Worker2)),

    AccessToken1D = initializer:create_access_token(?USER_ID_1, [], temporary),
    TokenCredentials1D = create_token_credentials(AccessToken1D),
    ?assertMatch({ok, ?USER(?USER_ID_1), undefined}, verify_credentials(Worker1, TokenCredentials1D)),
    ?assertEqual(6, get_auth_cache_size(Worker1)),

    % another update of the user document when the blocked value does not change
    % should not cause another block change event
    simulate_user_update_with_blocked_value(Nodes, ?USER_ID_1, false, 5),
    ?assertEqual(InitialBlockChangeEvents + 3, total_block_change_events(Nodes)),

    clear_auth_caches(Config).


auth_cache_named_token_events_test(Config) ->
    [Worker1, Worker2 | _] = Workers = ?config(op_worker_nodes, Config),

    clear_auth_caches(Config),

    AccessToken1 = initializer:create_access_token(?USER_ID_1, [], named),
    AccessToken2 = initializer:create_access_token(?USER_ID_2, [], named),

    TokenCredentials1 = create_token_credentials(AccessToken1, rest),
    TokenCredentials2 = create_token_credentials(AccessToken1, oneclient),
    TokenCredentials3 = create_token_credentials(AccessToken2, graphsync),

    lists:foreach(fun({TokenCredentials, Worker, ExpUserId, ExpCacheSize}) ->
        ?assertMatch({ok, ?USER(ExpUserId), undefined}, verify_credentials(Worker, TokenCredentials)),
        ?assertEqual(ExpCacheSize, get_auth_cache_size(Worker), ?ATTEMPTS)
    end, [
        {TokenCredentials1, Worker1, ?USER_ID_1, 1},
        {TokenCredentials2, Worker2, ?USER_ID_1, 1},
        {TokenCredentials3, Worker1, ?USER_ID_2, 2}
    ]),

    %% TOKEN EVENTS SEND ON ANY NODE SHOULD AFFECT ALL NODES AUTH CACHE

    % When AccessToken1 is revoked
    mock_token_logic_is_revoked(Workers, [AccessToken1]),
    simulate_gs_token_status_update(Worker2, AccessToken1, true),

    % Then only TokenCredentials based on that token should be revoked
    lists:foreach(fun({TokenCredentials, Worker}) ->
        ?assertMatch(?ERROR_TOKEN_REVOKED, verify_credentials(Worker, TokenCredentials))
    end, [{TokenCredentials1, Worker1}, {TokenCredentials2, Worker2}]),
    ?assertMatch({ok, ?USER(?USER_ID_2), undefined}, verify_credentials(Worker1, TokenCredentials3)),

    % Revoked tokens can be re-revoked and used again
    mock_token_logic_is_revoked(Workers, [AccessToken2]),
    simulate_gs_token_status_update(Worker1, AccessToken1, false),
    simulate_gs_token_status_update(Worker2, AccessToken2, true),

    lists:foreach(fun({TokenCredentials, Worker}) ->
        ?assertMatch({ok, ?USER(?USER_ID_1), _}, verify_credentials(Worker, TokenCredentials))
    end, [{TokenCredentials1, Worker1}, {TokenCredentials2, Worker2}]),
    ?assertMatch(?ERROR_TOKEN_REVOKED, verify_credentials(Worker1, TokenCredentials3)),

    % Deleting token should result in ?ERROR_TOKEN_INVALID
    simulate_gs_token_deletion(Worker1, AccessToken1),

    lists:foreach(fun({TokenCredentials, Worker}) ->
        ?assertMatch(?ERROR_TOKEN_INVALID, verify_credentials(Worker, TokenCredentials))
    end, [{TokenCredentials1, Worker1}, {TokenCredentials2, Worker2}]),
    ?assertMatch(?ERROR_TOKEN_REVOKED, verify_credentials(Worker1, TokenCredentials3)),

    % Revoking already deleted token should not change error
    mock_token_logic_is_revoked(Workers, [AccessToken2]),
    simulate_gs_token_status_update(Worker1, AccessToken1, true),

    lists:foreach(fun({TokenCredentials, Worker}) ->
        ?assertMatch(?ERROR_TOKEN_INVALID, verify_credentials(Worker, TokenCredentials))
    end, [{TokenCredentials1, Worker1}, {TokenCredentials2, Worker2}]),
    ?assertMatch(?ERROR_TOKEN_REVOKED, verify_credentials(Worker1, TokenCredentials3)),

    mock_token_logic_is_revoked(Workers, []),
    clear_auth_caches(Config).


auth_cache_temporary_token_events_test(Config) ->
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    clear_auth_caches(Config),

    AccessToken1 = initializer:create_access_token(?USER_ID_1, [], temporary),
    AccessToken2 = initializer:create_access_token(?USER_ID_2, [], temporary),

    TokenCredentials1 = create_token_credentials(AccessToken1, rest),
    TokenCredentials2 = create_token_credentials(AccessToken1, oneclient),
    TokenCredentials3 = create_token_credentials(AccessToken2, graphsync),

    lists:foreach(fun({TokenCredentials, Worker, ExpUserId}) ->
        ?assertMatch({ok, ?USER(ExpUserId), undefined}, verify_credentials(Worker, TokenCredentials))
    end, [
        {TokenCredentials1, Worker1, ?USER_ID_1},
        {TokenCredentials2, Worker2, ?USER_ID_1},
        {TokenCredentials3, Worker1, ?USER_ID_2}
    ]),

    %% TOKEN EVENTS SEND ON ANY NODE SHOULD AFFECT ALL NODES AUTH CACHE

    % When temporary tokens of ?USER_ID_1 are revoked
    simulate_gs_temporary_tokens_revocation(Worker1, ?USER_ID_1),

    % Then all TokenCredentials based on temporary tokens should be revoked
    lists:foreach(fun({TokenCredentials, Worker}) ->
        ?assertMatch(?ERROR_TOKEN_REVOKED, verify_credentials(Worker, TokenCredentials))
    end, [{TokenCredentials1, Worker1}, {TokenCredentials2, Worker2}]),
    ?assertMatch({ok, ?USER(?USER_ID_2), undefined}, verify_credentials(Worker1, TokenCredentials3)),

    % Deleting temporary tokens of ?USER_ID_1 should result in ?ERROR_TOKEN_INVALID
    simulate_gs_temporary_tokens_deletion(Worker1, ?USER_ID_1),

    lists:foreach(fun({TokenCredentials, Worker}) ->
        ?assertMatch(?ERROR_TOKEN_INVALID, verify_credentials(Worker, TokenCredentials))
    end, [{TokenCredentials1, Worker1}, {TokenCredentials2, Worker2}]),
    ?assertMatch({ok, ?USER(?USER_ID_2), undefined}, verify_credentials(Worker1, TokenCredentials3)),

    % Revoking already deleted token should not change error
    simulate_gs_temporary_tokens_revocation(Worker1, ?USER_ID_1),

    lists:foreach(fun({TokenCredentials, Worker}) ->
        ?assertMatch(?ERROR_TOKEN_INVALID, verify_credentials(Worker, TokenCredentials))
    end, [{TokenCredentials1, Worker1}, {TokenCredentials2, Worker2}]),
    ?assertMatch({ok, ?USER(?USER_ID_2), undefined}, verify_credentials(Worker1, TokenCredentials3)),

    clear_auth_caches(Config).


auth_cache_oz_conn_status_test(Config) ->
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),

    clear_auth_caches(Config),

    AccessToken1 = initializer:create_access_token(?USER_ID_1, [], temporary),
    AccessToken2 = initializer:create_access_token(?USER_ID_2, [], named),

    TokenCredentials1 = create_token_credentials(AccessToken1),
    TokenCredentials2 = create_token_credentials(AccessToken2),

    lists:foreach(fun({TokenCredentials, Worker, ExpUserId}) ->
        ?assertMatch({ok, ?USER(ExpUserId), undefined}, verify_credentials(Worker, TokenCredentials))
    end, [
        {TokenCredentials1, Worker1, ?USER_ID_1},
        {TokenCredentials2, Worker2, ?USER_ID_2}]
    ),

    ?assertEqual(1, get_auth_cache_size(Worker1)),
    ?assertEqual(1, get_auth_cache_size(Worker2)),

    % Connection start should cause cache purge as soon as the restart event
    % reaches auth_cache workers - retry the check several times as the
    % broadcast is asynchronous
    simulate_oz_connection_start(Worker1),
    ?assertEqual(0, get_auth_cache_size(Worker1), 10),
    ?assertEqual(0, get_auth_cache_size(Worker2), 10),

    lists:foreach(fun({TokenCredentials, Worker, ExpUserId}) ->
        ?assertMatch({ok, ?USER(ExpUserId), undefined}, verify_credentials(Worker, TokenCredentials))
    end, [
        {TokenCredentials1, Worker2, ?USER_ID_1},
        {TokenCredentials2, Worker1, ?USER_ID_2}]
    ),

    % Connection termination should schedule cache purge after 'auth_invalidation_delay'
    set_auth_cache_purge_delay(Worker1, timer:seconds(4)),
    set_auth_cache_purge_delay(Worker2, timer:seconds(6)),
    simulate_oz_connection_termination(Worker1),

    timer:sleep(timer:seconds(2)),
    ?assertEqual(1, get_auth_cache_size(Worker1)),
    ?assertEqual(1, get_auth_cache_size(Worker2)),
    timer:sleep(timer:seconds(3)),
    ?assertEqual(0, get_auth_cache_size(Worker1)),
    ?assertEqual(1, get_auth_cache_size(Worker2)),
    timer:sleep(timer:seconds(2)),
    ?assertEqual(0, get_auth_cache_size(Worker2)),

    clear_auth_caches(Config).


token_authentication(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    Nonce = <<"token_authentication">>,
    AccessToken = initializer:create_access_token(?USER_ID_1),
    TokenCredentials = auth_manager:build_token_credentials(
        AccessToken, undefined,
        initializer:local_ip_v4(), oneclient, allow_data_access_caveats
    ),

    % when
    {ok, {Sock, SessId}} = fuse_test_utils:connect_via_token(Worker1, [], Nonce, AccessToken),

    % then
    {ok, Doc} = ?assertMatch(
        {ok, #document{value = #session{identity = ?SUB(user, ?USER_ID_1)}}},
        rpc:call(Worker1, session, get, [SessId])
    ),
    ?assertMatch(TokenCredentials, session:get_credentials(Doc)),
    ?assertMatch(
        {ok, ?USER(?USER_ID_1), undefined},
        rpc:call(Worker1, auth_manager, verify_credentials, [TokenCredentials])
    ),
    ok = ssl:close(Sock).


token_expiration(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    Nonce = <<"token_expiration">>,

    % Session should be terminated after time caveat expiration
    AccessToken1 = initializer:create_access_token(?USER_ID_1, [#cv_time{
        valid_until = ?NOW() + 2
    }]),
    TokenCredentials1 = auth_manager:build_token_credentials(
        AccessToken1, undefined,
        initializer:local_ip_v4(), oneclient, allow_data_access_caveats
    ),

    {ok, {_, SessId1}} = fuse_test_utils:connect_via_token(Worker1, [], Nonce, AccessToken1),

    ?assertMatch(
        {ok, #document{value = #session{identity = ?SUB(user, ?USER_ID_1)}}},
        rpc:call(Worker1, session, get, [SessId1])
    ),
    ?assertMatch(
        {ok, ?USER(?USER_ID_1), _},
        rpc:call(Worker1, auth_manager, verify_credentials, [TokenCredentials1])
    ),

    timer:sleep(timer:seconds(4)),

    ?assertMatch(
        {error, not_found},
        rpc:call(Worker1, session, get, [SessId1])
    ),
    ?assertMatch(
        ?ERROR_UNAUTHORIZED,
        rpc:call(Worker1, auth_manager, verify_credentials, [TokenCredentials1])
    ),

    % But it is possible to update credentials and increase expiration
    AccessToken2 = initializer:create_access_token(?USER_ID_1, [#cv_time{
        valid_until = ?NOW() + 4
    }]),
    {ok, {_, SessId2}} = fuse_test_utils:connect_via_token(
        Worker1, [], Nonce, AccessToken2
    ),
    ?assertMatch(
        {ok, #document{value = #session{identity = ?SUB(user, ?USER_ID_1)}}},
        rpc:call(Worker1, session, get, [SessId2])
    ),

    timer:sleep(timer:seconds(2)),
    ?assertMatch(
        {ok, #document{value = #session{identity = ?SUB(user, ?USER_ID_1)}}},
        rpc:call(Worker1, session, get, [SessId2])
    ),

    AccessToken3 = initializer:create_access_token(?USER_ID_1, [#cv_time{
        valid_until = ?NOW() + 6
    }]),
    rpc:call(Worker1, incoming_session_watcher, update_credentials, [
        SessId2, AccessToken3, undefined
    ]),

    timer:sleep(timer:seconds(4)),
    ?assertMatch(
        {ok, #document{value = #session{identity = ?SUB(user, ?USER_ID_1)}}},
        rpc:call(Worker1, session, get, [SessId2])
    ),

    timer:sleep(timer:seconds(4)),
    ?assertMatch(
        {error, not_found},
        rpc:call(Worker1, session, get, [SessId2])
    ).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_testcase(auth_cache_expiration_with_time_warps_test = Case, Config) ->
    time_test_utils:freeze_time(Config),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(auth_cache_user_access_blocked_event_test = Case, Config) ->
    mock_file_meta(Config),
    mock_auth_cache_for_num_call_counting(Config),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(_Case, Config) ->
    ssl:start(),
    mock_provider_logic(Config),
    mock_space_logic(Config),
    mock_user_logic(Config),
    mock_token_logic(Config),
    Nodes = ?config(op_worker_nodes, Config),
    % required to trigger auth cache events that are based on run_after procedures
    utils:rpc_multicall(Nodes, gs_client_worker, enable_cache, []),
    utils:rpc_multicall(Nodes, od_user, invalidate_cache, [?USER_ID_1]),
    utils:rpc_multicall(Nodes, od_user, invalidate_cache, [?USER_ID_2]),
    Config.


end_per_testcase(auth_cache_expiration_with_time_warps_test = Case, Config) ->
    time_test_utils:unfreeze_time(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(auth_cache_size_test = Case, Config) ->
    % Ensure auth cache size limit will be restored after test (it is changed as part of test)
    [Worker1, Worker2 | _] = ?config(op_worker_nodes, Config),
    set_auth_cache_size_limit(Worker1, ?DEFAULT_AUTH_CACHE_SIZE_LIMIT),
    set_auth_cache_size_limit(Worker2, ?DEFAULT_AUTH_CACHE_SIZE_LIMIT),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(auth_cache_user_access_blocked_event_test = Case, Config) ->
    unmock_file_meta(Config),
    unmock_auth_cache(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_Case, Config) ->
    unmock_provider_logic(Config),
    unmock_space_logic(Config),
    unmock_user_logic(Config),
    unmock_token_logic(Config),
    ssl:stop().


%%%===================================================================
%%% Internal functions
%%%===================================================================


mock_provider_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, provider_logic, []),
    test_utils:mock_expect(Workers, provider_logic, get_spaces, fun() ->
        {ok, []}
    end),
    test_utils:mock_expect(Workers, provider_logic, has_eff_user, fun(UserId) ->
        lists:member(UserId, [?USER_ID_1, ?USER_ID_2])
    end).


unmock_provider_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, provider_logic).


mock_space_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, space_logic, []),
    test_utils:mock_expect(Workers, space_logic, get,
        fun(_, _) ->
            {ok, #document{value = #od_space{}}}
        end).


unmock_space_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, space_logic).


mock_user_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    Users = #{
        ?USER_ID_1 => {ok, #document{key = ?USER_ID_1, value = #od_user{}}},
        ?USER_ID_2 => {ok, #document{key = ?USER_ID_2, value = #od_user{}}}
    },

    test_utils:mock_new(Workers, user_logic, []),
    test_utils:mock_expect(Workers, user_logic, get, fun
        (?ROOT_SESS_ID, UserId) ->
            maps:get(UserId, Users, {error, not_found});
        (UserSessId, UserId) when is_binary(UserSessId) ->
            try session:get_user_id(UserSessId) of
                {ok, UserId} ->
                    maps:get(UserId, Users, ?ERROR_UNAUTHORIZED);
                _ ->
                    ?ERROR_UNAUTHORIZED
            catch
                _:_ -> ?ERROR_UNAUTHORIZED
            end;
        (TokenCredentials, UserId) ->
            case auth_manager:verify_credentials(TokenCredentials) of
                {ok, ?ROOT, _} ->
                    maps:get(UserId, Users, {error, not_found});
                {ok, ?USER(UserId), _} ->
                    maps:get(UserId, Users, ?ERROR_UNAUTHORIZED);
                _ ->
                    ?ERROR_UNAUTHORIZED
            end
    end).


unmock_user_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, user_logic).


mock_token_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, token_logic, []),
    test_utils:mock_expect(Workers, token_logic, verify_access_token, fun
        (AccessToken, _, _, _, _) ->
            case tokens:deserialize(AccessToken) of
                {ok, #token{subject = ?SUB(user, UserId)} = Token} ->
                    % this is set by access_block_changed/3
                    case od_user:get_from_cache(UserId) of
                        {ok, #document{value = #od_user{blocked = {true, _}}}} ->
                            ?ERROR_USER_BLOCKED;
                        _ ->
                            Caveats = tokens:get_caveats(Token),
                            case caveats:infer_ttl(Caveats) of
                                undefined ->
                                    {ok, ?SUB(user, UserId), undefined};
                                TokenTTL when TokenTTL > 0 ->
                                    {ok, ?SUB(user, UserId), TokenTTL};
                                _ ->
                                    ?ERROR_UNAUTHORIZED
                            end
                    end;
                {error, _} = Error ->
                    Error
            end
    end),
    test_utils:mock_expect(Workers, token_logic, is_token_revoked, fun(_TokenId) ->
        {ok, false}
    end),
    test_utils:mock_expect(Workers, token_logic, get_temporary_tokens_generation, fun(_UserId) ->
        {ok, 1}
    end).


unmock_token_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, token_logic).


% mocks the irrelevant logic called in od_user posthook
% (it is triggered by simulating od_user record changes)
mock_file_meta(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, user_root_dir),
    test_utils:mock_expect(Workers, user_root_dir, report_new_spaces_appeared, fun(_, _) ->
        ok
    end).


unmock_file_meta(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, file_meta).


mock_auth_cache_for_num_call_counting(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, auth_cache, [passthrough]).


unmock_auth_cache(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, auth_cache).


verify_credentials(Worker, TokenCredentials) ->
    rpc:call(Worker, auth_manager, verify_credentials, [TokenCredentials]).


clear_auth_cache(Worker) ->
    rpc:call(Worker, ets, delete_all_objects, [auth_cache]).


get_auth_cache_size(Worker) ->
    rpc:call(Worker, ets, info, [auth_cache, size]).


clear_auth_caches(Config) ->
    lists:foreach(fun(Node) ->
        clear_auth_cache(Node),
        ?assertEqual(0, get_auth_cache_size(Node))
    end, ?config(op_worker_nodes, Config)).


-spec create_token_credentials(auth_manager:access_token()) -> auth_manager:token_credentials().
create_token_credentials(AccessToken) ->
    Interface = case rand:uniform(4) of
        1 -> undefined;
        2 -> rest;
        3 -> graphsync;
        4 -> oneclient
    end,
    create_token_credentials(AccessToken, Interface).


-spec create_token_credentials(auth_manager:access_token(), undefined | cv_interface:interface()) ->
    auth_manager:token_credentials().
create_token_credentials(AccessToken, Interface) ->
    DataAccessCaveatsPolicy = case rand:uniform(2) of
        1 -> allow_data_access_caveats;
        2 -> disallow_data_access_caveats
    end,
    create_token_credentials(AccessToken, Interface, DataAccessCaveatsPolicy).


-spec create_token_credentials(auth_manager:access_token(), undefined | cv_interface:interface(),
    data_access_caveats:policy()) -> auth_manager:token_credentials().
create_token_credentials(AccessToken, Interface, DataAccessCaveatsPolicy) ->
    create_token_credentials(AccessToken, Interface, DataAccessCaveatsPolicy, undefined).


-spec create_token_credentials(auth_manager:access_token(), undefined | cv_interface:interface(),
    data_access_caveats:policy(), auth_manager:consumer_token()) -> auth_manager:token_credentials().
create_token_credentials(AccessToken, Interface, DataAccessCaveatsPolicy, ConsumerToken) ->
    auth_manager:build_token_credentials(
        AccessToken, ConsumerToken, initializer:local_ip_v4(),
        Interface, DataAccessCaveatsPolicy
    ).


-spec mock_token_logic_is_revoked([node()], [tokens:serialized()]) -> ok.
mock_token_logic_is_revoked(Nodes, RevokedTokens) ->
    RevokedTokenIds = lists:map(fun get_token_id/1, RevokedTokens),
    test_utils:mock_expect(Nodes, token_logic, is_token_revoked, fun(TokenId) ->
        {ok, lists:member(TokenId, RevokedTokenIds)}
    end).


-spec simulate_user_update_with_blocked_value([node()], od_user:id(), boolean(), gs_protocol:revision()) -> ok.
simulate_user_update_with_blocked_value(Nodes, UserId, Blocked, Revision) ->
    GRI = #gri{type = od_user, id = UserId, aspect = instance, scope = private},
    AsyncPid = rpc:call(lists_utils:random_element(Nodes), gs_client_worker, process_push_message, [
        #gs_push_graph{gri = GRI, change_type = updated, data = #{
            <<"gri">> => gri:serialize(GRI),
            <<"revision">> => Revision,
            <<"fullName">> => <<"whatever">>,
            <<"username">> => <<"whatever">>,
            <<"emails">> => [<<"whatever@example.com">>],
            <<"linkedAccounts">> => [],

            <<"blocked">> => Blocked,
            <<"spaceAliases">> => #{},
            <<"effectiveGroups">> => [],
            <<"effectiveSpaces">> => [],
            <<"effectiveHandleServices">> => [],
            <<"effectiveHandles">> => [],
            <<"effectiveAtmInventories">> => []
        }}
    ]),
    % the push message processing is async - wait for the process to finish
    Ref = erlang:monitor(process, AsyncPid),
    receive
        {'DOWN', Ref, process, _, _} ->
            ok
    end.


-spec simulate_gs_token_status_update(node(), tokens:serialized(), boolean()) -> ok.
simulate_gs_token_status_update(Node, Serialized, Revoked) ->
    TokenId = get_token_id(Serialized),
    logic_tests_common:simulate_push(Node, #gs_push_graph{
        gri = #gri{type = od_token, id = TokenId, aspect = instance, scope = shared},
        data = #{
            <<"revision">> => erlang:unique_integer([monotonic, positive]),
            <<"revoked">> => Revoked
        },
        change_type = updated
    }).


-spec simulate_gs_token_deletion(node(), tokens:serialized()) -> ok.
simulate_gs_token_deletion(Node, Serialized) ->
    TokenId = get_token_id(Serialized),
    logic_tests_common:simulate_push(Node, #gs_push_graph{
        gri = #gri{type = od_token, id = TokenId, aspect = instance, scope = shared},
        change_type = deleted
    }).


-spec simulate_gs_temporary_tokens_revocation(node(), binary()) -> ok.
simulate_gs_temporary_tokens_revocation(Node, UserId) ->
    logic_tests_common:simulate_push(Node, #gs_push_graph{
        gri = #gri{type = temporary_token_secret, id = UserId, aspect = user, scope = shared},
        data = #{
            <<"revision">> => erlang:unique_integer([monotonic, positive]),
            <<"generation">> => erlang:unique_integer([monotonic, positive])
        },
        change_type = updated
    }).


-spec simulate_gs_temporary_tokens_deletion(node(), binary()) -> ok.
simulate_gs_temporary_tokens_deletion(Node, UserId) ->
    logic_tests_common:simulate_push(Node, #gs_push_graph{
        gri = #gri{type = temporary_token_secret, id = UserId, aspect = user, scope = shared},
        change_type = deleted
    }).


-spec set_auth_cache_size_limit(node(), SizeLimit :: non_neg_integer()) -> ok.
set_auth_cache_size_limit(Node, SizeLimit) ->
    ?assertMatch(ok, rpc:call(Node, application, set_env, [
        ?APP_NAME, auth_cache_size_limit, SizeLimit
    ])).


-spec set_auth_cache_purge_delay(node(), Delay :: time:millis()) -> ok.
set_auth_cache_purge_delay(Node, Delay) ->
    ?assertMatch(ok, rpc:call(Node, application, set_env, [
        ?APP_NAME, auth_cache_purge_delay, Delay
    ])).


-spec set_auth_cache_default_ttl(node(), TTL :: time:seconds()) -> ok.
set_auth_cache_default_ttl(Node, TTL) ->
    ?assertMatch(ok, rpc:call(Node, application, set_env, [
        ?APP_NAME, auth_cache_item_default_ttl, TTL
    ])).


-spec simulate_oz_connection_start(node()) -> ok.
simulate_oz_connection_start(Node) ->
    rpc:call(Node, auth_cache, report_oz_connection_start, []),
    ok.


-spec simulate_oz_connection_termination(node()) -> ok.
simulate_oz_connection_termination(Node) ->
    rpc:call(Node, auth_cache, report_oz_connection_termination, []),
    ok.


% returns the count of all report_user_access_block_changed events generated on all nodes
-spec total_block_change_events([node()]) -> non_neg_integer().
total_block_change_events(Nodes) ->
    lists:sum(lists:map(fun(Node) ->
        rpc:call(Node, meck, num_calls, [auth_cache, report_user_access_block_changed, '_'])
    end, Nodes)).


-spec get_token_id(tokens:serialized()) -> tokens:id().
get_token_id(Serialized) ->
    {ok, #token{id = Id}} = tokens:deserialize(Serialized),
    Id.