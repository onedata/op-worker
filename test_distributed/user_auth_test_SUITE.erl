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
    auth_cache_size_test/1,
    auth_cache_named_token_events_test/1,
    auth_cache_temporary_token_events_test/1,
    auth_cache_oz_conn_status_test/1,
    token_authentication/1,
    token_expiration/1
]).

all() -> ?ALL([
    auth_cache_expiration_test,
    auth_cache_size_test,
    auth_cache_named_token_events_test,
    auth_cache_temporary_token_events_test,
    auth_cache_oz_conn_status_test,
    token_authentication,
    token_expiration
]).


-define(USER_ID_1, <<"test_id_1">>).
-define(USER_ID_2, <<"test_id_2">>).
-define(USER_FULL_NAME, <<"test_name">>).

-define(NOW(), time_utils:timestamp_seconds()).

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
    rpc:call(Worker1, application, set_env, [?APP_NAME, auth_cache_size_limit, 2]),
    timer:sleep(timer:seconds(5)),
    ?assertEqual(0, get_auth_cache_size(Worker1)),
    ?assertEqual(2, get_auth_cache_size(Worker2)),

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
    mock_token_logic_is_revoked(Workers, [?USER_ID_1]),
    simulate_gs_token_status_update(Worker2, ?USER_ID_1, true),

    % Then only TokenCredentials based on that token should be revoked
    lists:foreach(fun({TokenCredentials, Worker}) ->
        ?assertMatch(?ERROR_TOKEN_REVOKED, verify_credentials(Worker, TokenCredentials))
    end, [{TokenCredentials1, Worker1}, {TokenCredentials2, Worker2}]),
    ?assertMatch({ok, ?USER(?USER_ID_2), undefined}, verify_credentials(Worker1, TokenCredentials3)),

    % Revoked tokens can be re-revoked and used again
    mock_token_logic_is_revoked(Workers, [?USER_ID_2]),
    simulate_gs_token_status_update(Worker1, ?USER_ID_1, false),
    simulate_gs_token_status_update(Worker2, ?USER_ID_2, true),

    lists:foreach(fun({TokenCredentials, Worker}) ->
        ?assertMatch({ok, ?USER(?USER_ID_1), _}, verify_credentials(Worker, TokenCredentials))
    end, [{TokenCredentials1, Worker1}, {TokenCredentials2, Worker2}]),
    ?assertMatch(?ERROR_TOKEN_REVOKED, verify_credentials(Worker1, TokenCredentials3)),

    % Deleting token should result in ?ERROR_TOKEN_INVALID
    simulate_gs_token_deletion(Worker1, ?USER_ID_1),

    lists:foreach(fun({TokenCredentials, Worker}) ->
        ?assertMatch(?ERROR_TOKEN_INVALID, verify_credentials(Worker, TokenCredentials))
    end, [{TokenCredentials1, Worker1}, {TokenCredentials2, Worker2}]),
    ?assertMatch(?ERROR_TOKEN_REVOKED, verify_credentials(Worker1, TokenCredentials3)),

    % Revoking already deleted token should not change error
    mock_token_logic_is_revoked(Workers, [?USER_ID_2]),
    simulate_gs_token_status_update(Worker1, ?USER_ID_1, true),

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

    % Then all TokenCredentialss based on temporary tokens should be revoked
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


init_per_testcase(_Case, Config) ->
    ssl:start(),
    mock_provider_logic(Config),
    mock_space_logic(Config),
    mock_user_logic(Config),
    mock_token_logic(Config),
    Config.


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
    end),
    test_utils:mock_expect(Workers ++ [node()], provider_logic, zone_time_seconds,
        fun() ->
            time_utils:timestamp_seconds()
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
            end;
        (_, _) ->
            {error, not_found}
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
                    Caveats = tokens:get_caveats(Token),
                    case infer_ttl(Caveats) of
                        undefined ->
                            {ok, ?SUB(user, UserId), undefined};
                        TokenTTL when TokenTTL > 0 ->
                            {ok, ?SUB(user, UserId), TokenTTL};
                        _ ->
                            ?ERROR_UNAUTHORIZED
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


verify_credentials(Worker, TokenCredentials) ->
    rpc:call(Worker, auth_manager, verify_credentials, [TokenCredentials]).


clear_auth_cache(Worker) ->
    rpc:call(Worker, ets, delete_all_objects, [auth_cache]).


-spec infer_ttl([caveats:caveat()]) -> undefined | time_utils:seconds().
infer_ttl(Caveats) ->
    ValidUntil = lists:foldl(fun
        (#cv_time{valid_until = ValidUntil}, undefined) -> ValidUntil;
        (#cv_time{valid_until = ValidUntil}, Acc) -> min(ValidUntil, Acc)
    end, undefined, caveats:filter([cv_time], Caveats)),
    case ValidUntil of
        undefined -> undefined;
        _ -> ValidUntil - ?NOW()
    end.


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


mock_token_logic_is_revoked(Nodes, RevokedTokens) ->
    test_utils:mock_expect(Nodes, token_logic, is_token_revoked, fun(TokenId) ->
        {ok, lists:member(TokenId, RevokedTokens)}
    end).


-spec simulate_gs_token_status_update(node(), binary(), boolean()) -> ok.
simulate_gs_token_status_update(Node, TokenId, Revoked) ->
    logic_tests_common:simulate_push(Node, #gs_push_graph{
        gri = #gri{type = od_token, id = TokenId, aspect = instance, scope = shared},
        data = #{
            <<"revision">> => erlang:unique_integer([monotonic, positive]),
            <<"revoked">> => Revoked
        },
        change_type = updated
    }).


-spec simulate_gs_token_deletion(node(), binary()) -> ok.
simulate_gs_token_deletion(Node, TokenId) ->
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


-spec set_auth_cache_purge_delay(node(), Delay :: time_utils:millis()) -> ok.
set_auth_cache_purge_delay(Node, Delay) ->
    ?assertMatch(ok, rpc:call(Node, application, set_env, [
        ?APP_NAME, auth_cache_purge_delay, Delay
    ])).


-spec set_auth_cache_default_ttl(node(), TTL :: time_utils:seconds()) -> ok.
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
