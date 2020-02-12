%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
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
    auth_cache_size_test/1,
    auth_cache_named_token_events_test/1,
    auth_cache_temporary_token_events_test/1,
    auth_cache_oz_conn_status_test/1,
    token_authentication/1,
    token_expiration/1
]).

all() -> ?ALL([
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

-define(ATTEMPTS, 20).

%%%===================================================================
%%% Test functions
%%%===================================================================


auth_cache_size_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    clear_auth_cache(Worker1),
    ?assertEqual(0, get_auth_cache_size(Worker1)),

    AccessToken = initializer:create_access_token(?USER_ID_1),

    TokenAuth1 = create_token_auth(AccessToken),
    TokenAuth2 = create_token_auth(AccessToken),
    TokenAuth3 = create_token_auth(AccessToken),

    lists:foreach(fun({TokenAuth, ExpCacheSize}) ->
        ?assertMatch(
            {ok, ?USER(?USER_ID_1), undefined},
            verify_auth(Worker1, TokenAuth)
        ),
        ?assertEqual(ExpCacheSize, get_auth_cache_size(Worker1))
    end, [
        {TokenAuth1, 1},
        {TokenAuth2, 2},
        {TokenAuth3, 3}
    ]),

    % Default cache size limit is big enough so that 3 entries will not be purged
    timer:sleep(timer:seconds(5)),
    ?assertEqual(3, get_auth_cache_size(Worker1)),

    % After setting auth cache size limit to 2 entries should be purged during
    % next checkup (since they exceed limit)
    rpc:call(Worker1, application, set_env, [?APP_NAME, auth_cache_size_limit, 2]),
    timer:sleep(timer:seconds(5)),
    ?assertEqual(0, get_auth_cache_size(Worker1)),

    % Filling entries up to limit should not cause cache purge
    verify_auth(Worker1, TokenAuth1),
    verify_auth(Worker1, TokenAuth2),
    ?assertEqual(2, get_auth_cache_size(Worker1)),
    timer:sleep(timer:seconds(5)),
    ?assertEqual(2, get_auth_cache_size(Worker1)),

    clear_auth_cache(Worker1).


auth_cache_named_token_events_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    clear_auth_cache(Worker1),
    ?assertEqual(0, get_auth_cache_size(Worker1)),

    AccessToken1 = initializer:create_access_token(?USER_ID_1, [], named),
    AccessToken2 = initializer:create_access_token(?USER_ID_2, [], named),

    TokenAuth1 = create_token_auth(AccessToken1),
    TokenAuth2 = create_token_auth(AccessToken1),
    TokenAuth3 = create_token_auth(AccessToken2),

    lists:foreach(fun({TokenAuth, ExpUserId}) ->
        ?assertMatch({ok, ?USER(ExpUserId), undefined}, verify_auth(Worker1, TokenAuth))
    end, [
        {TokenAuth1, ?USER_ID_1},
        {TokenAuth2, ?USER_ID_1},
        {TokenAuth3, ?USER_ID_2}
    ]),

    % When AccessToken1 is revoked
    simulate_gs_token_status_update(Worker1, ?USER_ID_1, true),

    % Then only TokenAuths based on that token should be revoked
    lists:foreach(fun(TokenAuth) ->
        ?assertMatch(?ERROR_TOKEN_REVOKED, verify_auth(Worker1, TokenAuth))
    end, [TokenAuth1, TokenAuth2]),
    ?assertMatch({ok, ?USER(?USER_ID_2), undefined}, verify_auth(Worker1, TokenAuth3)),

    % Revoked tokens can be re-revoked and used again
    simulate_gs_token_status_update(Worker1, ?USER_ID_1, false),
    simulate_gs_token_status_update(Worker1, ?USER_ID_2, true),

    lists:foreach(fun(TokenAuth) ->
        ?assertMatch({ok, ?USER(?USER_ID_1), _}, verify_auth(Worker1, TokenAuth))
    end, [TokenAuth1, TokenAuth2]),
    ?assertMatch(?ERROR_TOKEN_REVOKED, verify_auth(Worker1, TokenAuth3)),

    % Deleting token should result in ?ERROR_TOKEN_INVALID
    simulate_gs_token_deletion(Worker1, ?USER_ID_1),

    lists:foreach(fun(TokenAuth) ->
        ?assertMatch(?ERROR_TOKEN_INVALID, verify_auth(Worker1, TokenAuth))
    end, [TokenAuth1, TokenAuth2]),
    ?assertMatch(?ERROR_TOKEN_REVOKED, verify_auth(Worker1, TokenAuth3)),

    % Revoking already deleted token should not change error
    simulate_gs_token_status_update(Worker1, ?USER_ID_1, true),

    lists:foreach(fun(TokenAuth) ->
        ?assertMatch(?ERROR_TOKEN_INVALID, verify_auth(Worker1, TokenAuth))
    end, [TokenAuth1, TokenAuth2]),
    ?assertMatch(?ERROR_TOKEN_REVOKED, verify_auth(Worker1, TokenAuth3)),

    clear_auth_cache(Worker1).


auth_cache_temporary_token_events_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    clear_auth_cache(Worker1),
    ?assertEqual(0, get_auth_cache_size(Worker1)),

    AccessToken1 = initializer:create_access_token(?USER_ID_1, [], temporary),
    AccessToken2 = initializer:create_access_token(?USER_ID_2, [], temporary),

    TokenAuth1 = create_token_auth(AccessToken1),
    TokenAuth2 = create_token_auth(AccessToken1),
    TokenAuth3 = create_token_auth(AccessToken2),

    lists:foreach(fun({TokenAuth, ExpUserId}) ->
        ?assertMatch({ok, ?USER(ExpUserId), undefined}, verify_auth(Worker1, TokenAuth))
    end, [
        {TokenAuth1, ?USER_ID_1},
        {TokenAuth2, ?USER_ID_1},
        {TokenAuth3, ?USER_ID_2}
    ]),

    % When temporary tokens of ?USER_ID_1 are revoked
    simulate_gs_temporary_tokens_revocation(Worker1, ?USER_ID_1),

    % Then all TokenAuths based on temporary tokens should be revoked
    lists:foreach(fun(TokenAuth) ->
        ?assertMatch(?ERROR_TOKEN_REVOKED, verify_auth(Worker1, TokenAuth))
    end, [TokenAuth1, TokenAuth2]),
    ?assertMatch({ok, ?USER(?USER_ID_2), undefined}, verify_auth(Worker1, TokenAuth3)),

    % Deleting temporary tokens of ?USER_ID_1 should result in ?ERROR_TOKEN_INVALID
    simulate_gs_temporary_tokens_deletion(Worker1, ?USER_ID_1),

    lists:foreach(fun(TokenAuth) ->
        ?assertMatch(?ERROR_TOKEN_INVALID, verify_auth(Worker1, TokenAuth))
    end, [TokenAuth1, TokenAuth2]),
    ?assertMatch({ok, ?USER(?USER_ID_2), undefined}, verify_auth(Worker1, TokenAuth3)),

    % Revoking already deleted token should not change error
    simulate_gs_temporary_tokens_revocation(Worker1, ?USER_ID_1),

    lists:foreach(fun(TokenAuth) ->
        ?assertMatch(?ERROR_TOKEN_INVALID, verify_auth(Worker1, TokenAuth))
    end, [TokenAuth1, TokenAuth2]),
    ?assertMatch({ok, ?USER(?USER_ID_2), undefined}, verify_auth(Worker1, TokenAuth3)),

    clear_auth_cache(Worker1).


auth_cache_oz_conn_status_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    clear_auth_cache(Worker1),
    ?assertEqual(0, get_auth_cache_size(Worker1)),

    AccessToken1 = initializer:create_access_token(?USER_ID_1, [], temporary),
    AccessToken2 = initializer:create_access_token(?USER_ID_2, [], named),

    TokenAuth1 = create_token_auth(AccessToken1),
    TokenAuth2 = create_token_auth(AccessToken2),

    lists:foreach(fun({TokenAuth, ExpUserId}) ->
        ?assertMatch({ok, ?USER(ExpUserId), undefined}, verify_auth(Worker1, TokenAuth))
    end, [{TokenAuth1, ?USER_ID_1}, {TokenAuth2, ?USER_ID_2}]),

    ?assertEqual(2, get_auth_cache_size(Worker1)),

    % Connection start should cause immediate cache purge
    simulate_oz_connection_start(Worker1),
    ?assertEqual(0, get_auth_cache_size(Worker1)),

    lists:foreach(fun({TokenAuth, ExpUserId}) ->
        ?assertMatch({ok, ?USER(ExpUserId), undefined}, verify_auth(Worker1, TokenAuth))
    end, [{TokenAuth1, ?USER_ID_1}, {TokenAuth2, ?USER_ID_2}]),

    % Connection termination should schedule cache purge after 'auth_invalidation_delay'
    rpc:call(Worker1, application, set_env, [?APP_NAME, auth_invalidation_delay, timer:seconds(4)]),
    simulate_oz_connection_termination(Worker1),

    timer:sleep(timer:seconds(2)),
    ?assertEqual(2, get_auth_cache_size(Worker1)),
    timer:sleep(timer:seconds(3)),
    ?assertEqual(0, get_auth_cache_size(Worker1)),

    clear_auth_cache(Worker1).


token_authentication(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    Nonce = <<"token_authentication">>,
    AccessToken = initializer:create_access_token(?USER_ID_1),
    TokenAuth = auth_manager:build_token_auth(
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
    ?assertMatch(
        TokenAuth,
        session:get_auth(Doc)
    ),
    ?assertMatch(
        {ok, ?USER(?USER_ID_1), undefined},
        rpc:call(Worker1, auth_manager, verify_auth, [TokenAuth])
    ),
    ok = ssl:close(Sock).


token_expiration(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    Nonce = <<"token_expiration">>,

    % Session should be terminated after time caveat expiration
    AccessToken1 = initializer:create_access_token(?USER_ID_1, [#cv_time{
        valid_until = time_utils:system_time_seconds() + 2
    }]),
    TokenAuth1 = create_token_auth(AccessToken1),

    {ok, {_, SessId1}} = fuse_test_utils:connect_via_token(Worker1, [], Nonce, AccessToken1),

    ?assertMatch(
        {ok, #document{value = #session{identity = ?SUB(user, ?USER_ID_1)}}},
        rpc:call(Worker1, session, get, [SessId1])
    ),
    ?assertMatch(
        {ok, ?USER(?USER_ID_1), _},
        rpc:call(Worker1, auth_manager, verify_auth, [TokenAuth1])
    ),

    timer:sleep(timer:seconds(4)),

    ?assertMatch(
        {error, not_found},
        rpc:call(Worker1, session, get, [SessId1])
    ),
    ?assertMatch(
        ?ERROR_UNAUTHORIZED,
        rpc:call(Worker1, auth_manager, verify_auth, [TokenAuth1])
    ),

    % But it is possible to update credentials and increase expiration
    AccessToken2 = initializer:create_access_token(?USER_ID_1, [#cv_time{
        valid_until = time_utils:system_time_seconds() + 4
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
        valid_until = time_utils:system_time_seconds() + 6
    }]),
    rpc:call(Worker1, incoming_session_watcher, request_credentials_update, [
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
    test_utils:mock_expect(Workers, provider_logic, has_eff_user,
        fun(UserId) ->
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
        (TokenAuth, UserId) ->
            case auth_manager:verify_auth(TokenAuth) of
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
        (AccessToken, _, _, _) ->
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


verify_auth(Worker, TokenAuth) ->
    rpc:call(Worker, auth_manager, verify_auth, [TokenAuth]).


clear_auth_cache(Worker) ->
    rpc:call(Worker, ets, delete_all_objects, [auth_manager]).


get_auth_cache_size(Worker) ->
    rpc:call(Worker, ets, info, [auth_manager, size]).


-spec infer_ttl([caveats:caveat()]) -> undefined | time_utils:seconds().
infer_ttl(Caveats) ->
    ValidUntil = lists:foldl(fun
        (#cv_time{valid_until = ValidUntil}, undefined) -> ValidUntil;
        (#cv_time{valid_until = ValidUntil}, Acc) -> min(ValidUntil, Acc)
    end, undefined, caveats:filter([cv_time], Caveats)),
    case ValidUntil of
        undefined -> undefined;
        _ -> ValidUntil - time_utils:system_time_seconds()
    end.


-spec create_token_auth(auth_manager:access_token()) -> auth_manager:token_auth().
create_token_auth(AccessToken) ->
    Interface = case rand:uniform(4) of
        1 -> undefined;
        2 -> rest;
        3 -> graphsync;
        4 -> oneclient
    end,
    DataAccessCaveatsPolicy = case rand:uniform(2) of
        1 -> allow_data_access_caveats;
        2 -> disallow_data_access_caveats
    end,
    auth_manager:build_token_auth(
        AccessToken, undefined, initializer:local_ip_v4(),
        Interface, DataAccessCaveatsPolicy
    ).


-spec simulate_gs_token_status_update(node(), binary(), boolean()) -> ok.
simulate_gs_token_status_update(Node, TokenId, Revoked) ->
    rpc:call(Node, gs_client_worker, process_push_message, [#gs_push_graph{
        gri = #gri{type = od_token, id = TokenId, aspect = instance, scope = shared},
        data = #{
            <<"revision">> => erlang:unique_integer([monotonic, positive]),
            <<"revoked">> => Revoked
        },
        change_type = updated
    }]),
    ok.


-spec simulate_gs_token_deletion(node(), binary()) -> ok.
simulate_gs_token_deletion(Node, TokenId) ->
    rpc:call(Node, gs_client_worker, process_push_message, [#gs_push_graph{
        gri = #gri{type = od_token, id = TokenId, aspect = instance, scope = shared},
        change_type = deleted
    }]),
    ok.


-spec simulate_gs_temporary_tokens_revocation(node(), binary()) -> ok.
simulate_gs_temporary_tokens_revocation(Node, UserId) ->
    rpc:call(Node, gs_client_worker, process_push_message, [#gs_push_graph{
        gri = #gri{type = temporary_token_secret, id = UserId, aspect = user, scope = shared},
        data = #{
            <<"revision">> => erlang:unique_integer([monotonic, positive]),
            <<"generation">> => erlang:unique_integer([monotonic, positive])
        },
        change_type = updated
    }]),
    ok.


-spec simulate_gs_temporary_tokens_deletion(node(), binary()) -> ok.
simulate_gs_temporary_tokens_deletion(Node, UserId) ->
    rpc:call(Node, gs_client_worker, process_push_message, [#gs_push_graph{
        gri = #gri{type = temporary_token_secret, id = UserId, aspect = user, scope = shared},
        change_type = deleted
    }]),
    ok.


-spec simulate_oz_connection_start(node()) -> ok.
simulate_oz_connection_start(Node) ->
    rpc:call(Node, auth_cache, report_oz_connection_start, []),
    ok.


-spec simulate_oz_connection_termination(node()) -> ok.
simulate_oz_connection_termination(Node) ->
    rpc:call(Node, auth_cache, report_oz_connection_termination, []),
    ok.
