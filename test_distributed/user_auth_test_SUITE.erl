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
-include("modules/datastore/datastore_models.hrl").
-include("proto/common/handshake_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/onedata.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([
    all/0,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    auth_cache_test/1,
    token_authentication/1,
    token_expiration/1
]).

all() -> ?ALL([
    auth_cache_test,
    token_authentication,
    token_expiration
]).


-define(USER_ID, <<"test_id">>).
-define(USER_FULL_NAME, <<"test_name">>).


%%%===================================================================
%%% Test functions
%%%===================================================================


auth_cache_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    clear_auth_cache(Worker1),
    ?assertEqual(0, get_auth_cache_size(Worker1)),

    AccessToken = initializer:create_access_token(?USER_ID),

    TokenAuth1 = auth_manager:build_token_auth(
        AccessToken, undefined,
        initializer:local_ip_v4(), undefined, disallow_data_access_caveats
    ),
    TokenAuth2 = auth_manager:build_token_auth(
        AccessToken, undefined,
        initializer:local_ip_v4(), graphsync, allow_data_access_caveats
    ),
    TokenAuth3 = auth_manager:build_token_auth(
        AccessToken, undefined,
        initializer:local_ip_v4(), rest, disallow_data_access_caveats
    ),

    lists:foreach(fun({TokenAuth, ExpCacheSize}) ->
        ?assertMatch(
            {ok, ?USER(?USER_ID), undefined},
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


token_authentication(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    Nonce = <<"token_authentication">>,
    AccessToken = initializer:create_access_token(?USER_ID),
    TokenAuth = auth_manager:build_token_auth(
        AccessToken, undefined,
        initializer:local_ip_v4(), oneclient, allow_data_access_caveats
    ),

    % when
    {ok, {Sock, SessId}} = fuse_test_utils:connect_via_token(Worker1, [], Nonce, AccessToken),

    % then
    {ok, Doc} = ?assertMatch(
        {ok, #document{value = #session{identity = ?SUB(user, ?USER_ID)}}},
        rpc:call(Worker1, session, get, [SessId])
    ),
    ?assertMatch(
        TokenAuth,
        session:get_auth(Doc)
    ),
    ?assertMatch(
        {ok, ?USER(?USER_ID), undefined},
        rpc:call(Worker1, auth_manager, verify_auth, [TokenAuth])
    ),
    ok = ssl:close(Sock).


token_expiration(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    Nonce = <<"token_expiration">>,

    % Session should be terminated after time caveat expiration
    AccessToken1 = initializer:create_access_token(?USER_ID, [#cv_time{
        valid_until = time_utils:system_time_seconds() + 2
    }]),
    TokenAuth1 = auth_manager:build_token_auth(
        AccessToken1, undefined,
        initializer:local_ip_v4(), oneclient, allow_data_access_caveats
    ),

    {ok, {_, SessId1}} = fuse_test_utils:connect_via_token(Worker1, [], Nonce, AccessToken1),

    ?assertMatch(
        {ok, #document{value = #session{identity = ?SUB(user, ?USER_ID)}}},
        rpc:call(Worker1, session, get, [SessId1])
    ),
    ?assertMatch(
        {ok, ?USER(?USER_ID), _},
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
    AccessToken2 = initializer:create_access_token(?USER_ID, [#cv_time{
        valid_until = time_utils:system_time_seconds() + 4
    }]),
    {ok, {_, SessId2}} = fuse_test_utils:connect_via_token(
        Worker1, [], Nonce, AccessToken2
    ),
    ?assertMatch(
        {ok, #document{value = #session{identity = ?SUB(user, ?USER_ID)}}},
        rpc:call(Worker1, session, get, [SessId2])
    ),

    timer:sleep(timer:seconds(2)),
    ?assertMatch(
        {ok, #document{value = #session{identity = ?SUB(user, ?USER_ID)}}},
        rpc:call(Worker1, session, get, [SessId2])
    ),

    AccessToken3 = initializer:create_access_token(?USER_ID, [#cv_time{
        valid_until = time_utils:system_time_seconds() + 6
    }]),
    rpc:call(Worker1, incoming_session_watcher, request_credentials_update, [
        SessId2, AccessToken3, undefined
    ]),

    timer:sleep(timer:seconds(4)),
    ?assertMatch(
        {ok, #document{value = #session{identity = ?SUB(user, ?USER_ID)}}},
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
            UserId =:= ?USER_ID
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
    UserDoc = {ok, #document{key = ?USER_ID, value = #od_user{}}},

    test_utils:mock_new(Workers, user_logic, []),
    test_utils:mock_expect(Workers, user_logic, get, fun
        (?ROOT_SESS_ID, ?USER_ID) ->
            UserDoc;
        (?ROOT_AUTH, ?USER_ID) ->
            UserDoc;
        (UserSessId, ?USER_ID) when is_binary(UserSessId) ->
            try session:get_user_id(UserSessId) of
                {ok, ?USER_ID} -> UserDoc;
                _ -> ?ERROR_UNAUTHORIZED
            catch
                _:_ -> ?ERROR_UNAUTHORIZED
            end;
        (TokenAuth, ?USER_ID) ->
            case tokens:deserialize(auth_manager:get_access_token(TokenAuth)) of
                {ok, #token{subject = ?SUB(user, ?USER_ID)}} ->
                    {ok, #document{key = ?USER_ID, value = #od_user{}}};
                {error, _} = Error ->
                    Error
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
                {ok, #token{subject = ?SUB(user, ?USER_ID)} = Token} ->
                    Caveats = tokens:get_caveats(Token),
                    case infer_ttl(Caveats) of
                        undefined ->
                            {ok, ?SUB(user, ?USER_ID), undefined};
                        TokenTTL when TokenTTL > 0 ->
                            {ok, ?SUB(user, ?USER_ID), TokenTTL};
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
