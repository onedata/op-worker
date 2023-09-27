%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% REST and CDMI tests
%%% @end
%%%-------------------------------------------------------------------
-module(rest_test_SUITE).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("proto/common/handshake_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

-export([
    token_auth/1,
    internal_error_when_handler_crashes/1,
    custom_error_when_handler_throws_error/1
]).


all() -> ?ALL([
    token_auth,
    internal_error_when_handler_crashes,
    custom_error_when_handler_throws_error
]).

-define(USER_ID, <<"test_id">>).
-define(USER_FULL_NAME, <<"test_name">>).

-define(SPACE_ID, <<"space0">>).
-define(SPACE_NAME, <<"Space 0">>).

-define(PROVIDER_ID, <<"provider1">>).

%%%===================================================================
%%% Test functions
%%%===================================================================

token_auth(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    DummyUserId = <<"dummyUserId">>,
    ShabbyUserId = <<"shabbyUserId">>,
    Endpoint = rest_endpoint(Worker) ++ "spaces",

    SerializedAccessToken = initializer:create_access_token(?USER_ID),
    SerializedAccessTokenWithConsumerCaveats = tokens:confine(SerializedAccessToken, [
        #cv_consumer{whitelist = [?SUB(user, DummyUserId)]}
    ]),
    SerializedDummyUserIdentityToken = initializer:create_identity_token(DummyUserId),
    SerializedShabbyUserIdentityToken = initializer:create_identity_token(ShabbyUserId),

    % when
    ?assertMatch(
        {ok, 401, _, _},
        do_request(Config, get, Endpoint, #{?HDR_X_AUTH_TOKEN => <<"invalid">>})
    ),
    ?assertMatch(
        {ok, 200, _, _},
        do_request(Config, get, Endpoint, #{?HDR_X_AUTH_TOKEN => SerializedAccessToken})
    ),
    ?assertMatch(
        {ok, 401, _, _},
        do_request(Config, get, Endpoint, #{?HDR_X_AUTH_TOKEN => SerializedAccessTokenWithConsumerCaveats})
    ),
    ?assertMatch(
        {ok, 401, _, _},
        do_request(Config, get, Endpoint, #{
            ?HDR_X_AUTH_TOKEN => SerializedAccessTokenWithConsumerCaveats,
            ?HDR_X_ONEDATA_CONSUMER_TOKEN => SerializedShabbyUserIdentityToken
        })
    ),
    ?assertMatch(
        {ok, 200, _, _},
        do_request(Config, get, Endpoint, #{
            ?HDR_X_AUTH_TOKEN => SerializedAccessTokenWithConsumerCaveats,
            ?HDR_X_ONEDATA_CONSUMER_TOKEN => SerializedDummyUserIdentityToken
        })
    ),
    ?assertMatch(
        {ok, 200, _, _},
        do_request(Config, get, Endpoint, #{?HDR_AUTHORIZATION => <<"Bearer ", SerializedAccessToken/binary>>})
    ),
    %% @todo VFS-5554 Deprecated, included for backward compatibility
    ?assertMatch(
        {ok, 200, _, _},
        do_request(Config, get, Endpoint, #{?HDR_MACAROON => SerializedAccessToken})
    ).

internal_error_when_handler_crashes(Config) ->
    % given
    Workers = [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),
    test_utils:mock_expect(Workers, space_oz_middleware_handler, get, fun test_crash/2),

    % when
    {ok, Status, _, _} = do_request(Config, get, Endpoint ++ "spaces"),

    % then
    ?assertEqual(500, Status).

custom_error_when_handler_throws_error(Config) ->
    % given
    Workers = [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),
    test_utils:mock_expect(Workers, space_oz_middleware_handler, get, fun(_, _) -> throw(?ERROR_BAD_VALUE_JSON(<<"dummy">>)) end),

    % when
    {ok, Status, _, Body} = do_request(Config, get, Endpoint ++ "spaces"),

    % then
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_BAD_VALUE_JSON(<<"dummy">>)),
    ?assertMatch(ExpRestError, {Status, json_utils:decode(Body)}).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        [Worker | _] = ?config(op_worker_nodes, NewConfig),
        initializer:clear_subscriptions(Worker),
        initializer:setup_storage(NewConfig)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


init_per_testcase(Case, Config) when
    Case =:= internal_error_when_handler_crashes;
    Case =:= custom_error_when_handler_throws_error
->
    Workers = ?config(op_worker_nodes, Config),
    ssl:start(),
    application:ensure_all_started(hackney),
    test_utils:mock_new(Workers, space_oz_middleware_handler),
    test_utils:mock_expect(Workers, space_oz_middleware_handler, authorize, fun(_, _) -> true end),
    mock_provider_id(Config),
    Config;
init_per_testcase(_Case, Config) ->
    ssl:start(),
    application:ensure_all_started(hackney),
    mock_provider_id(Config),
    mock_provider_logic(Config),
    mock_space_logic(Config),
    mock_user_logic(Config),
    Config.

end_per_testcase(Case, Config) when
    Case =:= internal_error_when_handler_crashes;
    Case =:= custom_error_when_handler_throws_error
->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, space_oz_middleware_handler),
    unmock_provider_id(Config),
    application:stop(hackney),
    ssl:stop();
end_per_testcase(_Case, Config) ->
    unmock_provider_id(Config),
    unmock_provider_logic(Config),
    unmock_space_logic(Config),
    unmock_user_logic(Config),
    application:stop(hackney),
    ssl:stop().


end_per_suite(_) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

% Performs a single request using http_client
do_request(Config, Method, URL) ->
    do_request(Config, Method, URL, #{}).
do_request(Config, Method, URL, Headers) ->
    do_request(Config, Method, URL, Headers, <<>>).
do_request(Config, Method, URL, Headers, Body) ->
    do_request(Config, Method, URL, Headers, Body, []).
do_request(Config, Method, URL, Headers, Body, Opts) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    CaCerts = rpc:call(Node, https_listener, get_cert_chain_ders, []),
    Opts2 = [{ssl_options, [{cacerts, CaCerts}]} | Opts],
    http_client:request(Method, URL, Headers, Body, Opts2).


rest_endpoint(Node) ->
    Port = case get(port) of
        undefined ->
            {ok, P} = test_utils:get_env(Node, ?APP_NAME, https_server_port),
            PStr = case P of
                443 -> "";
                _ -> ":" ++ integer_to_list(P)
            end,
            put(port, PStr),
            PStr;
        P -> P
    end,
    {ok, Domain} = test_utils:get_env(Node, ?APP_NAME, test_web_cert_domain),
    string:join(["https://", str_utils:to_list(Domain), Port, "/api/v3/oneprovider/"], "").

mock_provider_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, provider_logic, [passthrough]),
    test_utils:mock_expect(Workers, provider_logic, get_name,
        fun(ProviderId) -> {ok, ProviderId} end),
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
        fun(_, ?SPACE_ID) ->
            {ok, #document{value = #od_space{
                name = ?SPACE_NAME,
                eff_users = #{?USER_ID => []},
                providers = #{?PROVIDER_ID => 1000000000}
            }}}
        end),
    test_utils:mock_expect(Workers, space_logic, get_protected_data,
        fun(_, ?SPACE_ID) ->
            {ok, #document{value = #od_space{
                name = ?SPACE_NAME,
                providers = #{?PROVIDER_ID => 1000000000}
            }}}
        end),
    test_utils:mock_expect(Workers, space_logic, is_owner, fun(_, _) -> false end),
    test_utils:mock_expect(Workers, space_logic, get_name,
        fun(_, ?SPACE_ID) ->
            {ok, ?SPACE_NAME}
        end),
    test_utils:mock_expect(Workers, space_logic, get_local_storages,
        fun (_SpaceId) ->
            {ok, lists:foldl(fun(Worker, Acc) ->
                case ?config({storage_id, ?GET_DOMAIN(Worker)}, Config) of
                    undefined -> Acc;
                    StorageId -> [StorageId | Acc]
                end
            end, [], Workers)}
        end),
    test_utils:mock_expect(Workers, space_logic, has_eff_privilege, fun(_, _, ?SPACE_VIEW) -> true end).

unmock_space_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, space_logic).

mock_user_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, user_logic, []),

    UserDoc = {ok, #document{key = ?USER_ID, value = #od_user{
        full_name = ?USER_FULL_NAME,
        eff_spaces = [?SPACE_ID]
    }}},

    GetUserFun = fun
        (?ROOT_SESS_ID, ?USER_ID) ->
            UserDoc;
        (?ROOT_CREDENTIALS, ?USER_ID) ->
            UserDoc;
        (UserSessId, ?USER_ID) when is_binary(UserSessId) ->
            try session:get_user_id(UserSessId) of
                {ok, ?USER_ID} -> UserDoc;
                _ -> ?ERROR_UNAUTHORIZED
            catch
                _:_ -> ?ERROR_UNAUTHORIZED
            end;
        (TokenCredentials, ?USER_ID) ->
            case tokens:deserialize(auth_manager:get_access_token(TokenCredentials)) of
                {ok, #token{subject = ?SUB(user, ?USER_ID)}} ->
                    UserDoc;
                {error, _} = Error ->
                    Error
            end;
        (_, _) ->
            {error, not_found}
    end,
    initializer:mock_auth_manager(Config),

    test_utils:mock_expect(Workers, user_logic, get, GetUserFun),
    test_utils:mock_expect(Workers, user_logic, get_eff_spaces, fun(_, _) ->
        {ok, [?SPACE_ID]}
    end),
    [rpc:call(W, file_meta, reconcile_spaces_for_user, [?USER_ID, []]) || W <- Workers].


unmock_user_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:unmock_auth_manager(Config),
    test_utils:mock_validate_and_unload(Workers, user_logic).


mock_provider_id(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:mock_provider_id(
        Workers, ?PROVIDER_ID, <<"access-token">>, <<"identity-token">>
    ).


unmock_provider_id(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:unmock_provider_ids(Workers).


-spec test_crash(term(), term()) -> no_return().
test_crash(_, _) ->
    throw(test_crash).
