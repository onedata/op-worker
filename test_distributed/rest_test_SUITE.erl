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
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% API
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

-export([
    macaroon_auth/1,
    internal_error_when_handler_crashes/1,
    custom_code_when_handler_throws_code/1,
    custom_error_when_handler_throws_error/1
]).


all() -> ?ALL([
    macaroon_auth,
    internal_error_when_handler_crashes,
    custom_code_when_handler_throws_code,
    custom_error_when_handler_throws_error
]).

-define(MACAROON, <<"DUMMY-MACAROON">>).

-define(USER_ID, <<"test_id">>).
-define(USER_NAME, <<"test_name">>).

-define(SPACE_ID, <<"space0">>).
-define(SPACE_NAME, <<"Space 0">>).

-define(PROVIDER_ID, <<"provider1">>).

%%%===================================================================
%%% Test functions
%%%===================================================================

macaroon_auth(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),

    % when
    AuthFail = do_request(Config, get, Endpoint ++ "files", #{<<"X-Auth-Token">> => <<"invalid">>}),
    AuthSuccess1 = do_request(Config, get, Endpoint ++ "files", #{<<"X-Auth-Token">> => ?MACAROON}),
    AuthSuccess2 = do_request(Config, get, Endpoint ++ "files", #{<<"Macaroon">> => ?MACAROON}),
    AuthSuccess3 = do_request(Config, get, Endpoint ++ "files", #{<<"Authorization">> => <<"Bearer ", (?MACAROON)/binary>>}),

    % then
    ?assertMatch({ok, 401, _, _}, AuthFail),
    ?assertMatch({ok, 200, _, _}, AuthSuccess1),
    ?assertMatch({ok, 200, _, _}, AuthSuccess2),
    ?assertMatch({ok, 200, _, _}, AuthSuccess3).

internal_error_when_handler_crashes(Config) ->
    % given
    Workers = [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),
    test_utils:mock_expect(Workers, files, is_authorized, fun test_crash/2),

    % when
    {ok, Status, _, _} = do_request(Config, get, Endpoint ++ "files"),

    % then
    ?assertEqual(500, Status).

custom_code_when_handler_throws_code(Config) ->
    % given
    Workers = [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),
    test_utils:mock_expect(Workers, files, is_authorized, fun test_throw_400/2),

    % when
    {ok, Status, _, _} = do_request(Config, get, Endpoint ++ "files"),

    % then
    ?assertEqual(400, Status).

custom_error_when_handler_throws_error(Config) ->
    % given
    Workers = [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),
    test_utils:mock_expect(Workers, files, is_authorized, fun test_throw_400_with_description/2),

    % when
    {ok, Status, _, Body} = do_request(Config, get, Endpoint ++ "files"),

    % then
    ?assertEqual(400, Status),
    ?assertEqual(<<"{\"error\":\"badrequest\"}">>, Body).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        [Worker | _] = ?config(op_worker_nodes, NewConfig),
        initializer:clear_subscriptions(Worker),
        NewConfig
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


init_per_testcase(Case, Config) when
    Case =:= internal_error_when_handler_crashes;
    Case =:= custom_code_when_handler_throws_code;
    Case =:= custom_error_when_handler_throws_error ->
    Workers = ?config(op_worker_nodes, Config),
    ssl:start(),
    hackney:start(),
    test_utils:mock_new(Workers, files),
    mock_provider_id(Config),
    Config;
init_per_testcase(_Case, Config) ->
    ssl:start(),
    hackney:start(),
    mock_provider_id(Config),
    mock_provider_logic(Config),
    mock_space_logic(Config),
    mock_user_logic(Config),
    Config.

end_per_testcase(Case, Config) when
    Case =:= internal_error_when_handler_crashes;
    Case =:= custom_code_when_handler_throws_code;
    Case =:= custom_error_when_handler_throws_error ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, files),
    unmock_provider_id(Config),
    hackney:stop(),
    ssl:stop();
end_per_testcase(_Case, Config) ->
    unmock_provider_id(Config),
    unmock_provider_logic(Config),
    unmock_space_logic(Config),
    unmock_user_logic(Config),
    hackney:stop(),
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
    CaCerts = rpc:call(Node, https_listener, get_cert_chain_pems, []),
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
        fun(_, ?SPACE_ID) ->
            {ok, #document{value = #od_space{
                name = ?SPACE_NAME,
                eff_users = #{?USER_ID => []},
                providers = #{?PROVIDER_ID => 1000000000}
            }}}
        end),
    test_utils:mock_expect(Workers, space_logic, get_name,
        fun(_, ?SPACE_ID) ->
            {ok, ?SPACE_NAME}
        end).

unmock_space_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, space_logic).

mock_user_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, user_logic, []),

    UserDoc = {ok, #document{key = ?USER_ID, value = #od_user{
        name = ?USER_NAME,
        eff_spaces = [?SPACE_ID]
    }}},

    GetUserFun = fun
        (#macaroon_auth{macaroon = ?MACAROON}, ?USER_ID) ->
            UserDoc;
        (?ROOT_SESS_ID, ?USER_ID) ->
            UserDoc;
        (UserSessId, ?USER_ID) ->
            try session:get_user_id(UserSessId) of
                {ok, ?USER_ID} -> UserDoc;
                _ -> {error, forbidden}
            catch
                _:_ -> {error, forbidden}
            end;
        (_, _) ->
            {error, not_found}
    end,

    test_utils:mock_expect(Workers, user_logic, get, GetUserFun),
    test_utils:mock_expect(Workers, user_logic, get_by_auth, fun(Auth) ->
        GetUserFun(Auth, ?USER_ID)
    end),
    test_utils:mock_expect(Workers, user_logic, exists,
        fun(Auth, UserId) ->
            case GetUserFun(Auth, UserId) of
                {ok, _} ->
                    true;
                _ ->
                    false
            end
        end
    ),
    [rpc:call(W, file_meta, setup_onedata_user, [?USER_ID, []]) || W <- Workers].


unmock_user_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, user_logic).


mock_provider_id(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:mock_provider_id(
        Workers, ?PROVIDER_ID, <<"auth-macaroon">>, <<"identity-macaroon">>
    ).


unmock_provider_id(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:unmock_provider_ids(Workers).


-spec test_crash(term(), term()) -> no_return().
test_crash(_, _) ->
    throw(test_crash).

-spec test_throw_400(term(), term()) -> no_return().
test_throw_400(_, _) ->
    throw(400).

-spec test_throw_400_with_description(term(), term()) -> no_return().
test_throw_400_with_description(_, _) ->
    throw({400, #{<<"error">> => <<"badrequest">>}}).