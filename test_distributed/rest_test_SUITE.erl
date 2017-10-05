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
-include("proto/oneclient/handshake_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% API
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    token_auth/1,
    cert_auth/1,
    basic_auth/1,
    internal_error_when_handler_crashes/1,
    custom_code_when_handler_throws_code/1,
    custom_error_when_handler_throws_error/1
]).


all() -> ?ALL([
    token_auth,
%%    cert_auth, %todo reenable rest_cert_auth after appmock repair
    basic_auth,
    internal_error_when_handler_crashes,
    custom_code_when_handler_throws_code,
    custom_error_when_handler_throws_error
]).

-define(MACAROON, <<"DUMMY-MACAROON">>).
-define(BASIC_AUTH_CREDENTIALS, <<"dXNlcjpwYXNzd29yZAo=">>).
-define(BASIC_AUTH_HEADER, <<"Basic ", (?BASIC_AUTH_CREDENTIALS)/binary>>).

-define(USER_ID, <<"test_id">>).
-define(USER_NAME, <<"test_name">>).

-define(SPACE_ID, <<"space0">>).
-define(SPACE_NAME, <<"Space 0">>).

%%%===================================================================
%%% Test functions
%%%===================================================================

token_auth(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),

    % when
    AuthFail = do_request(get, Endpoint ++ "files", #{<<"X-Auth-Token">> => <<"invalid">>}),
    AuthSuccess1 = do_request(get, Endpoint ++ "files", #{<<"X-Auth-Token">> => ?MACAROON}),
    AuthSuccess2 = do_request(get, Endpoint ++ "files", #{<<"Macaroon">> => ?MACAROON}),

    % then
    ?assertMatch({ok, 401, _, _}, AuthFail),
    ?assertMatch({ok, 200, _, _}, AuthSuccess1),
    ?assertMatch({ok, 200, _, _}, AuthSuccess2).

cert_auth(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),
    CertUnknown = ?TEST_FILE(Config, "unknown_peer.pem"),
    CertKnown = ?TEST_FILE(Config, "known_peer.pem"),
    UnknownCertOpt = {ssl_options, [{certfile, CertUnknown}, {reuse_sessions, false}]},
    KnownCertOpt = {ssl_options, [{certfile, CertKnown}, {reuse_sessions, false}]},

    % then - unauthorized access
    {ok, 307, Headers, _} = do_request(get, Endpoint ++ "files", #{}, <<>>, [UnknownCertOpt]),
    Loc = maps:get(<<"location">>, Headers),
    ?assertMatch({ok, 401, _, _}, do_request(get, Loc, #{}, <<>>, [UnknownCertOpt])),

    % then - authorized access
    {ok, 307, Headers2, _} = do_request(get, Endpoint ++ "files", #{}, <<>>, [KnownCertOpt]),
    Loc2 = maps:get(<<"location">>, Headers2),
    {ok, 307, Headers3, _} = do_request(get, Loc2, #{}, <<>>, [KnownCertOpt]),
    Loc3 = maps:get(<<"location">>, Headers3),
    ?assertMatch({ok, 404, _, _}, do_request(get, Loc3, #{}, <<>>, [KnownCertOpt])).

basic_auth(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),

    % when
    AuthFail = do_request(get, Endpoint ++ "files", #{<<"Authorization">> => <<"invalid">>}),
    AuthSuccess = do_request(get, Endpoint ++ "files", #{<<"Authorization">> => ?BASIC_AUTH_HEADER}),

    % then
    ?assertMatch({ok, 401, _, _}, AuthFail),
    ?assertMatch({ok, 200, _, _}, AuthSuccess).

internal_error_when_handler_crashes(Config) ->
    % given
    Workers = [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),
    test_utils:mock_expect(Workers, files, is_authorized, fun test_crash/2),

    % when
    {ok, Status, _, _} = do_request(get, Endpoint ++ "files"),

    % then
    ?assertEqual(500, Status).

custom_code_when_handler_throws_code(Config) ->
    % given
    Workers = [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),
    test_utils:mock_expect(Workers, files, is_authorized, fun test_throw_400/2),

    % when
    {ok, Status, _, _} = do_request(get, Endpoint ++ "files"),

    % then
    ?assertEqual(400, Status).

custom_error_when_handler_throws_error(Config) ->
    % given
    Workers = [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),
    test_utils:mock_expect(Workers, files, is_authorized, fun test_throw_400_with_description/2),

    % when
    {ok, Status, _, Body} = do_request(get, Endpoint ++ "files"),

    % then
    ?assertEqual(400, Status),
    ?assertEqual(<<"{\"error\":\"badrequest\"}">>, Body).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        [Worker | _] = ?config(op_worker_nodes, NewConfig),
        initializer:clear_models(Worker, [subscription]),
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
    Config;
init_per_testcase(_Case, Config) ->
    ssl:start(),
    hackney:start(),
    mock_space_logic(Config),
    mock_user_logic(Config),
    Config.

end_per_testcase(Case, Config) when
    Case =:= internal_error_when_handler_crashes;
    Case =:= custom_code_when_handler_throws_code;
    Case =:= custom_error_when_handler_throws_error ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, files),
    hackney:stop(),
    ssl:stop();
end_per_testcase(_Case, Config) ->
    unmock_space_logic(Config),
    unmock_user_logic(Config),
    hackney:stop(),
    ssl:stop().

%%%===================================================================
%%% Internal functions
%%%===================================================================

% Performs a single request using http_client
do_request(Method, URL) ->
    do_request(Method, URL, #{}).
do_request(Method, URL, Headers) ->
    do_request(Method, URL, Headers, <<>>).
do_request(Method, URL, Headers, Body) ->
    do_request(Method, URL, Headers, Body, []).
do_request(Method, URL, Headers, Body, Opts) ->
    http_client:request(Method, URL, Headers, Body, [insecure | Opts]).


rest_endpoint(Node) ->
    Port =
        case get(port) of
            undefined ->
                {ok, P} = test_utils:get_env(Node, ?APP_NAME, rest_port),
                PStr = integer_to_list(P),
                put(port, PStr),
                PStr;
            P -> P
        end,
    string:join(["https://", utils:get_host(Node), ":", Port, "/api/v3/oneprovider/"], "").

mock_space_logic(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, space_logic, []),
    test_utils:mock_expect(Workers, space_logic, get,
        fun(_, ?SPACE_ID) ->
            {ok, #document{value = #od_space{
                name = ?SPACE_NAME,
                eff_users = #{?USER_ID => []}
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
        (#token_auth{token = ?MACAROON}, ?USER_ID) ->
            UserDoc;
        (#basic_auth{credentials = ?BASIC_AUTH_CREDENTIALS}, ?USER_ID) ->
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


-spec test_crash(term(), term()) -> no_return().
test_crash(_, _) ->
    throw(test_crash).

-spec test_throw_400(term(), term()) -> no_return().
test_throw_400(_, _) ->
    throw(400).

-spec test_throw_400_with_description(term(), term()) -> no_return().
test_throw_400_with_description(_, _) ->
    throw({400, #{<<"error">> => <<"badrequest">>}}).