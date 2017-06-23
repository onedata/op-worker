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
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").
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
-define(BASIC_AUTH_HEADER, <<"Basic ", (base64:encode(<<"user:password">>))/binary>>).

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
    mock_oz_spaces(Config),
    mock_oz_certificates(Config),
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
    unmock_oz_spaces(Config),
    unmock_oz_certificates(Config),
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

mock_oz_spaces(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_expect(Workers, oz_spaces, get_details,
        fun(_, _) -> {ok, #space_details{}} end),
    test_utils:mock_expect(Workers, oz_spaces, get_users,
        fun(_, _) -> {ok, []} end),
    test_utils:mock_expect(Workers, oz_spaces, get_groups,
        fun(_, _) -> {ok, []} end).

unmock_oz_spaces(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, oz_spaces).

mock_oz_certificates(Config) ->
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    OZUrl = rpc:call(Worker1, oz_plugin, get_oz_url, []),
    OZRestPort = rpc:call(Worker1, oz_plugin, get_oz_rest_port, []),
    OZRestApiPrefix = rpc:call(Worker1, oz_plugin, get_oz_rest_api_prefix, []),
    OzRestApiUrl = str_utils:format("~s:~B~s", [
        OZUrl,
        OZRestPort,
        OZRestApiPrefix
    ]),

    % save key and cert files on the workers
    % read the files
    {ok, KeyBin} = file:read_file(?TEST_FILE(Config, "grpkey.pem")),
    {ok, CertBin} = file:read_file(?TEST_FILE(Config, "grpcert.pem")),
    % choose paths for the files
    KeyPath = "/tmp/user_auth_test_key.pem",
    CertPath = "/tmp/user_auth_test_cert.pem",
    % and save them on workers
    lists:foreach(
        fun(Node) ->
            ok = rpc:call(Node, file, write_file, [KeyPath, KeyBin]),
            ok = rpc:call(Node, file, write_file, [CertPath, CertBin])
        end, Workers),
    % Use the cert paths on workers to mock oz_endpoint
    SSLOpts = {ssl_options, [{keyfile, KeyPath}, {certfile, CertPath}]},

    test_utils:mock_new(Workers, [oneprovider, oz_endpoint]),

    test_utils:mock_expect(Workers, oneprovider, get_provider_id,
        fun() -> <<"050fec8f157d6e4b31fd6d2924923c7a">> end),

    test_utils:mock_expect(Workers, oz_endpoint, provider_request,
        fun
        % @todo for now, in rest we only use the root macaroon
            (#macaroon_auth{macaroon = Macaroon}, URN, Method, Headers, Body, Options) ->
                do_request(Method, OzRestApiUrl ++ URN, Headers#{
                    <<"macaroon">> => Macaroon,
                    <<"content-type">> => <<"application/json">>
                }, Body, [SSLOpts | Options]);
            (#token_auth{token = Token}, URN, Method, Headers, Body, Options) ->
                do_request(Method, OzRestApiUrl ++ URN, Headers#{
                    <<"x-auth-token">> => Token,
                    <<"content-type">> => <<"application/json">>
                }, Body, [SSLOpts | Options]);
            (#basic_auth{credentials = Credentials}, URN, Method, Headers, Body, Options) ->
                do_request(Method, OzRestApiUrl ++ URN, Headers#{
                    <<"Authorization">> => Credentials,
                    <<"content-type">> => <<"application/json">>
                }, Body, [SSLOpts | Options]);
            (_, URN, Method, Headers, Body, Options) ->
                do_request(Method, OzRestApiUrl ++ URN, Headers#{
                    <<"content-type">> => <<"application/json">>
                }, Body, [SSLOpts | Options])
        end
    ).

unmock_oz_certificates(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, [oz_endpoint, oneprovider]).

-spec test_crash(term(), term()) -> no_return().
test_crash(_, _) ->
    throw(test_crash).

-spec test_throw_400(term(), term()) -> no_return().
test_throw_400(_, _) ->
    throw(400).

-spec test_throw_400_with_description(term(), term()) -> no_return().
test_throw_400_with_description(_, _) ->
    throw({400, #{<<"error">> => <<"badrequest">>}}).