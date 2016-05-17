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
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([
    token_auth/1,
    cert_auth/1,
    internal_error_when_handler_crashes/1,
    custom_code_when_handler_throws_code/1,
    custom_error_when_handler_throws_error/1
]).


all() -> ?ALL([
    token_auth,
%%    cert_auth, %todo reenable rest_cert_auth after appmock repair
    internal_error_when_handler_crashes,
    custom_code_when_handler_throws_code,
    custom_error_when_handler_throws_error
]).

-define(MACAROON, element(2, macaroon:serialize(macaroon:create("a", "b", "c")))).

%%%===================================================================
%%% Test functions
%%%===================================================================

token_auth(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),

    % when
    AuthFail = do_request(get, Endpoint ++ "random_path", [{<<"X-Auth-Token">>, <<"invalid">>}]),
    AuthSuccess = do_request(get, Endpoint ++ "random_path", [{<<"X-Auth-Token">>, ?MACAROON}]),

    % then
    ?assertMatch({ok, 401, _, _}, AuthFail),
    ?assertMatch({ok, 404, _, _}, AuthSuccess).

cert_auth(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),
    CertUnknown = ?TEST_FILE(Config, "unknown_peer.pem"),
    CertKnown = ?TEST_FILE(Config, "known_peer.pem"),
    UnknownCertOpt = {ssl_options, [{certfile, CertUnknown}, {reuse_sessions, false}]},
    KnownCertOpt = {ssl_options, [{certfile, CertKnown}, {reuse_sessions, false}]},

    % then - unauthorized access
    {ok, 307, Headers, _} = do_request(get, Endpoint ++ "random_path", [], <<>>, [UnknownCertOpt]),
    Loc = proplists:get_value(<<"location">>, Headers),
    ?assertMatch({ok, 401, _, _}, do_request(get, Loc, [], <<>>, [UnknownCertOpt])),

    % then - authorized access
    {ok, 307, Headers2, _} = do_request(get, Endpoint ++ "random_path", [], <<>>, [KnownCertOpt]),
    Loc2 = proplists:get_value(<<"location">>, Headers2),
    {ok, 307, Headers3, _} = do_request(get, Loc2, [], <<>>, [KnownCertOpt]),
    Loc3 = proplists:get_value(<<"location">>, Headers3),
    ?assertMatch({ok, 404, _, _}, do_request(get, Loc3, [], <<>>, [KnownCertOpt])).

internal_error_when_handler_crashes(Config) ->
    % given
    Workers = [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),
    test_utils:mock_expect(Workers, rest_handler, is_authorized, fun test_crash/2),

    % when
    {ok, Status, _, _} = do_request(get, Endpoint ++ "random_path"),

    % then
    ?assertEqual(500, Status).

custom_code_when_handler_throws_code(Config) ->
    % given
    Workers = [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),
    test_utils:mock_expect(Workers, rest_handler, is_authorized, fun test_throw_400/2),

    % when
    {ok, Status, _, _} = do_request(get, Endpoint ++ "random_path"),

    % then
    ?assertEqual(400, Status).

custom_error_when_handler_throws_error(Config) ->
    % given
    Workers = [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),
    test_utils:mock_expect(Workers, rest_handler, is_authorized, fun test_throw_400_with_description/2),

    % when
    {ok, Status, _, Body} = do_request(get, Endpoint ++ "random_path"),

    % then
    ?assertEqual(400, Status),
    ?assertEqual(<<"{\"error\":\"badrequest\"}">>, Body).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    NewConfig = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]),
    [Worker | _] = ?config(op_worker_nodes, NewConfig),
    initializer:clear_models(Worker, [subscription]),
    NewConfig.

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(Case, Config) when
    Case =:= internal_error_when_handler_crashes;
    Case =:= custom_code_when_handler_throws_code;
    Case =:= custom_error_when_handler_throws_error ->
    Workers = ?config(op_worker_nodes, Config),
    application:start(ssl2),
    hackney:start(),
    test_utils:mock_new(Workers, rest_handler),
    Config;
init_per_testcase(_, Config) ->
    application:start(ssl2),
    hackney:start(),
    mock_oz_spaces(Config),
    mock_oz_certificates(Config),
    Config.

end_per_testcase(Case, Config) when
    Case =:= internal_error_when_handler_crashes;
    Case =:= custom_code_when_handler_throws_code;
    Case =:= custom_error_when_handler_throws_error ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, rest_handler),
    hackney:stop(),
    application:stop(ssl2);
end_per_testcase(_, Config) ->
    unmock_oz_spaces(Config),
    unmock_oz_certificates(Config),
    hackney:stop(),
    application:stop(ssl2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

% Performs a single request using http_client
do_request(Method, URL) ->
    do_request(Method, URL, []).
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
    string:join(["https://", utils:get_host(Node), ":", Port, "/rest/latest/"], "").

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
    Url = rpc:call(Worker1, oz_plugin, get_oz_url, []),

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

    {ok, CACert} = file:read_file(?TEST_FILE(Config, "grpCA.pem")),
    [{_, CACertEncoded, _} | _] = rpc:call(Worker1, public_key, pem_decode, [CACert]),

    test_utils:mock_expect(Workers, oneprovider, get_provider_id,
        fun() -> <<"050fec8f157d6e4b31fd6d2924923c7a">> end),
    test_utils:mock_expect(Workers, oneprovider, get_oz_cert,
        fun() -> public_key:pkix_decode_cert(CACertEncoded, otp) end),

    test_utils:mock_expect(Workers, oz_endpoint, auth_request,
        fun
            (provider, URN, Method, Headers, Body, Options) ->
                do_request(Method, Url ++ URN,
                    [{<<"content-type">>,<< "application/json">>} | Headers],
                    Body, [SSLOpts | Options]);
            (client, URN, Method, Headers, Body, Options) ->
                do_request(Method, Url ++ URN,
                    [{<<"content-type">>,<< "application/json">>} | Headers],
                    Body, [SSLOpts | Options]);
            ({_, undefined}, URN, Method, Headers, Body, Options) ->
                do_request(Method, Url ++ URN,
                    [{<<"content-type">>,<< "application/json">>} | Headers],
                    Body, [SSLOpts | Options]);
            % @todo for now, in rest we only use the root macaroon
            ({_, {Macaroon, []}}, URN, Method, Headers, Body, Options) ->
                {ok, SrlzdMacaroon} = macaroon:serialize(Macaroon),
                AuthorizationHeader = {<<"macaroon">>, SrlzdMacaroon},
                do_request(Method, Url ++ URN,
                    [{<<"content-type">>,<< "application/json">>},
                        AuthorizationHeader | Headers],
                    Body, [SSLOpts | Options])
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
    throw({400, [{<<"error">>, <<"badrequest">>}]}).