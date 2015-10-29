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
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([token_auth/1, cert_auth/1, internal_error_when_handler_crashes/1,
    custom_code_when_handler_throws_code/1, custom_error_when_handler_throws_error/1]).

%todo reenable rest_cert_auth after appmock repair
-performance({test_cases, []}).
all() -> [token_auth, internal_error_when_handler_crashes,
    custom_code_when_handler_throws_code, custom_error_when_handler_throws_error].

-define(MACAROON, "macaroon").

%%%===================================================================
%%% Test functions
%%%===================================================================

token_auth(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),

    % when
    AuthFail = ibrowse:send_req(Endpoint ++ "random_path", [{"X-Auth-Token", "invalid"}], get),
    AuthSuccess = ibrowse:send_req(Endpoint ++ "random_path", [{"X-Auth-Token", ?MACAROON}], get),

    % then
    ?assertMatch({ok, "401", _, _}, AuthFail),
    ?assertMatch({ok, "404", _, _}, AuthSuccess).

cert_auth(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),
    CertUnknown = ?TEST_FILE(Config, "unknown_peer.pem"),
    CertKnown = ?TEST_FILE(Config, "known_peer.pem"),
    UnknownCertOpt = {ssl_options, [{certfile, CertUnknown}, {reuse_sessions, false}]},
    KnownCertOpt = {ssl_options, [{certfile, CertKnown}, {reuse_sessions, false}]},

    % then - unauthorized access
    {ok, "307", Headers, _} = ibrowse:send_req(Endpoint ++ "random_path", [], get, [], [UnknownCertOpt]),
    Loc = proplists:get_value("location", Headers),
    ?assertMatch({ok, "401", _, _}, ibrowse:send_req(Loc, [], get, [], [UnknownCertOpt])),

    % then - authorized access
    {ok, "307", Headers2, _} = ibrowse:send_req(Endpoint ++ "random_path", [], get, [], [KnownCertOpt]),
    Loc2 = proplists:get_value("location", Headers2),
    {ok, "307", Headers3, _} = ibrowse:send_req(Loc2, [], get, [], [KnownCertOpt]),
    Loc3 = proplists:get_value("location", Headers3),
    ?assertMatch({ok, "404", _, _}, ibrowse:send_req(Loc3, [], get, [], [KnownCertOpt])).

internal_error_when_handler_crashes(Config) ->
    % given
    Workers = [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),
    test_utils:mock_expect(Workers, rest_handler, is_authorized, fun test_crash/2),

    % when
    {ok, Status, _, _} =  ibrowse:send_req(Endpoint ++ "random_path", [], get, [], []),

    % then
    ?assertEqual("500", Status).

custom_code_when_handler_throws_code(Config) ->
    % given
    Workers = [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),
    test_utils:mock_expect(Workers, rest_handler, is_authorized, fun test_throw_400/2),

    % when
    {ok, Status, _, _} =  ibrowse:send_req(Endpoint ++ "random_path", [], get, [], []),

    % then
    ?assertEqual("400", Status).

custom_error_when_handler_throws_error(Config) ->
    % given
    Workers = [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),
    test_utils:mock_expect(Workers, rest_handler, is_authorized, fun test_throw_400_with_description/2),

    % when
    {ok, Status, _, Body} =  ibrowse:send_req(Endpoint ++ "random_path", [], get, [], []),

    % then
    ?assertEqual("400", Status),
    ?assertEqual("{\"error\":\"badrequest\"}", Body).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(Module, Config)
    when Module =:= internal_error_when_handler_crashes
    orelse Module =:= custom_code_when_handler_throws_code
    orelse Module =:= custom_error_when_handler_throws_error ->
    Workers = ?config(op_worker_nodes, Config),
    ssl:start(),
    ibrowse:start(),
    test_utils:mock_new(Workers, rest_handler),
    Config;
init_per_testcase(_, Config) ->
    ssl:start(),
    ibrowse:start(),
    mock_gr_certificates(Config),
    Config.

end_per_testcase(Module, Config)
    when Module =:= internal_error_when_handler_crashes
    orelse Module =:= custom_code_when_handler_throws_code
    orelse Module =:= custom_error_when_handler_throws_error ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, rest_handler),
    ibrowse:stop(),
    ssl:stop();
end_per_testcase(_, Config) ->
    unmock_gr_certificates(Config),
    ibrowse:stop(),
    ssl:stop().

%%%===================================================================
%%% Internal functions
%%%===================================================================

rest_endpoint(Node) ->
    Port =
        case get(port) of
            undefined ->
                {ok, P} = rpc:call(Node, application, get_env, [?APP_NAME, http_worker_rest_port]),
                PStr = integer_to_list(P),
                put(port, PStr),
                PStr;
            P -> P
        end,
    string:join(["https://", utils:get_host(Node), ":", Port, "/rest/latest/"], "").

mock_gr_certificates(Config) ->
    [Worker1, _] = Workers = ?config(op_worker_nodes, Config),
    Url = rpc:call(Worker1, gr_plugin, get_gr_url, []),
    {ok, Key} = file:read_file(?TEST_FILE(Config, "grpkey.pem")),
    {ok, Cert} = file:read_file(?TEST_FILE(Config, "grpcert.pem")),
    {ok, CACert} = file:read_file(?TEST_FILE(Config, "grpCA.pem")),
    [{KeyType, KeyEncoded, _} | _] = rpc:call(Worker1, public_key, pem_decode, [Key]),
    [{_, CertEncoded, _} | _] = rpc:call(Worker1, public_key, pem_decode, [Cert]),
    [{_, CACertEncoded, _} | _] = rpc:call(Worker1, public_key, pem_decode, [CACert]),
    SSLOptions = {ssl_options, [{cacerts, [CACertEncoded]}, {key, {KeyType, KeyEncoded}}, {cert, CertEncoded}]},

    test_utils:mock_new(Workers, [oneprovider]),
    test_utils:mock_expect(Workers, oneprovider, get_provider_id,
        fun() -> <<"050fec8f157d6e4b31fd6d2924923c7a">> end),
    test_utils:mock_expect(Workers, oneprovider, get_globalregistry_cert,
        fun() -> public_key:pkix_decode_cert(CACertEncoded, otp) end),

    test_utils:mock_new(Workers, [gr_endpoint]),
    test_utils:mock_expect(Workers, gr_endpoint, auth_request,
        fun
            (provider, URN, Method, Headers, Body, Options) ->
                ibrowse:send_req(Url ++ URN, [{"content-type", "application/json"} | Headers], Method, Body, [SSLOptions | Options]);
            (client, URN, Method, Headers, Body, Options) ->
                ibrowse:send_req(Url ++ URN, [{"content-type", "application/json"} | Headers], Method, Body, [SSLOptions | Options]);
            ({_, undefined}, URN, Method, Headers, Body, Options) ->
                ibrowse:send_req(Url ++ URN, [{"content-type", "application/json"} | Headers], Method, Body, [SSLOptions | Options]);
            % @todo for now, in rest we only use the root macaroon
            ({_, {Macaroon, []}}, URN, Method, Headers, Body, Options) ->
                AuthorizationHeader = {"macaroon", binary_to_list(Macaroon)},
                ibrowse:send_req(Url ++ URN, [{"content-type", "application/json"}, AuthorizationHeader | Headers], Method, Body, [SSLOptions | Options])
        end
    ).

unmock_gr_certificates(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate(Workers, [gr_endpoint]),
    test_utils:mock_unload(Workers, [gr_endpoint]),
    test_utils:mock_validate(Workers, [oneprovider]),
    test_utils:mock_unload(Workers, [oneprovider]).

-spec test_crash(term(), term()) -> no_return().
test_crash(_, _) ->
    throw(test_crash).

-spec test_throw_400(term(), term()) -> no_return().
test_throw_400(_, _) ->
    throw(400).

-spec test_throw_400_with_description(term(), term()) -> no_return().
test_throw_400_with_description(_, _) ->
    throw({400, [{<<"error">>, <<"badrequest">>}]}).