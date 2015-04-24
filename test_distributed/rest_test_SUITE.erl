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

-export([rest_token_auth/1, rest_cert_auth/1]).

-performance({test_cases, []}).
all() -> [rest_token_auth, rest_cert_auth].

-define(TOKEN, "TOKEN").

%%%===================================================================
%%% API
%%%===================================================================

rest_token_auth(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),

    % when
    AuthFail = ibrowse:send_req(Endpoint ++ "random_path", [{"X-Auth-Token", "invalid"}], get),
    AuthSuccess = ibrowse:send_req(Endpoint ++ "random_path", [{"X-Auth-Token", ?TOKEN}], get),

    % then
    ?assertMatch({ok, "401", _, _}, AuthFail),
    ?assertMatch({ok, "404", _, _}, AuthSuccess).

rest_cert_auth(Config) ->
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

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================
init_per_suite(Config) ->
    Config2 = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")),
    timer:sleep(timer:seconds(15)), % waiting for appmock. todo integrate with nagios
    Config2.

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    ssl:start(),
    ibrowse:start(),
    mock_gr_certificates(Config),
    Config.

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
            ({_, AccessToken}, URN, Method, Headers, Body, Options) ->
                AuthorizationHeader = {"authorization", "Bearer " ++ binary_to_list(AccessToken)},
                ibrowse:send_req(Url ++ URN, [{"content-type", "application/json"}, AuthorizationHeader | Headers], Method, Body, [SSLOptions | Options])

        end
    ).

unmock_gr_certificates(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate(Workers, [gr_endpoint]),
    test_utils:mock_unload(Workers, [gr_endpoint]),
    test_utils:mock_validate(Workers, [oneprovider]),
    test_utils:mock_unload(Workers, [oneprovider]).
