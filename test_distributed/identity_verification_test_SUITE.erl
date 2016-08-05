%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests identity verification.
%%% @end
%%%--------------------------------------------------------------------
-module(identity_verification_test_SUITE).
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([cert_connection_test/1]).

-define(NORMAL_CASES_NAMES, [
    cert_connection_test
]).

-define(PERFORMANCE_CASES_NAMES, [
]).

all() -> ?ALL(?NORMAL_CASES_NAMES, ?PERFORMANCE_CASES_NAMES).


%%%===================================================================
%%% Test functions
%%%===================================================================

cert_connection_test(Config) ->
    %% given
    [Node1, Node2 | _] = ?config(op_worker_nodes, Config),
    Host2 = get_hostname(Node2),

    VerifyFun = {verify_fun, {fun(Cert, Event, State) ->
        rpc:call(Node1, identity, ssl_verify_fun_impl, [Cert, Event, State])
    end, []}},

    %% when - request with cert published in DHT
    {ok, OZCertFile} = rpc:call(Node1, application, get_env, [?APP_NAME, identity_cert_file]),
    {ok, OZKeyFile} = rpc:call(Node1, application, get_env, [?APP_NAME, identity_key_file]),
    Options1 = [insecure, {ssl_options, [VerifyFun, {certfile, OZCertFile}, {keyfile, OZKeyFile}]}],
    Res1 = rpc:call(Node1, hackney, request, [get, <<"https://", Host2/binary, ":6443">>, [], [], Options1]),

    %% when - request with cert not present in DHT
    {ok, OtherCertFile} = rpc:call(Node1, application, get_env, [?APP_NAME, oz_provider_cert_path]),
    {ok, OtherKeyFile} = rpc:call(Node1, application, get_env, [?APP_NAME, oz_provider_key_path]),
    Options2 = [insecure, {ssl_options, [VerifyFun, {certfile, OtherCertFile}, {keyfile, OtherKeyFile}]}],
    Res2 = rpc:call(Node1, hackney, request, [get, <<"https://", Host2/binary, ":6443">>, [], [], Options2]),

    %% when - server with cert not present in DHT
    Res3 = rpc:call(Node1, hackney, request, [get, <<"https://", Host2/binary>>, [], [], Options1]),

    %% then
    ?assertMatch({ok, _, _, _}, Res1),
    ?assertMatch({error, _}, Res2),
    ?assertMatch({error, _}, Res3),
    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================


get_hostname(Node) ->
    Host = rpc:call(Node, oneprovider, get_node_hostname, []),
    list_to_binary(Host).