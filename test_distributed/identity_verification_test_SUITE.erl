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

-export([no_connection_on_identity_cert_used_for_nonprotected_endpoint/1,
    no_connection_on_nonpublished_cert_used/1,
    connection_on_identity_cert_used/1]).

-define(NORMAL_CASES_NAMES, [
    connection_on_identity_cert_used,
    no_connection_on_nonpublished_cert_used,
    no_connection_on_identity_cert_used_for_nonprotected_endpoint
]).

-define(PERFORMANCE_CASES_NAMES, [
]).

all() -> ?ALL(?NORMAL_CASES_NAMES, ?PERFORMANCE_CASES_NAMES).


%%%===================================================================
%%% Test functions
%%%===================================================================

connection_on_identity_cert_used(Config) ->
    %% given
    [WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),
    HostP2 = get_hostname(WorkerP2),
    TestPort = test_endpoint_port(WorkerP1),
    {IdentityKeyFile, IdentityCertFile} = get_identity_cert_paths(WorkerP1),

    %% when
    Result = request(WorkerP1, HostP2, TestPort, IdentityKeyFile, IdentityCertFile),
    ct:print("Result ~p", [Result]),

    %% then
    ?assertMatch({ok, _, _, _}, Result),
    ok.

no_connection_on_nonpublished_cert_used(Config) ->
    %% given
    [WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),
    HostP2 = get_hostname(WorkerP2),
    TestPort = test_endpoint_port(WorkerP1),
    {OtherKeyFile, OtherCertFile} = get_nonpublished_cert_paths(WorkerP1),

    %% when
    Result = request(WorkerP1, HostP2, TestPort, OtherKeyFile, OtherCertFile),
    ct:print("Result ~p", [Result]),

    %% then
    ?assertMatch({error, _}, Result),
    ok.

no_connection_on_identity_cert_used_for_nonprotected_endpoint(Config) ->
    %% given
    [WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),
    HostP2 = get_hostname(WorkerP2),
    {IdentityKeyFile, IdentityCertFile} = get_identity_cert_paths(WorkerP1),

    %% when
    Result = request(WorkerP1, HostP2, <<"80">>, IdentityKeyFile, IdentityCertFile, [insecure]),
    ct:print("Result ~p", [Result]),

    %% then
    ?assertMatch({error, _}, Result),
    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    UpdatedConfig = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [identity_test_listener]),
    Nodes = ?config(op_worker_nodes, UpdatedConfig),
    [ok = rpc:call(Node, identity_test_listener, start, []) || Node <- Nodes],
    UpdatedConfig.

end_per_suite(Config) ->
%%    ok.
    test_node_starter:clean_environment(Config).

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_nonpublished_cert_paths(Worker) ->
    TmpDir = rpc:call(Worker, utils, mkdtemp, []),
    KeyFile = TmpDir ++ "/key.pem",
    CertFile = TmpDir ++ "/cert.pem",
    PassFile = TmpDir ++ "/pass",
    CSRFile = TmpDir ++ "/csr",
    DomainForCN = binary_to_list(get_hostname(Worker)),

    rpc:call(Worker, os, cmd, [["openssl genrsa", " -des3 ", " -passout ", " pass:xx ", " -out ", PassFile, " 2048 "]]),
    rpc:call(Worker, os, cmd, [["openssl rsa", " -passin ", " pass:xx ", " -in ", PassFile, " -out ", KeyFile]]),
    rpc:call(Worker, os, cmd, [["openssl req", " -new ", " -key ", KeyFile, " -out ", CSRFile, " -subj ", "\"/CN=" ++ DomainForCN ++ "\""]]),
    rpc:call(Worker, os, cmd, [["openssl x509", " -req ", " -days ", " 365 ", " -in ", CSRFile, " -signkey ", KeyFile, " -out ", CertFile]]),

    ct:print("get_nonpublished_cert_paths ~p", [{KeyFile, CertFile}]),
    {KeyFile, CertFile}.

get_identity_cert_paths(WorkerP1) ->
    {ok, IdentityCertFile} = rpc:call(WorkerP1, application, get_env, [?APP_NAME, identity_cert_file]),
    {ok, IdentityKeyFile} = rpc:call(WorkerP1, application, get_env, [?APP_NAME, identity_key_file]),
    ct:print("get_identity_cert_paths ~p", [{IdentityKeyFile, IdentityCertFile}]),
    {IdentityKeyFile, IdentityCertFile}.

test_endpoint_port(Worker) ->
    integer_to_binary(rpc:call(Worker, identity_test_listener, port, [])).

request(Node, Host, Port, KeyPath, CertPath) ->
    request(Node, Host, Port, KeyPath, CertPath, []).
request(Node, Host, Port, KeyPath, CertPath, Options) ->
    VerifyFun = {verify_fun, {fun(Cert, Event, State) ->

        ct:print("CALLBACK ~p\n~p\n~p", [Cert, Event, State]),
        rpc:call(Node, identity, ssl_verify_fun_impl, [Cert, Event, State])
    end, []}},

    FinalOptions = [{ssl_options, [VerifyFun, {certfile, CertPath}, {keyfile, KeyPath}]} | Options],
    rpc:call(Node, hackney, request, [get, <<"https://", Host/binary, ":", Port/binary>>, [], [], FinalOptions]).

get_hostname(Node) ->
    Host = rpc:call(Node, oneprovider, get_node_hostname, []),
    list_to_binary(Host).