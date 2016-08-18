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
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([
    provider_certs_are_published_on_registration/1,
    verify_fails_on_forged_certs/1,
    verify_succeeds_on_published_certs/1,
    verify_succeeds_on_republished_cert/1,
    certs_should_be_cached_after_successful_publishing/1]).

-define(NORMAL_CASES_NAMES, [
    provider_certs_are_published_on_registration,
    verify_fails_on_forged_certs,
    verify_succeeds_on_published_certs,
    verify_succeeds_on_republished_cert,
    certs_should_be_cached_after_successful_publishing
]).

-define(PERFORMANCE_CASES_NAMES, [
]).

all() -> ?ALL(?NORMAL_CASES_NAMES, ?PERFORMANCE_CASES_NAMES).


%%%===================================================================
%%% Test functions
%%%===================================================================

provider_certs_are_published_on_registration(Config) ->
    %% given
    [WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),
    register_provider([WorkerP1]),
    register_provider([WorkerP2]),
    CertP1 = read_cert(WorkerP1),
    CertP2 = read_cert(WorkerP2),

    %% when
    Res1 = verify(WorkerP1, CertP1),
    Res2 = verify(WorkerP2, CertP1),
    Res3 = verify(WorkerP1, CertP2),
    Res4 = verify(WorkerP2, CertP2),

    %% then
    ?assertMatch(ok, Res1),
    ?assertMatch(ok, Res2),
    ?assertMatch(ok, Res3),
    ?assertMatch(ok, Res4).

verify_fails_on_forged_certs(Config) ->
    %% given
    [WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),
    register_provider([WorkerP1]),
    register_provider([WorkerP2]),
    ID1 = get_id(WorkerP1),
    ID2 = get_id(WorkerP2),
    ID3 = <<"verify_fails_on_forged_certs">>,
    Cert1 = new_self_signed_cert(ID1),
    Cert2 = new_self_signed_cert(ID2),
    Cert3 = new_self_signed_cert(ID3),

    %% when
    Res1 = verify(WorkerP1, Cert1),
    Res2 = verify(WorkerP1, Cert2),
    Res3 = verify(WorkerP1, Cert3),
    Res4 = verify(WorkerP2, Cert1),
    Res5 = verify(WorkerP2, Cert2),
    Res6 = verify(WorkerP2, Cert3),

    %% then
    ?assertMatch({error, key_does_not_match}, Res1),
    ?assertMatch({error, key_does_not_match}, Res2),
    ?assertMatch({error, _}, Res3),
    ?assertMatch({error, key_does_not_match}, Res4),
    ?assertMatch({error, key_does_not_match}, Res5),
    ?assertMatch({error, _}, Res6).

verify_succeeds_on_published_certs(Config) ->
    %% given
    [WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),
    ID1 = <<"1@verify_succeeds_on_published_certs">>,
    ID2 = <<"2@verify_succeeds_on_published_certs">>,
    PublishedCert = new_self_signed_cert(ID1),
    NonPublishedCert = new_self_signed_cert(ID2),
    ForgedCert = new_self_signed_cert(ID1),
    publish(WorkerP1, PublishedCert),

    %% when
    Res1 = verify(WorkerP1, PublishedCert),
    Res2 = verify(WorkerP1, NonPublishedCert),
    Res3 = verify(WorkerP1, ForgedCert),
    Res4 = verify(WorkerP2, PublishedCert),
    Res5 = verify(WorkerP2, NonPublishedCert),
    Res6 = verify(WorkerP2, ForgedCert),

    %% then
    ?assertMatch(ok, Res1),
    ?assertMatch({error, _}, Res2),
    ?assertMatch({error, key_does_not_match}, Res3),
    ?assertMatch(ok, Res4),
    ?assertMatch({error, _}, Res5),
    ?assertMatch({error, key_does_not_match}, Res6).

verify_succeeds_on_republished_cert(Config) ->
    %% given
    [WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),
    ID1 = <<"verify_succeeds_on_republished_cert">>,
    Cert = new_self_signed_cert(ID1),
    UpdatedCert = new_self_signed_cert(ID1),
    publish(WorkerP1, Cert),
    ?assertMatch(ok, verify(WorkerP1, Cert)),
    ?assertMatch(ok, verify(WorkerP2, Cert)),

    %% when
    publish(WorkerP1, UpdatedCert),
    Res1 = verify(WorkerP1, UpdatedCert),
    Res2 = verify(WorkerP2, UpdatedCert),

    %% then
    ?assertMatch(ok, Res1),
    ?assertMatch(ok, Res2).

certs_should_be_cached_after_successful_publishing(Config) ->
    %% given
    [WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),
    ID1 = <<"1@certs_should_be_cached_after_successful_publishing">>,
    ID2 = <<"2@certs_should_be_cached_after_successful_publishing">>,
    ID3 = <<"3@certs_should_be_cached_after_successful_publishing">>,
    Cert1 = new_self_signed_cert(ID1),
    Cert2 = new_self_signed_cert(ID2),
    Cert3 = new_self_signed_cert(ID3),

    %% when
    publish(WorkerP1, Cert1),
    publish(WorkerP2, Cert2),
    publish(WorkerP1, Cert3),

    %% then
    Res1 = rpc:call(WorkerP1, plugins, apply, [identity_cache, get, [ID1]]),
    Res2 = rpc:call(WorkerP1, plugins, apply, [identity_cache, get, [ID2]]),
    Res3 = rpc:call(WorkerP1, plugins, apply, [identity_cache, get, [ID3]]),
    Res4 = rpc:call(WorkerP2, plugins, apply, [identity_cache, get, [ID1]]),
    Res5 = rpc:call(WorkerP2, plugins, apply, [identity_cache, get, [ID2]]),
    Res6 = rpc:call(WorkerP2, plugins, apply, [identity_cache, get, [ID3]]),

    EncodedPK1 = identity_utils:encode(identity_utils:get_public_key(Cert1)),
    EncodedPK2 = identity_utils:encode(identity_utils:get_public_key(Cert2)),
    EncodedPK3 = identity_utils:encode(identity_utils:get_public_key(Cert3)),

    ?assertMatch({ok, EncodedPK1}, Res1),
    ?assertMatch({error, _}, Res2),
    ?assertMatch({ok, EncodedPK3}, Res3),
    ?assertMatch({error, _}, Res4),
    ?assertMatch({ok, EncodedPK2}, Res5),
    ?assertMatch({error, _}, Res6).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), []).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

register_provider(Workers) ->
    ?assertMatch({ok, _}, rpc:call(hd(Workers), oneprovider, register_provider_in_oz, [Workers])).

publish(Worker, Cert) ->
    rpc:call(Worker, identity, publish, [Cert]).

verify(Worker, Cert) ->
    rpc:call(Worker, identity, verify, [Cert]).

read_cert(Worker) ->
    {ok, IdentityCertFile} = rpc:call(Worker, application, get_env, [?APP_NAME, identity_cert_file]),
    rpc:call(Worker, identity_utils, read_cert, [IdentityCertFile]).

get_id(Worker) ->
    identity_utils:get_id(read_cert(Worker)).

new_self_signed_cert(ID) ->
    TmpDir = utils:mkdtemp(),
    KeyFile = TmpDir ++ "/key.pem",
    CertFile = TmpDir ++ "/cert.pem",
    PassFile = TmpDir ++ "/pass",
    CSRFile = TmpDir ++ "/csr",
    DomainForCN = binary_to_list(ID),

    os:cmd(["openssl genrsa", " -des3 ", " -passout ", " pass:x ", " -out ", PassFile, " 2048 "]),
    os:cmd(["openssl rsa", " -passin ", " pass:x ", " -in ", PassFile, " -out ", KeyFile]),
    os:cmd(["openssl req", " -new ", " -key ", KeyFile, " -out ", CSRFile, " -subj ", "\"/CN=" ++ DomainForCN ++ "\""]),
    os:cmd(["openssl x509", " -req ", " -days ", " 365 ", " -in ", CSRFile, " -signkey ", KeyFile, " -out ", CertFile]),

    Cert = identity_utils:read_cert(CertFile),
    utils:rmtempdir(TmpDir),
    Cert.