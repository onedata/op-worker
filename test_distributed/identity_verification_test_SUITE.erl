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
    verify_succeeds_on_republished_cert/1,
    certs_should_be_cached_after_successful_publishing/1]).

-define(NORMAL_CASES_NAMES, [
    provider_certs_are_published_on_registration,
    verify_fails_on_forged_certs,
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
    ?assertMatch({error, key_does_not_match}, Res3),
    ?assertMatch({error, key_does_not_match}, Res4),
    ?assertMatch({error, key_does_not_match}, Res5),
    ?assertMatch({error, key_does_not_match}, Res6).

verify_succeeds_on_republished_cert(Config) ->
    %% given
    [WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),
    register_provider([WorkerP1]),
    register_provider([WorkerP2]),
    UpdatedCert1 = new_self_signed_cert(get_id(WorkerP1)),
    UpdatedCert2 = new_self_signed_cert(get_id(WorkerP2)),

    %% when
    update(WorkerP1, UpdatedCert1),
    update(WorkerP2, UpdatedCert2),

    %% then
    ?assertMatch(ok, verify(WorkerP1, UpdatedCert1)),
    ?assertMatch(ok, verify(WorkerP2, UpdatedCert1)),
    ?assertMatch(ok, verify(WorkerP1, UpdatedCert2)),
    ?assertMatch(ok, verify(WorkerP2, UpdatedCert2)).

certs_should_be_cached_after_successful_publishing(Config) ->
    %% given
    [WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),
    ID1 = get_id(WorkerP1),
    ID2 = get_id(WorkerP2),
    register_provider([WorkerP1]),
    register_provider([WorkerP2]),
    UpdatedCert1 = new_self_signed_cert(ID1),
    UpdatedCert2 = new_self_signed_cert(ID2),

    %% when
    update(WorkerP1, UpdatedCert1),
    update(WorkerP2, UpdatedCert2),

    %% then
    Res1 = rpc:call(WorkerP1, plugins, apply, [identity_cache, get, [ID1]]),
    Res2 = rpc:call(WorkerP1, plugins, apply, [identity_cache, get, [ID2]]),
    Res3 = rpc:call(WorkerP2, plugins, apply, [identity_cache, get, [ID1]]),
    Res4 = rpc:call(WorkerP2, plugins, apply, [identity_cache, get, [ID2]]),

    EncodedPK1 = identity_utils:encode(identity_utils:get_public_key(UpdatedCert1)),
    EncodedPK2 = identity_utils:encode(identity_utils:get_public_key(UpdatedCert2)),

    ?assertMatch({ok, EncodedPK1}, Res1),
    ?assertMatch({error, _}, Res2),
    ?assertMatch({error, _}, Res3),
    ?assertMatch({ok, EncodedPK2}, Res4).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(RunConfig) ->
    Config = ?TEST_INIT(RunConfig, ?TEST_FILE(RunConfig, "env_desc.json"), []),

    %% ensure some provider certs present as ctool requires them
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        {ok, Key} = rpc:call(Worker, application, get_env, [?APP_NAME, oz_provider_key_path]),
        {ok, Cert} = rpc:call(Worker, application, get_env, [?APP_NAME, oz_provider_cert_path]),
        Domain = rpc:call(Worker, oneprovider, get_provider_domain, []),
        ok = rpc:call(Worker, identity_utils, ensure_synced_cert_present, [Key, Cert, Domain])
    end, Workers),

    Config.


end_per_suite(Config) ->
    ?TEST_STOP(Config).

init_per_testcase(Case, Config) ->
    ?CASE_START(Case),
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),

    %% clear OZ state - this key is defined in appmock
    %% state of actual OZ cannot be cleared like this
    rpc:call(Worker, oz_identities, get_public_key, [provider, <<"special-clear-state-key">>]),

    %% clear caches
    lists:foreach(fun(W) ->
        {ok, Docs} = rpc:call(W, cached_identity, list, []),
        lists:foreach(fun(#document{key = ID}) ->
            rpc:call(W, cached_identity, delete, [ID])
        end, Docs)
    end, Workers),

    Config.

end_per_testcase(Case, _Config) ->
    ?CASE_STOP(Case),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

register_provider(Workers) ->
    ?assertMatch({ok, _}, rpc:call(hd(Workers), oneprovider, register_provider_in_oz, [Workers])).

update(Worker, Cert) ->
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