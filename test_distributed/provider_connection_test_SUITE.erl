%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Provider connection tests
%%% @end
%%%-------------------------------------------------------------------
-module(provider_connection_test_SUITE).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("http/rest/cdmi/cdmi_capabilities.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    incompatible_providers_should_not_connect/1
]).

all() ->
    ?ALL([
        incompatible_providers_should_not_connect
    ]).


%%%===================================================================
%%% Test functions
%%%===================================================================


% Providers should not connect because incorrect supported_op_versions
% env variables are defined using env_desc.json
incompatible_providers_should_not_connect(Config) ->
    % providers should start connecting right after init_per_testcase
    % (spaces creation), alas wait some time before checking that connection failed
    timer:sleep(20 * 1000),

    Workers = [P1, P2] = ?config(op_worker_nodes, Config),
    ?assertMatch(false, connection_exists(P1, P2), 10),
    ?assertMatch(false, connection_exists(P2, P1), 10),

    {_AppId, _AppName, AppVersion} = lists:keyfind(
        ?APP_NAME, 1, rpc:call(hd(Workers), application, loaded_applications, [])
    ),
    rpc:multicall(Workers, application, set_env, [
        ?APP_NAME, supported_op_versions, [AppVersion]
    ]),
    ?assertMatch(true, connection_exists(P1, P2), 60),
    ?assertMatch(true, connection_exists(P2, P1), 60),

    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(Config) ->
    initializer:teardown_storage(Config).


init_per_testcase(_Case, Config) ->
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    initializer:enable_grpca_based_communication(Config),
    lfm_proxy:init(ConfigWithSessionInfo).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
     %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:disable_grpca_based_communication(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


connection_exists(Provider, PeerProvider) ->
    PeerProviderId = initializer:domain_to_provider_id(?GET_DOMAIN(PeerProvider)),
    IncomingSessId = rpc:call(Provider, session_manager, get_provider_session_id,
        [incoming, PeerProviderId]
    ),
    OutgoingSessId = rpc:call(Provider, session_manager, get_provider_session_id,
        [outgoing, PeerProviderId]
    ),
    session_exists(Provider, IncomingSessId)
        orelse session_exists(Provider, OutgoingSessId).


session_exists(Provider, SessId) ->
    rpc:call(Provider, provider_communicator, session_exists, [SessId]).
