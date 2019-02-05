%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
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
    incompatible_providers_should_not_connect/1,
    deprecated_configuration_endpoint_is_served/1,
    configuration_endpoints_give_same_results/1
]).

all() ->
    ?ALL([
        incompatible_providers_should_not_connect,
        deprecated_configuration_endpoint_is_served,
        configuration_endpoints_give_same_results
    ]).


%%%===================================================================
%%% Test functions
%%%===================================================================


% Providers should not connect because incorrect compatible_op_versions
% env variables are defined using in env_up_posthook
incompatible_providers_should_not_connect(Config) ->
    % providers should start connecting right after init_per_testcase
    % (spaces creation); just in case wait some time before checking
    % that connection failed
    timer:sleep(30 * 1000),

    Workers = [P1, P2] = ?config(op_worker_nodes, Config),
    ?assertMatch(false, connection_exists(P1, P2)),
    ?assertMatch(false, connection_exists(P2, P1)),

    {_AppId, _AppName, AppVersion} = lists:keyfind(
        ?APP_NAME, 1, rpc:call(hd(Workers), application, loaded_applications, [])
    ),
    rpc:multicall(Workers, application, set_env, [
        ?APP_NAME, compatible_op_versions, [AppVersion]
    ]),
    ?assertMatch(true, connection_exists(P1, P2), 90),
    ?assertMatch(true, connection_exists(P2, P1), 90),

    ok.


deprecated_configuration_endpoint_is_served(Config) ->
    Nodes = ?config(op_worker_nodes, Config),

    lists:foreach(fun(Node) ->
        ExpectedConfiguration = expected_configuration(Node),
        URL = str_utils:format("https://~s/configuration/",
            [maps:get(<<"domain">>, ExpectedConfiguration)]),
        {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
            http_client:get(URL, #{}, <<>>, [{ssl_options, [{secure, false}]}])),
        ?assertMatch(ExpectedConfiguration, json_utils:decode(Body))
    end, Nodes).


configuration_endpoints_give_same_results(Config) ->
    Nodes = ?config(op_worker_nodes, Config),

    lists:foreach(fun(Node) ->
        {ok, Domain} = rpc:call(Node, provider_logic, get_domain, []),
        OldURL = str_utils:format("https://~s/configuration", [Domain]),
        NewURL = str_utils:format("https://~s/api/v3/oneprovider/configuration", [Domain]),

        {_, _, _, OldBody} = ?assertMatch({ok, 200, _, _},
            http_client:get(OldURL, #{}, <<>>, [{ssl_options, [{secure, false}]}])),
        {_, _, _, NewBody} = ?assertMatch({ok, 200, _, _},
            http_client:get(NewURL, #{}, <<>>, [{ssl_options, [{secure, false}]}])),
        ?assertEqual(json_utils:decode(OldBody), json_utils:decode(NewBody))
    end, Nodes).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        Nodes = ?config(op_worker_nodes, NewConfig),
        rpc:multicall(Nodes, application, set_env, [
            ?APP_NAME, compatible_op_versions, ["16.04-rc5"]
        ]),
        NewConfig
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) when
    Case == deprecated_configuration_endpoint_is_served;
    Case == configuration_endpoints_give_same_results ->
    ssl:start(),
    hackney:start(),
    init_per_testcase(default, Config);

init_per_testcase(_Case, Config) ->
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).


end_per_testcase(Case, Config) when
    Case == deprecated_configuration_endpoint_is_served;
    Case == configuration_endpoints_give_same_results ->
    hackney:stop(),
    ssl:stop(),
    end_per_testcase(default, Config);

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
     %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


connection_exists(Provider, PeerProvider) ->
    PeerProviderId = initializer:domain_to_provider_id(?GET_DOMAIN(PeerProvider)),
    IncomingSessId = get_provider_session_id(Provider, incoming, PeerProviderId),
    OutgoingSessId = get_provider_session_id(Provider, outgoing, PeerProviderId),
    session_exists(Provider, IncomingSessId)
        orelse session_exists(Provider, OutgoingSessId).


get_provider_session_id(Worker, Type, ProviderId) ->
    rpc:call(Worker, session_utils, get_provider_session_id,
        [Type, ProviderId]
    ).


session_exists(Provider, SessId) ->
    rpc:call(Provider, session, exists, [SessId]).


-spec expected_configuration(node()) -> #{binary() := binary() | [binary()]}.
expected_configuration(Node) ->
    {ok, CompOzVersions} = test_utils:get_env(Node, ?APP_NAME, compatible_oz_versions),
    {ok, CompOpVersions} = test_utils:get_env(Node, ?APP_NAME, compatible_op_versions),
    {ok, CompOcVersions} = test_utils:get_env(Node, ?APP_NAME, compatible_oc_versions),
    {ok, Name} = rpc:call(Node, provider_logic, get_name, []),
    {ok, Domain} = rpc:call(Node, provider_logic, get_domain, []),

    #{
        <<"providerId">> => rpc:call(Node, oneprovider, get_id_or_undefined, []),
        <<"name">> => Name,
        <<"domain">> => Domain,
        <<"onezoneDomain">> => list_to_binary(rpc:call(Node, oneprovider, get_oz_domain, [])),
        <<"version">> => rpc:call(Node, oneprovider, get_version, []),
        <<"build">> => rpc:call(Node, oneprovider, get_build, []),
        <<"compatibleOnezoneVersions">> => to_binaries(CompOzVersions),
        <<"compatibleOneproviderVersions">> => to_binaries(CompOpVersions),
        <<"compatibleOneclientVersions">> => to_binaries(CompOcVersions)
    }.


-spec to_binaries([string()]) -> [binary()].
to_binaries(Strings) ->
    [list_to_binary(S) || S <- Strings].
