%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Provider to Zone connection tests
%%% @end
%%%-------------------------------------------------------------------
-module(zone_connection_test_SUITE).
-author("Bartosz Walkowicz").

-include("http/gui_paths.hrl").
-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/onedata.hrl").
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
    oneprovider_should_not_connect_to_incompatible_onezone/1
]).

all() ->
    ?ALL([
        oneprovider_should_not_connect_to_incompatible_onezone
    ]).


%%%===================================================================
%%% Test functions
%%%===================================================================

oneprovider_should_not_connect_to_incompatible_onezone(Config) ->
    [P1Worker, P2Worker] = ?config(op_worker_nodes, Config),
    timer:sleep(15),
    % One of providers should not connect because of mocked compatibility
    % check function in init_per_testcase.
    ?assertMatch(false, is_connected_to_oz(P1Worker), 30),
    ?assertMatch(true, is_connected_to_oz(P2Worker), 30).


is_connected_to_oz(Worker) ->
    Domain = ?GET_DOMAIN(Worker),
    Url = str_utils:format_bin("https://~s~s", [Domain, ?NAGIOS_OZ_CONNECTIVITY_PATH]),
    CaCerts = rpc:call(Worker, https_listener, get_cert_chain_pems, []),
    {ok, Domain} = test_utils:get_env(Worker, ?APP_NAME, test_web_cert_domain),
    Opts = [{ssl_options, [{cacerts, CaCerts}, {hostname, str_utils:to_binary(Domain)}]}],
    case http_client:get(Url, #{}, <<>>, Opts) of
        {ok, 200, _, Body} ->
            case json_utils:decode(Body) of
                #{<<"status">> := <<"ok">>} -> true;
                #{<<"status">> := <<"error">>} -> false
            end;
        _ ->
            error
    end.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

% Mock oz endpoint returning its versions and for one of providers
% mock compatibility check function so it should go down
init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        ssl:start(),
        hackney:start(),
        NewConfig
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


init_per_testcase(_Case, Config) ->
    Workers = [P1Worker | _] = ?config(op_worker_nodes, Config),
    % Mock OZ version
    {_AppId, _AppName, AppVersion} = lists:keyfind(
        ?APP_NAME, 1, rpc:call(hd(Workers), application, loaded_applications, [])
    ),
    test_utils:mock_new(P1Worker, compatibility),
    test_utils:mock_expect(P1Worker, compatibility, check_products_compatibility,
        fun(?ONEPROVIDER, _, ?ONEZONE, _) -> {false, []};
            (S1, V1, S2, V2) -> meck:passthrough([S1, V1, S2, V2])
        end),
    ZoneDomain = rpc:call(hd(Workers), oneprovider, get_oz_domain, []),
    ZoneConfigurationURL = str_utils:format_bin("https://~s~s", [
        ZoneDomain, ?ZONE_CONFIGURATION_PATH
    ]),
    % Sleep a while before mocking http_client - otherwise meck's reloading
    % and purging the module can cause the op-worker application to crash.
    timer:sleep(5000),
    ok = test_utils:mock_new(Workers, http_client, [passthrough]),
    ok = test_utils:mock_expect(Workers, http_client, get,
        fun(Url, Headers, Body, Options) ->
            case Url of
                ZoneConfigurationURL ->
                    {ok, 200, #{}, json_utils:encode(#{
                        <<"version">> => list_to_binary(AppVersion)
                    })};
                _ ->
                    meck:passthrough([Url, Headers, Body, Options])
            end
        end),
    initializer:mock_provider_ids(Config),
    Config.


end_per_testcase(_, Config) ->
    [P1Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(P1Worker, compatibility).


end_per_suite(_Config) ->
    ssl:stop(),
    hackney:stop().
