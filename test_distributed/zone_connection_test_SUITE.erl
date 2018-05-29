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
    incompatible_zone_should_not_connect/1
]).

all() ->
    ?ALL([
        incompatible_zone_should_not_connect
    ]).


%%%===================================================================
%%% Test functions
%%%===================================================================

% One of providers should not connect (and go down) because incorrect
% compatible_oz_versions env variables are defined in init_per_testcase.
incompatible_zone_should_not_connect(Config) ->
    [P1, P2] = ?config(op_worker_nodes, Config),
    ?assertMatch(true, is_down(P1), 180),
    ?assertMatch(true, is_alive(P2)),
    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

% Mock oz endpoint returning its versions and for one of providers
% set incorrect compatible_oz_versions env var so it should go down
init_per_suite(Config) ->
    [{?LOAD_MODULES, [initializer]} | Config].


init_per_testcase(_Case, Config) ->
    Workers = [P1 | _] = ?config(op_worker_nodes, Config),
    rpc:call(P1, application, set_env, [
        ?APP_NAME, compatible_oz_versions, ["16.04-rc5"]
    ]),
    % Mock OZ version
    {_AppId, _AppName, AppVersion} = lists:keyfind(
        ?APP_NAME, 1, rpc:call(hd(Workers), application, loaded_applications, [])
    ),
    ZoneDomain = rpc:call(hd(Workers), oneprovider, get_oz_domain, []),
    ZoneConfigurationURL = str_utils:format("https://~s~s", [
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
                        <<"version">> => list_to_binary(AppVersion),
                        <<"compatibleOneproviderVersions">> => [
                            % Return some random versions that will surely
                            % not match provider's version.
                            <<"13.04-rc2">>, <<"13.04-rc7">>
                        ]
                    })};
                _ ->
                    meck:passthrough([Url, Headers, Body, Options])
            end
        end),
    initializer:mock_provider_ids(Config),
    Config.


end_per_testcase(_Case, _Config) ->
    % Because one of the providers has died (due to incompatible version with
    % oz) cleanup would fail, so skip it
    ok.


end_per_suite(_Config) ->
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

is_alive(Provider) ->
    not is_down(Provider).

is_down(Provider) ->
    case rpc:call(Provider, erlang, node, []) of
        {badrpc, nodedown} -> true;
        _ -> false
    end.
