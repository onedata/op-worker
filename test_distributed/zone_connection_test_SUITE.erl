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

-include("http/http_common.hrl").
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
% compatible_oz_versions env variables are defined using env_desc.json
incompatible_zone_should_not_connect(Config) ->
    % When provider node goes down it will send {'EXIT',noconnection}
    % To avoid going down by test master it should trap exits
    process_flag(trap_exit, true),
    [P1, P2] = ?config(op_worker_nodes, Config),
    ?assertMatch(true, is_down(P1), 150),
    ?assertMatch(true, is_alive(P2)),
    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

% Mack oz endpoint returning its versions and for one of providers
% set incorrect compatible_oz_versions env var so it should go down
init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        Workers = [P1 | _] = ?config(op_worker_nodes, NewConfig),
        rpc:call(P1, application, set_env, [
            ?APP_NAME, compatible_oz_versions, ["16.04-rc5"]
        ]),
        % Mock OZ version
        {_AppId, _AppName, AppVersion} = lists:keyfind(
            ?APP_NAME, 1, rpc:call(hd(Workers), application, loaded_applications, [])
        ),
        ZoneDomain = rpc:call(hd(Workers), oneprovider, get_oz_domain, []),
        ZoneConfigurationURL = str_utils:format("https://~s~s", [
            ZoneDomain, ?zone_configuration_path
        ]),
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
        NewConfig
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(_Config) ->
    ok.


init_per_testcase(_Case, Config) ->
    % Try to init env to ensure op will make attempt to connect to oz
    % but, while connecting, op will go down (due to oz incompatible version)
    % so init will fail
    try
        initializer:mock_provider_ids(Config),
        Config
    catch
        _:_ -> Config
    end.


end_per_testcase(_Case, _Config) ->
    % Because one of providers have died (due to incompatible version with oz)
    % cleanup will fail, so skip it
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

is_alive(Provider) ->
    not is_down(Provider).

is_down(Provider) ->
    case net_adm:ping(Provider) of
        pong -> false;
        pang -> true
    end.
