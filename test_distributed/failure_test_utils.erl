%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utils used by ct_onenv tests that emulate node failures and db errors.
%%% @end
%%%-------------------------------------------------------------------
-module(failure_test_utils).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([kill_nodes/2, restart_nodes/2]).
%% SetUp and TearDown helpers
-export([init_per_suite/2]).

%%%===================================================================
%%% API
%%%===================================================================

kill_nodes(_Config, []) ->
    ok;
kill_nodes(Config, [Node | Nodes]) ->
    kill_nodes(Config, Node),
    kill_nodes(Config, Nodes);
kill_nodes(Config, Node) ->
    ok = oct_environment:kill_node(Config, Node),
    ?assertEqual({badrpc, nodedown}, rpc:call(Node, oneprovider, get_id, []), 10).

restart_nodes(Config, Nodes) when is_list(Nodes) ->
    lists:foreach(fun(Node) ->
        ok = oct_environment:start_node(Config, Node)
    end, Nodes),

    lists:foreach(fun(Node) ->
        ?assertMatch({ok, _}, rpc:call(Node, provider_auth, get_provider_id, []), 180),
        {ok, _} = rpc:call(Node, mock_manager, start, []),
        ?assertEqual(true, rpc:call(Node, gs_channel_service, is_connected, []), 60)
    end, Nodes),

    UpdatedConfig = provider_onenv_test_utils:setup_sessions(proplists:delete(sess_id, Config)),
    lfm_proxy:init(UpdatedConfig, false, Nodes),
    oct_background:update_background_config(UpdatedConfig);
restart_nodes(Config, Node) ->
    restart_nodes(Config, [Node]).

%%%===================================================================
%%% SetUp and TearDown helpers
%%%===================================================================

init_per_suite(Config, Scenario) ->
    Posthook = fun(NewConfig) ->
        provider_onenv_test_utils:initialize(NewConfig)
    end,
    test_config:set_many(Config, [
        {set_onenv_scenario, [Scenario]}, % name of yaml file in test_distributed/onenv_scenarios
        {set_posthook, Posthook}
    ]).