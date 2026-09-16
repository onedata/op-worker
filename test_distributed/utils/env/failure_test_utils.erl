%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utils used by onenv tests that emulate node failures and db errors: killing
%%% provider nodes and bringing them back up. Restarting rebuilds everything the
%%% test lost together with the node - mock manager, user sessions and lfm proxy
%%% handles - and returns an updated config, which the caller must use onwards.
%%% @end
%%%-------------------------------------------------------------------
-module(failure_test_utils).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([kill_nodes/2, restart_nodes/2]).

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

    % the mock manager died with the node, so it must be started anew - done by
    % calling the CT hook callback that normally does it after init_per_suite
    % (it takes the nodes from the config passed as its 3rd argument and ignores
    % the other two)
    cth_mock:post_init_per_suite(?MODULE, [], Config, []),

    lists:foreach(fun(Node) ->
        ?assertMatch({ok, _}, rpc:call(Node, provider_auth, get_provider_id, []), 180),
        ?assertEqual(true, rpc:call(Node, gs_channel_service, is_connected_and_initialized, []), 60)
    end, Nodes),

    UpdatedConfig = provider_test_utils:setup_sessions(proplists:delete(sess_id, Config)),
    lfm_proxy:init(UpdatedConfig, false, Nodes),
    oct_background:update_background_config(UpdatedConfig);
restart_nodes(Config, Node) ->
    restart_nodes(Config, [Node]).
