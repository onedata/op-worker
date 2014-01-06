%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests if control_panel module starts properly with the cluster,
%% by checking connection with an HTTP request.
%% @end
%% ===================================================================

-module(gui_test_SUITE).
-include("nodes_manager.hrl").
-include("registered_names.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([connection_test/1]).

all() -> [connection_test].

%% ====================================================================
%% Test functions
%% ====================================================================

%% Main test, starts a single-noded cluster and checks if control_panel 
%% listener is operational.
connection_test(Config) ->
    nodes_manager:check_start_assertions(Config),

    NodesUp = ?config(nodes, Config),
    [CCM | _] = NodesUp,

    gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
    gen_server:cast({global, ?CCM}, {set_monitoring, on}),
    gen_server:cast({global, ?CCM}, init_cluster),
    timer:sleep(500),

    {ok, Port} = rpc:call(CCM, application, get_env, [veil_cluster_node, control_panel_port]),

    ibrowse:start(),
    {_, Code, _, _} = ibrowse:send_req("https://localhost:" ++ integer_to_list(Port) , [], get),
    ?assertEqual(Code, "200").


%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(connection_test, Config) ->
    ?INIT_DIST_TEST,
    nodes_manager:start_deps_for_tester_node(),

    Nodes = nodes_manager:start_test_on_nodes(1),
    [Node1 | _] = Nodes,

    StartLog = nodes_manager:start_app_on_nodes(Nodes, 
        [[{node_type, ccm_test}, 
            {dispatcher_port, 5055}, 
            {ccm_nodes, [Node1]}, 
            {dns_port, 1308}]]),

    Assertions = [{false, lists:member(error, Nodes)}, {false, lists:member(error, StartLog)}],
    lists:append([{nodes, Nodes}, {assertions, Assertions}], Config).


end_per_testcase(connection_test, Config) ->
    Nodes = ?config(nodes, Config),
    StopLog = nodes_manager:stop_app_on_nodes(Nodes),
    StopAns = nodes_manager:stop_nodes(Nodes),
    ?assertEqual(false, lists:member(error, StopLog)),
    ?assertEqual(ok, StopAns).