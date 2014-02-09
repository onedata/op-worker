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

-module(gui_and_nagios_test_SUITE).
-include("nodes_manager.hrl").
-include("registered_names.hrl").
-include_lib("xmerl/include/xmerl.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([connection_test/1,nagios_test/1]).

all() -> [connection_test,nagios_test].

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
    nodes_manager:wait_for_cluster_cast(),
    gen_server:cast({global, ?CCM}, init_cluster),
    nodes_manager:wait_for_cluster_init(),

    {ok, Port} = rpc:call(CCM, application, get_env, [veil_cluster_node, control_panel_port]),

    ibrowse:start(),
    {_, Code, _, _} = ibrowse:send_req("https://localhost:" ++ integer_to_list(Port) , [], get),
    ?assertEqual(Code, "200").

%% sends nagios request and check if health status is ok, and if health report contains information about all workers
nagios_test(Config) ->
	nodes_manager:check_start_assertions(Config),
	NodesUp = ?config(nodes, Config),
	[CCM | _] = NodesUp,

	gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
	gen_server:cast({global, ?CCM}, {set_monitoring, on}),
	nodes_manager:wait_for_cluster_cast(),
	gen_server:cast({global, ?CCM}, init_cluster),
	nodes_manager:wait_for_cluster_init(),

	ibrowse:start(),
	NagiosUrl = "https://localhost/nagios",
	{ok, Code, _RespHeaders, Response} = rpc:call(CCM,ibrowse,send_req, [NagiosUrl,[],get]),
	{Xml,_} = xmerl_scan:string(Response),
	{Workers, _} = gen_server:call({global, ?CCM}, get_workers, 1000),

	[MainStatus] = [X#xmlAttribute.value || X <- Xml#xmlElement.attributes, X#xmlAttribute.name==status],
	ClusterReport = [X || X <- Xml#xmlElement.content, X#xmlElement.name==veil_cluster_node],
	AssertWorkerInReport = fun ({_WorkerNode,WorkerName}) ->
		Report = [Y || X <- Xml#xmlElement.content, X#xmlElement.name==worker, Y <- X#xmlElement.attributes, Y#xmlAttribute.value==atom_to_list(WorkerName)],
		?assertNotEqual(Report,[]) %if it fails, probably worker doesn't handle 'healthcheck' callback
	end,

	lists:foreach(AssertWorkerInReport,Workers),
	?assertNotEqual(ClusterReport,[]),
	?assertEqual(MainStatus,"ok"),
	?assertEqual(Code,"200").


%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(_, Config) ->
    ?INIT_DIST_TEST,
    nodes_manager:start_deps_for_tester_node(),

    Nodes = nodes_manager:start_test_on_nodes(1),
    [Node1 | _] = Nodes,

	DB_Node = nodes_manager:get_db_node(),

	StartLog = nodes_manager:start_app_on_nodes(Nodes,
		[[{node_type, ccm_test},
			{dispatcher_port, 5055},
			{ccm_nodes, [Node1]},
			{dns_port, 1308},
			{db_nodes, [DB_Node]}]]),

    Assertions = [{false, lists:member(error, Nodes)}, {false, lists:member(error, StartLog)}],
    lists:append([{nodes, Nodes}, {assertions, Assertions}], Config).


end_per_testcase(_, Config) ->
    Nodes = ?config(nodes, Config),
    StopLog = nodes_manager:stop_app_on_nodes(Nodes),
    StopAns = nodes_manager:stop_nodes(Nodes),
    ?assertEqual(false, lists:member(error, StopLog)),
    ?assertEqual(ok, StopAns).