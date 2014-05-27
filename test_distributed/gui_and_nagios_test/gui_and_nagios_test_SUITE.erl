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
-include("veil_modules/control_panel/connection_check_values.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("ctool/include/assertions.hrl").
-include_lib("ctool/include/test_node_starter.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([main_test/1]).

all() -> [main_test].

%% ====================================================================
%% Test functions
%% ====================================================================

%% Main test, it initializes cluster and runs all other tests
main_test(Config) ->
	%init
	nodes_manager:check_start_assertions(Config),
	NodesUp = ?config(nodes, Config),
	[CCM | _] = NodesUp,
	put(ccm, CCM),
	gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
	gen_server:cast({global, ?CCM}, {set_monitoring, on}),
	nodes_manager:wait_for_cluster_cast(),
	gen_server:cast({global, ?CCM}, init_cluster),
	nodes_manager:wait_for_cluster_init(),
	ibrowse:start(),

	%tests
	connection_test(),
    connection_check_test(),
	nagios_test(),

	%cleanup
	ibrowse:stop().

%% Checks if control_panel listener is operational.
connection_test() ->
    {ok, Port} = rpc:call(get(ccm), application, get_env, [veil_cluster_node, control_panel_port]),
    {_, Code, _, _} = ibrowse:send_req("https://localhost:" ++ integer_to_list(Port) , [], get),

    ?assertEqual("200", Code).

%% Checks if test callback returns "gui"
connection_check_test() ->
    {ok, Port} = rpc:call(get(ccm), application, get_env, [veil_cluster_node, control_panel_port]),
    {_, Code, _, Value} = ibrowse:send_req("https://localhost:" ++ integer_to_list(Port) ++ "/" ++binary_to_list(?connection_check_path), [], get),

    ?assertEqual("200",Code),
    ?assertEqual(binary_to_list(?gui_connection_check_value),Value).

%% Sends nagios request and check if health status is ok, and if health report contains information about all workers
nagios_test() ->
	{ok, Port} = rpc:call(get(ccm), application, get_env, [veil_cluster_node, control_panel_port]),
	NagiosUrl = "https://localhost:"++integer_to_list(Port)++"/nagios",
	{ok, Code, _RespHeaders, Response} = rpc:call(get(ccm),ibrowse,send_req, [NagiosUrl,[],get]),
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

init_per_testcase(main_test, Config) ->
    ?INIT_DIST_TEST,
    test_node_starter:start_deps_for_tester_node(),

    Nodes = test_node_starter:start_test_nodes(1),
    [Node1 | _] = Nodes,

	DB_Node = nodes_manager:get_db_node(),

	StartLog = nodes_manager:start_app_on_nodes(Nodes,
		[[{node_type, ccm_test},
			{dispatcher_port, 5055},
			{ccm_nodes, [Node1]},
			{dns_port, 1308},
      {heart_beat, 1},
			{db_nodes, [DB_Node]}]]),

    Assertions = [{false, lists:member(error, Nodes)}, {false, lists:member(error, StartLog)}],
    lists:append([{nodes, Nodes}, {assertions, Assertions}], Config).


end_per_testcase(main_test, Config) ->
    Nodes = ?config(nodes, Config),
    StopLog = nodes_manager:stop_app_on_nodes(Nodes),
    StopAns = test_node_starter:stop_test_nodes(Nodes),
    ?assertEqual(false, lists:member(error, StopLog)),
    ?assertEqual(ok, StopAns).