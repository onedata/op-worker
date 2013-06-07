%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of cluster_manager.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================

-module(cluster_manager_tests).
-include("registered_names.hrl").
-include("records.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

%% ====================================================================
%% Test setup and teardown
%% ====================================================================

setup() ->
  net_kernel:start([node1, shortnames]),
  ssl:start(),
  ok = application:start(ranch).

teardown(_Args) ->
  ok = application:stop(ranch),
  ok = application:stop(ssl),
  net_kernel:stop().

%% ====================================================================
%% Test generation
%% ====================================================================

generate_test_() ->
  {setup,
    fun setup/0,
    fun teardown/1,
    [?_test(env()),
      ?_test(wrong_request()),
      ?_test(nodes_counting_and_monitoring()),
      ?_test(worker_start_stop()),
      ?_test(modules_start_and_ping())]}.

%% ====================================================================
%% Functions used by tests
%% ====================================================================

%% This test checks if all environment variables needed by ccm are defined.
env() ->
	ok = application:start(?APP_Name),
	{ok, _InitTime} = application:get_env(?APP_Name, initialization_time),
	{ok, _Period} = application:get_env(?APP_Name, cluster_clontrol_period),
	ok = application:stop(?APP_Name).

%% This test checks if ccm is resistant to incorrect requests.
wrong_request() ->
	application:set_env(?APP_Name, node_type, ccm),
	application:set_env(?APP_Name, ccm_nodes, [node()]), 

	ok = application:start(?APP_Name),

	gen_server:cast({global, ?CCM}, abc),
	Reply = gen_server:call({global, ?CCM}, abc),
	?assert(Reply =:= wrong_request),
	
	ok = application:stop(?APP_Name).

%% This test checks if ccm properly registers nodes in cluster.
%% Furthermore, it checks if it properly monitors state of these nodes.
nodes_counting_and_monitoring() ->


	application:set_env(?APP_Name, node_type, ccm), 
	application:set_env(?APP_Name, ccm_nodes, [node()]), 

	ok = application:start(?APP_Name),

	Nodes = [n1, n2, n3],
	gen_server:cast({global, ?CCM}, {set_monitoring, off}),
	timer:sleep(10),
	lists:foreach(fun(Node) -> gen_server:call({global, ?CCM}, {node_is_up, Node}) end, Nodes),

  %% the test will be used when distributed test environment will be ready
%% 	Nodes2 = gen_server:call({global, ?CCM}, get_nodes),
%% 	?assert(length(Nodes) + 1 == length(Nodes2)),
%% 	lists:foreach(fun(Node) -> ?assert(lists:member(Node, Nodes2)) end, Nodes),
%% 	?assert(lists:member(node(), Nodes2)),
%%
%% 	gen_server:call({global, ?CCM}, {node_is_up, n2}),
%% 	gen_server:call({global, ?CCM}, {node_is_up, n1}),
%% 	Nodes3 = gen_server:call({global, ?CCM}, get_nodes),
%% 	?assert(length(Nodes) + 1 == length(Nodes3)),
	
	gen_server:cast({global, ?CCM}, {set_monitoring, on}),
	timer:sleep(10),
	Nodes4 = gen_server:call({global, ?CCM}, get_nodes),
	?assert(length(Nodes4) == 1),

	ok = application:stop(?APP_Name).

%% This test checks if ccm is able to start and stop workers.
worker_start_stop() ->
	application:set_env(?APP_Name, node_type, ccm),
	application:set_env(?APP_Name, ccm_nodes, [node()]), 

	ok = application:start(?APP_Name),

	State = gen_server:call({global, ?CCM}, get_state),
	?assert(length(State#cm_state.workers) == 0),

	Module = sample_plug_in,
	{ok, NewState} = cluster_manager:start_worker(node(), Module, [], State),
	?assert(length(NewState#cm_state.workers) == 1),

	{ok, NewState2} = cluster_manager:stop_worker(node(), Module, NewState),
	?assert(length(NewState2#cm_state.workers) == 0),

	ok = application:stop(?APP_Name).

%% This test checks if cluster manager correctly stars workers and if
%% the can be used inside cluster (using ping request).
modules_start_and_ping() ->
  Jobs = [cluster_rengine, control_panel, dao, fslogic, gateway, rtransfer, rule_manager, central_logger],

  application:set_env(?APP_Name, node_type, ccm),
  application:set_env(?APP_Name, ccm_nodes, [node()]),
  application:set_env(?APP_Name, initialization_time, 1),

  ok = application:start(?APP_Name),
  StateNum0 = gen_server:call({global, ?CCM}, get_state_num),
  ?assert(StateNum0 == 1),

  timer:sleep(300),
  State = gen_server:call({global, ?CCM}, get_state),
  Workers = State#cm_state.workers,
  ?assert(length(Workers) == 2),
  StateNum1 = gen_server:call({global, ?CCM}, get_state_num),
  ?assert(StateNum1 == 2),

  timer:sleep(1000),
  State2 = gen_server:call({global, ?CCM}, get_state),
  Workers2 = State2#cm_state.workers,
  ?assert(length(Workers2) == length(Jobs)),
  StateNum2 = gen_server:call({global, ?CCM}, get_state_num),
  ?assert(StateNum2 == 3),

  ProtocolVersion = 1,
  CheckModules = fun(M, Sum) ->
    Ans = gen_server:call(M, {test_call, ProtocolVersion, ping}),
    case Ans of
      pong -> Sum + 1;
      _Other -> Sum
    end
  end,
  PongsNum = lists:foldl(CheckModules, 0, Jobs),
  ?assert(PongsNum == length(Jobs)),

  ok = application:stop(?APP_Name).

-endif.