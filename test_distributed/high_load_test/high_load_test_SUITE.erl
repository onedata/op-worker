%% ===================================================================
%% @author Michal Sitko
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of dao.
%% It contains tests that base on ct.
%% @end
%% ===================================================================
-module(high_load_test_SUITE).

-include("nodes_manager.hrl").
-include("registered_names.hrl").

%% API
-export([main_test/1]).
-export([all/0, init_per_testcase/2, end_per_testcase/2]).

all() -> [main_test].

-define(ProtocolVersion, 1).

%% TODO - pamiętać o zbadaniu late answers
main_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),
  [CCM | _] = NodesUp,
  start_cluster(CCM),

  Pid = self(),
  TestFun = fun() ->
    spawn(fun() ->
      gen_server:call({?Dispatcher_Name, CCM}, {fslogic, ?ProtocolVersion, Pid, ping}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {dao, ?ProtocolVersion, Pid, ping}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {dns_worker, ?ProtocolVersion, Pid, ping}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {control_panel, ?ProtocolVersion, Pid, ping}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {remote_files_manager, ?ProtocolVersion, Pid, ping}, 500)
    end)
  end,

  TestRequestsNum = 100,
  for(1, TestRequestsNum, TestFun),

  Answers = count_answers(),
  ct:print("Answers ~p~n", [Answers]),
  ?assertEqual(5*TestRequestsNum, proplists:get_value(pong, Answers, 0)),

  ok.


%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(_, Config) ->
  ?INIT_DIST_TEST,
  nodes_manager:start_deps_for_tester_node(),

  NodesUp = nodes_manager:start_test_on_nodes(1),
  [Node1 | _] = NodesUp,

  DB_Node = nodes_manager:get_db_node(),
  Port = 6666,
  StartLog = nodes_manager:start_app_on_nodes(NodesUp, [[{node_type, ccm_test}, {dispatcher_port, Port}, {ccm_nodes, [Node1]}, {dns_port, 1317}, {db_nodes, [DB_Node]}]]),

  Assertions = [{false, lists:member(error, NodesUp)}, {false, lists:member(error, StartLog)}],
  lists:append([{port, Port}, {nodes, NodesUp}, {assertions, Assertions}], Config).

end_per_testcase(_, Config) ->
  Nodes = ?config(nodes, Config),
  StopLog = nodes_manager:stop_app_on_nodes(Nodes),
  StopAns = nodes_manager:stop_nodes(Nodes),
  nodes_manager:stop_deps_for_tester_node(),

  ?assertEqual(false, lists:member(error, StopLog)),
  ?assertEqual(ok, StopAns).

%% ====================================================================
%% Helper functions
%% ====================================================================

start_cluster(Node) ->
  gen_server:cast({?Node_Manager_Name, Node}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  nodes_manager:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  nodes_manager:wait_for_cluster_init().

for(N, N, F) -> [F()];
for(I, N, F) -> [F()|for(I+1, N, F)].

count_answers() ->
  count_answers([]).

count_answers(TmpAns) ->
  receive
    Msg ->
      NewCounter = proplists:get_value(Msg, TmpAns, 0) + 1,
      NewAns = [{Msg, NewCounter} | proplists:delete(Msg, TmpAns)],
      count_answers(NewAns)
  after 5000 ->
    TmpAns
  end.