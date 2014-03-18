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
-export([main_test/1, sub_proc_load_test/1]).
-export([all/0, init_per_testcase/2, end_per_testcase/2]).

%% export nodes' codes
-export([ccm_code1/0, ccm_code2/0, worker_code/0]).

all() -> [main_test, sub_proc_load_test].

-define(ProtocolVersion, 1).

%% ====================================================================
%% Code of nodes used during the test
%% ====================================================================

ccm_code1() ->
  gen_server:cast(?Node_Manager_Name, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  ok.

ccm_code2() ->
  gen_server:cast({global, ?CCM}, init_cluster),
  ok.

worker_code() ->
  gen_server:cast(?Node_Manager_Name, do_heart_beat),
  ok.

%% ====================================================================
%% Test function
%% ====================================================================

%% TODO - pamiętać o zbadaniu late answers
%% TODO - sprawdzić wszystkie gen_server:call (czy zawsze mamy ustawione timeouty i catche), cast, receive (czy nie mamy late answer), spawny (czy mamy wiedzę jak proces się wywala), logowanie, scalić proces monitorujący w ccmie
main_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),
  [CCM | _] = NodesUp,
  start_cluster(CCM),

  Pid = self(),
  TestFun = fun() ->
    ?assert(is_pid(spawn_link(fun() ->
      try
        ?assertEqual(ok, gen_server:call({?Dispatcher_Name, CCM}, {fslogic, ?ProtocolVersion, Pid, ping}, 500)),
        ?assertEqual(ok, gen_server:call({?Dispatcher_Name, CCM}, {dao, ?ProtocolVersion, Pid, ping}, 500)),
        ?assertEqual(ok, gen_server:call({?Dispatcher_Name, CCM}, {dns_worker, ?ProtocolVersion, Pid, ping}, 500)),
        ?assertEqual(ok, gen_server:call({?Dispatcher_Name, CCM}, {control_panel, ?ProtocolVersion, Pid, ping}, 500)),
        ?assertEqual(ok, gen_server:call({?Dispatcher_Name, CCM}, {remote_files_manager, ?ProtocolVersion, Pid, ping}, 500)),
        Pid ! test_fun_ok
      catch
        E11:E12 ->
          Pid ! {E11, E12}
      end
    end)))
  end,

  TestRequestsNum = 30000,
  for(1, TestRequestsNum, TestFun),
  Answers = count_answers(),

  ct:print("Answers ~p~n", [Answers]),
  ?assertEqual(5*TestRequestsNum, proplists:get_value(pong, Answers, 0)),
  ?assertEqual(TestRequestsNum, proplists:get_value(test_fun_ok, Answers, 0)).

sub_proc_load_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  nodes_manager:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    nodes_manager:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),
  nodes_manager:wait_for_cluster_init(),

  {Workers, _} = gen_server:call({global, ?CCM}, get_workers, 1000),
  StartAdditionalWorker = fun(Node) ->
    case lists:member({Node, fslogic}, Workers) of
      true -> ok;
      false ->
        StartAns = gen_server:call({global, ?CCM}, {start_worker, Node, fslogic, []}, 3000),
        ?assertEqual(ok, StartAns)
    end
  end,
  lists:foreach(StartAdditionalWorker, NodesUp),
  nodes_manager:wait_for_cluster_init(length(NodesUp) - 1),

  ProcFun = fun(_ProtocolVersion, {sub_proc_test, _, AnsPid}) ->
    AnsPid ! sub_proc_ok
  end,

  MapFun = fun({sub_proc_test, MapNum, _}) ->
    MapNum rem 10
  end,

  RequestMap = fun
    ({sub_proc_test, _, _}) ->
      sub_proc_test_proccess;
    (_) -> non
  end,

  DispMapFun = fun
    ({sub_proc_test, MapNum2, _}) ->
      trunc(MapNum2 / 10);
    (_) -> non
  end,

  RegisterSubProc = fun(Node) ->
    RegAns = gen_server:call({fslogic, Node}, {register_sub_proc, sub_proc_test_proccess, 2, 3, ProcFun, MapFun, RequestMap, DispMapFun}, 1000),
    ?assertEqual(ok, RegAns),
    nodes_manager:wait_for_cluster_cast({fslogic, Node})
  end,
  lists:foreach(RegisterSubProc, NodesUp),
  nodes_manager:wait_for_request_handling(),

  Self = self(),
  TestFun = fun() ->
    ?assert(is_pid(spawn_link(fun() ->
      try
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 11, Self}}, 500),
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 12, Self}}, 500),
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 13, Self}}, 500),
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 21, Self}}, 500),
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 22, Self}}, 500),
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 23, Self}}, 500),
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 31, Self}}, 500),
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 32, Self}}, 500),
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 33, Self}}, 500),
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 41, Self}}, 500),
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 42, Self}}, 500),
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 43, Self}}, 500)
      catch
        E11:E12 ->
          Self ! {E11, E12}
      end
    end)))
  end,

  TestRequestsNum = 10000,
  for(1, TestRequestsNum, TestFun),

  Answers = count_answers(),

  ct:print("Answers ~p~n", [Answers]),
  ?assertEqual(12*TestRequestsNum, proplists:get_value(sub_proc_ok, Answers, 0)).


%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(sub_proc_load_test, Config) ->
  ?INIT_DIST_TEST,
  nodes_manager:start_deps_for_tester_node(),

  NodesUp = nodes_manager:start_test_on_nodes(4),
  [CCM | _] = NodesUp,
  DBNode = nodes_manager:get_db_node(),

  StartLog = nodes_manager:start_app_on_nodes(NodesUp, [
    [{node_type, ccm_test}, {dispatcher_port, 5055}, {ccm_nodes, [CCM]}, {dns_port, 1308}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 6666}, {ccm_nodes, [CCM]}, {dns_port, 1309}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 7777}, {ccm_nodes, [CCM]}, {dns_port, 1310}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 8888}, {ccm_nodes, [CCM]}, {dns_port, 1311}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {heart_beat, 1}]]),

  Assertions = [{false, lists:member(error, NodesUp)}, {false, lists:member(error, StartLog)}],
  lists:append([{nodes, NodesUp}, {assertions, Assertions}, {dbnode, DBNode}], Config);

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