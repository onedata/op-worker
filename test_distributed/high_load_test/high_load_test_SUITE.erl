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

-include("test_utils.hrl").
-include("registered_names.hrl").
-include("modules_and_args.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").

%% API
-export([main_test/1, sub_proc_load_test/1, multi_node_test/1]).
-export([all/0, init_per_testcase/2, end_per_testcase/2]).

%% export nodes' codes
-export([ccm_code1/0, ccm_code2/0, worker_code/0]).

all() -> [main_test, sub_proc_load_test, multi_node_test].

-define(ProtocolVersion, 1).
-define(MessageTimeout, 5000).

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

%% Tests if one node cluster is able to answer a lot of messages at once
main_test(Config) ->
  NodesUp = ?config(nodes, Config),
  [CCM | _] = NodesUp,
  start_cluster(CCM),

  Pid = self(),
  TestFun = fun() ->
    ?assert(is_pid(spawn_link(fun() ->
      try
        ?assertEqual(ok, gen_server:call({?Dispatcher_Name, CCM}, {fslogic, ?ProtocolVersion, Pid, ping}, ?MessageTimeout)),
        Pid ! test_fun_ok,
        ?assertEqual(ok, gen_server:call({?Dispatcher_Name, CCM}, {dao_worker, ?ProtocolVersion, Pid, ping}, ?MessageTimeout)),
        Pid ! test_fun_ok,
        ?assertEqual(ok, gen_server:call({?Dispatcher_Name, CCM}, {dns_worker, ?ProtocolVersion, Pid, ping}, ?MessageTimeout)),
        Pid ! test_fun_ok,
        ?assertEqual(ok, gen_server:call({?Dispatcher_Name, CCM}, {control_panel, ?ProtocolVersion, Pid, ping}, ?MessageTimeout)),
        Pid ! test_fun_ok,
        ?assertEqual(ok, gen_server:call({?Dispatcher_Name, CCM}, {remote_files_manager, ?ProtocolVersion, Pid, ping}, ?MessageTimeout)),
        Pid ! test_fun_ok
      catch
        E11:E12 ->
          Pid ! {E11, E12}
      end
    end)))
  end,

  TestRequestsNum = 10000,
  for(1, TestRequestsNum, TestFun),
  Answers = count_answers(),

  ?assertEqual(5*TestRequestsNum, proplists:get_value(test_fun_ok, Answers, 0)),
  ?assertEqual(5*TestRequestsNum, proplists:get_value(pong, Answers, 0)).

%% Tests if many node cluster is able to answer a lot of messages at once
multi_node_test(Config) ->
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,

  DuplicatedPermanentNodes = (length(NodesUp) - 1) * length(?PERMANENT_MODULES),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  test_utils:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  {Workers, _} = gen_server:call({global, ?CCM}, get_workers, 1000),
  StartAdditionalWorker = fun(Node) ->
    case lists:member({Node, fslogic}, Workers) of
      true -> ok;
      false ->
        ?assertEqual(ok, gen_server:call({global, ?CCM}, {start_worker, Node, fslogic, []}, 3000))
    end,
    case lists:member({Node, dao_worker}, Workers) of
      true -> ok;
      false ->
        ?assertEqual(ok, gen_server:call({global, ?CCM}, {start_worker, Node, dao_worker, []}, 3000))
    end,
    case lists:member({Node, dns_worker}, Workers) of
      true -> ok;
      false ->
        ?assertEqual(ok, gen_server:call({global, ?CCM}, {start_worker, Node, dns_worker, []}, 3000))
    end,
    case lists:member({Node, cluster_rengine}, Workers) of
      true -> ok;
      false ->
        ?assertEqual(ok, gen_server:call({global, ?CCM}, {start_worker, Node, cluster_rengine, []}, 3000))
    end,
    case lists:member({Node, gateway}, Workers) of
      true -> ok;
      false ->
        ?assertEqual(ok, gen_server:call({global, ?CCM}, {start_worker, Node, gateway, []}, 3000))
    end
  end,
  lists:foreach(StartAdditionalWorker, NodesUp),
  test_utils:wait_for_cluster_init(5*length(NodesUp) - 5),

  Pid = self(),
  TestFun = fun() ->
    ?assert(is_pid(spawn_link(fun() ->
      try
        NodeTestFun = fun(Node) ->
          ?assertEqual(ok, gen_server:call({?Dispatcher_Name, Node}, {fslogic, ?ProtocolVersion, Pid, ping}, ?MessageTimeout)),
          Pid ! test_fun_ok,
          ?assertEqual(ok, gen_server:call({?Dispatcher_Name, Node}, {dao_worker, ?ProtocolVersion, Pid, ping}, ?MessageTimeout)),
          Pid ! test_fun_ok,
          ?assertEqual(ok, gen_server:call({?Dispatcher_Name, Node}, {dns_worker, ?ProtocolVersion, Pid, ping}, ?MessageTimeout)),
          Pid ! test_fun_ok,
          ?assertEqual(ok, gen_server:call({?Dispatcher_Name, Node}, {cluster_rengine, ?ProtocolVersion, Pid, ping}, ?MessageTimeout)),
          Pid ! test_fun_ok,
          ?assertEqual(ok, gen_server:call({?Dispatcher_Name, Node}, {gateway, ?ProtocolVersion, Pid, ping}, ?MessageTimeout)),
          Pid ! test_fun_ok
        end,
        lists:foreach(NodeTestFun, NodesUp)
      catch
        E11:E12 ->
          Pid ! {E11, E12}
      end
    end)))
  end,

  TestRequestsNum = 5000,
  for(1, TestRequestsNum, TestFun),
  Answers = count_answers(),

  ?assertEqual(5*length(NodesUp)*TestRequestsNum, proplists:get_value(test_fun_ok, Answers, 0)),
  ?assertEqual(5*length(NodesUp)*TestRequestsNum, proplists:get_value(pong, Answers, 0)).

%% Tests if many node cluster is able to answer a lot of messages to sub_processes at once
sub_proc_load_test(Config) ->
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,

  DuplicatedPermanentNodes = (length(NodesUp) - 1) * length(?PERMANENT_MODULES),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  test_utils:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

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
  test_utils:wait_for_cluster_init(length(NodesUp) - 1),

  ProcFun = fun(_ProtocolVersion, {sub_proc_test, _, AnsPid}) ->
    calc(10000),
    AnsPid ! sub_proc_ok
  end,

  MapFun = fun({sub_proc_test, MapNum, _}) ->
    calc(1000),
    MapNum rem 10
  end,

  RequestMap = fun
    ({sub_proc_test, _, _}) ->
      calc(1000),
      sub_proc_test_proccess;
    (_) -> non
  end,

  DispMapFun = fun
    ({sub_proc_test, MapNum2, _}) ->
      calc(1000),
      trunc(MapNum2 / 10);
    (_) -> non
  end,

  RegisterSubProc = fun(Node) ->
    RegAns = gen_server:call({fslogic, Node}, {register_or_update_sub_proc, sub_proc_test_proccess, 2, 3, ProcFun, MapFun, RequestMap, DispMapFun}, 1000),
    ?assertEqual(ok, RegAns),
    test_utils:wait_for_cluster_cast({fslogic, Node})
  end,
  lists:foreach(RegisterSubProc, NodesUp),
  test_utils:wait_for_request_handling(),

  Self = self(),
  TestFun = fun() ->
    ?assert(is_pid(spawn_link(fun() ->
      try
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 11, Self}}, ?MessageTimeout),
        Self ! test_fun_ok,
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 12, Self}}, ?MessageTimeout),
        Self ! test_fun_ok,
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 13, Self}}, ?MessageTimeout),
        Self ! test_fun_ok,
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 21, Self}}, ?MessageTimeout),
        Self ! test_fun_ok,
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 22, Self}}, ?MessageTimeout),
        Self ! test_fun_ok,
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 23, Self}}, ?MessageTimeout),
        Self ! test_fun_ok,
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 31, Self}}, ?MessageTimeout),
        Self ! test_fun_ok,
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 32, Self}}, ?MessageTimeout),
        Self ! test_fun_ok,
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 33, Self}}, ?MessageTimeout),
        Self ! test_fun_ok,
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 41, Self}}, ?MessageTimeout),
        Self ! test_fun_ok,
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 42, Self}}, ?MessageTimeout),
        Self ! test_fun_ok,
        gen_server:call({?Dispatcher_Name, CCM}, {fslogic, 1, {sub_proc_test, 43, Self}}, ?MessageTimeout),
        Self ! test_fun_ok
      catch
        E11:E12 ->
          Self ! {E11, E12}
      end
    end)))
  end,

  TestRequestsNum = 500,
  for(1, TestRequestsNum, TestFun),

  Answers = count_answers(),

  ?assertEqual(12*TestRequestsNum, proplists:get_value(test_fun_ok, Answers, 0)),
  ?assertEqual(12*TestRequestsNum, proplists:get_value(sub_proc_ok, Answers, 0)).


%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(main_test, Config) ->
  ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,
  test_node_starter:start_deps_for_tester_node(),

  NodesUp = test_node_starter:start_test_nodes(1),
  [Node1 | _] = NodesUp,

  DB_Node = ?DB_NODE,
  Port = 6666,

  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, NodesUp, [[{node_type, ccm_test}, {dispatcher_port, Port}, {ccm_nodes, [Node1]}, {dns_port, 1317}, {db_nodes, [DB_Node]}, {heart_beat, 1}]]),

  lists:append([{port, Port}, {nodes, NodesUp}], Config);

init_per_testcase(_, Config) ->
  ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,
  test_node_starter:start_deps_for_tester_node(),

  NodesUp = test_node_starter:start_test_nodes(4),
  [CCM | _] = NodesUp,
  DBNode = ?DB_NODE,

  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, NodesUp, [
    [{node_type, ccm_test}, {dispatcher_port, 5055}, {ccm_nodes, [CCM]}, {dns_port, 1308}, {control_panel_port, 2308}, {control_panel_redirect_port, 1354}, {rest_port, 3308}, {gateway_listener_port, 8877}, {gateway_proxy_port, 8876}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {heart_beat, 1}],
    [{node_type, worker},   {dispatcher_port, 6666}, {ccm_nodes, [CCM]}, {dns_port, 1309}, {control_panel_port, 2309}, {control_panel_redirect_port, 1355}, {rest_port, 3309}, {gateway_listener_port, 8879}, {gateway_proxy_port, 8878}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {heart_beat, 1}],
    [{node_type, worker},   {dispatcher_port, 7777}, {ccm_nodes, [CCM]}, {dns_port, 1310}, {control_panel_port, 2310}, {control_panel_redirect_port, 1356}, {rest_port, 3310}, {gateway_listener_port, 8881}, {gateway_proxy_port, 8880}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {heart_beat, 1}],
    [{node_type, worker},   {dispatcher_port, 8888}, {ccm_nodes, [CCM]}, {dns_port, 1311}, {control_panel_port, 2311}, {control_panel_redirect_port, 1357}, {rest_port, 3311}, {gateway_listener_port, 8883}, {gateway_proxy_port, 8882}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {heart_beat, 1}]]),

  lists:append([{nodes, NodesUp}, {dbnode, DBNode}], Config).

end_per_testcase(_, Config) ->
  Nodes = ?config(nodes, Config),
  test_node_starter:stop_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, Nodes),
  test_node_starter:stop_test_nodes(Nodes),
  test_node_starter:stop_deps_for_tester_node().

%% ====================================================================
%% Helper functions
%% ====================================================================

start_cluster(Node) ->
  gen_server:cast({?Node_Manager_Name, Node}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init().

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

calc(0) ->
  0;
calc(X) ->
  calc(X-1).
