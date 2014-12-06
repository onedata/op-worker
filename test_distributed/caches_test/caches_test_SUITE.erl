%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This test creates many Erlang virtual machines and uses them
%% to test how ccm manages workers and monitors nodes.
%% @end
%% ===================================================================

-module(caches_test_SUITE).

-include("test_utils.hrl").
-include("registered_names.hrl").
-include("modules_and_args.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").

-define(ProtocolVersion, 1).
-define(CacheClearingTime, 4).
-define(CacheLocation, control_panel).

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([sub_proc_test/1, node_cache_test/1, permanent_node_cache_test/1, sub_proc_cache_test/1, automatic_nodes_cache_clearing_test/1,
  sub_procs_automatic_cache_clearing_test/1, error_nodes_cache_clearing_test/1, error_permanent_nodes_cache_test/1, sub_procs_error_cache_clearing_test/1]).

%% export nodes' codes
-export([ccm_code1/0, ccm_code2/0, worker_code/0, cache_loop/0]).

all() -> [sub_proc_test, node_cache_test, permanent_node_cache_test, sub_proc_cache_test, automatic_nodes_cache_clearing_test,
  sub_procs_automatic_cache_clearing_test, error_nodes_cache_clearing_test, error_permanent_nodes_cache_test, sub_procs_error_cache_clearing_test].

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

cache_loop() ->
  receive
    {clear_simple_cache, LoopTime, Fun, StrongCacheConnection} ->
      worker_host:clear_simple_cache(LoopTime, Fun, StrongCacheConnection),
      cache_loop();
    stop_cache -> ok
  end.

%% ====================================================================
%% Test function
%% ====================================================================

%% Tests if caches are updated properly after node recovery
error_permanent_nodes_cache_test(Config) ->
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  test_utils:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),

  DuplicatedPermanentNodes = (length(NodesUp) - 1) * length(?PERMANENT_MODULES),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  CacheCheckFun = fun() ->
    Node = node(),
    ets:delete(test_cache, test_key2),
    K = get_atom_from_node(Node, test_key),
    V = get_atom_from_node(Node, test_new_value),
    ets:insert(test_cache, {K, V})
  end,

  CreateCaches = fun(Node, Ans) ->
    Pid = spawn_link(Node, fun() ->
      ?assertEqual(ok, worker_host:create_permanent_cache(test_cache, CacheCheckFun)),
      cache_loop()
    end),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node}),
    [Pid | Ans]
  end,
  CachesPids = lists:foldl(CreateCaches, [], WorkerNodes),

  AddDataToCaches = fun(Node) ->
    ?assert(rpc:call(Node, ets, insert, [test_cache, {test_key, test_value}])),
    ?assert(rpc:call(Node, ets, insert, [test_cache, {test_key2, test_value2}])),
    ?assert(rpc:call(Node, ets, insert, [test_cache, {test_key3, test_value3}])),
    ?assert(rpc:call(Node, ets, insert, [test_cache, {get_atom_from_node(Node, test_key), get_atom_from_node(Node, test_value)}])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(AddDataToCaches, WorkerNodes),

  CheckCaches1 = fun(Node) ->
    ?assertEqual(4, rpc:call(Node, ets, info, [test_cache, size])),
    ?assertEqual([{test_key, test_value}], rpc:call(Node, ets, lookup, [test_cache, test_key])),
    ?assertEqual([{test_key2, test_value2}], rpc:call(Node, ets, lookup, [test_cache, test_key2])),
    ?assertEqual([{test_key3, test_value3}], rpc:call(Node, ets, lookup, [test_cache, test_key3])),
    K = get_atom_from_node(Node, test_key),
    V = get_atom_from_node(Node, test_value),
    ?assertEqual([{K, V}], rpc:call(Node, ets, lookup, [test_cache, K]))
  end,
  lists:foreach(CheckCaches1, WorkerNodes),

  [W1 | _] = WorkerNodes,
  ?assert(rpc:call(CCM, erlang, disconnect_node, [W1])),
  test_utils:wait_for_cluster_cast({?Node_Manager_Name, W1}),

  ?assertEqual(ok, rpc:call(W1, ?MODULE, worker_code, [])),
  test_utils:wait_for_cluster_cast({?Node_Manager_Name, W1}),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  CheckCaches2 = fun(Node) ->
    ?assertEqual(3, rpc:call(Node, ets, info, [test_cache, size])),
    ?assertEqual([{test_key, test_value}], rpc:call(Node, ets, lookup, [test_cache, test_key])),
    ?assertEqual([{test_key3, test_value3}], rpc:call(Node, ets, lookup, [test_cache, test_key3])),
    K = get_atom_from_node(Node, test_key),
    V = get_atom_from_node(Node, test_new_value),
    ?assertEqual([{K, V}], rpc:call(Node, ets, lookup, [test_cache, K]))
  end,
  lists:foreach(CheckCaches2, WorkerNodes),

  lists:foreach(fun(Pid) -> Pid ! stop_cache end, CachesPids).

%% This node-wide permanent caches
permanent_node_cache_test(Config) ->
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  test_utils:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),

  DuplicatedPermanentNodes = (length(NodesUp) - 1) * length(?PERMANENT_MODULES),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  CreateCaches = fun(Node, Ans) ->
    Pid = spawn_link(Node, fun() ->
      ?assertEqual(ok, worker_host:create_permanent_cache(test_cache)),
      cache_loop()
    end),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node}),
    [Pid | Ans]
  end,
  CachesPids = lists:foldl(CreateCaches, [], WorkerNodes),

  AddDataToCaches = fun(Node) ->
    ?assert(rpc:call(Node, ets, insert, [test_cache, {test_key, test_value}])),
    ?assert(rpc:call(Node, ets, insert, [test_cache, {test_key2, test_value2}])),
    ?assert(rpc:call(Node, ets, insert, [test_cache, {test_key3, test_value3}])),
    ?assert(rpc:call(Node, ets, insert, [test_cache, {get_atom_from_node(Node, test_key), get_atom_from_node(Node, test_value)}])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(AddDataToCaches, WorkerNodes),

  CheckCaches = fun(Node) ->
    ?assertEqual(4, rpc:call(Node, ets, info, [test_cache, size])),
    ?assertEqual([{test_key, test_value}], rpc:call(Node, ets, lookup, [test_cache, test_key])),
    ?assertEqual([{test_key2, test_value2}], rpc:call(Node, ets, lookup, [test_cache, test_key2])),
    ?assertEqual([{test_key3, test_value3}], rpc:call(Node, ets, lookup, [test_cache, test_key3])),
    K = get_atom_from_node(Node, test_key),
    V = get_atom_from_node(Node, test_value),
    ?assertEqual([{K, V}], rpc:call(Node, ets, lookup, [test_cache, K]))
  end,
  lists:foreach(CheckCaches, WorkerNodes),

  {Workers, _} = gen_server:call({global, ?CCM}, get_workers, 1000),
  StartAdditionalWorker = fun(Node) ->
    case lists:member({Node, ?CacheLocation}, Workers) of
      true -> ok;
      false ->
        StartAns = gen_server:call({global, ?CCM}, {start_worker, Node, ?CacheLocation, []}, 3000),
        ?assertEqual(ok, StartAns)
    end
  end,
  lists:foreach(StartAdditionalWorker, NodesUp),
  test_utils:wait_for_cluster_init(length(NodesUp) - 1 + DuplicatedPermanentNodes),
  lists:foreach(CheckCaches, WorkerNodes),

  lists:foreach(fun(Pid) -> Pid ! stop_cache end, CachesPids).

%% Tests if caches are cleared properly after node error
sub_procs_error_cache_clearing_test(Config) ->
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  test_utils:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),

  DuplicatedPermanentNodes = (length(NodesUp) - 1) * length(?PERMANENT_MODULES),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  {Workers, _} = gen_server:call({global, ?CCM}, get_workers, 1000),
  StartAdditionalWorker = fun(Node) ->
    case lists:member({Node, ?CacheLocation}, Workers) of
      true -> ok;
      false ->
        StartAns = gen_server:call({global, ?CCM}, {start_worker, Node, ?CacheLocation, []}, 3000),
        ?assertEqual(ok, StartAns)
    end
  end,
  lists:foreach(StartAdditionalWorker, NodesUp),
  test_utils:wait_for_cluster_init(length(NodesUp) - 1 + DuplicatedPermanentNodes),

  ProcFun = fun
    (_ProtocolVersion, {update_cache, _, AnsPid, {Key, Value}}, CacheName) ->
      case ets:lookup(CacheName, Key) of
        [{Key, OldValue}] ->
          ets:insert(CacheName, {Key, OldValue + Value});
        _ ->
          ets:insert(CacheName, {Key, Value})
      end,
      AnsPid ! cache_updated;
    (_ProtocolVersion, {get_from_cache, _, Key}, CacheName) ->
      case ets:lookup(CacheName, Key) of
        [{Key, Value}] ->
          {cache_value, Value};
        _ ->
          {cache_value, 0}
      end;
    (_ProtocolVersion, {sub_proc_test, _, AnsPid}, _CacheName) ->
      AnsPid ! sub_proc_test_ok
  end,

  MapFun = fun
    ({update_cache,MapNum, _, _}) -> MapNum rem 10;
    ({get_from_cache, MapNum, _}) -> MapNum rem 10;
    ({sub_proc_test, MapNum, _}) -> MapNum rem 10
  end,

  RequestMap = fun
    ({update_cache, _, _, _}) -> sub_proc_test_proccess;
    ({get_from_cache, _, _}) -> sub_proc_test_proccess;
    ({sub_proc_test, _, _}) -> sub_proc_test_proccess;
    (_) -> non
  end,

  DispMapFun = fun
    ({update_cache,MapNum2, _, _}) -> trunc(MapNum2 / 10);
    ({get_from_cache, MapNum2, _}) -> trunc(MapNum2 / 10);
    ({sub_proc_test, MapNum2, _}) -> trunc(MapNum2 / 10);
    (_) -> non
  end,

  RegisterSubProc = fun(Node) ->
    RegAns = gen_server:call({?CacheLocation, Node}, {register_or_update_sub_proc, sub_proc_test_proccess, 2, 3, ProcFun, MapFun, RequestMap, DispMapFun, simple}, 1000),
    ?assertEqual(ok, RegAns),
    test_utils:wait_for_cluster_cast({?CacheLocation, Node})
  end,
  lists:foreach(RegisterSubProc, NodesUp),
  test_utils:wait_for_request_handling(),

  Pid = self(),

  %% send many request to multiply sub_procs
  TestFun = fun() ->
    spawn_link(fun() ->
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 11, Pid}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 12, Pid}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 13, Pid}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 21, Pid}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 31, Pid}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 41, Pid}}, 500)
    end)
  end,

  TestRequestsNum = 100,
  for(1, TestRequestsNum, TestFun),
  ?assertEqual(6*TestRequestsNum, count_answers2(6*TestRequestsNum, sub_proc_test_ok)),

  TestFun2 = fun() ->
    spawn_link(fun() ->
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 11, Pid, {k1, 1}}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 11, Pid, {k2, 1}}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 12, Pid, {k3, 1}}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 13, Pid, {k4, 1}}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 21, Pid, {k5, 1}}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 31, Pid, {k6, 1}}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 41, Pid, {k1, 1}}}, 500)
    end)
  end,
  for(1, TestRequestsNum, TestFun2),
  ?assertEqual(7*TestRequestsNum, count_answers2(7*TestRequestsNum, cache_updated)),

  VerifyFun = fun(MapVal, Key) ->
    gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, Pid, {get_from_cache, MapVal, Key}}, 500),
    receive
      {cache_value, Value} -> Value
    after 500 ->
      timeout
    end
  end,
  ?assertEqual(TestRequestsNum, VerifyFun(11, k1)),
  ?assertEqual(TestRequestsNum, VerifyFun(11, k2)),
  ?assertEqual(TestRequestsNum, VerifyFun(12, k3)),
  ?assertEqual(TestRequestsNum, VerifyFun(13, k4)),
  ?assertEqual(TestRequestsNum, VerifyFun(21, k5)),
  ?assertEqual(TestRequestsNum, VerifyFun(31, k6)),
  ?assertEqual(TestRequestsNum, VerifyFun(41, k1)),

  W1 = gen_server:call({?Dispatcher_Name, CCM}, {get_worker_node, {{get_from_cache, 11, k1}, ?CacheLocation}}, 500),
  ?assert(rpc:call(CCM, erlang, disconnect_node, [W1])),
  test_utils:wait_for_cluster_cast({?Node_Manager_Name, W1}),

  ?assertEqual(ok, rpc:call(W1, ?MODULE, worker_code, [])),
  test_utils:wait_for_cluster_cast({?Node_Manager_Name, W1}),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  VerNums = [11,12,13,21,22,23,31,32,33,41,42,43],
  VerifyAllCaches = fun(Key) ->
    lists:foldl(fun(Num, Acc) ->
      Acc + VerifyFun(Num, Key)
    end, 0, VerNums)
  end,

  ?assertEqual(0, VerifyAllCaches(k1)),
  ?assertEqual(0, VerifyAllCaches(k2)),
  ?assertEqual(0, VerifyAllCaches(k3)),
  ?assertEqual(0, VerifyAllCaches(k4)),
  ?assertEqual(0, VerifyAllCaches(k5)),
  ?assertEqual(0, VerifyAllCaches(k6)).

%% Tests if caches are cleared properly after node error
error_nodes_cache_clearing_test(Config) ->
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  test_utils:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),

  DuplicatedPermanentNodes = (length(NodesUp) - 1) * length(?PERMANENT_MODULES),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  CreateCaches = fun(Node, Ans) ->
    Pid = spawn_link(Node, fun() ->
      ?assertEqual(ok, worker_host:create_simple_cache(test_cache)),
      cache_loop()
    end),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node}),
    [Pid | Ans]
  end,
  CachesPids = lists:foldl(CreateCaches, [], WorkerNodes),

  AddDataToCaches = fun(Node) ->
    ?assert(rpc:call(Node, ets, insert, [test_cache, {test_key, test_value}])),
    ?assert(rpc:call(Node, ets, insert, [test_cache, {test_key2, test_value2}])),
    ?assert(rpc:call(Node, ets, insert, [test_cache, {test_key3, test_value3}])),
    ?assert(rpc:call(Node, ets, insert, [test_cache, {get_atom_from_node(Node, test_key), get_atom_from_node(Node, test_value)}])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(AddDataToCaches, WorkerNodes),

  CheckCaches1 = fun(Node) ->
    ?assertEqual(4, rpc:call(Node, ets, info, [test_cache, size])),
    ?assertEqual([{test_key, test_value}], rpc:call(Node, ets, lookup, [test_cache, test_key])),
    ?assertEqual([{test_key2, test_value2}], rpc:call(Node, ets, lookup, [test_cache, test_key2])),
    ?assertEqual([{test_key3, test_value3}], rpc:call(Node, ets, lookup, [test_cache, test_key3])),
    K = get_atom_from_node(Node, test_key),
    V = get_atom_from_node(Node, test_value),
    ?assertEqual([{K, V}], rpc:call(Node, ets, lookup, [test_cache, K]))
  end,
  lists:foreach(CheckCaches1, WorkerNodes),

  [W1 | _] = WorkerNodes,
  ?assert(rpc:call(CCM, erlang, disconnect_node, [W1])),
  test_utils:wait_for_cluster_cast({?Node_Manager_Name, W1}),

  ?assertEqual(ok, rpc:call(W1, ?MODULE, worker_code, [])),
  test_utils:wait_for_cluster_cast({?Node_Manager_Name, W1}),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  CheckCaches2 = fun(Node) ->
    ?assertEqual(0, rpc:call(Node, ets, info, [test_cache, size]))
  end,
  lists:foreach(CheckCaches2, WorkerNodes),

  lists:foreach(fun(Pid) -> Pid ! stop_cache end, CachesPids).

%% Tests if caches are cleared properly if automatic clearing is set
sub_procs_automatic_cache_clearing_test(Config) ->
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  test_utils:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),

  DuplicatedPermanentNodes = (length(NodesUp) - 1) * length(?PERMANENT_MODULES),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  {Workers, _} = gen_server:call({global, ?CCM}, get_workers, 1000),
  StartAdditionalWorker = fun(Node) ->
    case lists:member({Node, ?CacheLocation}, Workers) of
      true -> ok;
      false ->
        StartAns = gen_server:call({global, ?CCM}, {start_worker, Node, ?CacheLocation, []}, 3000),
        ?assertEqual(ok, StartAns)
    end
  end,
  lists:foreach(StartAdditionalWorker, NodesUp),
  test_utils:wait_for_cluster_init(length(NodesUp) - 1 + DuplicatedPermanentNodes),

  ProcFun = fun
    (_ProtocolVersion, {update_cache, _, AnsPid, {Key, Value}}, CacheName) ->
      case ets:lookup(CacheName, Key) of
        [{Key, OldValue}] ->
          ets:insert(CacheName, {Key, OldValue + Value});
        _ ->
          ets:insert(CacheName, {Key, Value})
      end,
      AnsPid ! cache_updated;
    (_ProtocolVersion, {get_from_cache, _, Key}, CacheName) ->
      case ets:lookup(CacheName, Key) of
        [{Key, Value}] ->
          {cache_value, Value};
        _ ->
          {cache_value, 0}
      end;
    (_ProtocolVersion, {sub_proc_test, _, AnsPid}, _CacheName) ->
      AnsPid ! sub_proc_test_ok
  end,

  MapFun = fun
    ({update_cache,MapNum, _, _}) -> MapNum rem 10;
    ({get_from_cache, MapNum, _}) -> MapNum rem 10;
    ({sub_proc_test, MapNum, _}) -> MapNum rem 10
  end,

  RequestMap = fun
    ({update_cache, _, _, _}) -> sub_proc_test_proccess;
    ({get_from_cache, _, _}) -> sub_proc_test_proccess;
    ({sub_proc_test, _, _}) -> sub_proc_test_proccess;
    (_) -> non
  end,

  DispMapFun = fun
    ({update_cache,MapNum2, _, _}) -> trunc(MapNum2 / 10);
    ({get_from_cache, MapNum2, _}) -> trunc(MapNum2 / 10);
    ({sub_proc_test, MapNum2, _}) -> trunc(MapNum2 / 10);
    (_) -> non
  end,

  ClearFun = fun(CacheName) -> ets:delete_all_objects(CacheName) end,
  RegisterSubProc = fun(Node) ->
    RegAns = gen_server:call({?CacheLocation, Node}, {register_or_update_sub_proc, sub_proc_test_proccess, 2, 3, ProcFun, MapFun, RequestMap, DispMapFun, {simple, ?CacheClearingTime, ClearFun}}, 1000),
    ?assertEqual(ok, RegAns),
    test_utils:wait_for_cluster_cast({?CacheLocation, Node})
  end,
  lists:foreach(RegisterSubProc, NodesUp),
  test_utils:wait_for_request_handling(),

  Pid = self(),

  %% send many request to multiply sub_procs
  TestFun = fun() ->
    spawn_link(fun() ->
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 11, Pid}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 12, Pid}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 13, Pid}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 21, Pid}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 31, Pid}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 41, Pid}}, 500)
    end)
  end,

  TestRequestsNum = 100,
  for(1, TestRequestsNum, TestFun),
  ?assertEqual(6*TestRequestsNum, count_answers2(6*TestRequestsNum, sub_proc_test_ok)),

  TestFun2 = fun() ->
    spawn_link(fun() ->
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 11, Pid, {k1, 1}}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 11, Pid, {k2, 1}}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 12, Pid, {k3, 1}}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 13, Pid, {k4, 1}}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 21, Pid, {k5, 1}}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 31, Pid, {k6, 1}}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 41, Pid, {k1, 1}}}, 500)
    end)
  end,
  for(1, TestRequestsNum, TestFun2),
  ?assertEqual(7*TestRequestsNum, count_answers2(7*TestRequestsNum, cache_updated)),

  VerifyFun = fun(MapVal, Key) ->
    gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, Pid, {get_from_cache, MapVal, Key}}, 500),
    receive
      {cache_value, Value} -> Value
    after 500 ->
      timeout
    end
  end,
  ?assertEqual(TestRequestsNum, VerifyFun(11, k1)),
  ?assertEqual(TestRequestsNum, VerifyFun(11, k2)),
  ?assertEqual(TestRequestsNum, VerifyFun(12, k3)),
  ?assertEqual(TestRequestsNum, VerifyFun(13, k4)),
  ?assertEqual(TestRequestsNum, VerifyFun(21, k5)),
  ?assertEqual(TestRequestsNum, VerifyFun(31, k6)),
  ?assertEqual(TestRequestsNum, VerifyFun(41, k1)),

  timer:sleep(2000 * ?CacheClearingTime),

  ?assertEqual(0, VerifyFun(11, k1)),
  ?assertEqual(0, VerifyFun(11, k2)),
  ?assertEqual(0, VerifyFun(12, k3)),
  ?assertEqual(0, VerifyFun(13, k4)),
  ?assertEqual(0, VerifyFun(21, k5)),
  ?assertEqual(0, VerifyFun(31, k6)),
  ?assertEqual(0, VerifyFun(41, k1)).

%% Tests if caches are cleared properly if automatic clearing is set
automatic_nodes_cache_clearing_test(Config) ->
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  test_utils:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),

  DuplicatedPermanentNodes = (length(NodesUp) - 1) * length(?PERMANENT_MODULES),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  ClearFun = fun() -> ets:delete_all_objects(test_cache) end,
  CreateCaches = fun(Node, Ans) ->
    Pid = spawn_link(Node, fun() ->
      ?assertEqual(ok, worker_host:create_simple_cache(test_cache, ?CacheClearingTime, ClearFun)),
      cache_loop()
    end),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node}),
    [Pid | Ans]
  end,
  CachesPids = lists:foldl(CreateCaches, [], WorkerNodes),

  AddDataToCaches = fun(Node) ->
    ?assert(rpc:call(Node, ets, insert, [test_cache, {test_key, test_value}])),
    ?assert(rpc:call(Node, ets, insert, [test_cache, {test_key2, test_value2}])),
    ?assert(rpc:call(Node, ets, insert, [test_cache, {test_key3, test_value3}])),
    ?assert(rpc:call(Node, ets, insert, [test_cache, {get_atom_from_node(Node, test_key), get_atom_from_node(Node, test_value)}])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(AddDataToCaches, WorkerNodes),

  CheckCaches1 = fun(Node) ->
    ?assertEqual(4, rpc:call(Node, ets, info, [test_cache, size])),
    ?assertEqual([{test_key, test_value}], rpc:call(Node, ets, lookup, [test_cache, test_key])),
    ?assertEqual([{test_key2, test_value2}], rpc:call(Node, ets, lookup, [test_cache, test_key2])),
    ?assertEqual([{test_key3, test_value3}], rpc:call(Node, ets, lookup, [test_cache, test_key3])),
    K = get_atom_from_node(Node, test_key),
    V = get_atom_from_node(Node, test_value),
    ?assertEqual([{K, V}], rpc:call(Node, ets, lookup, [test_cache, K]))
  end,
  lists:foreach(CheckCaches1, WorkerNodes),

  timer:sleep(2000 * ?CacheClearingTime),
  CheckCaches2 = fun(Node) ->
    ?assertEqual(0, rpc:call(Node, ets, info, [test_cache, size]))
  end,
  lists:foreach(CheckCaches2, WorkerNodes),

  lists:foreach(fun(Pid) -> Pid ! stop_cache end, CachesPids).

%% This test checks sub procs management (if requests are forwarded to apropriate sub procs)
sub_proc_cache_test(Config) ->
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  test_utils:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),

  DuplicatedPermanentNodes = (length(NodesUp) - 1) * length(?PERMANENT_MODULES),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  {Workers, _} = gen_server:call({global, ?CCM}, get_workers, 1000),
  StartAdditionalWorker = fun(Node) ->
    case lists:member({Node, ?CacheLocation}, Workers) of
      true -> ok;
      false ->
        StartAns = gen_server:call({global, ?CCM}, {start_worker, Node, ?CacheLocation, []}, 3000),
        ?assertEqual(ok, StartAns)
    end
  end,
  lists:foreach(StartAdditionalWorker, NodesUp),
  test_utils:wait_for_cluster_init(length(NodesUp) - 1 + DuplicatedPermanentNodes),

  ProcFun = fun
    (_ProtocolVersion, {update_cache, _, AnsPid, {Key, Value}}, CacheName) ->
      case ets:lookup(CacheName, Key) of
        [{Key, OldValue}] ->
          ets:insert(CacheName, {Key, OldValue + Value});
        _ ->
          ets:insert(CacheName, {Key, Value})
      end,
      AnsPid ! cache_updated;
    (_ProtocolVersion, {get_from_cache, _, Key}, CacheName) ->
      case ets:lookup(CacheName, Key) of
        [{Key, Value}] ->
          {cache_value, Value};
        _ ->
          {cache_value, 0}
      end;
    (_ProtocolVersion, {sub_proc_test, _, AnsPid}, _CacheName) ->
      AnsPid ! sub_proc_test_ok
  end,

  MapFun = fun
    ({update_cache,MapNum, _, _}) -> MapNum rem 10;
    ({get_from_cache, MapNum, _}) -> MapNum rem 10;
    ({sub_proc_test, MapNum, _}) -> MapNum rem 10
  end,

  RequestMap = fun
    ({update_cache, _, _, _}) -> sub_proc_test_proccess;
    ({get_from_cache, _, _}) -> sub_proc_test_proccess;
    ({sub_proc_test, _, _}) -> sub_proc_test_proccess;
    (_) -> non
  end,

  DispMapFun = fun
    ({update_cache,MapNum2, _, _}) -> trunc(MapNum2 / 10);
    ({get_from_cache, MapNum2, _}) -> trunc(MapNum2 / 10);
    ({sub_proc_test, MapNum2, _}) -> trunc(MapNum2 / 10);
    (_) -> non
  end,

  RegisterSubProc = fun(Node) ->
    RegAns = gen_server:call({?CacheLocation, Node}, {register_or_update_sub_proc, sub_proc_test_proccess, 2, 3, ProcFun, MapFun, RequestMap, DispMapFun, simple}, 1000),
    ?assertEqual(ok, RegAns),
    test_utils:wait_for_cluster_cast({?CacheLocation, Node})
  end,
  lists:foreach(RegisterSubProc, NodesUp),
  test_utils:wait_for_request_handling(),

  Pid = self(),

  %% send many request to multiply sub_procs
  TestFun = fun() ->
    spawn_link(fun() ->
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 11, Pid}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 12, Pid}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 13, Pid}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 21, Pid}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 31, Pid}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 41, Pid}}, 500)
    end)
  end,

  TestRequestsNum = 100,
  for(1, TestRequestsNum, TestFun),
  ?assertEqual(6*TestRequestsNum, count_answers2(6*TestRequestsNum, sub_proc_test_ok)),

  TestFun2 = fun() ->
    spawn_link(fun() ->
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 11, Pid, {k1, 1}}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 11, Pid, {k2, 1}}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 12, Pid, {k3, 1}}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 13, Pid, {k4, 1}}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 21, Pid, {k5, 1}}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 31, Pid, {k6, 1}}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {update_cache, 41, Pid, {k1, 1}}}, 500)
    end)
  end,
  for(1, TestRequestsNum, TestFun2),
  ?assertEqual(7*TestRequestsNum, count_answers2(7*TestRequestsNum, cache_updated)),

  VerifyFun = fun(MapVal, Key) ->
    gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, Pid, {get_from_cache, MapVal, Key}}, 500),
    receive
      {cache_value, Value} -> Value
    after 500 ->
      timeout
    end
  end,
  ?assertEqual(TestRequestsNum, VerifyFun(11, k1)),
  ?assertEqual(TestRequestsNum, VerifyFun(11, k2)),
  ?assertEqual(TestRequestsNum, VerifyFun(12, k3)),
  ?assertEqual(TestRequestsNum, VerifyFun(13, k4)),
  ?assertEqual(TestRequestsNum, VerifyFun(21, k5)),
  ?assertEqual(TestRequestsNum, VerifyFun(31, k6)),
  ?assertEqual(TestRequestsNum, VerifyFun(41, k1)),

  ?assertEqual(ok, rpc:call(CCM, worker_host, synch_cache_clearing, [{{sub_proc_cache, {?CacheLocation, sub_proc_test_proccess}}, k1}])),
  ?assertEqual(0, VerifyFun(11, k1)),
  ?assertEqual(TestRequestsNum, VerifyFun(11, k2)),
  ?assertEqual(TestRequestsNum, VerifyFun(12, k3)),
  ?assertEqual(TestRequestsNum, VerifyFun(13, k4)),
  ?assertEqual(TestRequestsNum, VerifyFun(21, k5)),
  ?assertEqual(TestRequestsNum, VerifyFun(31, k6)),
  ?assertEqual(0, VerifyFun(41, k1)),

  ?assertEqual(ok, rpc:call(CCM, worker_host, synch_cache_clearing, [{{sub_proc_cache, {?CacheLocation, sub_proc_test_proccess}}, [k3, k4, k5]}])),
  ?assertEqual(0, VerifyFun(11, k1)),
  ?assertEqual(TestRequestsNum, VerifyFun(11, k2)),
  ?assertEqual(0, VerifyFun(12, k3)),
  ?assertEqual(0, VerifyFun(13, k4)),
  ?assertEqual(0, VerifyFun(21, k5)),
  ?assertEqual(TestRequestsNum, VerifyFun(31, k6)),
  ?assertEqual(0, VerifyFun(41, k1)),

  ?assertEqual(ok, rpc:call(CCM, worker_host, synch_cache_clearing, [{sub_proc_cache, {?CacheLocation, sub_proc_test_proccess}}])),
  ?assertEqual(0, VerifyFun(11, k1)),
  ?assertEqual(0, VerifyFun(11, k2)),
  ?assertEqual(0, VerifyFun(12, k3)),
  ?assertEqual(0, VerifyFun(13, k4)),
  ?assertEqual(0, VerifyFun(21, k5)),
  ?assertEqual(0, VerifyFun(31, k6)),
  ?assertEqual(0, VerifyFun(41, k1)).

%% This node-wide caches
node_cache_test(Config) ->
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  test_utils:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),

  DuplicatedPermanentNodes = (length(WorkerNodes) - 1) * length(?PERMANENT_MODULES),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  CreateCaches = fun(Node, Ans) ->
    Pid = spawn_link(Node, fun() ->
      ?assertEqual(ok, worker_host:create_simple_cache(test_cache)),
      cache_loop()
    end),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node}),
    [Pid | Ans]
  end,
  CachesPids = lists:foldl(CreateCaches, [], WorkerNodes),

  AddDataToCaches = fun(Node) ->
    ?assert(rpc:call(Node, ets, insert, [test_cache, {test_key, test_value}])),
    ?assert(rpc:call(Node, ets, insert, [test_cache, {test_key2, test_value2}])),
    ?assert(rpc:call(Node, ets, insert, [test_cache, {test_key3, test_value3}])),
    ?assert(rpc:call(Node, ets, insert, [test_cache, {get_atom_from_node(Node, test_key), get_atom_from_node(Node, test_value)}])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(AddDataToCaches, WorkerNodes),

  ?assertEqual(ok, rpc:call(CCM, worker_host, synch_cache_clearing, [{test_cache, test_key}])),
  CheckCaches1 = fun(Node) ->
    ?assertEqual(3, rpc:call(Node, ets, info, [test_cache, size])),
    ?assertEqual([{test_key2, test_value2}], rpc:call(Node, ets, lookup, [test_cache, test_key2])),
    ?assertEqual([{test_key3, test_value3}], rpc:call(Node, ets, lookup, [test_cache, test_key3])),
    K = get_atom_from_node(Node, test_key),
    V = get_atom_from_node(Node, test_value),
    ?assertEqual([{K, V}], rpc:call(Node, ets, lookup, [test_cache, K]))
  end,
  lists:foreach(CheckCaches1, WorkerNodes),

  ?assertEqual(ok, rpc:call(CCM, worker_host, synch_cache_clearing, [{test_cache, [test_key2, test_key3]}])),
  CheckCaches2 = fun(Node) ->
    ?assertEqual(1, rpc:call(Node, ets, info, [test_cache, size])),
    K = get_atom_from_node(Node, test_key),
    V = get_atom_from_node(Node, test_value),
    ?assertEqual([{K, V}], rpc:call(Node, ets, lookup, [test_cache, K]))
  end,
  lists:foreach(CheckCaches2, WorkerNodes),

  [WN1 | WorkerNodes2] = WorkerNodes,
  ?assertEqual(ok, rpc:call(CCM, worker_host, synch_cache_clearing, [{test_cache, get_atom_from_node(WN1, test_key)}])),
  lists:foreach(CheckCaches2, WorkerNodes2),

  ?assertEqual(ok, rpc:call(CCM, worker_host, synch_cache_clearing, [test_cache])),
  CheckCaches3 = fun(Node) ->
    ?assertEqual(0, rpc:call(Node, ets, info, [test_cache, size]))
  end,
  lists:foreach(CheckCaches3, WorkerNodes),

  lists:foreach(fun(Pid) -> Pid ! stop_cache end, CachesPids).

%% This test checks sub procs management (if requests are forwarded to apropriate sub procs)
sub_proc_test(Config) ->
  NodesUp = ?config(nodes, Config),

  [CCM | WorkerNodes] = NodesUp,

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  test_utils:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),

  DuplicatedPermanentNodes = (length(NodesUp) - 1) * length(?PERMANENT_MODULES),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  {Workers, _} = gen_server:call({global, ?CCM}, get_workers, 1000),
  StartAdditionalWorker = fun(Node) ->
    case lists:member({Node, ?CacheLocation}, Workers) of
      true -> ok;
      false ->
        StartAns = gen_server:call({global, ?CCM}, {start_worker, Node, ?CacheLocation, []}, 3000),
        ?assertEqual(ok, StartAns)
    end
  end,
  lists:foreach(StartAdditionalWorker, NodesUp),
  test_utils:wait_for_cluster_init(length(NodesUp) - 1 + DuplicatedPermanentNodes),

  ProcFun = fun(_ProtocolVersion, {sub_proc_test, _, AnsPid}) ->
    Pid = self(),
    Node = node(),
    AnsPid ! {Pid, Node}
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
    RegAns = gen_server:call({?CacheLocation, Node}, {register_or_update_sub_proc, sub_proc_test_proccess, 2, 3, ProcFun, MapFun, RequestMap, DispMapFun}, 1000),
    ?assertEqual(ok, RegAns),
    test_utils:wait_for_cluster_cast({?CacheLocation, Node})
  end,
  lists:foreach(RegisterSubProc, NodesUp),
  test_utils:wait_for_request_handling(),

  Self = self(),
  TestFun = fun() ->
    spawn_link(fun() ->
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 11, Self}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 12, Self}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 13, Self}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 21, Self}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 31, Self}}, 500),
      gen_server:call({?Dispatcher_Name, CCM}, {?CacheLocation, 1, {sub_proc_test, 41, Self}}, 500)
    end)
  end,

  TestRequestsNum = 100,
  for(1, TestRequestsNum, TestFun),

  Ans = count_answers(6 * TestRequestsNum),
  ?assertEqual(10, length(Ans)),
  Keys = proplists:get_keys(Ans),
  ?assertEqual(6* TestRequestsNum, lists:foldl(fun(K, Sum) ->
    Sum + proplists:get_value(K, Ans, 0)
  end, 0, Keys)),

  NodesSums = lists:foldl(fun({P, Node}, Sum) ->
    V = proplists:get_value({P, Node}, Ans, 0),
    V2 = proplists:get_value(Node, Sum, 0),
    [{Node, V + V2} | proplists:delete(Node, Sum)]
  end, [], Keys),

  NodesSumSumarry = lists:foldl(fun({_, V}, Sum) ->
    V2 = proplists:get_value(V, Sum, 0),
    [{V, V2 + 1} | proplists:delete(V, Sum)]
  end, [], NodesSums),

  ?assertEqual(2, length(NodesSumSumarry)),
  ?assert(lists:member({TestRequestsNum, 3}, NodesSumSumarry)),
  ?assert(lists:member({3*TestRequestsNum, 1}, NodesSumSumarry)).



%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(_, Config) ->
  ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,
  test_node_starter:start_deps_for_tester_node(),

  NodesUp = test_node_starter:start_test_nodes(4),
  [CCM | _] = NodesUp,
  DBNode = ?DB_NODE,

    test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, NodesUp,  [
    [{node_type, ccm_test}, {dispatcher_port, 5055}, {ccm_nodes, [CCM]}, {dns_port, 1308}, {control_panel_port, 2308}, {control_panel_redirect_port, 1354}, {rest_port, 3308}, {gateway_listener_port, 3217}, {gateway_proxy_port, 3218}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 6666}, {ccm_nodes, [CCM]}, {dns_port, 1309}, {control_panel_port, 2309}, {control_panel_redirect_port, 1355}, {rest_port, 3309}, {gateway_listener_port, 3219}, {gateway_proxy_port, 3220}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 7777}, {ccm_nodes, [CCM]}, {dns_port, 1310}, {control_panel_port, 2310}, {control_panel_redirect_port, 1356}, {rest_port, 3310}, {gateway_listener_port, 3221}, {gateway_proxy_port, 3222}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 8888}, {ccm_nodes, [CCM]}, {dns_port, 1311}, {control_panel_port, 2311}, {control_panel_redirect_port, 1357}, {rest_port, 3311}, {gateway_listener_port, 3223}, {gateway_proxy_port, 3224}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {heart_beat, 1}]]),

  lists:append([{nodes, NodesUp}, {dbnode, DBNode}], Config).

end_per_testcase(_, Config) ->
  Nodes = ?config(nodes, Config),
  test_node_starter:stop_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS,Nodes),
  test_node_starter:stop_test_nodes(Nodes),
  test_node_starter:stop_deps_for_tester_node().

for(N, N, F) -> [F()];
for(I, N, F) -> [F()|for(I+1, N, F)].

count_answers(ExpectedNum) ->
  count_answers(ExpectedNum, []).

count_answers(0, TmpAns) ->
  TmpAns;

count_answers(ExpectedNum, TmpAns) ->
  receive
    {Msg1, Msg2} when is_atom(Msg2) ->
      NewCounter = proplists:get_value({Msg1, Msg2}, TmpAns, 0) + 1,
      NewAns = [{{Msg1, Msg2}, NewCounter} | proplists:delete({Msg1, Msg2}, TmpAns)],
      count_answers(ExpectedNum - 1, NewAns)
  after 5000 ->
    TmpAns
  end.

count_answers2(0, _Message) ->
  0;

count_answers2(ExpectedNum, Message) ->
  receive
    Message ->
      count_answers2(ExpectedNum - 1, Message) + 1
  after 5000 ->
    0
  end.

get_atom_from_node(Node, Ending) ->
  list_to_atom(atom_to_list(Node) ++ atom_to_list(Ending)).
