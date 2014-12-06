%% ==================================================================
%% @author Michal Sitko
%% @copyright (C) 2014, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ==================================================================
%% @doc: This test checks if event handlers subscription, configuration
%% and processing works correctly. Tests send events by themselves
%% mimicking behaviour of event producers.
%% @end
%% ==================================================================
-module(cluster_rengine_test_SUITE).

-include("test_utils.hrl").
-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/dao/dao_vfs.hrl").
-include_lib("dao/include/dao_helper.hrl").
-include_lib("oneprovider_modules/dao/dao_types.hrl").

-include("modules_and_args.hrl").
-include_lib("ctool/include/logging.hrl").
-include("registered_names.hrl").
-include("oneprovider_modules/cluster_rengine/cluster_rengine.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").

%% API
-export([test_event_subscription/1, test_multiple_trees/1, test_event_aggregation/1, test_dispatching/1, test_io_events_for_stats/1]).

-export([ccm_code1/0, ccm_code2/0, worker_code/0]).

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
all() -> [test_event_subscription, test_multiple_trees, test_event_aggregation, test_dispatching, test_io_events_for_stats].

-define(assert_received(ResponsePattern), receive
                                            ResponsePattern -> ok
                                          after 1000
                                            -> ?assert(false)
                                          end).

-define(assert_received(ResponsePattern, Timeout), receive
                                                     ResponsePattern -> ok
                                                   after Timeout
                                                     -> ?assert(false)
                                                   end).

-define(SH, "DirectIO").

test_event_subscription(Config) ->
  NodesUp = ?config(nodes, Config),

  [CCM | _] = NodesUp,

  WriteEvent = [{"type", "write_event_ct_test"}, {"user_id", "1234"}, {"ans_pid", self()}],

  % there was no subscription for events - sending events has no effect
  send_event(WriteEvent, CCM),
  assert_nothing_received(),

  % event handler sends back some message to producer to enable verification
  EventHandler = fun(WriteEv) ->
    AnsPid = proplists:get_value("ans_pid", WriteEv),
    case AnsPid of
      undefined -> ok;
      _ -> AnsPid ! {event_handled, standard, self()}
    end
  end,
  subscribe_for_write_events(CCM, standard, EventHandler), % subscribing for events
  send_event(WriteEvent, CCM),

  % this time there is a subscription for write_event - we should receive message
  ?assert_received({event_handled, standard, _}),
  assert_nothing_received(),

  EventHandler2 = fun(WriteEv) ->
    AnsPid = proplists:get_value("ans_pid", WriteEv),
    case AnsPid of
      undefined -> ok;
      _ -> AnsPid ! {event_handled, tree, self()}
    end
  end,

  subscribe_for_write_events(CCM, tree, EventHandler2, #event_stream_config{}),
  send_event(WriteEvent, CCM),

  % this time there are two handler registered
  ?assert_received({event_handled, standard, _}),
  ?assert_received({event_handled, tree, _}, 3000),
  assert_nothing_received().

test_io_events_for_stats(Config) ->
  NodesUp = ?config(nodes, Config),

  [CCM | _] = NodesUp,

  {ok, MonitoringInitialization} = rpc:call(CCM, application, get_env, [?APP_Name, cluster_monitoring_initialization]),
  {ok, MonitoringPeriod} = rpc:call(CCM, application, get_env, [?APP_Name, cluster_monitoring_period]),
  {ok, IOBytesThreshold} = rpc:call(CCM, application, get_env, [?APP_Name, io_bytes_threshold]),
  timer:sleep(2 * 1000 * MonitoringInitialization),

  BytesWritten = 2 * IOBytesThreshold,
  BytesRead = 3 * IOBytesThreshold,
  WriteEvent = [{"type", "write_event_ct_test"}, {"bytes", BytesWritten}],
  ReadEvent = [{"type", "read_event_ct_test"}, {"bytes", BytesRead}],

  WriteEventHandler = fun(WriteEv) ->
    Bytes = proplists:get_value("bytes", WriteEv),
    gen_server:cast({global, ?CCM}, {update_storage_write_b, Bytes})
  end,
  ReadEventHandler = fun(ReadEv) ->
    Bytes = proplists:get_value("bytes", ReadEv),
    gen_server:cast({global, ?CCM}, {update_storage_read_b, Bytes})
  end,

  subscribe_for_write_events(CCM, standard, WriteEventHandler),
  subscribe_for_read_events(CCM, standard, ReadEventHandler),
  send_event(WriteEvent, CCM),
  send_event(ReadEvent, CCM),

  timer:sleep(2 * 1000 * MonitoringPeriod),
  ClusterStats = gen_server:call({global, ?CCM}, {get_cluster_stats, long}, 500),
  WriteStats = proplists:get_value("storage_write_b", ClusterStats),
  ReadStats = proplists:get_value("storage_read_b", ClusterStats),
  ?assert(WriteStats > 0),
  ?assert(ReadStats > 0),
  ?assert(ReadStats > WriteStats).

test_multiple_trees(Config) ->
  NodesUp = ?config(nodes, Config),
  [CCM | _] = NodesUp,

  WriteEvent = [{"type", "write_event_ct_test"}, {"user_id", "1234"}, {"ans_pid", self()}],

  CreateEventHandler = fun(HandlerId) ->
    fun(WriteEv) ->
      AnsPid = proplists:get_value("ans_pid", WriteEv),
      case AnsPid of
        undefined -> ok;
        _ ->
          AnsPid ! {event_handled, HandlerId, self()}
      end
    end
  end,

  subscribe_for_write_events(CCM, tree, CreateEventHandler(tree1), #event_stream_config{}),
  subscribe_for_write_events(CCM, tree, CreateEventHandler(tree2), #event_stream_config{}),
  send_event(WriteEvent, CCM),

  ?assert_received({event_handled, tree1, _}, 3000),
  ?assert_received({event_handled, tree2, _}, 3000),

  lists:foreach(fun(Node) ->
    SubProcList = gen_server:call({cluster_rengine, Node}, getSubProcs),
    ?assert(is_list(SubProcList)),
    ?assertEqual(2, length(SubProcList))
  end, NodesUp),

  subscribe_for_write_events(CCM, tree, CreateEventHandler(tree3), #event_stream_config{}),
  send_event(WriteEvent, CCM),

  ?assert_received({event_handled, tree1, _}, 3000),
  ?assert_received({event_handled, tree2, _}, 3000),
  ?assert_received({event_handled, tree3, _}, 3000),
  lists:foreach(fun(Node) ->
    SubProcList = gen_server:call({cluster_rengine, Node}, getSubProcs),
    ?assert(is_list(SubProcList)),
    ?assertEqual(3, length(SubProcList))
  end, NodesUp).

test_event_aggregation(Config) ->
  NodesUp = ?config(nodes, Config),
  [CCM | _] = NodesUp,

  EventHandler = fun(WriteEv) ->
    AnsPid = proplists:get_value("ans_pid", WriteEv),
    case AnsPid of
      undefined -> ok;
      _ ->
        AnsPid ! {event_handled, tree, self()}
    end
  end,
  subscribe_for_write_events(CCM, tree, EventHandler, #event_stream_config{config = #aggregator_config{field_name = "user_id", fun_field_name = "count", threshold = 4}}),
  WriteEvent = [{"type", "write_event_ct_test"}, {"user_id", "1234"}, {"ans_pid", self()}],

  repeat(3, fun() -> send_event(WriteEvent, CCM) end),
  assert_nothing_received(),

  send_event(WriteEvent, CCM),
  ?assert_received({event_handled, tree, _}),
  assert_nothing_received(),

  repeat(3, fun() -> send_event(WriteEvent, CCM) end),
  assert_nothing_received(),
  send_event(WriteEvent, CCM),
  ?assert_received({event_handled, tree, _}),
  assert_nothing_received().

test_dispatching(Config) ->
  NodesUp = ?config(nodes, Config),
  [CCM | _] = NodesUp,

  EventHandler = fun(WriteEv) ->
    AnsPid = proplists:get_value("ans_pid", WriteEv),
    case AnsPid of
      undefined -> ok;
      _ -> AnsPid ! {event_handled, tree, self()}
    end
  end,
  subscribe_for_write_events(CCM, tree, EventHandler),

  WriteEvent1 = [{"type", "write_event_ct_test"}, {"user_id", "1234"}, {"ans_pid", self()}],
  WriteEvent2 = [{"type", "write_event_ct_test"}, {"user_id", "1235"}, {"ans_pid", self()}],
  WriteEvent3 = [{"type", "write_event_ct_test"}, {"user_id", "1334"}, {"ans_pid", self()}],
  WriteEvent4 = [{"type", "write_event_ct_test"}, {"user_id", "1335"}, {"ans_pid", self()}],
  WriteEvent5 = [{"type", "write_event_ct_test"}, {"user_id", "1236"}, {"ans_pid", self()}],
  WriteEvents = [WriteEvent1, WriteEvent2, WriteEvent3, WriteEvent4],
  SendWriteEvent = fun (Event) -> send_event(Event, CCM) end,

  SendWriteEvents = fun() ->
    spawn(fun() ->
      lists:foreach(SendWriteEvent, WriteEvents)
    end)
  end,

  SendWriteEvent(WriteEvent1),
  SendWriteEvent(WriteEvent2),

  SendWriteEvents(),
  count_answers(8),

  MessagesNum = 300,
  repeat(MessagesNum, SendWriteEvents),
  {Count, SetOfPids} = count_answers(4*MessagesNum),
  ?assertEqual(0, Count),
  ?assertEqual(6, sets:size(SetOfPids)),

  SendWriteEvents(),
  {Count2, SetOfPids2} = count_answers(4),
  ?assertEqual(0, Count2),
  ?assertEqual(4, sets:size(SetOfPids2)),

  % WriteEvent1 and WriteEvent5 should be dispatched to the same process
  SendWriteEvent(WriteEvent1),
  SendWriteEvent(WriteEvent5),
  {Count3, SetOfPids3} = count_answers(2),
  ?assertEqual(0, Count3),
  ?assertEqual(1, sets:size(SetOfPids3)),

  % WriteEvent1 and WriteEvent2 should be dispatched to different processes on the same node
  SendWriteEvent(WriteEvent1),
  SendWriteEvent(WriteEvent2),
  {Count4, SetOfPids4} = count_answers(2),
  ?assertEqual(0, Count4),
  ?assertEqual(2, sets:size(SetOfPids4)),
  NodesFromPids = fun(Pids) -> sets:from_list(lists:map(fun (Pid) -> node(Pid) end, sets:to_list(Pids))) end,
  SetOfNodes = NodesFromPids(SetOfPids4),
  ?assertEqual(1, sets:size(SetOfNodes)),

  % WriteEvent1 and WriteEvent3 should be dispatched to different processes on different nodes
  SendWriteEvent(WriteEvent1),
  SendWriteEvent(WriteEvent3),
  {Count5, SetOfPids5} = count_answers(2),
  ?assertEqual(0, Count5),
  ?assertEqual(2, sets:size(SetOfPids5)),
  SetOfNodes2 = NodesFromPids(SetOfPids5),
  ?assertEqual(2, sets:size(SetOfNodes2)).


%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(test_io_events_for_stats, Config) ->
  ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,
  test_node_starter:start_deps_for_tester_node(),

  NodesUp = test_node_starter:start_test_nodes(2),
  [CCM | WorkerNodes] = NodesUp,
  DBNode = ?DB_NODE,

  DuplicatedPermanentNodes = (length(WorkerNodes) - 1) * length(?PERMANENT_MODULES),

  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, NodesUp, [
    [{node_type, ccm}, {dispatcher_port, 5055}, {ccm_nodes, [CCM]}, {dns_port, 1313}, {control_panel_port, 2308}, {control_panel_redirect_port, 1354}, {rest_port, 3308}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {cluster_monitoring_initialization, 5}, {cluster_monitoring_period, 5}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 6666}, {ccm_nodes, [CCM]}, {dns_port, 1314}, {control_panel_port, 2309}, {control_panel_redirect_port, 1355}, {gateway_listener_port, 3217}, {gateway_proxy_port, 3218}, {rest_port, 3309}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {cluster_monitoring_initialization, 5}, {cluster_monitoring_period, 5}, {heart_beat, 1}]]),

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  test_utils:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),
  {Workers, _} = gen_server:call({global, ?CCM}, get_workers),

  StartAdditionalWorker = fun(Node, Module) ->
    case lists:member({Node, Module}, Workers) of
      true -> ok;
      false ->
        StartAns = gen_server:call({global, ?CCM}, {start_worker, Node, Module, []}, 3000),
        ?assertEqual(ok, StartAns)
    end
  end,

  lists:foreach(fun(Node) -> StartAdditionalWorker(Node, cluster_rengine) end, NodesUp),
  test_utils:wait_for_cluster_init(length(NodesUp) - 1),
  lists:append([{nodes, NodesUp}], Config);

init_per_testcase(_, Config) ->
  ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,
  test_node_starter:start_deps_for_tester_node(),

  NodesUp = test_node_starter:start_test_nodes(2),
  [CCM | WorkerNodes] = NodesUp,
  DBNode = ?DB_NODE,
  DuplicatedPermanentNodes = (length(NodesUp) - 1) * length(?PERMANENT_MODULES),

  test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, NodesUp, [
  [{node_type, ccm_test}, {dispatcher_port, 5055}, {ccm_nodes, [CCM]}, {dns_port, 1313}, {control_panel_port, 2308}, {control_panel_redirect_port, 1354}, {gateway_listener_port, 3217}, {gateway_proxy_port, 3218}, {rest_port, 3308}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {cluster_monitoring_initialization, 5}, {cluster_monitoring_period, 5}, {heart_beat, 1}],
  [{node_type, worker}, {dispatcher_port, 6666}, {ccm_nodes, [CCM]}, {dns_port, 1314}, {control_panel_port, 2309}, {control_panel_redirect_port, 1355}, {gateway_listener_port, 3219}, {gateway_proxy_port, 3220}, {rest_port, 3309}, {db_nodes, [DBNode]}, {fuse_session_expire_time, 2}, {dao_fuse_cache_loop_time, 1}, {cluster_monitoring_initialization, 5}, {cluster_monitoring_period, 5}, {heart_beat, 1}]]),

  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code1, [])),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),
  test_utils:wait_for_cluster_cast(),
  RunWorkerCode = fun(Node) ->
    ?assertEqual(ok, rpc:call(Node, ?MODULE, worker_code, [])),
    test_utils:wait_for_cluster_cast({?Node_Manager_Name, Node})
  end,
  lists:foreach(RunWorkerCode, WorkerNodes),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, ccm_code2, [])),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),
  {Workers, _} = gen_server:call({global, ?CCM}, get_workers),

  StartAdditionalWorker = fun(Node, Module) ->
    case lists:member({Node, Module}, Workers) of
      true -> ok;
      false ->
        StartAns = gen_server:call({global, ?CCM}, {start_worker, Node, Module, []}, 3000),
        ?assertEqual(ok, StartAns)
    end
  end,

  lists:foreach(fun(Node) -> StartAdditionalWorker(Node, cluster_rengine) end, NodesUp),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes + length(NodesUp) - 1),
  {ok, MonitoringInitialization} = rpc:call(CCM, application, get_env, [?APP_Name, cluster_monitoring_initialization]),
  timer:sleep(6 * 1000 * MonitoringInitialization),

  lists:append([{nodes, NodesUp}], Config).

end_per_testcase(distributed_test, Config) ->
  Nodes = ?config(nodes, Config),
  test_node_starter:stop_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, Nodes),
  test_node_starter:stop_test_nodes(Nodes),
  test_node_starter:stop_deps_for_tester_node();

end_per_testcase(_, Config) ->
  Nodes = ?config(nodes, Config),
  test_node_starter:stop_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, Nodes),
  test_node_starter:stop_test_nodes(Nodes).

subscribe_for_write_events(Node, ProcessingMethod, EventHandler) ->
  subscribe_for_write_events(Node, ProcessingMethod, EventHandler, #event_stream_config{}).

subscribe_for_write_events(Node, ProcessingMethod, EventHandler, ProcessingConfig) ->
  EventHandlerMapFun = fun(WriteEv) ->
    UserIdString = proplists:get_value("user_id", WriteEv),
    case UserIdString of
      undefined -> ok;
      _ -> string_to_integer(UserIdString)
    end
  end,

  EventHandlerDispMapFun = fun(WriteEv) ->
    UserIdString = proplists:get_value("user_id", WriteEv),
    case UserIdString of
      undefined -> ok;
      _ ->
        UserIdInt = string_to_integer(UserIdString),
        UserIdInt div 100
    end
  end,

  EventItem = #event_handler_item{processing_method = ProcessingMethod, handler_fun = EventHandler, map_fun = EventHandlerMapFun, disp_map_fun = EventHandlerDispMapFun, config = ProcessingConfig},
  gen_server:call({?Dispatcher_Name, Node}, {rule_manager, 1, self(), {add_event_handler, {"write_event_ct_test", EventItem}}}),

  receive
    ok -> ok
  after 5000 ->
    ?assert(false)
  end,

  timer:sleep(1000).

subscribe_for_read_events(Node, ProcessingMethod, EventHandler) ->
  subscribe_for_read_events(Node, ProcessingMethod, EventHandler, #event_stream_config{}).

subscribe_for_read_events(Node, ProcessingMethod, EventHandler, ProcessingConfig) ->
  EventHandlerMapFun = fun(ReadEv) ->
    UserIdString = proplists:get_value("user_id", ReadEv),
    case UserIdString of
      undefined -> ok;
      _ -> string_to_integer(UserIdString)
    end
  end,

  EventHandlerDispMapFun = fun(ReadEv) ->
    UserIdString = proplists:get_value("user_id", ReadEv),
    case UserIdString of
      undefined -> ok;
      _ ->
        UserIdInt = string_to_integer(UserIdString),
        UserIdInt div 100
    end
  end,

  EventItem = #event_handler_item{processing_method = ProcessingMethod, handler_fun = EventHandler, map_fun = EventHandlerMapFun, disp_map_fun = EventHandlerDispMapFun, config = ProcessingConfig},
  gen_server:call({?Dispatcher_Name, Node}, {rule_manager, 1, self(), {add_event_handler, {"read_event_ct_test", EventItem}}}),

  receive
    ok -> ok
  after 400 ->
    ?assert(false)
  end,

  timer:sleep(1000).

repeat(N, F) -> for(1, N, F).
for(N, N, F) -> [F()];
for(I, N, F) -> [F() | for(I + 1, N, F)].

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

string_to_integer(SomeString) ->
  {SomeInteger, _} = string:to_integer(SomeString),
  case SomeString of
    error ->
      throw(badarg);
    _ -> SomeInteger
  end.

assert_nothing_received() ->
  receive
    Ans = {event_handled, _, _} ->
      ct:print("assert nothing failed with: ~p", [Ans]),
      ?assert(false)
  after 1000
    -> ok
  end.

send_event(Event, Node) ->
  gen_server:call({?Dispatcher_Name, Node}, {cluster_rengine, 1, {event_arrived, Event}}).

count_answers(ToReceive) ->
  Set = sets:new(),
  receive
    {event_handled, _, Pid} -> count_answers(ToReceive - 1, sets:add_element(Pid, Set));
    _ -> count_answers(ToReceive, Set)
  after 2000 ->
    {ToReceive, Set}
  end.

count_answers(0, Set) ->
  {0, Set};
count_answers(ToReceive, Set) ->
  receive
    {'EXIT', _, _} -> count_answers(ToReceive, Set);
    {event_handled, _, Pid} -> count_answers(ToReceive - 1, sets:add_element(Pid, Set));
    _ -> count_answers(ToReceive, Set)
  after 2000 ->
    {ToReceive, Set}
  end.