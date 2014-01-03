%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module 
%% @end
%% ===================================================================

-module(central_logger_test_SUITE).
-include("nodes_manager.hrl").
-include("registered_names.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([init_and_cleanup_test/1, logging_test/1]).

%% export nodes' codes
-export([perform_10_logs/1, get_lager_traces/0, check_console_loglevel_functionalities/0]).

all() -> [logging_test, init_and_cleanup_test].

%% ====================================================================
%% Code of nodes used during the test
%% ====================================================================

perform_10_logs(0) ->
  ok;

perform_10_logs(NumberOfRepeats) ->
  lager:log(debug, [], "test_log"),
  lager:log(debug, [], "test_log"),
  lager:log(info, [], "test_log"),
  lager:log(info, [], "test_log"),

  lager:log(notice, [{pid, self()}], "test_log"),
  lager:log(warning, [{pid, self()}], "test_log"),

  lager:log(error, [{tag, tag_value}], "test_log"),
  lager:log(critical, [{tag, tag_value}], "test_log"),

  lager:log(alert, [{tag, tag_value}, {node, node()}], "test_log"),
  lager:log(emergency, [{tag, tag_value}, {node, node()}], "test_log"),

  perform_10_logs(NumberOfRepeats - 1).


get_lager_traces() ->
    gen_event:which_handlers(lager_event).   


%% This test function checks logger functionality of switching console loglevel (logger module)
check_console_loglevel_functionalities() ->  
  try
    ?assertEqual(ok, logger:set_console_loglevel(0)),
    ?assertEqual(logger:get_console_loglevel(), 0),

    ?assertEqual(ok, logger:set_console_loglevel(info)),
    ?assertEqual(logger:get_console_loglevel(), 1),

    ?assertEqual(ok, logger:set_console_loglevel(error)),
    ?assertEqual(logger:get_console_loglevel(), 4),

    ?assertEqual(ok, logger:set_console_loglevel(default)),
    {ok, Proplist} = application:get_env(lager, handlers),
    Default = proplists:get_value(lager_console_backend, Proplist),
    ?assertEqual(logger:get_console_loglevel(), logger:loglevel_atom_to_int(Default)),

    ok
  catch Type:Message ->
    {Type, Message}
  end.



%% ====================================================================
%% Test functions
%% ====================================================================


%% This test function checks if central_logger properly
%% sets up lager traces and if it cleans up after termination

init_and_cleanup_test(Config) ->
  nodes_manager:check_start_assertions(Config),

  NodesUp = ?config(nodes, Config),
  [CCM, W] = NodesUp,

  % Get standard trace configuration from worker node
  StandardTraces = rpc:call(W, ?MODULE, get_lager_traces, []),

  % Init cluster
  gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(3500),

  % Test logger's console loglevel switching functionalities
  ?assertEqual(ok, rpc:call(W, ?MODULE, check_console_loglevel_functionalities, [])),
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, check_console_loglevel_functionalities, [])),

  % Get new trace configuration after start of central_logger module
  NewTraces = rpc:call(W, ?MODULE, get_lager_traces, []),

  % Check if new configuration contains standard entries plus global log files
  lists:foreach(
    fun(Trace) ->
      ?assert(lists:member(Trace, NewTraces))
    end, StandardTraces), 
  ?assert(lists:member({lager_file_backend,"log/global_error.log"}, NewTraces)),
  ?assert(lists:member({lager_file_backend,"log/global_info.log"}, NewTraces)),
  ?assert(lists:member({lager_file_backend,"log/global_debug.log"}, NewTraces)),
  % And only those
  ?assertEqual(length(StandardTraces) + 3, length(NewTraces)),

  % Terminate central_logger worker
  gen_server:cast({global, ?CCM}, {stop_worker, W, central_logger}),
  timer:sleep(500),

  % Check if traces were reset to default
  TracesAfterCleanup = rpc:call(W, ?MODULE, get_lager_traces, []),
  ?assertEqual(StandardTraces, TracesAfterCleanup),

  % Check if lager still works at worker node
  ?assertEqual(ok, rpc:call(W, lager, log, [info, [], "log"])).



%% ====================================================
%% This test function checks following functionalities:
%% central_logging_backend:dispatch_log() 
%% central_logger subscribing and log stream

logging_test(Config) ->
  nodes_manager:check_start_assertions(Config),

  NodesUp = ?config(nodes, Config),
  [CCM, W1, W2, W3, W4] = NodesUp,

  % Init cluster
  gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  gen_server:cast({global, ?CCM}, init_cluster),
  timer:sleep(3000),

  % Subscribe for log stream
  Pid = self(),
  ?assertEqual(ok, gen_server:call({?Dispatcher_Name, W3}, {central_logger, 1, {subscribe, Pid}})),

  % Every call will produce [10 * Arg1] of logs
  ?assertEqual(ok, rpc:call(CCM, ?MODULE, perform_10_logs, [5])),
  ?assertEqual(ok, rpc:call(W1, ?MODULE, perform_10_logs, [3])),
  ?assertEqual(ok, rpc:call(W2, ?MODULE, perform_10_logs, [2])),
  ?assertEqual(ok, rpc:call(W3, ?MODULE, perform_10_logs, [11])),
  ?assertEqual(ok, rpc:call(W4, ?MODULE, perform_10_logs, [4])),
  
  % Assert that logs arrived and are correctly tagged
  check_logs(250),

  % Unsubscribe from log stream
  ?assertEqual(ok, gen_server:call({?Dispatcher_Name, W3}, {central_logger, 1, {unsubscribe, Pid}})),
  % Ask for subscribers list
  ?assertEqual(ok, gen_server:call({?Dispatcher_Name, W3}, {central_logger, 1, self(), message_id, get_subscribers})),
  % To confirm, that this pid is no longer subscribed
  ?assertEqual([], receive {worker_answer, message_id, Response} -> Response after 1000 -> timeout end).




%% ====================================================================
%% Auxiliary functions
%% ====================================================================

check_logs(ExpectedLogNumber) ->
  {StartTime, _} = statistics(wall_clock),
  {UnknownSourceLogs, ErrorLoggerLogs, LagerLogs} = count_logs(0, 0, 0, ExpectedLogNumber, StartTime),

  %% 4/10 of logs should have been classified as of unknown source
  ?assertEqual(ExpectedLogNumber div 10 * 4, UnknownSourceLogs),

  %% 2/10 of logs should have been classified as error_logger logs
  ?assertEqual(ExpectedLogNumber div 10 * 2, ErrorLoggerLogs),

  %% 4/10 of logs should have been classified as standard lager logs
  ?assertEqual(ExpectedLogNumber div 10 * 4, LagerLogs).


count_logs(UnknownSourceLogs, ErrorLoggerLogs, LagerLogs, Expected, StartTime) -> 
  {CurrentTime, _} = statistics(wall_clock),
  case CurrentTime - StartTime < 5000 of
    false -> 
      {UnknownSourceLogs, ErrorLoggerLogs, LagerLogs};
    true ->
      case UnknownSourceLogs + ErrorLoggerLogs + LagerLogs of
        Expected -> 
          {UnknownSourceLogs, ErrorLoggerLogs, LagerLogs};
        _ ->
          receive 
            {log, {"test_log", _, _, [{source, unknown}, {node, _}]}} -> 
              count_logs(UnknownSourceLogs + 1, ErrorLoggerLogs, LagerLogs, Expected, StartTime);

            {log, {"test_log", _, _, [{pid, _}, {source, error_logger}, {node, _}]}} -> 
              count_logs(UnknownSourceLogs, ErrorLoggerLogs + 1, LagerLogs, Expected, StartTime);

            {log, {"test_log", _, _, Metadata}} -> 
              true = (proplists:get_value(node, Metadata) /= undefined), 
              count_logs(UnknownSourceLogs, ErrorLoggerLogs, LagerLogs + 1, Expected, StartTime)

          after 100 ->
              count_logs(UnknownSourceLogs, ErrorLoggerLogs, LagerLogs, Expected, StartTime)
          end
      end
  end.




%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(logging_test, Config) ->
  ?INIT_DIST_TEST,
  nodes_manager:start_deps_for_tester_node(),

  Nodes = nodes_manager:start_test_on_nodes(5),
  [CCM | _] = Nodes,

  StartLog = nodes_manager:start_app_on_nodes(Nodes, 
    [[{node_type, ccm}, 
      {dispatcher_port, 5055}, 
      {ccm_nodes, [CCM]}, 
      {dns_port, 1308}],
    [{node_type, worker}, 
      {dispatcher_port, 5056}, 
      {ccm_nodes, [CCM]}, 
      {dns_port, 1309}],
    [{node_type, worker}, 
      {dispatcher_port, 5057}, 
      {ccm_nodes, [CCM]}, 
      {dns_port, 1310}],
    [{node_type, worker}, 
      {dispatcher_port, 5058}, 
      {ccm_nodes, [CCM]}, 
      {dns_port, 1311}],
    [{node_type, worker}, 
      {dispatcher_port, 5059}, 
      {ccm_nodes, [CCM]}, 
      {dns_port, 1312}]]),

  Assertions = [{false, lists:member(error, Nodes)}, {false, lists:member(error, StartLog)}],
  lists:append([{nodes, Nodes}, {assertions, Assertions}], Config);


init_per_testcase(init_and_cleanup_test, Config) ->
  ?INIT_DIST_TEST,
  nodes_manager:start_deps_for_tester_node(),

  Nodes = nodes_manager:start_test_on_nodes(2),
  [CCM | _] = Nodes,
   
  StartLog = nodes_manager:start_app_on_nodes(Nodes, 
    [[{node_type, ccm}, 
      {dispatcher_port, 5055}, 
      {ccm_nodes, [CCM]}, 
      {dns_port, 1308},
      {initialization_time, 1}],
    [{node_type, worker}, 
      {dispatcher_port, 5056}, 
      {ccm_nodes, [CCM]}, 
      {dns_port, 1309}]]),

  Assertions = [{false, lists:member(error, Nodes)}, {false, lists:member(error, StartLog)}],
  lists:append([{nodes, Nodes}, {assertions, Assertions}], Config).


end_per_testcase(_, Config) ->
  Nodes = ?config(nodes, Config),
  StopLog = nodes_manager:stop_app_on_nodes(Nodes),
  StopAns = nodes_manager:stop_nodes(Nodes),
  ?assertEqual(false, lists:member(error, StopLog)),
  ?assertEqual(ok, StopAns).
