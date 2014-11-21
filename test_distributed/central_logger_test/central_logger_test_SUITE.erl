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
-include("test_utils.hrl").
-include("registered_names.hrl").
-include("modules_and_args.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([init_and_cleanup_test/1, logging_test/1]).

%% export nodes' codes
-export([perform_10_logs/1, get_lager_traces/0, check_console_loglevel_functionalities/0]).

% Max time for the subscribing process to collect logs
-define(logs_collection_timeout, 10000).

all() -> [logging_test, init_and_cleanup_test].

%% ====================================================================
%% Code of nodes used during the test
%% ====================================================================

perform_10_logs(0) ->
    ok;

perform_10_logs(NumberOfRepeats) ->
    % Error logger logs
    error_logger:error_msg("test_log"),
    error_logger:error_msg("test_log"),

    error_logger:info_msg("test_log"),
    error_logger:info_msg("test_log"),

    % Lager logs
    lager:log(info, [], "test_log"),
    lager:log(info, [], "test_log"),

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

    NodesUp = ?config(nodes, Config),
    [CCM, W] = NodesUp,

    Workers = [W],
    % Get standard trace configuration from worker node
    StandardTraces = rpc:call(W, ?MODULE, get_lager_traces, []),

    DuplicatedPermanentNodes = (length(Workers) - 1) * length(?PERMANENT_MODULES),
    % Init cluster
    gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
    gen_server:cast({global, ?CCM}, {set_monitoring, on}),
    test_utils:wait_for_cluster_cast(),
    gen_server:cast({global, ?CCM}, init_cluster),
    test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

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
    ?assert(lists:member({lager_file_backend, "log/global_error.log"}, NewTraces)),
    ?assert(lists:member({lager_file_backend, "log/global_info.log"}, NewTraces)),
    ?assert(lists:member({lager_file_backend, "log/global_debug.log"}, NewTraces)),
    ?assert(lists:member({lager_file_backend, "log/client_error.log"}, NewTraces)),
    ?assert(lists:member({lager_file_backend, "log/client_info.log"}, NewTraces)),
    ?assert(lists:member({lager_file_backend, "log/client_debug.log"}, NewTraces)),
    % And only those
    ?assertEqual(length(StandardTraces) + 6, length(NewTraces)),

    % Terminate central_logger worker
    gen_server:cast({global, ?CCM}, {stop_worker, W, central_logger}),
    test_utils:wait_for_cluster_cast(),

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
    NodesUp = ?config(nodes, Config),
    [CCM, W1, W2, W3, W4] = NodesUp,
    WorkerNodes = [W1, W2, W3, W4],

    % Init cluster
    gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
    gen_server:cast({global, ?CCM}, {set_monitoring, on}),
    test_utils:wait_for_cluster_cast(),
    test_utils:wait_for_nodes_registration(length(NodesUp) - 1),
    gen_server:cast({global, ?CCM}, init_cluster),

    DuplicatedPermanentNodes = (length(WorkerNodes) - 1) * length(?PERMANENT_MODULES),
    test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

    % Subscribe for log stream
    Pid = self(),
    ?assertEqual(ok, gen_server:call({?Dispatcher_Name, W3}, {central_logger, 1, {subscribe, cluster, Pid}}, 1000)),

    % Every call will produce [10 * Arg1] of logs
    ?assertEqual(ok, rpc:call(CCM, ?MODULE, perform_10_logs, [5])),
    ?assertEqual(ok, rpc:call(W1, ?MODULE, perform_10_logs, [3])),
    ?assertEqual(ok, rpc:call(W2, ?MODULE, perform_10_logs, [2])),
    ?assertEqual(ok, rpc:call(W3, ?MODULE, perform_10_logs, [11])),
    ?assertEqual(ok, rpc:call(W4, ?MODULE, perform_10_logs, [4])),

    % Assert that logs arrived and are correctly tagged
    check_logs(250),

    % Unsubscribe from log stream
    ?assertEqual(ok, gen_server:call({?Dispatcher_Name, W3}, {central_logger, 1, {unsubscribe, cluster, Pid}}, 1000)),
    % Ask for subscribers list
    ?assertEqual(ok, gen_server:call({?Dispatcher_Name, W3}, {central_logger, 1, self(), message_id, {get_subscribers, cluster}}, 1000)),
    % To confirm, that this pid is no longer subscribed
    ?assertEqual([], receive {worker_answer, message_id, Response} -> Response after 1000 -> timeout end).


%% ====================================================================
%% Auxiliary functions
%% ====================================================================

check_logs(ExpectedLogNumber) ->
    {StartTime, _} = statistics(wall_clock),
    {ErrorLoggerLogs, LagerLogs} = count_logs(0, 0, ExpectedLogNumber, StartTime),

    %% 4/10 of logs should have been classified as error_logger logs
    ?assertEqual(ExpectedLogNumber div 10 * 4, ErrorLoggerLogs),

    %% 6/10 of logs should have been classified as standard lager logs
    ?assertEqual(ExpectedLogNumber div 10 * 6, LagerLogs).


count_logs(ErrorLoggerLogs, LagerLogs, Expected, StartTime) ->
    {CurrentTime, _} = statistics(wall_clock),
    case CurrentTime - StartTime < ?logs_collection_timeout of
        false ->
            {ErrorLoggerLogs, LagerLogs};
        true ->
            case ErrorLoggerLogs + LagerLogs of
                Expected ->
                    {ErrorLoggerLogs, LagerLogs};
                _ ->
                    receive
                        {log, {"test_log", _, _, [{node, _}, {pid, _}]}} ->
                            count_logs(ErrorLoggerLogs + 1, LagerLogs, Expected, StartTime);

                        {log, {"test_log", _, _, Metadata}} ->
                            true = (proplists:get_value(node, Metadata) /= undefined),
                            count_logs(ErrorLoggerLogs, LagerLogs + 1, Expected, StartTime)

                    after 100 ->
                        count_logs(ErrorLoggerLogs, LagerLogs, Expected, StartTime)
                    end
            end
    end.


%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(logging_test, Config) ->
    ?INIT_CODE_PATH, ?CLEAN_TEST_DIRS,
    test_node_starter:start_deps_for_tester_node(),

    Nodes = test_node_starter:start_test_nodes(5),
    [CCM | _] = Nodes,

    test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, Nodes,
        [[{node_type, ccm},
            {dispatcher_port, 5055},
            {ccm_nodes, [CCM]},
            {dns_port, 1308}, {control_panel_port, 2308}, {control_panel_redirect_port, 1354}, {rest_port, 3308}, {heart_beat, 1}],
            [{node_type, worker},
                {dispatcher_port, 5056},
                {ccm_nodes, [CCM]},
                {dns_port, 1309}, {control_panel_port, 2309}, {control_panel_redirect_port, 1355}, {gateway_listener_port, 3217}, {gateway_proxy_port, 3218}, {rest_port, 3309}, {heart_beat, 1}],
            [{node_type, worker},
                {dispatcher_port, 5057},
                {ccm_nodes, [CCM]},
                {dns_port, 1310}, {control_panel_port, 2310}, {control_panel_redirect_port, 1356}, {gateway_listener_port, 3219}, {gateway_proxy_port, 3220}, {rest_port, 3310}, {heart_beat, 1}],
            [{node_type, worker},
                {dispatcher_port, 5058},
                {ccm_nodes, [CCM]},
                {dns_port, 1311}, {control_panel_port, 2311}, {control_panel_redirect_port, 1357}, {gateway_listener_port, 3221}, {gateway_proxy_port, 3222}, {rest_port, 3311}, {heart_beat, 1}],
            [{node_type, worker},
                {dispatcher_port, 5059},
                {ccm_nodes, [CCM]},
                {dns_port, 1312}, {control_panel_port, 2312}, {control_panel_redirect_port, 1358}, {gateway_listener_port, 3223}, {gateway_proxy_port, 3224}, {rest_port, 3312}, {heart_beat, 1}]]),


    lists:append([{nodes, Nodes}], Config);

init_per_testcase(init_and_cleanup_test, Config) ->
    ?INIT_CODE_PATH, ?CLEAN_TEST_DIRS,
    test_node_starter:start_deps_for_tester_node(),

    Nodes = test_node_starter:start_test_nodes(2),
    [CCM | _] = Nodes,

    test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, Nodes,
        [[{node_type, ccm},
            {dispatcher_port, 5055},
            {ccm_nodes, [CCM]},
            {dns_port, 1308}, {control_panel_port, 2308}, {control_panel_redirect_port, 1354}, {rest_port, 3308}, {initialization_time, 1}, {heart_beat, 1}],
            [{node_type, worker},
                {dispatcher_port, 5056},
                {ccm_nodes, [CCM]},
                {dns_port, 1309}, {control_panel_port, 2309}, {control_panel_redirect_port, 1355}, {rest_port, 3309}, {gateway_listener_port, 3217}, {gateway_proxy_port, 3218}, {heart_beat, 1}]]),


    lists:append([{nodes, Nodes}], Config).

end_per_testcase(_, Config) ->
    Nodes = ?config(nodes, Config),
    test_node_starter:stop_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, Nodes),
    test_node_starter:stop_test_nodes(Nodes).
