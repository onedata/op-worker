%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests harvester logic API using mocked gs_client module.
%%% @end
%%%--------------------------------------------------------------------
-module(harvest_manager_test_SUITE).
-author("Jakub Kudzia").

-include("logic_tests_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").


%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

-export([
    manager_and_stream_sup_should_be_started/1,
    manager_should_be_restarted_when_stream_supervisor_dies/1,
    supervisor_should_be_restarted_when_stream_supervisor_dies/1,
    one_harvest_stream_should_be_started_for_one_space/1,
    many_harvest_streams_should_be_started_for_one_space/1,
    many_harvest_streams_should_be_started_for_many_spaces/1,
    harvest_stream_should_be_stopped_for_space/1,
    many_harvest_streams_should_be_stopped/1,
    many_harvest_streams_should_be_stopped_for_many_spaces/1,
    all_harvest_streams_should_be_stopped_when_the_space_is_deleted/1]).

all() -> ?ALL([
    manager_and_stream_sup_should_be_started,
    manager_should_be_restarted_when_stream_supervisor_dies,
    supervisor_should_be_restarted_when_stream_supervisor_dies,
    one_harvest_stream_should_be_started_for_one_space,
    many_harvest_streams_should_be_started_for_one_space,
    many_harvest_streams_should_be_started_for_many_spaces,
    harvest_stream_should_be_stopped_for_space,
    many_harvest_streams_should_be_stopped,
    many_harvest_streams_should_be_stopped_for_many_spaces,
    all_harvest_streams_should_be_stopped_when_the_space_is_deleted
]).

-define(ATTEMPTS, 10).

%%%===================================================================
%%% Test functions
%%%===================================================================

manager_and_stream_sup_should_be_started(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Node) -> manager_and_stream_sup_should_be_started(Config, Node) end, Nodes).

manager_should_be_restarted_when_stream_supervisor_dies(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Node) -> manager_should_be_restarted_when_stream_supervisor_dies(Config, Node) end, Nodes).

supervisor_should_be_restarted_when_stream_supervisor_dies(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Node) -> supervisor_should_be_restarted_when_stream_supervisor_dies(Config, Node) end, Nodes).

one_harvest_stream_should_be_started_for_one_space(Config) ->
    Nodes = [Node | _] = ?config(op_worker_nodes, Config),

    Space1GRI = #gri{type = od_space, id = ?SPACE_1, aspect = instance, scope = private},
    ChangedData1 = ?SPACE_PRIVATE_DATA_VALUE(?SPACE_1),
    PushMessage1 = #gs_push_graph{gri = Space1GRI, data = ChangedData1, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage1]),

    ?assertMatch(1, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS).

many_harvest_streams_should_be_started_for_one_space(Config) ->
    Nodes = [Node | _] = ?config(op_worker_nodes, Config),

    HarvestersNum = 10,
    Harvesters = [integer_to_binary(I) || I <- lists:seq(1, HarvestersNum)],

    Space1GRI = #gri{type = od_space, id = ?SPACE_1, aspect = instance, scope = private},
    Space1Data = ?SPACE_PRIVATE_DATA_VALUE(?SPACE_1),
    ChangedData1 = Space1Data#{<<"harvesters">> => Harvesters},
    PushMessage1 = #gs_push_graph{gri = Space1GRI, data = ChangedData1, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage1]),

    ?assertMatch(HarvestersNum, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS).

many_harvest_streams_should_be_started_for_many_spaces(Config) ->
    Nodes = [Node1, Node2 | _] = ?config(op_worker_nodes, Config),

    ?assertEqual(0, count_active_children(Nodes, harvest_stream_sup)),

    Space1GRI = #gri{type = od_space, id = ?SPACE_1, aspect = instance, scope = private},
    Space1Data = ?SPACE_PRIVATE_DATA_VALUE(?SPACE_1),

    Space2GRI = #gri{type = od_space, id = ?SPACE_2, aspect = instance, scope = private},
    Space2Data = ?SPACE_PRIVATE_DATA_VALUE(?SPACE_2),

    HarvestersNum1 = 10,
    HarvestersNum2 = 100,
    HarvestersTotal = HarvestersNum1 + HarvestersNum2,
    Harvesters1 = [integer_to_binary(I) || I <- lists:seq(1, HarvestersNum1)],
    Harvesters2 = [integer_to_binary(I) || I <- lists:seq(1, HarvestersNum2)],

    ChangedData1 = Space1Data#{<<"harvesters">> => Harvesters1},
    ChangedData2 = Space2Data#{<<"harvesters">> => Harvesters2},
    PushMessage1 = #gs_push_graph{gri = Space1GRI, data = ChangedData1, change_type = updated},
    PushMessage2 = #gs_push_graph{gri = Space2GRI, data = ChangedData2, change_type = updated},

    rpc:call(Node1, gs_client_worker, process_push_message, [PushMessage1]),
    rpc:call(Node2, gs_client_worker, process_push_message, [PushMessage2]),

    ?assertMatch(HarvestersTotal, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS).

harvest_stream_should_be_stopped_for_space(Config) ->
    Nodes = [Node | _] = ?config(op_worker_nodes, Config),

    ?assertEqual(0, count_active_children(Nodes, harvest_stream_sup)),

    Space1GRI = #gri{type = od_space, id = ?SPACE_1, aspect = instance, scope = private},
    ChangedData1 = ?SPACE_PRIVATE_DATA_VALUE(?SPACE_1),
    PushMessage1 = #gs_push_graph{gri = Space1GRI, data = ChangedData1, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage1]),

    ?assertMatch(1, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS),

    ChangedData2 = ChangedData1#{<<"harvesters">> => []},
    PushMessage2 = #gs_push_graph{gri = Space1GRI, data = ChangedData2, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage2]),

    ?assertMatch(0, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS).

many_harvest_streams_should_be_stopped(Config) ->
    Nodes = [Node | _] = ?config(op_worker_nodes, Config),

    HarvestersNum = 10,
    Harvesters = [integer_to_binary(I) || I <- lists:seq(1, HarvestersNum)],

    Space1GRI = #gri{type = od_space, id = ?SPACE_1, aspect = instance, scope = private},
    Space1Data = ?SPACE_PRIVATE_DATA_VALUE(?SPACE_1),
    ChangedData1 = Space1Data#{<<"harvesters">> => Harvesters},
    PushMessage1 = #gs_push_graph{gri = Space1GRI, data = ChangedData1, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage1]),

    ?assertMatch(HarvestersNum, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS),

    ChangedData2 = ChangedData1#{<<"harvesters">> => []},
    PushMessage2 = #gs_push_graph{gri = Space1GRI, data = ChangedData2, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage2]),

    ?assertMatch(0, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS).

many_harvest_streams_should_be_stopped_for_many_spaces(Config) ->
    Nodes = [Node1, Node2 | _] = ?config(op_worker_nodes, Config),

    ?assertEqual(0, count_active_children(Nodes, harvest_stream_sup)),

    Space1GRI = #gri{type = od_space, id = ?SPACE_1, aspect = instance, scope = private},
    Space1Data = ?SPACE_PRIVATE_DATA_VALUE(?SPACE_1),

    Space2GRI = #gri{type = od_space, id = ?SPACE_2, aspect = instance, scope = private},
    Space2Data = ?SPACE_PRIVATE_DATA_VALUE(?SPACE_2),

    HarvestersNum1 = 10,
    HarvestersNum2 = 100,
    HarvestersTotal = HarvestersNum1 + HarvestersNum2,
    Harvesters1 = [integer_to_binary(I) || I <- lists:seq(1, HarvestersNum1)],
    Harvesters2 = [integer_to_binary(I) || I <- lists:seq(1, HarvestersNum2)],

    ChangedData1 = Space1Data#{<<"harvesters">> => Harvesters1},
    ChangedData2 = Space2Data#{<<"harvesters">> => Harvesters2},
    PushMessage1 = #gs_push_graph{gri = Space1GRI, data = ChangedData1, change_type = updated},
    PushMessage2 = #gs_push_graph{gri = Space2GRI, data = ChangedData2, change_type = updated},

    rpc:call(Node1, gs_client_worker, process_push_message, [PushMessage1]),
    rpc:call(Node2, gs_client_worker, process_push_message, [PushMessage2]),

    ?assertMatch(HarvestersTotal, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS),

    ChangedData12 = ChangedData1#{<<"harvesters">> => []},
    ChangedData22 = ChangedData2#{<<"harvesters">> => []},

    PushMessage12 = #gs_push_graph{gri = Space1GRI, data = ChangedData12, change_type = updated},
    PushMessage22 = #gs_push_graph{gri = Space2GRI, data = ChangedData22, change_type = updated},
    rpc:call(Node2, gs_client_worker, process_push_message, [PushMessage12]),
    rpc:call(Node1, gs_client_worker, process_push_message, [PushMessage22]),

    ?assertMatch(0, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS).

all_harvest_streams_should_be_stopped_when_the_space_is_deleted(Config) ->
    Nodes = [Node | _] = ?config(op_worker_nodes, Config),

    HarvestersNum = 1,
    Harvesters = [integer_to_binary(I) || I <- lists:seq(1, HarvestersNum)],

    Space1GRI = #gri{type = od_space, id = ?SPACE_1, aspect = instance, scope = private},
    Space1Data = ?SPACE_PRIVATE_DATA_VALUE(?SPACE_1),
    ChangedData1 = Space1Data#{<<"harvesters">> => Harvesters},
    PushMessage1 = #gs_push_graph{gri = Space1GRI, data = ChangedData1, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage1]),

    ?assertMatch(HarvestersNum, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS),

    PushMessage2 = #gs_push_graph{gri = Space1GRI, data = undefined, change_type = deleted},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage2]),

    ?assertMatch(0, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS).

%%%===================================================================
%%% Test bases functions
%%%===================================================================

manager_and_stream_sup_should_be_started(_Config, Node) ->
    ?assertNotEqual(undefined, whereis(Node, harvest_manager)),
    ?assertNotEqual(undefined, whereis(Node, harvest_stream_sup)),
    ?assertMatch([
        {specs, 2},
        {active, 2},
        {supervisors, 1},
        {workers, 1}
    ], rpc:call(Node, supervisor, count_children, [harvest_worker_sup])).


manager_should_be_restarted_when_stream_supervisor_dies(_Config, Node) ->
    ManagerPid =  whereis(Node, harvest_manager),
    SupervisorPid = whereis(Node, harvest_stream_sup),

    exit(SupervisorPid, kill),

    ?assertMatch([
        {specs, 2},
        {active, 2},
        {supervisors, 1},
        {workers, 1}
    ], rpc:call(Node, supervisor, count_children, [harvest_worker_sup]), ?ATTEMPTS),

    ?assertNotEqual(undefined, whereis(Node, harvest_manager), ?ATTEMPTS),
    ?assertNotEqual(ManagerPid, whereis(Node, harvest_manager), ?ATTEMPTS),
    ?assertNotEqual(undefined, whereis(Node, harvest_stream_sup),10),
    ?assertNotEqual(SupervisorPid, whereis(Node, harvest_stream_sup), ?ATTEMPTS).

supervisor_should_be_restarted_when_stream_supervisor_dies(_Config, Node) ->
    ManagerPid =  whereis(Node, harvest_manager),
    SupervisorPid = whereis(Node, harvest_stream_sup),

    exit(ManagerPid, kill),

    ?assertMatch([
        {specs, 2},
        {active, 2},
        {supervisors, 1},
        {workers, 1}
    ], rpc:call(Node, supervisor, count_children, [harvest_worker_sup]), ?ATTEMPTS),

    ?assertNotEqual(undefined, whereis(Node, harvest_manager), ?ATTEMPTS),
    ?assertNotEqual(ManagerPid, whereis(Node, harvest_manager), ?ATTEMPTS),
    ?assertNotEqual(undefined, whereis(Node, harvest_stream_sup),10),
    ?assertNotEqual(SupervisorPid, whereis(Node, harvest_stream_sup), ?ATTEMPTS).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        logic_tests_common:mock_gs_client(NewConfig),
        NewConfig
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [logic_tests_common, initializer]} | Config].

init_per_testcase(_, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(Nodes, space_logic),
    ok = test_utils:mock_expect(Nodes, space_logic, is_supported, fun(_, _) -> true end),
    ok = test_utils:mock_new(Nodes, couchbase_changes_stream),
    ok = test_utils:mock_expect(Nodes, couchbase_changes_stream, start_link, fun(_, _, _, _, _) -> {ok, ok} end),
    logic_tests_common:init_per_testcase(Config).

end_per_testcase(_, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Nodes, space_logic),
    ok = test_utils:mock_unload(Nodes, couchbase_changes_stream),
    lists:foreach(fun(Node) ->
        true = rpc:call(Node, erlang, exit, [whereis(Node, harvest_manager), kill])
    end, Nodes),
    ?assertMatch(0, catch count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS),
    ok.

end_per_suite(Config) ->
    logic_tests_common:unmock_gs_client(Config),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

count_active_children(Nodes, Ref) ->
    lists:foldl(fun(Node, Sum) ->
        Result = rpc:call(Node, supervisor, count_children, [Ref]),
        Sum + proplists:get_value(active, Result)
    end, 0, Nodes).

whereis(Node, Name) ->
    rpc:call(Node, erlang, whereis, [Name]).