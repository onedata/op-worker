%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests harvest_worker supervision tree.
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
    stream_supervisor_should_be_restarted/1,
    one_harvest_stream_should_be_started_for_one_index/1,
    harvest_stream_should_be_started_for_each_index_one_space_one_harvester/1,
    harvest_stream_should_be_started_for_each_index_many_spaces_one_harvester/1,
    harvest_stream_should_be_started_for_each_index_many_spaces_many_harvesters/1,
    adding_index_to_harvester_should_start_new_stream/1,
    adding_space_to_harvester_should_start_new_stream/1,
    harvest_stream_should_be_stopped_when_harvester_is_deleted/1,
    harvest_stream_should_be_stopped_when_index_is_deleted_from_harvester/1,
    harvest_stream_should_be_stopped_when_space_is_deleted_from_harvester/1,
    start_stop_streams_mixed_test/1
]).

all() -> ?ALL([
    manager_and_stream_sup_should_be_started,
    manager_should_be_restarted_when_stream_supervisor_dies,
    stream_supervisor_should_be_restarted,
    one_harvest_stream_should_be_started_for_one_index,
    harvest_stream_should_be_started_for_each_index_one_space_one_harvester,
    harvest_stream_should_be_started_for_each_index_many_spaces_one_harvester,
    harvest_stream_should_be_started_for_each_index_many_spaces_many_harvesters,
    adding_index_to_harvester_should_start_new_stream,
    adding_space_to_harvester_should_start_new_stream,
    harvest_stream_should_be_stopped_when_harvester_is_deleted,
    harvest_stream_should_be_stopped_when_index_is_deleted_from_harvester,
    harvest_stream_should_be_stopped_when_space_is_deleted_from_harvester,
    start_stop_streams_mixed_test
]).

-define(ATTEMPTS, 30).

%%%===================================================================
%%% Test functions
%%%===================================================================

manager_and_stream_sup_should_be_started(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Node) -> manager_and_stream_sup_should_be_started(Config, Node) end, Nodes).

manager_should_be_restarted_when_stream_supervisor_dies(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Node) -> manager_should_be_restarted_when_stream_supervisor_dies(Config, Node) end, Nodes).

stream_supervisor_should_be_restarted(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Node) -> stream_supervisor_should_be_restarted(Config, Node) end, Nodes).

one_harvest_stream_should_be_started_for_one_index(Config) ->
    update_harvest_streams_test_base(Config, #{
        ?HARVESTER_1 => #{indices => 1, spaces => 1}
    }).

harvest_stream_should_be_started_for_each_index_one_space_one_harvester(Config) ->
    update_harvest_streams_test_base(Config, #{
        ?HARVESTER_1 => #{indices => 10, spaces => 1}
    }).

harvest_stream_should_be_started_for_each_index_many_spaces_one_harvester(Config) ->
    update_harvest_streams_test_base(Config, #{
        ?HARVESTER_1 => #{indices => 10, spaces => 5}
    }).

harvest_stream_should_be_started_for_each_index_many_spaces_many_harvesters(Config) ->
    update_harvest_streams_test_base(Config, #{
        ?HARVESTER_1 => #{indices => 10, spaces => 50},
        ?HARVESTER_2 => #{indices => 100, spaces => 5}
    }).

adding_index_to_harvester_should_start_new_stream(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    update_harvest_streams_test_base(Config, #{
        ?HARVESTER_1 => #{indices => 1, spaces => 1}
    }),
    ?assertMatch(1, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS),
    update_harvest_streams_test_base(Config, #{
        ?HARVESTER_1 => #{indices => 2, spaces => 1}
    }),
    ?assertMatch(2, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS).

adding_space_to_harvester_should_start_new_stream(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    update_harvest_streams_test_base(Config, #{
        ?HARVESTER_1 => #{indices => 1, spaces => 1}
    }),
    ?assertMatch(1, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS),
    update_harvest_streams_test_base(Config, #{
        ?HARVESTER_1 => #{indices => 1, spaces => 2}
    }),
    ?assertMatch(2, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS).

harvest_stream_should_be_stopped_when_harvester_is_deleted(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    Timeout = 10,
    test_utils:set_env(Nodes, ?APP_NAME, harvest_stream_healthcheck_interval, timer:seconds(Timeout)),
    update_harvest_streams_test_base(Config, #{?HARVESTER_1 => #{indices => 1, spaces => 1}}),
    ?assertMatch(1, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS),
    update_harvest_streams_test_base(Config, #{}),
    ?assertMatch(0, count_active_children(Nodes, harvest_stream_sup), Timeout).

harvest_stream_should_be_stopped_when_index_is_deleted_from_harvester(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    Timeout = 10,
    test_utils:set_env(Nodes, ?APP_NAME, harvest_stream_healthcheck_interval, timer:seconds(Timeout)),
    update_harvest_streams_test_base(Config, #{?HARVESTER_1 => #{indices => 1, spaces => 1}}),
    ?assertMatch(1, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS),
    update_harvest_streams_test_base(Config, #{?HARVESTER_1 => #{indices => 0, spaces => 1}}),
    ?assertMatch(0, count_active_children(Nodes, harvest_stream_sup), Timeout).

harvest_stream_should_be_stopped_when_space_is_deleted_from_harvester(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    Timeout = 10,
    test_utils:set_env(Nodes, ?APP_NAME, harvest_stream_healthcheck_interval, timer:seconds(Timeout)),
    update_harvest_streams_test_base(Config, #{?HARVESTER_1 => #{indices => 1, spaces => 1}}),
    ?assertMatch(1, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS),
    update_harvest_streams_test_base(Config, #{?HARVESTER_1 => #{indices => 1, spaces => 0}}),
    ?assertMatch(0, count_active_children(Nodes, harvest_stream_sup), Timeout).

start_stop_streams_mixed_test(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    update_harvest_streams_test_base(Config, #{
        ?HARVESTER_1 => #{indices => 1, spaces => 10}
    }),
    ?assertMatch(10, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS),

    update_harvest_streams_test_base(Config, #{
        ?HARVESTER_1 => #{indices => 1, spaces => 1},
        ?HARVESTER_2 => #{indices => 100, spaces => 25}
    }),

    ?assertMatch(2501, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS),
    update_harvest_streams_test_base(Config, #{?HARVESTER_2 => #{indices => 3, spaces => 5}}),
    ?assertMatch(15, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS),
    update_harvest_streams_test_base(Config, #{}),
    ?assertMatch(0, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS).

%%%===================================================================
%%% Test bases functions
%%%===================================================================

manager_and_stream_sup_should_be_started(_Config, Node) ->
    ?assertNotEqual(undefined, whereis(Node, harvest_stream_sup)),
    ?assertMatch([
        {specs, 2},
        {active, 2},
        {supervisors, 1},
        {workers, 1}
    ], rpc:call(Node, supervisor, count_children, [harvest_worker_sup])).

manager_should_be_restarted_when_stream_supervisor_dies(_Config, Node) ->
    ManagerPid = whereis(Node, harvest_manager),
    SupervisorPid = whereis(Node, harvest_stream_sup),
    exit(SupervisorPid, kill),
    ?assertMatch([
        {specs, 2},
        {active, 2},
        {supervisors, 1},
        {workers, 1}
    ], rpc:call(Node, supervisor, count_children, [harvest_worker_sup]), ?ATTEMPTS),
    ?assertNotEqual(undefined, whereis(Node, harvest_manager), ?ATTEMPTS),
    ?assertNotEqual(undefined, whereis(Node, harvest_stream_sup), ?ATTEMPTS),
    ?assertNotEqual(ManagerPid, whereis(Node, harvest_manager), ?ATTEMPTS),
    ?assertNotEqual(SupervisorPid, whereis(Node, harvest_stream_sup), ?ATTEMPTS).

stream_supervisor_should_be_restarted(_Config, Node) ->
    ManagerPid = whereis(Node, harvest_manager),
    SupervisorPid = whereis(Node, harvest_stream_sup),
    exit(SupervisorPid, kill),
    ?assertMatch([
        {specs, 2},
        {active, 2},
        {supervisors, 1},
        {workers, 1}
    ], rpc:call(Node, supervisor, count_children, [harvest_worker_sup]), ?ATTEMPTS),
    ?assertNotEqual(undefined, whereis(Node, harvest_manager), ?ATTEMPTS),
    ?assertNotEqual(undefined, whereis(Node, harvest_stream_sup), ?ATTEMPTS),
    ?assertNotEqual(ManagerPid, whereis(Node, harvest_manager), ?ATTEMPTS),
    ?assertNotEqual(SupervisorPid, whereis(Node, harvest_stream_sup), ?ATTEMPTS).

update_harvest_streams_test_base(Config, HarvestersDescription) ->
    [Node1 | _] = Nodes = ?config(op_worker_nodes, Config),
    Node = random_element(Nodes),
    ProviderId = provider_id(Node1),
    HarvestersConfig = harvesters_config(HarvestersDescription),
    mock_harvester_logic_get(Nodes, HarvestersConfig),

    TotalIndicesNum = maps:fold(fun(_, #document{value=OH}, AccIn) ->
        length(OH#od_harvester.indices) * length(OH#od_harvester.spaces) + AccIn
    end, 0, HarvestersConfig),

    Harvesters = maps:keys(HarvestersConfig),
    {ok, D = #document{value = OD}} = rpc:call(Node, provider_logic, get, [ProviderId]),
    rpc:call(Node, od_provider, save, [D#document{
        value = OD#od_provider{eff_harvesters = Harvesters}
    }]),

    ?assertMatch(TotalIndicesNum, count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [initializer]} | Config].

init_per_testcase(_, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(Nodes, harvester_logic),
    ok = test_utils:mock_new(Nodes, couchbase_changes_stream),
    ok = test_utils:mock_expect(Nodes, couchbase_changes_stream, start_link, fun(_, _, _, _, _) -> {ok, ok} end),
    ?assertEqual(0, count_active_children(Nodes, harvest_stream_sup)),
    initializer:communicator_mock(Nodes),
    initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config).

end_per_testcase(_, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Nodes, harvester_logic),
    ok = test_utils:mock_unload(Nodes, couchbase_changes_stream),
    lists:foreach(fun(Node) ->
        true = rpc:call(Node, erlang, exit, [whereis(Node, harvest_stream_sup), kill])
    end, Nodes),
    ?assertMatch(0, catch count_active_children(Nodes, harvest_stream_sup), ?ATTEMPTS),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Nodes, [communicator]).

end_per_suite(_Config) ->
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

provider_id(Node) ->
    rpc:call(Node, oneprovider, get_id, []).

harvesters_config(HarvestersDescription) ->
    maps:fold(fun(HarvesterId, HarvesterConfig, HarvestersConfigIn) ->
        HarvestersConfigIn#{
            HarvesterId => #document{
                key = HarvesterId,
                value = #od_harvester{
                    indices = [<<"index", (integer_to_binary(I))/binary>> || I <- lists:seq(1, maps:get(indices, HarvesterConfig, 0))],
                    spaces = [<<"space", (integer_to_binary(I))/binary>> || I <- lists:seq(1, maps:get(spaces, HarvesterConfig, 0))]
                }
            }
        }
    end, #{}, HarvestersDescription).

mock_harvester_logic_get(Nodes, HarvestersConfig) ->
    ok = test_utils:mock_expect(Nodes, harvester_logic, get, fun(HarvesterId) ->
        od_harvester:save(maps:get(HarvesterId, HarvestersConfig)),
        {ok, maps:get(HarvesterId, HarvestersConfig)}
    end).

count_active_children(Nodes, Ref) ->
    lists:foldl(fun(Node, Sum) ->
        Result = rpc:call(Node, supervisor, count_children, [Ref]),
        Sum + proplists:get_value(active, Result)
    end, 0, Nodes).

whereis(Node, Name) ->
    rpc:call(Node, erlang, whereis, [Name]).

random_element(List) ->
    lists:nth(rand:uniform(length(List)), List).