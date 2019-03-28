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
-module(harvester_logic_test_SUITE).
-author("Jakub Kudzia").

-include("logic_tests_common.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

-export([
    get_test/1,
    subscribe_test/1,
    harvest_test/1,
    delete_entry_test/1]).

all() -> ?ALL([
    get_test,
    subscribe_test,
    harvest_test,
    delete_entry_test
]).

-define(FILE_ID, <<"dummyFileId">>).
-define(PAYLOAD, #{<<"dummyKey">> => <<"dummyValue">>}).
-define(INDICES, #{<<"dummyKey">> => <<"dummyValue">>}).
-define(SEQ, 123).
-define(MAX_SEQ, 456).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?HARVESTER_PRIVATE_DATA_MATCHER(?HARVESTER_1)},
        rpc:call(Node, harvester_logic, get, [?HARVESTER_1])
    ),

    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Harvester protected data should now be cached
    ?assertMatch(
        {ok, ?HARVESTER_PRIVATE_DATA_MATCHER(?HARVESTER_1)},
        rpc:call(Node, harvester_logic, get, [?HARVESTER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that provider can access non-cached data
    logic_tests_common:invalidate_cache(Config, od_harvester, ?HARVESTER_1),
    ?assertMatch(
        {ok, ?HARVESTER_PRIVATE_DATA_MATCHER(?HARVESTER_1)},
        rpc:call(Node, harvester_logic, get, [?HARVESTER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    % Simulate received updates
    Harvester1PrivateGRI = #gri{type = od_harvester, id = ?HARVESTER_1, aspect = instance, scope = private},
    Harvester1PrivateData = ?HARVESTER_PRIVATE_DATA_VALUE(?HARVESTER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % private scope
    ?assertMatch(
        {ok, ?HARVESTER_PRIVATE_DATA_MATCHER(?HARVESTER_1)},
        rpc:call(Node, harvester_logic, get, [?HARVESTER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ChangedData1 = Harvester1PrivateData#{
        <<"indices">> => ?HARVESTER_INDICES2(?HARVESTER_1),
        <<"spaces">> => ?HARVESTER_SPACES2(?HARVESTER_1)
    },
    PushMessage1 = #gs_push_graph{gri = Harvester1PrivateGRI, data = ChangedData1, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage1]),

    ?assertMatch(
        {ok, #document{key = ?HARVESTER_1, value = #od_harvester{
            indices = ?HARVESTER_INDICES2(?HARVESTER_1),
            spaces = ?HARVESTER_SPACES2(?HARVESTER_1)
        }}},
        rpc:call(Node, harvester_logic, get, [?HARVESTER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),


    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage4 = #gs_push_graph{gri = Harvester1PrivateGRI, change_type = deleted},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage4]),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_harvester, get, [?HARVESTER_1])
    ),

    % Simulate a 'nosub' push and see if cache was invalidated, fetch the
    % record first.
    ?assertMatch(
        {ok, ?HARVESTER_PRIVATE_DATA_MATCHER(?HARVESTER_1)},
        rpc:call(Node, harvester_logic, get, [?HARVESTER_1])
    ),

    PushMessage5 = #gs_push_nosub{gri = Harvester1PrivateGRI, reason = forbidden},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage5]),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_harvester, get, [?HARVESTER_1])
    ),

    ok.

harvest_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    
    FileId = <<"dummyFileId">>,
    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(ok, rpc:call(Node, harvester_logic, submit_entry, [?HARVESTER_1, FileId, ?PAYLOAD, ?HARVESTER_INDICES(?HARVESTER_1), 5, 10])),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(ok, rpc:call(Node, harvester_logic, submit_entry, [?HARVESTER_1, FileId, ?PAYLOAD, ?HARVESTER_INDICES(?HARVESTER_1), 5, 10])),
    % get result is cached now
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ok.

delete_entry_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    FileId = <<"dummyFileId">>,
    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(ok, rpc:call(Node, harvester_logic, delete_entry, [?HARVESTER_1, FileId, ?HARVESTER_INDICES(?HARVESTER_1), 5, 10])),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(ok, rpc:call(Node, harvester_logic, delete_entry, [?HARVESTER_1, FileId, ?HARVESTER_INDICES(?HARVESTER_1), 5, 10])),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ok.

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
    logic_tests_common:init_per_testcase(Config).

end_per_testcase(_, _Config) ->
    ok.

end_per_suite(Config) ->
    logic_tests_common:unmock_gs_client(Config),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================