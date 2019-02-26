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

%%%===================================================================
%%% Test functions
%%%===================================================================

get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    % Users other than ROOT cannot access harvester data
    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?HARVESTER_PROTECTED_DATA_MATCHER(?HARVESTER_1)},
        rpc:call(Node, harvester_logic, get, [?HARVESTER_1])
    ),

    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Harvester protected data should now be cached
    ?assertMatch(
        {ok, ?HARVESTER_PROTECTED_DATA_MATCHER(?HARVESTER_1)},
        rpc:call(Node, harvester_logic, get, [?HARVESTER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that provider can access non-cached data
    logic_tests_common:invalidate_cache(Config, od_harvester, ?HARVESTER_1),
    ?assertMatch(
        {ok, ?HARVESTER_PROTECTED_DATA_MATCHER(?HARVESTER_1)},
        rpc:call(Node, harvester_logic, get, [?HARVESTER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    % Simulate received updates on different scopes (in rising order)
    Harvester1ProtectedGRI = #gri{type = od_harvester, id = ?HARVESTER_1, aspect = instance, scope = protected},
    Harvester1ProtectedData = ?HARVESTER_PROTECTED_DATA_VALUE(?HARVESTER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % protected scope
    ?assertMatch(
        {ok, ?HARVESTER_PROTECTED_DATA_MATCHER(?HARVESTER_1)},
        rpc:call(Node, harvester_logic, get, [?HARVESTER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ChangedData1 = Harvester1ProtectedData#{
        <<"entryTypeField">> => ?HARVESTER_ENTRY_TYPE_FIELD2(?HARVESTER_1),
        <<"defaultEntryType">> => ?HARVESTER_DEFAULT_ENTRY_TYPE2(?HARVESTER_1),
        <<"acceptedEntryTypes">> => ?HARVESTER_ACCEPTED_ENTRY_TYPES2(?HARVESTER_1)

    },
    PushMessage1 = #gs_push_graph{gri = Harvester1ProtectedGRI, data = ChangedData1, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage1]),

    ?assertMatch(
        {ok, #document{key = ?HARVESTER_1, value = #od_harvester{
            entry_type_field = ?HARVESTER_ENTRY_TYPE_FIELD2(?HARVESTER_1),
            default_entry_type = ?HARVESTER_DEFAULT_ENTRY_TYPE2(?HARVESTER_1),
            accepted_entry_types = ?HARVESTER_ACCEPTED_ENTRY_TYPES2(?HARVESTER_1)
        }}},
        rpc:call(Node, harvester_logic, get, [?HARVESTER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),


    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage4 = #gs_push_graph{gri = Harvester1ProtectedGRI, change_type = deleted},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage4]),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_harvester, get, [?HARVESTER_1])
    ),

    % Simulate a 'nosub' push and see if cache was invalidated, fetch the
    % record first.
    ?assertMatch(
        {ok, ?HARVESTER_PROTECTED_DATA_MATCHER(?HARVESTER_1)},
        rpc:call(Node, harvester_logic, get, [?HARVESTER_1])
    ),

    PushMessage5 = #gs_push_nosub{gri = Harvester1ProtectedGRI, reason = forbidden},
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

    tracer:start(Node),
    tracer:trace_calls(harvester_logic, prepare_payload),
    tracer:trace_calls(harvester_logic, submit_entry),

    ?assertMatch(ok, rpc:call(Node, harvester_logic, harvest, [?HARVESTER_1, FileId, #{}])),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(ok, rpc:call(Node, harvester_logic, harvest, [?HARVESTER_1, FileId, #{}])),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ok.

delete_entry_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    FileId = <<"dummyFileId">>,
    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(ok, rpc:call(Node, harvester_logic, delete_entry, [?HARVESTER_1, FileId])),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(ok, rpc:call(Node, harvester_logic, delete_entry, [?HARVESTER_1, FileId])),
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

end_per_testcase(_, Config) ->
    logic_tests_common:invalidate_cache(Config, od_harvester, ?HARVESTER_1),
    ok.

end_per_suite(Config) ->
    logic_tests_common:unmock_gs_client(Config),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================