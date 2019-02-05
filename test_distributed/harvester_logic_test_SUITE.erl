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
    submit_test/1
]).

all() -> ?ALL([
    get_test,
    subscribe_test,
    submit_test

]).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    % Users other than ROOT cannot access harvester data
    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, harvester_logic, get, [User1Sess, ?HARVESTER_1])
    ),

    ?assertMatch(
        {ok, ?HARVESTER_PROTECTED_DATA_MATCHER(?HARVESTER_1)},
        rpc:call(Node, harvester_logic, get, [?ROOT_SESS_ID, ?HARVESTER_1])
    ),

    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that other users cannot access cached data
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, harvester_logic, get, [User1Sess, ?HARVESTER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Harvester protected data should now be cached
    ?assertMatch(
        {ok, ?HARVESTER_PROTECTED_DATA_MATCHER(?HARVESTER_1)},
        rpc:call(Node, harvester_logic, get, [?ROOT_SESS_ID, ?HARVESTER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that provider can access non-cached data
    logic_tests_common:invalidate_cache(Config, od_harvester, ?HARVESTER_1),
    ?assertMatch(
        {ok, ?HARVESTER_PROTECTED_DATA_MATCHER(?HARVESTER_1)},
        rpc:call(Node, harvester_logic, get, [?ROOT_SESS_ID, ?HARVESTER_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that other users cannot access non-cached data
    logic_tests_common:invalidate_cache(Config, od_harvester, ?HARVESTER_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, harvester_logic, get, [User1Sess, ?HARVESTER_1])
    ),
    ?assertEqual(GraphCalls + 4, logic_tests_common:count_reqs(Config, graph)),

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
        rpc:call(Node, harvester_logic, get, [?ROOT_SESS_ID, ?HARVESTER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ChangedData1 = Harvester1ProtectedData#{<<"spaces">> => [<<"s1">>, <<"s2">>, <<"s3">>]},
    PushMessage1 = #gs_push_graph{gri = Harvester1ProtectedGRI, data = ChangedData1, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage1]),

    ?assertMatch(
        {ok, #document{key = ?HARVESTER_1, value = #od_harvester{
            spaces = [<<"s1">>, <<"s2">>, <<"s3">>]
        }}},
        rpc:call(Node, harvester_logic, get, [?ROOT_SESS_ID, ?HARVESTER_1])
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
        rpc:call(Node, harvester_logic, get, [?ROOT_SESS_ID, ?HARVESTER_1])
    ),

    PushMessage5 = #gs_push_nosub{gri = Harvester1ProtectedGRI, reason = forbidden},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage5]),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_harvester, get, [?HARVESTER_1])
    ),

    ok.


submit_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(ok, rpc:call(Node, harvester_logic, submit, [?ROOT_SESS_ID, ?HARVESTER_1, #{}])),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(ok, rpc:call(Node, harvester_logic, submit, [?ROOT_SESS_ID, ?HARVESTER_1, #{}])),
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