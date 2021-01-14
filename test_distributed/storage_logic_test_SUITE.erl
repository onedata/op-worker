%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests storage logic API using mocked gs_client module.
%%% @end
%%%--------------------------------------------------------------------
-module(storage_logic_test_SUITE).
-author("Michal Stanisz").

-include("logic_tests_common.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

-export([
    get_test/1,
    get_shared_data_test/1,
    mixed_get_test/1,
    subscribe_test/1,
    resupport_cleanup_test/1,
    revise_supported_spaces_test/1
]).

all() -> ?ALL([
    get_test,
    get_shared_data_test,
    mixed_get_test,
    subscribe_test,
    resupport_cleanup_test,
    revise_supported_spaces_test
]).


%%%===================================================================
%%% Test functions
%%%===================================================================

get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    
    StorageGriMatcher = #gri{type = od_storage, id = ?STORAGE_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, StorageGriMatcher),

    Storage1Provider = ?STORAGE_PROVIDER(?STORAGE_1),

    ?assertMatch(
        {ok, ?STORAGE_PRIVATE_DATA_MATCHER(?STORAGE_1, Storage1Provider)},
        rpc:call(Node, storage_logic, get, [?STORAGE_1])
    ),

    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, StorageGriMatcher)),

    % Storage private data should now be cached
    ?assertMatch(
        {ok, ?STORAGE_PRIVATE_DATA_MATCHER(?STORAGE_1, Storage1Provider)},
        rpc:call(Node, storage_logic, get, [?STORAGE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, StorageGriMatcher)),

    % Make sure that provider can access non-cached data
    logic_tests_common:invalidate_cache(Config, od_storage, ?STORAGE_1),
    ?assertMatch(
        {ok, ?STORAGE_PRIVATE_DATA_MATCHER(?STORAGE_1, Storage1Provider)},
        rpc:call(Node, storage_logic, get, [?STORAGE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, StorageGriMatcher)),
    ok.


get_shared_data_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    % cache space records that cause additional calls for storage records
    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1]),
    rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_2]),

    logic_tests_common:invalidate_cache(Config, od_storage, ?STORAGE_2),
    StorageGriMatcher = #gri{type = od_storage, id = ?STORAGE_2, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, StorageGriMatcher),

    Storage2Provider = ?STORAGE_PROVIDER(?STORAGE_2),

    ?assertMatch(
        {ok, ?STORAGE_SHARED_DATA_MATCHER(?STORAGE_2, Storage2Provider)},
        rpc:call(Node, storage_logic, get_shared_data, [?STORAGE_2, ?SPACE_1])
    ),

    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, StorageGriMatcher)),

    % Storage private data should now be cached
    ?assertMatch(
        {ok, ?STORAGE_SHARED_DATA_MATCHER(?STORAGE_2, Storage2Provider)},
        rpc:call(Node, storage_logic, get_shared_data, [?STORAGE_2, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, StorageGriMatcher)),

    % Make sure that provider can access non-cached data
    logic_tests_common:invalidate_cache(Config, od_storage, ?STORAGE_2),
    ?assertMatch(
        {ok, ?STORAGE_SHARED_DATA_MATCHER(?STORAGE_2, Storage2Provider)},
        rpc:call(Node, storage_logic, get_shared_data, [?STORAGE_2, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, StorageGriMatcher)),
    ok.


mixed_get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    % cache space records that cause additional calls for storage records
    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1]),
    rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_2]),

    logic_tests_common:invalidate_cache(Config, od_storage, ?STORAGE_1),
    StorageGriMatcher = #gri{type = od_storage, id = ?STORAGE_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, StorageGriMatcher),
    UnsubCalls = logic_tests_common:count_reqs(Config, unsub, StorageGriMatcher),

    Storage1Provider = ?STORAGE_PROVIDER(?STORAGE_1),

    % Fetching rising scopes should cause an unsub and new fetch every time
    ?assertMatch(
        {ok, ?STORAGE_SHARED_DATA_MATCHER(?STORAGE_1, Storage1Provider)},
        rpc:call(Node, storage_logic, get_shared_data, [?STORAGE_1, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, StorageGriMatcher)),
    ?assertEqual(UnsubCalls, logic_tests_common:count_reqs(Config, unsub, StorageGriMatcher)),

    ?assertMatch(
        {ok, ?STORAGE_PRIVATE_DATA_MATCHER(?STORAGE_1, Storage1Provider)},
        rpc:call(Node, storage_logic, get, [?STORAGE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, StorageGriMatcher)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub, StorageGriMatcher)),

    % When private data is cached, any scope should always be fetched from cache

    ?assertMatch(
        {ok, ?STORAGE_PRIVATE_DATA_MATCHER(?STORAGE_1, Storage1Provider)},
        rpc:call(Node, storage_logic, get, [?STORAGE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, StorageGriMatcher)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub, StorageGriMatcher)),

    ?assertMatch(
        {ok, ?STORAGE_SHARED_DATA_MATCHER(?STORAGE_1, Storage1Provider)},
        rpc:call(Node, storage_logic, get_shared_data, [?STORAGE_1, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, StorageGriMatcher)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub, StorageGriMatcher)),

    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    logic_tests_common:invalidate_cache(Config, od_storage, ?STORAGE_1),
    % Simulate received updates
    Storage1PrivateGRI = #gri{type = od_storage, id = ?STORAGE_1, aspect = instance, scope = private},
    Storage1PrivateData = ?STORAGE_PRIVATE_DATA_VALUE(?STORAGE_1),
    
    StorageGriMatcher = #gri{type = od_storage, id = ?STORAGE_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, StorageGriMatcher),

    Storage1Provider = ?STORAGE_PROVIDER(?STORAGE_1),

    % private scope
    ?assertMatch(
        {ok, ?STORAGE_PRIVATE_DATA_MATCHER(?STORAGE_1, Storage1Provider)},
        rpc:call(Node, storage_logic, get, [?STORAGE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, StorageGriMatcher)),

    NewQosParams = #{<<"key">> => <<"value">>},
    ChangedData1 = Storage1PrivateData#{
        <<"qosParameters">> => NewQosParams,
        <<"revision">> => 6
    },
    PushMessage1 = #gs_push_graph{gri = Storage1PrivateGRI, data = ChangedData1, change_type = updated},
    logic_tests_common:simulate_push(Config, PushMessage1),

    ?assertMatch(
        {ok, #document{key = ?STORAGE_1, value = #od_storage{
            qos_parameters = NewQosParams,
            cache_state = #{revision := 6},
            imported = false
        }}},
        rpc:call(Node, storage_logic, get, [?STORAGE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, StorageGriMatcher)),


    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage4 = #gs_push_graph{gri = Storage1PrivateGRI, change_type = deleted},
    logic_tests_common:simulate_push(Config, PushMessage4),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_storage, get_from_cache, [?STORAGE_1])
    ),

    % Simulate a 'nosub' push and see if cache was invalidated, fetch the
    % record first.
    logic_tests_common:invalidate_cache(Config, od_storage, ?STORAGE_1),
    ?assertMatch(
        {ok, ?STORAGE_PRIVATE_DATA_MATCHER(?STORAGE_1, Storage1Provider)},
        rpc:call(Node, storage_logic, get, [?STORAGE_1])
    ),

    PushMessage5 = #gs_push_nosub{gri = Storage1PrivateGRI, reason = forbidden},
    logic_tests_common:simulate_push(Config, PushMessage5),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_storage, get_from_cache, [?STORAGE_1])
    ),

    ok.

resupport_cleanup_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    SpaceId = <<"space1">>,
    
    {ok, StorageId} = rpc:call(Node, space_logic, get_local_storage_id, [SpaceId]),
    
    ok = rpc:call(Node, storage_import, set_or_configure_auto_mode, [SpaceId,
        #{enabled => true, max_depth => 5, sync_acl => true}]),
    ok = rpc:call(Node, file_popularity_api, enable, [SpaceId]),
    ACConfig =  #{
        enabled => true,
        target => 0,
        threshold => 100
    },
    ok = rpc:call(Node, autocleaning_api, configure, [SpaceId, ACConfig]),
    ok = rpc:call(Node, auto_storage_import_worker, schedule_spaces_check, [0]),
    
    ?assertMatch({ok, _}, rpc:call(Node, storage_import_config, get, [SpaceId])),
    ?assertMatch({ok, _}, rpc:call(Node, storage_import_monitoring, get, [SpaceId]), 10),
    ?assertMatch({ok, _}, rpc:call(Node, autocleaning, get, [SpaceId])),
    ?assertMatch({ok, _}, rpc:call(Node, file_popularity_config, get, [SpaceId])),
    ?assertMatch(true, rpc:call(Node, file_popularity_api, is_enabled, [SpaceId])),
    
    {ok, Token} = tokens:serialize(tokens:construct(#token{
        onezone_domain = <<"zone">>,
        subject = ?SUB(user, <<"user1">>),
        id = <<"user1">>,
        type = ?INVITE_TOKEN(?SUPPORT_SPACE, SpaceId, support_parameters:build(global, eager)),
        persistence = named
    }, <<"secret">>, [])),
    
    % force cleanup by adding new support when remnants of previous one still exist
    {ok, _} = rpc:call(Node, space_support, add, [StorageId, Token, 10]),
    
    ?assertEqual({error, not_found}, rpc:call(Node, storage_import_config, get, [SpaceId])),
    ?assertEqual({error, not_found}, rpc:call(Node, storage_import_monitoring, get, [SpaceId])),
    ?assertEqual(undefined, rpc:call(Node, autocleaning, get_config, [SpaceId])),
    ?assertEqual({error, not_found}, rpc:call(Node, autocleaning, get, [SpaceId])),
    ?assertEqual(false, rpc:call(Node, file_popularity_api, is_enabled, [SpaceId])),
    ?assertEqual({error, not_found}, rpc:call(Node, file_popularity_config, get, [SpaceId])).


revise_supported_spaces_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    % onezone supports (provider_logic and space_logic) are mocked in init_per_testcase
    ok = rpc:call(Node, supported_spaces, add, [<<"a">>, <<"st1">>]),
    ok = rpc:call(Node, supported_spaces, add, [<<"a">>, <<"st2">>]),
    ok = rpc:call(Node, supported_spaces, add, [<<"b">>, <<"st2">>]),
    ok = rpc:call(Node, supported_spaces, add, [<<"b">>, <<"st3">>]),
    
    ?assertEqual(ok, rpc:call(Node, supported_spaces, revise, [])),
    test_utils:mock_assert_num_calls(Node, space_unsupport_engine, schedule_start, 3, 0),
    % simulate space forced unsupport/deletion by adding another support to locally persisted
    rpc:call(Node, supported_spaces, add, [<<"c">>, <<"st2">>]),
    ?assertEqual(ok, rpc:call(Node, supported_spaces, revise, [])),
    test_utils:mock_assert_num_calls(Node, space_unsupport_engine, schedule_start, 3, 1),
    test_utils:mock_assert_num_calls(Node, space_unsupport_engine, schedule_start, [<<"c">>, <<"st2">>, forced], 1).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        logic_tests_common:mock_gs_client(NewConfig),
        NewConfig
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [logic_tests_common, initializer]} | Config].

init_per_testcase(resupport_cleanup_test, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Nodes, provider_logic),
    test_utils:mock_new(Nodes, space_logic),
    test_utils:mock_expect(Nodes, provider_logic, supports_space, fun(_) -> false end),
    test_utils:mock_expect(Nodes, provider_logic, get_support_size, fun(_) -> {ok, 1000} end),
    test_utils:mock_expect(Nodes, provider_logic, get_spaces, fun() -> {ok, [<<"space1">>]} end),
    Config1 = initializer:setup_storage(Config),
    lists:foreach(fun(Node) ->
        StorageId = ?config({storage_id, ?GET_DOMAIN(Node)}, Config1),
        test_utils:mock_expect(Node, space_logic, get_local_storage_id, fun(_) -> {ok, StorageId} end)
    end, Nodes),
    test_utils:mock_expect(Nodes, space_support, init_space_support, fun(_, _, _) ->  {ok, <<"space1">>} end),
    test_utils:mock_expect(Nodes, storage_logic, is_imported, fun(_) ->  {ok, true} end),
    Config1;
init_per_testcase(revise_supported_spaces_test, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Nodes, space_unsupport_engine, [passthrough]),
    test_utils:mock_new(Nodes, provider_logic, [passthrough]),
    test_utils:mock_new(Nodes, space_logic, [passthrough]),
    test_utils:mock_expect(Nodes, space_unsupport_engine, schedule_start, fun(_, _, _) -> ok end),
    test_utils:mock_expect(Nodes, provider_logic, get_spaces, fun() -> {ok, [<<"a">>, <<"b">>]} end),
    test_utils:mock_expect(Nodes, space_logic, get_local_storage_ids,
        fun
            (<<"a">>) -> {ok, [<<"st1">>, <<"st2">>]};
            (<<"b">>) -> {ok, [<<"st2">>, <<"st3">>]}
        end),
    Config;
init_per_testcase(_, Config) ->
    logic_tests_common:init_per_testcase(Config).

end_per_testcase(resupport_cleanup_test, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Nodes);
end_per_testcase(revise_supported_spaces_test, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Nodes, provider_logic),
    test_utils:mock_unload(Nodes, space_logic),
    test_utils:mock_unload(Nodes, space_unsupport);
end_per_testcase(_, _Config) ->
    ok.

end_per_suite(Config) ->
    logic_tests_common:unmock_gs_client(Config),
    ok.
