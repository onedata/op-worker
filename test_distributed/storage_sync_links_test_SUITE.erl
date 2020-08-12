%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests storage_sync_links module.
%%% @end
%%%--------------------------------------------------------------------
-module(storage_sync_links_test_SUITE).
-author("Jakub Kudzia").

-include("storage_sync_links_test_utils.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    add_child_link_test/1, add_child_link_imported_storage_test/1, add_child_link_and_mark_leave_test/1,
    add_existing_child_link_test/1, add_existing_child_link_imported_storage_test/1,
    add_existing_child_link_imported_storage_mark_leaves_test/1,
    add_children_links_test/1, add_children_links_imported_storage_test/1,
    add_children_links_mark_leaves_test/1,
    add_children_links_recursive_test/1, add_children_links_recursive_imported_storage_test/1,
    add_children_links_recursive_mark_leaves_test/1,
    list_children_links_test/1, list_children_links_imported_storage_test/1,
    list_children_links_token_test/1, list_children_links_token_imported_storage_test/1,
    delete_link_test/1, delete_link_imported_storage_test/1,
    delete_links_test/1, delete_links_imported_storage_test/1,
    delete_links_recursive_test/1, delete_links_recursive_imported_storage_test/1,
    add_many_children_links_recursive_test/1,
    add_many_children_links_recursive_imported_storage_test/1,
    delete_many_children_links_recursive_test/1,
    delete_many_children_links_recursive_imported_storage_test/1]).


-define(TEST_CASES, [
    add_child_link_test,
    add_child_link_imported_storage_test,
    add_child_link_and_mark_leave_test,
    add_existing_child_link_test,
    add_existing_child_link_imported_storage_test,
    add_existing_child_link_imported_storage_mark_leaves_test,
    add_children_links_test,
    add_children_links_imported_storage_test,
    add_children_links_mark_leaves_test,
    add_children_links_recursive_test,
    add_children_links_recursive_imported_storage_test,
    add_children_links_recursive_mark_leaves_test,
    list_children_links_test,
    list_children_links_imported_storage_test,
    list_children_links_token_test,
    list_children_links_token_imported_storage_test,
    delete_link_test,
    delete_link_imported_storage_test,
    delete_links_test,
    delete_links_imported_storage_test,
    delete_links_recursive_test,
    delete_links_recursive_imported_storage_test,
    add_many_children_links_recursive_test,
    add_many_children_links_recursive_imported_storage_test,
    delete_many_children_links_recursive_test,
    delete_many_children_links_recursive_imported_storage_test
]).

all() -> ?ALL(?TEST_CASES).

-define(POOL, ?MODULE).

-define(RAND_STR, <<(crypto:strong_rand_bytes(16))/binary>>).
-define(SPACE_ID, <<"space_", ?RAND_STR/binary>>).
-define(STORAGE_ID, <<"storage_", ?RAND_STR/binary>>).


%%%==================================================================
%%% Test functions
%%%===================================================================

add_child_link_test(Config) ->
    add_child_link_test_base(Config, false).

add_child_link_imported_storage_test(Config) ->
    add_child_link_test_base(Config, true).

add_child_link_and_mark_leave_test(Config) ->
    add_child_link_test_base(Config, false, true).

add_existing_child_link_test(Config) ->
    add_existing_child_link_test_base(Config, false).

add_existing_child_link_imported_storage_test(Config) ->
    add_existing_child_link_test_base(Config, true).

add_existing_child_link_imported_storage_mark_leaves_test(Config) ->
    add_existing_child_link_test_base(Config, true, true).

add_children_links_test(Config) ->
    add_children_links_test_base(Config, false).

add_children_links_imported_storage_test(Config) ->
    add_children_links_test_base(Config, true).

add_children_links_mark_leaves_test(Config) ->
    add_children_links_test_base(Config, false, true).

add_children_links_recursive_test(Config) ->
    add_children_links_recursive_test_base(Config, false).

add_children_links_recursive_imported_storage_test(Config) ->
    add_children_links_recursive_test_base(Config, true).

add_children_links_recursive_mark_leaves_test(Config) ->
    add_children_links_recursive_test_base(Config, false, true).

list_children_links_test(Config) ->
    list_children_links_test_base(Config, false).

list_children_links_imported_storage_test(Config) ->
    list_children_links_test_base(Config, true).

list_children_links_token_test(Config) ->
    list_children_links_token_test_base(Config, false).

list_children_links_token_imported_storage_test(Config) ->
    list_children_links_token_test_base(Config, true).

delete_link_test(Config) ->
    delete_link_test_base(Config, false).

delete_link_imported_storage_test(Config) ->
    delete_link_test_base(Config, true).

delete_links_test(Config) ->
    delete_links_test_base(Config, false).

delete_links_imported_storage_test(Config) ->
    delete_links_test_base(Config, true).

delete_links_recursive_test(Config) ->
    delete_links_recursive_test_base(Config, false).

delete_links_recursive_imported_storage_test(Config) ->
    delete_links_test_base(Config, true).

add_many_children_links_recursive_test(Config) ->
    add_many_children_links_recursive_test_base(Config, false).

delete_many_children_links_recursive_test(Config) ->
    delete_many_children_links_recursive_test_base(Config, false).

add_many_children_links_recursive_imported_storage_test(Config) ->
    add_many_children_links_recursive_test_base(Config, true).

delete_many_children_links_recursive_imported_storage_test(Config) ->
    delete_many_children_links_recursive_test_base(Config, true).

%===================================================================
% SetUp and TearDown functions
%===================================================================

add_child_link_test_base(Config, ImportedStorage) ->
    add_child_link_test_base(Config, ImportedStorage, false).

add_child_link_test_base(Config, ImportedStorage, MarkLeaves) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID,
    StorageId = ?STORAGE_ID,
    RootStorageFileId = space_storage_file_id(SpaceId, ImportedStorage),
    ChildName = <<"child1">>,
    ChildStorageFileId = filename:join([RootStorageFileId, ChildName]),
    ok = storage_sync_links_test_utils:add_link(W, RootStorageFileId, StorageId, ChildStorageFileId, MarkLeaves),
    case MarkLeaves of
        true ->
            ?assertMatch({ok, undefined},
                storage_sync_links_test_utils:get_link(W, RootStorageFileId, StorageId, ChildName));
        false ->
            {ok, ChildRootId} = ?assertMatch({ok, _},
                storage_sync_links_test_utils:get_link(W, RootStorageFileId, StorageId, ChildName)),
            ?assertNotEqual(undefined, ChildRootId)
    end.

add_existing_child_link_test_base(Config, ImportedStorage) ->
    add_existing_child_link_test_base(Config, ImportedStorage, false).

add_existing_child_link_test_base(Config, ImportedStorage, MarkLeaves) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID,
    StorageId = ?STORAGE_ID,
    RootStorageFileId = space_storage_file_id(SpaceId, ImportedStorage),
    ChildName = <<"child1">>,
    ChildStorageFileId = filename:join([RootStorageFileId, ChildName]),
    ok = storage_sync_links_test_utils:add_link(W, RootStorageFileId, StorageId, ChildStorageFileId, MarkLeaves),
    ok = storage_sync_links_test_utils:add_link(W, RootStorageFileId, StorageId, ChildStorageFileId, MarkLeaves),
    case MarkLeaves of
        true ->
            ?assertMatch({ok, undefined}, storage_sync_links_test_utils:get_link(W, RootStorageFileId, StorageId, ChildName));
        false ->
            {ok, ChildRootId} = ?assertMatch({ok, _},
                storage_sync_links_test_utils:get_link(W, RootStorageFileId, StorageId, ChildName)),
            ?assertNotEqual(undefined, ChildRootId)
    end.

add_children_links_test_base(Config, ImportedStorage) ->
    add_children_links_test_base(Config, ImportedStorage, false).

add_children_links_test_base(Config, ImportedStorage, MarkLeaves) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID,
    StorageId = ?STORAGE_ID,
    RootStorageFileId = space_storage_file_id(SpaceId, ImportedStorage),
    ChildrenNum = 5,
    ChildrenNames = [<<"child", (integer_to_binary(N))/binary>> || N <- lists:seq(1, ChildrenNum)],
    ChildStorageFileIds = [filename:join([RootStorageFileId, CN]) || CN <- ChildrenNames],
    lists:foreach(fun(ChildStorageFileId) ->
        storage_sync_links_test_utils:add_link(W, RootStorageFileId, StorageId, ChildStorageFileId, MarkLeaves)
    end, ChildStorageFileIds),
    lists:foreach(fun(ChildName) ->
        case MarkLeaves of
            true ->
                ?assertMatch({ok, undefined}, storage_sync_links_test_utils:get_link(
                    W, RootStorageFileId, StorageId, ChildName));
            false ->
                {ok, ChildRootId} = ?assertMatch({ok, _},
                    storage_sync_links_test_utils:get_link(
                        W, RootStorageFileId, StorageId, ChildName)),
                ?assertNotEqual(undefined, ChildRootId)
        end
    end, ChildrenNames).

add_children_links_recursive_test_base(Config, ImportedStorage) ->
    add_children_links_recursive_test_base(Config, ImportedStorage, false).

add_children_links_recursive_test_base(Config, ImportedStorage, MarkLeaves) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID,
    StorageId = ?STORAGE_ID,
    RootStorageFileId = space_storage_file_id(SpaceId, ImportedStorage),
    ChildName1 = <<"child1">>,
    ChildName2 = <<"child2">>,
    ChildName3 = <<"child3">>,
    ChildName4 = <<"child4">>,
    ChildName5 = <<"child5">>,
    ChildStorageFileId1 = filename:join([RootStorageFileId, ChildName1]),
    ChildStorageFileId2 = filename:join([ChildStorageFileId1, ChildName2]),
    ChildStorageFileId3 = filename:join([ChildStorageFileId2, ChildName3]),
    ChildStorageFileId4 = filename:join([ChildStorageFileId3, ChildName4]),
    ChildStorageFileId5 = filename:join([ChildStorageFileId4, ChildName5]),

    ok = storage_sync_links_test_utils:add_link(W, RootStorageFileId, StorageId, ChildStorageFileId5, MarkLeaves),

    {ok, RootId1} = ?assertMatch({ok, _},
        storage_sync_links_test_utils:get_link(W, RootStorageFileId, StorageId, ChildName1)),
    ?assertNotEqual(RootId1, undefined),

    {ok, RootId2} = ?assertMatch({ok, _},
        storage_sync_links_test_utils:get_link(W, ChildStorageFileId1, StorageId, ChildName2)),
    ?assertNotEqual(RootId2, undefined),
    ?assertEqual({ok, RootId2},
        storage_sync_links_test_utils:get_link(W, RootId1, ChildName2)),

    {ok, RootId3} = ?assertMatch({ok, _},
        storage_sync_links_test_utils:get_link(W, ChildStorageFileId2, StorageId, ChildName3)),
    ?assertNotEqual(RootId3, undefined),
    ?assertEqual({ok, RootId3},
        storage_sync_links_test_utils:get_link(W, RootId2, ChildName3)),

    {ok, RootId4} = ?assertMatch({ok, _},
        storage_sync_links_test_utils:get_link(W, ChildStorageFileId3, StorageId, ChildName4)),
    ?assertNotEqual(RootId4, undefined),
    ?assertEqual({ok, RootId4},
        storage_sync_links_test_utils:get_link(W, RootId3, ChildName4)),

    case MarkLeaves of
        true ->
            ?assertMatch({ok, undefined},
                storage_sync_links_test_utils:get_link(W, ChildStorageFileId4, StorageId, ChildName5)),
            ?assertEqual({ok, undefined},
                storage_sync_links_test_utils:get_link(W, RootId4, ChildName5));
        false ->
            {ok, RootId5} = ?assertMatch({ok, _},
                storage_sync_links_test_utils:get_link(W, ChildStorageFileId4, StorageId, ChildName5)),
            ?assertNotEqual(undefined, RootId5),
            ?assertEqual({ok, RootId5},
                storage_sync_links_test_utils:get_link(W, RootId4, ChildName5))
    end.

list_children_links_test_base(Config, ImportedStorage) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID,
    StorageId = ?STORAGE_ID,
    RootStorageFileId = space_storage_file_id(SpaceId, ImportedStorage),
    ChildrenNum = 5,
    ChildrenNames = [<<"child", (integer_to_binary(N))/binary>> || N <- lists:seq(1, ChildrenNum)],
    ChildStorageFileIds = [filename:join([RootStorageFileId, CN]) || CN <- ChildrenNames],
    lists:foreach(fun(ChildStorageFileId) ->
        storage_sync_links_test_utils:add_link(W, RootStorageFileId, StorageId, ChildStorageFileId)
    end, ChildStorageFileIds),

    {{ok, Children}, _} = ?assertMatch({{ok, _}, #link_token{}},
        storage_sync_links_test_utils:list(W, RootStorageFileId, StorageId, 10)),
    ?assertEqual(ChildrenNames, [C || {C, _} <- Children]).

list_children_links_token_test_base(Config, ImportedStorage) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID,
    StorageId = ?STORAGE_ID,
    RootStorageFileId = space_storage_file_id(SpaceId, ImportedStorage),
    ChildName1 = <<"child1">>,
    ChildName2 = <<"child2">>,
    ChildName3 = <<"child3">>,
    ChildName4 = <<"child4">>,
    ChildName5 = <<"child5">>,
    ChildrenNames = [ChildName1, ChildName2, ChildName3, ChildName4, ChildName5],
    ChildStorageFileIds = [filename:join([RootStorageFileId, CN]) || CN <- ChildrenNames],
    lists:foreach(fun(ChildStorageFileId) ->
        storage_sync_links_test_utils:add_link(W, RootStorageFileId, StorageId, ChildStorageFileId)
    end, ChildStorageFileIds),
    {{ok, _}, T1} = ?assertMatch({{ok, [{ChildName1, _}]}, #link_token{}},
        storage_sync_links_test_utils:list(W, RootStorageFileId, StorageId, 1)),
    {{ok, _}, T2} = ?assertMatch({{ok, [{ChildName2, _}]}, #link_token{}},
        storage_sync_links_test_utils:list(W, RootStorageFileId, StorageId, T1, 1)),
    {{ok, _}, T3} = ?assertMatch({{ok, [{ChildName3, _}]}, #link_token{}},
        storage_sync_links_test_utils:list(W, RootStorageFileId, StorageId, T2, 1)),
    {{ok, _}, T4} = ?assertMatch({{ok, [{ChildName4, _}]}, #link_token{}},
        storage_sync_links_test_utils:list(W, RootStorageFileId, StorageId, T3, 1)),
    {{ok, _}, T5} = ?assertMatch({{ok, [{ChildName5, _}]}, #link_token{}},
        storage_sync_links_test_utils:list(W, RootStorageFileId, StorageId, T4, 1)),
    ?assertEqual(true, T5#link_token.is_last).

delete_link_test_base(Config, ImportedStorage) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID,
    StorageId = ?STORAGE_ID,
    RootStorageFileId = space_storage_file_id(SpaceId, ImportedStorage),
    ChildName = <<"child1">>,
    ChildStorageFileId = filename:join([RootStorageFileId, ChildName]),
    ok = storage_sync_links_test_utils:add_link(W, RootStorageFileId, StorageId, ChildStorageFileId),
    ?assertMatch({ok, _},
        storage_sync_links_test_utils:get_link(W, RootStorageFileId, StorageId, ChildName)),
    ok = storage_sync_links_test_utils:delete_link(W, RootStorageFileId, StorageId, ChildName),
    ?assertEqual({error, not_found},
        storage_sync_links_test_utils:get_link(W, RootStorageFileId, StorageId, ChildName)),
    ?assertMatch({{ok, []}, #link_token{}}, storage_sync_links_test_utils:list(
        W, RootStorageFileId, StorageId, 10)).

delete_links_test_base(Config, ImportedStorage) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID,
    StorageId = ?STORAGE_ID,
    RootStorageFileId = space_storage_file_id(SpaceId, ImportedStorage),
    ChildrenNum = 5,
    ChildrenNames = [<<"child", (integer_to_binary(N))/binary>> || N <- lists:seq(1, ChildrenNum)],
    ChildStorageFileIds = [filename:join([RootStorageFileId, CN]) || CN <- ChildrenNames],
    lists:foreach(fun(ChildStorageFileId) ->
        storage_sync_links_test_utils:add_link(W, RootStorageFileId, StorageId, ChildStorageFileId)
    end, ChildStorageFileIds),
    lists:foreach(fun(ChildName) ->
        ?assertMatch({ok, _},
            storage_sync_links_test_utils:get_link(W, RootStorageFileId, StorageId, ChildName))
    end, ChildrenNames),
    ok = storage_sync_links_test_utils:delete_recursive(W, RootStorageFileId, StorageId),
    ?assertMatch({{ok, []}, #link_token{}},
        storage_sync_links_test_utils:list(W, RootStorageFileId, StorageId, 10)),
    lists:foreach(fun(ChildName) ->
        ?assertMatch({error, not_found},
            storage_sync_links_test_utils:get_link(W, RootStorageFileId, StorageId, ChildName))
    end, ChildrenNames).

delete_links_recursive_test_base(Config, ImportedStorage) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID,
    StorageId = ?STORAGE_ID,
    RootStorageFileId = space_storage_file_id(SpaceId, ImportedStorage),
    ChildName1 = <<"child1">>,
    ChildName2 = <<"child2">>,
    ChildName3 = <<"child3">>,
    ChildName4 = <<"child4">>,
    ChildName5 = <<"child5">>,
    ChildStorageFileId1 = filename:join([RootStorageFileId, ChildName1]),
    ChildStorageFileId2 = filename:join([ChildStorageFileId1, ChildName2]),
    ChildStorageFileId3 = filename:join([ChildStorageFileId2, ChildName3]),
    ChildStorageFileId4 = filename:join([ChildStorageFileId3, ChildName4]),
    ChildStorageFileId5 = filename:join([ChildStorageFileId4, ChildName5]),
    ChildStorageFileIds = [ChildStorageFileId1, ChildStorageFileId2, ChildStorageFileId3, ChildStorageFileId4, ChildStorageFileId5],

    ok = storage_sync_links_test_utils:add_link(W, RootStorageFileId, StorageId, ChildStorageFileId5),

    {ok, RootId1} = ?assertMatch({ok, _},
        storage_sync_links_test_utils:get_link(W, RootStorageFileId, StorageId, ChildName1)),
    ?assertNotEqual(RootId1, undefined),

    {ok, RootId2} = ?assertMatch({ok, _},
        storage_sync_links_test_utils:get_link(W, ChildStorageFileId1, StorageId, ChildName2)),
    ?assertEqual({ok, RootId2}, storage_sync_links_test_utils:get_link(W, RootId1, ChildName2)),

    {ok, RootId3} = ?assertMatch({ok, _},
        storage_sync_links_test_utils:get_link(W, ChildStorageFileId2, StorageId, ChildName3)),
    ?assertEqual({ok, RootId3}, storage_sync_links_test_utils:get_link(W, RootId2, ChildName3)),

    {ok, RootId4} = ?assertMatch({ok, _},
        storage_sync_links_test_utils:get_link(W, ChildStorageFileId3, StorageId, ChildName4)),
    ?assertEqual({ok, RootId4}, storage_sync_links_test_utils:get_link(W, RootId3, ChildName4)),

    {ok, RootId5} = ?assertMatch({ok, _},
        storage_sync_links_test_utils:get_link(W, ChildStorageFileId4, StorageId, ChildName5)),
    ?assertEqual({ok, RootId5}, storage_sync_links_test_utils:get_link(W, RootId4, ChildName5)),

    ok = storage_sync_links_test_utils:delete_recursive(W, RootStorageFileId, StorageId),
    ?assertMatch({{ok, []}, #link_token{}},
        storage_sync_links_test_utils:list(W, RootStorageFileId, StorageId, 10)),

    lists:foreach(fun(ChildStorageFileId) ->
        ?assertMatch({{ok, []}, _},
            storage_sync_links_test_utils:list(W, ChildStorageFileId, StorageId, 10))
    end, ChildStorageFileIds).

add_many_children_links_recursive_test_base(Config, ImportedStorage) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID,
    StorageId = ?STORAGE_ID,
    RootStorageFileId = space_storage_file_id(SpaceId, ImportedStorage),
    FilesStructure = [10, 10, 10, 10],
    StorageFileIds = generate_storage_file_ids(RootStorageFileId, FilesStructure),
    lists:foreach(fun(StorageFileId) ->
        cast_add_link(W, RootStorageFileId, StorageId, StorageFileId)
    end, StorageFileIds),

    ?assertList(StorageFileIds, W, RootStorageFileId, StorageId).

delete_many_children_links_recursive_test_base(Config, ImportedStorage) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID,
    StorageId = ?STORAGE_ID,
    RootStorageFileId = space_storage_file_id(SpaceId, ImportedStorage),
    FilesStructure = [10, 10, 10, 10],
    StorageFileIds = generate_storage_file_ids(RootStorageFileId, FilesStructure),
    lists:foreach(fun(StorageFileId) ->
        cast_add_link(W, RootStorageFileId, StorageId, StorageFileId)
    end, StorageFileIds),

    ?assertList(StorageFileIds, W, RootStorageFileId, StorageId),

    storage_sync_links_test_utils:delete_recursive(W, RootStorageFileId, StorageId),
    ?assertList([], W, RootStorageFileId, StorageId).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        ssl:start(),
        hackney:start(),
        initializer:disable_quota_limit(NewConfig),
        initializer:mock_provider_ids(NewConfig),
        multi_provider_file_ops_test_base:init_env(NewConfig)
    end,
    {ok, _} = application:ensure_all_started(worker_pool),
    {ok, _} = worker_pool:start_sup_pool(?POOL, [{workers, 8}]),
    [{?LOAD_MODULES, [initializer]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    ok = worker_pool:stop_sup_pool(?POOL),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    initializer:unmock_provider_ids(Config),
    ssl:stop().

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%===================================================================
% Internal functions
%===================================================================

space_storage_file_id(_SpaceId, true) ->
    <<"/">>;
space_storage_file_id(SpaceId, false) ->
    <<"/", SpaceId/binary>>.


generate_storage_file_ids(RootStorageFileId, [Level]) ->
    [filename:join([RootStorageFileId, integer_to_binary(N)]) || N <- lists:seq(1, Level)];
generate_storage_file_ids(RootStorageFileId, [Level | Rest]) ->
    lists:foldl(fun(N, AccIn) ->
        Child = filename:join([RootStorageFileId, integer_to_binary(N)]),
        [Child] ++ generate_storage_file_ids(Child, Rest) ++ AccIn
    end, [], lists:seq(1, Level)).

cast_add_link(Worker, RootStorageFileId, StorageId, StorageFileId) ->
    ok = worker_pool:cast(?POOL,
        {storage_sync_links_test_utils, add_link, [Worker, RootStorageFileId, StorageId, StorageFileId]}).