%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of automation list store.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_tree_forest_store_test_SUITE).
-author("Michal Stanisz").

-include("modules/automation/atm_tmp.hrl").
-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").

-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


%% exported for CT
-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    create_store_with_invalid_args_test/1,
    apply_operation_test/1,
    iterator_queue_test/1,
    iterate_one_by_one_test/1,
    iterate_in_chunks_test/1,
    restart_iteration_test/1,
    restart_partial_iteration_test/1,
    iteration_with_deleted_root/1
]).

groups() -> [
    {all_tests, [parallel], [
        create_store_with_invalid_args_test,
        apply_operation_test,
        iterator_queue_test,
        iterate_one_by_one_test,
        iterate_in_chunks_test,
        restart_iteration_test,
        restart_partial_iteration_test,
        iteration_with_deleted_root
    ]}
].

all() -> [
    {group, all_tests}
].


-define(ATM_TREE_FOREST_STORE_SCHEMA, ?ATM_TREE_FOREST_STORE_SCHEMA(atm_file_type)).
-define(ATM_TREE_FOREST_STORE_SCHEMA(DataType), #atm_store_schema{
    id = <<"dummyId">>,
    name = <<"tree_forest_store">>,
    description = <<"description">>,
    requires_initial_value = false,
    type = tree_forest,
    data_spec = #atm_data_spec{type = DataType}
}).

-define(ATTEMPTS, 30).

%%%===================================================================
%%% API functions
%%%===================================================================

create_store_with_invalid_args_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    ?assertEqual(?ERROR_ATM_STORE_MISSING_REQUIRED_INITIAL_VALUE,
        atm_store_test_utils:create_store(Node, undefined, (?ATM_TREE_FOREST_STORE_SCHEMA)#atm_store_schema{requires_initial_value = true})),
    ?assertEqual(?ERROR_ATM_BAD_DATA(<<"initialValue">>, <<"not a list">>),
        atm_store_test_utils:create_store(Node, 8, ?ATM_TREE_FOREST_STORE_SCHEMA)),
    lists:foreach(fun(DataType) ->
        ?assertEqual(?ERROR_NOT_SUPPORTED,
            atm_store_test_utils:create_store(Node, undefined, ?ATM_TREE_FOREST_STORE_SCHEMA(DataType)))
    end, atm_store_test_utils:all_data_types() -- [atm_file_type]).


apply_operation_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    {ok, AtmListStoreId0} = atm_store_test_utils:create_store(Node, undefined, ?ATM_TREE_FOREST_STORE_SCHEMA),
    
    %% @TODO VFS-7676 test adding actual files
    ?assertEqual(?ERROR_NOT_SUPPORTED,
        atm_store_test_utils:apply_operation(Node, set, <<"NaN">>, #{}, AtmListStoreId0)).


iterator_queue_test(_Config) ->
    Name0 = <<"name0">>,
    Name1 = <<"name1">>,
    Name2 = <<"name2">>,
    TestQueueId = datastore_key:new(),
    ?assertEqual(ok, queue_init(TestQueueId)),
    ?assertEqual(ok, queue_push(TestQueueId, lists:map(fun(Num) -> {<<"entry", (integer_to_binary(Num))/binary>>, Name1} end, lists:seq(1, 19)), 0)),
    #atm_tree_forest_iterator_queue{values = FirstNodeValues} = 
        ?assertMatch(#atm_tree_forest_iterator_queue{
            entry_count = 19, 
            processed_index = 0, 
            last_pruned_doc_num = 0, 
            discriminator = {0, Name1}
        }, get_queue_node(TestQueueId, 0)),
    ?assertEqual(9, maps:size(FirstNodeValues)), % there is no entry with index 0
    check_queue_values(FirstNodeValues, lists:seq(1, 9), true),
    #atm_tree_forest_iterator_queue{values = SecondNodeValues} =
        ?assertMatch(#atm_tree_forest_iterator_queue{}, get_queue_node(TestQueueId, 1)),
    ?assertEqual(10, maps:size(SecondNodeValues)),
    check_queue_values(SecondNodeValues, lists:seq(10, 19), true),
    
    % pushing with a name lower than last already given should not be accepted
    ?assertEqual(ok, queue_push(TestQueueId, lists:map(fun(Num) -> {<<"entry", (integer_to_binary(Num))/binary>>, Name0} end, lists:seq(1, 20)), 0)),
    ?assertMatch(#atm_tree_forest_iterator_queue{
        values = FirstNodeValues,
        entry_count = 19,
        processed_index = 0,
        last_pruned_doc_num = 0,
        discriminator = {0, Name1}
    }, get_queue_node(TestQueueId, 0)),
    ?assertMatch(#atm_tree_forest_iterator_queue{values = SecondNodeValues}, get_queue_node(TestQueueId, 1)),
    ?assertMatch({error, not_found}, get_queue_node(TestQueueId, 2)),
    
    % however push with higher name should be accepted
    ?assertEqual(ok, queue_push(TestQueueId, [{<<"entry">>, Name2}], 0)),
    ?assertMatch(#atm_tree_forest_iterator_queue{
        values = FirstNodeValues,
        entry_count = 20,
        processed_index = 0,
        last_pruned_doc_num = 0,
        discriminator = {0, Name2}
    }, get_queue_node(TestQueueId, 0)),
    ?assertMatch(#atm_tree_forest_iterator_queue{values = SecondNodeValues}, get_queue_node(TestQueueId, 1)),
    ?assertMatch(#atm_tree_forest_iterator_queue{values = #{20 := <<"entry">>}}, get_queue_node(TestQueueId, 2)),
    
    % also pushing values originating from old index should not add new values
    ?assertEqual({ok, <<"entry1">>}, queue_pop(TestQueueId, 1)),
    ?assertMatch(#atm_tree_forest_iterator_queue{processed_index = 1}, get_queue_node(TestQueueId, 0)),
    
    ?assertEqual(ok, queue_push(TestQueueId, lists:map(fun(Num) -> {<<"entry", (integer_to_binary(Num))/binary>>, Name1} end, lists:seq(1, 20)), 0)),
    ?assertMatch(#atm_tree_forest_iterator_queue{
        values = FirstNodeValues,
        entry_count = 20,
        processed_index = 1,
        last_pruned_doc_num = 0,
        discriminator = {0, Name2}
    }, get_queue_node(TestQueueId, 0)),
    ?assertMatch(#atm_tree_forest_iterator_queue{values = SecondNodeValues}, get_queue_node(TestQueueId, 1)),
    ?assertNotMatch(#atm_tree_forest_iterator_queue{values = #{21 := _}}, get_queue_node(TestQueueId, 2)),
    
    % but push with higher origin index should always be accepted
    ?assertEqual(ok, queue_push(TestQueueId, [{<<"entry">>, Name0}], 2)),
    ?assertMatch(#atm_tree_forest_iterator_queue{
        values = FirstNodeValues,
        entry_count = 21,
        processed_index = 1,
        last_pruned_doc_num = 0,
        discriminator = {2, Name0}
    }, get_queue_node(TestQueueId, 0)),
    ?assertMatch(#atm_tree_forest_iterator_queue{values = SecondNodeValues}, get_queue_node(TestQueueId, 1)),
    ?assertMatch(#atm_tree_forest_iterator_queue{values = #{21 := <<"entry">>}}, get_queue_node(TestQueueId, 2)),
    
    ?assertEqual(ok, queue_clean(TestQueueId, 8)),
    #atm_tree_forest_iterator_queue{values = Values0} =
        ?assertMatch(#atm_tree_forest_iterator_queue{
            entry_count = 21,
            last_pruned_doc_num = 0
        }, get_queue_node(TestQueueId, 0)),
    ?assertEqual(1, maps:size(Values0)),
    check_queue_values(Values0, lists:seq(1, 8), false),
    check_queue_values(Values0, [9], true),
    ?assertMatch(#atm_tree_forest_iterator_queue{values = SecondNodeValues}, get_queue_node(TestQueueId, 1)),
    
    % clean never deletes first doc
    ?assertEqual(ok, queue_clean(TestQueueId, 20)),
    #atm_tree_forest_iterator_queue{values = Values1} =
        ?assertMatch(#atm_tree_forest_iterator_queue{
            entry_count = 21,
            last_pruned_doc_num = 2
        }, get_queue_node(TestQueueId, 0)),
    ?assertEqual(0, maps:size(Values1)),
    ?assertMatch({error, not_found}, get_queue_node(TestQueueId, 1)),
    #atm_tree_forest_iterator_queue{values = Values2} =
        ?assertMatch(#atm_tree_forest_iterator_queue{}, get_queue_node(TestQueueId, 2)),
    ?assertEqual(1, maps:size(Values2)),
    check_queue_values(Values2, [20], false),
    check_queue_values(Values2, [21], true),
    
    ?assertEqual(ok, queue_destroy(TestQueueId)),
    ?assertMatch({error, not_found}, get_queue_node(TestQueueId, 0)),
    ?assertMatch({error, not_found}, get_queue_node(TestQueueId, 1)),
    ?assertMatch({error, not_found}, get_queue_node(TestQueueId, 2)).
    

iterate_one_by_one_test(_Config) ->
    Depth = 6,
    AtmStoreIteratorStrategy = #atm_store_iterator_serial_strategy{},
    iterate_test_base(AtmStoreIteratorStrategy, Depth).


iterate_in_chunks_test(_Config) ->
    Depth = 6,
    ChunkSize = rand:uniform(Depth) + 8,
    AtmStoreIteratorStrategy = #atm_store_iterator_batch_strategy{size = ChunkSize},
    iterate_test_base(AtmStoreIteratorStrategy, Depth).


restart_iteration_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    AtmStoreIteratorStrategy = #atm_store_iterator_batch_strategy{size = 3},
    {AtmSerialIterator0, FilesMap} = create_iteration_test_env(Node, AtmStoreIteratorStrategy, 1),
    
    IteratorsAndResults = check_iterator_listing(Node, AtmSerialIterator0, lists:flatten(maps:values(FilesMap)), return_iterators ),
    
    lists:foreach(fun({Iterator, ExpectedResults}) ->
        check_iterator_listing(Node, Iterator, ExpectedResults, return_none)
    end, IteratorsAndResults).


restart_partial_iteration_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    AtmStoreIteratorStrategy = #atm_store_iterator_batch_strategy{size = 50},
    {AtmStoreIterator0, FileMap} = create_iteration_test_env(Node, AtmStoreIteratorStrategy, 3),
    FileList = lists:flatten(maps:values(FileMap)),
    {ok, Res0, AtmStoreIterator1} = ?assertMatch({ok, _, _}, atm_store_test_utils:iterator_get_next(Node, AtmStoreIterator0)),
    {ok, _, _} = ?assertMatch({ok, _, _}, atm_store_test_utils:iterator_get_next(Node, AtmStoreIterator1)),
    {ok, Res1, AtmStoreIterator2} = ?assertMatch({ok, _, _}, atm_store_test_utils:iterator_get_next(Node, AtmStoreIterator1)),
    check_listed_values(Res1, FileList -- Res0),
    {ok, Res2, AtmStoreIterator3} = ?assertMatch({ok, _, _}, atm_store_test_utils:iterator_get_next(Node, AtmStoreIterator2)),
    check_listed_values(Res2, FileList -- Res1),
    ?assertMatch(stop, atm_store_test_utils:iterator_get_next(Node, AtmStoreIterator3)).


iteration_with_deleted_root(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    User1Session = oct_background:get_user_session_id(user1, krakow),
    AtmStoreIteratorStrategy = #atm_store_iterator_batch_strategy{size = 50},
    {AtmStoreIterator0, FilesMap} = create_iteration_test_env(Node, AtmStoreIteratorStrategy, 3),
    
    [RootToDelete0 | _RootsTail] = maps:keys(FilesMap),
    ?assertEqual(ok, lfm_proxy:rm_recursive(Node, User1Session, ?FILE_REF(RootToDelete0))),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Node, User1Session, ?FILE_REF(RootToDelete0)), ?ATTEMPTS),
    ExpectedFiles0 = lists:flatten(maps:values(maps:without([RootToDelete0], FilesMap))),
    
    check_iterator_listing(Node, AtmStoreIterator0, ExpectedFiles0, return_none).


%%%===================================================================
%%% Helper functions
%%%===================================================================

-spec iterate_test_base(atm_store_iterator_config:record(), non_neg_integer()) -> ok.
iterate_test_base(AtmStoreIteratorStrategy, Depth) ->
    Node = oct_background:get_random_provider_node(krakow),
    {AtmStoreIterator0, FilesMap} = create_iteration_test_env(Node, AtmStoreIteratorStrategy, Depth),
    check_iterator_listing(Node, AtmStoreIterator0, lists:flatten(maps:values(FilesMap)), return_none).


-spec check_iterator_listing(node(), atm_store_iterator:record(), [file_id:file_guid()], return_iterators | return_none) -> 
    [{atm_store_iterator:record(), [file_id:file_guid()]}].
check_iterator_listing(Node, Iterator, [], _) ->
    ?assertEqual(stop, atm_store_test_utils:iterator_get_next(Node, Iterator)),
    [];
check_iterator_listing(Node, Iterator, ExpectedList, ReturnStrategy) ->
    % duplication here is deliberate
    {ok, Res, NewIterator} = ?assertMatch({ok, _, _}, atm_store_test_utils:iterator_get_next(Node, Iterator)),
    ?assertMatch({ok, Res, NewIterator}, atm_store_test_utils:iterator_get_next(Node, Iterator)),
    ResList = utils:ensure_list(Res),
    check_listed_values(ResList, ExpectedList),
    case ReturnStrategy of
        return_iterators ->
            [{Iterator, ExpectedList}] ++ check_iterator_listing(Node, NewIterator, ExpectedList -- ResList, ReturnStrategy);
        _ ->
            ok = atm_store_test_utils:iterator_mark_exhausted(Node, Iterator),
            check_iterator_listing(Node, NewIterator, ExpectedList -- ResList, ReturnStrategy)
    end.


-spec check_listed_values([file_id:file_guid()], [file_id:file_guid()]) -> ok.
check_listed_values(Values, Expected) ->
    lists:foreach(fun(G) -> ?assertEqual(true, lists:member(G, Expected)) end, Values).


-spec create_iteration_test_env(node(), atm_store_iterator_spec:strategy(), non_neg_integer()) ->
    {atm_store_iterator:record(), #{file_id:file_guid() => [file_id:file_guid()]}}.
create_iteration_test_env(Node, AtmStoreIteratorStrategy, Depth) ->
    SpaceId = oct_background:get_space_id(space_krk),
    ChildrenSpecGen = fun
        F(0) -> [];
        F(Depth) ->
            Children = F(Depth - 1),
            [#dir_spec{children = Children}, #dir_spec{children = Children}, #dir_spec{children = Children}, #file_spec{}]
    end,
    ChildrenSpec = ChildrenSpecGen(Depth),
    DirSpec = [#dir_spec{children = ChildrenSpec}, #dir_spec{children = ChildrenSpec}, #file_spec{}],
    Objects = onenv_file_test_utils:create_and_sync_file_tree(
        user1, SpaceId, DirSpec, krakow
    ),
    ObjectToListFun = fun
        F(#object{guid = G, children = undefined}) -> [G];
        F(#object{guid = G, children = Children}) -> [G] ++ lists:flatmap(F, Children)
    end,
    FileMap = lists:foldl(fun(#object{guid = Guid} = Object, Acc) ->
        Acc#{Guid => ObjectToListFun(Object)}
    end, #{}, Objects),
    
    AtmRangeStoreDummySchemaId = <<"dummyId">>,
    
    {ok, AtmListStoreId} = atm_store_test_utils:create_store(Node, maps:keys(FileMap), ?ATM_TREE_FOREST_STORE_SCHEMA),
    AtmStoreIteratorSpec = #atm_store_iterator_spec{
        store_schema_id = AtmRangeStoreDummySchemaId,
        strategy = AtmStoreIteratorStrategy
    },
    AtmWorkflowExecutionEnv = #atm_workflow_execution_env{
        store_registry = #{AtmRangeStoreDummySchemaId => AtmListStoreId}
    },
    AtmStoreIterator0 = atm_store_test_utils:acquire_store_iterator(Node, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec),
    {AtmStoreIterator0, FileMap}.


queue_init(Id) ->
    Node = oct_background:get_random_provider_node(krakow),
    rpc:call(Node, atm_tree_forest_iterator_queue, report_new_tree, [Id, 0]).


queue_push(Id, Entries, OriginIndex) ->
    Node = oct_background:get_random_provider_node(krakow),
    rpc:call(Node, atm_tree_forest_iterator_queue, push, [Id, Entries, OriginIndex]).


queue_pop(Id, Index) ->
    Node = oct_background:get_random_provider_node(krakow),
    rpc:call(Node, atm_tree_forest_iterator_queue, get, [Id, Index]).


queue_clean(Id, Index) ->
    Node = oct_background:get_random_provider_node(krakow),
    rpc:call(Node, atm_tree_forest_iterator_queue, prune, [Id, Index]).


queue_destroy(Id) ->
    Node = oct_background:get_random_provider_node(krakow),
    rpc:call(Node, atm_tree_forest_iterator_queue, destroy, [Id]).


get_queue_node(Id ,Num) ->
    Node = oct_background:get_random_provider_node(krakow),
    case rpc:call(Node, datastore_model, get, [atm_tree_forest_iterator_queue:get_ctx(), datastore_key:adjacent_from_digest(Num, Id)]) of
        {ok, #document{value = Record}} ->
            Record;
        {error, _} = Error ->
            Error
    end.


check_queue_values(Values, ExpectedKeys, Expectation) ->
    lists:foreach(fun(Num) ->
        ?assertEqual(Expectation, maps:is_key(Num, Values))
    end, ExpectedKeys).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {atm_tree_forest_iterator_queue_max_values_per_doc, 10}
        ]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(_Group, Config) ->
    lfm_proxy:init(Config, false).


end_per_group(_Group, Config) ->
    lfm_proxy:teardown(Config).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
