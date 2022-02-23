%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of automation tree forest store.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_tree_forest_store_test_SUITE).
-author("Michal Stanisz").

-include("modules/datastore/datastore_runner.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").

-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").
-include_lib("onenv_ct/include/test_rpc.hrl").


%% exported for CT
-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    % infinite_log_based_stores_common_tests
    create_test/1,
    apply_operation_test/1,
    browse_by_index_test/1,
    browse_by_offset_test/1,

    % tree_forest_store_specific_tests
    iterator_queue_test/1,
    iterate_files_test/1,
    iterate_datasets_test/1,
    restart_iteration_test/1,
    restart_partial_iteration_test/1,
    iteration_with_deleted_root/1,
    iteration_after_restart_with_deleted_root/1,
    iteration_after_restart_with_new_dirs_root/1,
    iteration_without_permission/1
]).

groups() -> [
    {infinite_log_based_stores_common_tests, [parallel], [
        create_test,
        apply_operation_test,
        browse_by_index_test,
        browse_by_offset_test
    ]},
    {tree_forest_store_specific_tests, [parallel], [
        iterator_queue_test,
        iterate_files_test,
        iterate_datasets_test,
        restart_iteration_test,
        restart_partial_iteration_test,
        iteration_with_deleted_root,
        iteration_after_restart_with_deleted_root,
        iteration_after_restart_with_new_dirs_root,
        iteration_without_permission
    ]}
].

all() -> [
    {group, infinite_log_based_stores_common_tests},
    {group, tree_forest_store_specific_tests}
].


-define(PROVIDER_SELECTOR, krakow).
-define(rpc(Expr), ?rpc(?PROVIDER_SELECTOR, Expr)).
-define(rpc(ProviderSelector, Expr), ?opw_test_rpc(ProviderSelector, Expr)).

-define(ATTEMPTS, 30).


%%%===================================================================
%%% API functions
%%%===================================================================


create_test(_Config) ->
    atm_infinite_log_based_stores_test_base:create_test_base(#{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1
    }).


apply_operation_test(_Config) ->
    atm_infinite_log_based_stores_test_base:apply_operation_test_base(#{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1,
        input_item_to_exp_store_item => fun input_item_to_exp_store_item/3
    }).


browse_by_index_test(_Config) ->
    atm_infinite_log_based_stores_test_base:browse_content_test_base(index, #{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1,
        input_item_to_exp_store_item => fun input_item_to_exp_store_item/3,
        randomly_remove_entity_referenced_by_item => fun randomly_remove_entity_referenced_by_item/3
    }).


browse_by_offset_test(_Config) ->
    atm_infinite_log_based_stores_test_base:browse_content_test_base(offset, #{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1,
        input_item_to_exp_store_item => fun input_item_to_exp_store_item/3,
        randomly_remove_entity_referenced_by_item => fun randomly_remove_entity_referenced_by_item/3
    }).


iterator_queue_test(_Config) ->
    Name0 = <<"name0">>,
    Name1 = <<"name1">>,
    Name2 = <<"name2">>,
    {ok, TestQueueId} = ?assertMatch({ok, _}, queue_init(10)),
    ?assertEqual(ok, queue_push(TestQueueId, lists:map(fun(Num) -> {<<"entry", (integer_to_binary(Num))/binary>>, Name1} end, lists:seq(1, 19)), 0)),
    #atm_tree_forest_iterator_queue{values = FirstNodeValues} = 
        ?assertMatch(#atm_tree_forest_iterator_queue{
            last_pushed_value_index = 19, 
            highest_peeked_value_index = 0, 
            last_pruned_node_num = 0, 
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
        last_pushed_value_index = 19,
        highest_peeked_value_index = 0,
        last_pruned_node_num = 0,
        discriminator = {0, Name1}
    }, get_queue_node(TestQueueId, 0)),
    ?assertMatch(#atm_tree_forest_iterator_queue{values = SecondNodeValues}, get_queue_node(TestQueueId, 1)),
    ?assertMatch({error, not_found}, get_queue_node(TestQueueId, 2)),
    
    % however push with higher name should be accepted
    ?assertEqual(ok, queue_push(TestQueueId, [{<<"entry">>, Name2}], 0)),
    ?assertMatch(#atm_tree_forest_iterator_queue{
        values = FirstNodeValues,
        last_pushed_value_index = 20,
        highest_peeked_value_index = 0,
        last_pruned_node_num = 0,
        discriminator = {0, Name2}
    }, get_queue_node(TestQueueId, 0)),
    ?assertMatch(#atm_tree_forest_iterator_queue{values = SecondNodeValues}, get_queue_node(TestQueueId, 1)),
    ?assertMatch(#atm_tree_forest_iterator_queue{values = #{20 := <<"entry">>}}, get_queue_node(TestQueueId, 2)),
    
    % also pushing values originating from lower (older) origin index should not add new values
    ?assertEqual({ok, <<"entry1">>}, queue_peek(TestQueueId, 1)),
    ?assertMatch(#atm_tree_forest_iterator_queue{highest_peeked_value_index = 1}, get_queue_node(TestQueueId, 0)),
    
    ?assertEqual(ok, queue_push(TestQueueId, lists:map(fun(Num) -> {<<"entry", (integer_to_binary(Num))/binary>>, Name1} end, lists:seq(1, 20)), 0)),
    ?assertMatch(#atm_tree_forest_iterator_queue{
        values = FirstNodeValues,
        last_pushed_value_index = 20,
        highest_peeked_value_index = 1,
        last_pruned_node_num = 0,
        discriminator = {0, Name2}
    }, get_queue_node(TestQueueId, 0)),
    ?assertMatch(#atm_tree_forest_iterator_queue{values = SecondNodeValues}, get_queue_node(TestQueueId, 1)),
    ?assertNotMatch(#atm_tree_forest_iterator_queue{values = #{21 := _}}, get_queue_node(TestQueueId, 2)),
    
    % but push with higher origin index should always be accepted
    ?assertEqual(ok, queue_push(TestQueueId, [{<<"entry">>, Name0}], 2)),
    ?assertMatch(#atm_tree_forest_iterator_queue{
        values = FirstNodeValues,
        last_pushed_value_index = 21,
        highest_peeked_value_index = 1,
        last_pruned_node_num = 0,
        discriminator = {2, Name0}
    }, get_queue_node(TestQueueId, 0)),
    ?assertMatch(#atm_tree_forest_iterator_queue{values = SecondNodeValues}, get_queue_node(TestQueueId, 1)),
    ?assertMatch(#atm_tree_forest_iterator_queue{values = #{21 := <<"entry">>}}, get_queue_node(TestQueueId, 2)),
    
    ?assertEqual(ok, queue_clean(TestQueueId, 8)),
    #atm_tree_forest_iterator_queue{values = Values0} =
        ?assertMatch(#atm_tree_forest_iterator_queue{
            last_pushed_value_index = 21,
            last_pruned_node_num = 0
        }, get_queue_node(TestQueueId, 0)),
    ?assertEqual(1, maps:size(Values0)),
    check_queue_values(Values0, lists:seq(1, 8), false),
    check_queue_values(Values0, [9], true),
    ?assertMatch(#atm_tree_forest_iterator_queue{values = SecondNodeValues}, get_queue_node(TestQueueId, 1)),
    
    % clean never deletes first doc
    ?assertEqual(ok, queue_clean(TestQueueId, 20)),
    #atm_tree_forest_iterator_queue{values = Values1} =
        ?assertMatch(#atm_tree_forest_iterator_queue{
            last_pushed_value_index = 21,
            last_pruned_node_num = 2
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
    

iterate_files_test(_Config) ->
    Depth = 6,
    iterate_test_base(rand:uniform(100), Depth, atm_file_type).


iterate_datasets_test(_Config) ->
    Depth = 4,
    iterate_test_base(rand:uniform(100), Depth, atm_dataset_type).


restart_iteration_test(_Config) ->
    {AtmWorkflowExecutionEnv, AtmSerialIterator0, _FilesMap, Expected} = create_iteration_test_env(krakow, 3, 2, atm_file_type),
    
    IteratorsAndResults = check_iterator_listing(
        krakow, AtmWorkflowExecutionEnv, AtmSerialIterator0, Expected, return_iterators, atm_file_type
    ),
    
    lists:foreach(fun({Iterator, ExpectedResults}) ->
        check_iterator_listing(krakow, AtmWorkflowExecutionEnv, Iterator, ExpectedResults, return_iterators, atm_file_type)
    end, IteratorsAndResults).


restart_partial_iteration_test(_Config) ->
    {AtmWorkflowExecutionEnv, AtmStoreIterator0, _FilesMap, FileList} = create_iteration_test_env(krakow, 50, 3, atm_file_type),
    {ok, Res0, AtmStoreIterator1} = ?assertMatch({ok, _, _}, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, AtmStoreIterator0))),
    {ok, _, _} = ?assertMatch({ok, _, _}, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, AtmStoreIterator1))),
    {ok, Res1, AtmStoreIterator2} = ?assertMatch({ok, _, _}, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, AtmStoreIterator1))),
    check_listed_values(Res1, FileList -- Res0, atm_file_type),
    {ok, Res2, AtmStoreIterator3} = ?assertMatch({ok, _, _}, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, AtmStoreIterator2))),
    check_listed_values(Res2, FileList -- Res1, atm_file_type),
    ?assertMatch(stop, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, AtmStoreIterator3))).


iteration_with_deleted_root(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    User1Session = oct_background:get_user_session_id(user1, krakow),
    {AtmWorkflowExecutionEnv, AtmStoreIterator0, FilesMap, _Expected} = create_iteration_test_env(krakow, 50, 3, atm_file_type),
    
    [RootToDelete0 | _RootsTail] = maps:keys(FilesMap),
    ?assertEqual(ok, lfm_proxy:rm_recursive(Node, User1Session, ?FILE_REF(RootToDelete0))),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Node, User1Session, ?FILE_REF(RootToDelete0)), ?ATTEMPTS),
    ExpectedFiles0 = lists:flatten(lists:map(fun({_, V}) -> V end, maps:values(maps:without([RootToDelete0], FilesMap)))),
    
    check_iterator_listing(krakow, AtmWorkflowExecutionEnv, AtmStoreIterator0, ExpectedFiles0, return_none, atm_file_type).


iteration_after_restart_with_deleted_root(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    User1Session = oct_background:get_user_session_id(user1, krakow),
    {AtmWorkflowExecutionEnv, AtmStoreIterator0, FilesMap, ExpectedBefore} = create_iteration_test_env(krakow, 50, 3, atm_file_type),
    
    check_iterator_listing(krakow, AtmWorkflowExecutionEnv, AtmStoreIterator0, ExpectedBefore, return_iterators, atm_file_type),
    [RootToDelete0 | _RootsTail] = maps:keys(FilesMap),
    ?assertEqual(ok, lfm_proxy:rm_recursive(Node, User1Session, ?FILE_REF(RootToDelete0))),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Node, User1Session, ?FILE_REF(RootToDelete0)), ?ATTEMPTS),
    ExpectedAfter = lists:flatten(lists:map(fun({_, V}) -> V end, maps:values(maps:without([RootToDelete0], FilesMap)))),
    check_iterator_listing(krakow, AtmWorkflowExecutionEnv, AtmStoreIterator0, ExpectedAfter, return_none, atm_file_type).


iteration_after_restart_with_new_dirs_root(_Config) ->
    {AtmWorkflowExecutionEnv, AtmStoreIterator0, FilesMap, ExpectedBefore} = create_iteration_test_env(krakow, 50, 3, atm_file_type),
    
    check_iterator_listing(krakow, AtmWorkflowExecutionEnv, AtmStoreIterator0, ExpectedBefore, return_iterators, atm_file_type),
    [Root1 | _] = maps:fold(
        fun (K, {?DIRECTORY_TYPE, _}, Acc) -> [K | Acc];
            (_K, _, Acc) -> Acc
        end, [], FilesMap),
            
    
    Spec = [#dir_spec{children = [], mode = 8#705}, #dir_spec{children = [#file_spec{}], mode = 8#705}, #file_spec{}],
    Objects = onenv_file_test_utils:create_and_sync_file_tree(user1, Root1, Spec, krakow),
    RegFiles = lists:filtermap(
        fun (#object{type = ?REGULAR_FILE_TYPE, guid = Guid}) -> 
                {true, Guid}; 
            (_) -> 
                false 
        end, Objects),
    check_iterator_listing(krakow, AtmWorkflowExecutionEnv, AtmStoreIterator0, ExpectedBefore ++ RegFiles, return_none, atm_file_type).
    

iteration_without_permission(_Config) ->
    {AtmWorkflowExecutionEnv, AtmStoreIterator0, FilesMap, _Expected} = create_iteration_test_env(krakow, 50, 1, atm_file_type, user2),
    Roots = maps:keys(FilesMap),
    
    check_iterator_listing(krakow, AtmWorkflowExecutionEnv, AtmStoreIterator0, Roots, return_none, atm_file_type).


%%%===================================================================
%%% Helper functions
%%%===================================================================


%% @private
-spec example_configs() -> [atm_single_value_store_config:record()].
example_configs() ->
    lists:map(fun(ItemDataSpec) ->
        #atm_tree_forest_store_config{item_data_spec = ItemDataSpec}
    end, [
        #atm_data_spec{type = atm_dataset_type},
        #atm_data_spec{type = atm_file_type}
    ]).


%% @private
-spec get_input_item_generator_seed_data_spec(atm_list_store_config:record()) ->
    atm_data_spec:record().
get_input_item_generator_seed_data_spec(#atm_tree_forest_store_config{item_data_spec = ItemDataSpec}) ->
    ItemDataSpec.


%% @private
-spec input_item_formatter(automation:item()) -> automation:item().
input_item_formatter(Item) -> Item.


%% @private
-spec input_item_to_exp_store_item(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_store:id()
) ->
    atm_value:expanded().
input_item_to_exp_store_item(AtmWorkflowExecutionAuth, ItemInitializer, ItemDataSpec) ->
    atm_store_test_utils:compress_and_expand_data(
        ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, ItemInitializer, ItemDataSpec
    ).


%% @private
-spec randomly_remove_entity_referenced_by_item(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_data_spec:record()
) ->
    false | {true, errors:error()}.
randomly_remove_entity_referenced_by_item(AtmWorkflowExecutionAuth, Item, ItemDataSpec) ->
    atm_store_test_utils:randomly_remove_entity_referenced_by_item(
        ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, Item, ItemDataSpec
    ).


-spec iterate_test_base(pos_integer(), non_neg_integer(), atm_data_type:type()) -> ok.
iterate_test_base(MaxBatchSize, Depth, Type) ->
    {AtmWorkflowExecutionEnv, AtmStoreIterator0, _FilesMap, Expected} = create_iteration_test_env(krakow, MaxBatchSize, Depth, Type),
    check_iterator_listing(krakow, AtmWorkflowExecutionEnv, AtmStoreIterator0, Expected, return_none, Type).


-spec check_iterator_listing(
    oct_background:entity_selector(),
    atm_workflow_execution_env:record(), 
    atm_store_iterator:record(), 
    [file_id:file_guid()], 
    return_iterators | return_none,
    atm_data_type:type()
) -> 
    [{atm_store_iterator:record(), [file_id:file_guid()]}].
check_iterator_listing(ProviderSelector, AtmWorkflowExecutionEnv, Iterator, [], _, _Type) ->
    ?assertEqual(stop, ?rpc(ProviderSelector, iterator:get_next(AtmWorkflowExecutionEnv, Iterator))),
    [];
check_iterator_listing(ProviderSelector, AtmWorkflowExecutionEnv, Iterator, ExpectedList, ReturnStrategy, Type) ->
    % duplication here is deliberate to check reuse of iterator
    {ok, Res, NewIterator} = ?assertMatch({ok, _, _}, ?rpc(ProviderSelector, iterator:get_next(AtmWorkflowExecutionEnv, Iterator))),
    ?assertMatch({ok, Res, NewIterator}, ?rpc(ProviderSelector, iterator:get_next(AtmWorkflowExecutionEnv, Iterator))),
    ResList = utils:ensure_list(Res),
    NewExpectedList = check_listed_values(ResList, ExpectedList, Type),
    case ReturnStrategy of
        return_iterators ->
            [{Iterator, ExpectedList}] ++ check_iterator_listing(ProviderSelector, AtmWorkflowExecutionEnv, NewIterator, NewExpectedList, ReturnStrategy, Type);
        _ ->
            ok = ?rpc(ProviderSelector, iterator:forget_before(Iterator)),
            check_iterator_listing(ProviderSelector, AtmWorkflowExecutionEnv, NewIterator, NewExpectedList, ReturnStrategy, Type)
    end.


-spec check_listed_values([file_id:file_guid()], [file_id:file_guid()], atm_data_type:type()) -> ok.
check_listed_values(Values, Expected, Type) ->
    IdsList = lists:map(fun(Value) -> 
        Id = retrieve_id(Type, Value),
        ?assertEqual(true, lists:member(Id, Expected)),
        Id
    end, Values),
    Expected -- IdsList.


-spec retrieve_id(atm_data_type:type(), automation:item()) -> automation:item().
retrieve_id(atm_file_type, #{<<"file_id">> := CdmiId}) ->
    {ok, Guid} = file_id:objectid_to_guid(CdmiId),
    Guid;
retrieve_id(atm_dataset_type, #{<<"datasetId">> := DatasetId}) ->
    DatasetId;
retrieve_id(_, Value) ->
    Value.


-spec create_iteration_test_env(oct_background:entity_selector(), pos_integer(), non_neg_integer(), atm_data_type:type()) ->
    {atm_store_iterator:record(), #{file_id:file_guid() => [file_id:file_guid()]}, [term()]}.
create_iteration_test_env(ProviderSelector, MaxBatchSize, Depth, Type) ->
    create_iteration_test_env(ProviderSelector, MaxBatchSize, Depth, Type, user1).

-spec create_iteration_test_env(oct_background:entity_selector(), pos_integer(), non_neg_integer(), atm_data_type:type(), atom()) ->
    {atm_store_iterator:record(), #{file_id:file_guid() => [file_id:file_guid()]}, [term()]}.
create_iteration_test_env(ProviderSelector, MaxBatchSize, Depth, Type, WorkflowUserPlaceholder) ->
    Node = oct_background:get_random_provider_node(ProviderSelector),
    SpaceId = oct_background:get_space_id(space_krk),
    WorkflowId = datastore_key:new(),
    SessId = oct_background:get_user_session_id(WorkflowUserPlaceholder, krakow),
    UserCtx = rpc:call(Node, user_ctx, new, [SessId]),
    ok = rpc:call(Node, atm_workflow_execution_session, init, [WorkflowId, UserCtx]),
    AtmWorkflowExecutionAuth = rpc:call(Node, atm_workflow_execution_auth, build, [SpaceId, WorkflowId, UserCtx]),
    ChildrenSpecGen = fun
        F(0) -> [];
        F(Depth) ->
            Children = F(Depth - 1),
            [#dir_spec{children = Children}, #dir_spec{children = Children}, #dir_spec{children = Children}, #file_spec{}]
    end,
    ChildrenSpec = ChildrenSpecGen(Depth),
    Spec = [#dir_spec{children = ChildrenSpec, mode = 8#705}, #dir_spec{children = ChildrenSpec, mode = 8#705}, #file_spec{}],
    Objects = onenv_file_test_utils:create_and_sync_file_tree(
        user1, SpaceId, Spec, krakow
    ),
    ObjectToListFun = fun
        F(#object{guid = G, children = undefined}) -> [G];
        F(#object{guid = G, children = Children}) -> [G] ++ lists:flatmap(F, Children)
    end,
    FilesMap = lists:foldl(fun(#object{guid = Guid, type = Type} = Object, Acc) ->
        Acc#{Guid => {Type, ObjectToListFun(Object)}}
    end, #{}, Objects),
    
    {Roots, Expected} = case Type of
        atm_file_type -> {maps:keys(FilesMap), lists:flatten(lists:map(fun({_, V}) -> V end, maps:values(FilesMap)))};
        atm_dataset_type ->
            lists:foldl(fun(Guid, {AccRoots, AccExpected} = Acc) ->
                case {lists:member(Guid, maps:keys(FilesMap)), rand:uniform(2)} of
                    {false, 1} -> Acc;
                    {false, _} ->
                        {ok, DatasetId} = opt_datasets:establish(Node, ?ROOT_SESS_ID, #file_ref{guid = Guid}),
                        {AccRoots, [DatasetId | AccExpected]};
                    {true, _} ->
                        {ok, DatasetId} = opt_datasets:establish(Node, ?ROOT_SESS_ID, #file_ref{guid = Guid}),
                        {[DatasetId | AccRoots], [DatasetId | AccExpected]}
                end
            end, {[], []}, lists:flatten(lists:map(fun({_, V}) -> V end, maps:values(FilesMap))))
    end,
    
    AtmStoreDummySchemaId = <<"dummyId">>,
    
    RootsToAdd = lists:map(fun(Root) ->
        case Type of
            atm_file_type -> 
                {ok, CdmiId} = file_id:guid_to_objectid(Root),
                #{<<"file_id">> => CdmiId};
            atm_dataset_type -> #{<<"datasetId">> => Root}
        end
    end, Roots),
    AtmStoreSchema = atm_store_test_utils:build_store_schema(#atm_tree_forest_store_config{
        item_data_spec = #atm_data_spec{type = Type}
    }),
    {ok, AtmStoreId} = ?extract_key(?rpc(ProviderSelector, atm_store_api:create(
        AtmWorkflowExecutionAuth, RootsToAdd, AtmStoreSchema
    ))),
    AtmStoreIteratorSpec = #atm_store_iterator_spec{
        store_schema_id = AtmStoreDummySchemaId,
        max_batch_size = MaxBatchSize
    },
    AtmWorkflowExecutionEnv = atm_workflow_execution_env:build(
        SpaceId, WorkflowId, 0, #{AtmStoreDummySchemaId => AtmStoreId}
    ),
    AtmStoreIterator0 = ?rpc(ProviderSelector, atm_store_api:acquire_iterator(AtmStoreId, AtmStoreIteratorSpec)),
    {AtmWorkflowExecutionEnv, AtmStoreIterator0, FilesMap, Expected}.


queue_init(MaxValuesPerNode) ->
    Node = oct_background:get_random_provider_node(krakow),
    rpc:call(Node, atm_tree_forest_iterator_queue, init, [MaxValuesPerNode]).


queue_push(Id, Entries, OriginIndex) ->
    Node = oct_background:get_random_provider_node(krakow),
    rpc:call(Node, atm_tree_forest_iterator_queue, push, [Id, Entries, OriginIndex]).


queue_peek(Id, Index) ->
    Node = oct_background:get_random_provider_node(krakow),
    rpc:call(Node, atm_tree_forest_iterator_queue, peek, [Id, Index]).


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
    ModulesToLoad = [?MODULE | atm_infinite_log_based_stores_test_base:modules_to_load()],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60}
        ]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(infinite_log_based_stores_common_tests, Config) ->
    atm_infinite_log_based_stores_test_base:init_per_group(Config);
init_per_group(tree_forest_store_specific_tests, Config) ->
    lfm_proxy:init(Config, false).


end_per_group(infinite_log_based_stores_common_tests, Config) ->
    atm_infinite_log_based_stores_test_base:end_per_group(Config);
end_per_group(tree_forest_store_specific_tests, Config) ->
    lfm_proxy:teardown(Config).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
