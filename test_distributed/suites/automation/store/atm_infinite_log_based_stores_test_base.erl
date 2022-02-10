%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides base for testing automation stores based on infinite log.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_infinite_log_based_stores_test_base).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("test_rpc.hrl").

-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


%% exported for CT
-export([
    create_test_base/1,
    apply_operation_test_base/1,
    iterator_test_base/1,
    browse_content_test_base/2
]).

-type get_item_initializer_data_spec_fun() :: fun((atm_store_config:record()) -> atm_data_spec:record()).
-type prepare_item_initializer_fun() :: fun((atm_value:expanded()) -> atm_value:expanded()).
-type prepare_item_fun() :: fun((
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_store:id()
) ->
    atm_value:expanded()
).
-type randomly_remove_item_fun() :: fun((
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_data_spec:record()
) ->
    boolean()
).

-export_type([
    get_item_initializer_data_spec_fun/0,
    prepare_item_initializer_fun/0,
    prepare_item_fun/0,
    randomly_remove_item_fun/0
]).


-define(PROVIDER_SELECTOR, krakow).

-define(rpc(Expr), ?rpc(?PROVIDER_SELECTOR, Expr)).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec create_test_base(#{
    store_configs := [atm_store_config:record()],
    get_item_initializer_data_spec_fun := get_item_initializer_data_spec_fun(),
    prepare_item_initializer_fun := prepare_item_initializer_fun()
}) ->
    ok | no_return().
create_test_base(#{
    store_configs := AtmStoreConfigs,
    get_item_initializer_data_spec_fun := GetItemInitializerDataSpecFun,
    prepare_item_initializer_fun := PrepareItemInitializerFun
}) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),
    ExampleAtmStoreConfig = lists_utils:random_element(AtmStoreConfigs),

    % Assert creating store with no content initializer (in schema or in args)
    % when it is required fails
    ?assertEqual(
        ?ERROR_ATM_STORE_MISSING_REQUIRED_INITIAL_CONTENT,
        ?rpc(catch atm_store_api:create(
            AtmWorkflowExecutionAuth,
            undefined,
            atm_store_test_utils:build_store_schema(ExampleAtmStoreConfig, true)
        ))
    ),

    % Assert creating store with no content initializer (in schema or in args)
    % when it is optional succeed
    ?assertMatch(
        {ok, #document{value = #atm_store{initial_content = undefined, frozen = false}}},
        ?rpc(atm_store_api:create(
            AtmWorkflowExecutionAuth,
            undefined,
            atm_store_test_utils:build_store_schema(ExampleAtmStoreConfig, false)
        ))
    ),

    lists:foreach(fun(AtmStoreConfig) ->
        ItemInitializerDataSpec = GetItemInitializerDataSpecFun(AtmStoreConfig),
        ItemInitializerDataType = ItemInitializerDataSpec#atm_data_spec.type,

        DefaultItemInitializer = PrepareItemInitializerFun(gen_valid_data(
            AtmWorkflowExecutionAuth, ItemInitializerDataSpec
        )),
        InvalidItemInitializer = PrepareItemInitializerFun(gen_invalid_data(
            AtmWorkflowExecutionAuth, ItemInitializerDataSpec
        )),
        ValidItemInitializer = PrepareItemInitializerFun(gen_valid_data(
            AtmWorkflowExecutionAuth, ItemInitializerDataSpec
        )),

        CreateStoreFun = fun(ContentInitializer) ->
            case rand:uniform(3) of
                1 ->
                    StoreSchema = atm_store_test_utils:build_store_schema(AtmStoreConfig, false),
                    atm_store_api:create(AtmWorkflowExecutionAuth, ContentInitializer, StoreSchema);
                2 ->
                    StoreSchema = atm_store_test_utils:build_store_schema(
                        AtmStoreConfig, false, ContentInitializer
                    ),
                    atm_store_api:create(AtmWorkflowExecutionAuth, undefined, StoreSchema);
                3 ->
                    % Default content initializer (from schema) should be overridden
                    % by one specified in args when creating store
                    StoreSchema = atm_store_test_utils:build_store_schema(
                        AtmStoreConfig, false, [DefaultItemInitializer]
                    ),
                    atm_store_api:create(AtmWorkflowExecutionAuth, ContentInitializer, StoreSchema)
            end
        end,

        % Assert creating store with non array initializer fails
        ?assertEqual(
            ?ERROR_ATM_DATA_TYPE_UNVERIFIED(<<"NaN">>, atm_array_type),
            ?rpc(catch CreateStoreFun(<<"NaN">>))
        ),

        % Assert creating store with array initializer containing some invalid items
        % fails
        ?assertEqual(
            ?ERROR_ATM_DATA_TYPE_UNVERIFIED(InvalidItemInitializer, ItemInitializerDataType),
            ?rpc(catch CreateStoreFun([ValidItemInitializer, InvalidItemInitializer]))
        ),

        % Assert creating store with array initializer containing only valid items
        % succeed
        ValidContentInitializer = [ValidItemInitializer, ValidItemInitializer],
        ?assertMatch(
            {ok, #document{value = #atm_store{initial_content = ValidContentInitializer, frozen = false}}},
            ?rpc(CreateStoreFun(ValidContentInitializer))
        )
    end, AtmStoreConfigs).


-spec apply_operation_test_base(#{
    store_configs := [atm_store_config:record()],
    get_item_initializer_data_spec_fun := get_item_initializer_data_spec_fun(),
    prepare_item_initializer_fun := prepare_item_initializer_fun(),
    prepare_item_fun := prepare_item_fun()
}) ->
    ok | no_return().
apply_operation_test_base(#{
    store_configs := AtmStoreConfigs,
    get_item_initializer_data_spec_fun := GetItemInitializerDataSpecFun,
    prepare_item_initializer_fun := PrepareItemInitializerFun,
    prepare_item_fun := PrepareItemFun
}) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    GenValidItemInitializerFun = fun(AtmDataSpec) ->
        PrepareItemInitializerFun(gen_valid_data(AtmWorkflowExecutionAuth, AtmDataSpec))
    end,
    GenInvalidItemInitializerFun = fun(AtmDataSpec) ->
        PrepareItemInitializerFun(gen_invalid_data(AtmWorkflowExecutionAuth, AtmDataSpec))
    end,
    PrepareExpItemFun = fun(ItemInitializer, AtmDataSpec) ->
        PrepareItemFun(AtmWorkflowExecutionAuth, ItemInitializer, AtmDataSpec)
    end,

    lists:foreach(fun(AtmStoreConfig) ->
        AtmStoreSchema = atm_store_test_utils:build_store_schema(AtmStoreConfig, false),
        ItemInitializerDataSpec = GetItemInitializerDataSpecFun(AtmStoreConfig),
        ItemInitializerDataType = ItemInitializerDataSpec#atm_data_spec.type,

        ContentInitializer = case rand:uniform(2) of
            1 -> undefined;
            2 -> [GenValidItemInitializerFun(ItemInitializerDataSpec)]
        end,
        InitialContent = case ContentInitializer of
            undefined -> [];
            [ItemInitializer] -> [PrepareExpItemFun(ItemInitializer, ItemInitializerDataSpec)]
        end,
        {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
            AtmWorkflowExecutionAuth, ContentInitializer, AtmStoreSchema
        ))),

        NewItemInitializer1 = GenValidItemInitializerFun(ItemInitializerDataSpec),
        NewItem1 = PrepareExpItemFun(NewItemInitializer1, ItemInitializerDataSpec),

        % Assert none operations beside 'append' and 'extend' are supported
        lists:foreach(fun(Operation) ->
            ?assertEqual(?ERROR_NOT_SUPPORTED, ?rpc(catch atm_store_api:apply_operation(
                AtmWorkflowExecutionAuth, Operation, NewItemInitializer1, #{}, AtmStoreId
            ))),
            ?assertMatch(InitialContent, get_content(AtmWorkflowExecutionAuth, AtmStoreId))
        end, atm_task_schema_result_mapper:all_dispatch_functions() -- [append, extend]),

        % Assert append/extend with invalid arg(s) should fail
        InvalidItemInitializer = GenInvalidItemInitializerFun(ItemInitializerDataSpec),
        lists:foreach(fun({Op, Args, ExpError}) ->
            ?assertEqual(ExpError, ?rpc(catch atm_store_api:apply_operation(
                AtmWorkflowExecutionAuth, Op, Args, #{}, AtmStoreId
            ))),
            ?assertMatch(InitialContent, get_content(AtmWorkflowExecutionAuth, AtmStoreId))
        end, [
            {append, InvalidItemInitializer,
                ?ERROR_ATM_DATA_TYPE_UNVERIFIED(InvalidItemInitializer, ItemInitializerDataType)},
            {extend, [NewItemInitializer1, InvalidItemInitializer],
                ?ERROR_ATM_DATA_TYPE_UNVERIFIED(InvalidItemInitializer, ItemInitializerDataType)}
            %% TODO VFS-8686 refactor atm data types errors to properly handle list types
%%            {extend, NewItemInitializer1,
%%                ?ERROR_ATM_DATA_TYPE_UNVERIFIED(NewItemInitializer1, atm_array_type)}
        ]),

        % Assert it is not possible to perform operation on frozen store
        ?rpc(atm_store_api:freeze(AtmStoreId)),
        RandomOp = lists_utils:random_element([append, extend]),
        ?assertEqual(
            ?ERROR_ATM_STORE_FROZEN(AtmStoreSchema#atm_store_schema.id),
            ?rpc(catch atm_store_api:apply_operation(
                AtmWorkflowExecutionAuth, RandomOp, NewItemInitializer1, #{}, AtmStoreId
            ))
        ),
        ?assertMatch(InitialContent, get_content(AtmWorkflowExecutionAuth, AtmStoreId)),

        % Otherwise operation should succeed
        ?rpc(atm_store_api:unfreeze(AtmStoreId)),
        ?assertEqual(ok, ?rpc(atm_store_api:apply_operation(
            AtmWorkflowExecutionAuth, append, NewItemInitializer1, #{}, AtmStoreId
        ))),
        ExpContent1 = InitialContent ++ [NewItem1],
        ?assertMatch(ExpContent1, get_content(AtmWorkflowExecutionAuth, AtmStoreId)),

        % simulate time passage as operations of some stores (audit log) may depend on it
        time_test_utils:simulate_seconds_passing(10),

        NewItemInitializer2 = GenValidItemInitializerFun(ItemInitializerDataSpec),
        NewItemInitializer3 = GenValidItemInitializerFun(ItemInitializerDataSpec),
        ?assertEqual(ok, ?rpc(atm_store_api:apply_operation(
            AtmWorkflowExecutionAuth, extend, [NewItemInitializer2, NewItemInitializer3], #{}, AtmStoreId
        ))),

        NewItem2 = PrepareExpItemFun(NewItemInitializer2, ItemInitializerDataSpec),
        NewItem3 = PrepareExpItemFun(NewItemInitializer3, ItemInitializerDataSpec),
        ExpContent2 = ExpContent1 ++ [NewItem2, NewItem3],
        ?assertMatch(ExpContent2, get_content(AtmWorkflowExecutionAuth, AtmStoreId))

    end, AtmStoreConfigs).


-spec iterator_test_base(#{
    store_configs := [atm_store_config:record()],
    get_item_initializer_data_spec_fun := get_item_initializer_data_spec_fun(),
    prepare_item_initializer_fun := prepare_item_initializer_fun(),
    prepare_item_fun := prepare_item_fun(),
    randomly_remove_item_fun := randomly_remove_item_fun()
}) ->
    ok | no_return().
iterator_test_base(#{
    store_configs := AtmStoreConfigs,
    get_item_initializer_data_spec_fun := GetItemInitializerDataSpecFun,
    prepare_item_initializer_fun := PrepareItemInitializerFun,
    prepare_item_fun := PrepareItemFun,
    randomly_remove_item_fun := RandomlyRemoveItemFun
}) ->
    ItemsCount = 10 + rand:uniform(100),
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    lists:foreach(fun(AtmStoreConfig) ->
        AtmStoreSchema = atm_store_test_utils:build_store_schema(AtmStoreConfig, false),
        AtmDataSpec = GetItemInitializerDataSpecFun(AtmStoreConfig),

        {ContentInitializer, ExpContent} = lists:unzip(lists:map(fun(_) ->
            ItemInitializer = PrepareItemInitializerFun(gen_valid_data(AtmWorkflowExecutionAuth, AtmDataSpec)),
            Item = PrepareItemFun(AtmWorkflowExecutionAuth, ItemInitializer, AtmDataSpec),
            {ItemInitializer, Item}
        end, lists:seq(1, ItemsCount))),

        {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
            AtmWorkflowExecutionAuth, ContentInitializer, AtmStoreSchema
        ))),
        MaxBatchSize = rand:uniform(ItemsCount),
        ExpBatches = atm_store_test_utils:split_into_chunks(MaxBatchSize, [], ExpContent),

        AtmWorkflowExecutionEnv = atm_store_test_utils:build_workflow_execution_env(
            AtmWorkflowExecutionAuth, AtmStoreSchema, AtmStoreId
        ),

        % Assert entire store content can be iterated in batches using iterators
        AtmStoreIterator0 = ?rpc(atm_store_api:acquire_iterator(AtmStoreId, #atm_store_iterator_spec{
            store_schema_id = AtmStoreSchema#atm_store_schema.id,
            max_batch_size = MaxBatchSize
        })),
        {UsedIterators, LastIterator} = lists:mapfoldl(fun(ExpBatch, Iterator) ->
            {ok, _, NewIterator} = ?assertMatch(
                {ok, ExpBatch, _},
                ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, Iterator))
            ),
            {Iterator, NewIterator}
        end, AtmStoreIterator0, ExpBatches),
        ?assertEqual(stop, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, LastIterator))),

        % Assert iterators can be reused
        {Index, IteratorToReuse} = lists_utils:random_element(lists_utils:enumerate(UsedIterators)),
        NewLastIterator = lists:foldl(fun(ExpBatch, Iterator) ->
            {ok, _, NewIterator} = ?assertMatch(
                {ok, ExpBatch, _},
                ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, Iterator))
            ),
            NewIterator
        end, IteratorToReuse, lists:nthtail(Index - 1, ExpBatches)),
        ?assertEqual(stop, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, NewLastIterator))),

        %% Assert non accessible items (e.g. removed) are omitted from iterated items
        AccessibleItems = lists:filter(fun(ExpItem) ->
            not RandomlyRemoveItemFun(AtmWorkflowExecutionAuth, ExpItem, AtmDataSpec)
        end, ExpContent),
        AtmStoreIterator1 = ?rpc(atm_store_api:acquire_iterator(AtmStoreId, #atm_store_iterator_spec{
            store_schema_id = AtmStoreSchema#atm_store_schema.id,
            max_batch_size = ItemsCount
        })),
        ?assertMatch(
            {ok, AccessibleItems, _},
            ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, AtmStoreIterator1))
        )

    end, AtmStoreConfigs).


-spec browse_content_test_base(index | offset, #{
    store_configs := [atm_store_config:record()],
    get_item_initializer_data_spec_fun := get_item_initializer_data_spec_fun(),
    prepare_item_initializer_fun := prepare_item_initializer_fun(),
    prepare_item_fun := prepare_item_fun(),
    randomly_remove_item_fun := randomly_remove_item_fun()
}) ->
    ok | no_return().
browse_content_test_base(BrowsingMethod, #{
    store_configs := AtmStoreConfigs,
    get_item_initializer_data_spec_fun := GetItemInitializerDataSpecFun,
    prepare_item_initializer_fun := PrepareItemInitializerFun,
    prepare_item_fun := PrepareItemFun,
    randomly_remove_item_fun := RandomlyRemoveItemFun
}) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),
    Length = rand:uniform(10),

    lists:foreach(fun(AtmStoreConfig) ->
        AtmStoreSchema = atm_store_test_utils:build_store_schema(AtmStoreConfig, false),
        AtmDataSpec = GetItemInitializerDataSpecFun(AtmStoreConfig),

        ContentInitializer = lists:map(fun(_) ->
            PrepareItemInitializerFun(gen_valid_data(AtmWorkflowExecutionAuth, AtmDataSpec))
        end, lists:seq(1, Length)),
        Content = lists:map(fun(ItemInitializer) ->
            PrepareItemFun(AtmWorkflowExecutionAuth, ItemInitializer, AtmDataSpec)
        end, ContentInitializer),

        {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
            AtmWorkflowExecutionAuth, ContentInitializer, AtmStoreSchema
        ))),

        %% TODO rm some items? - browsing should contain errors

        lists:foreach(fun(_) ->
            StartIndex = rand:uniform(Length),
            BrowseOpts0 = case BrowsingMethod of
                index -> #{start_index => integer_to_binary(StartIndex)};
                offset -> #{offset => StartIndex}
            end,
            Limit = rand:uniform(Length),
            BrowseOpts1 = BrowseOpts0#{limit => Limit},

            Expected = lists:map(fun(Index) ->
                {integer_to_binary(Index), {ok, lists:nth(Index + 1, Content)}}
            end, lists:seq(StartIndex, min(StartIndex + Limit - 1, Length - 1))),

            ?assertEqual(
                {Expected, StartIndex + Limit >= Length},
                ?rpc(atm_store_api:browse_content(AtmWorkflowExecutionAuth, BrowseOpts1, AtmStoreId))
            )
        end, lists:seq(1, 8))

    end, AtmStoreConfigs).


%===================================================================
% Helper functions
%===================================================================


%% @private
-spec create_workflow_execution_auth() -> atm_workflow_execution_auth:record().
create_workflow_execution_auth() ->
    atm_store_test_utils:create_workflow_execution_auth(
        ?PROVIDER_SELECTOR, user1, space_krk
    ).


%% @private
-spec gen_valid_data(atm_workflow_execution_auth:record(), atm_data_spec:record()) ->
    atm_value:expanded().
gen_valid_data(AtmWorkflowExecutionAuth, ItemDataSpec) ->
    atm_store_test_utils:gen_valid_data(
        ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, ItemDataSpec
    ).


%% @private
-spec gen_invalid_data(atm_workflow_execution_auth:record(), atm_data_spec:record()) ->
    atm_value:expanded().
gen_invalid_data(AtmWorkflowExecutionAuth, ItemDataSpec) ->
    atm_store_test_utils:gen_invalid_data(
        ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, ItemDataSpec
    ).


%% @private
-spec get_content(atm_workflow_execution_auth:record(), atm_store:id()) ->
    undefined | atm_value:expanded().
get_content(AtmWorkflowExecutionAuth, AtmStoreId) ->
    BrowseOpts = #{offset => 0, limit => 1000},
    {Items, true} = ?rpc(atm_store_api:browse_content(
        AtmWorkflowExecutionAuth, BrowseOpts, AtmStoreId
    )),
    lists:map(fun({_, {ok, Item}}) -> Item end, Items).
