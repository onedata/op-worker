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
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").


%% API
-export([
    modules_to_load/0,
    init_per_group/1,
    end_per_group/1
]).
-export([
    create_test_base/1,
    update_content_test_base/1,
    iterator_test_base/1,
    browse_content_test_base/2
]).

% Returns data spec used to generate valid or invalid data that would be used to
% format store input item (some stores may support several formats of input item)
-type get_input_item_generator_seed_data_spec() :: fun((atm_store_config:record()) -> atm_data_spec:record()).

% Formats data generated using `get_input_item_generator_seed_data_spec()`
% into one of the possible forms accepted as input by given store
-type input_item_formatter() :: fun((automation:item()) -> automation:item()).

% Prepares expected item (as stored and returned when browsing) from corresponding input item
-type input_item_to_exp_store_item() :: fun((
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_store:id(),
    Index :: non_neg_integer()
) ->
    automation:item()
).

% Some atm data types are only references to entities in op. Removing such
% entities should affect store iteration (they are omitted) and browsing
% (error item is returned)
-type randomly_remove_entity_referenced_by_item() :: fun((
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_data_spec:record()
) ->
    false | {true, errors:error()}
).

-type build_content_update_options() :: fun((atm_list_store_content_update_options:update_function()) ->
    atm_store_content_update_options:record()
).

% Returns entire store content
-type get_content() :: fun((atm_workflow_execution_auth:record(), atm_store:id()) ->
    [automation:item()]
).

-type build_content_browse_options() :: fun((json_utils:json_map()) ->
    atm_store_content_browse_options:record()
).

-type build_content_browse_result() :: fun(([atm_store_container_infinite_log_backend:entry()], boolean()) ->
    atm_store_content_browse_result:record()
).

-export_type([
    get_input_item_generator_seed_data_spec/0,
    input_item_formatter/0,
    input_item_to_exp_store_item/0,
    randomly_remove_entity_referenced_by_item/0,
    build_content_update_options/0,
    get_content/0,
    build_content_browse_options/0,
    build_content_browse_result/0
]).


-define(PROVIDER_SELECTOR, krakow).
-define(rpc(Expr), ?rpc(?PROVIDER_SELECTOR, Expr)).


%%%===================================================================
%%% API functions
%%%===================================================================


modules_to_load() ->
    [?MODULE, atm_store_test_utils].


init_per_group(Config) ->
    time_test_utils:freeze_time(Config),
    Config.


end_per_group(Config) ->
    time_test_utils:unfreeze_time(Config).


-spec create_test_base(#{
    store_configs := [atm_store_config:record()],
    get_input_item_generator_seed_data_spec := get_input_item_generator_seed_data_spec(),
    input_item_formatter := input_item_formatter()
}) ->
    ok | no_return().
create_test_base(#{
    store_configs := AtmStoreConfigs,
    get_input_item_generator_seed_data_spec := GetInputItemGeneratorSeedDataSpecFun,
    input_item_formatter := InputItemFormatterFun
}) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),
    ExampleAtmStoreConfig = lists_utils:random_element(AtmStoreConfigs),

    % Assert creating store with no initial content (in schema or in args)
    % when it is required fails
    ?assertEqual(
        ?ERROR_ATM_STORE_MISSING_REQUIRED_INITIAL_CONTENT,
        ?rpc(catch atm_store_api:create(
            AtmWorkflowExecutionAuth,
            undefined,
            atm_store_test_utils:build_store_schema(ExampleAtmStoreConfig, true)
        ))
    ),

    % Assert creating store with no initial content (in schema or in args)
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
        InputItemGeneratorSeedDataSpec = GetInputItemGeneratorSeedDataSpecFun(AtmStoreConfig),

        DefaultInputItem = InputItemFormatterFun(gen_valid_data(
            AtmWorkflowExecutionAuth, InputItemGeneratorSeedDataSpec
        )),
        CreateStoreFun = atm_store_test_utils:build_create_store_with_initial_content_fun(
            AtmWorkflowExecutionAuth, AtmStoreConfig, [DefaultInputItem]
        ),
        ValidInputItemDataSeed = gen_valid_data(
            AtmWorkflowExecutionAuth, InputItemGeneratorSeedDataSpec
        ),
        ValidInputItem = InputItemFormatterFun(ValidInputItemDataSeed),
        InvalidInputItemDataSeed = gen_invalid_data(AtmWorkflowExecutionAuth, InputItemGeneratorSeedDataSpec),
        InvalidInputItem = InputItemFormatterFun(InvalidInputItemDataSeed),

        % Assert creating store with non array initial content fails
        ?assertEqual(
            ?ERROR_ATM_DATA_TYPE_UNVERIFIED(<<"NaN">>, atm_array_type),
            ?rpc(catch CreateStoreFun(<<"NaN">>))
        ),

        % Assert creating store with array initial content containing some invalid items fails
        ExpError = ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
            [ValidInputItemDataSeed, InvalidInputItemDataSeed],
            atm_array_type,
            #{<<"$[1]">> => errors:to_json(atm_store_test_utils:infer_exp_invalid_data_error(
                InvalidInputItemDataSeed, InputItemGeneratorSeedDataSpec
            ))}
        ),
        ?assertEqual(ExpError,
            ?rpc(catch CreateStoreFun([ValidInputItem, InvalidInputItem]))
        ),

        % Assert creating store with array initial content containing only valid items succeed
        ValidInputContent = [ValidInputItem, ValidInputItem],
        ?assertMatch(
            {ok, #document{value = #atm_store{initial_content = ValidInputContent, frozen = false}}},
            ?rpc(CreateStoreFun(ValidInputContent))
        )
    end, AtmStoreConfigs).


-spec update_content_test_base(#{
    store_configs := [atm_store_config:record()],
    get_input_item_generator_seed_data_spec := get_input_item_generator_seed_data_spec(),
    input_item_formatter := input_item_formatter(),
    input_item_to_exp_store_item := input_item_to_exp_store_item(),
    build_content_update_options := build_content_update_options(),
    get_content := get_content()
}) ->
    ok | no_return().
update_content_test_base(#{
    store_configs := AtmStoreConfigs,
    get_input_item_generator_seed_data_spec := GetInputItemGeneratorSeedDataSpecFun,
    input_item_formatter := InputItemFormatterFun,
    input_item_to_exp_store_item := InputItemToExpStoreItemFun,
    build_content_update_options := BuildContentUpdateOptionsFun,
    get_content := GetContentFun
}) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    GenValidInputItemFun = fun(AtmDataSpec) ->
        InputItemFormatterFun(gen_valid_data(AtmWorkflowExecutionAuth, AtmDataSpec))
    end,
    PrepareExpStoreItemFun = fun(InputItem, AtmDataSpec, Index) ->
        InputItemToExpStoreItemFun(AtmWorkflowExecutionAuth, InputItem, AtmDataSpec, Index)
    end,

    lists:foreach(fun(AtmStoreConfig) ->
        AtmStoreSchema = atm_store_test_utils:build_store_schema(AtmStoreConfig, false),
        InputItemGeneratorSeedDataSpec = GetInputItemGeneratorSeedDataSpecFun(AtmStoreConfig),

        InitialInputContent = case rand:uniform(2) of
            1 -> undefined;
            2 -> [GenValidInputItemFun(InputItemGeneratorSeedDataSpec)]
        end,
        {InitialStoreContent, Offset} = case InitialInputContent of
            undefined -> {[], 0};
            [InputItem] -> {[PrepareExpStoreItemFun(InputItem, InputItemGeneratorSeedDataSpec, 0)], 1}
        end,
        {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
            AtmWorkflowExecutionAuth, InitialInputContent, AtmStoreSchema
        ))),

        NewInputItemDataSeed1 = gen_valid_data(
            AtmWorkflowExecutionAuth, InputItemGeneratorSeedDataSpec
        ),
        NewInputItem1 = InputItemFormatterFun(NewInputItemDataSeed1),
        NewItem1 = PrepareExpStoreItemFun(NewInputItem1, InputItemGeneratorSeedDataSpec, Offset),

        % Assert append/extend with invalid arg(s) should fail
        InvalidInputItemDataSeed = gen_invalid_data(
            AtmWorkflowExecutionAuth, InputItemGeneratorSeedDataSpec
        ),
        InvalidInputItem = InputItemFormatterFun(InvalidInputItemDataSeed),
        ExpInvalidInputItemError = atm_store_test_utils:infer_exp_invalid_data_error(
            InvalidInputItemDataSeed, InputItemGeneratorSeedDataSpec
        ),
        lists:foreach(fun({Function, Args, ExpError}) ->
            ?assertEqual(ExpError, ?rpc(catch atm_store_api:update_content(
                AtmWorkflowExecutionAuth, Args, BuildContentUpdateOptionsFun(Function), AtmStoreId
            ))),
            ?assertEqual(InitialStoreContent, GetContentFun(AtmWorkflowExecutionAuth, AtmStoreId))
        end, [
            {append, InvalidInputItem, ExpInvalidInputItemError},
            {extend, [NewInputItem1, InvalidInputItem], ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                [NewInputItemDataSeed1, InvalidInputItemDataSeed],
                atm_array_type,
                #{<<"$[1]">> => errors:to_json(ExpInvalidInputItemError)}
            )},
            {extend, NewInputItem1, atm_store_test_utils:infer_exp_invalid_data_error(
                NewInputItem1, #atm_array_data_spec{
                    item_data_spec = InputItemGeneratorSeedDataSpec
                }
            )}
        ]),

        % Assert it is not possible to perform operation on frozen store
        ?rpc(atm_store_api:freeze(AtmStoreId)),
        ?assertEqual(
            ?ERROR_ATM_STORE_FROZEN(AtmStoreSchema#atm_store_schema.id),
            ?rpc(catch atm_store_api:update_content(
                AtmWorkflowExecutionAuth,
                NewInputItem1,
                BuildContentUpdateOptionsFun(lists_utils:random_element([append, extend])),
                AtmStoreId
            ))
        ),
        ?assertEqual(InitialStoreContent, GetContentFun(AtmWorkflowExecutionAuth, AtmStoreId)),

        % Otherwise operation should succeed
        ?rpc(atm_store_api:unfreeze(AtmStoreId)),
        ?assertEqual(ok, ?rpc(atm_store_api:update_content(
            AtmWorkflowExecutionAuth,
            NewInputItem1,
            BuildContentUpdateOptionsFun(append),
            AtmStoreId
        ))),
        ExpStoreContent1 = InitialStoreContent ++ [NewItem1],
        ?assertEqual(ExpStoreContent1, GetContentFun(AtmWorkflowExecutionAuth, AtmStoreId)),

        NewInputItem2 = GenValidInputItemFun(InputItemGeneratorSeedDataSpec),
        NewInputItem3 = GenValidInputItemFun(InputItemGeneratorSeedDataSpec),
        ?assertEqual(ok, ?rpc(atm_store_api:update_content(
            AtmWorkflowExecutionAuth,
            [NewInputItem2, NewInputItem3],
            BuildContentUpdateOptionsFun(extend),
            AtmStoreId
        ))),
        NewItem2 = PrepareExpStoreItemFun(NewInputItem2, InputItemGeneratorSeedDataSpec, Offset + 1),
        NewItem3 = PrepareExpStoreItemFun(NewInputItem3, InputItemGeneratorSeedDataSpec, Offset + 2),
        ExpStoreContent2 = ExpStoreContent1 ++ [NewItem2, NewItem3],
        ?assertEqual(ExpStoreContent2, GetContentFun(AtmWorkflowExecutionAuth, AtmStoreId))

    end, AtmStoreConfigs).


-spec iterator_test_base(#{
    store_configs := [atm_store_config:record()],
    get_input_item_generator_seed_data_spec := get_input_item_generator_seed_data_spec(),
    input_item_formatter := input_item_formatter(),
    input_item_to_exp_store_item := input_item_to_exp_store_item(),
    randomly_remove_entity_referenced_by_item := randomly_remove_entity_referenced_by_item()
}) ->
    ok | no_return().
iterator_test_base(#{
    store_configs := AtmStoreConfigs,
    get_input_item_generator_seed_data_spec := GetInputItemGeneratorSeedDataSpecFun,
    input_item_formatter := InputItemFormatterFun,
    input_item_to_exp_store_item := InputItemToExpStoreItemFun,
    randomly_remove_entity_referenced_by_item := RandomlyRemoveEntityReferencedByItemFun
}) ->
    ItemsCount = 10 + rand:uniform(100),
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    lists:foreach(fun(AtmStoreConfig) ->
        AtmStoreSchema = atm_store_test_utils:build_store_schema(AtmStoreConfig, false),
        AtmDataSpec = GetInputItemGeneratorSeedDataSpecFun(AtmStoreConfig),

        {InitialInputContent, ExpStoreContent} = lists:unzip(lists:map(fun(Index) ->
            InputItem = InputItemFormatterFun(gen_valid_data(AtmWorkflowExecutionAuth, AtmDataSpec)),
            Item = InputItemToExpStoreItemFun(AtmWorkflowExecutionAuth, InputItem, AtmDataSpec, Index - 1),
            {InputItem, Item}
        end, lists:seq(1, ItemsCount))),

        {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
            AtmWorkflowExecutionAuth, InitialInputContent, AtmStoreSchema
        ))),
        MaxBatchSize = rand:uniform(ItemsCount),
        ExpBatches = atm_store_test_utils:split_into_chunks(MaxBatchSize, [], ExpStoreContent),

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
            case RandomlyRemoveEntityReferencedByItemFun(AtmWorkflowExecutionAuth, ExpItem, AtmDataSpec) of
                {true, _} -> false;
                false -> true
            end
        end, ExpStoreContent),
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
    get_input_item_generator_seed_data_spec := get_input_item_generator_seed_data_spec(),
    input_item_formatter := input_item_formatter(),
    input_item_to_exp_store_item := input_item_to_exp_store_item(),
    randomly_remove_entity_referenced_by_item := randomly_remove_entity_referenced_by_item(),
    build_content_browse_options := build_content_browse_options(),
    build_content_browse_result := build_content_browse_result()
}) ->
    ok | no_return().
browse_content_test_base(BrowsingMethod, #{
    store_configs := AtmStoreConfigs,
    get_input_item_generator_seed_data_spec := GetInputItemGeneratorSeedDataSpecFun,
    input_item_formatter := InputItemFormatterFun,
    input_item_to_exp_store_item := InputItemToExpStoreItemFun,
    randomly_remove_entity_referenced_by_item := RandomlyRemoveEntityReferencedByItemFun,
    build_content_browse_options := BuildContentBrowseOptionsFun,
    build_content_browse_result := BuildContentBrowseResultFun
}) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),
    Length = rand:uniform(10),

    lists:foreach(fun(AtmStoreConfig) ->
        AtmStoreSchema = atm_store_test_utils:build_store_schema(AtmStoreConfig, false),
        AtmDataSpec = GetInputItemGeneratorSeedDataSpecFun(AtmStoreConfig),

        InitialInputContent = lists:map(fun(_) ->
            InputItemFormatterFun(gen_valid_data(AtmWorkflowExecutionAuth, AtmDataSpec))
        end, lists:seq(1, Length)),
        {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
            AtmWorkflowExecutionAuth, InitialInputContent, AtmStoreSchema
        ))),

        Content = lists:map(fun({Index, InputItem}) ->
            Item = InputItemToExpStoreItemFun(
                AtmWorkflowExecutionAuth, InputItem, AtmDataSpec, Index - 1
            ),
            case RandomlyRemoveEntityReferencedByItemFun(
                AtmWorkflowExecutionAuth, Item, AtmDataSpec
            ) of
                {true, ExpError} -> ExpError;
                false -> {ok, Item}
            end
        end, lists_utils:enumerate(InitialInputContent)),

        lists:foreach(fun(_) ->
            StartIndex = rand:uniform(Length),
            BrowseOptsJson0 = case BrowsingMethod of
                index -> #{<<"index">> => integer_to_binary(StartIndex)};
                offset -> #{<<"offset">> => StartIndex}
            end,
            Limit = rand:uniform(Length),
            BrowseOptsJson1 = BrowseOptsJson0#{<<"limit">> => Limit},
            BrowseOpts = BuildContentBrowseOptionsFun(BrowseOptsJson1),

            ExpContent = lists:map(fun(Index) ->
                {integer_to_binary(Index), lists:nth(Index + 1, Content)}
            end, lists:seq(StartIndex, min(StartIndex + Limit - 1, Length - 1))),
            ExpBrowseResult = BuildContentBrowseResultFun(ExpContent, StartIndex + Limit >= Length),

            ?assertEqual(ExpBrowseResult, ?rpc(atm_store_api:browse_content(
                AtmWorkflowExecutionAuth, BrowseOpts, AtmStoreId
            )))
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
    automation:item().
gen_valid_data(AtmWorkflowExecutionAuth, ItemDataSpec) ->
    atm_store_test_utils:gen_valid_data(
        ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, ItemDataSpec
    ).


%% @private
-spec gen_invalid_data(atm_workflow_execution_auth:record(), atm_data_spec:record()) ->
    automation:item().
gen_invalid_data(AtmWorkflowExecutionAuth, ItemDataSpec) ->
    atm_store_test_utils:gen_invalid_data(
        ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, ItemDataSpec
    ).
