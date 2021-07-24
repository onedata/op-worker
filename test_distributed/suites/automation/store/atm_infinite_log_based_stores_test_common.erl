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
-module(atm_infinite_log_based_stores_test_common).
-author("Michal Stanisz").

-include("modules/automation/atm_tmp.hrl").
-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_runner.hrl").

-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


%% exported for CT
-export([
    create_store_with_invalid_args_test_base/1,
    apply_operation_test_base/1,
    iterate_one_by_one_test_base/2,
    iterate_in_chunks_test_base/2,
    reuse_iterator_test_base/2,
    browse_by_index_test_base/2,
    browse_by_offset_test_base/2
]).

-define(ATTEMPTS, 30).


%%%===================================================================
%%% API functions
%%%===================================================================


create_store_with_invalid_args_test_base(AtmStoreSchema) ->
    AtmWorkflowExecutionAuth = atm_store_test_utils:create_workflow_execution_auth(
        krakow, user1, space_krk
    ),

    ?assertEqual(?ERROR_ATM_STORE_MISSING_REQUIRED_INITIAL_VALUE, atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionAuth, undefined, AtmStoreSchema#atm_store_schema{requires_initial_value = true}
    )),
    ?assertEqual(?ERROR_ATM_BAD_DATA(<<"value">>, <<"not a batch">>), atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionAuth, 8, AtmStoreSchema
    )),

    lists:foreach(fun(DataType) ->
        BadValue = atm_store_test_utils:example_bad_data(DataType),
        ValidValue = atm_store_test_utils:example_data(DataType),
        ?assertEqual(?ERROR_ATM_DATA_TYPE_UNVERIFIED(BadValue, DataType), atm_store_test_utils:create_store(
            krakow, AtmWorkflowExecutionAuth, [ValidValue, BadValue, ValidValue],
            AtmStoreSchema#atm_store_schema{data_spec = #atm_data_spec{type = DataType}}
        ))
    end, atm_store_test_utils:all_data_types()).


apply_operation_test_base(AtmStoreSchema) ->
    AtmWorkflowExecutionAuth = atm_store_test_utils:create_workflow_execution_auth(
        krakow, user1, space_krk
    ),

    {ok, AtmStoreId0} = atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionAuth, undefined, AtmStoreSchema
    ),
    
    ?assertEqual(?ERROR_NOT_SUPPORTED, atm_store_test_utils:apply_operation(
        krakow, AtmWorkflowExecutionAuth, set, <<"NaN">>, #{}, AtmStoreId0
    )),
    
    lists:foreach(fun(DataType) ->
        {ok, AtmStoreId} = atm_store_test_utils:create_store(
            krakow, AtmWorkflowExecutionAuth, undefined,
            AtmStoreSchema#atm_store_schema{data_spec = #atm_data_spec{type = DataType}}
        ),
        BadValue = atm_store_test_utils:example_bad_data(DataType),
        ValidValue = atm_store_test_utils:example_data(DataType),
        
        ?assertEqual(?ERROR_ATM_DATA_TYPE_UNVERIFIED(BadValue, DataType), atm_store_test_utils:apply_operation(
            krakow, AtmWorkflowExecutionAuth, append, BadValue, #{}, AtmStoreId
        )),
        ?assertEqual(ok, atm_store_test_utils:apply_operation(
            krakow, AtmWorkflowExecutionAuth, append, ValidValue, #{}, AtmStoreId
        )),
        ?assertEqual(?ERROR_ATM_DATA_TYPE_UNVERIFIED(BadValue, DataType), atm_store_test_utils:apply_operation(
            krakow, AtmWorkflowExecutionAuth, append, [ValidValue, BadValue, ValidValue], #{<<"isBatch">> => true}, AtmStoreId
        )),
        ?assertEqual(ok, atm_store_test_utils:apply_operation(
            krakow, AtmWorkflowExecutionAuth, append, lists:duplicate(8, ValidValue), #{<<"isBatch">> => true}, AtmStoreId
        ))
    end, atm_store_test_utils:all_data_types()).


iterate_one_by_one_test_base(AtmStoreSchema, ResultMapper) ->
    Length = 8,
    AtmStoreIteratorStrategy = #atm_store_iterator_serial_strategy{},
    iterate_test_base_internal(AtmStoreSchema, AtmStoreIteratorStrategy, Length, lists:seq(1, Length), ResultMapper).


iterate_in_chunks_test_base(AtmStoreSchema, ResultMapper) ->
    Length = 8,
    ChunkSize = rand:uniform(Length),
    AtmStoreIteratorStrategy = #atm_store_iterator_batch_strategy{size = ChunkSize},
    ExpectedResultsList = atm_store_test_utils:split_into_chunks(ChunkSize, [], lists:seq(1, Length)),
    iterate_test_base_internal(AtmStoreSchema, AtmStoreIteratorStrategy, Length, ExpectedResultsList, ResultMapper).


%% @private
-spec iterate_test_base_internal(atm_store_schema:record(), atm_store_iterator_config:record(), 
    non_neg_integer(), [term()], fun((automation:item()) -> automation:item())
) ->
    ok.
iterate_test_base_internal(AtmStoreSchema, AtmStoreIteratorStrategy, Length, ExpectedResultsList, ResultMapper) ->
    Items = lists:seq(1, Length),

    AtmWorkflowExecutionAuth = atm_store_test_utils:create_workflow_execution_auth(
        krakow, user1, space_krk
    ),

    {ok, AtmStoreId} = atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionAuth, Items, AtmStoreSchema
    ),

    AtmStoreDummySchemaId = <<"dummyId">>,
    
    AtmWorkflowExecutionEnv = atm_workflow_execution_env:build(
        atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),
        atm_workflow_execution_auth:get_workflow_execution_id(AtmWorkflowExecutionAuth),
        #{AtmStoreDummySchemaId => AtmStoreId}
    ),
    AtmStoreIteratorSpec = #atm_store_iterator_spec{
        store_schema_id = AtmStoreDummySchemaId,
        strategy = AtmStoreIteratorStrategy
    },
    AtmStoreIterator0 = atm_store_test_utils:acquire_store_iterator(
        krakow, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec
    ),

    lists:foldl(fun(Expected, Iterator) ->
        {ok, _, NewIterator} = ?assertMatch({ok, Expected, _}, 
            map_iteration_result(atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, Iterator), ResultMapper)),
        NewIterator
    end, AtmStoreIterator0, ExpectedResultsList),
    ok.


reuse_iterator_test_base(AtmStoreSchema, ResultMapper) ->
    AtmWorkflowExecutionAuth = atm_store_test_utils:create_workflow_execution_auth(
        krakow, user1, space_krk
    ),

    Items = lists:seq(1, 5),
    {ok, AtmStoreId} = atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionAuth, Items, AtmStoreSchema
    ),
    
    AtmStoreDummySchemaId = <<"dummyId">>,

    AtmWorkflowExecutionEnv = atm_workflow_execution_env:build(
        atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),
        atm_workflow_execution_auth:get_workflow_execution_id(AtmWorkflowExecutionAuth),
        #{AtmStoreDummySchemaId => AtmStoreId}
    ),
    AtmStoreIteratorSpec = #atm_store_iterator_spec{
        store_schema_id = AtmStoreDummySchemaId,
        strategy = #atm_store_iterator_serial_strategy{}
    },
    AtmSerialIterator0 = atm_store_test_utils:acquire_store_iterator(krakow, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec),

    {ok, _, AtmSerialIterator1} = ?assertMatch({ok, 1, _}, 
        map_iteration_result(atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator0), ResultMapper)),
    {ok, _, AtmSerialIterator2} = ?assertMatch({ok, 2, _}, 
        map_iteration_result(atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator1), ResultMapper)),
    {ok, _, AtmSerialIterator3} = ?assertMatch({ok, 3, _}, 
        map_iteration_result(atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator2), ResultMapper)),
    {ok, _, AtmSerialIterator4} = ?assertMatch({ok, 4, _}, 
        map_iteration_result(atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator3), ResultMapper)),
    {ok, _, AtmSerialIterator5} = ?assertMatch({ok, 5, _}, 
        map_iteration_result(atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator4), ResultMapper)),
    ?assertMatch(stop, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator5)),
    
    ?assertMatch({ok, 1, _}, 
        map_iteration_result(atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator0), ResultMapper)),
    
    {ok, _, AtmSerialIterator7} = ?assertMatch({ok, 4, _}, 
        map_iteration_result(atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator3), ResultMapper)),
    ?assertMatch({ok, 5, _}, 
        map_iteration_result(atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator7), ResultMapper)),

    {ok, _, AtmSerialIterator9} = ?assertMatch({ok, 2, _}, 
        map_iteration_result(atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator1), ResultMapper)),
    ?assertMatch({ok, 3, _}, 
        map_iteration_result(atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator9), ResultMapper)).


browse_by_index_test_base(AtmStoreSchema, ResultMapper) ->
    browse_content_test_base(AtmStoreSchema, ResultMapper, index).


browse_by_offset_test_base(AtmStoreSchema, ResultMapper) ->
    browse_content_test_base(AtmStoreSchema, ResultMapper, offset).


browse_content_test_base(AtmStoreSchema, ResultMapper, Type) ->
    AtmWorkflowExecutionAuth = atm_store_test_utils:create_workflow_execution_auth(
        krakow, user1, space_krk
    ),
    ItemsNum = rand:uniform(10),
    Items = lists:seq(1, ItemsNum),
    {ok, AtmStoreId} = atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionAuth, Items, AtmStoreSchema
    ),
    {ok, AtmStore} = atm_store_test_utils:get(krakow, AtmStoreId),
    lists:foreach(fun(_) ->
        StartIndex = rand:uniform(ItemsNum),
        Limit = rand:uniform(ItemsNum),
        Expected = lists:map(fun(Index) ->
            {integer_to_binary(Index), {ok, Index + 1}}
        end, lists:seq(StartIndex, min(StartIndex + Limit - 1, ItemsNum - 1))),
        Opts = case Type of
            index -> #{start_index => integer_to_binary(StartIndex)};
            offset -> #{offset => StartIndex}
        end,
        {Result, IsLast} = atm_store_test_utils:browse_content(krakow, AtmWorkflowExecutionAuth, Opts#{
            limit => Limit
        }, AtmStore),
        ?assertEqual(
            {Expected, StartIndex + Limit >= ItemsNum}, 
            {lists:map(fun({I, {ok, R}}) -> {I, {ok, ResultMapper(R)}} end, Result), IsLast}
        )
    end, lists:seq(1, 8)).
    


%% @private
map_iteration_result({ok, Result, Iterator}, ResultMapper) when is_list(Result) ->
    {ok, lists:map(ResultMapper, Result), Iterator};
map_iteration_result({ok, Result, Iterator}, ResultMapper) ->
    {ok, ResultMapper(Result), Iterator};
map_iteration_result(Result, _ResultMapper) -> Result.
