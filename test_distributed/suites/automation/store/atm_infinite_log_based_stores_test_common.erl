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
    iterate_one_by_one_test_base/1,
    iterate_in_chunks_test_base/1,
    reuse_iterator_test_base/1
]).

-define(ATTEMPTS, 30).


%%%===================================================================
%%% API functions
%%%===================================================================


create_store_with_invalid_args_test_base(AtmStoreSchema) ->
    AtmWorkflowExecutionCtx = atm_store_test_utils:create_workflow_execution_ctx(
        krakow, user1, space_krk
    ),

    ?assertEqual(?ERROR_ATM_STORE_MISSING_REQUIRED_INITIAL_VALUE, atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionCtx, undefined, AtmStoreSchema#atm_store_schema{requires_initial_value = true}
    )),
    ?assertEqual(?ERROR_ATM_BAD_DATA(<<"value">>, <<"not a batch">>), atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionCtx, 8, AtmStoreSchema
    )),

    lists:foreach(fun(DataType) ->
        BadValue = atm_store_test_utils:example_bad_data(DataType),
        ValidValue = atm_store_test_utils:example_data(DataType),
        ?assertEqual(?ERROR_ATM_DATA_TYPE_UNVERIFIED(BadValue, DataType), atm_store_test_utils:create_store(
            krakow, AtmWorkflowExecutionCtx, [ValidValue, BadValue, ValidValue],
            AtmStoreSchema#atm_store_schema{data_spec = #atm_data_spec{type = DataType}}
        ))
    end, atm_store_test_utils:all_data_types()).


apply_operation_test_base(AtmStoreSchema) ->
    AtmWorkflowExecutionCtx = atm_store_test_utils:create_workflow_execution_ctx(
        krakow, user1, space_krk
    ),

    {ok, AtmListStoreId0} = atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionCtx, undefined, AtmStoreSchema
    ),
    
    ?assertEqual(?ERROR_NOT_SUPPORTED, atm_store_test_utils:apply_operation(
        krakow, AtmWorkflowExecutionCtx, set, <<"NaN">>, #{}, AtmListStoreId0
    )),
    
    lists:foreach(fun(DataType) ->
        {ok, AtmListStoreId} = atm_store_test_utils:create_store(
            krakow, AtmWorkflowExecutionCtx, undefined,
            AtmStoreSchema#atm_store_schema{data_spec = #atm_data_spec{type = DataType}}
        ),
        BadValue = atm_store_test_utils:example_bad_data(DataType),
        ValidValue = atm_store_test_utils:example_data(DataType),
        
        ?assertEqual(?ERROR_ATM_DATA_TYPE_UNVERIFIED(BadValue, DataType), atm_store_test_utils:apply_operation(
            krakow, AtmWorkflowExecutionCtx, append, BadValue, #{}, AtmListStoreId
        )),
        ?assertEqual(ok, atm_store_test_utils:apply_operation(
            krakow, AtmWorkflowExecutionCtx, append, ValidValue, #{}, AtmListStoreId
        )),
        ?assertEqual(?ERROR_ATM_DATA_TYPE_UNVERIFIED(BadValue, DataType), atm_store_test_utils:apply_operation(
            krakow, AtmWorkflowExecutionCtx, append, [ValidValue, BadValue, ValidValue], #{<<"isBatch">> => true}, AtmListStoreId
        )),
        ?assertEqual(ok, atm_store_test_utils:apply_operation(
            krakow, AtmWorkflowExecutionCtx, append, lists:duplicate(8, ValidValue), #{<<"isBatch">> => true}, AtmListStoreId
        ))
    end, atm_store_test_utils:all_data_types()).


iterate_one_by_one_test_base(AtmStoreSchema) ->
    Length = 8,
    AtmStoreIteratorStrategy = #atm_store_iterator_serial_strategy{},
    iterate_test_base_internal(AtmStoreSchema, AtmStoreIteratorStrategy, Length, lists:seq(1, Length)).


iterate_in_chunks_test_base(AtmStoreSchema) ->
    Length = 8,
    ChunkSize = rand:uniform(Length),
    AtmStoreIteratorStrategy = #atm_store_iterator_batch_strategy{size = ChunkSize},
    ExpectedResultsList = atm_store_test_utils:split_into_chunks(ChunkSize, [], lists:seq(1, Length)),
    iterate_test_base_internal(AtmStoreSchema, AtmStoreIteratorStrategy, Length, ExpectedResultsList).


% @private
-spec iterate_test_base_internal(atm_store_schema:record(), atm_store_iterator_config:record(), non_neg_integer(), [term()]) ->
    ok.
iterate_test_base_internal(AtmStoreSchema, AtmStoreIteratorStrategy, Length, ExpectedResultsList) ->
    Items = lists:seq(1, Length),

    AtmWorkflowExecutionCtx = atm_store_test_utils:create_workflow_execution_ctx(
        krakow, user1, space_krk
    ),

    {ok, AtmListStoreId} = atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionCtx, Items, AtmStoreSchema
    ),

    AtmListStoreDummySchemaId = <<"dummyId">>,
    
    AtmWorkflowExecutionEnv = atm_workflow_execution_env:build(
        atm_workflow_execution_ctx:get_space_id(AtmWorkflowExecutionCtx),
        atm_workflow_execution_ctx:get_workflow_execution_id(AtmWorkflowExecutionCtx),
        #{AtmListStoreDummySchemaId => AtmListStoreId}
    ),
    AtmStoreIteratorSpec = #atm_store_iterator_spec{
        store_schema_id = AtmListStoreDummySchemaId,
        strategy = AtmStoreIteratorStrategy
    },
    AtmStoreIterator0 = atm_store_test_utils:acquire_store_iterator(
        krakow, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec
    ),

    lists:foldl(fun(Expected, Iterator) ->
        {ok, _, NewIterator} = ?assertMatch({ok, Expected, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, Iterator)),
        NewIterator
    end, AtmStoreIterator0, ExpectedResultsList),
    ok.


reuse_iterator_test_base(AtmStoreSchema) ->
    AtmWorkflowExecutionCtx = atm_store_test_utils:create_workflow_execution_ctx(
        krakow, user1, space_krk
    ),

    Items = lists:seq(1, 5),
    {ok, AtmListStoreId} = atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionCtx, Items, AtmStoreSchema
    ),
    
    AtmListStoreDummySchemaId = <<"dummyId">>,

    AtmWorkflowExecutionEnv = atm_workflow_execution_env:build(
        atm_workflow_execution_ctx:get_space_id(AtmWorkflowExecutionCtx),
        atm_workflow_execution_ctx:get_workflow_execution_id(AtmWorkflowExecutionCtx),
        #{AtmListStoreDummySchemaId => AtmListStoreId}
    ),
    AtmStoreIteratorSpec = #atm_store_iterator_spec{
        store_schema_id = AtmListStoreDummySchemaId,
        strategy = #atm_store_iterator_serial_strategy{}
    },
    AtmSerialIterator0 = atm_store_test_utils:acquire_store_iterator(krakow, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec),

    {ok, _, AtmSerialIterator1} = ?assertMatch({ok, 1, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator0)),
    {ok, _, AtmSerialIterator2} = ?assertMatch({ok, 2, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator1)),
    {ok, _, AtmSerialIterator3} = ?assertMatch({ok, 3, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator2)),
    {ok, _, AtmSerialIterator4} = ?assertMatch({ok, 4, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator3)),
    {ok, _, AtmSerialIterator5} = ?assertMatch({ok, 5, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator4)),
    ?assertMatch(stop, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator5)),
    
    ?assertMatch({ok, 1, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator0)),
    
    {ok, _, AtmSerialIterator7} = ?assertMatch({ok, 4, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator3)),
    ?assertMatch({ok, 5, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator7)),

    {ok, _, AtmSerialIterator9} = ?assertMatch({ok, 2, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator1)),
    ?assertMatch({ok, 3, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator9)).
