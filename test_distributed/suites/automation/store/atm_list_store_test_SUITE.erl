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
-module(atm_list_store_test_SUITE).
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
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    create_store_with_invalid_args_test/1,
    apply_operation_test/1,
    iterate_one_by_one_test/1,
    iterate_in_chunks_test/1,
    reuse_iterator_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        create_store_with_invalid_args_test,
        apply_operation_test,
        iterate_one_by_one_test,
        iterate_in_chunks_test,
        reuse_iterator_test
    ]}
].

all() -> [
    {group, all_tests}
].


-define(ATM_LIST_STORE_SCHEMA, ?ATM_LIST_STORE_SCHEMA(atm_integer_type)).
-define(ATM_LIST_STORE_SCHEMA(DataType), #atm_store_schema{
    id = <<"dummyId">>,
    name = <<"list_store">>,
    description = <<"description">>,
    requires_initial_value = false,
    type = list,
    data_spec = #atm_data_spec{type = DataType}
}).

-define(ATTEMPTS, 30).


%%%===================================================================
%%% API functions
%%%===================================================================


create_store_with_invalid_args_test(_Config) ->
    AtmWorkflowExecutionCtx = atm_store_test_utils:create_workflow_execution_ctx(
        krakow, user1, space_krk
    ),

    ?assertEqual(?ERROR_ATM_STORE_MISSING_REQUIRED_INITIAL_VALUE, atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionCtx, undefined, (?ATM_LIST_STORE_SCHEMA)#atm_store_schema{requires_initial_value = true}
    )),
    ?assertEqual(?ERROR_ATM_BAD_DATA(<<"value">>, <<"not a batch">>), atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionCtx, 8, ?ATM_LIST_STORE_SCHEMA
    )),

    lists:foreach(fun(DataType) ->
        BadValue = atm_store_test_utils:example_bad_data(DataType),
        ValidValue = atm_store_test_utils:example_data(DataType),
        ?assertEqual(?ERROR_ATM_DATA_TYPE_UNVERIFIED(BadValue, DataType), atm_store_test_utils:create_store(
            krakow, AtmWorkflowExecutionCtx, [ValidValue, BadValue, ValidValue], ?ATM_LIST_STORE_SCHEMA(DataType)
        ))
    end, atm_store_test_utils:all_data_types()).


apply_operation_test(_Config) ->
    AtmWorkflowExecutionCtx = atm_store_test_utils:create_workflow_execution_ctx(
        krakow, user1, space_krk
    ),

    {ok, AtmListStoreId0} = atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionCtx, undefined, ?ATM_LIST_STORE_SCHEMA
    ),
    
    ?assertEqual(?ERROR_NOT_SUPPORTED, atm_store_test_utils:apply_operation(
        krakow, AtmWorkflowExecutionCtx, set, <<"NaN">>, #{}, AtmListStoreId0
    )),
    
    lists:foreach(fun(DataType) ->
        {ok, AtmListStoreId} = atm_store_test_utils:create_store(
            krakow, AtmWorkflowExecutionCtx, undefined, ?ATM_LIST_STORE_SCHEMA(DataType)
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


iterate_one_by_one_test(_Config) ->
    Length = 8,
    AtmStoreIteratorStrategy = #atm_store_iterator_serial_strategy{},
    iterate_test_base(AtmStoreIteratorStrategy, Length, lists:seq(1, Length)).


iterate_in_chunks_test(_Config) ->
    Length = 8,
    ChunkSize = rand:uniform(Length),
    AtmStoreIteratorStrategy = #atm_store_iterator_batch_strategy{size = ChunkSize},
    ExpectedResultsList = atm_store_test_utils:split_into_chunks(ChunkSize, [], lists:seq(1, Length)),
    iterate_test_base(AtmStoreIteratorStrategy, Length, ExpectedResultsList).


-spec iterate_test_base(atm_store_iterator_config:record(), non_neg_integer(), [term()]) -> ok.
iterate_test_base(AtmStoreIteratorStrategy, Length, ExpectedResultsList) ->
    Items = lists:seq(1, Length),

    AtmWorkflowExecutionCtx = atm_store_test_utils:create_workflow_execution_ctx(
        krakow, user1, space_krk
    ),

    {ok, AtmListStoreId} = atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionCtx, Items, ?ATM_LIST_STORE_SCHEMA
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


reuse_iterator_test(_Config) ->
    AtmWorkflowExecutionCtx = atm_store_test_utils:create_workflow_execution_ctx(
        krakow, user1, space_krk
    ),

    Items = lists:seq(1, 5),
    {ok, AtmListStoreId} = atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionCtx, Items, ?ATM_LIST_STORE_SCHEMA
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


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
