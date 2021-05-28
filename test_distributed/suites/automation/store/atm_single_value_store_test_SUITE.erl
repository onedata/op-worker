%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of automation single value store.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_single_value_store_test_SUITE).
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


-define(ATM_SINGLE_VALUE_STORE_SCHEMA, ?ATM_SINGLE_VALUE_STORE_SCHEMA(atm_integer_type)).
-define(ATM_SINGLE_VALUE_STORE_SCHEMA(__DataType), #atm_store_schema{
    id = <<"dummyId">>,
    name = <<"single_value_store">>,
    description = <<"description">>,
    requires_initial_value = false,
    type = single_value,
    data_spec = #atm_data_spec{type = __DataType}
}).
-define(RAND_ATM_WORKFLOW_EXECUTION_CTX(), #atm_workflow_execution_ctx{
    space_id = oct_background:get_space_id(space_krk),
    workflow_execution_id = str_utils:rand_hex(32),
    session_id = oct_background:get_user_session_id(user1, krakow)
}).

-define(ATTEMPTS, 30).


%%%===================================================================
%%% API functions
%%%===================================================================

create_store_with_invalid_args_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    AtmWorkflowExecutionCxt = ?RAND_ATM_WORKFLOW_EXECUTION_CTX(),

    lists:foreach(fun(DataType) ->
        BadValue = atm_store_test_utils:example_bad_data(DataType),
        ?assertEqual(
            ?ERROR_ATM_DATA_TYPE_UNVERIFIED(BadValue, DataType),
            atm_store_test_utils:create_store(
                Node, AtmWorkflowExecutionCxt, BadValue, ?ATM_SINGLE_VALUE_STORE_SCHEMA(DataType)
            )
        )
    end, atm_store_test_utils:all_data_types()).


apply_operation_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    AtmWorkflowExecutionCxt = ?RAND_ATM_WORKFLOW_EXECUTION_CTX(),
    {ok, AtmStoreId0} = atm_store_test_utils:create_store(
        Node, AtmWorkflowExecutionCxt, undefined, ?ATM_SINGLE_VALUE_STORE_SCHEMA
    ),
    ?assertEqual(?ERROR_NOT_SUPPORTED, atm_store_test_utils:apply_operation(
        Node, AtmWorkflowExecutionCxt, append, <<"NaN">>, #{}, AtmStoreId0
    )),
    
    lists:foreach(fun(DataType) ->
        {ok, AtmStoreId} = atm_store_test_utils:create_store(
            Node, AtmWorkflowExecutionCxt, undefined, ?ATM_SINGLE_VALUE_STORE_SCHEMA(DataType)
        ),
        BadValue = atm_store_test_utils:example_bad_data(DataType),
        ?assertEqual(
            ?ERROR_ATM_DATA_TYPE_UNVERIFIED(BadValue, DataType),
            atm_store_test_utils:apply_operation(Node, AtmWorkflowExecutionCxt, set, BadValue, #{}, AtmStoreId)
        ),
        ValidValue = atm_store_test_utils:example_data(DataType),
        ?assertEqual(ok, atm_store_test_utils:apply_operation(
            Node, AtmWorkflowExecutionCxt, set, ValidValue, #{}, AtmStoreId
        ))
    end, atm_store_test_utils:all_data_types()).


iterate_one_by_one_test(_Config) ->
    AtmStoreIteratorStrategy = #atm_store_iterator_serial_strategy{},
    iterate_test_base(AtmStoreIteratorStrategy, 8, 8).


iterate_in_chunks_test(_Config) ->
    iterate_test_base(#atm_store_iterator_batch_strategy{size = 8}, 8, [8]).


iterate_test_base(AtmStoreIteratorStrategy, ValueToSet, ExpectedValue) ->
    Node = oct_background:get_random_provider_node(krakow),

    #atm_workflow_execution_ctx{
        space_id = SpaceId,
        workflow_execution_id = AtmWorkflowExecutionId
    } = AtmWorkflowExecutionCxt = ?RAND_ATM_WORKFLOW_EXECUTION_CTX(),

    {ok, AtmStoreId} = atm_store_test_utils:create_store(
        Node, AtmWorkflowExecutionCxt, undefined, ?ATM_SINGLE_VALUE_STORE_SCHEMA
    ),
    
    AtmListStoreDummySchemaId = <<"dummyId">>,
    
    AtmWorkflowExecutionEnv = #atm_workflow_execution_env{
        space_id = SpaceId,
        workflow_execution_id = AtmWorkflowExecutionId,
        store_registry = #{AtmListStoreDummySchemaId => AtmStoreId}
    },
    AtmStoreIteratorSpec = #atm_store_iterator_spec{
        store_schema_id = AtmListStoreDummySchemaId,
        strategy = AtmStoreIteratorStrategy
    },
    AtmStoreIterator = atm_store_test_utils:acquire_store_iterator(
        Node, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec),
    
    ?assertEqual(stop, atm_store_test_utils:iterator_get_next(Node, AtmStoreIterator)),
    ?assertEqual(ok, atm_store_test_utils:apply_operation(
        Node, AtmWorkflowExecutionCxt, set, ValueToSet, #{}, AtmStoreId)
    ),
    AtmStoreIterator1 = atm_store_test_utils:acquire_store_iterator(Node, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec),
    {ok, _, AtmIterator2} = ?assertMatch({ok, ExpectedValue, _}, atm_store_test_utils:iterator_get_next(Node, AtmStoreIterator1)),
    ?assertEqual(stop, atm_store_test_utils:iterator_get_next(Node, AtmIterator2)).


reuse_iterator_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),

    #atm_workflow_execution_ctx{
        space_id = SpaceId,
        workflow_execution_id = AtmWorkflowExecutionId
    } = AtmWorkflowExecutionCxt = ?RAND_ATM_WORKFLOW_EXECUTION_CTX(),

    {ok, AtmStoreId} = atm_store_test_utils:create_store(
        Node, AtmWorkflowExecutionCxt, 8, ?ATM_SINGLE_VALUE_STORE_SCHEMA
    ),

    AtmListStoreDummySchemaId = <<"dummyId">>,
    
    AtmWorkflowExecutionEnv = #atm_workflow_execution_env{
        space_id = SpaceId,
        workflow_execution_id = AtmWorkflowExecutionId,
        store_registry = #{AtmListStoreDummySchemaId => AtmStoreId}
    },
    AtmStoreIteratorSpec = #atm_store_iterator_spec{
        store_schema_id = AtmListStoreDummySchemaId,
        strategy = #atm_store_iterator_serial_strategy{}
    },
    AtmSerialIterator0 = atm_store_test_utils:acquire_store_iterator(
        Node, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec),

    {ok, _, AtmSerialIterator1} = ?assertMatch({ok, 8, _}, atm_store_test_utils:iterator_get_next(Node, AtmSerialIterator0)),
    ?assertMatch(stop, atm_store_test_utils:iterator_get_next(Node, AtmSerialIterator1)),
    ?assertMatch({ok, 8, _}, atm_store_test_utils:iterator_get_next(Node, AtmSerialIterator0)),
    
    ?assertMatch(stop, atm_store_test_utils:iterator_get_next(Node, AtmSerialIterator1)).


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
