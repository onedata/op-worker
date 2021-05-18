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
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    create_store_with_invalid_args_test/1,
    update_store_test/1,
    iterate_one_by_one_test/1,
    iterate_in_chunks_test/1,
    iterator_cursor_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        create_store_with_invalid_args_test,
        update_store_test,
        iterate_one_by_one_test,
        iterate_in_chunks_test,
        iterator_cursor_test
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

-define(ATTEMPTS, 30).


%%%===================================================================
%%% API functions
%%%===================================================================

create_store_with_invalid_args_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    
    lists:foreach(fun(DataType) ->
        BadValue = automation_store_test_utils:example_bad_data(DataType),
        ?assertEqual(?ERROR_ATM_DATA_TYPE_UNVERIFIED(BadValue, DataType), 
            automation_store_test_utils:create_store(Node, ?ATM_SINGLE_VALUE_STORE_SCHEMA(DataType), BadValue))
    end, automation_store_test_utils:all_data_types()).


update_store_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    {ok, AtmStoreId0} = automation_store_test_utils:create_store(Node, ?ATM_SINGLE_VALUE_STORE_SCHEMA, undefined),
    ?assertEqual(?ERROR_NOT_SUPPORTED,
        automation_store_test_utils:update_store(Node, AtmStoreId0, append, #{}, <<"NaN">>)),
    
    lists:foreach(fun(DataType) ->
        {ok, AtmStoreId} = automation_store_test_utils:create_store(Node, ?ATM_SINGLE_VALUE_STORE_SCHEMA(DataType), undefined),
        BadValue = automation_store_test_utils:example_bad_data(DataType),
        ?assertEqual(?ERROR_ATM_DATA_TYPE_UNVERIFIED(BadValue, DataType),
            automation_store_test_utils:update_store(Node, AtmStoreId, set, #{}, BadValue)),
        ValidValue = automation_store_test_utils:example_data(DataType),
        ?assertEqual(ok, automation_store_test_utils:update_store(Node, AtmStoreId, set, #{}, ValidValue))
    end, automation_store_test_utils:all_data_types()).


iterate_one_by_one_test(_Config) ->
    AtmStoreIteratorStrategy = #atm_store_iterator_serial_strategy{},
    iterate_test_base(AtmStoreIteratorStrategy, 8, 8).


iterate_in_chunks_test(_Config) ->
    iterate_test_base(#atm_store_iterator_batch_strategy{size = 8}, 8, [8]).


iterate_test_base(AtmStoreIteratorStrategy, ValueToSet, ExpectedValue) ->
    Node = oct_background:get_random_provider_node(krakow),

    {ok, AtmStoreId} = automation_store_test_utils:create_store(Node, ?ATM_SINGLE_VALUE_STORE_SCHEMA, undefined),
    
    AtmListStoreDummySchemaId = <<"dummyId">>,
    
    AtmWorkflowExecutionEnv = #atm_workflow_execution_env{
        store_registry = #{AtmListStoreDummySchemaId => AtmStoreId}
    },
    AtmStoreIteratorSpec = #atm_store_iterator_spec{
        store_schema_id = AtmListStoreDummySchemaId,
        strategy = AtmStoreIteratorStrategy
    },
    AtmStoreIterator = automation_store_test_utils:acquire_store_iterator(
        Node, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec),
    
    ?assertEqual(stop, automation_store_test_utils:iterator_get_next(Node, AtmStoreIterator)),
    ?assertEqual(ok, automation_store_test_utils:update_store(Node, AtmStoreId, set, #{}, ValueToSet)),
    AtmStoreIterator1 = automation_store_test_utils:acquire_store_iterator(Node, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec),
    {ok, _, _, AtmIterator2} = ?assertMatch({ok, ExpectedValue, _, _}, automation_store_test_utils:iterator_get_next(Node, AtmStoreIterator1)),
    ?assertEqual(stop, automation_store_test_utils:iterator_get_next(Node, AtmIterator2)).


iterator_cursor_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),

    {ok, AtmStoreId} = automation_store_test_utils:create_store(Node, ?ATM_SINGLE_VALUE_STORE_SCHEMA, 8),
    
    AtmListStoreDummySchemaId = <<"dummyId">>,
    
    AtmWorkflowExecutionEnv = #atm_workflow_execution_env{
        store_registry = #{AtmListStoreDummySchemaId => AtmStoreId}
    },
    AtmStoreIteratorSpec = #atm_store_iterator_spec{
        store_schema_id = AtmListStoreDummySchemaId,
        strategy = #atm_store_iterator_serial_strategy{}
    },
    AtmSerialIterator0 = automation_store_test_utils:acquire_store_iterator(
        Node, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec),

    {ok, _, Cursor1, AtmSerialIterator1} = ?assertMatch({ok, 8, _, _}, automation_store_test_utils:iterator_get_next(Node, AtmSerialIterator0)),
    ?assertMatch(stop, automation_store_test_utils:iterator_get_next(Node, AtmSerialIterator1)),
    
    AtmSerialIterator2 = automation_store_test_utils:iterator_jump_to(Node, Cursor1, AtmSerialIterator1),
    ?assertMatch(stop, automation_store_test_utils:iterator_get_next(Node, AtmSerialIterator2)),


    % Assert <<>> cursor shifts iterator to the beginning
    AtmSerialIterator3 = automation_store_test_utils:iterator_jump_to(Node, <<>>, AtmSerialIterator2),
    ?assertMatch({ok, 8, _, _}, automation_store_test_utils:iterator_get_next(Node, AtmSerialIterator3)),

    % Invalid cursors should be rejected
    ?assertMatch(?EINVAL, automation_store_test_utils:iterator_jump_to(Node, <<"dummy">>, AtmSerialIterator2)),
    ?assertMatch(?EINVAL, automation_store_test_utils:iterator_jump_to(Node, <<"-2">>, AtmSerialIterator2)),
    ?assertMatch(?EINVAL, automation_store_test_utils:iterator_jump_to(Node, <<"3">>, AtmSerialIterator2)),
    ?assertMatch(?EINVAL, automation_store_test_utils:iterator_jump_to(Node, <<"20">>, AtmSerialIterator2)).


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


init_per_group(_Group, Config) ->
    lfm_proxy:init(Config, false).


end_per_group(_Group, Config) ->
    lfm_proxy:teardown(Config).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
