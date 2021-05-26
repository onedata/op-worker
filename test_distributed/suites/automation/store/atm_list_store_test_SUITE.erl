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
    iterator_cursor_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        create_store_with_invalid_args_test,
        apply_operation_test,
        iterate_one_by_one_test,
        iterate_in_chunks_test,
        iterator_cursor_test
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
    Node = oct_background:get_random_provider_node(krakow),
    ?assertEqual(?ERROR_ATM_STORE_MISSING_REQUIRED_INITIAL_VALUE,
        atm_store_test_utils:create_store(Node, undefined, (?ATM_LIST_STORE_SCHEMA)#atm_store_schema{requires_initial_value = true})),
    ?assertEqual(?ERROR_ATM_BAD_DATA(<<"initialValue">>, <<"not a list">>),
        atm_store_test_utils:create_store(Node, 8, ?ATM_LIST_STORE_SCHEMA)),
    
    lists:foreach(fun(DataType) ->
        BadValue = atm_store_test_utils:example_bad_data(DataType),
        ValidValue = atm_store_test_utils:example_data(DataType),
        ?assertEqual(?ERROR_ATM_DATA_TYPE_UNVERIFIED(BadValue, DataType),
            atm_store_test_utils:create_store(Node, [ValidValue, BadValue, ValidValue], ?ATM_LIST_STORE_SCHEMA(DataType)))
    end, atm_store_test_utils:all_data_types()).


apply_operation_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    {ok, AtmListStoreId0} = atm_store_test_utils:create_store(Node, undefined, ?ATM_LIST_STORE_SCHEMA),
    
    ?assertEqual(?ERROR_NOT_SUPPORTED,
        atm_store_test_utils:apply_operation(Node, AtmListStoreId0, set, #{}, <<"NaN">>)),
    
    lists:foreach(fun(DataType) ->
        {ok, AtmListStoreId} = atm_store_test_utils:create_store(Node, undefined, ?ATM_LIST_STORE_SCHEMA(DataType)),
        BadValue = atm_store_test_utils:example_bad_data(DataType),
        ValidValue = atm_store_test_utils:example_data(DataType),
        
        ?assertEqual(?ERROR_ATM_DATA_TYPE_UNVERIFIED(BadValue, DataType),
            atm_store_test_utils:apply_operation(Node, AtmListStoreId, append, #{}, BadValue)),
        ?assertEqual(ok, atm_store_test_utils:apply_operation(Node, AtmListStoreId, append, #{}, ValidValue)),
        ?assertEqual(?ERROR_ATM_DATA_TYPE_UNVERIFIED(BadValue, DataType),
            atm_store_test_utils:apply_operation(Node, AtmListStoreId, append, #{<<"isBatch">> => true}, [ValidValue, BadValue, ValidValue])),
        ?assertEqual(ok, atm_store_test_utils:apply_operation(Node, AtmListStoreId, append, #{<<"isBatch">> => true}, lists:duplicate(8, ValidValue)))
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
    Node = oct_background:get_random_provider_node(krakow),
    Items = lists:seq(1, Length),
    {ok, AtmListStoreId} = atm_store_test_utils:create_store(Node, Items, ?ATM_LIST_STORE_SCHEMA),
    
    AtmListStoreDummySchemaId = <<"dummyId">>,
    
    AtmWorkflowExecutionEnv = #atm_workflow_execution_env{
        store_registry = #{AtmListStoreDummySchemaId => AtmListStoreId}
    },
    AtmStoreIteratorSpec = #atm_store_iterator_spec{
        store_schema_id = AtmListStoreDummySchemaId,
        strategy = AtmStoreIteratorStrategy
    },
    AtmStoreIterator0 = atm_store_test_utils:acquire_store_iterator(
        Node, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec),
    
    lists:foldl(fun(Expected, Iterator) ->
        {ok, _, _, NewIterator} = ?assertMatch({ok, Expected, _,_}, atm_store_test_utils:iterator_get_next(Node, Iterator)),
        NewIterator
    end, AtmStoreIterator0, ExpectedResultsList),
    ok.


iterator_cursor_test(_Config) ->
    
    Node = oct_background:get_random_provider_node(krakow),
    
    Items = lists:seq(1, 5),
    {ok, AtmListStoreId} = atm_store_test_utils:create_store(Node, Items, ?ATM_LIST_STORE_SCHEMA),
    
    AtmListStoreDummySchemaId = <<"dummyId">>,
    
    AtmWorkflowExecutionEnv = #atm_workflow_execution_env{
        store_registry = #{AtmListStoreDummySchemaId => AtmListStoreId}
    },
    
    AtmStoreIteratorSpec = #atm_store_iterator_spec{
        store_schema_id = AtmListStoreDummySchemaId,
        strategy = #atm_store_iterator_serial_strategy{}
    },
    AtmSerialIterator0 = atm_store_test_utils:acquire_store_iterator(Node, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec),

    {ok, _, Cursor1, AtmSerialIterator1} = ?assertMatch({ok, 1, _, _}, atm_store_test_utils:iterator_get_next(Node, AtmSerialIterator0)),
    {ok, _, _Cursor2, AtmSerialIterator2} = ?assertMatch({ok, 2, _, _}, atm_store_test_utils:iterator_get_next(Node, AtmSerialIterator1)),
    {ok, _, Cursor3, AtmSerialIterator3} = ?assertMatch({ok, 3, _, _}, atm_store_test_utils:iterator_get_next(Node, AtmSerialIterator2)),
    {ok, _, _Cursor4, AtmSerialIterator4} = ?assertMatch({ok, 4, _, _}, atm_store_test_utils:iterator_get_next(Node, AtmSerialIterator3)),
    {ok, _, _Cursor5, AtmSerialIterator5} = ?assertMatch({ok, 5, _, _}, atm_store_test_utils:iterator_get_next(Node, AtmSerialIterator4)),
    ?assertMatch(stop, atm_store_test_utils:iterator_get_next(Node, AtmSerialIterator5)),

    AtmSerialIterator6 = atm_store_test_utils:iterator_jump_to(Node, Cursor3, AtmSerialIterator5),
    {ok, _, _Cursor7, AtmSerialIterator7} = ?assertMatch({ok, 4, _, _}, atm_store_test_utils:iterator_get_next(Node, AtmSerialIterator6)),
    ?assertMatch({ok, 5, _, _}, atm_store_test_utils:iterator_get_next(Node, AtmSerialIterator7)),

    AtmSerialIterator8 = atm_store_test_utils:iterator_jump_to(Node, Cursor1, AtmSerialIterator7),
    {ok, _, _Cursor9, AtmSerialIterator9} = ?assertMatch({ok, 2, _, _}, atm_store_test_utils:iterator_get_next(Node, AtmSerialIterator8)),
    ?assertMatch({ok, 3, _, _}, atm_store_test_utils:iterator_get_next(Node, AtmSerialIterator9)),

    % Assert <<>> cursor shifts iterator to the beginning
    AtmSerialIterator10 = atm_store_test_utils:iterator_jump_to(Node, <<>>, AtmSerialIterator9),
    ?assertMatch({ok, 1, _, _}, atm_store_test_utils:iterator_get_next(Node, AtmSerialIterator10)),

    % Invalid cursors should be rejected
    ?assertMatch(?EINVAL, atm_store_test_utils:iterator_jump_to(Node, <<"dummy">>, AtmSerialIterator9)).


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
