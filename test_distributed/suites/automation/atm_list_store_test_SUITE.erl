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
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    update_store_test/1,
    iterate_one_by_one_test/1,
    iterate_in_chunks_test/1,
    iterator_cursor_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        update_store_test,
        iterate_one_by_one_test,
        iterate_in_chunks_test,
        iterator_cursor_test
    ]}
].

all() -> [
    {group, all_tests}
].


-define(ATM_LIST_STORE_SCHEMA, #atm_store_schema{
    id = <<"dummyId">>,
    name = <<"list_store">>,
    description = <<"description">>,
    requires_initial_value = true,
    type = list,
    data_spec = #atm_data_spec{type = atm_integer_type}
}).

-define(ATTEMPTS, 30).


%%%===================================================================
%%% API functions
%%%===================================================================


update_store_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    {ok, AtmListStoreId} = automation_test_utils:create_store(Node, ?ATM_LIST_STORE_SCHEMA, #{}),
    
    ?assertEqual(?ERROR_NOT_SUPPORTED,
        automation_test_utils:update_store(Node, AtmListStoreId, set, #{}, <<"NaN">>)),
    ?assertEqual(?ERROR_ATM_DATA_TYPE_UNVERIFIED(<<"NaN">>, atm_integer_type),
        automation_test_utils:update_store(Node, AtmListStoreId, append, #{}, <<"NaN">>)),
    ?assertEqual(ok,
        automation_test_utils:update_store(Node, AtmListStoreId, append, #{}, 8)),
    ?assertEqual(?ERROR_ATM_DATA_TYPE_UNVERIFIED(<<"NaN">>, atm_integer_type),
        automation_test_utils:update_store(Node, AtmListStoreId, append, #{<<"isBatch">> => true}, [1,2,<<"NaN">>,4])),
    ?assertEqual(ok,
        automation_test_utils:update_store(Node, AtmListStoreId, append, #{<<"isBatch">> => true}, [1,2,3,4])).


iterate_one_by_one_test(_Config) ->
    Length = 8,
    AtmStoreIteratorSpec = #atm_store_iterator_spec{
        strategy = #atm_store_iterator_serial_strategy{}
    },
    iterate_test_base(AtmStoreIteratorSpec, Length, lists:seq(1, Length)).


iterate_in_chunks_test(_Config) ->
    ChunkSize = 5,
    Length = 8,
    AtmStoreIteratorSpec = #atm_store_iterator_spec{
        strategy = #atm_store_iterator_batch_strategy{size = ChunkSize}
    },
    ExpectedResultsList = automation_test_utils:split_into_chunks(5, [], lists:seq(1, Length)),
    iterate_test_base(AtmStoreIteratorSpec, Length, ExpectedResultsList).


-spec iterate_test_base(atm_store_iterator_spec(), non_neg_integer(), [term()]) -> ok.
iterate_test_base(AtmStoreIteratorSpec, Length, ExpectedResultsList) ->
    Node = oct_background:get_random_provider_node(krakow),
    {ok, AtmListStoreId} = automation_test_utils:create_store(Node, ?ATM_LIST_STORE_SCHEMA, #{}),
    Items = lists:seq(1, Length),
    ok = automation_test_utils:update_store(Node, AtmListStoreId, append, #{<<"isBatch">> => true}, Items),
    
    AtmListStoreDummySchemaId = <<"dummyId">>,
    
    AtmWorkflowExecutionEnv = #atm_workflow_execution_env{
        store_registry = #{AtmListStoreDummySchemaId => AtmListStoreId}
    },
    AtmSerialIterator0 = automation_test_utils:acquire_store_iterator(
        Node, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec#atm_store_iterator_spec{store_schema_id = AtmListStoreDummySchemaId}),
    
    lists:foldl(fun(Expected, Iterator) ->
        {ok, _, _, NewIterator} = ?assertMatch({ok, Expected, _,_}, automation_test_utils:iterator_get_next(Node, Iterator)),
        NewIterator
    end, AtmSerialIterator0, ExpectedResultsList),
    ok.


iterator_cursor_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),

    {ok, AtmListStoreId} = automation_test_utils:create_store(Node, ?ATM_LIST_STORE_SCHEMA, #{}),
    
    AtmListStoreDummySchemaId = <<"dummyId">>,
    
    AtmWorkflowExecutionEnv = #atm_workflow_execution_env{
        store_registry = #{AtmListStoreDummySchemaId => AtmListStoreId}
    },

    AtmStoreIteratorSpec = #atm_store_iterator_spec{
        store_schema_id = AtmListStoreDummySchemaId,
        strategy = #atm_store_iterator_serial_strategy{}
    },
    Items = lists:seq(1,5),
    ok = automation_test_utils:update_store(Node, AtmListStoreId, append, #{<<"isBatch">> => true}, Items),
    AtmSerialIterator0 = automation_test_utils:acquire_store_iterator(Node, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec),

    {ok, _, Cursor1, AtmSerialIterator1} = ?assertMatch({ok, 1, _, _}, automation_test_utils:iterator_get_next(Node, AtmSerialIterator0)),
    {ok, _, _Cursor2, AtmSerialIterator2} = ?assertMatch({ok, 2, _, _}, automation_test_utils:iterator_get_next(Node, AtmSerialIterator1)),
    {ok, _, Cursor3, AtmSerialIterator3} = ?assertMatch({ok, 3, _, _}, automation_test_utils:iterator_get_next(Node, AtmSerialIterator2)),
    {ok, _, _Cursor4, AtmSerialIterator4} = ?assertMatch({ok, 4, _, _}, automation_test_utils:iterator_get_next(Node, AtmSerialIterator3)),
    {ok, _, _Cursor5, AtmSerialIterator5} = ?assertMatch({ok, 5, _, _}, automation_test_utils:iterator_get_next(Node, AtmSerialIterator4)),
    ?assertMatch(stop, automation_test_utils:iterator_get_next(Node, AtmSerialIterator5)),

    % Assert cursor shifts iterator to the beginning
    AtmSerialIterator6 = automation_test_utils:iterator_jump_to(Node, Cursor3, AtmSerialIterator5),
    {ok, _, _Cursor7, AtmSerialIterator7} = ?assertMatch({ok, 4, _, _}, automation_test_utils:iterator_get_next(Node, AtmSerialIterator6)),
    ?assertMatch({ok, 5, _, _}, automation_test_utils:iterator_get_next(Node, AtmSerialIterator7)),

    AtmSerialIterator8 = automation_test_utils:iterator_jump_to(Node, Cursor1, AtmSerialIterator7),
    {ok, _, _Cursor9, AtmSerialIterator9} = ?assertMatch({ok, 2, _, _}, automation_test_utils:iterator_get_next(Node, AtmSerialIterator8)),
    ?assertMatch({ok, 3, _, _}, automation_test_utils:iterator_get_next(Node, AtmSerialIterator9)),

    % Assert <<>> cursor shifts iterator to the beginning
    AtmSerialIterator10 = automation_test_utils:iterator_jump_to(Node, <<>>, AtmSerialIterator9),
    ?assertMatch({ok, 1, _, _}, automation_test_utils:iterator_get_next(Node, AtmSerialIterator10)),

    % Invalid cursors should be rejected
    ?assertMatch(?EINVAL, automation_test_utils:iterator_jump_to(Node, <<"dummy">>, AtmSerialIterator9)).


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
