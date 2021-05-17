%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of automation range store.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_range_store_test_SUITE).
-author("Bartosz Walkowicz").

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

    iterate_one_by_one_with_end_100_test/1,
    iterate_one_by_one_with_start_25_end_100_test/1,
    iterate_one_by_one_with_start_25_end_100_step_4_test/1,
    iterate_one_by_one_with_start_50_end_minus_50_step_minus_2_test/1,
    iterate_one_by_one_with_start_10_end_10_step_1_test/1,

    iterate_in_chunks_5_with_start_10_end_50_step_2_test/1,
    iterate_in_chunks_10_with_start_1_end_2_step_10_test/1,
    iterate_in_chunks_10_with_start_minus_50_end_50_step_4_test/1,
    iterate_in_chunks_7_with_start_50_end_minus_50_step_minus_3_test/1,
    iterate_in_chunks_3_with_start_10_end_10_step_2_test/1,

    iterator_cursor_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        create_store_with_invalid_args_test,
        update_store_test,

        iterate_one_by_one_with_end_100_test,
        iterate_one_by_one_with_start_25_end_100_test,
        iterate_one_by_one_with_start_25_end_100_step_4_test,
        iterate_one_by_one_with_start_50_end_minus_50_step_minus_2_test,
        iterate_one_by_one_with_start_10_end_10_step_1_test,

        iterate_in_chunks_5_with_start_10_end_50_step_2_test,
        iterate_in_chunks_10_with_start_1_end_2_step_10_test,
        iterate_in_chunks_10_with_start_minus_50_end_50_step_4_test,
        iterate_in_chunks_7_with_start_50_end_minus_50_step_minus_3_test,
        iterate_in_chunks_3_with_start_10_end_10_step_2_test,

        iterator_cursor_test
    ]}
].

all() -> [
    {group, all_tests}
].


-define(ATM_RANGE_STORE_SCHEMA, #atm_store_schema{
    id = <<"dummyId">>,
    name = <<"range_store">>,
    description = <<"description">>,
    requires_initial_value = true,
    type = range,
    data_spec = #atm_data_spec{type = atm_integer_type}
}).

-define(ATTEMPTS, 30).


%%%===================================================================
%%% API functions
%%%===================================================================


create_store_with_invalid_args_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),

    lists:foreach(fun({InvalidInitialValue, ExpError}) ->
        ?assertEqual(ExpError, automation_test_utils:create_store(Node, InvalidInitialValue, ?ATM_RANGE_STORE_SCHEMA))
    end, [
        {undefined, ?ERROR_ATM_STORE_MISSING_REQUIRED_INITIAL_VALUE},
        {#{<<"end">> => <<"NaN">>},
            ?ERROR_ATM_BAD_DATA(<<"end">>, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(<<"NaN">>, atm_integer_type))
        },
        {#{<<"start">> => <<"NaN">>, <<"end">> => 10},
            ?ERROR_ATM_BAD_DATA(<<"start">>, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(<<"NaN">>, atm_integer_type))
        },
        {#{<<"start">> => 5, <<"end">> => 10, <<"step">> => <<"NaN">>},
            ?ERROR_ATM_BAD_DATA(<<"step">>, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(<<"NaN">>, atm_integer_type))
        },
        {#{<<"start">> => 5, <<"end">> => 10, <<"step">> => 0}, ?ERROR_ATM_BAD_DATA},
        {#{<<"start">> => 15, <<"end">> => 10, <<"step">> => 1}, ?ERROR_ATM_BAD_DATA},
        {#{<<"start">> => -15, <<"end">> => -10, <<"step">> => -1}, ?ERROR_ATM_BAD_DATA},
        {#{<<"start">> => 10, <<"end">> => 15, <<"step">> => -1}, ?ERROR_ATM_BAD_DATA}
    ]).


update_store_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    {ok, AtmRangeStoreId} = automation_test_utils:create_store(Node, ?ATM_RANGE_STORE_SCHEMA, #{<<"end">> => 8}),
    
    ?assertEqual(?ERROR_NOT_SUPPORTED,
        automation_test_utils:update_store(Node, AtmRangeStoreId, append, #{}, <<"NaN">>)),
    ?assertEqual(?ERROR_NOT_SUPPORTED,
        automation_test_utils:update_store(Node, AtmRangeStoreId, set, #{}, <<"NaN">>)).


iterate_one_by_one_with_end_100_test(_Config) ->
    iterate_one_by_one_test_base(#{<<"end">> => 100}).


iterate_one_by_one_with_start_25_end_100_test(_Config) ->
    iterate_one_by_one_test_base(#{<<"start">> => 25, <<"end">> => 100}).


iterate_one_by_one_with_start_25_end_100_step_4_test(_Config) ->
    iterate_one_by_one_test_base(#{<<"start">> => 25, <<"end">> => 100, <<"step">> => 4}).


iterate_one_by_one_with_start_50_end_minus_50_step_minus_2_test(_Config) ->
    iterate_one_by_one_test_base(#{<<"start">> => 50, <<"end">> => -50, <<"step">> => -2}).


iterate_one_by_one_with_start_10_end_10_step_1_test(_Config) ->
    iterate_one_by_one_test_base(#{<<"start">> => 10, <<"end">> => 10, <<"step">> => 1}).


%% @private
-spec iterate_one_by_one_test_base(atm_store_api:initial_value()) -> ok | no_return().
iterate_one_by_one_test_base(#{<<"end">> := End} = InitialValue) ->
    Start = maps:get(<<"start">>, InitialValue, 0),
    Step = maps:get(<<"step">>, InitialValue, 1),

    AtmStoreIteratorStrategy = #atm_store_iterator_serial_strategy{},
    iterate_test_base(InitialValue, AtmStoreIteratorStrategy, lists:seq(Start, End, Step)).


iterate_in_chunks_5_with_start_10_end_50_step_2_test(_Config) ->
    iterate_in_chunks_test_base(5, #{<<"start">> => 10, <<"end">> => 50, <<"step">> => 2}).


iterate_in_chunks_10_with_start_1_end_2_step_10_test(_Config) ->
    iterate_in_chunks_test_base(10, #{<<"start">> => 1, <<"end">> => 2, <<"step">> => 10}).


iterate_in_chunks_10_with_start_minus_50_end_50_step_4_test(_Config) ->
    iterate_in_chunks_test_base(10, #{<<"start">> => -50, <<"end">> => 50, <<"step">> => 4}).


iterate_in_chunks_7_with_start_50_end_minus_50_step_minus_3_test(_Config) ->
    iterate_in_chunks_test_base(7, #{<<"start">> => 50, <<"end">> => -50, <<"step">> => -3}).


iterate_in_chunks_3_with_start_10_end_10_step_2_test(_Config) ->
    iterate_in_chunks_test_base(3, #{<<"start">> => 10, <<"end">> => 10, <<"step">> => 2}).


%% @private
-spec iterate_in_chunks_test_base(pos_integer(), atm_store_api:initial_value()) ->
    ok | no_return().
iterate_in_chunks_test_base(ChunkSize, #{<<"end">> := End} = InitialValue) ->
    Start = maps:get(<<"start">>, InitialValue, 0),
    Step = maps:get(<<"step">>, InitialValue, 1),

    iterate_test_base(
        InitialValue,
        #atm_store_iterator_batch_strategy{size = ChunkSize},
        automation_test_utils:split_into_chunks(ChunkSize, [], lists:seq(Start, End, Step))
    ).


%% @private
-spec iterate_test_base(
    atm_store_api:initial_value(),
    atm_store_iterator_spec:strategy(),
    [item()] | [[item()]]
) ->
    ok | no_return().
iterate_test_base(AtmRangeStoreInitialValue, AtmStoreIteratorStrategy, ExpItems) ->
    Node = oct_background:get_random_provider_node(krakow),

    AtmRangeStoreDummySchemaId = <<"dummyId">>,

    {ok, AtmRangeStoreId} =  automation_test_utils:create_store(Node, AtmRangeStoreInitialValue, ?ATM_RANGE_STORE_SCHEMA),
    AtmStoreIteratorSpec = #atm_store_iterator_spec{
        store_schema_id = AtmRangeStoreDummySchemaId,
        strategy = AtmStoreIteratorStrategy
    },
    AtmWorkflowExecutionEnv = #atm_workflow_execution_env{
        store_registry = #{AtmRangeStoreDummySchemaId => AtmRangeStoreId}
    },
    AtmStoreIterator =  automation_test_utils:acquire_store_iterator(Node, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec),

    assert_all_items_listed(Node, AtmStoreIterator, ExpItems).


%% @private
-spec assert_all_items_listed(node(), atm_store_iterator:record(), [item()] | [[item()]]) ->
    ok | no_return().
assert_all_items_listed(Node, AtmStoreIterator, []) ->
    ?assertEqual(stop, automation_test_utils:iterator_get_next(Node, AtmStoreIterator)),
    ok;
assert_all_items_listed(Node, AtmStoreIterator0, [ExpItem | RestItems]) ->
    {ok, _, _, AtmStoreIterator1} = ?assertMatch(
        {ok, ExpItem, _, _}, automation_test_utils:iterator_get_next(Node, AtmStoreIterator0)
    ),
    assert_all_items_listed(Node, AtmStoreIterator1, RestItems).


iterator_cursor_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),

    AtmRangeStoreDummySchemaId = <<"dummyId">>,
    InitialValue = #{<<"start">> => 2, <<"end">> => 16, <<"step">> => 3},
    {ok, AtmRangeStoreId} = automation_test_utils:create_store(Node, InitialValue, ?ATM_RANGE_STORE_SCHEMA),

    AtmStoreIteratorSpec = #atm_store_iterator_spec{
        store_schema_id = AtmRangeStoreDummySchemaId,
        strategy = #atm_store_iterator_serial_strategy{}
    },
    AtmWorkflowExecutionEnv = #atm_workflow_execution_env{
        store_registry = #{AtmRangeStoreDummySchemaId => AtmRangeStoreId}
    },
    AtmSerialIterator0 =  automation_test_utils:acquire_store_iterator(Node, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec),

    {ok, _, Cursor1, AtmSerialIterator1} = ?assertMatch({ok, 2, _, _}, automation_test_utils:iterator_get_next(Node, AtmSerialIterator0)),
    {ok, _, _Cursor2, AtmSerialIterator2} = ?assertMatch({ok, 5, _, _}, automation_test_utils:iterator_get_next(Node, AtmSerialIterator1)),
    {ok, _, Cursor3, AtmSerialIterator3} = ?assertMatch({ok, 8, _, _}, automation_test_utils:iterator_get_next(Node, AtmSerialIterator2)),
    {ok, _, _Cursor4, AtmSerialIterator4} = ?assertMatch({ok, 11, _, _}, automation_test_utils:iterator_get_next(Node, AtmSerialIterator3)),
    {ok, _, _Cursor5, AtmSerialIterator5} = ?assertMatch({ok, 14, _, _}, automation_test_utils:iterator_get_next(Node, AtmSerialIterator4)),
    ?assertMatch(stop, automation_test_utils:iterator_get_next(Node, AtmSerialIterator5)),

    % Assert cursor shifts iterator to the beginning
    AtmSerialIterator6 = automation_test_utils:iterator_jump_to(Node, Cursor3, AtmSerialIterator5),
    {ok, _, _Cursor7, AtmSerialIterator7} = ?assertMatch({ok, 11, _, _}, automation_test_utils:iterator_get_next(Node, AtmSerialIterator6)),
    ?assertMatch({ok, 14, _, _}, automation_test_utils:iterator_get_next(Node, AtmSerialIterator7)),

    AtmSerialIterator8 = automation_test_utils:iterator_jump_to(Node, Cursor1, AtmSerialIterator7),
    {ok, _, _Cursor9, AtmSerialIterator9} = ?assertMatch({ok, 5, _, _}, automation_test_utils:iterator_get_next(Node, AtmSerialIterator8)),
    ?assertMatch({ok, 8, _, _}, automation_test_utils:iterator_get_next(Node, AtmSerialIterator9)),

    % Assert <<>> cursor shifts iterator to the beginning
    AtmSerialIterator10 = automation_test_utils:iterator_jump_to(Node, <<>>, AtmSerialIterator9),
    ?assertMatch({ok, 2, _, _}, automation_test_utils:iterator_get_next(Node, AtmSerialIterator10)),

    % Invalid cursors should be rejected
    ?assertMatch(?EINVAL, automation_test_utils:iterator_jump_to(Node, <<"dummy">>, AtmSerialIterator9)),
    ?assertMatch(?EINVAL, automation_test_utils:iterator_jump_to(Node, <<"-2">>, AtmSerialIterator9)),
    ?assertMatch(?EINVAL, automation_test_utils:iterator_jump_to(Node, <<"3">>, AtmSerialIterator9)),
    ?assertMatch(?EINVAL, automation_test_utils:iterator_jump_to(Node, <<"20">>, AtmSerialIterator9)).


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
