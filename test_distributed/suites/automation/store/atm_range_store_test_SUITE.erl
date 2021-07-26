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
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    create_store_with_invalid_args_test/1,
    apply_operation_test/1,

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

    reuse_iterator_test/1,
    browse_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        create_store_with_invalid_args_test,
        apply_operation_test,

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

        reuse_iterator_test,
        browse_test
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
    AtmWorkflowExecutionAuth = atm_store_test_utils:create_workflow_execution_auth(
        krakow, user1, space_krk
    ),

    lists:foreach(fun({InvalidInitialValue, ExpError}) ->
        ?assertEqual(ExpError, atm_store_test_utils:create_store(
            krakow, AtmWorkflowExecutionAuth, InvalidInitialValue, ?ATM_RANGE_STORE_SCHEMA
        ))
    end, [
        {undefined, ?ERROR_ATM_STORE_MISSING_REQUIRED_INITIAL_VALUE},
        {#{}, ?ERROR_MISSING_REQUIRED_VALUE(<<"end">>)},
        {#{<<"end">> => <<"NaN">>},
            ?ERROR_ATM_BAD_DATA(<<"end">>, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(<<"NaN">>, atm_integer_type))
        },
        {#{<<"start">> => <<"NaN">>, <<"end">> => 10},
            ?ERROR_ATM_BAD_DATA(<<"start">>, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(<<"NaN">>, atm_integer_type))
        },
        {#{<<"start">> => 5, <<"end">> => 10, <<"step">> => <<"NaN">>},
            ?ERROR_ATM_BAD_DATA(<<"step">>, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(<<"NaN">>, atm_integer_type))
        },
        {#{<<"start">> => 5, <<"end">> => 10, <<"step">> => 0},
            ?ERROR_ATM_BAD_DATA(<<"range">>, <<"invalid range specification">>)
        },
        {#{<<"start">> => 15, <<"end">> => 10, <<"step">> => 1},
            ?ERROR_ATM_BAD_DATA(<<"range">>, <<"invalid range specification">>)
        },
        {#{<<"start">> => -15, <<"end">> => -10, <<"step">> => -1},
            ?ERROR_ATM_BAD_DATA(<<"range">>, <<"invalid range specification">>)
        },
        {#{<<"start">> => 10, <<"end">> => 15, <<"step">> => -1},
            ?ERROR_ATM_BAD_DATA(<<"range">>, <<"invalid range specification">>)
        }
    ]).


apply_operation_test(_Config) ->
    AtmWorkflowExecutionAuth = atm_store_test_utils:create_workflow_execution_auth(
        krakow, user1, space_krk
    ),
    {ok, AtmRangeStoreId} = atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionAuth, #{<<"end">> => 8}, ?ATM_RANGE_STORE_SCHEMA
    ),

    ?assertEqual(?ERROR_NOT_SUPPORTED, atm_store_test_utils:apply_operation(
        krakow, AtmWorkflowExecutionAuth, append, <<"NaN">>, #{}, AtmRangeStoreId
    )),
    ?assertEqual(?ERROR_NOT_SUPPORTED, atm_store_test_utils:apply_operation(
        krakow, AtmWorkflowExecutionAuth, set, <<"NaN">>, #{}, AtmRangeStoreId
    )).


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
        atm_store_test_utils:split_into_chunks(ChunkSize, [], lists:seq(Start, End, Step))
    ).


%% @private
-spec iterate_test_base(
    atm_store_api:initial_value(),
    atm_store_iterator_spec:strategy(),
    [automation:item()] | [[automation:item()]]
) ->
    ok | no_return().
iterate_test_base(AtmRangeStoreInitialValue, AtmStoreIteratorStrategy, ExpItems) ->
    AtmWorkflowExecutionAuth = atm_store_test_utils:create_workflow_execution_auth(
        krakow, user1, space_krk
    ),

    {ok, AtmRangeStoreId} = atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionAuth, AtmRangeStoreInitialValue, ?ATM_RANGE_STORE_SCHEMA
    ),

    AtmRangeStoreDummySchemaId = <<"dummyId">>,

    AtmWorkflowExecutionEnv = atm_workflow_execution_env:build(
        atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),
        atm_workflow_execution_auth:get_workflow_execution_id(AtmWorkflowExecutionAuth),
        #{AtmRangeStoreDummySchemaId => AtmRangeStoreId}, undefined, undefined
    ),
    AtmStoreIteratorSpec = #atm_store_iterator_spec{
        store_schema_id = AtmRangeStoreDummySchemaId,
        strategy = AtmStoreIteratorStrategy
    },
    AtmStoreIterator = atm_store_test_utils:acquire_store_iterator(krakow, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec),

    assert_all_items_listed(krakow, AtmWorkflowExecutionEnv, AtmStoreIterator, ExpItems).


%% @private
-spec assert_all_items_listed(
    node(),
    atm_workflow_execution_env:record(),
    atm_store_iterator:record(),
    [automation:item()] | [[automation:item()]]
) ->
    ok | no_return().
assert_all_items_listed(Node, AtmWorkflowExecutionEnv, AtmStoreIterator, []) ->
    ?assertEqual(stop, atm_store_test_utils:iterator_get_next(Node, AtmWorkflowExecutionEnv, AtmStoreIterator)),
    ok;
assert_all_items_listed(Node, AtmWorkflowExecutionEnv, AtmStoreIterator0, [ExpItem | RestItems]) ->
    {ok, _, AtmStoreIterator1} = ?assertMatch(
        {ok, ExpItem, _}, atm_store_test_utils:iterator_get_next(Node, AtmWorkflowExecutionEnv, AtmStoreIterator0)
    ),
    assert_all_items_listed(Node, AtmWorkflowExecutionEnv, AtmStoreIterator1, RestItems).


reuse_iterator_test(_Config) ->
    AtmWorkflowExecutionAuth = atm_store_test_utils:create_workflow_execution_auth(
        krakow, user1, space_krk
    ),

    InitialValue = #{<<"start">> => 2, <<"end">> => 16, <<"step">> => 3},
    {ok, AtmRangeStoreId} = atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionAuth, InitialValue, ?ATM_RANGE_STORE_SCHEMA
    ),

    AtmRangeStoreDummySchemaId = <<"dummyId">>,

    AtmWorkflowExecutionEnv = atm_workflow_execution_env:build(
        atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),
        atm_workflow_execution_auth:get_workflow_execution_id(AtmWorkflowExecutionAuth),
        #{AtmRangeStoreDummySchemaId => AtmRangeStoreId}, undefined, undefined
    ),
    AtmStoreIteratorSpec = #atm_store_iterator_spec{
        store_schema_id = AtmRangeStoreDummySchemaId,
        strategy = #atm_store_iterator_serial_strategy{}
    },
    AtmSerialIterator0 =  atm_store_test_utils:acquire_store_iterator(krakow, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec),

    {ok, _, AtmSerialIterator1} = ?assertMatch({ok, 2, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator0)),
    {ok, _, AtmSerialIterator2} = ?assertMatch({ok, 5, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator1)),
    {ok, _, AtmSerialIterator3} = ?assertMatch({ok, 8, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator2)),
    {ok, _, AtmSerialIterator4} = ?assertMatch({ok, 11, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator3)),
    {ok, _, AtmSerialIterator5} = ?assertMatch({ok, 14, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator4)),
    ?assertMatch(stop, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator5)),
    
    ?assertMatch({ok, 2, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator0)),
    
    {ok, _, AtmSerialIterator7} = ?assertMatch({ok, 11, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator3)),
    ?assertMatch({ok, 14, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator7)),

    {ok, _, AtmSerialIterator9} = ?assertMatch({ok, 5, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator1)),
    ?assertMatch({ok, 8, _}, atm_store_test_utils:iterator_get_next(krakow, AtmWorkflowExecutionEnv, AtmSerialIterator9)).


browse_test(_Config) ->
    AtmWorkflowExecutionAuth = atm_store_test_utils:create_workflow_execution_auth(
        krakow, user1, space_krk
    ),
    InitialValue = #{<<"start">> => 2, <<"end">> => 16, <<"step">> => 3},
    {ok, AtmStoreId} = atm_store_test_utils:create_store(
        krakow, AtmWorkflowExecutionAuth, InitialValue, ?ATM_RANGE_STORE_SCHEMA
    ),
    {ok, AtmStore} = atm_store_test_utils:get(krakow, AtmStoreId),
    ?assertEqual({[{<<>>, {ok, InitialValue}}], true},
        atm_store_test_utils:browse_content(krakow, AtmWorkflowExecutionAuth, #{}, AtmStore)).


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
