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

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("test_rpc.hrl").

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
    create_test/1,
    apply_operation_test/1,

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
        create_test,
        apply_operation_test,

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


-define(PROVIDER_SELECTOR, krakow).
-define(STORE_SCHEMA_ID, <<"dummy_range_store_id">>).

-define(STORE_SCHEMA, #atm_store_schema{
    id = ?STORE_SCHEMA_ID,
    name = <<"range_store">>,
    description = <<"description">>,
    requires_initial_content = true,
    type = range,
    config = #atm_range_store_config{}
}).

-define(rpc(Expr), ?rpc(?PROVIDER_SELECTOR, Expr)).


%%%===================================================================
%%% API functions
%%%===================================================================


create_test(_Config) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    lists:foreach(fun({InvalidInitialContent, ExpError}) ->
        ?assertEqual(ExpError, ?rpc(catch atm_store_api:create(
            AtmWorkflowExecutionAuth, InvalidInitialContent, ?STORE_SCHEMA
        )))
    end, [
        {undefined, ?ERROR_ATM_STORE_MISSING_REQUIRED_INITIAL_CONTENT},
        {#{}, ?ERROR_MISSING_REQUIRED_VALUE(<<"end">>)},
        {#{<<"end">> => <<"NaN">>},
            ?ERROR_BAD_DATA(<<"end">>, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(<<"NaN">>, atm_integer_type))
        },
        {#{<<"start">> => <<"NaN">>, <<"end">> => 10},
            ?ERROR_BAD_DATA(<<"start">>, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(<<"NaN">>, atm_integer_type))
        },
        {#{<<"start">> => 5, <<"end">> => 10, <<"step">> => <<"NaN">>},
            ?ERROR_BAD_DATA(<<"step">>, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(<<"NaN">>, atm_integer_type))
        },
        {#{<<"start">> => 5, <<"end">> => 10, <<"step">> => 0},
            ?ERROR_BAD_DATA(<<"range">>, <<"invalid range specification">>)
        },
        {#{<<"start">> => 15, <<"end">> => 10, <<"step">> => 1},
            ?ERROR_BAD_DATA(<<"range">>, <<"invalid range specification">>)
        },
        {#{<<"start">> => -15, <<"end">> => -10, <<"step">> => -1},
            ?ERROR_BAD_DATA(<<"range">>, <<"invalid range specification">>)
        },
        {#{<<"start">> => 10, <<"end">> => 15, <<"step">> => -1},
            ?ERROR_BAD_DATA(<<"range">>, <<"invalid range specification">>)
        }
    ]),

    lists:foreach(fun(ValidInitialContent) ->
        ?assertMatch(
            {ok, #document{value = #atm_store{initial_content = ValidInitialContent, frozen = false}}},
            ?rpc(atm_store_api:create(AtmWorkflowExecutionAuth, ValidInitialContent, ?STORE_SCHEMA))
        )
    end, [
        #{<<"end">> => 10},
        #{<<"start">> => 1, <<"end">> => 10},
        #{<<"start">> => 5, <<"end">> => 10, <<"step">> => 2},
        #{<<"start">> => 15, <<"end">> => 10, <<"step">> => -1}
    ]).


apply_operation_test(_Config) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    InitialContent = #{<<"start">> => 5, <<"end">> => 10, <<"step">> => 2},
    {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
        AtmWorkflowExecutionAuth, InitialContent, ?STORE_SCHEMA
    ))),

    % Assert none operation is supported
    NewContent = #{<<"end">> => 100},
    lists:foreach(fun(Operation) ->
        ?assertEqual(?ERROR_NOT_SUPPORTED, ?rpc(catch atm_store_api:apply_operation(
            AtmWorkflowExecutionAuth, Operation, NewContent, #{}, AtmStoreId
        ))),
        ?assertEqual(InitialContent, get_content(AtmWorkflowExecutionAuth, AtmStoreId))
    end, atm_task_schema_result_mapper:all_dispatch_functions()).


iterate_in_chunks_5_with_start_10_end_50_step_2_test(_Config) ->
    iterate_test_base(5, #{<<"start">> => 10, <<"end">> => 50, <<"step">> => 2}).


iterate_in_chunks_10_with_start_1_end_2_step_10_test(_Config) ->
    iterate_test_base(10, #{<<"start">> => 1, <<"end">> => 2, <<"step">> => 10}).


iterate_in_chunks_10_with_start_minus_50_end_50_step_4_test(_Config) ->
    iterate_test_base(10, #{<<"start">> => -50, <<"end">> => 50, <<"step">> => 4}).


iterate_in_chunks_7_with_start_50_end_minus_50_step_minus_3_test(_Config) ->
    iterate_test_base(7, #{<<"start">> => 50, <<"end">> => -50, <<"step">> => -3}).


iterate_in_chunks_3_with_start_10_end_10_step_2_test(_Config) ->
    iterate_test_base(3, #{<<"start">> => 10, <<"end">> => 10, <<"step">> => 2}).


%% @private
-spec iterate_test_base(pos_integer(), atm_store_api:initial_content()) ->
    ok | no_return().
iterate_test_base(ChunkSize, AtmRangeStoreInitialValue) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
        AtmWorkflowExecutionAuth, AtmRangeStoreInitialValue, ?STORE_SCHEMA
    ))),
    AtmWorkflowExecutionEnv = build_workflow_execution_env(AtmWorkflowExecutionAuth, AtmStoreId),

    Iterator = ?rpc(atm_store_api:acquire_iterator(AtmStoreId, #atm_store_iterator_spec{
        store_schema_id = ?STORE_SCHEMA_ID,
        max_batch_size = ChunkSize
    })),
    ExpBatches = atm_store_test_utils:split_into_chunks(ChunkSize, [], lists:seq(
        maps:get(<<"start">>, AtmRangeStoreInitialValue, 0),
        maps:get(<<"end">>, AtmRangeStoreInitialValue),
        maps:get(<<"step">>, AtmRangeStoreInitialValue, 1)
    )),

    assert_all_items_listed(AtmWorkflowExecutionEnv, Iterator, ExpBatches).


reuse_iterator_test(_Config) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
        AtmWorkflowExecutionAuth,
        #{<<"start">> => 2, <<"end">> => 16, <<"step">> => 3},
        ?STORE_SCHEMA
    ))),
    AtmWorkflowExecutionEnv = build_workflow_execution_env(AtmWorkflowExecutionAuth, AtmStoreId),

    Iterator0 = ?rpc(atm_store_api:acquire_iterator(AtmStoreId, #atm_store_iterator_spec{
        store_schema_id = ?STORE_SCHEMA_ID,
        max_batch_size = 1
    })),

    {ok, _, Iterator1} = ?assertMatch({ok, [2], _}, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, Iterator0))),
    {ok, _, Iterator2} = ?assertMatch({ok, [5], _}, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, Iterator1))),
    {ok, _, Iterator3} = ?assertMatch({ok, [8], _}, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, Iterator2))),
    {ok, _, Iterator4} = ?assertMatch({ok, [11], _}, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, Iterator3))),
    {ok, _, Iterator5} = ?assertMatch({ok, [14], _}, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, Iterator4))),
    ?assertMatch(stop, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, Iterator5))),

    ?assertMatch({ok, [2], _}, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, Iterator0))),

    {ok, _, Iterator7} = ?assertMatch({ok, [11], _}, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, Iterator3))),
    ?assertMatch({ok, [14], _}, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, Iterator7))),

    {ok, _, Iterator9} = ?assertMatch({ok, [5], _}, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, Iterator1))),
    ?assertMatch({ok, [8], _}, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, Iterator9))).


browse_test(_Config) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    Content = #{<<"start">> => 2, <<"end">> => 16, <<"step">> => 3},
    {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
        AtmWorkflowExecutionAuth, Content, ?STORE_SCHEMA
    ))),

    ?assertEqual(
        {[{<<>>, {ok, Content}}], true},
        ?rpc(atm_store_api:browse_content(AtmWorkflowExecutionAuth, #{}, AtmStoreId))
    ).


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
-spec build_workflow_execution_env(atm_workflow_execution_auth:record(), atm_store:id()) ->
    atm_workflow_execution_env:record().
build_workflow_execution_env(AtmWorkflowExecutionAuth, AtmStoreId) ->
    atm_workflow_execution_env:build(
        atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),
        atm_workflow_execution_auth:get_workflow_execution_id(AtmWorkflowExecutionAuth),
        0,
        #{?STORE_SCHEMA_ID => AtmStoreId}
    ).


%% @private
-spec get_content(atm_workflow_execution_auth:record(), atm_store:id()) ->
    undefined | atm_value:expanded().
get_content(AtmWorkflowExecutionAuth, AtmStoreId) ->
    case ?rpc(atm_store_api:browse_content(AtmWorkflowExecutionAuth, #{}, AtmStoreId)) of
        {[], true} ->
            undefined;
        {[{_, {ok, Item}}], true} ->
            Item
    end.


%% @private
-spec assert_all_items_listed(
    atm_workflow_execution_env:record(),
    atm_store_iterator:record(),
    [automation:item()] | [[automation:item()]]
) ->
    ok | no_return().
assert_all_items_listed(AtmWorkflowExecutionEnv, Iterator, []) ->
    ?assertEqual(stop, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, Iterator))),
    ok;
assert_all_items_listed(AtmWorkflowExecutionEnv, Iterator0, [ExpBatch | RestBatches]) ->
    {ok, _, Iterator1} = ?assertMatch(
        {ok, ExpBatch, _},
        ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, Iterator0))
    ),
    assert_all_items_listed(AtmWorkflowExecutionEnv, Iterator1, RestBatches).


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
