%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Bodies of the automation range store test cases, run by
%%% 'atm_store_test_SUITE'.
%%%
%%% Supplies this store's parametrisation of the contract shared with the
%%% other singleton-content stores, which is asserted in
%%% 'atm_store_singleton_content_based_tests', and holds the range specific
%%% cases: iteration in chunks over ranges with assorted start/end/step
%%% combinations, and iterator reuse.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_range_tests).
-author("Bartosz Walkowicz").

% This module indirectly includes eunit.hrl, whose parse transform would
% otherwise auto-export every arity 0 function named *_test - clashing with
% the export list below.
-define(EUNIT_NOAUTO, 1).

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("test_rpc.hrl").
-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


%% test cases
-export([
    create_test/0,
    update_content_test/0,

    iterate_in_chunks_5_with_start_10_end_50_step_2_test/0,
    iterate_in_chunks_10_with_start_1_end_2_step_10_test/0,
    iterate_in_chunks_10_with_start_minus_50_end_50_step_4_test/0,
    iterate_in_chunks_7_with_start_50_end_minus_50_step_minus_3_test/0,
    iterate_in_chunks_3_with_start_10_end_10_step_2_test/0,

    reuse_iterator_test/0,
    browse_content_test/0
]).


-define(ATM_STORE_CONFIG, #atm_range_store_config{}).

-define(PROVIDER_SELECTOR, krakow).
-define(rpc(Expr), ?rpc(?PROVIDER_SELECTOR, Expr)).
-define(erpc(Expr), ?erpc(?PROVIDER_SELECTOR, Expr)).


%%%===================================================================
%%% API functions
%%%===================================================================


create_test() ->
    atm_store_singleton_content_based_tests:create_test_base(
        [?ATM_STORE_CONFIG],
        fun get_item_data_spec/1
    ).


update_content_test() ->
    atm_store_singleton_content_based_tests:update_content_test_base(
        [?ATM_STORE_CONFIG],
        fun get_item_data_spec/1,
        #atm_range_store_content_update_options{},
        fun get_content/2
    ).


browse_content_test() ->
    atm_store_singleton_content_based_tests:browse_content_test_base(
        [?ATM_STORE_CONFIG],
        fun get_item_data_spec/1,
        #atm_range_store_content_browse_options{},
        fun set_content/3,
        fun(Content) -> #atm_range_store_content_browse_result{range = Content} end
    ).


iterate_in_chunks_5_with_start_10_end_50_step_2_test() ->
    iterate_test_base(5, #{<<"start">> => 10, <<"end">> => 50, <<"step">> => 2}).


iterate_in_chunks_10_with_start_1_end_2_step_10_test() ->
    iterate_test_base(10, #{<<"start">> => 1, <<"end">> => 2, <<"step">> => 10}).


iterate_in_chunks_10_with_start_minus_50_end_50_step_4_test() ->
    iterate_test_base(10, #{<<"start">> => -50, <<"end">> => 50, <<"step">> => 4}).


iterate_in_chunks_7_with_start_50_end_minus_50_step_minus_3_test() ->
    iterate_test_base(7, #{<<"start">> => 50, <<"end">> => -50, <<"step">> => -3}).


iterate_in_chunks_3_with_start_10_end_10_step_2_test() ->
    iterate_test_base(3, #{<<"start">> => 10, <<"end">> => 10, <<"step">> => 2}).


%% @private
-spec iterate_test_base(pos_integer(), atm_store_api:initial_content()) ->
    ok | no_return().
iterate_test_base(ChunkSize, AtmRangeStoreInitialValue) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),
    AtmStoreSchema = atm_store_test_utils:build_store_schema(?ATM_STORE_CONFIG),
    AtmStoreSchemaId = AtmStoreSchema#atm_store_schema.id,

    {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
        AtmWorkflowExecutionAuth, ?DEBUG_AUDIT_LOG_SEVERITY_INT, AtmRangeStoreInitialValue, AtmStoreSchema
    ))),
    AtmWorkflowExecutionEnv = build_workflow_execution_env(
        AtmWorkflowExecutionAuth, AtmStoreSchemaId, AtmStoreId
    ),

    Iterator = ?rpc(atm_store_api:acquire_iterator(AtmStoreId, #atm_store_iterator_spec{
        store_schema_id = AtmStoreSchemaId,
        max_batch_size = ChunkSize
    })),

    Step = maps:get(<<"step">>, AtmRangeStoreInitialValue, 1),
    ExclusiveEnd = maps:get(<<"end">>, AtmRangeStoreInitialValue),
    InclusiveEnd = case Step > 0 of
        true -> ExclusiveEnd - 1;
        false -> ExclusiveEnd + 1
    end,
    ExpBatches = atm_store_test_utils:split_into_chunks(ChunkSize, [], lists:seq(
        maps:get(<<"start">>, AtmRangeStoreInitialValue, 0), InclusiveEnd, Step
    )),

    assert_all_items_listed(AtmWorkflowExecutionEnv, Iterator, ExpBatches).


reuse_iterator_test() ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),
    AtmStoreSchema = atm_store_test_utils:build_store_schema(?ATM_STORE_CONFIG),
    AtmStoreSchemaId = AtmStoreSchema#atm_store_schema.id,

    {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
        AtmWorkflowExecutionAuth,
        ?DEBUG_AUDIT_LOG_SEVERITY_INT,
        #{<<"start">> => 2, <<"end">> => 16, <<"step">> => 3},
        AtmStoreSchema
    ))),
    AtmWorkflowExecutionEnv = build_workflow_execution_env(
        AtmWorkflowExecutionAuth, AtmStoreSchemaId, AtmStoreId
    ),
    Iterator0 = ?rpc(atm_store_api:acquire_iterator(AtmStoreId, #atm_store_iterator_spec{
        store_schema_id = AtmStoreSchemaId,
        max_batch_size = 1
    })),

    {ok, _, Iterator1} = ?assertMatch({ok, [2], _}, iterator_get_next(AtmWorkflowExecutionEnv, Iterator0)),
    {ok, _, Iterator2} = ?assertMatch({ok, [5], _}, iterator_get_next(AtmWorkflowExecutionEnv, Iterator1)),
    {ok, _, Iterator3} = ?assertMatch({ok, [8], _}, iterator_get_next(AtmWorkflowExecutionEnv, Iterator2)),
    {ok, _, Iterator4} = ?assertMatch({ok, [11], _}, iterator_get_next(AtmWorkflowExecutionEnv, Iterator3)),
    {ok, _, Iterator5} = ?assertMatch({ok, [14], _}, iterator_get_next(AtmWorkflowExecutionEnv, Iterator4)),
    ?assertMatch(stop, iterator_get_next(AtmWorkflowExecutionEnv, Iterator5)),

    ?assertMatch({ok, [2], _}, iterator_get_next(AtmWorkflowExecutionEnv, Iterator0)),

    {ok, _, Iterator7} = ?assertMatch({ok, [11], _}, iterator_get_next(AtmWorkflowExecutionEnv, Iterator3)),
    ?assertMatch({ok, [14], _}, iterator_get_next(AtmWorkflowExecutionEnv, Iterator7)),

    {ok, _, Iterator9} = ?assertMatch({ok, [5], _}, iterator_get_next(AtmWorkflowExecutionEnv, Iterator1)),
    ?assertMatch({ok, [8], _}, iterator_get_next(AtmWorkflowExecutionEnv, Iterator9)).


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
-spec build_workflow_execution_env(
    atm_workflow_execution_auth:record(), automation:id(), atm_store:id()) ->
    atm_workflow_execution_env:record().
build_workflow_execution_env(AtmWorkflowExecutionAuth, AtmStoreSchemaId, AtmStoreId) ->
    atm_workflow_execution_env:build(
        atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),
        atm_workflow_execution_auth:get_workflow_execution_id(AtmWorkflowExecutionAuth),
        0,
        ?DEBUG_AUDIT_LOG_SEVERITY_INT,
        #{AtmStoreSchemaId => AtmStoreId}
    ).


%% @private
-spec get_item_data_spec(atm_range_store_config:record()) -> atm_data_spec:record().
get_item_data_spec(#atm_range_store_config{}) -> #atm_range_data_spec{}.


%% @private
-spec set_content(atm_workflow_execution_auth:record(), automation:item(), atm_store:id()) ->
    ok.
set_content(AtmWorkflowExecutionAuth, Item, AtmStoreId) ->
    ?rpc(atm_store_api:update_content(
        AtmWorkflowExecutionAuth,
        Item,
        #atm_range_store_content_update_options{},
        AtmStoreId
    )).


%% @private
-spec get_content(atm_workflow_execution_auth:record(), atm_store:id()) ->
    undefined | automation:item().
get_content(AtmWorkflowExecutionAuth, AtmStoreId) ->
    try
        #atm_range_store_content_browse_result{range = RangeJson} = ?erpc(atm_store_api:browse_content(
            AtmWorkflowExecutionAuth,
            #atm_range_store_content_browse_options{},
            AtmStoreId
        )),
        RangeJson
    catch throw:?ERR_ATM_STORE_CONTENT_NOT_SET(_) ->
        undefined
    end.


%% @private
-spec assert_all_items_listed(
    atm_workflow_execution_env:record(),
    atm_store_iterator:record(),
    [automation:item()] | [[automation:item()]]
) ->
    ok | no_return().
assert_all_items_listed(AtmWorkflowExecutionEnv, Iterator, []) ->
    ?assertEqual(stop, iterator_get_next(AtmWorkflowExecutionEnv, Iterator)),
    ok;
assert_all_items_listed(AtmWorkflowExecutionEnv, Iterator0, [ExpBatch | RestBatches]) ->
    {ok, _, Iterator1} = ?assertMatch(
        {ok, ExpBatch, _},
        iterator_get_next(AtmWorkflowExecutionEnv, Iterator0)
    ),
    assert_all_items_listed(AtmWorkflowExecutionEnv, Iterator1, RestBatches).


%% @private
iterator_get_next(AtmWorkflowExecutionEnv, Iterator) ->
    atm_store_test_utils:iterator_get_next(?PROVIDER_SELECTOR, AtmWorkflowExecutionEnv, Iterator).
