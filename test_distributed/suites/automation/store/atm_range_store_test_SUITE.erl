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

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
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
    create_test/1,
    update_content_test/1,

    iterate_in_chunks_5_with_start_10_end_50_step_2_test/1,
    iterate_in_chunks_10_with_start_1_end_2_step_10_test/1,
    iterate_in_chunks_10_with_start_minus_50_end_50_step_4_test/1,
    iterate_in_chunks_7_with_start_50_end_minus_50_step_minus_3_test/1,
    iterate_in_chunks_3_with_start_10_end_10_step_2_test/1,

    reuse_iterator_test/1,
    browse_content_test/1
]).

groups() -> [
    {singular_item_based_stores_common_tests, [parallel], [
        create_test,
        update_content_test,
        browse_content_test
    ]},
    {range_store_specific_tests, [parallel], [
        iterate_in_chunks_5_with_start_10_end_50_step_2_test,
        iterate_in_chunks_10_with_start_1_end_2_step_10_test,
        iterate_in_chunks_10_with_start_minus_50_end_50_step_4_test,
        iterate_in_chunks_7_with_start_50_end_minus_50_step_minus_3_test,
        iterate_in_chunks_3_with_start_10_end_10_step_2_test,
        reuse_iterator_test
    ]}
].

all() -> [
    {group, singular_item_based_stores_common_tests},
    {group, range_store_specific_tests}
].


-define(ATM_STORE_CONFIG, #atm_range_store_config{}).

-define(PROVIDER_SELECTOR, krakow).
-define(rpc(Expr), ?rpc(?PROVIDER_SELECTOR, Expr)).
-define(erpc(Expr), ?erpc(?PROVIDER_SELECTOR, Expr)).


%%%===================================================================
%%% API functions
%%%===================================================================


create_test(_Config) ->
    atm_singleton_content_based_stores_test_base:create_test_base(
        [?ATM_STORE_CONFIG],
        fun get_item_data_spec/1
    ).


update_content_test(_Config) ->
    atm_singleton_content_based_stores_test_base:update_content_test_base(
        [?ATM_STORE_CONFIG],
        fun get_item_data_spec/1,
        #atm_range_store_content_update_options{},
        fun get_content/2
    ).


browse_content_test(_Config) ->
    atm_singleton_content_based_stores_test_base:browse_content_test_base(
        [?ATM_STORE_CONFIG],
        fun get_item_data_spec/1,
        #atm_range_store_content_browse_options{},
        fun set_content/3,
        fun(Content) -> #atm_range_store_content_browse_result{range = Content} end
    ).


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
    AtmStoreSchema = atm_store_test_utils:build_store_schema(?ATM_STORE_CONFIG),
    AtmStoreSchemaId = AtmStoreSchema#atm_store_schema.id,

    {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
        AtmWorkflowExecutionAuth, AtmRangeStoreInitialValue, AtmStoreSchema
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


reuse_iterator_test(_Config) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),
    AtmStoreSchema = atm_store_test_utils:build_store_schema(?ATM_STORE_CONFIG),
    AtmStoreSchemaId = AtmStoreSchema#atm_store_schema.id,

    {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
        AtmWorkflowExecutionAuth,
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
        ?LOGGER_DEBUG,
        #{AtmStoreSchemaId => AtmStoreId}
    ).


%% @private
-spec get_item_data_spec(atm_range_store_config:record()) -> atm_data_spec:record().
get_item_data_spec(#atm_range_store_config{}) -> #atm_data_spec{type = atm_range_type}.


%% @private
-spec set_content(atm_workflow_execution_auth:record(), atm_value:expanded(), atm_store:id()) ->
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
    undefined | atm_value:expanded().
get_content(AtmWorkflowExecutionAuth, AtmStoreId) ->
    try
        #atm_range_store_content_browse_result{range = RangeJson} = ?erpc(atm_store_api:browse_content(
            AtmWorkflowExecutionAuth,
            #atm_range_store_content_browse_options{},
            AtmStoreId
        )),
        RangeJson
    catch throw:?ERROR_ATM_STORE_CONTENT_NOT_SET(_) ->
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
    ModulesToLoad = [?MODULE | atm_singleton_content_based_stores_test_base:modules_to_load()],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(singular_item_based_stores_common_tests, Config) ->
    atm_singleton_content_based_stores_test_base:init_per_group(Config);
init_per_group(range_store_specific_tests, Config) ->
    Config.


end_per_group(singular_item_based_stores_common_tests, Config) ->
    atm_singleton_content_based_stores_test_base:end_per_group(Config);
end_per_group(range_store_specific_tests, _Config) ->
    ok.


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
