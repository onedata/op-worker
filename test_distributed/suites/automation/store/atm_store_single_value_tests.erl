%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Bodies of the automation single value store test cases, run by
%%% 'atm_store_test_SUITE'.
%%%
%%% Supplies this store's parametrisation of the contract shared with the
%%% other singleton-content stores, which is asserted in
%%% 'atm_store_singleton_content_based_tests', and holds the single-value
%%% specific case: iteration over the one held item.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_single_value_tests).
-author("Michal Stanisz").

% This module indirectly includes eunit.hrl, whose parse transform would
% otherwise auto-export every arity 0 function named *_test - clashing with
% the export list below.
-define(EUNIT_NOAUTO, 1).

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("test_rpc.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% test cases
-export([
    create_test/0,
    update_content_test/0,
    iterator_test/0,
    browse_content_test/0
]).


-define(PROVIDER_SELECTOR, krakow).
-define(rpc(Expr), ?rpc(?PROVIDER_SELECTOR, Expr)).
-define(erpc(Expr), ?erpc(?PROVIDER_SELECTOR, Expr)).


%%%===================================================================
%%% Test functions
%%%===================================================================


create_test() ->
    atm_store_singleton_content_based_tests:create_test_base(
        example_configs(),
        fun get_item_data_spec/1
    ).


update_content_test() ->
    atm_store_singleton_content_based_tests:update_content_test_base(
        example_configs(),
        fun get_item_data_spec/1,
        #atm_single_value_store_content_update_options{},
        fun browse_content/2
    ).


browse_content_test() ->
    atm_store_singleton_content_based_tests:browse_content_test_base(
        example_configs(),
        fun get_item_data_spec/1,
        #atm_single_value_store_content_browse_options{},
        fun set_content/3,
        fun(Content) -> #atm_single_value_store_content_browse_result{item = {ok, Content}} end
    ).


iterator_test() ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    lists:foreach(fun(Config = #atm_single_value_store_config{item_data_spec = ItemDataSpec}) ->
        AtmStoreSchema = atm_store_test_utils:build_store_schema(Config),
        {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
            AtmWorkflowExecutionAuth, ?DEBUG_AUDIT_LOG_SEVERITY_INT, undefined, AtmStoreSchema
        ))),
        AtmStoreIteratorSpec = #atm_store_iterator_spec{
            store_schema_id = AtmStoreSchema#atm_store_schema.id,
            max_batch_size = rand:uniform(8)
        },
        AtmWorkflowExecutionEnv = atm_workflow_execution_env:build(
            atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),
            atm_workflow_execution_auth:get_workflow_execution_id(AtmWorkflowExecutionAuth),
            0,
            ?DEBUG_AUDIT_LOG_SEVERITY_INT,
            #{AtmStoreSchema#atm_store_schema.id => AtmStoreId}
        ),

        AtmStoreIterator0 = ?rpc(atm_store_api:acquire_iterator(AtmStoreId, AtmStoreIteratorSpec)),
        ?assertEqual(stop, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, AtmStoreIterator0))),

        Item = gen_valid_data(AtmWorkflowExecutionAuth, ItemDataSpec),
        FullyExpandedItem = to_iterated_item(AtmWorkflowExecutionAuth, Item, ItemDataSpec),
        set_content(AtmWorkflowExecutionAuth, Item, AtmStoreId),

        AtmStoreIterator1 = ?rpc(atm_store_api:acquire_iterator(AtmStoreId, AtmStoreIteratorSpec)),
        {ok, _, AtmStoreIterator2} = ?assertMatch(
            {ok, [#atm_item_execution{value = FullyExpandedItem}], _},
            ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, AtmStoreIterator1))
        ),
        ?assertEqual(stop, ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, AtmStoreIterator2))),

        %% Assert previous iterators can be reused
        ?assertMatch(
            {ok, [#atm_item_execution{value = FullyExpandedItem}], _},
            ?rpc(iterator:get_next(AtmWorkflowExecutionEnv, AtmStoreIterator1))
        )

    end, example_configs()).


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
-spec example_configs() -> [atm_single_value_store_config:record()].
example_configs() ->
    lists:map(fun(ItemDataType) ->
        #atm_single_value_store_config{item_data_spec = atm_store_test_utils:example_data_spec(
            ItemDataType
        )}
    end, [
        atm_array_type,
        atm_boolean_type,
        atm_dataset_type,
        atm_file_type,
        atm_group_type,
        atm_number_type,
        atm_object_type,
        atm_range_type,
        atm_string_type,
        atm_time_series_measurement_type
    ]).


%% @private
-spec get_item_data_spec(atm_single_value_store_config:record()) -> atm_data_spec:record().
get_item_data_spec(#atm_single_value_store_config{item_data_spec = ItemDataSpec}) ->
    ItemDataSpec.


%% @private
-spec to_iterated_item(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_store:id()
) ->
    automation:item().
to_iterated_item(AtmWorkflowExecutionAuth, Item, ItemDataSpec) ->
    atm_store_test_utils:to_iterated_item(
        ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, Item, ItemDataSpec
    ).


%% @private
-spec gen_valid_data(atm_workflow_execution_auth:record(), atm_data_spec:record()) ->
    automation:item().
gen_valid_data(AtmWorkflowExecutionAuth, ItemDataSpec) ->
    atm_store_test_utils:gen_valid_data(
        ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, ItemDataSpec
    ).


%% @private
-spec set_content(atm_workflow_execution_auth:record(), automation:item(), atm_store:id()) ->
    ok.
set_content(AtmWorkflowExecutionAuth, Item, AtmStoreId) ->
    ?rpc(atm_store_api:update_content(
        AtmWorkflowExecutionAuth,
        Item,
        #atm_single_value_store_content_update_options{},
        AtmStoreId
    )).


%% @private
-spec browse_content(atm_workflow_execution_auth:record(), atm_store:id()) ->
    undefined | automation:item().
browse_content(AtmWorkflowExecutionAuth, AtmStoreId) ->
    BrowseOpts = #atm_single_value_store_content_browse_options{},
    try
        #atm_single_value_store_content_browse_result{
            item = {ok, Item}
        } = ?erpc(atm_store_api:browse_content(
            AtmWorkflowExecutionAuth, BrowseOpts, AtmStoreId
        )),
        Item
    catch throw:?ERR_ATM_STORE_CONTENT_NOT_SET(_) ->
        undefined
    end.
