%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Bodies of the automation list store test cases, run by
%%% 'atm_store_test_SUITE'.
%%%
%%% Supplies this store's parametrisation of the contract shared with the
%%% other infinite-log backed stores, which is asserted in
%%% 'atm_store_infinite_log_based_tests'. There are no store specific cases -
%%% a list store adds nothing to that shared contract.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_list_tests).
-author("Michal Stanisz").

% This module indirectly includes eunit.hrl, whose parse transform would
% otherwise auto-export every arity 0 function named *_test - clashing with
% the export list below.
-define(EUNIT_NOAUTO, 1).

-include("modules/automation/atm_execution.hrl").
-include("test_rpc.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


%% test cases
-export([
    create_test/0,
    update_content_test/0,
    iterator_test/0,
    browse_content_by_index_test/0,
    browse_content_by_offset_test/0
]).


-define(PROVIDER_SELECTOR, krakow).


%%%===================================================================
%%% API functions
%%%===================================================================


create_test() ->
    atm_store_infinite_log_based_tests:create_test_base(#{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1
    }).


update_content_test() ->
    atm_store_infinite_log_based_tests:update_content_test_base(#{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1,
        describe_item => fun describe_item/4,
        build_content_update_options => fun build_content_update_options/1,
        browse_content => fun browse_content/2
    }).


iterator_test() ->
    atm_store_infinite_log_based_tests:iterator_test_base(#{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1,
        input_item_to_exp_iterated_item => fun input_item_to_exp_iterated_item/4,
        randomly_remove_entity_referenced_by_item => fun randomly_remove_entity_referenced_by_item/3
    }).


browse_content_by_index_test() ->
    atm_store_infinite_log_based_tests:browse_content_test_base(index, #{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1,
        describe_item => fun describe_item/4,
        randomly_remove_entity_referenced_by_item => fun randomly_remove_entity_referenced_by_item/3,
        build_content_browse_options => fun build_content_browse_options/1,
        build_content_browse_result => fun build_content_browse_result/2
    }).


browse_content_by_offset_test() ->
    atm_store_infinite_log_based_tests:browse_content_test_base(offset, #{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1,
        describe_item => fun describe_item/4,
        randomly_remove_entity_referenced_by_item => fun randomly_remove_entity_referenced_by_item/3,
        build_content_browse_options => fun build_content_browse_options/1,
        build_content_browse_result => fun build_content_browse_result/2
    }).


%===================================================================
% Helper functions
%===================================================================


%% @private
-spec example_configs() -> [atm_single_value_store_config:record()].
example_configs() ->
    lists:map(fun(ItemDataType) ->
        #atm_list_store_config{item_data_spec = atm_store_test_utils:example_data_spec(
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
-spec get_input_item_generator_seed_data_spec(atm_list_store_config:record()) ->
    atm_data_spec:record().
get_input_item_generator_seed_data_spec(#atm_list_store_config{item_data_spec = ItemDataSpec}) ->
    ItemDataSpec.


%% @private
-spec input_item_formatter(automation:item()) -> automation:item().
input_item_formatter(Item) -> Item.


%% @private
-spec describe_item(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_store:id(),
    non_neg_integer()
) ->
    automation:item().
describe_item(AtmWorkflowExecutionAuth, ItemInitializer, ItemDataSpec, _Index) ->
    atm_store_test_utils:to_described_item(
        ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, ItemInitializer, ItemDataSpec
    ).


%% @private
-spec input_item_to_exp_iterated_item(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_store:id(),
    non_neg_integer()
) ->
    automation:item().
input_item_to_exp_iterated_item(AtmWorkflowExecutionAuth, ItemInitializer, ItemDataSpec, _Index) ->
    atm_store_test_utils:to_iterated_item(
        ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, ItemInitializer, ItemDataSpec
    ).


%% @private
-spec randomly_remove_entity_referenced_by_item(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_data_spec:record()
) ->
    false | {true, errors:error()}.
randomly_remove_entity_referenced_by_item(AtmWorkflowExecutionAuth, Item, ItemDataSpec) ->
    atm_store_test_utils:randomly_remove_entity_referenced_by_item(
        ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, Item, ItemDataSpec
    ).


%% @private
-spec build_content_update_options(atm_list_store_content_update_options:update_function()) ->
    atm_list_store_content_update_options:record().
build_content_update_options(UpdateFun) ->
    #atm_list_store_content_update_options{function = UpdateFun}.


%% @private
-spec browse_content(atm_workflow_execution_auth:record(), atm_store:id()) ->
    [automation:item()].
browse_content(AtmWorkflowExecutionAuth, AtmStoreId) ->
    BrowseOpts = build_content_browse_options(#{<<"limit">> => 1000}),
    #atm_list_store_content_browse_result{
        items = Items,
        is_last = true
    } = ?rpc(?PROVIDER_SELECTOR, atm_store_api:browse_content(
        AtmWorkflowExecutionAuth, BrowseOpts, AtmStoreId
    )),
    lists:map(fun({_, {ok, Item}}) -> Item end, Items).


%% @private
-spec build_content_browse_options(json_utils:json_map()) ->
    atm_list_store_content_browse_options:record().
build_content_browse_options(OptsJson) ->
    atm_list_store_content_browse_options:sanitize(OptsJson#{
        <<"type">> => <<"listStoreContentBrowseOptions">>
    }).


%% @private
-spec build_content_browse_result([atm_store_container_infinite_log_backend:entry()], boolean()) ->
    atm_list_store_content_browse_result:record().
build_content_browse_result(Entries, IsLast) ->
    #atm_list_store_content_browse_result{items = Entries, is_last = IsLast}.
