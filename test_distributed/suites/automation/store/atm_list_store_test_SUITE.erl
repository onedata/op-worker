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

-include("modules/automation/atm_execution.hrl").
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
    apply_operation_test/1,
    iterator_test/1,
    browse_by_index_test/1,
    browse_by_offset_test/1
]).

groups() -> [
    {infinite_log_based_stores_common_tests, [parallel], [
        create_test,
        apply_operation_test,
        iterator_test,
        browse_by_index_test,
        browse_by_offset_test
    ]}
].

all() -> [
    {group, infinite_log_based_stores_common_tests}
].


-define(PROVIDER_SELECTOR, krakow).


%%%===================================================================
%%% API functions
%%%===================================================================


create_test(_Config) ->
    atm_infinite_log_based_stores_test_base:create_test_base(#{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1
    }).


apply_operation_test(_Config) ->
    atm_infinite_log_based_stores_test_base:apply_operation_test_base(#{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1,
        input_item_to_exp_store_item => fun input_item_to_exp_store_item/3
    }).


iterator_test(_Config) ->
    atm_infinite_log_based_stores_test_base:iterator_test_base(#{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1,
        input_item_to_exp_store_item => fun input_item_to_exp_store_item/3,
        randomly_remove_entity_referenced_by_item => fun randomly_remove_entity_referenced_by_item/3
    }).


browse_by_index_test(_Config) ->
    atm_infinite_log_based_stores_test_base:browse_content_test_base(index, #{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1,
        input_item_to_exp_store_item => fun input_item_to_exp_store_item/3,
        randomly_remove_entity_referenced_by_item => fun randomly_remove_entity_referenced_by_item/3
    }).


browse_by_offset_test(_Config) ->
    atm_infinite_log_based_stores_test_base:browse_content_test_base(offset, #{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1,
        input_item_to_exp_store_item => fun input_item_to_exp_store_item/3,
        randomly_remove_entity_referenced_by_item => fun randomly_remove_entity_referenced_by_item/3
    }).


%===================================================================
% Helper functions
%===================================================================


%% @private
-spec example_configs() -> [atm_single_value_store_config:record()].
example_configs() ->
    lists:map(fun(ItemDataSpec) ->
        #atm_list_store_config{item_data_spec = ItemDataSpec}
    end, [
        %% TODO VFS-8686 enable after implementing compress/expand for array
%%        #atm_data_spec{type = atm_array_type},
        #atm_data_spec{type = atm_dataset_type},
        #atm_data_spec{type = atm_file_type},
        #atm_data_spec{type = atm_integer_type},
        #atm_data_spec{type = atm_object_type},
        #atm_data_spec{type = atm_string_type}
        #atm_data_spec{type = atm_time_series_measurements_type}
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
-spec input_item_to_exp_store_item(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_store:id()
) ->
    atm_value:expanded().
input_item_to_exp_store_item(AtmWorkflowExecutionAuth, ItemInitializer, ItemDataSpec) ->
    atm_store_test_utils:compress_and_expand_data(
        ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, ItemInitializer, ItemDataSpec
    ).


%% @private
-spec randomly_remove_entity_referenced_by_item(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_data_spec:record()
) ->
    false | {true, errors:error()}.
randomly_remove_entity_referenced_by_item(AtmWorkflowExecutionAuth, Item, ItemDataSpec) ->
    atm_store_test_utils:randomly_remove_entity_referenced_by_item(
        ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, Item, ItemDataSpec
    ).


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [?MODULE | atm_infinite_log_based_stores_test_base:modules_to_load()],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(infinite_log_based_stores_common_tests, Config) ->
    atm_infinite_log_based_stores_test_base:init_per_group(Config).


end_per_group(infinite_log_based_stores_common_tests, Config) ->
    atm_infinite_log_based_stores_test_base:end_per_group(Config).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 3}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
