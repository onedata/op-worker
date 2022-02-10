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
-include("modules/logical_file_manager/lfm.hrl").
-include("test_rpc.hrl").

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
    {all_tests, [parallel], [
        create_test,
        apply_operation_test,
        iterator_test,
        browse_by_index_test,
        browse_by_offset_test
    ]}
].

all() -> [
    {group, all_tests}
].


-define(PROVIDER_SELECTOR, krakow).
-define(rpc(Expr), ?rpc(?PROVIDER_SELECTOR, Expr)).


%%%===================================================================
%%% API functions
%%%===================================================================


create_test(_Config) ->
    atm_infinite_log_based_stores_test_base:create_test_base(#{
        store_configs => example_configs(),
        get_item_initializer_data_spec_fun => fun get_item_initializer_data_spec/1,
        prepare_item_initializer_fun => fun prepare_item_initializer/1
    }).


apply_operation_test(_Config) ->
    atm_infinite_log_based_stores_test_base:apply_operation_test_base(#{
        store_configs => example_configs(),
        get_item_initializer_data_spec_fun => fun get_item_initializer_data_spec/1,
        prepare_item_initializer_fun => fun prepare_item_initializer/1,
        prepare_item_fun => fun ensure_fully_expanded_data/3
    }).


iterator_test(_Config) ->
    atm_infinite_log_based_stores_test_base:iterator_test_base(#{
        store_configs => example_configs(),
        get_item_initializer_data_spec_fun => fun get_item_initializer_data_spec/1,
        prepare_item_initializer_fun => fun prepare_item_initializer/1,
        prepare_item_fun => fun ensure_fully_expanded_data/3,
        randomly_remove_item_fun => fun randomly_remove_item/3
    }).


browse_by_index_test(_Config) ->
    atm_infinite_log_based_stores_test_base:browse_content_test_base(index, #{
        store_configs => example_configs(),
        get_item_initializer_data_spec_fun => fun get_item_initializer_data_spec/1,
        prepare_item_initializer_fun => fun prepare_item_initializer/1,
        prepare_item_fun => fun ensure_fully_expanded_data/3,
        randomly_remove_item_fun => fun randomly_remove_item/3
    }).


browse_by_offset_test(_Config) ->
    atm_infinite_log_based_stores_test_base:browse_content_test_base(offset, #{
        store_configs => example_configs(),
        get_item_initializer_data_spec_fun => fun get_item_initializer_data_spec/1,
        prepare_item_initializer_fun => fun prepare_item_initializer/1,
        prepare_item_fun => fun ensure_fully_expanded_data/3,
        randomly_remove_item_fun => fun randomly_remove_item/3
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
-spec get_item_initializer_data_spec(atm_list_store_config:record()) ->
    atm_data_spec:record().
get_item_initializer_data_spec(#atm_list_store_config{item_data_spec = ItemDataSpec}) ->
    ItemDataSpec.


%% @private
-spec prepare_item_initializer(automation:item()) -> automation:item().
prepare_item_initializer(Item) -> Item.


%% @private
-spec ensure_fully_expanded_data(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_store:id()
) ->
    atm_value:expanded().
ensure_fully_expanded_data(AtmWorkflowExecutionAuth, Item, ItemDataSpec) ->
    atm_store_test_utils:ensure_fully_expanded_data(
        ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, Item, ItemDataSpec
    ).


%% @private
-spec randomly_remove_item(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_data_spec:record()
) ->
    boolean().
randomly_remove_item(AtmWorkflowExecutionAuth, #{<<"file_id">> := ObjectId}, #atm_data_spec{
    type = atm_file_type
}) ->
    case rand:uniform(5) of
        1 ->
            SessionId = atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth),
            {ok, FileGuid} = file_id:objectid_to_guid(ObjectId),
            ?rpc(lfm:rm_recursive(SessionId, ?FILE_REF(FileGuid))),
            true;
        _ ->
            false
    end;

randomly_remove_item(AtmWorkflowExecutionAuth, #{<<"datasetId">> := DatasetId}, #atm_data_spec{
    type = atm_dataset_type
}) ->
    case rand:uniform(5) of
        1 ->
            SessionId = atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth),
            ?rpc(mi_datasets:remove(SessionId, DatasetId)),
            true;
        _ ->
            false
    end;

randomly_remove_item(_AtmWorkflowExecutionAuth, _Item, _AtmDataSpec) ->
    false.


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [?MODULE, atm_store_test_utils, atm_infinite_log_based_stores_test_base],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(_Group, Config) ->
    time_test_utils:freeze_time(Config),
    Config.


end_per_group(_Group, Config) ->
    time_test_utils:unfreeze_time(Config).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
