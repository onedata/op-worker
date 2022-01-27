%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of records and macros used in automation
%%% workflow execution CT tests.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ATM_WORKFLOW_EXECUTION_TEST_RUNNER_HRL).
-define(ATM_WORKFLOW_EXECUTION_TEST_RUNNER_HRL, 1).


-include_lib("ctool/include/automation/automation.hrl").


-record(atm_hook_call_ctx, {
    workflow_execution_id :: atm_workflow_execution:id(),
    workflow_execution_test_view :: atm_workflow_execution_test_view:view(),
    current_lane_index :: atm_lane_execution:index(),
    current_run_num :: atm_lane_execution:run_num(),
    call_args :: [term()]
}).

-record(atm_lane_run_execution_test_spec, {
    selector :: atm_lane_execution:lane_run_selector(),

    pre_prepare_lane_hook = default :: default | atm_workflow_execution_test_runner:hook(),
    pre_create_run_hook = default :: default | atm_workflow_execution_test_runner:hook(),
    post_create_run_hook = default :: default | atm_workflow_execution_test_runner:hook(),
    post_prepare_lane_hook = default :: default | atm_workflow_execution_test_runner:hook(),

    pre_process_item_hook = default :: default | atm_workflow_execution_test_runner:hook(),
    post_process_item_hook = default :: default | atm_workflow_execution_test_runner:hook(),

    pre_process_result_hook = default :: default | atm_workflow_execution_test_runner:hook(),
    post_process_result_hook = default :: default | atm_workflow_execution_test_runner:hook(),

    pre_report_item_error_hook = default :: default | atm_workflow_execution_test_runner:hook(),
    post_report_item_error_hook = default :: default | atm_workflow_execution_test_runner:hook(),

    pre_handle_task_execution_ended_hook = default :: default | atm_workflow_execution_test_runner:hook(),
    post_handle_task_execution_ended_hook = default :: default | atm_workflow_execution_test_runner:hook(),

    pre_handle_lane_execution_ended_hook = default :: default | atm_workflow_execution_test_runner:hook(),
    post_handle_lane_execution_ended_hook = default :: default | atm_workflow_execution_test_runner:hook()
}).

-record(atm_workflow_execution_incarnation_test_spec, {
    lane_runs :: [atm_workflow_execution_test_runner:lane_run_test_spec()],
    pre_handle_workflow_execution_ended_hook = default :: default | atm_workflow_execution_test_runner:hook(),
    post_handle_workflow_execution_ended_hook = default :: default | atm_workflow_execution_test_runner:hook()
}).

-record(atm_workflow_execution_test_spec, {
    provider :: oct_background:entity_selector(),
    user :: oct_background:entity_selector(),
    space :: oct_background:entity_selector(),

    workflow_schema_alias :: atm_test_inventory:atm_workflow_schema_alias(),
    workflow_schema_revision_num :: atm_workflow_schema_revision:revision_number(),

    store_initial_values = #{} :: atm_workflow_execution_api:store_initial_values(),
    callback_url = undefined :: undefined | http_client:url(),

    incarnations :: [atm_workflow_execution_test_runner:incarnation_test_spec()]
}).


-define(JSON_PATH(__QUERY_BIN), binary:split(__QUERY_BIN, <<".">>, [global])).


-endif.
