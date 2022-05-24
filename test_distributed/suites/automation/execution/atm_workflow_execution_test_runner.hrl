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


-record(atm_mock_call_ctx, {
    provider :: oct_background:entity_selector(),
    space :: oct_background:entity_selector(),
    session_id :: session:id(),
    workflow_execution_id :: atm_workflow_execution:id(),
    workflow_execution_exp_state :: atm_workflow_execution_exp_state_builder:exp_state(),
    lane_count :: non_neg_integer(),
    current_lane_index :: atm_lane_execution:index(),
    current_run_num :: atm_lane_execution:run_num(),
    call_args :: [term()]
}).

-record(atm_step_mock_spec, {
    % can be used to block atm execution process until other step phase is executed
    % and as such enforce specific order of events in parallel execution environment
    defer_after = undefined :: undefined | atm_workflow_execution_test_runner:step_phase_selector(),

    before_step_hook = undefined :: undefined | atm_workflow_execution_test_runner:hook(),
    before_step_exp_state_diff = default ::
        no_diff |
        % changes that would happen in case of 'happy path' that is if no execution error occurred
        default |
        % changes defined by test author
        atm_workflow_execution_test_runner:exp_state_diff(),

    strategy = passthrough :: atm_workflow_execution_test_runner:mock_strategy(),

    % below checks will not be executed in case of mock_result = {true, _}
    % (step has not been executed and as such no change compared to before_step_* should occur)
    after_step_hook = undefined :: undefined | atm_workflow_execution_test_runner:hook(),
    after_step_exp_state_diff = default :: default | atm_workflow_execution_test_runner:exp_state_diff()
}).

-record(atm_lane_run_execution_test_spec, {
    selector :: atm_lane_execution:lane_run_selector(),

    prepare_lane = #atm_step_mock_spec{} :: atm_workflow_execution_test_runner:step_mock_spec(),
    create_run = #atm_step_mock_spec{} :: atm_workflow_execution_test_runner:step_mock_spec(),

    run_task_for_item = #atm_step_mock_spec{} :: atm_workflow_execution_test_runner:step_mock_spec(),
    process_task_result_for_item = #atm_step_mock_spec{} :: atm_workflow_execution_test_runner:step_mock_spec(),
    report_item_error = #atm_step_mock_spec{} :: atm_workflow_execution_test_runner:step_mock_spec(),

    handle_task_execution_ended = #atm_step_mock_spec{} :: atm_workflow_execution_test_runner:step_mock_spec(),
    handle_lane_execution_ended = #atm_step_mock_spec{} :: atm_workflow_execution_test_runner:step_mock_spec()
}).

-record(atm_workflow_execution_incarnation_test_spec, {
    incarnation_num :: atm_workflow_execution:incarnation(),
    lane_runs :: [atm_workflow_execution_test_runner:lane_run_test_spec()],
    handle_workflow_execution_ended = #atm_step_mock_spec{} :: atm_workflow_execution_test_runner:step_mock_spec()
}).

-record(atm_workflow_execution_test_spec, {
    provider :: oct_background:entity_selector(),
    user :: oct_background:entity_selector(),
    space :: oct_background:entity_selector(),

    workflow_schema_dump_or_draft ::
        atm_test_inventory:atm_workflow_schema_dump() |
        atm_test_schema_factory:atm_workflow_schema_dump_draft(),
    workflow_schema_revision_num :: atm_workflow_schema_revision:revision_number(),

    store_initial_content_overlay = #{} :: atm_workflow_execution_api:store_initial_content_overlay(),
    callback_url = undefined :: undefined | http_client:url(),

    incarnations :: [atm_workflow_execution_test_runner:incarnation_test_spec()]
}).


-define(ATM_WORKFLOW_EXECUTION_TEST_UTILS, [
    atm_openfaas_docker_mock,
    atm_openfaas_task_executor_mock,
    atm_test_inventory,
    atm_workflow_execution_test_runner
]).


-endif.
