%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used by automation execution
%%% machinery.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ATM_EXECUTION_HRL).
-define(ATM_EXECUTION_HRL, 1).


-include("global_definitions.hrl").
-include("modules/automation/atm_logging.hrl").
-include("modules/automation/atm_openfaas.hrl").
-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").


-record(atm_workflow_execution_summary, {
    atm_workflow_execution_id :: atm_workflow_execution:id(),

    name :: automation:name(),
    atm_workflow_schema_revision_num :: atm_workflow_schema_revision:revision_number(),
    atm_inventory_id :: od_atm_inventory:id(),

    status :: atm_workflow_execution:status(),

    schedule_time :: atm_workflow_execution:timestamp(),
    start_time :: atm_workflow_execution:timestamp(),
    suspend_time :: atm_workflow_execution:timestamp(),
    finish_time :: atm_workflow_execution:timestamp()
}).

-record(atm_lane_execution, {
    schema_id :: automation:id(),
    retries_left :: non_neg_integer(),
    % runs are kept in reverse order of their execution meaning that the head
    % of the list is always the newest run. This simplifies runs management
    % (only newest run is ever modified) and shows most relevant entries first.
    runs :: [atm_lane_execution:run()]
}).

-record(atm_lane_execution_run, {
    % run_num is set right before execution of concrete lane run starts. Until
    % then it is left 'undefined' (e.g lane runs being prepared in advance) as
    % it is not possible to predict what it will be (due to possible automatic
    % retries of previous lane runs)
    run_num :: undefined | pos_integer(),
    % origin_run_num is set only if this lane run is a retry of a previous
    % (origin) lane run
    origin_run_num = undefined :: undefined | pos_integer(),

    status :: atm_lane_execution:run_status(),
    % Flag used to differentiate reasons why lane execution run is aborting
    stopping_reason = undefined :: undefined | atm_lane_execution:run_stopping_reason(),

    iterated_store_id = undefined :: undefined | atm_store:id(),
    exception_store_id = undefined :: undefined | atm_store:id(),

    parallel_boxes = [] :: [atm_parallel_box_execution:record()]
}).

% Record used only during creation of atm lane execution run (it is not persisted anywhere)
-record(atm_lane_execution_run_creation_args, {
    lane_run_selector :: atm_lane_execution:lane_run_selector(),

    type :: atm_lane_execution:run_type(),
    workflow_execution_ctx :: atm_workflow_execution_ctx:record(),
    workflow_execution_doc :: atm_workflow_execution:doc(),

    lane_index :: pos_integer(),
    lane_schema :: atm_lane_schema:record(),
    origin_run :: undefined | atm_lane_execution:run(),

    iterated_store_id :: atm_store:id()
}).

% Record used only during creation of atm parallel box execution (it is not persisted anywhere)
-record(atm_parallel_box_execution_creation_args, {
    lane_execution_run_creation_args :: atm_lane_execution_factory:run_creation_args(),

    parallel_box_index :: pos_integer(),
    parallel_box_schema :: atm_parallel_box_schema:record()
}).

-record(atm_task_executor_creation_args, {
    workflow_execution_ctx :: atm_workflow_execution_ctx:record(),
    lane_execution_index :: atm_lane_execution:index(),
    task_id :: atm_task_execution:id(),
    task_schema :: atm_task_schema:record(),
    lambda_revision :: atm_lambda_revision:record()
}).

-record(atm_task_executor_initiation_ctx, {
    workflow_execution_ctx :: atm_workflow_execution_ctx:record(),
    task_execution_id :: atm_task_execution:id(),
    task_schema :: atm_task_schema:record(),
    lambda_revision :: atm_lambda_revision:record(),
    uncorrelated_results :: [automation:name()]
}).

%% Record used as an argument for lambda call
-record(atm_lambda_input, {
    workflow_execution_id :: atm_workflow_execution:id(),
    log_level :: audit_log:entry_severity_int(),
    job_batch_id :: atm_task_executor:job_batch_id(),
    config :: json_utils:json_map(),
    args_batch :: [atm_task_executor:job_args()]
}).

%% Record used as an return value from lambda call
-record(atm_lambda_output, {
    results_batch :: undefined | [undefined | atm_task_executor:job_results()]
}).

-record(atm_item_execution, {
    trace_id :: binary(),
    value :: automation:item()
}).

%% Atm data types related macros

-define(ATM_ARRAY_DATA_SPEC(__ITEM_DATA_TYPE_SPEC), #atm_array_data_spec{
    item_data_spec = __ITEM_DATA_TYPE_SPEC
}).


%% Atm stores related macros

-record(atm_system_store_schema, {
    id :: automation:name(),
    name :: automation:name(),
    type :: atm_store:type(),
    config :: atm_store:config()
}).

% Record used only during creation of atm store container (it is not persisted anywhere)
-record(atm_store_container_creation_args, {
    workflow_execution_auth :: atm_workflow_execution_auth:record(),
    log_level :: audit_log:entry_severity_int(),
    store_config :: atm_store:config(),
    initial_content :: atm_store_container:initial_content()
}).

-define(ATM_SYSTEM_AUDIT_LOG_STORE_SCHEMA(__ID), #atm_system_store_schema{
    id = __ID,
    name = __ID,
    type = audit_log,
    config = #atm_audit_log_store_config{
        log_content_data_spec = #atm_object_data_spec{}
    }
}).

-define(ATM_TASK_TIME_SERIES_STORE_SCHEMA(__CONFIG), #atm_store_schema{
    id = ?CURRENT_TASK_TIME_SERIES_STORE_SCHEMA_ID,
    name = ?CURRENT_TASK_TIME_SERIES_STORE_SCHEMA_ID,
    description = <<>>,
    type = time_series,
    config = __CONFIG,
    requires_initial_content = false
}).

-define(CURRENT_LANE_RUN_EXCEPTION_STORE_SCHEMA_ID, <<"CURRENT_LANE_RUN_EXCEPTION_STORE">>).

-record(atm_exception_store_config, {
    item_data_spec :: atm_data_spec:record()
}).

-define(ATM_LANE_RUN_EXCEPTION_STORE_SCHEMA(__ITEM_DATA_SPEC), #atm_system_store_schema{
    id = ?CURRENT_LANE_RUN_EXCEPTION_STORE_SCHEMA_ID,
    name = ?CURRENT_LANE_RUN_EXCEPTION_STORE_SCHEMA_ID,
    type = exception,
    config = #atm_exception_store_config{item_data_spec = __ITEM_DATA_SPEC}
}).

-record(atm_store_content_browse_req, {
    store_schema_id :: automation:id(),
    workflow_execution_auth :: atm_workflow_execution_auth:record(),
    options :: atm_store_content_browse_options:record()
}).

-record(atm_audit_log_store_content_browse_options, {
    browse_opts :: audit_log_browse_opts:opts()
}).

-record(atm_audit_log_store_content_browse_result, {
    result :: audit_log:browse_result()
}).

-record(atm_exception_store_content_browse_options, {
    listing_opts :: atm_store_container_infinite_log_backend:timestamp_agnostic_listing_opts()
}).

-record(atm_exception_store_content_browse_result, {
    items :: [atm_store_container_infinite_log_backend:entry()],
    is_last :: boolean()
}).

-record(atm_list_store_content_browse_options, {
    listing_opts :: atm_store_container_infinite_log_backend:timestamp_agnostic_listing_opts()
}).

-record(atm_list_store_content_browse_result, {
    items :: [atm_store_container_infinite_log_backend:entry()],
    is_last :: boolean()
}).

-record(atm_range_store_content_browse_options, {}).

-record(atm_range_store_content_browse_result, {
    range :: atm_range_value:range_json()
}).

-record(atm_single_value_store_content_browse_options, {}).

-record(atm_single_value_store_content_browse_result, {
    item :: {ok, automation:item()} | errors:error()
}).

-record(atm_time_series_store_content_browse_options, {
    request :: ts_browse_request:record()
}).

-record(atm_time_series_store_content_browse_result, {
    result :: ts_browse_result:record()
}).

-record(atm_tree_forest_store_content_browse_options, {
    listing_opts :: atm_store_container_infinite_log_backend:timestamp_agnostic_listing_opts()
}).

-record(atm_tree_forest_store_content_browse_result, {
    tree_roots :: [atm_store_container_infinite_log_backend:entry()],
    is_last :: boolean()
}).

-record(atm_store_content_update_req, {
    workflow_execution_auth :: atm_workflow_execution_auth:record(),
    argument ::
        automation:item() |
        audit_log:append_request() |
        % for exception store
        atm_workflow_execution_handler:item() |
        [atm_workflow_execution_handler:item()],
    options :: atm_store:content_update_options()
}).

-record(atm_exception_store_content_update_options, {
    function :: append | extend
}).


%% Atm status and phase related macros

-define(WAITING_PHASE, waiting).
-define(ONGOING_PHASE, ongoing).
-define(SUSPENDED_PHASE, suspended).
-define(ENDED_PHASE, ended).

-define(RESUMING_STATUS, resuming).
-define(SCHEDULED_STATUS, scheduled).
-define(PREPARING_STATUS, preparing).
-define(ENQUEUED_STATUS, enqueued).
-define(PENDING_STATUS, pending).
-define(ACTIVE_STATUS, active).
-define(STOPPING_STATUS, stopping).
-define(FINISHED_STATUS, finished).
-define(CANCELLED_STATUS, cancelled).
-define(FAILED_STATUS, failed).
-define(INTERRUPTED_STATUS, interrupted).
-define(PAUSED_STATUS, paused).
-define(CRASHED_STATUS, crashed).
-define(SKIPPED_STATUS, skipped).


-define(ATM_SUPERVISION_WORKER_SUP, atm_supervision_worker_sup).

-define(ATM_WARDEN_SERVICE_NAME, <<"atm_warden_service">>).
-define(ATM_WARDEN_SERVICE_ID, datastore_key:new_from_digest(?ATM_WARDEN_SERVICE_NAME)).


-endif.
