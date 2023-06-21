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
-include_lib("cluster_worker/include/audit_log.hrl").
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
    type :: regular | rerun | retry,
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


% Record carrying an activity report of an OpenFaaS function
-record(atm_openfaas_activity_report, {
    type :: atm_openfaas_activity_report:type(),
    batch :: [atm_openfaas_activity_report:body()]
}).

% Record carrying a status report of a pod that executes given OpenFaaS function
% (currently the only possible type of OpenFaaS function activity report), used
% to build atm_openfaas_function_pod_status_summary
-record(atm_openfaas_function_pod_status_report, {
    function_id :: atm_openfaas_task_executor:function_id(),
    pod_id :: atm_openfaas_function_pod_status_registry:pod_id(),

    pod_status :: atm_openfaas_function_pod_status_report:pod_status(),
    containers_readiness :: atm_openfaas_function_pod_status_report:containers_readiness(),

    event_timestamp :: atm_openfaas_function_pod_status_report:event_timestamp(),
    event_type :: atm_openfaas_function_pod_status_report:event_type(),
    event_reason :: atm_openfaas_function_pod_status_report:event_reason(),
    event_message :: atm_openfaas_function_pod_status_report:event_message()
}).

% Record holding the summary of status changes for a single pod of an OpenFaaS function
% (single entry in the atm_openfaas_function_pod_status_registry)
-record(atm_openfaas_function_pod_status_summary, {
    current_status :: atm_openfaas_function_pod_status_report:pod_status(),
    current_containers_readiness :: atm_openfaas_function_pod_status_report:containers_readiness(),
    last_status_change_timestamp :: atm_openfaas_function_pod_status_report:event_timestamp(),
    event_log_id :: infinite_log:log_id()
}).

% Record carrying a generic result streamer report
-record(atm_openfaas_result_streamer_report, {
    id :: atm_openfaas_result_streamer_report:id(),
    body :: atm_openfaas_result_streamer_report:body()
}).

% Record carrying a status report of a lambda result streamer of type 'registration'
-record(atm_openfaas_result_streamer_registration_report, {
    workflow_execution_id :: atm_workflow_execution:id(),
    task_execution_id :: atm_task_execution:id(),
    result_streamer_id :: atm_openfaas_result_streamer_registry:result_streamer_id()
}).

% Record carrying a status report of a lambda result streamer of type 'chunk'
-record(atm_openfaas_result_streamer_chunk_report, {
    chunk :: atm_openfaas_result_streamer_chunk_report:chunk()
}).

% Record carrying a status report of a lambda result streamer of type 'invalidData'
-record(atm_openfaas_result_streamer_invalid_data_report, {
    result_name :: automation:name(),
    base_64_encoded_data :: binary()
}).

% Record carrying a status report of a lambda result streamer of type 'deregistration'
-record(atm_openfaas_result_streamer_deregistration_report, {
}).

% Record expressing the push message sent to lambda result streamers to
% acknowledge that a result streamer report has been processed
-record(atm_openfaas_result_streamer_report_ack, {
    id :: atm_openfaas_result_streamer_report:id()
}).

% Record expressing the push message sent to lambda result streamers to
% cue their finalization (flushing of all results and deregistering)
-record(atm_openfaas_result_streamer_finalization_signal, {
}).

%% Atm data types related macros

-define(ATM_ARRAY_DATA_SPEC(__ITEM_DATA_TYPE_SPEC), #atm_array_data_spec{
    item_data_spec = __ITEM_DATA_TYPE_SPEC
}).


%% Atm stores related macros

% Record used only during creation of atm store container (it is not persisted anywhere)
-record(atm_store_container_creation_args, {
    workflow_execution_auth :: atm_workflow_execution_auth:record(),
    log_level :: audit_log:entry_severity_int(),
    store_config :: atm_store_config:record(),
    initial_content :: atm_store_container:initial_content()
}).

-define(ATM_SYSTEM_AUDIT_LOG_STORE_SCHEMA(__ID), #atm_store_schema{
    id = __ID,
    name = __ID,
    description = <<>>,
    type = audit_log,
    config = #atm_audit_log_store_config{
        log_content_data_spec = #atm_object_data_spec{}
    },
    requires_initial_content = false
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

-define(ATM_LANE_RUN_EXCEPTION_STORE_SCHEMA(__ITEM_DATA_SPEC), #atm_store_schema{
    id = ?CURRENT_LANE_RUN_EXCEPTION_STORE_SCHEMA_ID,
    name = ?CURRENT_LANE_RUN_EXCEPTION_STORE_SCHEMA_ID,
    description = <<>>,
    type = list,
    config = #atm_list_store_config{item_data_spec = __ITEM_DATA_SPEC},
    requires_initial_content = false
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
    argument :: automation:item() | audit_log:append_request(),
    options :: atm_store_content_update_options:record()
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


%% Atm logging related macros

-define(atm_task_system_log(__LOG_CONTENT, __LOG_SEVERITY, __LOG_LEVEL, __LOGGER),
    case atm_workflow_execution_logger:should_log(__LOGGER, __LOG_LEVEL) of
        true ->
            atm_workflow_execution_logger:task_append_system_log(__LOG_CONTENT, __LOG_SEVERITY, __LOGGER);
        false ->
            ok
    end
).

-define(atm_task_debug(__LOG_CONTENT, __LOGGER), ?atm_task_system_log(
    __LOG_CONTENT, ?DEBUG_AUDIT_LOG_SEVERITY, ?DEBUG_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_task_debug(__FORMAT, __ARGS, __LOGGER), ?atm_task_debug(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_task_info(__LOG_CONTENT, __LOGGER), ?atm_task_system_log(
    __LOG_CONTENT, ?INFO_AUDIT_LOG_SEVERITY, ?INFO_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_task_info(__FORMAT, __ARGS, __LOGGER), ?atm_task_info(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_task_notice(__LOG_CONTENT, __LOGGER), ?atm_task_system_log(
    __LOG_CONTENT, ?NOTICE_AUDIT_LOG_SEVERITY, ?NOTICE_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_task_notice(__FORMAT, __ARGS, __LOGGER), ?atm_task_notice(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_task_warning(__LOG_CONTENT, __LOGGER), ?atm_task_system_log(
    __LOG_CONTENT, ?WARNING_AUDIT_LOG_SEVERITY, ?WARNING_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_task_warning(__FORMAT, __ARGS, __LOGGER), ?atm_task_warning(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_task_error(__LOG_CONTENT, __LOGGER), ?atm_task_system_log(
    __LOG_CONTENT, ?ERROR_AUDIT_LOG_SEVERITY, ?ERROR_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_task_error(__FORMAT, __ARGS, __LOGGER), ?atm_task_error(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_task_critical(__LOG_CONTENT, __LOGGER), ?atm_task_system_log(
    __LOG_CONTENT, ?CRITICAL_AUDIT_LOG_SEVERITY, ?CRITICAL_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_task_critical(__FORMAT, __ARGS, __LOGGER), ?atm_task_critical(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_task_alert(__LOG_CONTENT, __LOGGER), ?atm_task_system_log(
    __LOG_CONTENT, ?ALERT_AUDIT_LOG_SEVERITY, ?ALERT_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_task_alert(__FORMAT, __ARGS, __LOGGER), ?atm_task_alert(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_task_emergency(__LOG_CONTENT, __LOGGER), ?atm_task_system_log(
    __LOG_CONTENT, ?EMERGENCY_AUDIT_LOG_SEVERITY, ?EMERGENCY_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_task_emergency(__FORMAT, __ARGS, __LOGGER), ?atm_task_emergency(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_workflow_system_log(__LOG_CONTENT, __LOG_SEVERITY, __LOG_LEVEL, __LOGGER),
    case atm_workflow_execution_logger:should_log(__LOGGER, __LOG_LEVEL) of
        true ->
            atm_workflow_execution_logger:workflow_append_system_log(__LOG_CONTENT, __LOG_SEVERITY, __LOGGER);
        false ->
            ok
    end
).

-define(atm_workflow_debug(__LOG_CONTENT, __LOGGER), ?atm_workflow_system_log(
    __LOG_CONTENT, ?DEBUG_AUDIT_LOG_SEVERITY, ?DEBUG_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_workflow_debug(__FORMAT, __ARGS, __LOGGER), ?atm_workflow_debug(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_workflow_info(__LOG_CONTENT, __LOGGER), ?atm_workflow_system_log(
    __LOG_CONTENT, ?INFO_AUDIT_LOG_SEVERITY, ?INFO_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_workflow_info(__FORMAT, __ARGS, __LOGGER), ?atm_workflow_info(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_workflow_notice(__LOG_CONTENT, __LOGGER), ?atm_workflow_system_log(
    __LOG_CONTENT, ?NOTICE_AUDIT_LOG_SEVERITY, ?NOTICE_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_workflow_notice(__FORMAT, __ARGS, __LOGGER), ?atm_workflow_notice(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_workflow_warning(__LOG_CONTENT, __LOGGER), ?atm_workflow_system_log(
    __LOG_CONTENT, ?WARNING_AUDIT_LOG_SEVERITY, ?WARNING_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_workflow_warning(__FORMAT, __ARGS, __LOGGER), ?atm_workflow_warning(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_workflow_error(__LOG_CONTENT, __LOGGER), ?atm_workflow_system_log(
    __LOG_CONTENT, ?ERROR_AUDIT_LOG_SEVERITY, ?ERROR_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_workflow_error(__FORMAT, __ARGS, __LOGGER), ?atm_workflow_error(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_workflow_critical(__LOG_CONTENT, __LOGGER), ?atm_workflow_system_log(
    __LOG_CONTENT, ?CRITICAL_AUDIT_LOG_SEVERITY, ?CRITICAL_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_workflow_critical(__FORMAT, __ARGS, __LOGGER), ?atm_workflow_critical(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_workflow_alert(__LOG_CONTENT, __LOGGER), ?atm_workflow_system_log(
    __LOG_CONTENT, ?ALERT_AUDIT_LOG_SEVERITY, ?ALERT_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_workflow_alert(__FORMAT, __ARGS, __LOGGER), ?atm_workflow_alert(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_workflow_emergency(__LOG_CONTENT, __LOGGER), ?atm_workflow_system_log(
    __LOG_CONTENT, ?EMERGENCY_AUDIT_LOG_SEVERITY, ?EMERGENCY_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_workflow_emergency(__FORMAT, __ARGS, __LOGGER), ?atm_workflow_emergency(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).


-define(ATM_SUPERVISION_WORKER_SUP, atm_supervision_worker_sup).

-define(ATM_WARDEN_SERVICE_NAME, <<"atm_warden_service">>).
-define(ATM_WARDEN_SERVICE_ID, datastore_key:new_from_digest(?ATM_WARDEN_SERVICE_NAME)).


-endif.
