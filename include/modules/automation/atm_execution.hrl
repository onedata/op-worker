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
-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/errors.hrl").


-record(atm_workflow_execution_summary, {
    atm_workflow_execution_id :: atm_workflow_execution:id(),

    name :: automation:name(),
    atm_inventory_id :: od_atm_inventory:id(),

    status :: atm_workflow_execution:status(),

    schedule_time :: atm_workflow_execution:timestamp(),
    start_time :: atm_workflow_execution:timestamp(),
    finish_time :: atm_workflow_execution:timestamp()
}).

-record(atm_workflow_execution_creation_ctx, {
    workflow_execution_id :: atm_workflow_execution:id(),
    workflow_execution_auth :: atm_workflow_execution_auth:record(),
    store_initial_values :: atm_workflow_execution_api:store_initial_values(),

    lambda_docs :: #{od_atm_lambda:id() => od_atm_lambda:doc()},
    workflow_schema_doc :: od_atm_workflow_schema:doc(),
    system_audit_log_schema :: atm_store_schema:record(),

    callback_url :: undefined | http_client:url()
}).

-record(atm_store_container_operation, {
    type :: atm_store_container:operation_type(),
    options :: atm_store_container:operation_options(),
    argument :: automation:item(),
    workflow_execution_auth :: atm_workflow_execution_auth:record()
}).

-record(atm_lane_execution_run, {
    run_no :: pos_integer(),
    status :: atm_workflow_execution:status(),

    iterated_store_id :: undefined | atm_store:id(),
    exception_store_id :: undefined | atm_store:id(),

    parallel_boxes :: [atm_parallel_box_execution:record()]
}).

-record(atm_lane_execution_rec, {
    schema_id :: automation:id(),
    runs :: [atm_lane_execution:run()]
}).


-define(WAITING_PHASE, waiting).
-define(ONGOING_PHASE, ongoing).
-define(ENDED_PHASE, ended).

-define(SCHEDULED_STATUS, scheduled).
-define(PREPARING_STATUS, preparing).
-define(ENQUEUED_STATUS, enqueued).
-define(PENDING_STATUS, pending).
-define(ACTIVE_STATUS, active).
-define(ABORTING_STATUS, aborting).
-define(FINISHED_STATUS, finished).
-define(CANCELLED_STATUS, cancelled).
-define(FAILED_STATUS, failed).
-define(SKIPPED_STATUS, skipped).


%% Atm logging related macros

-define(LOGGER_DEBUG, <<"debug">>).
-define(LOGGER_INFO, <<"info">>).
-define(LOGGER_NOTICE, <<"notice">>).
-define(LOGGER_WARNING, <<"warning">>).
-define(LOGGER_ALERT, <<"alert">>).
-define(LOGGER_ERROR, <<"error">>).
-define(LOGGER_CRITICAL, <<"critical">>).
-define(LOGGER_EMERGENCY, <<"emergency">>).

-endif.
