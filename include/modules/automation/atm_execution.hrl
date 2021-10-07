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

-record(atm_store_container_operation, {
    type :: atm_store_container:operation_type(),
    options :: atm_store_container:operation_options(),
    argument :: automation:item(),
    workflow_execution_auth :: atm_workflow_execution_auth:record()
}).

-record(atm_lane_execution, {
    schema_id :: automation:id(),
    runs :: [atm_lane_execution:run()]
}).

-record(atm_lane_execution_run_elements, {
    exception_store_id = undefined :: undefined | atm_store:id(),
    parallel_boxes = undefined :: undefined | [atm_parallel_box_execution:record()]
}).

-record(atm_lane_execution_run_create_ctx, {
    workflow_execution_ctx :: atm_workflow_execution_ctx:record(),

    workflow_schema_snapshot_doc :: atm_workflow_schema_snapshot:doc(),
    workflow_execution_doc :: atm_workflow_execution:doc(),

    lane_index :: pos_integer(),
    lane_schema :: atm_lane_schema:record(),
    iterated_store_id :: atm_store:id(),

    elements :: atm_lane_execution:run_elements()
}).

-record(atm_lane_execution_run, {
    run_no :: undefined | pos_integer(),
    src_run_no = undefined :: undefined | pos_integer(),

    status :: atm_lane_execution:status(),
    % Flag used to differentiate reasons why lane execution run is aborting
    aborting_reason = undefined :: undefined | cancel | failure,

    iterated_store_id = undefined :: undefined | atm_store:id(),
    exception_store_id = undefined :: undefined | atm_store:id(),

    parallel_boxes = [] :: [atm_parallel_box_execution:record()]
}).

-record(atm_parallel_box_execution_create_ctx, {
    lane_execution_run_create_ctx :: atm_lane_execution_factory:create_run_ctx(),

    parallel_box_index :: pos_integer(),
    parallel_box_schema :: atm_parallel_box_schema:record(),

    tasks :: [atm_task_execution:doc()]
}).


%% Atm system stores related macros

-define(CURRENT_LANE_EXCEPTION_STORE_SCHEMA_ID, <<"CURRENT_LANE_EXCEPTION_STORE">>).


%% Atm status and phase related macros

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
-define(INTERRUPTED_STATUS, interrupted).
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
