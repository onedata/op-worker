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
-include_lib("ctool/include/logging.hrl").


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
    retries_left :: non_neg_integer(),
    runs :: [atm_lane_execution:run()]
}).

-record(atm_lane_execution_run, {
    run_num :: undefined | pos_integer(),
    origin_run_num = undefined :: undefined | pos_integer(),

    status :: atm_lane_execution:status(),
    % Flag used to differentiate reasons why lane execution run is aborting
    aborting_reason = undefined :: undefined | cancel | failure,

    iterated_store_id = undefined :: undefined | atm_store:id(),
    exception_store_id = undefined :: undefined | atm_store:id(),

    parallel_boxes = [] :: [atm_parallel_box_execution:record()]
}).

% Record used only during creation of atm lane execution run (it is not persisted anywhere)
-record(atm_lane_execution_run_creation_args, {
    workflow_execution_ctx :: atm_workflow_execution_ctx:record(),
    workflow_execution_doc :: atm_workflow_execution:doc(),

    lane_index :: pos_integer(),
    lane_schema :: atm_lane_schema:record(),
    iterated_store_id :: atm_store:id()
}).

% Record used only during teardown of atm lane execution run (it is not persisted anywhere)
-record(atm_lane_execution_run_teardown_ctx, {
    workflow_execution_ctx :: atm_workflow_execution_ctx:record(),
    is_retried_scheduled :: boolean()
}).

% Record used only during creation of atm parallel box execution (it is not persisted anywhere)
-record(atm_parallel_box_execution_creation_args, {
    lane_execution_run_creation_args :: atm_lane_execution_factory:run_creation_args(),

    parallel_box_index :: pos_integer(),
    parallel_box_schema :: atm_parallel_box_schema:record()
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


-define(atm_examine_error(__TYPE, __REASON, __STACKTRACE),
    case __TYPE of
        throw ->
            __REASON;
        _ ->
            __ERROR_REF = str_utils:rand_hex(5),

            ?error_stacktrace(
                "[~p:~p] Unexpected error (ref. ~s): ~p:~p",
                [?MODULE, ?FUNCTION_NAME, __ERROR_REF, __TYPE, __REASON],
                __STACKTRACE
            ),
            ?ERROR_UNEXPECTED_ERROR(__ERROR_REF)
    end
).


-endif.
