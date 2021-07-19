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


-include("modules/automation/atm_tmp.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/automation/automation.hrl").


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
    workflow_execution_ctx :: atm_workflow_execution_ctx:record(),
    workflow_schema_doc :: od_atm_workflow_schema:doc(),
    lambda_docs :: #{od_atm_lambda:id() => od_atm_lambda:doc()},
    store_initial_values :: atm_workflow_execution_api:store_initial_values(),
    callback_url :: undefined | http_client:url()
}).

-record(atm_store_container_operation, {
    type :: atm_store_container:operation_type(),
    options :: atm_store_container:operation_options(),
    value :: automation:item(),
    workflow_execution_ctx :: atm_workflow_execution_ctx:record()
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


-endif.
