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


-record(atm_workflow_execution_creation_ctx, {
    workflow_execution_ctx :: atm_workflow_execution_ctx:record(),
    workflow_schema_doc :: od_atm_workflow_schema:doc(),
    lambda_docs :: #{od_atm_lambda:id() => od_atm_lambda:doc()},
    initial_values :: atm_api:initial_values()
}).

-record(atm_container_operation, {
    type :: atm_container:operation_type(),
    options :: atm_container:operation_options(),
    value :: atm_api:item(),
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
-define(FINISHED_STATUS, finished).
-define(FAILED_STATUS, failed).


-define(check_atm(__FunctionCall), atm_middleware_utils:check_result(__FunctionCall)).

-endif.
