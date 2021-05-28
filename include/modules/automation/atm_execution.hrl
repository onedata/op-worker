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


-record(atm_workflow_execution_ctx, {
    space_id :: od_space:id(),
    workflow_execution_id :: atm_workflow_execution:id(),
    session_id :: session:id()
}).

-record(atm_workflow_execution_creation_ctx, {
    workflow_execution_ctx :: atm_workflow_execution_ctx:record(),
    workflow_schema_doc :: od_atm_workflow_schema:doc(),
    initial_values :: atm_api:initial_values()
}).

-record(atm_workflow_execution_env, {
    space_id :: od_space:id(),
    workflow_execution_id :: atm_workflow_execution:id(),
    store_registry :: atm_workflow_execution:store_registry()
}).

-record(atm_container_operation, {
    type :: atm_container:operation_type(),
    options :: atm_container:operation_options(),
    value :: atm_api:item(),
    workflow_execution_ctx :: atm_workflow_execution_ctx:record()
}).

-record(atm_lane_execution, {
    schema_id :: automation:id(),
    status :: atm_lane_execution:status(),
    parallel_boxes :: [atm_parallel_box_execution:record()]
}).

-record(atm_parallel_box_execution, {
    schema_id :: automation:id(),
    status :: atm_parallel_box_execution:status(),
    tasks :: #{atm_task_execution:id() => atm_task_execution:status()}
}).

-record(atm_task_execution_ctx, {
    workflow_execution_env :: atm_workflow_execution_env:record(),
    workflow_execution_ctx :: atm_workflow_execution_ctx:record(),
    item :: json_utils:json_term()
}).

-record(atm_task_execution_argument_spec, {
    name :: automation:name(),
    value_builder :: atm_task_argument_value_builder:record(),
    data_spec :: atm_data_spec:record(),
    is_batch :: boolean()
}).


-define(WAITING_PHASE, waiting).
-define(ONGOING_PHASE, ongoing).
-define(ENDED_PHASE, ended).

-define(WAITING_TREE, <<"waiting">>).
-define(ONGOING_TREE, <<"ongoing">>).
-define(ENDED_TREE, <<"ended">>).

-define(SCHEDULED_STATUS, scheduled).
-define(PREPARING_STATUS, preparing).
-define(ENQUEUED_STATUS, enqueued).
-define(PENDING_STATUS, pending).
-define(ACTIVE_STATUS, active).
-define(FINISHED_STATUS, finished).
-define(FAILED_STATUS, failed).


-endif.
