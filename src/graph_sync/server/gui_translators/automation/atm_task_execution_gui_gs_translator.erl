%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% automation task execution entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_gui_gs_translator).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([translate_value/2, translate_resource/2]).


%%%===================================================================
%%% API
%%%===================================================================

-spec translate_value(gri:gri(), Value :: term()) -> gs_protocol:data().
translate_value(#gri{aspect = content}, BrowseResult) ->
    BrowseResult.


-spec translate_resource(gri:gri(), Data :: term()) -> gs_protocol:data().
translate_resource(#gri{aspect = instance, scope = private}, AtmTaskExecution) ->
    translate_atm_task_execution(AtmTaskExecution);

translate_resource(#gri{aspect = openfaas_function_activity_registry, scope = private}, ActivityRegistry) ->
    translate_openfaas_function_activity_registry(ActivityRegistry).


%%%===================================================================
%%% Util functions
%%%===================================================================


-spec translate_atm_task_execution(atm_task_execution:record()) -> gs_protocol:data().
translate_atm_task_execution(#atm_task_execution{
    workflow_execution_id = AtmWorkflowExecutionId,

    schema_id = AtmTaskSchemaId,

    system_audit_log_id = AtmTaskAuditLogId,

    status = AtmTaskExecutionStatus,

    items_in_processing = ItemsInProcessing,
    items_processed = ItemsProcessed,
    items_failed = ItemsFailed
}) ->
    #{
        <<"atmWorkflowExecution">> => gri:serialize(#gri{
            type = op_atm_workflow_execution, id = AtmWorkflowExecutionId,
            aspect = instance, scope = private
        }),
        <<"schemaId">> => AtmTaskSchemaId,

        <<"systemAuditLogId">> => utils:undefined_to_null(AtmTaskAuditLogId),

        <<"status">> => atom_to_binary(AtmTaskExecutionStatus, utf8),

        <<"itemsInProcessing">> => ItemsInProcessing,
        <<"itemsProcessed">> => ItemsProcessed,
        <<"itemsFailed">> => ItemsFailed
    }.


-spec translate_openfaas_function_activity_registry(atm_openfaas_function_activity_registry:record()) ->
    gs_protocol:data().
translate_openfaas_function_activity_registry(#atm_openfaas_function_activity_registry{
    pod_status_registry = PodStatusRegistry
}) ->
    #{
        <<"registry">> => jsonable_record:to_json(PodStatusRegistry, atm_openfaas_function_pod_status_registry)
    }.
