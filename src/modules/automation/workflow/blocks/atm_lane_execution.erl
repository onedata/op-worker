%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module operating on automation lane executions.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lane_execution).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    create_all/1, create/3,
    prepare_all/1, prepare/1,
    delete_all/1, delete/1,

    get_parallel_box_execution_specs/1,

    gather_statuses/1,
    update_task_status/4
]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type status() :: atm_task_execution:status().
-type record() :: #atm_lane_execution{}.

-export_type([status/0, record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_all(atm_workflow_execution:creation_ctx()) -> [record()] | no_return().
create_all(#atm_workflow_execution_creation_ctx{
    workflow_schema_doc = #document{value = #od_atm_workflow_schema{
        lanes = AtmLaneSchemas
    }}
} = AtmWorkflowExecutionCreationCtx) ->
    lists:reverse(lists:foldl(fun({AtmLaneIndex, #atm_lane_schema{
        id = AtmLaneSchemaId
    } = AtmLaneSchema}, Acc) ->
        try
            AtmLaneExecution = create(AtmWorkflowExecutionCreationCtx, AtmLaneIndex, AtmLaneSchema),
            [AtmLaneExecution | Acc]
        catch _:Reason ->
            catch delete_all(Acc),
            throw(?ERROR_ATM_LANE_EXECUTION_CREATION_FAILED(AtmLaneSchemaId, Reason))
        end
    end, [], lists_utils:enumerate(AtmLaneSchemas))).


-spec create(
    atm_workflow_execution:creation_ctx(),
    non_neg_integer(),
    atm_lane_schema:record()
) ->
    record() | no_return().
create(AtmWorkflowExecutionCreationCtx, AtmLaneIndex, #atm_lane_schema{
    id = AtmLaneSchemaId,
    parallel_boxes = AtmParallelBoxSchemas
}) ->
    AtmParallelBoxExecutions = atm_parallel_box_execution:create_all(
        AtmWorkflowExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxSchemas
    ),

    #atm_lane_execution{
        schema_id = AtmLaneSchemaId,
        status = atm_status_utils:converge(atm_parallel_box_execution:gather_statuses(
            AtmParallelBoxExecutions
        )),
        parallel_boxes = AtmParallelBoxExecutions
    }.


-spec prepare_all([record()]) -> ok | no_return().
prepare_all(AtmLaneExecutions) ->
    lists:foreach(fun(#atm_lane_execution{schema_id = AtmLaneSchemaId} = AtmLaneExecution) ->
        try
            prepare(AtmLaneExecution)
        catch _:Reason ->
            throw(?ERROR_ATM_LANE_EXECUTION_PREPARATION_FAILED(AtmLaneSchemaId, Reason))
        end
    end, AtmLaneExecutions).


-spec prepare(record()) -> ok | no_return().
prepare(#atm_lane_execution{parallel_boxes = AtmParallelBoxExecutions}) ->
    atm_parallel_box_execution:prepare_all(AtmParallelBoxExecutions).


-spec delete_all([record()]) -> ok.
delete_all(AtmLaneExecutions) ->
    lists:foreach(fun delete/1, AtmLaneExecutions).


-spec delete(record()) -> ok.
delete(#atm_lane_execution{parallel_boxes = AtmParallelBoxExecutions}) ->
    atm_parallel_box_execution:delete_all(AtmParallelBoxExecutions).


-spec get_parallel_box_execution_specs(record()) -> [workflow_engine:parallel_box_spec()].
get_parallel_box_execution_specs(#atm_lane_execution{parallel_boxes = AtmParallelBoxExecutions}) ->
    lists:map(fun(AtmParallelBoxExecution) ->
        atm_parallel_box_execution:get_spec(AtmParallelBoxExecution)
    end, AtmParallelBoxExecutions).


-spec gather_statuses([record()]) -> [status()].
gather_statuses(AtmLaneExecutions) ->
    lists:map(fun(#atm_lane_execution{status = Status}) -> Status end, AtmLaneExecutions).


-spec update_task_status(
    non_neg_integer(),
    atm_task_execution:id(),
    atm_task_execution:status(),
    record()
) ->
    {ok, record()} | {error, term()}.
update_task_status(AtmParallelBoxIndex, AtmTaskExecutionId, NewStatus, #atm_lane_execution{
    parallel_boxes = AtmParallelBoxExecutions
} = AtmLaneExecution) ->
    AtmParallelBoxExecution = lists:nth(AtmParallelBoxIndex, AtmParallelBoxExecutions),

    case atm_parallel_box_execution:update_task_status(
        AtmTaskExecutionId, NewStatus, AtmParallelBoxExecution
    ) of
        {ok, NewParallelBoxExecution} ->
            NewAtmParallelBoxExecutions = atm_status_utils:replace_at(
                NewParallelBoxExecution, AtmParallelBoxIndex, AtmParallelBoxExecutions
            ),
            {ok, AtmLaneExecution#atm_lane_execution{
                status = atm_status_utils:converge(atm_parallel_box_execution:gather_statuses(
                    NewAtmParallelBoxExecutions
                )),
                parallel_boxes = NewAtmParallelBoxExecutions
            }};
        {error, _} = Error ->
            Error
    end.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_lane_execution{
    schema_id = AtmLaneSchemaId,
    status = AtmLaneExecutionStatus,
    parallel_boxes = AtmParallelBoxExecutions
}, NestedRecordEncoder) ->
    #{
        <<"schemaId">> => AtmLaneSchemaId,
        <<"status">> => atom_to_binary(AtmLaneExecutionStatus, utf8),
        <<"parallelBoxes">> => lists:map(fun(AtmParallelBoxExecution) ->
            NestedRecordEncoder(AtmParallelBoxExecution, atm_parallel_box_execution)
        end, AtmParallelBoxExecutions)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"schemaId">> := AtmLaneSchemaId,
    <<"status">> := AtmLaneExecutionStatusBin,
    <<"parallelBoxes">> := AtmParallelBoxExecutionsJson
}, NestedRecordDecoder) ->
    #atm_lane_execution{
        schema_id = AtmLaneSchemaId,
        status = binary_to_atom(AtmLaneExecutionStatusBin, utf8),
        parallel_boxes = lists:map(fun(AtmParallelBoxExecutionJson) ->
            NestedRecordDecoder(AtmParallelBoxExecutionJson, atm_parallel_box_execution)
        end, AtmParallelBoxExecutionsJson)
    }.
