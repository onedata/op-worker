%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module operating on automation parallel box executions.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_parallel_box_execution).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    create_all/1, create/3,
    initiate_all/2, initiate/2,
    stop_all/3, stop/3,
    ensure_all_ended/1,
    teardown_all/2, teardown/2,
    delete_all/1, delete/1
]).
-export([set_tasks_run_num/2, update_task_status/3]).
-export([gather_statuses/1]).
-export([to_json/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type creation_args() :: #atm_parallel_box_execution_creation_args{}.

-type status() :: atm_task_execution:status().

-record(atm_parallel_box_execution, {
    schema_id :: automation:id(),
    status :: status(),
    task_registry :: #{AtmTaskSchemaId :: automation:id() => atm_task_execution:id()},
    task_statuses :: #{atm_task_execution:id() => atm_task_execution:status()}
}).
-type record() :: #atm_parallel_box_execution{}.

-export_type([creation_args/0, status/0, record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_all(atm_lane_execution_factory:run_creation_args()) ->
    [record()] | no_return().
create_all(AtmLaneExecutionRunCreationArgs = #atm_lane_execution_run_creation_args{
    lane_schema = #atm_lane_schema{parallel_boxes = AtmParallelBoxSchemas}
}) ->
    lists:foldr(fun({AtmParallelBoxIndex, AtmParallelBoxSchema}, AtmParallelBoxExecutions) ->
        try
            AtmParallelBoxExecution = create(
                AtmLaneExecutionRunCreationArgs, AtmParallelBoxIndex, AtmParallelBoxSchema
            ),
            [AtmParallelBoxExecution | AtmParallelBoxExecutions]
        catch Type:Reason:Stacktrace ->
            catch delete_all(AtmParallelBoxExecutions),

            throw(?ERROR_ATM_PARALLEL_BOX_EXECUTION_CREATION_FAILED(
                AtmParallelBoxSchema#atm_parallel_box_schema.id,
                ?atm_examine_error(Type, Reason, Stacktrace)
            ))
        end
    end, [], lists_utils:enumerate(AtmParallelBoxSchemas)).


-spec create(
    atm_lane_execution_factory:run_creation_args(),
    pos_integer(),
    atm_parallel_box_schema:record()
) ->
    record().
create(AtmLaneExecutionRunCreationArgs, AtmParallelBoxIndex, #atm_parallel_box_schema{
    id = AtmParallelBoxSchemaId
} = AtmParallelBoxSchema) ->
    AtmTaskExecutionDocs = atm_task_execution_factory:create_all(#atm_parallel_box_execution_creation_args{
        lane_execution_run_creation_args = AtmLaneExecutionRunCreationArgs,
        parallel_box_index = AtmParallelBoxIndex,
        parallel_box_schema = AtmParallelBoxSchema
    }),

    {AtmTaskExecutionRegistry, AtmTaskExecutionStatuses} = lists:foldl(fun(
        #document{key = AtmTaskExecutionId, value = #atm_task_execution{
            schema_id = AtmTaskSchemaId,
            status = AtmTaskExecutionStatus
        }},
        {AtmTaskRegistryAcc, AtmTaskExecutionStatusesAcc}
    ) ->
        {
            AtmTaskRegistryAcc#{AtmTaskSchemaId => AtmTaskExecutionId},
            AtmTaskExecutionStatusesAcc#{AtmTaskExecutionId => AtmTaskExecutionStatus}
        }
    end, {#{}, #{}}, AtmTaskExecutionDocs),

    #atm_parallel_box_execution{
        schema_id = AtmParallelBoxSchemaId,
        status = infer_status(?PENDING_STATUS, maps:values(AtmTaskExecutionStatuses)),
        task_registry = AtmTaskExecutionRegistry,
        task_statuses = AtmTaskExecutionStatuses
    }.


-spec initiate_all(atm_workflow_execution_ctx:record(), [record()]) ->
    {[workflow_engine:parallel_box_spec()], atm_workflow_execution_env:diff()} | no_return().
initiate_all(AtmWorkflowExecutionCtx, AtmParallelBoxExecutions) ->
    AtmParallelBoxesInitiationResult = atm_parallel_runner:map(fun(#atm_parallel_box_execution{
        schema_id = AtmParallelBoxSchemaId
    } = AtmParallelBoxExecution) ->
        try
            initiate(AtmWorkflowExecutionCtx, AtmParallelBoxExecution)
        catch Type:Reason:Stacktrace ->
            Error = ?atm_examine_error(Type, Reason, Stacktrace),
            throw(?ERROR_ATM_PARALLEL_BOX_EXECUTION_INITIATION_FAILED(AtmParallelBoxSchemaId, Error))
        end
    end, AtmParallelBoxExecutions),

    lists:foldr(fun(
        {AtmParallelBoxExecutionSpec, AtmWorkflowExecutionEnvDiff},
        {AtmParallelBoxExecutionSpecsAcc, AtmWorkflowExecutionEnvDiffAcc}
    ) ->
        {
            [AtmParallelBoxExecutionSpec | AtmParallelBoxExecutionSpecsAcc],
            fun(Env) -> AtmWorkflowExecutionEnvDiff(AtmWorkflowExecutionEnvDiffAcc(Env)) end
        }
    end, {[], fun(Env) -> Env end}, AtmParallelBoxesInitiationResult).


-spec initiate(atm_workflow_execution_ctx:record(), record()) ->
    {workflow_engine:parallel_box_spec(), atm_workflow_execution_env:diff()} | no_return().
initiate(AtmWorkflowExecutionCtx0, #atm_parallel_box_execution{
    task_registry = AtmTaskExecutionRegistry
}) ->
    AtmTaskExecutionsInitiationResult = atm_parallel_runner:map(fun(
        {AtmTaskSchemaId, AtmTaskExecutionId}
    ) ->
        AtmWorkflowExecutionCtx1 = atm_workflow_execution_ctx:configure_processed_task_id(
            AtmTaskExecutionId, AtmWorkflowExecutionCtx0
        ),
        try
            {AtmTaskExecutionId, atm_task_execution_handler:initiate(
                AtmWorkflowExecutionCtx1, AtmTaskExecutionId
            )}
        catch Type:Reason:Stacktrace ->
            Error = ?atm_examine_error(Type, Reason, Stacktrace),
            throw(?ERROR_ATM_TASK_EXECUTION_INITIATION_FAILED(AtmTaskSchemaId, Error))
        end
    end, maps:to_list(AtmTaskExecutionRegistry)),

    lists:foldl(fun(
        {AtmTaskExecutionId, {AtmTaskExecutionSpec, AtmWorkflowExecutionEnvDiff}},
        {AtmParallelBoxExecutionSpec, AtmWorkflowExecutionEnvDiffAcc}
    ) ->
        {
            AtmParallelBoxExecutionSpec#{AtmTaskExecutionId => AtmTaskExecutionSpec},
            fun(Env) -> AtmWorkflowExecutionEnvDiff(AtmWorkflowExecutionEnvDiffAcc(Env)) end
        }
    end, {#{}, fun(Env) -> Env end}, AtmTaskExecutionsInitiationResult).


-spec stop_all(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:stopping_reason(),
    [record()]
) ->
    ok.
stop_all(AtmWorkflowExecutionCtx, Reason, AtmParallelBoxExecutions) ->
    lists:foreach(fun(AtmParallelBoxExecution) ->
        stop(AtmWorkflowExecutionCtx, Reason, AtmParallelBoxExecution)
    end, AtmParallelBoxExecutions).


-spec stop(atm_workflow_execution_ctx:record(), atm_task_execution:stopping_reason(), record()) ->
    ok.
stop(AtmWorkflowExecutionCtx0, Reason, #atm_parallel_box_execution{
    task_registry = AtmTaskExecutionRegistry
}) ->
    lists:foreach(fun(AtmTaskExecutionId) ->
        AtmWorkflowExecutionCtx1 = atm_workflow_execution_ctx:configure_processed_task_id(
            AtmTaskExecutionId, AtmWorkflowExecutionCtx0
        ),
        catch atm_task_execution_handler:stop(AtmWorkflowExecutionCtx1, AtmTaskExecutionId, Reason)
    end, maps:values(AtmTaskExecutionRegistry)).


-spec ensure_all_ended([record()]) -> ok | no_return().
ensure_all_ended(AtmParallelBoxExecutions) ->
    pforeach_not_ended_task(fun atm_task_execution_handler:handle_ended/1, AtmParallelBoxExecutions).


-spec teardown_all(atm_workflow_execution_ctx:record(), [record()]) -> ok.
teardown_all(AtmWorkflowExecutionCtx, AtmParallelBoxExecutions) ->
    lists:foreach(fun(AtmParallelBoxExecution) ->
        teardown(AtmWorkflowExecutionCtx, AtmParallelBoxExecution)
    end, AtmParallelBoxExecutions).


-spec teardown(atm_workflow_execution_ctx:record(), record()) -> ok.
teardown(AtmWorkflowExecutionCtx0, #atm_parallel_box_execution{
    task_registry = AtmTaskExecutionRegistry
}) ->
    lists:foreach(fun(AtmTaskExecutionId) ->
        AtmWorkflowExecutionCtx1 = atm_workflow_execution_ctx:configure_processed_task_id(
            AtmTaskExecutionId, AtmWorkflowExecutionCtx0
        ),
        catch atm_task_execution_handler:teardown(AtmWorkflowExecutionCtx1, AtmTaskExecutionId)
    end, maps:values(AtmTaskExecutionRegistry)).


-spec delete_all([record()]) -> ok.
delete_all(AtmParallelBoxExecutions) ->
    lists:foreach(fun delete/1, AtmParallelBoxExecutions).


-spec delete(record()) -> ok.
delete(#atm_parallel_box_execution{task_registry = AtmTaskExecutions}) ->
    atm_task_execution_factory:delete_all(maps:values(AtmTaskExecutions)).


-spec set_tasks_run_num(atm_lane_execution:run_num(), record() | [record()]) ->
    ok.
set_tasks_run_num(RunNum, #atm_parallel_box_execution{
    task_registry = AtmTaskExecutionRegistry
}) ->
    atm_parallel_runner:foreach(fun(AtmTaskExecutionId) ->
        atm_task_execution_handler:set_run_num(RunNum, AtmTaskExecutionId)
    end, maps:values(AtmTaskExecutionRegistry));

set_tasks_run_num(RunNum, AtmParallelBoxExecutions) ->
    atm_parallel_runner:foreach(fun(AtmParallelBoxExecution) ->
        set_tasks_run_num(RunNum, AtmParallelBoxExecution)
    end, AtmParallelBoxExecutions).


%%--------------------------------------------------------------------
%% @doc
%% Updates specified task status.
%%
%%                              !! CAUTION !!
%% This function is called when updating atm_workflow_execution_doc and as such
%% shouldn't touch any other persistent models.
%% @end
%%--------------------------------------------------------------------
-spec update_task_status(atm_task_execution:id(), atm_task_execution:status(), record()) ->
    {ok, record()} | {error, term()}.
update_task_status(AtmTaskExecutionId, NewStatus, #atm_parallel_box_execution{
    status = AtmParallelBoxExecutionStatus,
    task_statuses = AtmTaskExecutionStatuses
} = AtmParallelBoxExecution) ->
    CurrentStatus = maps:get(AtmTaskExecutionId, AtmTaskExecutionStatuses),

    case atm_task_execution_status:is_transition_allowed(CurrentStatus, NewStatus) of
        true ->
            NewAtmTaskExecutionStatuses = AtmTaskExecutionStatuses#{
                AtmTaskExecutionId => NewStatus
            },
            {ok, AtmParallelBoxExecution#atm_parallel_box_execution{
                status = infer_status(
                    AtmParallelBoxExecutionStatus,
                    maps:values(NewAtmTaskExecutionStatuses)
                ),
                task_statuses = NewAtmTaskExecutionStatuses
            }};
        false ->
            ?ERROR_ATM_INVALID_STATUS_TRANSITION(CurrentStatus, NewStatus)
    end.


-spec gather_statuses([record()]) -> [status()].
gather_statuses(AtmParallelBoxExecutions) ->
    lists:map(
        fun(#atm_parallel_box_execution{status = Status}) -> Status end,
        AtmParallelBoxExecutions
    ).


-spec to_json(record()) -> json_utils:json_term().
to_json(#atm_parallel_box_execution{
    schema_id = AtmParallelBoxSchemaId,
    status = AtmParallelBoxExecutionStatus,
    task_registry = AtmTaskExecutionRegistry
}) ->
    #{
        <<"schemaId">> => AtmParallelBoxSchemaId,
        <<"status">> => atom_to_binary(AtmParallelBoxExecutionStatus, utf8),
        <<"taskRegistry">> => AtmTaskExecutionRegistry
    }.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_parallel_box_execution{
    schema_id = AtmParallelBoxSchemaId,
    status = AtmParallelBoxExecutionStatus,
    task_registry = AtmTaskExecutionRegistry,
    task_statuses = AtmTaskExecutionStatuses
}, _NestedRecordEncoder) ->
    #{
        <<"schemaId">> => AtmParallelBoxSchemaId,
        <<"status">> => atom_to_binary(AtmParallelBoxExecutionStatus, utf8),
        <<"taskRegistry">> => AtmTaskExecutionRegistry,
        <<"taskStatuses">> => maps:map(fun(_AtmTaskExecutionId, AtmTaskExecutionStatus) ->
            atom_to_binary(AtmTaskExecutionStatus, utf8)
        end, AtmTaskExecutionStatuses)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"schemaId">> := AtmParallelBoxSchemaId,
    <<"status">> := AtmParallelBoxExecutionStatusBin,
    <<"taskRegistry">> := AtmTaskExecutionRegistry,
    <<"taskStatuses">> := AtmTaskExecutionStatusesJson
}, _NestedRecordDecoder) ->
    #atm_parallel_box_execution{
        schema_id = AtmParallelBoxSchemaId,
        status = binary_to_existing_atom(AtmParallelBoxExecutionStatusBin, utf8),
        task_registry = AtmTaskExecutionRegistry,
        task_statuses = maps:map(fun(_AtmTaskExecutionId, AtmTaskExecutionStatusBin) ->
            binary_to_existing_atom(AtmTaskExecutionStatusBin, utf8)
        end, AtmTaskExecutionStatusesJson)
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes given Callback for each not ended task. Specified function
%% must take into account possible race conditions when task transition to
%% ended phase just right before Callback is called.
%% @end
%%--------------------------------------------------------------------
-spec pforeach_not_ended_task(
    fun((atm_task_execution:id()) -> ok | {error, term()}),
    [atm_task_execution:id()]
) ->
    ok | no_return().
pforeach_not_ended_task(Callback, AtmParallelBoxExecutions) ->
    atm_parallel_runner:foreach(fun(Record = #atm_parallel_box_execution{
        task_statuses = AtmTaskExecutionStatuses
    }) ->
        case is_ended(Record) of
            true ->
                ok;
            false ->
                atm_parallel_runner:foreach(fun({AtmTaskExecutionId, AtmTaskExecutionStatus}) ->
                    case atm_task_execution_status:is_ended(AtmTaskExecutionStatus) of
                        true -> ok;
                        false -> Callback(AtmTaskExecutionId)
                    end
                end, maps:to_list(AtmTaskExecutionStatuses))
        end
    end, AtmParallelBoxExecutions).


%% @private
-spec is_ended(record()) -> boolean().
is_ended(#atm_parallel_box_execution{status = AtmParallelBoxExecutionStatus}) ->
    atm_task_execution_status:is_ended(AtmParallelBoxExecutionStatus).


%% @private
-spec infer_status(atm_task_execution:status(), [atm_task_execution:status()]) ->
    status().
infer_status(CurrentStatus, AtmTaskExecutionStatuses) ->
    PossibleNewStatus = case lists:usort(AtmTaskExecutionStatuses) of
        [Status] ->
            Status;
        UniqueStatuses ->
            hd(lists:dropwhile(fun(Status) -> not lists:member(Status, UniqueStatuses) end, [
                ?STOPPING_STATUS, ?ACTIVE_STATUS, ?PENDING_STATUS,
                ?CANCELLED_STATUS, ?FAILED_STATUS, ?INTERRUPTED_STATUS,
                ?PAUSED_STATUS, ?FINISHED_STATUS
            ]))
    end,

    case atm_task_execution_status:is_transition_allowed(CurrentStatus, PossibleNewStatus) of
        true ->
            PossibleNewStatus;
        false ->
            % possible when status is not changing or in case of races (e.g.
            % stopping task ended while some other task has not been marked
            % as stopping yet and as such is still active - overall status
            % should remain as stopping rather than regressing to active)
            CurrentStatus
    end.
