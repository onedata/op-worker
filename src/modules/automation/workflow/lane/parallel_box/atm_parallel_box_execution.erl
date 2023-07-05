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
    start_all/2,
    init_stop_all/3,
    resume_all/2,
    ensure_all_stopped/1,
    teardown_all/2,
    delete_all/1, delete/1
]).
-export([set_tasks_run_num/2, update_task_status/3]).
-export([get_task_id/2, gather_task_statuses/1]).
-export([to_json/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type creation_args() :: #atm_parallel_box_execution_creation_args{}.

-type initiate_task_fun() :: fun((atm_workflow_execution_ctx:record(), atm_task_execution:id()) ->
    ignored |
    {ok, {workflow_engine:task_spec(), atm_workflow_execution_env:diff()}} |
    no_return()
).

-record(atm_parallel_box_execution, {
    schema_id :: automation:id(),
    task_registry :: #{AtmTaskSchemaId :: automation:id() => atm_task_execution:id()},
    task_statuses :: #{atm_task_execution:id() => atm_task_execution:status()}
}).
-opaque record() :: #atm_parallel_box_execution{}.

-export_type([creation_args/0, record/0]).


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
                ?examine_exception(Type, Reason, Stacktrace)
            ))
        end
    end, [], lists:enumerate(1, AtmParallelBoxSchemas)).


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
        task_registry = AtmTaskExecutionRegistry,
        task_statuses = AtmTaskExecutionStatuses
    }.


-spec start_all(atm_workflow_execution_ctx:record(), [record()]) ->
    {[workflow_engine:parallel_box_spec()], atm_workflow_execution_env:diff()} | no_return().
start_all(AtmWorkflowExecutionCtx0, AtmParallelBoxExecutions) ->
    InitiateFun = fun(AtmWorkflowExecutionCtx1, AtmTaskExecutionId) ->
        {ok, atm_task_execution_handler:start(AtmWorkflowExecutionCtx1, AtmTaskExecutionId)}
    end,
    initiate_all(AtmWorkflowExecutionCtx0, AtmParallelBoxExecutions, InitiateFun).


-spec init_stop_all(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:stopping_reason(),
    [record()]
) ->
    ok.
init_stop_all(AtmWorkflowExecutionCtx0, Reason, AtmParallelBoxExecutions) ->
    Callback = fun(AtmWorkflowExecutionCtx1, AtmTaskExecutionId) ->
        catch atm_task_execution_handler:init_stop(AtmWorkflowExecutionCtx1, AtmTaskExecutionId, Reason)
    end,
    foreach_task(AtmWorkflowExecutionCtx0, AtmParallelBoxExecutions, Callback).


-spec resume_all(atm_workflow_execution_ctx:record(), [record()]) ->
    {[workflow_engine:parallel_box_spec()], atm_workflow_execution_env:diff()} | no_return().
resume_all(AtmWorkflowExecutionCtx0, AtmParallelBoxExecutions) ->
    InitiateFun = fun atm_task_execution_handler:resume/2,
    initiate_all(AtmWorkflowExecutionCtx0, AtmParallelBoxExecutions, InitiateFun).


-spec ensure_all_stopped([record()]) -> ok | no_return().
ensure_all_stopped(AtmParallelBoxExecutions) ->
    pforeach_running_task(fun atm_task_execution_handler:handle_stopped/1, AtmParallelBoxExecutions).


-spec teardown_all(atm_workflow_execution_ctx:record(), [record()]) -> ok.
teardown_all(AtmWorkflowExecutionCtx0, AtmParallelBoxExecutions) ->
    Callback = fun(AtmWorkflowExecutionCtx1, AtmTaskExecutionId) ->
        catch atm_task_execution_handler:teardown(AtmWorkflowExecutionCtx1, AtmTaskExecutionId)
    end,
    foreach_task(AtmWorkflowExecutionCtx0, AtmParallelBoxExecutions, Callback).


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
    task_statuses = AtmTaskExecutionStatuses
} = AtmParallelBoxExecution) ->
    CurrentStatus = maps:get(AtmTaskExecutionId, AtmTaskExecutionStatuses),

    case atm_task_execution_status:is_transition_allowed(CurrentStatus, NewStatus) of
        true ->
            {ok, AtmParallelBoxExecution#atm_parallel_box_execution{
                task_statuses = AtmTaskExecutionStatuses#{
                    AtmTaskExecutionId => NewStatus
                }
            }};
        false ->
            ?ERROR_ATM_INVALID_STATUS_TRANSITION(CurrentStatus, NewStatus)
    end.


-spec get_task_id(automation:id(), record()) -> atm_task_execution:id().
get_task_id(AtmTaskSchemaId, #atm_parallel_box_execution{
    task_registry = AtmTaskExecutionRegistry
}) ->
    maps:get(AtmTaskSchemaId, AtmTaskExecutionRegistry).


-spec gather_task_statuses([record()]) -> [atm_task_execution:status()].
gather_task_statuses(AtmParallelBoxExecutions) ->
    lists:flatmap(fun(#atm_parallel_box_execution{task_statuses = AtmTaskExecutionStatuses}) ->
        maps:values(AtmTaskExecutionStatuses)
    end, AtmParallelBoxExecutions).


-spec to_json(record()) -> json_utils:json_term().
to_json(#atm_parallel_box_execution{
    schema_id = AtmParallelBoxSchemaId,
    task_registry = AtmTaskExecutionRegistry
}) ->
    #{
        <<"schemaId">> => AtmParallelBoxSchemaId,
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
    task_registry = AtmTaskExecutionRegistry,
    task_statuses = AtmTaskExecutionStatuses
}, _NestedRecordEncoder) ->
    #{
        <<"schemaId">> => AtmParallelBoxSchemaId,
        <<"taskRegistry">> => AtmTaskExecutionRegistry,
        <<"taskStatuses">> => maps:map(fun(_AtmTaskExecutionId, AtmTaskExecutionStatus) ->
            atom_to_binary(AtmTaskExecutionStatus, utf8)
        end, AtmTaskExecutionStatuses)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"schemaId">> := AtmParallelBoxSchemaId,
    <<"taskRegistry">> := AtmTaskExecutionRegistry,
    <<"taskStatuses">> := AtmTaskExecutionStatusesJson
}, _NestedRecordDecoder) ->
    #atm_parallel_box_execution{
        schema_id = AtmParallelBoxSchemaId,
        task_registry = AtmTaskExecutionRegistry,
        task_statuses = maps:map(fun(_AtmTaskExecutionId, AtmTaskExecutionStatusBin) ->
            binary_to_existing_atom(AtmTaskExecutionStatusBin, utf8)
        end, AtmTaskExecutionStatusesJson)
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec initiate_all(atm_workflow_execution_ctx:record(), [record()], initiate_task_fun()) ->
    {[workflow_engine:parallel_box_spec()], atm_workflow_execution_env:diff()} | no_return().
initiate_all(AtmWorkflowExecutionCtx0, AtmParallelBoxExecutions, InitiateTaskFun) ->
    AtmParallelBoxesInitiationResult = atm_parallel_runner:map(fun(#atm_parallel_box_execution{
        schema_id = AtmParallelBoxSchemaId
    } = AtmParallelBoxExecution) ->
        try
            initiate(AtmWorkflowExecutionCtx0, AtmParallelBoxExecution, InitiateTaskFun)
        catch
            throw:?ERROR_ATM_WORKFLOW_EXECUTION_STOPPING ->
                throw(?ERROR_ATM_WORKFLOW_EXECUTION_STOPPING);
            Type:Reason:Stacktrace ->
                Error = ?examine_exception(Type, Reason, Stacktrace),
                throw(?ERROR_ATM_PARALLEL_BOX_EXECUTION_INITIATION_FAILED(AtmParallelBoxSchemaId, Error))
        end
    end, AtmParallelBoxExecutions),

    lists:foldr(fun
        (
            {AtmParallelBoxExecutionSpec, AtmWorkflowExecutionEnvDiff},
            {AtmParallelBoxExecutionSpecsAcc, AtmWorkflowExecutionEnvDiffAcc}
        ) when map_size(AtmParallelBoxExecutionSpec) > 0 ->
            {
                [AtmParallelBoxExecutionSpec | AtmParallelBoxExecutionSpecsAcc],
                fun(Env) -> AtmWorkflowExecutionEnvDiff(AtmWorkflowExecutionEnvDiffAcc(Env)) end
            };

        (_, Acc) ->
            Acc
    end, {[], fun(Env) -> Env end}, AtmParallelBoxesInitiationResult).


%% @private
-spec initiate(atm_workflow_execution_ctx:record(), record(), initiate_task_fun()) ->
    {workflow_engine:parallel_box_spec(), atm_workflow_execution_env:diff()} | no_return().
initiate(
    AtmWorkflowExecutionCtx0,
    #atm_parallel_box_execution{task_registry = AtmTaskExecutionRegistry},
    InitiateTaskFun
) ->
    AtmTaskExecutionsInitiationResult = atm_parallel_runner:map(fun(
        {AtmTaskSchemaId, AtmTaskExecutionId}
    ) ->
        try
            AtmWorkflowExecutionCtx1 = atm_workflow_execution_ctx:configure_processed_task_id(
                AtmTaskExecutionId, AtmWorkflowExecutionCtx0
            ),
            {AtmTaskExecutionId, InitiateTaskFun(AtmWorkflowExecutionCtx1, AtmTaskExecutionId)}
        catch
            throw:?ERROR_ATM_WORKFLOW_EXECUTION_STOPPING ->
                throw(?ERROR_ATM_WORKFLOW_EXECUTION_STOPPING);
            Type:Reason:Stacktrace ->
                Error = ?examine_exception(Type, Reason, Stacktrace),
                throw(?ERROR_ATM_TASK_EXECUTION_INITIATION_FAILED(AtmTaskSchemaId, Error))
        end
    end, maps:to_list(AtmTaskExecutionRegistry)),

    lists:foldl(fun
        (
            {AtmTaskExecutionId, {ok, {AtmTaskExecutionSpec, AtmWorkflowExecutionEnvDiff}}},
            {AtmParallelBoxExecutionSpec, AtmWorkflowExecutionEnvDiffAcc}
        ) ->
            {
                AtmParallelBoxExecutionSpec#{AtmTaskExecutionId => AtmTaskExecutionSpec},
                fun(Env) -> AtmWorkflowExecutionEnvDiff(AtmWorkflowExecutionEnvDiffAcc(Env)) end
            };

        ({_AtmTaskExecutionId, ignored}, Acc) ->
            Acc
    end, {#{}, fun(Env) -> Env end}, AtmTaskExecutionsInitiationResult).


%% @private
-spec foreach_task(
    atm_workflow_execution_ctx:record(),
    [record()],
    fun((atm_workflow_execution_ctx:record(), atm_task_execution:id()) -> ok)
) ->
    ok.
foreach_task(AtmWorkflowExecutionCtx0, AtmParallelBoxExecutions, Callback) ->
    lists:foreach(fun(#atm_parallel_box_execution{task_registry = AtmTaskExecutionRegistry}) ->
        lists:foreach(fun(AtmTaskExecutionId) ->
            AtmWorkflowExecutionCtx1 = atm_workflow_execution_ctx:configure_processed_task_id(
                AtmTaskExecutionId, AtmWorkflowExecutionCtx0
            ),
            Callback(AtmWorkflowExecutionCtx1, AtmTaskExecutionId)
        end, maps:values(AtmTaskExecutionRegistry))
    end, AtmParallelBoxExecutions).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes given Callback for each running task. Specified function
%% must take into account possible race conditions when task transition to
%% suspended or ended phase just right before Callback is called.
%% @end
%%--------------------------------------------------------------------
-spec pforeach_running_task(
    fun((atm_task_execution:id()) -> ok | {error, term()}),
    [atm_task_execution:id()]
) ->
    ok | no_return().
pforeach_running_task(Callback, AtmParallelBoxExecutions) ->
    atm_parallel_runner:foreach(fun(#atm_parallel_box_execution{
        task_statuses = AtmTaskExecutionStatuses
    }) ->
        atm_parallel_runner:foreach(fun({AtmTaskExecutionId, AtmTaskExecutionStatus}) ->
            case atm_task_execution_status:is_running(AtmTaskExecutionStatus) of
                true -> Callback(AtmTaskExecutionId);
                false -> ok
            end
        end, maps:to_list(AtmTaskExecutionStatuses))
    end, AtmParallelBoxExecutions).
