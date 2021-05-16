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

-include("modules/automation/atm_wokflow_execution.hrl").

-export([
    create_all/3, create/4,
    init_all/1, init/1,
    delete_all/1, delete/1
]).


-type status() :: ?PENDING_STATUS | ?ACTIVE_STATUS | ?FINISHED_STATUS | ?FAILED_STATUS.
-type record() :: #atm_parallel_box_execution{}.

-export_type([status/0, record/0]).


%%%===================================================================
%%% atm_container callbacks
%%%===================================================================


-spec create_all(
    atm_workflow_execution:id(),
    non_neg_integer(),
    [atm_parallel_box_schema()]
) ->
    [record()] | no_return().
create_all(AtmWorkflowExecutionId, AtmLaneNo, AtmParallelBoxSchemas) ->
    lists:reverse(lists:foldl(fun({AtmParallelBoxNo, #atm_parallel_box_schema{
        id = AtmParallelBoxSchemaId
    } = AtmParallelBoxSchema}, Acc) ->
        try
            AtmParallelBoxExecution = create(
                AtmWorkflowExecutionId, AtmLaneNo, AtmParallelBoxNo, AtmParallelBoxSchema
            ),
            [AtmParallelBoxExecution | Acc]
        catch _:Reason ->
            delete_all(Acc),
            throw(?ERROR_ATM_PARALLEL_BOX_EXECUTION_CREATION_FAILED(AtmParallelBoxSchemaId, Reason))
        end
    end, [], lists_utils:enumerate(AtmParallelBoxSchemas))).


-spec create(
    atm_workflow_execution:id(),
    non_neg_integer(),
    non_neg_integer(),
    atm_parallel_box_schema()
) ->
    record() | no_return().
create(AtmWorkflowExecutionId, AtmLaneNo, AtmParallelBoxNo, #atm_parallel_box_schema{
    id = AtmParallelBoxSchemaId,
    name = AtmParallelBoxName,
    tasks = AtmTaskSchemas
}) ->
    AtmTaskExecutionRegistry = atm_task_execution_api:create_all(
        AtmWorkflowExecutionId, AtmLaneNo, AtmParallelBoxNo, AtmTaskSchemas
    ),

    #atm_parallel_box_execution{
        status = ?PENDING_STATUS,
        schema_id = AtmParallelBoxSchemaId,
        name = AtmParallelBoxName,
        tasks = AtmTaskExecutionRegistry
    }.


-spec init_all([record()]) -> ok | no_return().
init_all(AtmParallelBoxExecutions) ->
    lists:foreach(fun(#atm_parallel_box_execution{
        schema_id = AtmParallelBoxSchemaId
    } = AtmParallelBoxExecution) ->
        try
            init(AtmParallelBoxExecution)
        catch _:Reason ->
            throw(?ERROR_ATM_PARALLEL_BOX_EXECUTION_INIT_FAILED(AtmParallelBoxSchemaId, Reason))
        end
    end, AtmParallelBoxExecutions).


-spec init(record()) -> ok | no_return().
init(#atm_parallel_box_execution{tasks = AtmTaskExecutionRegistry}) ->
    atm_task_execution_api:init_all(maps:keys(AtmTaskExecutionRegistry)).


-spec delete_all([record()]) -> ok.
delete_all(AtmParallelBoxExecutions) ->
    lists:foreach(fun delete/1, AtmParallelBoxExecutions).


-spec delete(record()) -> ok.
delete(#atm_parallel_box_execution{tasks = AtmTaskExecutions}) ->
    atm_task_execution_api:delete_all(maps:keys(AtmTaskExecutions)).
