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

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    create_all/3, create/4,
    init_all/1, init/1,
    delete_all/1, delete/1,

    gather_statuses/1,
    update_task_status/4
]).


-type status() :: atm_task_execution:status().
-type record() :: #atm_lane_execution{}.

-export_type([status/0, record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_all(
    atm_workflow_execution:id(),
    atm_store_api:registry(),
    [atm_lane_schema:record()]
) ->
    [record()] | no_return().
create_all(AtmWorkflowExecutionId, AtmStoreRegistry, AtmLaneSchemas) ->
    lists:reverse(lists:foldl(fun({AtmLaneNo, #atm_lane_schema{
        id = AtmLaneSchemaId
    } = AtmLaneSchema}, Acc) ->
        try
            AtmLaneExecution = create(
                AtmWorkflowExecutionId, AtmStoreRegistry, AtmLaneNo, AtmLaneSchema
            ),
            [AtmLaneExecution | Acc]
        catch _:Reason ->
            catch delete_all(Acc),
            throw(?ERROR_ATM_LANE_EXECUTION_CREATION_FAILED(AtmLaneSchemaId, Reason))
        end
    end, [], lists_utils:enumerate(AtmLaneSchemas))).


-spec create(
    atm_workflow_execution:id(),
    atm_store_api:registry(),
    non_neg_integer(),
    atm_lane_schema:record()
) ->
    record() | no_return().
create(AtmWorkflowExecutionId, AtmStoreRegistry, AtmLaneNo, #atm_lane_schema{
    id = AtmLaneSchemaId,
    name = AtmLaneName,
    parallel_boxes = AtmParallelBoxSchemas,
    store_iterator_spec = AtmStoreIteratorSpec
}) ->
    AtmStoreIteratorConfig = atm_store_api:build_iterator_config(
        AtmStoreRegistry, AtmStoreIteratorSpec
    ),
    AtmParallelBoxExecutions = atm_parallel_box_execution:create_all(
        AtmWorkflowExecutionId, AtmLaneNo, AtmParallelBoxSchemas
    ),

    #atm_lane_execution{
        status = ?PENDING_STATUS,
        schema_id = AtmLaneSchemaId,
        name = AtmLaneName,
        store_iterator_config = AtmStoreIteratorConfig,
        parallel_boxes = AtmParallelBoxExecutions
    }.


-spec init_all([record()]) -> ok | no_return().
init_all(AtmLaneExecutions) ->
    lists:foreach(fun(#atm_lane_execution{schema_id = AtmLaneSchemaId} = AtmLaneExecution) ->
        try
            init(AtmLaneExecution)
        catch _:Reason ->
            throw(?ERROR_ATM_LANE_EXECUTION_INIT_FAILED(AtmLaneSchemaId, Reason))
        end
    end, AtmLaneExecutions).


-spec init(record()) -> ok | no_return().
init(#atm_lane_execution{parallel_boxes = AtmParallelBoxExecutions}) ->
    atm_parallel_box_execution:init_all(AtmParallelBoxExecutions).


-spec delete_all([record()]) -> ok.
delete_all(AtmLaneExecutions) ->
    lists:foreach(fun delete/1, AtmLaneExecutions).


-spec delete(record()) -> ok.
delete(#atm_lane_execution{parallel_boxes = ParallelBoxExecutions}) ->
    atm_parallel_box_execution:delete_all(ParallelBoxExecutions).


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
update_task_status(AtmParallelBoxExecutionNo, AtmTaskExecutionId, NewStatus, #atm_lane_execution{
    parallel_boxes = AtmParallelBoxExecutions
} = AtmLaneExecution) ->
    AtmParallelBoxExecution = lists:nth(AtmParallelBoxExecutionNo, AtmParallelBoxExecutions),

    case atm_parallel_box_execution:update_task_status(
        AtmTaskExecutionId, NewStatus, AtmParallelBoxExecution
    ) of
        {ok, NewParallelBoxExecution} ->
            NewAtmParallelBoxExecutions = atm_status_utils:replace_at(
                NewParallelBoxExecution, AtmParallelBoxExecutionNo, AtmParallelBoxExecutions
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
