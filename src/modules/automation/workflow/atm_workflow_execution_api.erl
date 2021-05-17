%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API module for performing operations on automation workflow executions.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_api).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_wokflow_execution.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    create/3, init/1, delete/1,
    report_task_status_change/5
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(od_space:id(), atm_workflow_schema(), atm_store_api:initial_values()) ->
    {ok, atm_workflow_execution:id()} | no_return().
create(SpaceId, #atm_workflow_schema{
    id = AtmWorkflowSchemaId,
    state = AtmWorkflowSchemaState,
    name = AtmWorkflowSchemaName,
    description = AtmWorkflowSchemaDescription,
    stores = AtmStoreSchemas,
    lanes = AtmLaneSchemas
}, InitialValues) ->
    AtmWorkflowExecutionId = datastore_key:new(),

    AtmStoreRegistry = atm_store_api:create_all(AtmStoreSchemas, InitialValues),

    AtmLaneExecutions = try
        atm_lane_execution:create_all(AtmWorkflowExecutionId, AtmStoreRegistry, AtmLaneSchemas)
    catch Type:Reason ->
        atm_store_api:delete_all(maps:values(AtmStoreRegistry)),
        erlang:Type(Reason)
    end,

    atm_workflow_execution:create(#document{
        key = AtmWorkflowExecutionId,
        value = #atm_workflow_execution{
            schema_id = AtmWorkflowSchemaId,
            schema_state = AtmWorkflowSchemaState,
            name = AtmWorkflowSchemaName,
            description = AtmWorkflowSchemaDescription,

            space_id = SpaceId,
            stores = maps:values(AtmStoreRegistry),
            lanes = AtmLaneExecutions,

            status = ?SCHEDULED_STATUS,
            schedule_time = global_clock:timestamp_seconds(),
            start_time = 0,
            finish_time = 0
        }
    }),
    % TODO VFS-7672 add to scheduled link tree
    {ok, AtmWorkflowExecutionId}.


-spec init(atm_workflow_execution:id()) -> ok | no_return().
init(AtmWorkflowExecutionId) ->
    {ok, #atm_workflow_execution{lanes = AtmLaneExecutions}} = transit_to_status(
        AtmWorkflowExecutionId, ?INITIALIZING_STATUS
    ),

    try
        atm_lane_execution:init_all(AtmLaneExecutions)
    catch Type:Reason ->
        transit_to_failed_status(AtmWorkflowExecutionId),
        erlang:Type(Reason)
    end,
    transit_to_status(AtmWorkflowExecutionId, ?ENQUEUED_STATUS),

    ok.


-spec delete(atm_workflow_execution:id()) -> ok | no_return().
delete(AtmWorkflowExecutionId) ->
    {ok, #document{value = #atm_workflow_execution{
        lanes = AtmLaneExecutions
    }}} = atm_workflow_execution:get(AtmWorkflowExecutionId),

    atm_lane_execution:delete_all(AtmLaneExecutions),
    atm_workflow_execution:delete(AtmWorkflowExecutionId).


-spec report_task_status_change(
    atm_workflow_execution:id(),
    non_neg_integer(),
    non_neg_integer(),
    atm_task_execution:id(),
    atm_task_execution:status()
) ->
    ok.
report_task_status_change(
    AtmWorkflowExecutionId,
    AtmLaneExecutionNo,
    AtmParallelBoxExecutionNo,
    AtmTaskExecutionId,
    NewStatus
) ->
    Diff = fun(#atm_workflow_execution{} = AtmWorkflowExecution) ->
        update_task_status(
            AtmLaneExecutionNo, AtmParallelBoxExecutionNo,
            AtmTaskExecutionId, NewStatus, AtmWorkflowExecution
        )
    end,
    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, #document{value = #atm_workflow_execution{status_changed = true}}} ->
            % TODO VFS-7672 change link tree
            ok;
        _ ->
            ok
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec transit_to_status(atm_workflow_execution:id(), atm_workflow_execution:status()) ->
    {ok, atm_workflow_execution:record()} | no_return().
transit_to_status(AtmWorkflowExecutionId, NewStatus) ->
    Diff = fun(#atm_workflow_execution{status = Status} = AtmWorkflowExecution) ->
        case atm_status_utils:is_transition_allowed(Status, NewStatus) of
            true ->
                {ok, AtmWorkflowExecution#atm_workflow_execution{status = NewStatus}};
            false ->
                {error, Status}
        end
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, #document{value = AtmWorkflowExecution}} ->
            {ok, AtmWorkflowExecution};
        {error, CurrStatus} ->
            throw(?ERROR_ATM_INVALID_STATUS_TRANSITION(CurrStatus, NewStatus))
    end.


%% @private
-spec transit_to_failed_status(atm_workflow_execution:id()) -> ok.
transit_to_failed_status(AtmWorkflowExecutionId) ->
    % TODO should fail here change status of all lanes, pboxes and tasks ??
    TransitFun = fun(#atm_workflow_execution{} = AtmWorkflowExecution) ->
        {ok, AtmWorkflowExecution#atm_workflow_execution{status = ?FAILED_STATUS}}
    end,
    atm_workflow_execution:update(AtmWorkflowExecutionId, TransitFun),
    ok.


%% @private
-spec update_task_status(
    non_neg_integer(),
    non_neg_integer(),
    atm_task_execution:id(),
    atm_task_execution:status(),
    atm_workflow_execution:record()
) ->
    {ok, atm_workflow_execution:record()} | {error, term()}.
update_task_status(
    AtmLaneExecutionNo,
    AtmParallelBoxExecutionNo,
    AtmTaskExecutionId,
    NewStatus,
    AtmWorkflowExecution = #atm_workflow_execution{
        status = CurrentStatus,
        lanes = AtmLaneExecutions
    }
) ->
    AtmLanExecution = lists:nth(AtmLaneExecutionNo, AtmLaneExecutions),

    case atm_lane_execution:update_task_status(
        AtmParallelBoxExecutionNo, AtmTaskExecutionId, NewStatus, AtmLanExecution
    ) of
        {ok, NewLaneExecution} ->
            NewAtmLaneExecutions = atm_status_utils:replace_at(
                NewLaneExecution, AtmLaneExecutionNo, AtmLaneExecutions
            ),
            NewAtmWorkflowStatus = atm_status_utils:converge(lists:usort(
                atm_lane_execution:gather_statuses(NewAtmLaneExecutions)
            )),
            {ok, AtmWorkflowExecution#atm_workflow_execution{
                status = NewAtmWorkflowStatus,
                status_changed = NewAtmWorkflowStatus /= CurrentStatus,
                lanes = NewAtmLaneExecutions
            }};
        {error, _} = Error ->
            Error
    end.
