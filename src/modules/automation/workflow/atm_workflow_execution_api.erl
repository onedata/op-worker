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

-include("modules/automation/atm_execution.hrl").

%% API
-export([list/4]).
-export([create/1]).
-export([get_summary/1, get_summary/2]).
-export([report_task_status_change/5]).


-type listing_mode() :: basic | summary.

-type basic_entries() :: atm_workflow_executions_forest:entries().
-type summary_entries() :: [{atm_workflow_executions_forest:index(), atm_workflow_execution:summary()}].
-type entries() :: basic_entries() | summary_entries().

-export_type([listing_mode/0, basic_entries/0, summary_entries/0, entries/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec list(
    od_space:id(),
    atm_workflow_execution:phase(),
    listing_mode(),
    atm_workflow_executions_forest:listing_opts()
) ->
    {ok, entries(), IsLast :: boolean()}.
list(SpaceId, Phase, basic, ListingOpts) ->
    AtmWorkflowExecutionBasicEntries = list_basic_entries(SpaceId, Phase, ListingOpts),
    IsLast = maps:get(limit, ListingOpts) > length(AtmWorkflowExecutionBasicEntries),

    {ok, AtmWorkflowExecutionBasicEntries, IsLast};

list(SpaceId, Phase, summary, ListingOpts) ->
    AtmWorkflowExecutionBasicEntries = list_basic_entries(SpaceId, Phase, ListingOpts),
    IsLast = maps:get(limit, ListingOpts) > length(AtmWorkflowExecutionBasicEntries),

    AtmWorkflowExecutionSummaryEntries = lists_utils:pfiltermap(fun({Index, AtmWorkflowExecutionId}) ->
        {ok, #document{value = AtmWorkflowExecution}} = atm_workflow_execution:get(
            AtmWorkflowExecutionId
        ),
        case atm_workflow_execution_status:infer_phase(AtmWorkflowExecution) of
            Phase ->
                {true, {Index, get_summary(AtmWorkflowExecutionId, AtmWorkflowExecution)}};
            _ ->
                false
        end
    end, AtmWorkflowExecutionBasicEntries),

    {ok, AtmWorkflowExecutionSummaryEntries, IsLast}.


-spec create(atm_workflow_execution:creation_ctx()) ->
    {ok, atm_workflow_execution:doc()} | no_return().
create(AtmWorkflowExecutionCreationCtx) ->
    atm_workflow_execution_factory:create(AtmWorkflowExecutionCreationCtx).


-spec get_summary(atm_workflow_execution:id() | atm_workflow_execution:doc()) ->
    atm_workflow_execution:summary().
get_summary(#document{key = AtmWorkflowExecutionId, value = AtmWorkflowExecution}) ->
    get_summary(AtmWorkflowExecutionId, AtmWorkflowExecution);
get_summary(AtmWorkflowExecutionId) ->
    {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),
    get_summary(AtmWorkflowExecutionDoc).


-spec get_summary(atm_workflow_execution:id(), atm_workflow_execution:record()) ->
    atm_workflow_execution:summary().
get_summary(AtmWorkflowExecutionId, #atm_workflow_execution{
    name = Name,
    atm_inventory_id = AtmInventoryId,
    status = AtmWorkflowExecutionStatus,
    schedule_time = ScheduleTime,
    start_time = StartTime,
    finish_time = FinishTime
}) ->
    #atm_workflow_execution_summary{
        atm_workflow_execution_id = AtmWorkflowExecutionId,
        name = Name,
        atm_inventory_id = AtmInventoryId,
        status = AtmWorkflowExecutionStatus,
        schedule_time = ScheduleTime,
        start_time = StartTime,
        finish_time = FinishTime
    }.


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
    AtmLaneExecutionIndex,
    AtmParallelBoxExecutionIndex,
    AtmTaskExecutionId,
    NewStatus
) ->
    atm_workflow_execution_status:report_task_status_change(
        AtmWorkflowExecutionId, AtmLaneExecutionIndex, AtmParallelBoxExecutionIndex,
        AtmTaskExecutionId, NewStatus
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec list_basic_entries(
    od_space:id(),
    atm_workflow_execution:phase(),
    atm_workflow_executions_forest:listing_opts()
) ->
    basic_entries().
list_basic_entries(SpaceId, ?WAITING_PHASE, ListingOpts) ->
    atm_waiting_workflow_executions:list(SpaceId, ListingOpts);
list_basic_entries(SpaceId, ?ONGOING_PHASE, ListingOpts) ->
    atm_ongoing_workflow_executions:list(SpaceId, ListingOpts);
list_basic_entries(SpaceId, ?ENDED_PHASE, ListingOpts) ->
    atm_ended_workflow_executions:list(SpaceId, ListingOpts).
