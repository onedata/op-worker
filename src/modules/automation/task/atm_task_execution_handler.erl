%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles the task execution process.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_handler).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    process_item/5,
    process_results/3,

    handle_ended/1
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec process_item(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    json_utils:json_term(),
    binary(),
    binary()
) ->
    ok | no_return().
process_item(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, ReportResultUrl, HeartbeatUrl) ->
    #document{value = #atm_task_execution{
        executor = AtmTaskExecutor,
        argument_specs = AtmTaskExecutionArgSpecs
    }} = update_items_in_processing(AtmTaskExecutionId),

    AtmJobExecutionCtx = atm_job_execution_ctx:build(
        AtmWorkflowExecutionCtx, atm_task_executor:in_readonly_mode(AtmTaskExecutor),
        Item, ReportResultUrl, HeartbeatUrl
    ),
    Args = atm_task_execution_arguments:construct_args(AtmJobExecutionCtx, AtmTaskExecutionArgSpecs),

    atm_task_executor:run(AtmJobExecutionCtx, Args, AtmTaskExecutor).


-spec process_results(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    error | json_utils:json_map()
) ->
    ok | no_return().
process_results(_AtmWorkflowExecutionCtx, AtmTaskExecutionId, error) ->
    update_items_failed_and_processed(AtmTaskExecutionId);

process_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Results) when is_map(Results) ->
    {ok, #document{value = #atm_task_execution{
        result_specs = AtmTaskExecutionResultSpecs
    }}} = atm_task_execution:get(AtmTaskExecutionId),

    atm_task_execution_results:apply(AtmWorkflowExecutionCtx, AtmTaskExecutionResultSpecs, Results),
    update_items_processed(AtmTaskExecutionId);

process_results(_AtmWorkflowExecutionEnv, _AtmTaskExecutionId, _Results) ->
    throw(?ERROR_ATM_BAD_DATA(<<"results">>, <<"not an object">>)).


-spec handle_ended(atm_task_execution:id()) -> ok.
handle_ended(AtmTaskExecutionId) ->
    Diff = fun
        (#atm_task_execution{status = ?PENDING_STATUS} = AtmTaskExecution) ->
            {ok, AtmTaskExecution#atm_task_execution{status = ?SKIPPED_STATUS}};
        (#atm_task_execution{
            status = ?ACTIVE_STATUS,
            items_in_processing = ItemsInProcessing,
            items_processed = ItemsProcessed,
            items_failed = ItemsFailed
        } = AtmTaskExecution) ->
            % atm workflow execution may have been abruptly interrupted by e.g.
            % provider restart which resulted in stale `items_in_processing`
            NewAtmTaskExecution = case ItemsFailed + ItemsInProcessing of
                0 ->
                    AtmTaskExecution#atm_task_execution{status = ?FINISHED_STATUS};
                AllFailedItems ->
                    AtmTaskExecution#atm_task_execution{
                        status = ?FAILED_STATUS,
                        items_in_processing = 0,
                        items_processed = ItemsProcessed + ItemsInProcessing,
                        items_failed = AllFailedItems
                    }
            end,
            {ok, NewAtmTaskExecution};
        (_) ->
            {error, already_ended}
    end,
    case atm_task_execution:update(AtmTaskExecutionId, Diff) of
        {ok, AtmTaskExecutionDoc} ->
            handle_status_change(AtmTaskExecutionDoc);
        {error, already_ended} ->
            ok
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec update_items_in_processing(atm_task_execution:id()) ->
    atm_task_execution:doc().
update_items_in_processing(AtmTaskExecutionId) ->
    Diff = fun
        (#atm_task_execution{status = ?PENDING_STATUS, items_in_processing = 0} = AtmTaskExecution) ->
            {ok, AtmTaskExecution#atm_task_execution{
                status = ?ACTIVE_STATUS,
                items_in_processing = 1
            }};
        (#atm_task_execution{status = ?ACTIVE_STATUS, items_in_processing = Num} = AtmTaskExecution) ->
            {ok, AtmTaskExecution#atm_task_execution{items_in_processing = Num + 1}};
        (_) ->
            {error, already_ended}
    end,
    case atm_task_execution:update(AtmTaskExecutionId, Diff) of
        {ok, AtmTaskExecutionDoc} ->
            handle_status_change(AtmTaskExecutionDoc),
            AtmTaskExecutionDoc;
        {error, already_ended} ->
            throw(?ERROR_ATM_TASK_EXECUTION_ENDED)
    end.


%% @private
-spec update_items_processed(atm_task_execution:id()) -> ok.
update_items_processed(AtmTaskExecutionId) ->
    {ok, _} = atm_task_execution:update(AtmTaskExecutionId, fun(#atm_task_execution{
        items_in_processing = ItemsInProcessing,
        items_processed = ItemsProcessed
    } = AtmTaskExecution) ->
        {ok, AtmTaskExecution#atm_task_execution{
            items_in_processing = ItemsInProcessing - 1,
            items_processed = ItemsProcessed + 1
        }}
    end),
    ok.


%% @private
-spec update_items_failed_and_processed(atm_task_execution:id()) -> ok.
update_items_failed_and_processed(AtmTaskExecutionId) ->
    {ok, _} = atm_task_execution:update(AtmTaskExecutionId, fun(#atm_task_execution{
        items_in_processing = ItemsInProcessing,
        items_processed = ItemsProcessed,
        items_failed = ItemsFailed
    } = AtmTaskExecution) ->
        {ok, AtmTaskExecution#atm_task_execution{
            items_in_processing = ItemsInProcessing - 1,
            items_processed = ItemsProcessed + 1,
            items_failed = ItemsFailed + 1
        }}
    end),
    ok.


%% @private
-spec handle_status_change(atm_task_execution:doc()) -> ok.
handle_status_change(#document{value = #atm_task_execution{status_changed = false}}) ->
    ok;
handle_status_change(#document{
    key = AtmTaskExecutionId,
    value = #atm_task_execution{
        workflow_execution_id = AtmWorkflowExecutionId,
        lane_index = AtmLaneIndex,
        parallel_box_index = AtmParallelBoxIndex,
        status = NewStatus,
        status_changed = true
    }
}) ->
    atm_workflow_execution_status:report_task_status_change(
        AtmWorkflowExecutionId, AtmLaneIndex, AtmParallelBoxIndex,
        AtmTaskExecutionId, NewStatus
    ).
