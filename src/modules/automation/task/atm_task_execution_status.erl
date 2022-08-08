%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides utility functions for management of automation
%%% task execution status.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_status).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([is_transition_allowed/2, is_ended/1]).

-export([
    handle_items_in_processing/2,
    handle_item_processed/1,
    handle_items_failed/2,

    handle_aborting/2,
    handle_ended/1
]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec is_transition_allowed(atm_task_execution:status(), atm_task_execution:status()) ->
    boolean().
is_transition_allowed(?PENDING_STATUS, ?ACTIVE_STATUS) -> true;
is_transition_allowed(?PENDING_STATUS, ?SKIPPED_STATUS) -> true;
is_transition_allowed(?ACTIVE_STATUS, ?FINISHED_STATUS) -> true;
is_transition_allowed(?ACTIVE_STATUS, ?ABORTING_STATUS) -> true;
is_transition_allowed(?ABORTING_STATUS, ?CANCELLED_STATUS) -> true;
is_transition_allowed(?ABORTING_STATUS, ?FAILED_STATUS) -> true;
is_transition_allowed(?ABORTING_STATUS, ?INTERRUPTED_STATUS) -> true;
is_transition_allowed(?ABORTING_STATUS, ?PAUSED_STATUS) -> true;
is_transition_allowed(_, _) -> false.


-spec is_ended(atm_task_execution:status()) -> boolean().
is_ended(?FINISHED_STATUS) -> true;
is_ended(?SKIPPED_STATUS) -> true;
is_ended(?CANCELLED_STATUS) -> true;
is_ended(?FAILED_STATUS) -> true;
is_ended(?INTERRUPTED_STATUS) -> true;
is_ended(?PAUSED_STATUS) -> true;
is_ended(_) -> false.


-spec handle_items_in_processing(atm_task_execution:id(), pos_integer()) ->
    {ok, atm_task_execution:doc()} | {error, aborting} | {error, already_ended}.
handle_items_in_processing(AtmTaskExecutionId, ItemsNum) ->
    apply_diff(AtmTaskExecutionId, fun
        (AtmTaskExecution = #atm_task_execution{
            status = ?PENDING_STATUS,
            items_in_processing = 0
        }) ->
            {ok, AtmTaskExecution#atm_task_execution{
                status = ?ACTIVE_STATUS,
                items_in_processing = ItemsNum
            }};

        (AtmTaskExecution = #atm_task_execution{
            status = ?ACTIVE_STATUS,
            items_in_processing = CurrentItemsInProcessingNum
        }) ->
            {ok, AtmTaskExecution#atm_task_execution{
                items_in_processing = CurrentItemsInProcessingNum + ItemsNum
            }};

        (#atm_task_execution{status = ?ABORTING_STATUS}) ->
            {error, aborting};

        (_) ->
            {error, already_ended}
    end).


-spec handle_item_processed(atm_task_execution:id()) -> ok.
handle_item_processed(AtmTaskExecutionId) ->
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


-spec handle_items_failed(atm_task_execution:id(), pos_integer()) -> ok.
handle_items_failed(AtmTaskExecutionId, ItemsNum) ->
    {ok, _} = atm_task_execution:update(AtmTaskExecutionId, fun(#atm_task_execution{
        items_in_processing = ItemsInProcessing,
        items_processed = ItemsProcessed,
        items_failed = ItemsFailed
    } = AtmTaskExecution) ->
        {ok, AtmTaskExecution#atm_task_execution{
            items_in_processing = ItemsInProcessing - ItemsNum,
            items_processed = ItemsProcessed + ItemsNum,
            items_failed = ItemsFailed + ItemsNum
        }}
    end),
    ok.


-spec handle_aborting(
    atm_task_execution:id(),
    atm_task_execution:aborting_reason()
) ->
    {ok, atm_task_execution:doc()} | {error, already_ended}.
handle_aborting(AtmTaskExecutionId, Reason) ->
    apply_diff(AtmTaskExecutionId, fun
        (AtmTaskExecution = #atm_task_execution{status = ?PENDING_STATUS}) ->
            {ok, AtmTaskExecution#atm_task_execution{status = ?SKIPPED_STATUS}};

        (AtmTaskExecution = #atm_task_execution{status = ?ACTIVE_STATUS}) ->
            {ok, AtmTaskExecution#atm_task_execution{
                status = ?ABORTING_STATUS,
                aborting_reason = Reason
            }};

        (AtmTaskExecution = #atm_task_execution{
            status = ?ABORTING_STATUS,
            aborting_reason = PrevReason
        }) ->
            {ok, AtmTaskExecution#atm_task_execution{
                aborting_reason = case should_overwrite_aborting_reason(PrevReason, Reason) of
                    true -> Reason;
                    false -> PrevReason
                end
            }};

        (_) ->
            {error, already_ended}
    end).


-spec handle_ended(atm_task_execution:id()) ->
    {ok, atm_task_execution:doc()} | {error, already_ended}.
handle_ended(AtmTaskExecutionId) ->
    apply_diff(AtmTaskExecutionId, fun
        (AtmTaskExecution = #atm_task_execution{status = ?PENDING_STATUS}) ->
            {ok, AtmTaskExecution#atm_task_execution{status = ?SKIPPED_STATUS}};

        (AtmTaskExecution = #atm_task_execution{status = ?ACTIVE_STATUS, items_failed = 0}) ->
            {ok, AtmTaskExecution#atm_task_execution{status = ?FINISHED_STATUS}};

        (AtmTaskExecution = #atm_task_execution{status = ?ACTIVE_STATUS}) ->
            % All jobs were executed but some must have failed
            {ok, AtmTaskExecution#atm_task_execution{status = ?FAILED_STATUS}};

        (AtmTaskExecution = #atm_task_execution{
            status = ?ABORTING_STATUS,
            aborting_reason = AbortingReason,
            items_in_processing = ItemsInProcessing,
            items_processed = ItemsProcessed,
            items_failed = ItemsFailed
        }) ->
            % atm workflow execution may have been abruptly interrupted by e.g.
            % provider restart which resulted in stale `items_in_processing`
            UpdatedProcessedItems = ItemsProcessed + ItemsInProcessing,
            UpdatedFailedItems = ItemsFailed + ItemsInProcessing,

            {ok, AtmTaskExecution#atm_task_execution{
                status = case AbortingReason of
                    pause -> ?PAUSED_STATUS;
                    interrupt -> ?INTERRUPTED_STATUS;
                    failure -> ?FAILED_STATUS;
                    cancel -> ?CANCELLED_STATUS
                end,
                items_in_processing = 0,
                items_processed = UpdatedProcessedItems,
                items_failed = UpdatedFailedItems
            }};

        (_) ->
            {error, already_ended}
    end).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec should_overwrite_aborting_reason(
    atm_task_execution:aborting_reason(),
    atm_task_execution:aborting_reason()
) ->
    boolean().
should_overwrite_aborting_reason(PrevReason, NewReason) ->
    aborting_reason_priority(NewReason) > aborting_reason_priority(PrevReason).


%% @private
-spec aborting_reason_priority(atm_task_execution:aborting_reason()) ->
    non_neg_integer().
aborting_reason_priority(pause) -> 0;
aborting_reason_priority(interrupt) -> 1;
aborting_reason_priority(failure) -> 2;
aborting_reason_priority(cancel) -> 3.


%% @private
-spec apply_diff(atm_task_execution:id(), atm_task_execution:diff()) ->
    {ok, atm_task_execution:doc()} | {error, term()}.
apply_diff(AtmTaskExecutionId, Diff) ->
    case atm_task_execution:update(AtmTaskExecutionId, Diff) of
        {ok, AtmTaskExecutionDoc} = Result ->
            handle_status_change(AtmTaskExecutionDoc),
            Result;
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates atm task execution status stored in atm parallel box execution of
%% corresponding atm lane run if it was changed in task execution doc.
%%
%% NOTE: normally this should happen only after lane run processing has started
%% and concrete 'run_num' was set for all its tasks (it is not possible to
%% foresee what it will be beforehand as previous lane run may retried numerous
%% times). However, in case of failure/interruption during lane run preparation
%% after task execution documents have been created, this function will also
%% be called. Despite not having 'run_num' set there is no ambiguity to which
%% lane run it belongs as it can only happen to the newest run of given lane.
%% @end
%%--------------------------------------------------------------------
-spec handle_status_change(atm_task_execution:doc()) -> ok.
handle_status_change(#document{value = #atm_task_execution{status_changed = false}}) ->
    ok;

handle_status_change(#document{
    key = AtmTaskExecutionId,
    value = #atm_task_execution{
        workflow_execution_id = AtmWorkflowExecutionId,
        lane_index = AtmLaneIndex,
        run_num = RunNumOrUndefined,
        parallel_box_index = AtmParallelBoxIndex,
        status = NewStatus,
        status_changed = true
    }
}) ->
    RunSelector = utils:ensure_defined(RunNumOrUndefined, current),

    ok = atm_lane_execution_status:handle_task_status_change(
        AtmWorkflowExecutionId, {AtmLaneIndex, RunSelector}, AtmParallelBoxIndex,
        AtmTaskExecutionId, NewStatus
    ).
