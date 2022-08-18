%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions that handle atm task execution status
%%% transitions according (with some exceptions described below) to following
%%% state machine:
%%%                                       |      <WAITING PHASE (initiation)>
%%%           resuming execution          v
%%%     ------------------------------> +-----------+
%%%   /                                 |  PENDING  |
%%%  |              ------------------- +-----------+
%%%  |            /                           |
%%%  |    ending task execution               |
%%%  |      with no item ever            first item
%%%  |     scheduled to process      scheduled to process
%%%  |           |                   /
%%% =|===========|==================/==========================================================================
%%%  |           |                  |                   <ONGOING PHASE>
%%%  |           |                  |                                           ____
%%%  |           |                  v                                         /      \ overriding ^stopping
%%%  |           |            +----------+       ^stopping        +------------+     /       reason
%%%  |           |            |  ACTIVE  | ---------------------> |  STOPPING  | <--
%%%  |           |            +----------+                        +------------+
%%%  |           |                  |                                   |
%%%  |           |         ending task execution                        |
%%%  |           |            with all items           ending task execution with ^stopping reason
%%%  |           |               processed            /            /           \                  \
%%%  |           |           /              \       1*            /             \                 4*
%%%  |           |      successfully       else     |            2*              3*                |
%%%  |           |           |               |      |            |                |                |
%%%  |           |           |               |      |            |                |                |
%%% =|===========|===========|===============|======|============|================|================|===========
%%%  |           |           |               |      |            |                |                |
%%%  |           |           |               |      |            |                |                |
%%%  |           v           v               v      v            v                V                v
%%%  |     +-----------+   +----------+     +--------+    +-------------+    +-----------+        +--------+
%%%  |     |  SKIPPED  |   | FINISHED |     | FAILED |    | INTERRUPTED |    | CANCELLED |        | PAUSED |     %% TODO pause -> cancel/stopping?
%%%  |     +-----------+   +----------+     +--------+    +-------------+    +-----------+        +--------+
%%%  |           |                                              |                |                 |
%%%   \          |                                              |                |                /
%%%     --------------------------------------------------------o----------------o---------------
%%%
%%%                                                  <ENDED PHASE (teardown)>
%%%
%%% ^stopping - common step when halting execution due to:
%%% 1* - failure severe enough to cause stopping of entire automation workflow execution
%%%      (e.g. error when processing uncorrelated results).
%%% 2* - abrupt interruption when some other component (e.g task or external service like OpenFaaS)
%%%      has failed and entire automation workflow execution is being stopped.
%%% 3* - user cancelling entire automation workflow execution.
%%% 4* - user pausing entire automation workflow execution.
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
    handle_items_dequeued/2,
    handle_items_failed/2,

    handle_stopping/2,
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
is_transition_allowed(?ACTIVE_STATUS, ?FAILED_STATUS) -> true;
is_transition_allowed(?ACTIVE_STATUS, ?STOPPING_STATUS) -> true;
is_transition_allowed(?STOPPING_STATUS, ?CANCELLED_STATUS) -> true;
is_transition_allowed(?STOPPING_STATUS, ?FAILED_STATUS) -> true;
is_transition_allowed(?STOPPING_STATUS, ?INTERRUPTED_STATUS) -> true;
is_transition_allowed(?STOPPING_STATUS, ?PAUSED_STATUS) -> true;

is_transition_allowed(_, _) -> false.  %% TODO pause -> cancel/stopping?


-spec is_ended(atm_task_execution:status()) -> boolean().
is_ended(?FINISHED_STATUS) -> true;
is_ended(?SKIPPED_STATUS) -> true;
is_ended(?CANCELLED_STATUS) -> true;
is_ended(?FAILED_STATUS) -> true;
is_ended(?INTERRUPTED_STATUS) -> true;
is_ended(?PAUSED_STATUS) -> true;
is_ended(_) -> false.


-spec handle_items_in_processing(atm_task_execution:id(), pos_integer()) ->
    {ok, atm_task_execution:doc()} | {error, task_stopping} | {error, task_ended}.
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

        (#atm_task_execution{status = ?STOPPING_STATUS}) ->
            {error, task_stopping};

        (_) ->
            {error, task_ended}
    end).


-spec handle_item_processed(atm_task_execution:id()) -> ok.
handle_item_processed(AtmTaskExecutionId) ->
    {ok, _} = apply_diff(AtmTaskExecutionId, fun(#atm_task_execution{
        items_in_processing = ItemsInProcessing,
        items_processed = ItemsProcessed
    } = AtmTaskExecution) ->
        {ok, AtmTaskExecution#atm_task_execution{
            items_in_processing = ItemsInProcessing - 1,
            items_processed = ItemsProcessed + 1
        }}
    end),
    ok.


-spec handle_items_dequeued(atm_task_execution:id(), pos_integer()) ->
    {ok, atm_task_execution:doc()} | {error, task_not_stopping}.
handle_items_dequeued(AtmTaskExecutionId, ItemsNum) ->
    apply_diff(AtmTaskExecutionId, fun
        (AtmTaskExecution = #atm_task_execution{
            status = ?STOPPING_STATUS,
            items_in_processing = ItemsInProcessing
        }) ->
            {ok, AtmTaskExecution#atm_task_execution{
                items_in_processing = ItemsInProcessing - ItemsNum
            }};

        (_) ->
            {error, task_not_stopping}
    end).


-spec handle_items_failed(atm_task_execution:id(), pos_integer()) -> ok.
handle_items_failed(AtmTaskExecutionId, ItemsNum) ->
    {ok, _} = apply_diff(AtmTaskExecutionId, fun(#atm_task_execution{
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


-spec handle_stopping(
    atm_task_execution:id(),
    atm_task_execution:stopping_reason()
) ->
    {ok, atm_task_execution:doc()} | {error, task_ended}.
handle_stopping(AtmTaskExecutionId, Reason) ->
    apply_diff(AtmTaskExecutionId, fun
        (AtmTaskExecution = #atm_task_execution{status = ?PENDING_STATUS}) ->
            {ok, AtmTaskExecution#atm_task_execution{status = ?SKIPPED_STATUS}};

        (AtmTaskExecution = #atm_task_execution{status = ?ACTIVE_STATUS}) ->
            {ok, AtmTaskExecution#atm_task_execution{
                status = ?STOPPING_STATUS,
                stopping_reason = Reason
            }};

        (AtmTaskExecution = #atm_task_execution{
            status = ?STOPPING_STATUS,
            stopping_reason = PrevReason
        }) ->
            {ok, AtmTaskExecution#atm_task_execution{
                stopping_reason = case should_overwrite_stopping_reason(PrevReason, Reason) of
                    true -> Reason;
                    false -> PrevReason
                end
            }};

        (_) ->
            {error, task_ended}
    end).


-spec handle_ended(atm_task_execution:id()) ->
    {ok, atm_task_execution:doc()} | {error, task_ended}.
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
            status = ?STOPPING_STATUS,
            stopping_reason = StoppingReason,
            items_in_processing = ItemsInProcessing,
            items_processed = ItemsProcessed,
            items_failed = ItemsFailed
        }) ->
            % atm workflow execution may have been abruptly interrupted by e.g.
            % provider restart which resulted in stale `items_in_processing`
            UpdatedProcessedItems = ItemsProcessed + ItemsInProcessing,
            UpdatedFailedItems = ItemsFailed + ItemsInProcessing,

            {ok, AtmTaskExecution#atm_task_execution{
                status = case StoppingReason of
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
            {error, task_ended}
    end).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec should_overwrite_stopping_reason(
    atm_task_execution:stopping_reason(),
    atm_task_execution:stopping_reason()
) ->
    boolean().
should_overwrite_stopping_reason(PrevReason, NewReason) ->
    stopping_reason_priority(NewReason) > stopping_reason_priority(PrevReason).


%% @private
-spec stopping_reason_priority(atm_task_execution:stopping_reason()) ->
    non_neg_integer().
stopping_reason_priority(pause) -> 0;
stopping_reason_priority(interrupt) -> 1;
stopping_reason_priority(failure) -> 2;
stopping_reason_priority(cancel) -> 3.


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
