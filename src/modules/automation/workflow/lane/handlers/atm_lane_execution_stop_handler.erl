%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021-2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles the lane execution stopping process (for information
%%% about state machine @see 'atm_lane_execution_status.erl').
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lane_execution_stop_handler).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include("workflow_engine.hrl").

%% API
-export([
    init_stop/3,
    handle_stopping/4,
    handle_stopped/3
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec init_stop(
    atm_lane_execution:lane_run_selector(),
    atm_lane_execution:run_stopping_reason(),
    atm_workflow_execution_ctx:record()
) ->
    {ok, stopping | stopped} | errors:error().
init_stop(OriginalAtmLaneRunSelector, Reason, OriginalAtmWorkflowExecutionCtx) ->
    AtmWorkflowExecutionId = atm_workflow_execution_ctx:get_workflow_execution_id(
        OriginalAtmWorkflowExecutionCtx
    ),
    case atm_lane_execution_status:handle_stopping(
        OriginalAtmLaneRunSelector, AtmWorkflowExecutionId, Reason
    ) of
        {ok, AtmWorkflowExecutionDoc = #document{value = AtmWorkflowExecution}} ->
            % resolve selector in case it is {current. current} (aka stopping entire execution)
            {AtmLaneRunSelector, AtmWorkflowExecutionCtx} = ensure_task_selector_registry_up_to_date(
                AtmWorkflowExecutionDoc, OriginalAtmLaneRunSelector, OriginalAtmWorkflowExecutionCtx
            ),
            log_init_stop(AtmLaneRunSelector, Reason, AtmWorkflowExecutionCtx, AtmWorkflowExecution),

            handle_stopping(
                AtmLaneRunSelector, Reason, AtmWorkflowExecutionCtx, AtmWorkflowExecutionDoc
            );

        {error, already_stopping} when Reason =:= cancel; Reason =:= pause ->
            % ignore user trying to stop already stopping execution
            {ok, stopping};

        {error, already_stopping} ->
            % repeat stopping procedure just in case if previously it wasn't finished
            % (e.g. provider abrupt shutdown and restart)
            {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),
            {AtmLaneRunSelector, AtmWorkflowExecutionCtx} = ensure_task_selector_registry_up_to_date(
                AtmWorkflowExecutionDoc, OriginalAtmLaneRunSelector, OriginalAtmWorkflowExecutionCtx
            ),
            handle_stopping(
                AtmLaneRunSelector, Reason, AtmWorkflowExecutionCtx, AtmWorkflowExecutionDoc
            );

        {error, _} = Error ->
            Error
    end.


-spec handle_stopping(
    atm_lane_execution:lane_run_selector(),
    atm_lane_execution:run_stopping_reason(),
    atm_workflow_execution_ctx:record(),
    atm_workflow_execution:doc()
) ->
    {ok, stopping | stopped}.
handle_stopping(AtmLaneRunSelector, Reason, AtmWorkflowExecutionCtx, AtmWorkflowExecutionDoc) ->
    AtmWorkflowExecution = AtmWorkflowExecutionDoc#document.value,
    PrevStatus = AtmWorkflowExecution#atm_workflow_execution.prev_status,
    IsCurrentLaneRun = atm_lane_execution:is_current_lane_run(AtmLaneRunSelector, AtmWorkflowExecution),

    case atm_workflow_execution_status:status_to_phase(PrevStatus) of
        ?SUSPENDED_PHASE when IsCurrentLaneRun ->
            handle_suspended_current_lane_run_stopping(
                AtmLaneRunSelector, Reason, AtmWorkflowExecutionCtx, AtmWorkflowExecutionDoc
            );

        _ when IsCurrentLaneRun ->
            handle_running_current_lane_run_stopping(
                AtmLaneRunSelector, Reason, AtmWorkflowExecutionCtx, AtmWorkflowExecutionDoc
            );

        _ ->
            init_stop_tasks(AtmLaneRunSelector, Reason, AtmWorkflowExecutionCtx, AtmWorkflowExecution),
            {ok, stopping}
    end.


-spec handle_stopped(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    workflow_handler:lane_stopped_callback_result() | no_return().
handle_stopped(AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    {IsRetryScheduled, NextAtmWorkflowExecution = #atm_workflow_execution{
        current_lane_index = NextAtmLaneIndex,
        current_run_num = NextRunNum
    }} = stop_lane_run(AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx),

    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    ?atm_workflow_info(Logger, ?ATM_WORKFLOW_LANE_RUN_LOG(AtmLaneRunSelector, <<"Stopped.">>)),
    IsRetryScheduled andalso ?atm_workflow_info(Logger, ?ATM_WORKFLOW_LANE_RUN_LOG(
        AtmLaneRunSelector, <<"Scheduled automatic retry.">>, #{
            <<"scheduledLaneRunSelector">> => ?lane_run_selector_json({NextAtmLaneIndex, NextRunNum}),
            <<"retriesLeft">> => begin
                {ok, NextLane} = atm_lane_execution:get(NextAtmLaneIndex, NextAtmWorkflowExecution),
                NextLane#atm_lane_execution.retries_left
            end
        }
    )),

    atm_lane_execution_hooks_handler:exec_current_lane_run_pre_execution_hooks(NextAtmWorkflowExecution),

    infer_next_execution_policy(IsRetryScheduled, NextAtmWorkflowExecution).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Tries to resolve lane run selector (especially in case of '{current, current}'
%% given when concrete lane run is not known beforehand) and ensures that task
%% selector registry is actual for resolved lane run.
%% @end
%%-------------------------------------------------------------------
-spec ensure_task_selector_registry_up_to_date(
    atm_workflow_execution:doc(),
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution_ctx:record()
) ->
    {atm_lane_execution:lane_run_selector(), atm_workflow_execution_ctx:record()}.
ensure_task_selector_registry_up_to_date(
    AtmWorkflowExecutionDoc = #document{value = AtmWorkflowExecution},
    OriginalAtmLaneRunSelector,
    OriginalAtmWorkflowExecutionCtx
) ->
    AtmLaneRunSelector = atm_lane_execution:try_resolving_lane_run_selector(
        OriginalAtmLaneRunSelector, AtmWorkflowExecution
    ),
    AtmWorkflowExecutionEnv = atm_workflow_execution_env:ensure_task_selector_registry_up_to_date(
        AtmWorkflowExecutionDoc, AtmLaneRunSelector, atm_workflow_execution_ctx:get_env(
            OriginalAtmWorkflowExecutionCtx
        )
    ),
    AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:set_env(
        AtmWorkflowExecutionEnv, OriginalAtmWorkflowExecutionCtx
    ),
    {AtmLaneRunSelector, AtmWorkflowExecutionCtx}.


%% @private
-spec log_init_stop(
    atm_lane_execution:lane_run_selector(),
    atm_lane_execution:run_stopping_reason(),
    atm_workflow_execution_ctx:record(),
    atm_workflow_execution:record()
) ->
    ok.
log_init_stop(AtmLaneRunSelector, Reason, AtmWorkflowExecutionCtx, AtmWorkflowExecution) ->
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),

    case atm_lane_execution:is_current_lane_run(AtmLaneRunSelector, AtmWorkflowExecution) of
        true ->
            ?atm_workflow_notice(Logger, #atm_workflow_log_schema{
                description = <<"Stopping execution...">>,
                details = #{<<"reason">> => Reason}
            });
        false ->
            ok
    end,

    ?atm_workflow_info(Logger, ?ATM_WORKFLOW_LANE_RUN_LOG(
        AtmLaneRunSelector, <<"Initiating stop...">>, #{<<"reason">> => Reason}
    )).


%% @private
-spec handle_suspended_current_lane_run_stopping(
    atm_lane_execution:lane_run_selector(),
    atm_lane_execution:run_stopping_reason(),
    atm_workflow_execution_ctx:record(),
    atm_workflow_execution:doc()
) ->
    {ok, stopped}.
handle_suspended_current_lane_run_stopping(
    AtmLaneRunSelector,
    Reason,
    AtmWorkflowExecutionCtx,
    #document{key = AtmWorkflowExecutionId, value = AtmWorkflowExecution}
) ->
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),

    % Stopping suspended execution - since there was no active process
    % handling it, manual cleanup and callback calls are necessary
    workflow_engine:cleanup_execution(AtmWorkflowExecutionId),
    ?atm_workflow_debug(Logger, ?ATM_WORKFLOW_ENGINE_LOG(<<"Cleaned up execution">>)),

    init_stop_tasks(AtmLaneRunSelector, Reason, AtmWorkflowExecutionCtx, AtmWorkflowExecution),

    atm_lane_execution_status:handle_stopped(AtmLaneRunSelector, AtmWorkflowExecutionId),
    ?atm_workflow_info(Logger, ?ATM_WORKFLOW_LANE_RUN_LOG(
        AtmLaneRunSelector, <<"Stopped previously suspended.">>
    )),

    {ok, stopped}.


%% @private
-spec handle_running_current_lane_run_stopping(
    atm_lane_execution:lane_run_selector(),
    atm_lane_execution:run_stopping_reason(),
    atm_workflow_execution_ctx:record(),
    atm_workflow_execution:doc()
) ->
    {ok, stopping}.
handle_running_current_lane_run_stopping(AtmLaneRunSelector, Reason, AtmWorkflowExecutionCtx, #document{
    key = AtmWorkflowExecutionId,
    value = AtmWorkflowExecution
}) ->
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),

    % Currently executed lane run stopping == entire workflow execution is stopping
    ?atm_workflow_debug(Logger, ?ATM_WORKFLOW_ENGINE_LOG(<<"Initiating cancel procedure...">>)),
    workflow_engine:init_cancel_procedure(AtmWorkflowExecutionId),
    ?atm_workflow_debug(Logger, ?ATM_WORKFLOW_ENGINE_LOG(<<"Initiated cancel procedure.">>)),

    init_stop_tasks(AtmLaneRunSelector, Reason, AtmWorkflowExecutionCtx, AtmWorkflowExecution),

    workflow_engine:finish_cancel_procedure(AtmWorkflowExecutionId),
    ?atm_workflow_debug(Logger, ?ATM_WORKFLOW_ENGINE_LOG(<<"Finished cancel procedure.">>)),

    {ok, stopping}.


%% @private
-spec init_stop_tasks(
    atm_lane_execution:lane_run_selector(),
    atm_lane_execution:run_stopping_reason(),
    atm_workflow_execution_ctx:record(),
    atm_workflow_execution:record()
) ->
    ok | no_return().
init_stop_tasks(
    AtmLaneRunSelector,
    AtmLaneRunStoppingReason,
    AtmWorkflowExecutionCtx,
    AtmWorkflowExecution
) ->
    AtmTaskExecutionStoppingReason = infer_task_stopping_reason(AtmLaneRunStoppingReason),

    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    ?atm_workflow_debug(Logger, #atm_workflow_log_schema{
        selector = {lane_run, AtmLaneRunSelector},
        description = <<"Initiating tasks stop...">>,
        details = #{<<"taskStoppingReason">> => AtmTaskExecutionStoppingReason}
    }),

    {ok, AtmLaneRun} = atm_lane_execution:get_run(AtmLaneRunSelector, AtmWorkflowExecution),

    atm_parallel_box_execution:init_stop_all(
        AtmWorkflowExecutionCtx,
        AtmTaskExecutionStoppingReason,
        AtmLaneRun#atm_lane_execution_run.parallel_boxes
    ).


%% @private
-spec infer_task_stopping_reason(atm_lane_execution:run_stopping_reason()) ->
    atm_task_execution:stopping_reason().
infer_task_stopping_reason(failure) -> interrupt;
infer_task_stopping_reason(crash) -> interrupt;
infer_task_stopping_reason(op_worker_stopping) -> pause;
infer_task_stopping_reason(Reason) -> Reason.


%% @private
-spec stop_lane_run(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    {boolean(), atm_workflow_execution:record()}.
stop_lane_run(AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    {ok, #document{value = AtmWorkflowExecution}} = atm_workflow_execution:get(AtmWorkflowExecutionId),
    {ok, AtmLaneRun} = atm_lane_execution:get_run(AtmLaneRunSelector, AtmWorkflowExecution),
    AtmParallelBoxExecutions = AtmLaneRun#atm_lane_execution_run.parallel_boxes,

    atm_parallel_box_execution:ensure_all_stopped(AtmParallelBoxExecutions, AtmWorkflowExecutionCtx),
    unfreeze_iterated_store_in_case_of_global_store(AtmLaneRun, AtmWorkflowExecutionCtx),
    freeze_exception_store(AtmLaneRun),

    #document{value = NewAtmWorkflowExecution} = atm_lane_execution_status:handle_stopped(
        AtmLaneRunSelector, AtmWorkflowExecutionId
    ),
    atm_parallel_box_execution:teardown_all(AtmWorkflowExecutionCtx, AtmParallelBoxExecutions),

    IsRetryScheduled = is_retry_scheduled(AtmLaneRunSelector, AtmWorkflowExecution, NewAtmWorkflowExecution),

    {IsRetryScheduled, NewAtmWorkflowExecution}.


%% @private
-spec unfreeze_iterated_store_in_case_of_global_store(
    atm_lane_execution:run(),
    atm_workflow_execution_ctx:record()
) ->
    ok.
unfreeze_iterated_store_in_case_of_global_store(
    #atm_lane_execution_run{iterated_store_id = undefined},
    _AtmWorkflowExecutionCtx
) ->
    ok;
unfreeze_iterated_store_in_case_of_global_store(
    #atm_lane_execution_run{iterated_store_id = AtmIteratedStoreId},
    AtmWorkflowExecutionCtx
) ->
    case atm_workflow_execution_ctx:is_global_store(AtmIteratedStoreId, AtmWorkflowExecutionCtx) of
        true -> atm_store_api:unfreeze(AtmIteratedStoreId);
        false -> ok
    end.


%% @private
-spec freeze_exception_store(atm_lane_execution:run()) -> ok.
freeze_exception_store(#atm_lane_execution_run{exception_store_id = undefined}) ->
    ok;
freeze_exception_store(#atm_lane_execution_run{exception_store_id = AtmExceptionStoreId}) ->
    atm_store_api:freeze(AtmExceptionStoreId).


%% @private
-spec is_retry_scheduled(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:record(),
    atm_workflow_execution:record()
) ->
    boolean().
is_retry_scheduled(
    StoppedAtmLaneRunSelector,
    PrevAtmWorkflowExecution = #atm_workflow_execution{
        current_lane_index = PrevAtmLaneIndex,
        current_run_num = PrevRunNum
    },
    _NewAtmWorkflowExecution = #atm_workflow_execution{
        current_lane_index = NewAtmLaneIndex,
        current_run_num = NewRunNum
    }
) ->
    atm_lane_execution:is_current_lane_run(StoppedAtmLaneRunSelector, PrevAtmWorkflowExecution)
        andalso NewAtmLaneIndex == PrevAtmLaneIndex
        andalso NewRunNum == PrevRunNum + 1.


%% @private
-spec infer_next_execution_policy(boolean(), atm_workflow_execution:record()) ->
    workflow_handler:lane_stopped_callback_result().
infer_next_execution_policy(IsRetryScheduled, NextAtmWorkflowExecution = #atm_workflow_execution{
    current_lane_index = NextAtmLaneIndex,
    current_run_num = NextRunNum,
    lanes_count = AtmLanesCount
}) ->
    {ok, NextLaneRun} = atm_lane_execution:get_run({current, current}, NextAtmWorkflowExecution),

    case atm_lane_execution_status:status_to_phase(NextLaneRun#atm_lane_execution_run.status) of
        ?SUSPENDED_PHASE ->
            ?END_EXECUTION;
        ?ENDED_PHASE ->
            ?END_EXECUTION;
        _ ->
            NextAtmLaneRunSelector = case IsRetryScheduled of
                true ->
                    {NextAtmLaneIndex, NextRunNum};
                false ->
                    % Because this lane run was already introduced as `{NextAtmLaneIndex, current}`
                    % to workflow engine (when specifying lane run to be prepared in advance before
                    % previous lane run execution) the same id must be used also now (for now there
                    % is no way to tell workflow engine that different ids points to the same lane
                    % run).
                    {NextAtmLaneIndex, current}
            end,
            AtmLaneRunToPrepareInAdvanceSelector = case NextAtmLaneIndex < AtmLanesCount of
                true -> {NextAtmLaneIndex + 1, current};
                false -> undefined
            end,
            ?CONTINUE(NextAtmLaneRunSelector, AtmLaneRunToPrepareInAdvanceSelector)
    end.
