%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles the lane execution process (for information about
%%% state machine consult 'atm_lane_execution_status.erl').
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lane_execution_handler).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include("workflow_engine.hrl").

%% API
-export([
    prepare/3,
    handle_ended/3
]).


-type teardown_ctx() :: #atm_lane_execution_run_teardown_ctx{}.

-export_type([teardown_ctx/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec prepare(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    workflow_engine:lane_spec() | no_return().
prepare(AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    try
        AtmWorkflowExecutionDoc = atm_lane_execution_status:handle_preparing(
            AtmLaneRunSelector, AtmWorkflowExecutionId
        ),

        NewAtmWorkflowExecutionDoc = atm_lane_execution_factory:create_run(
            AtmLaneRunSelector, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx
        ),
        AtmLaneExecutionSpec = initiate_lane_run(
            AtmLaneRunSelector, NewAtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx
        ),

        #document{value = AtmWorkflowExecution} = atm_lane_execution_status:handle_enqueued(
            AtmLaneRunSelector, AtmWorkflowExecutionId
        ),
        call_current_lane_run_pre_execution_hooks(AtmWorkflowExecution),

        AtmLaneExecutionSpec
    catch Type:Reason:Stacktrace ->
        atm_lane_execution_status:handle_aborting(AtmLaneRunSelector, AtmWorkflowExecutionId, failure),
        handle_ended(AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx),
        throw(?atm_examine_error(Type, Reason, Stacktrace))
    end.


-spec handle_ended(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    workflow_handler:lane_ended_callback_result() | no_return().
handle_ended(AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    {IsRetryScheduled, NextAtmWorkflowExecution = #atm_workflow_execution{
        current_lane_index = NextAtmLaneIndex,
        current_run_num = NextRunNum,
        lanes_count = AtmLanesCount
    }} = end_lane_run(AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx),

    call_current_lane_run_pre_execution_hooks(NextAtmWorkflowExecution),  %% for next lane run
    {ok, NextLaneRun} = atm_lane_execution:get_run({current, current}, NextAtmWorkflowExecution),

    case atm_lane_execution_status:status_to_phase(NextLaneRun#atm_lane_execution_run.status) of
        ?ENDED_PHASE ->
            ?END_EXECUTION;
        _ ->
            NextAtmLaneRunSelector = case IsRetryScheduled of
                true ->
                    {NextAtmLaneIndex, NextRunNum};
                false ->
                    % Due to limitations of workflow engine (how it handles cache
                    % of prepared in advance lanes) instead of specifying known
                    % run number 'current' placeholder must be specified
                    {NextAtmLaneIndex, current}
            end,
            AtmLaneRunToPrepareInAdvanceSelector = case NextAtmLaneIndex < AtmLanesCount of
                true -> {NextAtmLaneIndex + 1, current};
                false -> undefined
            end,
            ?CONTINUE(NextAtmLaneRunSelector, AtmLaneRunToPrepareInAdvanceSelector)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec initiate_lane_run(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    workflow_engine:lane_spec() | no_return().
initiate_lane_run(AtmLaneRunSelector, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx) ->
    AtmWorkflowExecution = AtmWorkflowExecutionDoc#document.value,
    AtmWorkflowExecutionEnv = atm_workflow_execution_ctx:get_env(AtmWorkflowExecutionCtx),

    try
        {ok, #atm_lane_execution_run{
            iterated_store_id = AtmIteratedStoreId,
            exception_store_id = ExceptionStoreId,
            parallel_boxes = AtmParallelBoxExecutions
        }} = atm_lane_execution:get_run(AtmLaneRunSelector, AtmWorkflowExecution),

        {AtmParallelBoxExecutionSpecs, AtmWorkflowExecutionEnvDiff} = atm_parallel_box_execution:initiate_all(
            AtmWorkflowExecutionCtx, AtmParallelBoxExecutions
        ),
        {ok, #atm_store{container = AtmLaneRunExceptionStoreContainer}} = atm_store_api:get(ExceptionStoreId),
        NewAtmWorkflowExecutionEnv = atm_workflow_execution_env:set_lane_run_exception_store_container(
            AtmLaneRunExceptionStoreContainer, AtmWorkflowExecutionEnv
        ),
        #atm_lane_schema{store_iterator_spec = AtmStoreIteratorSpec} = atm_lane_execution:get_schema(
            AtmLaneRunSelector, AtmWorkflowExecution
        ),

        #{
            execution_context => AtmWorkflowExecutionEnvDiff(NewAtmWorkflowExecutionEnv),
            parallel_boxes => AtmParallelBoxExecutionSpecs,
            iterator => atm_store_api:acquire_iterator(AtmIteratedStoreId, AtmStoreIteratorSpec)
        }
    catch Type:Reason:Stacktrace ->
        throw(?ERROR_ATM_LANE_EXECUTION_INITIATION_FAILED(
            atm_lane_execution:get_schema_id(AtmLaneRunSelector, AtmWorkflowExecution),
            ?atm_examine_error(Type, Reason, Stacktrace)
        ))
    end.


%% @private
-spec end_lane_run(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    {boolean(), atm_workflow_execution:record()}.
end_lane_run(AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    {ok, #document{value = AtmWorkflowExecution = #atm_workflow_execution{
        current_lane_index = CurrentAtmLaneIndex,
        current_run_num = CurrentRunNum
    }}} = atm_workflow_execution:get(AtmWorkflowExecutionId),

    IsCurrentAtmLaneRun = atm_lane_execution:is_current_lane_run(
        AtmLaneRunSelector, AtmWorkflowExecution
    ),
    {ok, CurrentRun} = atm_lane_execution:get_run(AtmLaneRunSelector, AtmWorkflowExecution),
    AtmParallelBoxExecutions = CurrentRun#atm_lane_execution_run.parallel_boxes,

    unfreeze_iterated_store_in_case_of_global_store(CurrentRun, AtmWorkflowExecutionCtx),
    freeze_exception_store(CurrentRun),
    atm_parallel_box_execution:ensure_all_ended(AtmParallelBoxExecutions),

    #document{value = NewAtmWorkflowExecution = #atm_workflow_execution{
        current_lane_index = NextAtmLaneIndex,
        current_run_num = NextRunNum
    }} = atm_lane_execution_status:handle_ended(AtmLaneRunSelector, AtmWorkflowExecutionId),

    IsRetryScheduled = IsCurrentAtmLaneRun andalso
        NextAtmLaneIndex == CurrentAtmLaneIndex andalso
        NextRunNum == CurrentRunNum + 1,

    AtmLaneExecutionRunTeardownCtx = #atm_lane_execution_run_teardown_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx,
        is_retry_scheduled = IsRetryScheduled
    },
    atm_parallel_box_execution:teardown_all(AtmLaneExecutionRunTeardownCtx, AtmParallelBoxExecutions),

    {IsRetryScheduled, NewAtmWorkflowExecution}.


%% @private
-spec unfreeze_iterated_store_in_case_of_global_store(
    atm_lane_execution:run(),
    atm_workflow_execution_ctx:record()
) ->
    ok.
unfreeze_iterated_store_in_case_of_global_store(
    #atm_lane_execution_run{exception_store_id = undefined},
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


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Performs any operation that needs to be done right before current lane run
%% execution (all previous preparations must have been ended and lane run ready
%% to execute).
%% @end
%%--------------------------------------------------------------------
-spec call_current_lane_run_pre_execution_hooks(atm_workflow_execution:record()) ->
    ok.
call_current_lane_run_pre_execution_hooks(AtmWorkflowExecution = #atm_workflow_execution{
    current_run_num = CurrentRunNum
}) ->
    case atm_lane_execution:get_run({current, current}, AtmWorkflowExecution) of
        {ok, #atm_lane_execution_run{status = ?ENQUEUED_STATUS, run_num = CurrentRunNum} = Run} ->
            atm_parallel_box_execution:set_tasks_run_num(
                CurrentRunNum, Run#atm_lane_execution_run.parallel_boxes
            ),
            atm_store_api:freeze(Run#atm_lane_execution_run.iterated_store_id);
        _ ->
            % execution must have ended or lane run is still not ready
            % (either not fully prepared or previous run is still ongoing)
            ok
    end.
