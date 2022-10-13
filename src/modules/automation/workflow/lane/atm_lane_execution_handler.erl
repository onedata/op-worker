%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021-2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles the lane execution process (for information about
%%% state machine @see 'atm_lane_execution_status.erl').
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lane_execution_handler).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include("workflow_engine.hrl").

%% API
-export([
    prepare/3,
    stop/3,
    resume/3,
    handle_stopped/3
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec prepare(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    {ok, workflow_engine:lane_spec()} | error.
prepare(AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    case atm_lane_execution_status:handle_preparing(AtmLaneRunSelector, AtmWorkflowExecutionId) of
        {ok, AtmWorkflowExecutionDoc} ->
            prepare_lane_run(AtmLaneRunSelector, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx);

        % Execution must have been suspended before lane run preparation finished but after
        % lane run components has been created - resume it
        ?ERROR_ATM_INVALID_STATUS_TRANSITION(?RESUMING_STATUS, ?PREPARING_STATUS) ->
            {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),

            case resume_lane_run(AtmLaneRunSelector, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx) of
                {ok, LaneSpec} ->
                    LaneIterator = acquire_iterator(AtmLaneRunSelector, AtmWorkflowExecutionDoc),
                    {ok, LaneSpec#{iterator => LaneIterator}};
                error ->
                    error
            end;

        ?ERROR_ATM_WORKFLOW_EXECUTION_ABORTING ->
            handle_setup_exception(
                execution_stopping, AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx
            )
    end.


-spec stop(
    atm_lane_execution:lane_run_selector(),
    atm_lane_execution:run_stopping_reason(),
    atm_workflow_execution_ctx:record()
) ->
    {ok, stopping | stopped} | errors:error().
stop(AtmLaneRunSelector, Reason, AtmWorkflowExecutionCtx) ->
    AtmWorkflowExecutionId = atm_workflow_execution_ctx:get_workflow_execution_id(
        AtmWorkflowExecutionCtx
    ),
    case atm_lane_execution_status:handle_stopping(AtmLaneRunSelector, AtmWorkflowExecutionId, Reason) of
        {ok, AtmWorkflowExecutionDoc} ->
            handle_lane_run_stopping(
                AtmLaneRunSelector, Reason, AtmWorkflowExecutionCtx, AtmWorkflowExecutionDoc
            );

        {error, already_stopping} when Reason =:= cancel; Reason =:= pause ->
            % ignore user trying to stop already stopping execution
            {ok, stopping};

        {error, already_stopping} ->
            % repeat stopping procedure just in case if previously it wasn't finished
            % (e.g. provider abrupt shutdown and restart)
            {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),
            handle_lane_run_stopping(
                AtmLaneRunSelector, Reason, AtmWorkflowExecutionCtx, AtmWorkflowExecutionDoc
            );

        {error, _} = Error ->
            Error
    end.


-spec resume(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    {ok, workflow_engine:lane_spec()} | error.
resume(AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),
    resume_lane_run(AtmLaneRunSelector, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx).


-spec handle_stopped(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    workflow_handler:lane_stopped_callback_result() | no_return().
handle_stopped(AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    {IsRetryScheduled, NextAtmWorkflowExecution = #atm_workflow_execution{
        current_lane_index = NextAtmLaneIndex,
        current_run_num = NextRunNum,
        lanes_count = AtmLanesCount
    }} = end_lane_run(AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx),

    call_current_lane_run_pre_execution_hooks(NextAtmWorkflowExecution),  %% for next lane run
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


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec prepare_lane_run(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    {ok, workflow_engine:lane_spec()} | error.
prepare_lane_run(AtmLaneRunSelector, AtmWorkflowExecutionDoc0, AtmWorkflowExecutionCtx) ->
    AtmWorkflowExecutionId = AtmWorkflowExecutionDoc0#document.key,

    try
        AtmWorkflowExecutionDoc1 = atm_lane_execution_factory:create_run(
            AtmLaneRunSelector, AtmWorkflowExecutionDoc0, AtmWorkflowExecutionCtx
        ),
        BasicLaneSpec = initiate_lane_run(
            AtmLaneRunSelector,
            AtmWorkflowExecutionDoc1,
            AtmWorkflowExecutionCtx,
            fun atm_parallel_box_execution:start_all/2
        ),
        LaneIterator = acquire_iterator(AtmLaneRunSelector, AtmWorkflowExecutionDoc1),
        LaneSpec = BasicLaneSpec#{iterator => LaneIterator},

        AtmWorkflowExecutionDoc2 = atm_lane_execution_status:handle_enqueued(
            AtmLaneRunSelector, AtmWorkflowExecutionDoc1#document.key
        ),
        call_current_lane_run_pre_execution_hooks(AtmWorkflowExecutionDoc2),

        {ok, LaneSpec}
    catch
        throw:?ERROR_ATM_WORKFLOW_EXECUTION_ABORTING ->
            handle_setup_exception(
                execution_stopping, AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx
            );

        Type:Reason:Stacktrace ->
            LogContent = #{
                <<"description">> => str_utils:format_bin(
                    "Failed to prepare next run of lane number ~B.",
                    [element(1, AtmLaneRunSelector)]
                ),
                <<"reason">> => errors:to_json(?atm_examine_error(Type, Reason, Stacktrace))
            },
            Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
            atm_workflow_execution_logger:workflow_critical(LogContent, Logger),

            handle_setup_exception(
                setup_failure, AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx
            )
    end.


%% @private
-spec resume_lane_run(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    {ok, workflow_engine:lane_spec()} | error.
resume_lane_run(AtmLaneRunSelector, AtmWorkflowExecutionDoc0, AtmWorkflowExecutionCtx) ->
    AtmWorkflowExecutionId = AtmWorkflowExecutionDoc0#document.key,

    try
        LaneSpec = initiate_lane_run(
            AtmLaneRunSelector,
            AtmWorkflowExecutionDoc0,
            AtmWorkflowExecutionCtx,
            fun atm_parallel_box_execution:resume_all/2
        ),
        unfreeze_exception_store(AtmLaneRunSelector, AtmWorkflowExecutionDoc0),

        AtmWorkflowExecutionDoc1 = atm_lane_execution_status:handle_resumed(
            AtmLaneRunSelector, AtmWorkflowExecutionDoc0#document.key
        ),
        call_current_lane_run_pre_execution_hooks(AtmWorkflowExecutionDoc1),

        {ok, LaneSpec}
    catch
        throw:?ERROR_ATM_WORKFLOW_EXECUTION_ABORTING ->
            handle_setup_exception(
                execution_stopping, AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx
            );

        Type:Reason:Stacktrace ->
            {AtmLaneIndex, RunNum} = AtmLaneRunSelector,
            LogContent = #{
                <<"description">> => str_utils:format_bin(
                    "Failed to resume ~B run of lane number ~B.",
                    [RunNum, AtmLaneIndex]
                ),
                <<"reason">> => errors:to_json(?atm_examine_error(Type, Reason, Stacktrace))
            },
            Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
            atm_workflow_execution_logger:workflow_critical(LogContent, Logger),

            handle_setup_exception(
                setup_failure, AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx
            )
    end.


%% @private
-spec unfreeze_exception_store(atm_lane_execution:lane_run_selector(), atm_workflow_execution:doc()) ->
    ok.
unfreeze_exception_store(AtmLaneRunSelector, #document{value = AtmWorkflowExecution}) ->
    {ok, #atm_lane_execution_run{exception_store_id = AtmExceptionStoreId}} = atm_lane_execution:get_run(
        AtmLaneRunSelector, AtmWorkflowExecution
    ),
    atm_store_api:unfreeze(AtmExceptionStoreId).


%% @private
-spec initiate_lane_run(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record(),
    fun((atm_workflow_execution_ctx:record(), [atm_parallel_box_execution:record()]) ->
        {[workflow_engine:parallel_box_spec()], atm_workflow_execution_env:diff()}
    )
) ->
    workflow_engine:lane_spec() | no_return().
initiate_lane_run(
    AtmLaneRunSelector,
    #document{value = AtmWorkflowExecution},
    AtmWorkflowExecutionCtx,
    InitiateParallelBoxExecutionsFun
) ->
    try
        {ok, AtmLaneRun} = atm_lane_execution:get_run(AtmLaneRunSelector, AtmWorkflowExecution),

        {ok, AtmStore} = atm_store_api:get(AtmLaneRun#atm_lane_execution_run.exception_store_id),
        AtmWorkflowExecutionEnv = atm_workflow_execution_env:set_lane_run_exception_store_container(
            AtmStore#atm_store.container,
            atm_workflow_execution_ctx:get_env(AtmWorkflowExecutionCtx)
        ),

        {AtmParallelBoxExecutionSpecs, AtmWorkflowExecutionEnvDiff} = InitiateParallelBoxExecutionsFun(
            AtmWorkflowExecutionCtx,
            AtmLaneRun#atm_lane_execution_run.parallel_boxes
        ),

        #{
            execution_context => AtmWorkflowExecutionEnvDiff(AtmWorkflowExecutionEnv),
            parallel_boxes => AtmParallelBoxExecutionSpecs
        }
    catch Type:Reason:Stacktrace ->
        throw(?ERROR_ATM_LANE_EXECUTION_INITIATION_FAILED(
            atm_lane_execution:get_schema_id(AtmLaneRunSelector, AtmWorkflowExecution),
            ?atm_examine_error(Type, Reason, Stacktrace)
        ))
    end.


%% @private
-spec handle_setup_exception(
    execution_stopping | setup_failure | setup_interruption,
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    error.
handle_setup_exception(execution_stopping, AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    {ok, #document{value = AtmWorkflowExecution}} = atm_workflow_execution:get(AtmWorkflowExecutionId),

    case atm_lane_execution:is_current_lane_run(AtmLaneRunSelector, AtmWorkflowExecution) of
        true ->
            % Call via ?MODULE: to allow mocking in tests
            ?MODULE:handle_stopped(AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx),
            error;
        false ->
            handle_setup_exception(
                setup_interruption, AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx
            )
    end;

handle_setup_exception(Type, AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    StoppingReason = case Type of
        setup_failure -> failure;
        setup_interruption -> interrupt
    end,
    case stop(AtmLaneRunSelector, StoppingReason, AtmWorkflowExecutionCtx) of
        {ok, _} ->
            % Call via ?MODULE: to allow mocking in tests
            ?MODULE:handle_stopped(AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx);
        ?ERROR_NOT_FOUND ->
            % failed to create lane run in advance
            ok
    end,
    error.


%% @private
-spec acquire_iterator(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:doc()
) ->
    atm_store_iterator:record().
acquire_iterator(AtmLaneRunSelector, #document{value = AtmWorkflowExecution}) ->
    IteratorSpec = get_iterator_spec(AtmLaneRunSelector, AtmWorkflowExecution),
    {ok, AtmLaneRun} = atm_lane_execution:get_run(AtmLaneRunSelector, AtmWorkflowExecution),
    atm_store_api:acquire_iterator(AtmLaneRun#atm_lane_execution_run.iterated_store_id, IteratorSpec).


%% @private
-spec get_iterator_spec(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:record()
) ->
    atm_store_iterator_spec:record().
get_iterator_spec(AtmLaneRunSelector, AtmWorkflowExecution = #atm_workflow_execution{
    lambda_snapshot_registry = AtmLambdaSnapshotRegistry
}) ->
    #atm_lane_schema{
        parallel_boxes = AtmParallelBoxSchemas,
        store_iterator_spec = AtmStoreIteratorSpec = #atm_store_iterator_spec{
            max_batch_size = MaxBatchSize
        }
    } = atm_lane_execution:get_schema(AtmLaneRunSelector, AtmWorkflowExecution),

    %% TODO VFS-8679 reuse atm_workflow_schema_revision:extract_atm_lambda_references
    AtmLambdas = lists:foldl(fun(#atm_parallel_box_schema{tasks = AtmTaskSchemas}, OuterAcc) ->
        lists:foldl(fun(#atm_task_schema{
            lambda_id = AtmLambdaId,
            lambda_revision_number = AtmLambdaRevisionNum
        }, InnerAcc) ->
            [{AtmLambdaId, AtmLambdaRevisionNum} | InnerAcc]
        end, OuterAcc, AtmTaskSchemas)
    end, [], AtmParallelBoxSchemas),

    NewMaxBatchSize = lists:foldl(fun({AtmLambdaId, RevisionNum}, MinBatchSize) ->
        {ok, #document{value = AtmLambdaSnapshot}} = atm_lambda_snapshot:get(
            maps:get(AtmLambdaId, AtmLambdaSnapshotRegistry)
        ),
        AtmLambdaRevision = atm_lambda_snapshot:get_revision(RevisionNum, AtmLambdaSnapshot),
        min(MinBatchSize, AtmLambdaRevision#atm_lambda_revision.preferred_batch_size)
    end, MaxBatchSize, lists:usort(AtmLambdas)),

    AtmStoreIteratorSpec#atm_store_iterator_spec{max_batch_size = NewMaxBatchSize}.


%% @private
-spec handle_lane_run_stopping(
    atm_lane_execution:lane_run_selector(),
    atm_lane_execution:run_stopping_reason(),
    atm_workflow_execution_ctx:record(),
    atm_workflow_execution:doc()
) ->
    {ok, stopping | stopped}.
handle_lane_run_stopping(AtmLaneRunSelector, Reason, AtmWorkflowExecutionCtx, #document{
    key = AtmWorkflowExecutionId,
    value = AtmWorkflowExecution = #atm_workflow_execution{prev_status = PrevStatus}
}) ->
    IsCurrentLaneRun = atm_lane_execution:is_current_lane_run(
        AtmLaneRunSelector, AtmWorkflowExecution
    ),

    case atm_workflow_execution_status:status_to_phase(PrevStatus) of
        ?SUSPENDED_PHASE when IsCurrentLaneRun ->
            % Stopping suspended execution - since there was no active process
            % handling it, manual cleanup and callback calls are necessary
            workflow_engine:cleanup_execution(AtmWorkflowExecutionId),
            stop_parallel_boxes(AtmLaneRunSelector, Reason, AtmWorkflowExecutionCtx, AtmWorkflowExecution),
            atm_lane_execution_status:handle_stopped(AtmLaneRunSelector, AtmWorkflowExecutionId),
            {ok, stopped};
        _ when IsCurrentLaneRun ->
            % Currently executed lane run stopping == entire workflow execution is stopping
            workflow_engine:init_cancel_procedure(AtmWorkflowExecutionId),
            stop_parallel_boxes(AtmLaneRunSelector, Reason, AtmWorkflowExecutionCtx, AtmWorkflowExecution),
            workflow_engine:finish_cancel_procedure(AtmWorkflowExecutionId),
            {ok, stopping};
        _ ->
            stop_parallel_boxes(AtmLaneRunSelector, Reason, AtmWorkflowExecutionCtx, AtmWorkflowExecution),
            {ok, stopping}
    end.


%% @private
-spec stop_parallel_boxes(
    atm_lane_execution:lane_run_selector(),
    atm_lane_execution:run_stopping_reason(),
    atm_workflow_execution_ctx:record(),
    atm_workflow_execution:record()
) ->
    ok | no_return().
stop_parallel_boxes(
    AtmLaneRunSelector,
    AtmLaneRunStoppingReason,
    AtmWorkflowExecutionCtx,
    AtmWorkflowExecution
) ->
    AtmTaskExecutionStoppingReason = case AtmLaneRunStoppingReason of
        failure -> interrupt;
        crash -> interrupt;
        Reason -> Reason
    end,
    {ok, AtmLaneRun} = atm_lane_execution:get_run(AtmLaneRunSelector, AtmWorkflowExecution),

    atm_parallel_box_execution:stop_all(
        AtmWorkflowExecutionCtx,
        AtmTaskExecutionStoppingReason,
        AtmLaneRun#atm_lane_execution_run.parallel_boxes
    ).


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

    % Check if current lane run is being stopped here rather then right before
    % this bool usage as below calls may shift current lane run selector
    IsCurrentAtmLaneRun = atm_lane_execution:is_current_lane_run(
        AtmLaneRunSelector, AtmWorkflowExecution
    ),
    {ok, CurrentRun} = atm_lane_execution:get_run(AtmLaneRunSelector, AtmWorkflowExecution),
    AtmParallelBoxExecutions = CurrentRun#atm_lane_execution_run.parallel_boxes,

    unfreeze_iterated_store_in_case_of_global_store(CurrentRun, AtmWorkflowExecutionCtx),
    freeze_exception_store(CurrentRun),

    atm_parallel_box_execution:ensure_all_stopped(AtmParallelBoxExecutions),

    #document{
        value = NewAtmWorkflowExecution = #atm_workflow_execution{
            current_lane_index = NextAtmLaneIndex,
            current_run_num = NextRunNum
        }
    } = atm_lane_execution_status:handle_stopped(AtmLaneRunSelector, AtmWorkflowExecutionId),

    atm_parallel_box_execution:teardown_all(AtmWorkflowExecutionCtx, AtmParallelBoxExecutions),

    IsRetryScheduled = IsCurrentAtmLaneRun andalso
        NextAtmLaneIndex == CurrentAtmLaneIndex andalso
        NextRunNum == CurrentRunNum + 1,

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
%% execution (all previous preparations must have been stopped and lane run ready
%% to execute).
%% @end
%%--------------------------------------------------------------------
-spec call_current_lane_run_pre_execution_hooks(
    atm_workflow_execution:record() | atm_workflow_execution:doc()
) ->
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
            % execution must have stopped or lane run is still not ready
            % (either not fully prepared or previous run is still ongoing)
            ok
    end;

call_current_lane_run_pre_execution_hooks(#document{value = AtmWorkflowExecution}) ->
    call_current_lane_run_pre_execution_hooks(AtmWorkflowExecution).
