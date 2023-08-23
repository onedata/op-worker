%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021-2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles the lane execution setup process (for information about
%%% state machine @see 'atm_lane_execution_status.erl').
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lane_execution_setup_handler).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include("workflow_engine.hrl").

%% API
-export([prepare/3, resume/3]).


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

        ?ERROR_ATM_INVALID_STATUS_TRANSITION(?RESUMING_STATUS, ?PREPARING_STATUS) ->
            resume_prepared_lane_run(AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx);

        ?ERROR_ATM_WORKFLOW_EXECUTION_STOPPING ->
            handle_setup_exception(
                execution_stopping, AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx
            )
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

    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    ?atm_workflow_debug(Logger, ?ATM_WORKFLOW_LANE_RUN_LOG(AtmLaneRunSelector, <<"Preparing...">>)),

    try
        LaneSpec = prepare_lane_run_insecure(
            AtmLaneRunSelector, AtmWorkflowExecutionDoc0, AtmWorkflowExecutionCtx
        ),
        ?atm_workflow_info(Logger, ?ATM_WORKFLOW_LANE_RUN_LOG(AtmLaneRunSelector, <<"Prepared.">>)),
        {ok, LaneSpec}
    catch
        throw:?ERROR_ATM_WORKFLOW_EXECUTION_STOPPING ->
            handle_setup_exception(
                execution_stopping, AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx
            );

        Type:Reason:Stacktrace ->
            Error = ?examine_exception(Type, Reason, Stacktrace),
            ?atm_workflow_critical(Logger, #atm_workflow_log_schema{
                selector = {lane, element(1, AtmLaneRunSelector)},
                description = <<"Failed to prepare next run.">>,
                details = #{<<"reason">> => errors:to_json(Error)}
            }),

            handle_setup_exception(
                infer_setup_exception(Reason),
                AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx
            )
    end.


%% @private
-spec prepare_lane_run_insecure(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    workflow_engine:lane_spec() | no_return().
prepare_lane_run_insecure(AtmLaneRunSelector, AtmWorkflowExecutionDoc0, AtmWorkflowExecutionCtx) ->
    AtmWorkflowExecutionDoc1 = atm_lane_execution_factory:create_run(
        AtmLaneRunSelector, AtmWorkflowExecutionDoc0, AtmWorkflowExecutionCtx
    ),

    LaneSpec = start_lane_run(AtmLaneRunSelector, AtmWorkflowExecutionDoc1, AtmWorkflowExecutionCtx),

    AtmWorkflowExecutionDoc2 = atm_lane_execution_status:handle_enqueued(
        AtmLaneRunSelector, AtmWorkflowExecutionDoc1#document.key
    ),
    atm_lane_execution_hooks_handler:exec_current_lane_run_pre_execution_hooks(AtmWorkflowExecutionDoc2),

    LaneSpec.


%% @private
-spec start_lane_run(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    workflow_engine:lane_spec() | no_return().
start_lane_run(AtmLaneRunSelector, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx) ->
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    ?atm_workflow_debug(Logger, ?ATM_WORKFLOW_LANE_RUN_LOG(AtmLaneRunSelector, <<"Starting...">>)),

    BasicLaneSpec = initiate_lane_run(
        AtmLaneRunSelector,
        AtmWorkflowExecutionDoc,
        AtmWorkflowExecutionCtx,
        fun atm_parallel_box_execution:start_all/2
    ),
    LaneIterator = acquire_iterator(AtmLaneRunSelector, AtmWorkflowExecutionDoc),
    BasicLaneSpec#{iterator => LaneIterator}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resumes lane run execution that have been stopped right before it's
%% preparation finished (prepare_lane workflow_engine callback returned)
%% but after all it's components have been created.
%% @end
%%--------------------------------------------------------------------
-spec resume_prepared_lane_run(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    {ok, workflow_engine:lane_spec()} | error.
resume_prepared_lane_run(AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),

    case resume_lane_run(AtmLaneRunSelector, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx) of
        {ok, LaneSpec} ->
            LaneIterator = acquire_iterator(AtmLaneRunSelector, AtmWorkflowExecutionDoc),
            {ok, LaneSpec#{iterator => LaneIterator}};
        error ->
            error
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

    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    ?atm_workflow_debug(Logger, ?ATM_WORKFLOW_LANE_RUN_LOG(AtmLaneRunSelector, <<"Resuming...">>)),

    try
        LaneSpec = resume_lane_run_insecure(
            AtmLaneRunSelector, AtmWorkflowExecutionDoc0, AtmWorkflowExecutionCtx
        ),
        ?atm_workflow_info(Logger, ?ATM_WORKFLOW_LANE_RUN_LOG(AtmLaneRunSelector, <<"Resumed.">>)),
        {ok, LaneSpec}
    catch
        throw:?ERROR_ATM_WORKFLOW_EXECUTION_STOPPING ->
            handle_setup_exception(
                execution_stopping, AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx
            );

        Type:Reason:Stacktrace ->
            Error = ?examine_exception(Type, Reason, Stacktrace),
            ?atm_workflow_critical(Logger, ?ATM_WORKFLOW_LANE_RUN_LOG(
                AtmLaneRunSelector, <<"Failed to resume.">>, #{<<"reason">> => errors:to_json(Error)})
            ),

            handle_setup_exception(
                infer_setup_exception(Reason),
                AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx
            )
    end.


%% @private
-spec resume_lane_run_insecure(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    workflow_engine:lane_spec() | no_return().
resume_lane_run_insecure(AtmLaneRunSelector, AtmWorkflowExecutionDoc0, AtmWorkflowExecutionCtx) ->
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
    atm_lane_execution_hooks_handler:exec_current_lane_run_pre_execution_hooks(AtmWorkflowExecutionDoc1),

    LaneSpec.


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
    AtmWorkflowExecutionDoc = #document{value = AtmWorkflowExecution},
    AtmWorkflowExecutionCtx1,
    InitiateParallelBoxExecutionsFun
) ->
    try
        {ok, AtmLaneRun} = atm_lane_execution:get_run(AtmLaneRunSelector, AtmWorkflowExecution),

        AtmWorkflowExecutionCtx2 = update_workflow_ctx_with_lane_run_specific_data(
            AtmLaneRunSelector, AtmLaneRun, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx1
        ),
        {AtmParallelBoxExecutionSpecs, AtmWorkflowExecutionEnvDiff} = InitiateParallelBoxExecutionsFun(
            AtmWorkflowExecutionCtx2, AtmLaneRun#atm_lane_execution_run.parallel_boxes
        ),

        #{
            execution_context => AtmWorkflowExecutionEnvDiff(atm_workflow_execution_ctx:get_env(
                AtmWorkflowExecutionCtx2
            )),
            parallel_boxes => AtmParallelBoxExecutionSpecs
        }
    catch
        throw:?ERROR_ATM_WORKFLOW_EXECUTION_STOPPING ->
            throw(?ERROR_ATM_WORKFLOW_EXECUTION_STOPPING);

        Type:Reason:Stacktrace ->
            throw(?ERROR_ATM_LANE_EXECUTION_INITIATION_FAILED(
                atm_lane_execution:get_schema_id(AtmLaneRunSelector, AtmWorkflowExecution),
                ?examine_exception(Type, Reason, Stacktrace)
            ))
    end.


%% @private
-spec update_workflow_ctx_with_lane_run_specific_data(
    atm_lane_execution:lane_run_selector(),
    atm_lane_execution:run(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    atm_workflow_execution_ctx:record().
update_workflow_ctx_with_lane_run_specific_data(
    AtmLaneRunSelector,
    #atm_lane_execution_run{exception_store_id = AtmExceptionStoreId},
    AtmWorkflowExecutionDoc = #document{value = AtmWorkflowExecution},
    AtmWorkflowExecutionCtx
) ->
    AtmWorkflowExecutionEnv0 = atm_workflow_execution_ctx:get_env(AtmWorkflowExecutionCtx),

    {ok, AtmExceptionStore} = atm_store_api:get(AtmExceptionStoreId),
    AtmWorkflowExecutionEnv1 = atm_workflow_execution_env:set_lane_run_exception_store_container(
        AtmExceptionStore#atm_store.container, AtmWorkflowExecutionEnv0
    ),

    AtmLaneSchema = atm_lane_execution:get_schema(AtmLaneRunSelector, AtmWorkflowExecution),
    AtmWorkflowExecutionEnv2 = atm_workflow_execution_env:set_lane_run_instant_failure_exception_threshold(
        AtmLaneSchema#atm_lane_schema.instant_failure_exception_threshold,
        AtmWorkflowExecutionEnv1
    ),

    AtmWorkflowExecutionEnv3 = atm_workflow_execution_env:ensure_task_selector_registry_up_to_date(
        AtmWorkflowExecutionDoc, AtmLaneRunSelector, AtmWorkflowExecutionEnv2
    ),

    atm_workflow_execution_ctx:set_env(AtmWorkflowExecutionEnv3, AtmWorkflowExecutionCtx).


%% @private
-spec acquire_iterator(atm_lane_execution:lane_run_selector(), atm_workflow_execution:doc()) ->
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
    AtmLaneSchema = #atm_lane_schema{
        store_iterator_spec = AtmStoreIteratorSpec = #atm_store_iterator_spec{
            max_batch_size = OriginMaxBatchSize
        }
    } = atm_lane_execution:get_schema(AtmLaneRunSelector, AtmWorkflowExecution),

    MaxBatchSize = lists:foldl(fun({AtmLambdaId, RevisionNum}, MinBatchSize) ->
        {ok, #document{value = AtmLambdaSnapshot}} = atm_lambda_snapshot:get(
            maps:get(AtmLambdaId, AtmLambdaSnapshotRegistry)
        ),
        AtmLambdaRevision = atm_lambda_snapshot:get_revision(RevisionNum, AtmLambdaSnapshot),
        min(MinBatchSize, AtmLambdaRevision#atm_lambda_revision.preferred_batch_size)
    end, OriginMaxBatchSize, lists:usort(extract_lambda_references(AtmLaneSchema))),

    AtmStoreIteratorSpec#atm_store_iterator_spec{max_batch_size = MaxBatchSize}.


%% TODO VFS-8679 reuse atm_workflow_schema_revision:extract_atm_lambda_references
%% @private
-spec extract_lambda_references(atm_lane_schema:record()) ->
    [{automation:id(), atm_lambda_revision:revision_number()}].
extract_lambda_references(#atm_lane_schema{parallel_boxes = AtmParallelBoxSchemas}) ->
    lists:foldl(fun(#atm_parallel_box_schema{tasks = AtmTaskSchemas}, OuterAcc) ->
        lists:foldl(fun(#atm_task_schema{
            lambda_id = AtmLambdaId,
            lambda_revision_number = AtmLambdaRevisionNum
        }, InnerAcc) ->
            [{AtmLambdaId, AtmLambdaRevisionNum} | InnerAcc]
        end, OuterAcc, AtmTaskSchemas)
    end, [], AtmParallelBoxSchemas).


%% @private
-spec unfreeze_exception_store(atm_lane_execution:lane_run_selector(), atm_workflow_execution:doc()) ->
    ok.
unfreeze_exception_store(AtmLaneRunSelector, #document{value = AtmWorkflowExecution}) ->
    {ok, AtmLaneRun} = atm_lane_execution:get_run(AtmLaneRunSelector, AtmWorkflowExecution),
    atm_store_api:unfreeze(AtmLaneRun#atm_lane_execution_run.exception_store_id).


%% TODO VFS-11227 call init stop from task setup try cache
%% @private
-spec infer_setup_exception(errors:error()) -> setup_failure | setup_interruption.
infer_setup_exception(?ERROR_ATM_LANE_EXECUTION_INITIATION_FAILED(_, Error)) ->
    infer_setup_exception(Error);

infer_setup_exception(?ERROR_ATM_PARALLEL_BOX_EXECUTION_INITIATION_FAILED(_, Error)) ->
    infer_setup_exception(Error);

infer_setup_exception(?ERROR_ATM_TASK_EXECUTION_INITIATION_FAILED(_, Error)) ->
    infer_setup_exception(Error);

infer_setup_exception(?ERROR_ATM_OPENFAAS_QUERY_FAILED) ->
    setup_interruption;

infer_setup_exception(?ERROR_ATM_OPENFAAS_QUERY_FAILED(_)) ->
    setup_interruption;

infer_setup_exception(?ERROR_ATM_OPENFAAS_FUNCTION_REGISTRATION_FAILED) ->
    setup_interruption;

infer_setup_exception(_) ->
    setup_failure.


%% @private
-spec handle_setup_exception(
    execution_stopping | setup_failure | setup_interruption,
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    error.
handle_setup_exception(execution_stopping, AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),
    AtmWorkflowExecution = AtmWorkflowExecutionDoc#document.value,

    case atm_lane_execution:is_current_lane_run(AtmLaneRunSelector, AtmWorkflowExecution) of
        true ->
            handle_current_lane_run_stopping(
                AtmLaneRunSelector, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx
            ),
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
    stop_lane_run(AtmLaneRunSelector, StoppingReason, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx),

    error.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Repeats stopping procedures just in case if it wasn't finished
%% (e.g. concurrent user and lane preparing process stopping)
%% @end
%%--------------------------------------------------------------------
-spec handle_current_lane_run_stopping(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    ok.
handle_current_lane_run_stopping(AtmLaneRunSelector, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx) ->
    {ok, #atm_lane_execution_run{stopping_reason = StoppingReason}} = atm_lane_execution:get_run(
        AtmLaneRunSelector, AtmWorkflowExecutionDoc#document.value
    ),
    atm_lane_execution_stop_handler:handle_stopping(
        AtmLaneRunSelector, StoppingReason, AtmWorkflowExecutionCtx, AtmWorkflowExecutionDoc
    ),
    atm_lane_execution_stop_handler:handle_stopped(
        AtmLaneRunSelector, AtmWorkflowExecutionDoc#document.key, AtmWorkflowExecutionCtx
    ),

    ok.


%% @private
-spec stop_lane_run(
    atm_lane_execution:lane_run_selector(),
    failure | interrupt,
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    ok.
stop_lane_run(AtmLaneRunSelector, StoppingReason, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    case atm_lane_execution_stop_handler:init_stop(
        AtmLaneRunSelector, StoppingReason, AtmWorkflowExecutionCtx
    ) of
        {ok, _} ->
            atm_lane_execution_stop_handler:handle_stopped(
                AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx
            ),
            ok;

        ?ERROR_NOT_FOUND ->
            % failed to create lane run in advance
            ok
    end.
