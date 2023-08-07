%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021-2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles the task execution stop procedures (for information
%%% about state machine @see 'atm_task_execution_status.erl').
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_stop_handler).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    init_stop/3,
    handle_stopped/2,

    teardown/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec init_stop(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    atm_task_execution:stopping_reason()
) ->
    ok | no_return().
init_stop(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Reason) ->
    case atm_task_execution_status:handle_stopping(
        AtmTaskExecutionId,
        Reason,
        atm_workflow_execution_ctx:get_workflow_execution_incarnation(AtmWorkflowExecutionCtx)
    ) of
        {ok, _} when Reason =:= pause ->
            % when execution is paused, ongoing jobs aren't abruptly stopped (the
            % execution engine will wait for them before transitioning to paused status)
            log_stopping_reason(AtmWorkflowExecutionCtx, Reason);

        {ok, #document{value = #atm_task_execution{executor = AtmTaskExecutor}}} ->
            log_stopping_reason(AtmWorkflowExecutionCtx, Reason),

            % for other reasons than pause, ongoing jobs are immediately aborted
            atm_task_executor:abort(AtmWorkflowExecutionCtx, AtmTaskExecutor);

        {error, task_already_stopping} ->
            ok;

        {error, task_already_stopped} ->
            ok
    end.


-spec handle_stopped(atm_workflow_execution_ctx:record(), atm_task_execution:id()) ->
    ok.
handle_stopped(AtmWorkflowExecutionCtx, AtmTaskExecutionId) ->
    case atm_task_execution_status:handle_stopped(AtmTaskExecutionId) of
        {ok, #document{value = AtmTaskExecution}} ->
            Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
            ?atm_task_info(Logger, <<"Task stopped.">>),
            ?atm_workflow_info(Logger, ?ATM_WORKFLOW_TASK_LOG(AtmTaskExecutionId, <<"Stopped.">>)),

            freeze_stores(AtmTaskExecution);

        {error, task_already_stopped} ->
            ok
    end.


-spec teardown(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id() | atm_task_execution:doc()
) ->
    ok | no_return().
teardown(AtmWorkflowExecutionCtx, AtmTaskExecutionIdOrDoc) ->
    AtmTaskExecutionDoc = ensure_atm_task_execution_doc(AtmTaskExecutionIdOrDoc),
    atm_task_executor:teardown(AtmWorkflowExecutionCtx, AtmTaskExecutionDoc).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec log_stopping_reason(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:stopping_reason()
) ->
    ok.
log_stopping_reason(AtmWorkflowExecutionCtx, StoppingReason) ->
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),

    Details = #{<<"reason">> => StoppingReason},
    AtmTaskExecutionId = atm_workflow_execution_ctx:get_task_execution_id(AtmWorkflowExecutionCtx),

    ?atm_task_info(Logger, #{
        <<"description">> => <<"Task stop initiated.">>,
        <<"details">> => Details
    }),
    ?atm_workflow_info(Logger, ?ATM_WORKFLOW_TASK_LOG(
        AtmTaskExecutionId, <<"Stop initiated.">>, Details
    )).


%% @private
-spec freeze_stores(atm_task_execution:record()) -> ok.
freeze_stores(#atm_task_execution{
    system_audit_log_store_id = AtmSystemAuditLogStoreId,
    time_series_store_id = AtmTSStoreId
}) ->
    AtmTSStoreId /= undefined andalso atm_store_api:freeze(AtmTSStoreId),
    atm_store_api:freeze(AtmSystemAuditLogStoreId).


%% @private
-spec ensure_atm_task_execution_doc(atm_task_execution:id() | atm_task_execution:doc()) ->
    atm_task_execution:doc().
ensure_atm_task_execution_doc(#document{value = #atm_task_execution{}} = AtmTaskExecutionDoc) ->
    AtmTaskExecutionDoc;
ensure_atm_task_execution_doc(AtmTaskExecutionId) ->
    {ok, AtmTaskExecutionDoc = #document{}} = atm_task_execution:get(AtmTaskExecutionId),
    AtmTaskExecutionDoc.
