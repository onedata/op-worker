%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles the task execution process according to following
%%% state machine:
%%%
%%%                      <WAITING PHASE (initiation)>
%%%     +-----------+
%%%     |  PENDING  |---------------------------------------
%%%     +-----------+                                        \
%%%           |                               ending task execution with no item
%%%  first item scheduled to process               ever scheduled to process
%%%           |                                               |
%%% ==========|===============================================|===============
%%%           v               <ONGOING PHASE>                 |
%%%         +-----------+                                     |
%%%         |   ACTIVE  |                                     |
%%%         +-----------+                                     |
%%%                   |                                       |
%%%                   |                                       |
%%%        ending task execution with                         |
%%%             /                \                            |
%%%            /                  \                           |
%%%  all items successfully      else                         |
%%%        processed               |                          |
%%%           |                    |                          |
%%% ==========|====================|==========================|===============
%%%           |           <ENDED PHASE (teardown)>            |
%%%           |                    |                          |
%%%           v                    v                          v
%%%     +-----------+        +-----------+              +-----------+
%%%     |  FINISHED |        |   FAILED  |              |  SKIPPED  |
%%%     +-----------+        +-----------+              +-----------+
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_handler).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    initiate/2,
    teardown/2,
    set_run_num/2,

    process_item/5,
    process_results/4,

    handle_ended/1
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec initiate(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id() | atm_task_execution:doc()
) ->
    {workflow_engine:task_spec(), atm_workflow_execution_env:diff()} | no_return().
initiate(AtmWorkflowExecutionCtx, AtmTaskExecutionIdOrDoc) ->
    #document{
        key = AtmTaskExecutionId,
        value = #atm_task_execution{
            executor = AtmTaskExecutor,
            system_audit_log_id = AtmSystemAuditLogId
        }
    } = ensure_atm_task_execution_doc(AtmTaskExecutionIdOrDoc),

    AtmTaskExecutionSpec = atm_task_executor:initiate(AtmWorkflowExecutionCtx, AtmTaskExecutor),

    {ok, #atm_store{container = AtmTaskAuditLogStoreContainer}} = atm_store_api:get(
        AtmSystemAuditLogId
    ),
    AtmWorkflowExecutionEnvDiff = fun(AtmWorkflowExecutionEnv) ->
        atm_workflow_execution_env:add_task_audit_log_store_container(
            AtmTaskExecutionId, AtmTaskAuditLogStoreContainer, AtmWorkflowExecutionEnv
        )
    end,

    {AtmTaskExecutionSpec, AtmWorkflowExecutionEnvDiff}.


-spec teardown(atm_lane_execution_handler:teardown_ctx(), atm_task_execution:id()) ->
    ok | no_return().
teardown(AtmLaneExecutionRunTeardownCtx, AtmTaskExecutionId) ->
    #document{value = #atm_task_execution{
        executor = AtmTaskExecutor
    }} = ensure_atm_task_execution_doc(AtmTaskExecutionId),

    atm_task_executor:teardown(AtmLaneExecutionRunTeardownCtx, AtmTaskExecutor).


-spec set_run_num(atm_lane_execution:run_num(), atm_task_execution:id()) ->
    ok.
set_run_num(RunNum, AtmTaskExecutionId) ->
    {ok, _} = atm_task_execution:update(AtmTaskExecutionId, fun(AtmTaskExecution) ->
        {ok, AtmTaskExecution#atm_task_execution{run_num = RunNum}}
    end),
    ok.


-spec process_item(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    automation:item(),
    binary(),
    binary()
) ->
    ok | error | no_return().
process_item(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, ReportResultUrl, HeartbeatUrl) ->
    try
        process_item_insecure(
            AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, ReportResultUrl, HeartbeatUrl
        )
    catch Type:Reason:Stacktrace ->
        Error = ?atm_examine_error(Type, Reason, Stacktrace),
        process_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Error)
    end.


-spec process_results(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    automation:item(),
    errors:error() | json_utils:json_map()
) ->
    ok | error | no_return().
process_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, {error, _} = Error) ->
    handle_exception(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Error);

process_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, #{<<"exception">> := _} = Exception) ->
    handle_exception(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Exception);

process_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Results) when is_map(Results) ->
    {ok, #document{value = #atm_task_execution{
        result_specs = AtmTaskExecutionResultSpecs
    }}} = atm_task_execution:get(AtmTaskExecutionId),

    try
        atm_task_execution_results:consume_results(AtmWorkflowExecutionCtx, AtmTaskExecutionResultSpecs, Results),
        update_items_processed(AtmTaskExecutionId)
    catch Type:Reason:Stacktrace ->
        Error = ?atm_examine_error(Type, Reason, Stacktrace),
        handle_exception(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Error)
    end;

process_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, _MalformedResults) ->
    handle_exception(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, ?ERROR_MALFORMED_DATA).


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
        {ok, #document{value = AtmTaskExecution} = AtmTaskExecutionDoc} ->
            atm_store_api:freeze(AtmTaskExecution#atm_task_execution.system_audit_log_id),
            handle_status_change(AtmTaskExecutionDoc);
        {error, already_ended} ->
            ok
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec ensure_atm_task_execution_doc(atm_task_execution:id() | atm_task_execution:doc()) ->
    atm_task_execution:doc().
ensure_atm_task_execution_doc(#document{value = #atm_task_execution{}} = AtmTaskExecutionDoc) ->
    AtmTaskExecutionDoc;
ensure_atm_task_execution_doc(AtmTaskExecutionId) ->
    {ok, AtmTaskExecutionDoc = #document{}} = atm_task_execution:get(AtmTaskExecutionId),
    AtmTaskExecutionDoc.


%% @private
-spec process_item_insecure(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    automation:item(),
    binary(),
    binary()
) ->
    ok | no_return().
process_item_insecure(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, ReportResultUrl, HeartbeatUrl) ->
    #document{
        value = #atm_task_execution{
            executor = AtmTaskExecutor,
            argument_specs = AtmTaskExecutionArgSpecs
        }
    } = update_items_in_processing(AtmTaskExecutionId),

    AtmJobCtx = atm_job_ctx:build(
        AtmWorkflowExecutionCtx, atm_task_executor:in_readonly_mode(AtmTaskExecutor),
        Item, ReportResultUrl, HeartbeatUrl
    ),
    Args = atm_task_execution_arguments:construct_args(AtmJobCtx, AtmTaskExecutionArgSpecs),

    atm_task_executor:run(AtmJobCtx, Args, AtmTaskExecutor).


%% @private
-spec handle_exception(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    automation:item(),
    errors:error() | json_utils:json_map()
) ->
    error.
handle_exception(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, #{<<"exception">> := Reason}) ->
    log_exception(Item, Reason, AtmWorkflowExecutionCtx),
    update_items_failed_and_processed(AtmTaskExecutionId),
    error;

handle_exception(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, {error, _} = Error) ->
    handle_exception(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, #{
        <<"exception">> => errors:to_json(Error)
    }).


%% @private
-spec log_exception(automation:item(), json_utils:json_term(), atm_workflow_execution_ctx:record()) ->
    ok.
log_exception(Item, Reason, AtmWorkflowExecutionCtx) ->
    Log = #{
        <<"severity">> => ?LOGGER_ERROR,
        <<"item">> => Item,
        <<"reason">> => Reason
    },
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),

    atm_workflow_execution_logger:task_append_logs(Log, #{}, Logger).


%% @private
-spec update_items_in_processing(atm_task_execution:id()) -> atm_task_execution:doc().
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
        run_num = RunNum,
        parallel_box_index = AtmParallelBoxIndex,
        status = NewStatus,
        status_changed = true
    }
}) ->
    ok = atm_lane_execution_status:handle_task_status_change(
        AtmWorkflowExecutionId, {AtmLaneIndex, RunNum}, AtmParallelBoxIndex,
        AtmTaskExecutionId, NewStatus
    ).
