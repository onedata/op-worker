%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements callbacks for handling automation workflow
%%% execution process.
%%% @end
%%%--------------------------------------------------------------------
-module(atm_workflow_execution_handler).
-author("Bartosz Walkowicz").

-behaviour(workflow_handler).

-include("modules/automation/atm_execution.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("workflow_engine.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").

% API
-export([
    init_engine/0,
    start/3,
    stop/3,
    repeat/4,
    resume/2
]).
-export([
    on_provider_restart/1,
    on_openfaas_down/2
]).

% workflow_handler callbacks
-export([
    prepare_lane/3,
    restart_lane/3,

    run_task_for_item/5,
    process_task_result_for_item/5,
    process_streamed_task_data/4,
    handle_task_results_processed_for_all_items/3,
    handle_task_execution_stopped/3,

    report_item_error/3,

    handle_lane_execution_stopped/3,

    handle_workflow_execution_stopped/2,

    handle_exception/5
]).


-define(ATM_WORKFLOW_EXECUTION_ENGINE, <<"atm_workflow_execution_engine">>).

-define(ENGINE_ASYNC_CALLS_LIMIT, op_worker:get_env(atm_workflow_engine_async_calls_limit, 1000)).
-define(ENGINE_SLOTS_COUNT, op_worker:get_env(atm_workflow_engine_slots_count, 20)).
-define(JOB_TIMEOUT_SEC, op_worker:get_env(atm_workflow_job_timeout_sec, 1800)).
-define(JOB_TIMEOUT_CHECK_PERIOD_SEC, op_worker:get_env(atm_workflow_job_timeout_check_period_sec, 1800)).

-define(INITIAL_NOTIFICATION_INTERVAL(), rand:uniform(timer:seconds(2))).
-define(MAX_NOTIFICATION_INTERVAL, timer:hours(2)).
-define(MAX_NOTIFICATION_RETRIES, 30).


%%%===================================================================
%%% API
%%%===================================================================


-spec init_engine() -> ok.
init_engine() ->
    Options = #{
        workflow_async_call_pools_to_use => [{?DEFAULT_ASYNC_CALL_POOL_ID, ?ENGINE_ASYNC_CALLS_LIMIT}],
        slots_limit => ?ENGINE_SLOTS_COUNT,
        default_keepalive_timeout => ?JOB_TIMEOUT_SEC,
        init_workflow_timeout_server => {true, ?JOB_TIMEOUT_CHECK_PERIOD_SEC}
    },
    workflow_engine:init(?ATM_WORKFLOW_EXECUTION_ENGINE, Options).


-spec start(user_ctx:ctx(), atm_workflow_execution_env:record(), atm_workflow_execution:doc()) ->
    ok.
start(UserCtx, AtmWorkflowExecutionEnv, #document{
    key = AtmWorkflowExecutionId,
    value = #atm_workflow_execution{
        lanes_count = AtmLanesCount
    }
}) ->
    ok = atm_workflow_execution_session:init(AtmWorkflowExecutionId, UserCtx),

    workflow_engine:execute_workflow(?ATM_WORKFLOW_EXECUTION_ENGINE, #{
        id => AtmWorkflowExecutionId,
        workflow_handler => ?MODULE,
        execution_context => AtmWorkflowExecutionEnv,
        first_lane_id => {1, 1},
        next_lane_id => case 1 < AtmLanesCount of
            true -> {2, current};
            false -> undefined
        end
    }).


-spec stop(
    user_ctx:ctx(),
    atm_workflow_execution:id(),
    cancel | pause
) ->
    ok | errors:error().
stop(UserCtx, AtmWorkflowExecutionId, Reason) ->
    {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),
    AtmWorkflowExecutionEnv = acquire_global_env(AtmWorkflowExecutionDoc),
    SpaceId = atm_workflow_execution_env:get_space_id(AtmWorkflowExecutionEnv),

    AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(
        undefined,
        atm_workflow_execution_auth:build(SpaceId, AtmWorkflowExecutionId, UserCtx),
        AtmWorkflowExecutionEnv
    ),
    case atm_lane_execution_handler:stop({current, current}, Reason, AtmWorkflowExecutionCtx) of
        {ok, stopping} ->
            ok;
        {ok, stopped} ->
            % atm workflow execution was stopped and there are no active processes handling it
            % (e.g. cancelling already suspended execution) - end procedures must be called manually
            end_workflow_execution(AtmWorkflowExecutionId, AtmWorkflowExecutionCtx),
            ok;
        {error, _} = Error ->
            Error
    end.


-spec repeat(
    user_ctx:ctx(),
    atm_workflow_execution:repeat_type(),
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:id()
) ->
    ok | errors:error().
repeat(UserCtx, Type, AtmLaneRunSelector, AtmWorkflowExecutionId) ->
    case atm_lane_execution_status:handle_manual_repeat(
        Type, AtmLaneRunSelector, AtmWorkflowExecutionId
    ) of
        {ok, AtmWorkflowExecutionDoc = #document{value = #atm_workflow_execution{
            lanes_count = AtmLanesCount,
            current_lane_index = CurrentAtmLaneIndex,
            current_run_num = CurrentRunNum
        }}} ->
            unfreeze_global_stores(AtmWorkflowExecutionDoc),
            ok = atm_workflow_execution_session:init(AtmWorkflowExecutionId, UserCtx),

            workflow_engine:execute_workflow(?ATM_WORKFLOW_EXECUTION_ENGINE, #{
                id => AtmWorkflowExecutionId,
                workflow_handler => ?MODULE,
                force_clean_execution => true,
                execution_context => acquire_global_env(AtmWorkflowExecutionDoc),
                first_lane_id => {CurrentAtmLaneIndex, CurrentRunNum},
                next_lane_id => case CurrentAtmLaneIndex < AtmLanesCount of
                    true -> {CurrentAtmLaneIndex + 1, current};
                    false -> undefined
                end
            });
        {error, _} = Error ->
            Error
    end.


-spec resume(user_ctx:ctx(), atm_workflow_execution:id()) ->
    ok | errors:error().
resume(UserCtx, AtmWorkflowExecutionId) ->
    case atm_lane_execution_status:handle_resume(AtmWorkflowExecutionId) of
        {ok, AtmWorkflowExecutionDoc} ->
            unfreeze_global_stores(AtmWorkflowExecutionDoc),
            ok = atm_workflow_execution_session:init(AtmWorkflowExecutionId, UserCtx),

            workflow_engine:execute_workflow(?ATM_WORKFLOW_EXECUTION_ENGINE, #{
                id => AtmWorkflowExecutionId,
                workflow_handler => ?MODULE,
                execution_context => acquire_global_env(AtmWorkflowExecutionDoc)
            });
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% This function should be called only after provider restart to handle
%% stale (processes handling execution no longer exists) workflow.
%% Specified workflow execution is:
%% a) terminated as ?CRASHED/?CANCELLED/?FAILED if execution was already stopping
%% b) terminated as ?INTERRUPTED otherwise (running execution was interrupted by
%%    provider shutdown). In such case, it will be resumed.
%% @end
%%--------------------------------------------------------------------
-spec on_provider_restart(atm_workflow_execution:id()) -> ok | no_return().
on_provider_restart(AtmWorkflowExecutionId) ->
    {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),
    AtmWorkflowExecutionEnv = acquire_global_env(AtmWorkflowExecutionDoc),

    try
        AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(AtmWorkflowExecutionEnv),
        atm_lane_execution_handler:stop({current, current}, interrupt, AtmWorkflowExecutionCtx),
        case end_workflow_execution(AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) of
            #document{value = #atm_workflow_execution{status = ?INTERRUPTED_STATUS}} ->
                UserCtx = atm_workflow_execution_auth:get_user_ctx(atm_workflow_execution_ctx:get_auth(
                    AtmWorkflowExecutionCtx
                )),
                resume(UserCtx, AtmWorkflowExecutionId);
            _ ->
                ok
        end
    catch throw:{session_acquisition_failed, _} = Reason ->
        handle_exception(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, throw, Reason, [])
    end,

    ok.


-spec on_openfaas_down(atm_workflow_execution:id(), errors:error()) ->
    ok | no_return().
on_openfaas_down(AtmWorkflowExecutionId, Error) ->
    {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),
    AtmWorkflowExecutionEnv = acquire_global_env(AtmWorkflowExecutionDoc),

    try
        AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(AtmWorkflowExecutionEnv),

        Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
        LogContent = #{
            <<"description">> => "OpenFaaS service is not healthy (see error reason).",
            <<"reason">> => errors:to_json(Error)
        },
        atm_workflow_execution_logger:workflow_critical(LogContent, Logger),

        %% TODO MW call to engine that no responses will be sent ??
        atm_lane_execution_handler:stop({current, current}, interrupt, AtmWorkflowExecutionCtx),
        end_workflow_execution(AtmWorkflowExecutionId, AtmWorkflowExecutionCtx)
    catch throw:{session_acquisition_failed, _} = Reason ->
        handle_exception(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, throw, Reason, [])
    end,

    ok.


%%%===================================================================
%%% workflow_handler callbacks
%%%===================================================================


-spec prepare_lane(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_lane_execution:lane_run_selector()
) ->
    {ok, workflow_engine:lane_spec()} | error.
prepare_lane(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmLaneRunSelector) ->
    atm_lane_execution_handler:prepare(
        AtmLaneRunSelector,
        AtmWorkflowExecutionId,
        atm_workflow_execution_ctx:acquire(AtmWorkflowExecutionEnv)
    ).


-spec restart_lane(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_lane_execution:lane_run_selector()
) ->
    {ok, workflow_engine:lane_spec()} | error.
restart_lane(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmLaneRunSelector) ->
    atm_lane_execution_handler:resume(
        AtmLaneRunSelector,
        AtmWorkflowExecutionId,
        atm_workflow_execution_ctx:acquire(AtmWorkflowExecutionEnv)
    ).


-spec run_task_for_item(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id(),
    atm_task_executor:job_batch_id(),
    [automation:item()]
) ->
    ok | {error, running_item_failed} | {error, task_already_stopping} | {error, task_already_stopped}.
run_task_for_item(
    _AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmTaskExecutionId,
    AtmJobBatchId, ItemBatch
) ->
    atm_task_execution_handler:run_job_batch(
        atm_workflow_execution_ctx:acquire(AtmTaskExecutionId, AtmWorkflowExecutionEnv),
        AtmTaskExecutionId, AtmJobBatchId, ItemBatch
    ).


-spec process_task_result_for_item(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id(),
    [automation:item()],
    atm_task_executor:job_batch_result()
) ->
    ok | error.
process_task_result_for_item(
    _AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmTaskExecutionId,
    ItemBatch, JobBatchResult
) ->
    atm_task_execution_handler:process_job_batch_result(
        atm_workflow_execution_ctx:acquire(AtmTaskExecutionId, AtmWorkflowExecutionEnv),
        AtmTaskExecutionId, ItemBatch, JobBatchResult
    ).


-spec process_streamed_task_data(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id(),
    atm_task_executor:streamed_data()
) ->
    ok | error.
process_streamed_task_data(
    _AtmWorkflowExecutionId,
    AtmWorkflowExecutionEnv,
    AtmTaskExecutionId,
    StreamedData
) ->
    atm_task_execution_handler:process_streamed_data(
        atm_workflow_execution_ctx:acquire(AtmTaskExecutionId, AtmWorkflowExecutionEnv),
        AtmTaskExecutionId, StreamedData
    ).


-spec handle_task_results_processed_for_all_items(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id()
) ->
    ok.
handle_task_results_processed_for_all_items(
    AtmWorkflowExecutionId,
    _AtmWorkflowExecutionEnv,
    AtmTaskExecutionId
) ->
    atm_openfaas_result_stream_handler:trigger_conclusion(AtmWorkflowExecutionId, AtmTaskExecutionId).


-spec handle_task_execution_stopped(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id()
) ->
    ok.
handle_task_execution_stopped(_AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId) ->
    atm_task_execution_handler:handle_stopped(AtmTaskExecutionId).


-spec report_item_error(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    automation:item()
) ->
    ok.
report_item_error(_AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, ItemBatch) ->
    AtmWorkflowExecutionAuth = atm_workflow_execution_env:acquire_auth(AtmWorkflowExecutionEnv),

    % NOTE: atm_store_api is bypassed for performance reasons. It is possible as list store update
    % does not modify store document itself but only referenced infinite log
    atm_list_store_container:update_content(
        atm_workflow_execution_env:get_lane_run_exception_store_container(AtmWorkflowExecutionEnv),
        #atm_store_content_update_req{
            workflow_execution_auth = AtmWorkflowExecutionAuth,
            argument = ItemBatch,
            options = #atm_list_store_content_update_options{function = extend}
        }
    ),

    ok.


-spec handle_lane_execution_stopped(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_lane_execution:lane_run_selector()
) ->
    workflow_handler:lane_stopped_callback_result().
handle_lane_execution_stopped(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmLaneRunSelector) ->
    atm_lane_execution_handler:handle_stopped(
        AtmLaneRunSelector, AtmWorkflowExecutionId,
        atm_workflow_execution_ctx:acquire(AtmWorkflowExecutionEnv)
    ).


-spec handle_workflow_execution_stopped(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record()
) ->
    ok.
handle_workflow_execution_stopped(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv) ->
    AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(AtmWorkflowExecutionEnv),
    end_workflow_execution(AtmWorkflowExecutionId, AtmWorkflowExecutionCtx),
    ok.


-spec handle_exception(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    throw | error | exit,
    term(),
    list()
) ->
    ?END_EXECUTION.
handle_exception(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, Type, Reason, Stacktrace) ->
    AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(
        undefined,
        % user session may no longer be available (e.g. session expiration caused exception) -
        % use provider root session just to stop execution
        get_root_workflow_execution_auth(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv),
        AtmWorkflowExecutionEnv
    ),

    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    log_exception(Logger, Type, Reason, Stacktrace),

    StoppingReason = case Type of
        throw -> interrupt;
        _ -> crash
    end,
    atm_lane_execution_handler:stop({current, current}, StoppingReason, AtmWorkflowExecutionCtx),
    end_workflow_execution(AtmWorkflowExecutionId, AtmWorkflowExecutionCtx),

    ?END_EXECUTION.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec acquire_global_env(atm_workflow_execution:doc()) -> atm_workflow_execution_env:record().
acquire_global_env(#document{key = AtmWorkflowExecutionId, value = #atm_workflow_execution{
    space_id = SpaceId,
    incarnation = AtmWorkflowExecutionIncarnation,
    store_registry = AtmGlobalStoreRegistry,
    system_audit_log_store_id = AtmWorkflowAuditLogStoreId
}}) ->
    {ok, #atm_store{container = AtmWorkflowAuditLogStoreContainer}} = atm_store_api:get(
        AtmWorkflowAuditLogStoreId
    ),
    Env = atm_workflow_execution_env:build(
        SpaceId, AtmWorkflowExecutionId, AtmWorkflowExecutionIncarnation, AtmGlobalStoreRegistry
    ),
    atm_workflow_execution_env:set_workflow_audit_log_store_container(
        AtmWorkflowAuditLogStoreContainer, Env
    ).


%% @private
-spec end_workflow_execution(
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    atm_workflow_execution:doc().
end_workflow_execution(AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    {ok, AtmWorkflowExecutionDoc0} = atm_workflow_execution:get(AtmWorkflowExecutionId),
    ensure_all_lane_runs_stopped(AtmWorkflowExecutionDoc0, AtmWorkflowExecutionCtx),
    {ok, AtmWorkflowExecutionDoc1} = delete_all_lane_runs_prepared_in_advance(
        AtmWorkflowExecutionDoc0
    ),
    freeze_global_stores(AtmWorkflowExecutionDoc1),

    atm_workflow_execution_session:terminate(AtmWorkflowExecutionId),

    {ok, StoppedAtmWorkflowExecutionDoc} = atm_workflow_execution_status:handle_stopped(
        AtmWorkflowExecutionId
    ),
    notify_stopped(StoppedAtmWorkflowExecutionDoc),
    StoppedAtmWorkflowExecutionDoc.


%% @private
-spec ensure_all_lane_runs_stopped(
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    ok.
ensure_all_lane_runs_stopped(#document{
    key = AtmWorkflowExecutionId,
    value = AtmWorkflowExecution = #atm_workflow_execution{
        lanes_count = AtmLanesCount,
        current_lane_index = CurrentAtmLaneIndex
    }
}, AtmWorkflowExecutionCtx) ->
    lists:foreach(fun(AtmLaneIndex) ->
        AtmLaneRunSelector = {AtmLaneIndex, current},

        case atm_lane_execution:get_run(AtmLaneRunSelector, AtmWorkflowExecution) of
            {ok, #atm_lane_execution_run{status = Status}} ->
                case atm_lane_execution_status:status_to_phase(Status) of
                    ?WAITING_PHASE when AtmLaneIndex > CurrentAtmLaneIndex ->
                        atm_lane_execution_handler:stop(
                            AtmLaneRunSelector, interrupt, AtmWorkflowExecutionCtx
                        ),
                        atm_lane_execution_handler:handle_stopped(
                            AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx
                        );
                    ?ONGOING_PHASE ->
                        atm_lane_execution_handler:handle_stopped(
                            AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx
                        );
                    ?SUSPENDED_PHASE ->
                        ok;
                    ?ENDED_PHASE ->
                        ok
                end;
            ?ERROR_NOT_FOUND ->
                ok
        end
    end, lists:seq(CurrentAtmLaneIndex, AtmLanesCount)).


%% @private
-spec delete_all_lane_runs_prepared_in_advance(atm_workflow_execution:doc()) ->
    {ok, atm_workflow_execution:doc()}.
delete_all_lane_runs_prepared_in_advance(#document{
    key = AtmWorkflowExecutionId,
    value = AtmWorkflowExecution = #atm_workflow_execution{
        lanes_count = AtmLanesCount,
        current_lane_index = CurrentAtmLaneIndex,
        current_run_num = CurrentRunNum
    }
}) ->
    AtmLaneExecutionDiff = fun
        (AtmLaneExecution = #atm_lane_execution{runs = [
            AtmLaneRunPreparedInAdvance = #atm_lane_execution_run{run_num = RunNum}
            | PreviousLaneRuns
        ]}) when
            RunNum =:= undefined;
            RunNum =:= CurrentRunNum
        ->
            atm_lane_execution_factory:delete_run(AtmLaneRunPreparedInAdvance),
            {ok, AtmLaneExecution#atm_lane_execution{runs = PreviousLaneRuns}};

        (_) ->
            ?ERROR_NOT_FOUND
    end,
    NewAtmWorkflowExecution = lists_utils:foldl_while(fun(AtmLaneIndex, AtmWorkflowExecutionAcc) ->
        case atm_lane_execution:update(AtmLaneIndex, AtmLaneExecutionDiff, AtmWorkflowExecutionAcc) of
            {ok, NewAtmWorkflowExecutionAcc} -> {cont, NewAtmWorkflowExecutionAcc};
            ?ERROR_NOT_FOUND -> {halt, AtmWorkflowExecutionAcc}
        end
    end, AtmWorkflowExecution, lists:seq(CurrentAtmLaneIndex + 1, AtmLanesCount)),

    atm_workflow_execution:update(
        AtmWorkflowExecutionId,
        fun(_) -> {ok, NewAtmWorkflowExecution} end
    ).


%% @private
-spec freeze_global_stores(atm_workflow_execution:doc()) -> ok.
freeze_global_stores(#document{value = #atm_workflow_execution{
    store_registry = AtmStoreRegistry
}}) ->
    lists:foreach(
        fun(AtmStoreId) -> catch atm_store_api:freeze(AtmStoreId) end,
        maps:values(AtmStoreRegistry)
    ).


%% @private
-spec unfreeze_global_stores(atm_workflow_execution:doc()) -> ok.
unfreeze_global_stores(#document{value = #atm_workflow_execution{
    store_registry = AtmStoreRegistry
}}) ->
    lists:foreach(
        fun(AtmStoreId) -> atm_store_api:unfreeze(AtmStoreId) end,
        maps:values(AtmStoreRegistry)
    ).


%% @private
-spec notify_stopped(atm_workflow_execution:doc()) -> ok.
notify_stopped(#document{value = #atm_workflow_execution{callback = undefined}}) ->
    ok;
notify_stopped(#document{key = AtmWorkflowExecutionId, value = #atm_workflow_execution{
    status = AtmWorkflowExecutionStatus,
    callback = CallbackUrl
}}) ->
    spawn(fun() ->
        Headers = #{
            ?HDR_CONTENT_TYPE => <<"application/json">>
        },
        Payload = json_utils:encode(#{
            <<"atmWorkflowExecutionId">> => AtmWorkflowExecutionId,
            <<"status">> => AtmWorkflowExecutionStatus
        }),
        try_to_notify(
            AtmWorkflowExecutionId, CallbackUrl, Headers, Payload,
            ?INITIAL_NOTIFICATION_INTERVAL(), ?MAX_NOTIFICATION_RETRIES + 1
        )
    end),
    ok.


%% @private
-spec try_to_notify(
    atm_workflow_execution:id(),
    http_client:url(),
    http_client:headers(),
    http_client:body(),
    non_neg_integer(),
    non_neg_integer()
) ->
    ok.
try_to_notify(AtmWorkflowExecutionId, CallbackUrl, _Headers, _Payload, _Interval, 0) ->
    ?error(
        "Failed to send atm workflow execution (~s) notification to '~s' (no retries left)",
        [AtmWorkflowExecutionId, CallbackUrl]
    );
try_to_notify(AtmWorkflowExecutionId, CallbackUrl, Headers, Payload, Interval, RetriesLeft) ->
    case send_notification(CallbackUrl, Headers, Payload) of
        ok ->
            ok;
        {error, _} = Error ->
            ?warning(
                "Failed to send atm workflow execution (~s) notification to ~s due to ~p.~n"
                "Next retry in ~p seconds. Number of retries left: ~p",
                [AtmWorkflowExecutionId, CallbackUrl, Error, Interval / 1000, RetriesLeft - 1]
            ),
            timer:sleep(Interval),
            NextInterval = min(2 * Interval, ?MAX_NOTIFICATION_INTERVAL),
            try_to_notify(AtmWorkflowExecutionId, CallbackUrl, Headers, Payload, NextInterval, RetriesLeft - 1)
    end.


%% @private
-spec send_notification(http_client:url(), http_client:headers(), http_client:body()) ->
    ok | {error, term()}.
send_notification(CallbackUrl, Headers, Payload) ->
    case http_client:post(CallbackUrl, Headers, Payload) of
        {ok, ResponseCode, _, ResponseBody} ->
            case http_utils:is_success_code(ResponseCode) of
                true -> ok;
                false -> {error, {http_response, ResponseCode, ResponseBody}}
            end;
        {error, _} = Error ->
            Error
    end.


%% @private
-spec get_root_workflow_execution_auth(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record()
) ->
    atm_workflow_execution_auth:record().
get_root_workflow_execution_auth(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv) ->
    atm_workflow_execution_auth:build(
        atm_workflow_execution_env:get_space_id(AtmWorkflowExecutionEnv),
        AtmWorkflowExecutionId,
        ?ROOT_SESS_ID
    ).


%% @private
-spec log_exception(
    atm_workflow_execution_logger:record(),
    throw | error | exit,
    term(),
    list()
) ->
    ok.
log_exception(Logger, throw, {session_acquisition_failed, Error}, _Stacktrace) ->
    LogContent = #{
        <<"description">> => <<"Failed to acquire user session.">>,
        <<"reason">> => errors:to_json(Error)
    },
    atm_workflow_execution_logger:workflow_critical(LogContent, Logger);

log_exception(Logger, throw, Reason, _Stacktrace) ->
    LogContent = #{
        <<"description">> => <<"Unexpected error occured.">>,
        <<"reason">> => errors:to_json(Reason)
    },
    atm_workflow_execution_logger:workflow_critical(LogContent, Logger);

log_exception(Logger, Type, Reason, Stacktrace) ->
    LogContent = #{
        <<"description">> => <<"Unexpected emergency occured.">>,
        <<"reason">> => errors:to_json(?atm_examine_error(Type, Reason, Stacktrace))
    },
    atm_workflow_execution_logger:workflow_emergency(LogContent, Logger).
