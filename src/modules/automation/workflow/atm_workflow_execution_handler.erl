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
-include("workflow_engine.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").

% API
-export([
    init_engine/0,
    start/3,
    cancel/1,
    repeat/4
]).

% workflow_handler callbacks
-export([
    prepare_lane/3,
    restart_lane/3,

    process_item/6,
    process_result/5,
    report_item_error/3,

    handle_task_execution_ended/3,
    handle_lane_execution_ended/3,
    handle_workflow_execution_ended/2
]).


-define(ATM_WORKFLOW_EXECUTION_ENGINE, <<"atm_workflow_execution_engine">>).

-define(ENGINE_ASYNC_CALLS_LIMIT, op_worker:get_env(atm_workflow_engine_async_calls_limit, 1000)).
-define(ENGINE_SLOTS_COUNT, op_worker:get_env(atm_workflow_engine_slots_count, 20)).
-define(JOB_TIMEOUT_SEC, op_worker:get_env(atm_workflow_job_timeout_sec, 1800)).
-define(JOB_TIMEOUT_CHECK_PERIOD_SEC, op_worker:get_env(atm_workflow_job_timeout_check_period_sec, 1800)).

-define(INITIAL_NOTIFICATION_INTERVAL(), rand:uniform(timer:seconds(2))).
-define(MAX_NOTIFICATION_INTERVAL, timer:hours(2)).
-define(MAX_NOTIFICATION_RETRIES, 30).


-define(run(__ATM_WORKFLOW_EXECUTION_ID, __EXPR),
    try
        __EXPR
    catch __TYPE:__REASON:__STACKTRACE ->
        ?error_stacktrace(
            "Unexpected error during atm workflow execution (~p) in ~w:~w - ~w:~p",
            [__ATM_WORKFLOW_EXECUTION_ID, ?MODULE, ?FUNCTION_NAME, __TYPE, __REASON],
            __STACKTRACE
        ),
        error
    end
).


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


-spec cancel(atm_workflow_execution:id()) -> ok | errors:error().
cancel(AtmWorkflowExecutionId) ->
    case atm_lane_execution_status:handle_aborting({current, current}, AtmWorkflowExecutionId, cancel) of
        {ok, _} ->
            workflow_engine:cancel_execution(AtmWorkflowExecutionId);
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
                execution_context => acquire_env(AtmWorkflowExecutionDoc),
                first_lane_id => {CurrentAtmLaneIndex, CurrentRunNum},
                next_lane_id => case CurrentAtmLaneIndex < AtmLanesCount of
                    true -> {CurrentAtmLaneIndex + 1, current};
                    false -> undefined
                end
            });
        {error, _} = Error ->
            Error
    end.


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
    AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(undefined, AtmWorkflowExecutionEnv),

    try
        {ok, atm_lane_execution_handler:prepare(
            AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx
        )}
    catch Type:Reason:Stacktrace ->
        % TODO VFS-8273 use audit log
        ?error("[~p] FAILED TO PREPARE WORKFLOW DUE TO: ~p", [
            AtmWorkflowExecutionId, ?atm_examine_error(Type, Reason, Stacktrace)
        ]),
        error
    end.


-spec restart_lane(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_lane_execution:lane_run_selector()
) ->
    error.
restart_lane(_, _, _) ->
    error.


-spec process_item(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id(),
    [automation:item()],
    binary(),
    binary()
) ->
    ok | error.
process_item(
    AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmTaskExecutionId,
    ItemsBatch, ReportResultUrl, HeartbeatUrl
) ->
    AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(
        AtmTaskExecutionId, AtmWorkflowExecutionEnv
    ),
    ?run(AtmWorkflowExecutionId, atm_task_execution_handler:process_items_batch(
        AtmWorkflowExecutionCtx, AtmTaskExecutionId, ItemsBatch,
        ReportResultUrl, HeartbeatUrl
    )).


-spec process_result(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id(),
    [automation:item()],
    atm_task_executor:outcome()
) ->
    ok | error.
process_result(
    AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmTaskExecutionId,
    ItemsBatch, Outcome
) ->
    AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(
        AtmTaskExecutionId, AtmWorkflowExecutionEnv
    ),
    ?run(AtmWorkflowExecutionId, atm_task_execution_handler:process_outcome(
        AtmWorkflowExecutionCtx, AtmTaskExecutionId, ItemsBatch, Outcome
    )).


-spec report_item_error(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    automation:item()
) ->
    ok.
report_item_error(_AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, ItemsBatch) ->
    AtmLaneRunExceptionStoreContainer = atm_workflow_execution_env:get_lane_run_exception_store_container(
        AtmWorkflowExecutionEnv
    ),
    Operation = #atm_store_container_operation{
        type = extend,
        options = #{},
        argument = ItemsBatch,
        workflow_execution_auth = atm_workflow_execution_env:acquire_auth(AtmWorkflowExecutionEnv)
    },
    atm_list_store_container:apply_operation(AtmLaneRunExceptionStoreContainer, Operation),

    ok.


-spec handle_task_execution_ended(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id()
) ->
    ok.
handle_task_execution_ended(AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId) ->
    try
        ok = atm_task_execution_handler:handle_ended(AtmTaskExecutionId)
    catch Type:Reason:Stacktrace ->
        % TODO VFS-8273 use audit log
        ?error("[~p] FAILED TO MARK TASK EXECUTION ~p AS ENDED DUE TO: ~p", [
            AtmWorkflowExecutionId, AtmTaskExecutionId,
            ?atm_examine_error(Type, Reason, Stacktrace)
        ])
    end.


-spec handle_lane_execution_ended(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_lane_execution:lane_run_selector()
) ->
    workflow_handler:lane_ended_callback_result().
handle_lane_execution_ended(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmLaneRunSelector) ->
    AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(undefined, AtmWorkflowExecutionEnv),

    try
        atm_lane_execution_handler:handle_ended(
            AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx
        )
    catch Type:Reason:Stacktrace ->
        % TODO VFS-8273 use audit log
        ?error("[~p] FAILED TO MARK LANE RUN EXECUTION ~p AS ENDED DUE TO: ~p", [
            AtmWorkflowExecutionId, AtmLaneRunSelector, ?atm_examine_error(Type, Reason, Stacktrace)
        ])
    end.


-spec handle_workflow_execution_ended(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record()
) ->
    ok.
handle_workflow_execution_ended(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv) ->
    AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(undefined, AtmWorkflowExecutionEnv),

    try
        {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),
        ensure_all_lane_executions_ended(AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx),
        freeze_global_stores(AtmWorkflowExecutionDoc),

        atm_workflow_execution_session:terminate(AtmWorkflowExecutionId),

        {ok, EndedAtmWorkflowExecutionDoc} = atm_workflow_execution_status:handle_ended(
            AtmWorkflowExecutionId
        ),
        notify_ended(EndedAtmWorkflowExecutionDoc)
    catch Type:Reason:Stacktrace ->
        % TODO VFS-8273 use audit log
        ?error("[~p] FAILED TO MARK WORKFLOW EXECUTION AS ENDED DUE TO: ~p", [
            AtmWorkflowExecutionId, ?atm_examine_error(Type, Reason, Stacktrace)
        ])
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec ensure_all_lane_executions_ended(
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    ok.
ensure_all_lane_executions_ended(#document{
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
                    ?ENDED_PHASE ->
                        ok;
                    _ ->
                        atm_lane_execution_handler:handle_ended(
                            AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx
                        )
                end;
            ?ERROR_NOT_FOUND ->
                ok
        end
    end, lists:seq(CurrentAtmLaneIndex, AtmLanesCount)).


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
-spec acquire_env(atm_workflow_execution:doc()) -> atm_workflow_execution_env:record().
acquire_env(#document{key = AtmWorkflowExecutionId, value = #atm_workflow_execution{
    space_id = SpaceId,
    incarnation = AtmWorkflowExecutionIncarnation,
    store_registry = AtmGlobalStoreRegistry,
    system_audit_log_id = AtmWorkflowAuditLogId
}}) ->
    Env = atm_workflow_execution_env:build(
        SpaceId, AtmWorkflowExecutionId, AtmWorkflowExecutionIncarnation, AtmGlobalStoreRegistry
    ),

    AtmWorkflowAuditLogStoreContainer = case atm_store_api:get(AtmWorkflowAuditLogId) of
        {ok, #atm_store{container = Container}} ->
            Container;
        ?ERROR_NOT_FOUND ->
            undefined
    end,
    atm_workflow_execution_env:set_workflow_audit_log_store_container(
        AtmWorkflowAuditLogStoreContainer, Env
    ).


%% @private
-spec notify_ended(atm_workflow_execution:doc()) -> ok.
notify_ended(#document{value = #atm_workflow_execution{callback = undefined}}) ->
    ok;
notify_ended(#document{key = AtmWorkflowExecutionId, value = #atm_workflow_execution{
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
