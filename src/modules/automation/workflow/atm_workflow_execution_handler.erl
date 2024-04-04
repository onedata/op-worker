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
    init_stop/3,
    repeat/4,
    resume/2,
    force_continue/2
]).
-export([
    restart/1,
    on_openfaas_down/2
]).

% workflow_handler callbacks
-export([
    prepare_lane/3,
    resume_lane/3,
    handle_lane_execution_started/3,

    run_task_for_item/5,
    process_task_result_for_item/5,
    process_streamed_task_data/4,
    handle_task_results_processed_for_all_items/3,
    handle_task_execution_stopped/3,

    report_item_error/3,

    handle_lane_execution_stopped/3,

    handle_workflow_execution_stopped/2,

    handle_exception/5,
    handle_workflow_abruptly_stopped/3
]).

-type item() :: #atm_item_execution{}.
-export_type([item/0]).

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
        init_workflow_timeout_server => {true, ?JOB_TIMEOUT_CHECK_PERIOD_SEC},
        enqueuing_timeout => infinity
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

    ok = workflow_engine:execute_workflow(?ATM_WORKFLOW_EXECUTION_ENGINE, #{
        id => AtmWorkflowExecutionId,
        workflow_handler => ?MODULE,
        execution_context => AtmWorkflowExecutionEnv,
        first_lane_id => {1, 1},
        next_lane_id => case 1 < AtmLanesCount of
            true -> {2, current};
            false -> undefined
        end
    }),

    Logger = get_logger(UserCtx, AtmWorkflowExecutionEnv),
    ?atm_workflow_notice(Logger, #atm_workflow_log_schema{
        description = <<"Scheduled execution.">>,
        details = #{<<"scheduledLaneRunSelector">> => ?lane_run_selector_json({1, 1})}
    }).


-spec init_stop(
    user_ctx:ctx(),
    atm_workflow_execution:id(),
    cancel | pause | op_worker_stopping
) ->
    ok | errors:error().
init_stop(UserCtx, AtmWorkflowExecutionId, Reason) ->
    {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),
    AtmWorkflowExecutionEnv = acquire_global_env(AtmWorkflowExecutionDoc),
    SpaceId = atm_workflow_execution_env:get_space_id(AtmWorkflowExecutionEnv),

    AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(
        undefined,
        atm_workflow_execution_auth:build(SpaceId, AtmWorkflowExecutionId, UserCtx),
        AtmWorkflowExecutionEnv
    ),
    case atm_lane_execution_stop_handler:init_stop({current, current}, Reason, AtmWorkflowExecutionCtx) of
        {ok, stopping} ->
            ok;
        {ok, stopped} ->
            % atm workflow execution was stopped and there are no active processes handling it
            % (e.g. cancelling already suspended execution) - end procedures must be called manually
            finalize_workflow_execution(AtmWorkflowExecutionId, AtmWorkflowExecutionCtx),
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
            AtmWorkflowExecutionEnv = acquire_global_env(AtmWorkflowExecutionDoc),
            ok = atm_workflow_execution_session:init(AtmWorkflowExecutionId, UserCtx),

            CurrentAtmLaneRunSelector = {CurrentAtmLaneIndex, CurrentRunNum},
            ok = workflow_engine:execute_workflow(?ATM_WORKFLOW_EXECUTION_ENGINE, #{
                id => AtmWorkflowExecutionId,
                workflow_handler => ?MODULE,
                force_clean_execution => true,
                execution_context => AtmWorkflowExecutionEnv,
                first_lane_id => CurrentAtmLaneRunSelector,
                next_lane_id => case CurrentAtmLaneIndex < AtmLanesCount of
                    true -> {CurrentAtmLaneIndex + 1, current};
                    false -> undefined
                end
            }),

            Logger = get_logger(UserCtx, AtmWorkflowExecutionEnv),
            ?atm_workflow_notice(Logger, #atm_workflow_log_schema{
                selector = {lane_run, AtmLaneRunSelector},
                description = ?fmt_bin("Scheduled manual ~ts.", [Type]),
                details = #{
                    <<"repeatType">> => Type,
                    <<"scheduledLaneRunSelector">> => ?lane_run_selector_json(
                        CurrentAtmLaneRunSelector
                    )
                }
            });
        {error, _} = Error ->
            Error
    end.


-spec resume(user_ctx:ctx(), atm_workflow_execution:id()) ->
    ok | errors:error().
resume(UserCtx, AtmWorkflowExecutionId) ->
    case atm_lane_execution_status:handle_resume(AtmWorkflowExecutionId) of
        {ok, AtmWorkflowExecutionDoc = #document{value = #atm_workflow_execution{
            current_lane_index = CurrentAtmLaneIndex,
            current_run_num = CurrentRunNum
        }}} ->
            unfreeze_global_stores(AtmWorkflowExecutionDoc),
            AtmWorkflowExecutionEnv = acquire_global_env(AtmWorkflowExecutionDoc),
            ok = atm_workflow_execution_session:init(AtmWorkflowExecutionId, UserCtx),

            Logger = get_logger(UserCtx, AtmWorkflowExecutionEnv),
            ?atm_workflow_notice(Logger, #atm_workflow_log_schema{
                description = <<"Resuming execution...">>,
                details = #{<<"scheduledLaneRunSelector">> => ?lane_run_selector_json(
                    {CurrentAtmLaneIndex, CurrentRunNum}
                )}
            }),

            ok = workflow_engine:execute_workflow(?ATM_WORKFLOW_EXECUTION_ENGINE, #{
                id => AtmWorkflowExecutionId,
                workflow_handler => ?MODULE,
                execution_context => AtmWorkflowExecutionEnv
            });
        {error, _} = Error ->
            Error
    end.


-spec force_continue(user_ctx:ctx(), atm_workflow_execution:id()) ->
    ok | errors:error().
force_continue(UserCtx, AtmWorkflowExecutionId) ->
    case atm_workflow_execution_status:handle_forced_continue(AtmWorkflowExecutionId) of
        {ok, AtmWorkflowExecutionDoc = #document{value = #atm_workflow_execution{
            current_lane_index = CurrentAtmLaneIndex,
            current_run_num = CurrentRunNum,
            lanes_count = AtmLanesCount
        }}} ->
            unfreeze_global_stores(AtmWorkflowExecutionDoc),
            AtmWorkflowExecutionEnv = acquire_global_env(AtmWorkflowExecutionDoc),
            ok = atm_workflow_execution_session:init(AtmWorkflowExecutionId, UserCtx),

            CurrentAtmLaneRunSelector = {CurrentAtmLaneIndex, CurrentRunNum},
            workflow_engine:execute_workflow(?ATM_WORKFLOW_EXECUTION_ENGINE, #{
                id => AtmWorkflowExecutionId,
                workflow_handler => ?MODULE,
                force_clean_execution => true,
                execution_context => AtmWorkflowExecutionEnv,
                first_lane_id => CurrentAtmLaneRunSelector,
                next_lane_id => case CurrentAtmLaneIndex < AtmLanesCount of
                    true -> {CurrentAtmLaneIndex + 1, current};
                    false -> undefined
                end
            }),

            Logger = get_logger(UserCtx, AtmWorkflowExecutionEnv),
            ?atm_workflow_notice(Logger, #atm_workflow_log_schema{
                description = <<"Scheduled forced continuation.">>,
                details = #{<<"scheduledLaneRunSelector">> => ?lane_run_selector_json(
                    CurrentAtmLaneRunSelector
                )}
            });
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% This function should be called only after provider restart to:
%% 1. handle stale atm workflow executions (waiting or ongoing execution where
%%    processes no longer exists). They are:
%%    a) terminated as ?CRASHED/?CANCELLED/?FAILED if execution was already stopping
%%    b) terminated as ?INTERRUPTED otherwise (running execution was interrupted by
%%       provider shutdown). In such case, it will be resumed.
%% 2. resume atm workflow executions paused due to op worker stopping.
%% @end
%%--------------------------------------------------------------------
-spec restart(atm_workflow_execution:id()) -> ok | no_return().
restart(AtmWorkflowExecutionId) ->
    {ok, AtmWorkflowExecutionDoc = #document{value = AtmWorkflowExecution}} = atm_workflow_execution:get(
        AtmWorkflowExecutionId
    ),
    AtmWorkflowExecutionEnv = acquire_global_env(AtmWorkflowExecutionDoc),

    try
        {ok, #atm_lane_execution_run{stopping_reason = StoppingReason}} = atm_lane_execution:get_run(
            {current, current}, AtmWorkflowExecution
        ),

        case atm_workflow_execution_status:infer_phase(AtmWorkflowExecution) of
            RunningPhase when
                RunningPhase =:= ?WAITING_PHASE;
                RunningPhase =:= ?ONGOING_PHASE
            ->
                AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(AtmWorkflowExecutionEnv),

                Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
                ?atm_workflow_critical(Logger, <<
                    "Failed to cleanly suspend execution before the Oneprovider service has stopped."
                >>),

                atm_lane_execution_stop_handler:init_stop(
                    {current, current}, interrupt, AtmWorkflowExecutionCtx
                ),
                case finalize_workflow_execution(AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) of
                    #document{value = #atm_workflow_execution{status = ?INTERRUPTED_STATUS}} ->
                        ok = resume(AtmWorkflowExecutionCtx);
                    _ ->
                        ok
                end;

            ?SUSPENDED_PHASE when StoppingReason =:= op_worker_stopping ->
                AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(AtmWorkflowExecutionEnv),
                ok = resume(AtmWorkflowExecutionCtx);

            ?SUSPENDED_PHASE ->
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
        ?atm_workflow_critical(Logger, #{
            <<"description">> => <<"OpenFaaS service is not healthy (see error reason).">>,
            <<"details">> => #{<<"reason">> => errors:to_json(Error)}
        }),

        atm_lane_execution_stop_handler:init_stop({current, current}, interrupt, AtmWorkflowExecutionCtx)
    after
        workflow_engine:abandon(AtmWorkflowExecutionId, interrupt)
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
    atm_lane_execution_setup_handler:prepare(
        AtmLaneRunSelector,
        AtmWorkflowExecutionId,
        atm_workflow_execution_ctx:acquire(AtmWorkflowExecutionEnv)
    ).


-spec resume_lane(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_lane_execution:lane_run_selector()
) ->
    {ok, workflow_engine:lane_spec()} | error.
resume_lane(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmLaneRunSelector) ->
    atm_lane_execution_setup_handler:resume(
        AtmLaneRunSelector,
        AtmWorkflowExecutionId,
        atm_workflow_execution_ctx:acquire(AtmWorkflowExecutionEnv)
    ).


-spec handle_lane_execution_started(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_lane_execution:lane_run_selector()
) ->
    {atm_lane_execution:lane_run_selector(), atm_workflow_execution_env:record()}.
handle_lane_execution_started(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv0, AtmLaneRunSelector) ->
    {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),

    ResolvedAtmLaneRunSelector = atm_lane_execution:try_resolving_lane_run_selector(
        AtmLaneRunSelector, AtmWorkflowExecutionDoc#document.value
    ),
    AtmWorkflowExecutionEnv1 = atm_workflow_execution_env:ensure_task_selector_registry_up_to_date(
        AtmWorkflowExecutionDoc, ResolvedAtmLaneRunSelector, AtmWorkflowExecutionEnv0
    ),

    {ResolvedAtmLaneRunSelector, AtmWorkflowExecutionEnv1}.


-spec run_task_for_item(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id(),
    atm_task_executor:job_batch_id(),
    [item()]
) ->
    ok | {error, running_item_failed} | {error, task_already_stopping} | {error, task_already_stopped}.
run_task_for_item(
    _AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmTaskExecutionId,
    AtmJobBatchId, ItemBatch
) ->
    atm_task_execution_job_handler:run_job_batch(
        atm_workflow_execution_ctx:acquire(AtmTaskExecutionId, AtmWorkflowExecutionEnv),
        AtmTaskExecutionId, AtmJobBatchId, ItemBatch
    ).


-spec process_task_result_for_item(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id(),
    [item()],
    atm_task_executor:job_batch_result()
) ->
    ok | error.
process_task_result_for_item(
    _AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmTaskExecutionId,
    ItemBatch, JobBatchResult
) ->
    atm_task_execution_job_handler:process_job_batch_result(
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
    atm_task_execution_stream_handler:process_streamed_data(
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
    _AtmWorkflowExecutionId,
    AtmWorkflowExecutionEnv,
    AtmTaskExecutionId
) ->
    AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(
        AtmTaskExecutionId, AtmWorkflowExecutionEnv
    ),

    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    ?atm_workflow_debug(Logger, #atm_workflow_log_schema{
        selector = {task, AtmTaskExecutionId},
        description = <<"Processed all streamed results.">>,
        referenced_tasks = [AtmTaskExecutionId]
    }),

    atm_task_execution_stream_handler:trigger_stream_conclusion(
        AtmWorkflowExecutionCtx, AtmTaskExecutionId
    ).


-spec handle_task_execution_stopped(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id()
) ->
    ok.
handle_task_execution_stopped(_AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmTaskExecutionId) ->
    atm_task_execution_stop_handler:handle_stopped(
        atm_workflow_execution_ctx:acquire(AtmTaskExecutionId, AtmWorkflowExecutionEnv),
        AtmTaskExecutionId
    ).


-spec report_item_error(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    [item()]
) ->
    ok.
report_item_error(_AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, ItemBatch) ->
    AtmWorkflowExecutionAuth = atm_workflow_execution_env:acquire_auth(AtmWorkflowExecutionEnv),

    % NOTE: atm_store_api is bypassed for performance reasons. It is possible as exception
    % store update does not modify store document itself but only referenced infinite log
    atm_exception_store_container:update_content(
        atm_workflow_execution_env:get_lane_run_exception_store_container(AtmWorkflowExecutionEnv),
        #atm_store_content_update_req{
            workflow_execution_auth = AtmWorkflowExecutionAuth,
            argument = ItemBatch,
            options = #atm_exception_store_content_update_options{function = extend}
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
    atm_lane_execution_stop_handler:handle_stopped(
        AtmLaneRunSelector, AtmWorkflowExecutionId,
        atm_workflow_execution_ctx:acquire(AtmWorkflowExecutionEnv)
    ).


-spec handle_workflow_execution_stopped(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record()
) ->
    workflow_handler:progress_data_persistence().
handle_workflow_execution_stopped(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv) ->
    AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(AtmWorkflowExecutionEnv),

    infer_progress_data_persistence_policy(finalize_workflow_execution(
        AtmWorkflowExecutionId, AtmWorkflowExecutionCtx
    )).


-spec handle_exception(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    throw | error | exit,
    term(),
    list()
) ->
    interrupt | crash.
handle_exception(
    AtmWorkflowExecutionId,
    AtmWorkflowExecutionEnv,
    ExceptionType,
    ExceptionReason,
    ExceptionStacktrace
) ->
    AtmWorkflowExecutionCtx = get_root_workflow_execution_ctx(
        AtmWorkflowExecutionId, AtmWorkflowExecutionEnv
    ),

    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    log_exception(Logger, ExceptionType, ExceptionReason, ExceptionStacktrace),

    AbruptStoppingReason = case ExceptionType of
        throw -> interrupt;
        _ -> crash
    end,

    case atm_lane_execution_stop_handler:init_stop(
        {current, current}, AbruptStoppingReason, AtmWorkflowExecutionCtx
    ) of
        {ok, _} ->
            AbruptStoppingReason;

        % if last lane run already stopped then abrupt stopping hasn't crashed
        % and only workflow needs to be marked as stopped
        ?ERROR_ATM_INVALID_STATUS_TRANSITION(Status, ?STOPPING_STATUS) ->
            LastAtmLaneRunPhase = atm_lane_execution_status:status_to_phase(Status),
            case lists:member(LastAtmLaneRunPhase, [?SUSPENDED_PHASE, ?ENDED_PHASE]) of
                true -> AbruptStoppingReason;
                _ -> crash
            end;

        {error, _} ->
            crash
    end.


-spec handle_workflow_abruptly_stopped(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    undefined | interrupt | crash
) ->
    workflow_handler:progress_data_persistence().
handle_workflow_abruptly_stopped(
    AtmWorkflowExecutionId,
    AtmWorkflowExecutionEnv,
    OriginalAbruptStoppingReason
) ->
    AtmWorkflowExecutionCtx = get_root_workflow_execution_ctx(
        AtmWorkflowExecutionId, AtmWorkflowExecutionEnv
    ),

    AbruptStoppingReason = try
        finalize_workflow_execution_components(AtmWorkflowExecutionId, AtmWorkflowExecutionCtx),
        utils:ensure_defined(OriginalAbruptStoppingReason, crash)
    catch Type:Reason:Stacktrace ->
        ?error_stacktrace(
            "Emergency atm workflow execution components finalization failed due to ~tp:~tp",
            [Type, Reason],
            Stacktrace
        ),
        crash
    end,

    {ok, StoppedAtmWorkflowExecutionDoc} = case AbruptStoppingReason of
        crash -> atm_workflow_execution_status:handle_crashed(AtmWorkflowExecutionId);
        _ -> atm_workflow_execution_status:handle_stopped(AtmWorkflowExecutionId)
    end,
    notify_stopped(StoppedAtmWorkflowExecutionDoc),
    infer_progress_data_persistence_policy(StoppedAtmWorkflowExecutionDoc).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_logger(user_ctx:ctx(), atm_workflow_execution_env:record()) ->
    atm_workflow_execution_logger:record().
get_logger(UserCtx, AtmWorkflowExecutionEnv) ->
    SpaceId = atm_workflow_execution_env:get_space_id(AtmWorkflowExecutionEnv),
    AtmWorkflowExecutionId = atm_workflow_execution_env:get_workflow_execution_id(AtmWorkflowExecutionEnv),
    AtmWorkflowExecutionAuth = atm_workflow_execution_auth:build(SpaceId, AtmWorkflowExecutionId, UserCtx),
    atm_workflow_execution_env:build_logger(undefined, AtmWorkflowExecutionAuth, AtmWorkflowExecutionEnv).


%% @private
-spec acquire_global_env(atm_workflow_execution:doc()) -> atm_workflow_execution_env:record().
acquire_global_env(Doc = #document{key = AtmWorkflowExecutionId, value = #atm_workflow_execution{
    space_id = SpaceId,
    incarnation = AtmWorkflowExecutionIncarnation,
    store_registry = AtmGlobalStoreRegistry,
    system_audit_log_store_id = AtmWorkflowAuditLogStoreId,
    log_level = LogLevel
}}) ->
    {ok, #atm_store{container = AtmWorkflowAuditLogStoreContainer}} = atm_store_api:get(
        AtmWorkflowAuditLogStoreId
    ),
    Env0 = atm_workflow_execution_env:build(
        SpaceId, AtmWorkflowExecutionId, AtmWorkflowExecutionIncarnation,
        LogLevel, AtmGlobalStoreRegistry
    ),
    Env1 = atm_workflow_execution_env:set_workflow_audit_log_store_container(
        AtmWorkflowAuditLogStoreContainer, Env0
    ),
    atm_workflow_execution_env:ensure_task_selector_registry_up_to_date(Doc, {current, current}, Env1).


%% @private
-spec resume(atm_workflow_execution_ctx:record()) -> ok | errors:error().
resume(AtmWorkflowExecutionCtx) ->
    AtmWorkflowExecutionId = atm_workflow_execution_ctx:get_workflow_execution_id(
        AtmWorkflowExecutionCtx
    ),
    AtmWorkflowExecutionAuth = atm_workflow_execution_ctx:get_auth(AtmWorkflowExecutionCtx),
    UserCtx = atm_workflow_execution_auth:get_user_ctx(AtmWorkflowExecutionAuth),

    resume(UserCtx, AtmWorkflowExecutionId).


%% @private
-spec infer_progress_data_persistence_policy(atm_workflow_execution:doc()) ->
    workflow_handler:progress_data_persistence().
infer_progress_data_persistence_policy(#document{value = AtmWorkflowExecution}) ->
    case atm_workflow_execution_status:infer_phase(AtmWorkflowExecution) of
        ?SUSPENDED_PHASE -> save_progress;
        ?ENDED_PHASE -> clean_progress
    end.


%% @private
-spec finalize_workflow_execution(
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    atm_workflow_execution:doc().
finalize_workflow_execution(AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    finalize_workflow_execution_components(AtmWorkflowExecutionId, AtmWorkflowExecutionCtx),

    {ok, StoppedAtmWorkflowExecutionDoc = #document{
        value = #atm_workflow_execution{
            status = StoppedStatus
        }
    }} = atm_workflow_execution_status:handle_stopped(AtmWorkflowExecutionId),

    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    ?atm_workflow_notice(Logger, #atm_workflow_log_schema{
        description = <<"Stopped execution.">>,
        details = #{<<"status">> => StoppedStatus}
    }),

    notify_stopped(StoppedAtmWorkflowExecutionDoc),
    StoppedAtmWorkflowExecutionDoc.


%% @private
-spec finalize_workflow_execution_components(
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    ok.
finalize_workflow_execution_components(AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    ensure_all_lane_runs_stopped(AtmWorkflowExecutionId, AtmWorkflowExecutionCtx),
    AtmWorkflowExecutionDoc = delete_all_lane_runs_prepared_in_advance(
        AtmWorkflowExecutionId, AtmWorkflowExecutionCtx
    ),
    freeze_global_stores(AtmWorkflowExecutionDoc),

    case atm_lane_execution:get_run({current, current}, AtmWorkflowExecutionDoc#document.value) of
        {ok, #atm_lane_execution_run{stopping_reason = op_worker_stopping}} ->
            % Do not terminate session for executions paused due to op worker stopping
            % as it will be resumed right after provider restarts
            ok;
        _ ->
            atm_workflow_execution_session:terminate(AtmWorkflowExecutionId)
    end.


%% @private
-spec ensure_all_lane_runs_stopped(
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    ok.
ensure_all_lane_runs_stopped(AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    {ok, #document{
        value = AtmWorkflowExecution = #atm_workflow_execution{
            lanes_count = AtmLanesCount,
            current_lane_index = CurrentAtmLaneIndex,
            current_run_num = CurrentRunNum
        }
    }} = atm_workflow_execution:get(AtmWorkflowExecutionId),

    lists:foreach(fun(AtmLaneIndex) ->
        IsAtmLaneRunPreparedInAdvance = AtmLaneIndex > CurrentAtmLaneIndex,
        AtmLaneRunSelector = {AtmLaneIndex, case IsAtmLaneRunPreparedInAdvance of
            true -> current;  % lane runs prepared in advance do not have nums assigned
            false -> CurrentRunNum
        end},

        case atm_lane_execution:get_run(AtmLaneRunSelector, AtmWorkflowExecution) of
            {ok, #atm_lane_execution_run{status = Status}} ->
                case atm_lane_execution_status:status_to_phase(Status) of
                    ?WAITING_PHASE when IsAtmLaneRunPreparedInAdvance ->
                        atm_lane_execution_stop_handler:init_stop(
                            AtmLaneRunSelector, interrupt, AtmWorkflowExecutionCtx
                        ),
                        atm_lane_execution_stop_handler:handle_stopped(
                            AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx
                        );
                    ?ONGOING_PHASE ->
                        atm_lane_execution_stop_handler:handle_stopped(
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
-spec delete_all_lane_runs_prepared_in_advance(
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    atm_workflow_execution:doc().
delete_all_lane_runs_prepared_in_advance(AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    % lane runs MUST be deleted outside of workflow execution document update
    % to delete it's components: e.g. task execution documents
    Key = {?MODULE, AtmWorkflowExecutionId},
    {ok, NewDoc, DeletedLaneRunsIndices} = critical_section:run(Key, fun() ->
        {ok, #document{
            value = AtmWorkflowExecution = #atm_workflow_execution{
                lanes_count = AtmLanesCount,
                current_lane_index = CurrentAtmLaneIndex,
                current_run_num = CurrentRunNum
            }
        }} = atm_workflow_execution:get(AtmWorkflowExecutionId),

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
        {NewAtmWorkflowExecution, Indices} = lists_utils:foldl_while(fun
            (AtmLaneIndex, Acc = {AtmWorkflowExecutionAcc, IndicesAcc}) ->
                case atm_lane_execution:update(
                    AtmLaneIndex, AtmLaneExecutionDiff, AtmWorkflowExecutionAcc
                ) of
                    {ok, NewAtmWorkflowExecutionAcc} ->
                        {cont, {NewAtmWorkflowExecutionAcc, [AtmLaneIndex | IndicesAcc]}};
                    ?ERROR_NOT_FOUND ->
                        {halt, Acc}
                end
        end, {AtmWorkflowExecution, []}, lists:seq(CurrentAtmLaneIndex + 1, AtmLanesCount)),

        {ok, Doc} = atm_workflow_execution:update(AtmWorkflowExecutionId, fun(_) ->
            {ok, NewAtmWorkflowExecution}
        end),

        {ok, Doc, Indices}
    end),

    case DeletedLaneRunsIndices of
        [] ->
            ok;
        _ ->
            Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
            ?atm_workflow_info(Logger, #atm_workflow_log_schema{
                description = <<"Deleted lane runs that were prepared in advance.">>,
                details = #{<<"laneIndices">> => lists:sort(DeletedLaneRunsIndices)}
            })
    end,

    NewDoc.


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
        "Failed to send atm workflow execution (~ts) notification to '~ts' (no retries left)",
        [AtmWorkflowExecutionId, CallbackUrl]
    );
try_to_notify(AtmWorkflowExecutionId, CallbackUrl, Headers, Payload, Interval, RetriesLeft) ->
    case send_notification(CallbackUrl, Headers, Payload) of
        ok ->
            ok;
        {error, _} = Error ->
            ?warning(
                "Failed to send atm workflow execution (~ts) notification to ~ts due to ~tp.~n"
                "Next retry in ~tp seconds. Number of retries left: ~tp",
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
get_root_workflow_execution_ctx(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv) ->
    % user session may no longer be available (e.g. session expiration caused exception) -
    % use provider root session just to stop execution
    AtmWorkflowExecutionAuth = atm_workflow_execution_auth:build(
        atm_workflow_execution_env:get_space_id(AtmWorkflowExecutionEnv),
        AtmWorkflowExecutionId,
        ?ROOT_SESS_ID
    ),

    atm_workflow_execution_ctx:acquire(undefined, AtmWorkflowExecutionAuth, AtmWorkflowExecutionEnv).


%% @private
-spec log_exception(
    atm_workflow_execution_logger:record(),
    throw | error | exit,
    term(),
    list()
) ->
    ok.
log_exception(Logger, throw, {session_acquisition_failed, Error}, _Stacktrace) ->
    ?atm_workflow_critical(Logger, #{
        <<"description">> => <<"Failed to acquire user session.">>,
        <<"details">> => #{<<"reason">> => errors:to_json(Error)}
    });

log_exception(Logger, throw, Reason, _Stacktrace) ->
    ?atm_workflow_critical(Logger, #{
        <<"description">> => <<"Unexpected error occured.">>,
        <<"details">> => #{<<"reason">> => errors:to_json(Reason)}
    });

log_exception(Logger, Type, Reason, Stacktrace) ->
    Error = ?examine_exception(Type, Reason, Stacktrace),

    ?atm_workflow_emergency(Logger, #{
        <<"description">> => <<"Unexpected emergency occured.">>,
        <<"details">> => #{<<"reason">> => errors:to_json(Error)}
    }).
