%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module used to execute workflows. It uses wpool that manage processes
%%% which execute tasks. It operates on slots that represent these
%%% processes. Acquisition of slot is equal to acquisition of the right to
%%% execute task on pool's process. Pool processes use workflow_engine_state
%%% to choose execution that should acquire free slot and
%%% then workflow_execution_state to choose task and item to be
%%% executed.
%%% % TODO VFS-7919 - move all workflow_handler callback calls to separate module.
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_engine).
-author("Michal Wrzeszcz").

-include("workflow_engine.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([init/1, init/2, execute_workflow/2, cleanup_execution/1,
    init_cancel_procedure/1, init_cancel_procedure/2,
    wait_for_pending_callbacks/1, finish_cancel_procedure/1, abandon/2]).
-export([stream_task_data/3, report_task_data_streaming_concluded/3]).
-export([report_async_task_result/3, report_async_task_heartbeat/2]).
%% Framework internal API
-export([get_async_call_pools/1, trigger_job_scheduling/1,
    call_handler/5, call_handle_task_execution_stopped_for_all_tasks/4,
    call_handle_task_results_processed_for_all_items_for_all_tasks/4, call_handlers_for_cancelled_lane/5,
    handle_exception/8, execute_exception_handler/6, get_enqueuing_timeout/1]).
%% Test API
-export([set_enqueuing_timeout/2]).

%% Functions exported for internal_services engine - do not call directly
-export([init_service/2, takeover_service/3]).

%% Function executed by wpool - do not call directly
-export([process_job_or_result/3, process_streamed_task_data/3, prepare_lane/7]).

-type id() :: binary(). % Id of an engine
-type execution_id() :: binary().
-type execution_context() :: term().
-type lane_id() :: term(). % lane_id is opaque term for workflow engine (ids are managed by modules that use workflow engine)
-type task_id() :: binary().
% Data connected with task provided using stream_task_data/3 function (not processed as a part of job).
-type streamed_task_data() :: {chunk, term()} | errors:error().
-type stream_closing_result() :: success | {failure, term()}.
-type subject_id() :: workflow_cached_item:id() | 
    workflow_cached_async_result:result_ref() | workflow_cached_task_data:id().
-type execution_spec() :: #execution_spec{}.
-type processing_stage() :: ?SYNC_CALL | ?ASYNC_CALL_STARTED | ?ASYNC_CALL_ENDED | ?ASYNC_RESULT_PROCESSED.
-type handler_execution_result() :: workflow_handler:handler_execution_result() | {ok, KeepaliveTimeout :: time:seconds()}.
-type processing_result() :: handler_execution_result() | workflow_handler:async_processing_result().
-type preparation_mode() :: ?PREPARE_CURRENT | ?PREPARE_IN_ADVANCE.
-type cancel_pred() :: #{lane_id => lane_id()}.

%% @formatter:off
-type options() :: #{
    slots_limit => non_neg_integer(),
    workflow_async_call_pools_to_use => [{workflow_async_call_pool:id(), SlotsLimit :: non_neg_integer()}],
    default_keepalive_timeout => time:seconds(),
    enqueuing_timeout => time:seconds() | infinity, % async task can be enqueued after calling process_task_result_for_item
                                                    % (depending on workflow_handler implementation) ; in such a case,
                                                    % first heartbeat (or result) should appear before enqueuing_timeout
    init_workflow_timeout_server => {true, workflow_timeout_monitor:check_period()} | false
}.

-type workflow_execution_spec() :: #{
    id := id(),
    workflow_handler := workflow_handler:handler(),
    execution_context => execution_context(),
    first_lane_id => lane_id(), % does not have to be defined if execution is resumed from snapshot
    next_lane_id => lane_id(),
    force_clean_execution => boolean(),
    snapshot_mode => workflow_execution_state:snapshot_mode()
}.

-type task_type() :: sync | async.
-type task_spec() :: #{
    type := task_type(),
    data_stream_enabled => boolean(),
    async_call_pools => [workflow_async_call_pool:id()] | undefined,
    keepalive_timeout => time:seconds()
}.
-type parallel_box_spec() :: #{task_id() => task_spec()}.
-type lane_spec() :: #{
    parallel_boxes := [parallel_box_spec()],
    iterator => iterator:iterator(),
    execution_context := execution_context(),
    failure_count_to_cancel => non_neg_integer()
}.
-type execution_ended_info() :: #execution_ended{}.
%% @formatter:on

-export_type([id/0, execution_id/0, execution_context/0, lane_id/0, task_id/0, streamed_task_data/0, stream_closing_result/0,
    subject_id/0, execution_spec/0, processing_stage/0, handler_execution_result/0, processing_result/0,
    task_spec/0, parallel_box_spec/0, lane_spec/0, preparation_mode/0, cancel_pred/0]).

-type handler_function() :: atom().
-type handler_args() :: [term()].
-type handler_result() :: term().

-define(POOL_ID(EngineId), binary_to_atom(EngineId, utf8)).
-define(DEFAULT_SLOT_COUNT, 20).
-define(DEFAULT_CALLS_LIMIT, 1000).
-define(DEFAULT_TIMEOUT_CHECK_PERIOD_SEC, 300).
-define(USE_TIMEOUT_SERVER_DEFAULT, {true, ?DEFAULT_TIMEOUT_CHECK_PERIOD_SEC}).
-define(DEFAULT_KEEPALIVE_TIMEOUT_SEC, 300).

-define(WF_ERROR_NOTHING_TO_START, {error, nothing_to_start}).

% Job triggering modes (see function trigger_job_scheduling/2)
-define(TAKE_UP_FREE_SLOTS, take_up_free_slots).
-define(FOR_CURRENT_SLOT_FIRST, for_current_slot_first).

%%%===================================================================
%%% API
%%%===================================================================

-spec init(id()) -> ok.
init(Id) ->
    init(Id, #{}).

-spec init(id(), options()) -> ok.
init(Id, Options) ->
    % TODO VFS-7788 Implement internal_service HA callbacks
    ServiceOptions = #{
        start_function => init_service,
        start_function_args => [Id, Options],
        takeover_function => takeover_service,
        takeover_function_args => [Id, Options, node()],
        migrate_function => undefined,
        migrate_function_args => [],
        stop_function => stop_service,
        stop_function_args => [Id]
    },
    ok = internal_services_manager:start_service(?MODULE, Id, ServiceOptions).

-spec execute_workflow(id(), workflow_execution_spec()) -> ok.
execute_workflow(EngineId, ExecutionSpec) ->
    ExecutionId = maps:get(id, ExecutionSpec),
    Handler = maps:get(workflow_handler, ExecutionSpec),
    Context = maps:get(execution_context, ExecutionSpec, undefined),
    FirstLaneId = maps:get(first_lane_id, ExecutionSpec, undefined),
    NextLaneId = maps:get(next_lane_id, ExecutionSpec, undefined),
    SnapshotMode = maps:get(snapshot_mode, ExecutionSpec, ?ALL_ITEMS),

    InitAns = case ExecutionSpec of
        #{force_clean_execution := true} -> 
            workflow_execution_state:init(ExecutionId, EngineId, Handler, Context, FirstLaneId, NextLaneId, SnapshotMode);
        _ ->
            workflow_execution_state:resume_from_snapshot(
                ExecutionId, EngineId, Handler, Context, FirstLaneId, NextLaneId, SnapshotMode)
    end,

    case InitAns of
        ok ->
            workflow_engine_state:add_execution_id(EngineId, ExecutionId),
            trigger_job_scheduling(EngineId, ?TAKE_UP_FREE_SLOTS);
        ?WF_ERROR_PREPARATION_FAILED ->
            InterruptReason = execute_exception_handler(ExecutionId, Context, Handler, error, preparation_failed, []),
            case call_handler(ExecutionId, Context, Handler, handle_workflow_abruptly_stopped, [InterruptReason]) of
                clean_progress -> cleanup_execution(ExecutionId);
                _ -> ok
            end
    end.


-spec cleanup_execution(execution_id()) -> ok.
cleanup_execution(ExecutionId) ->
    workflow_execution_state_dump:delete(ExecutionId),
    workflow_iterator_snapshot:cleanup(ExecutionId).


-spec init_cancel_procedure(execution_id()) -> ok.
init_cancel_procedure(ExecutionId) ->
    workflow_execution_state:init_cancel(ExecutionId, #{}).


-spec init_cancel_procedure(execution_id(), cancel_pred()) -> ok | ?WF_ERROR_PRED_NOT_MEET.
init_cancel_procedure(ExecutionId, Pred) ->
    workflow_execution_state:init_cancel(ExecutionId, Pred).


%%--------------------------------------------------------------------
%% @doc
%% Function used to wait until all pending callbacks are finished for execution.
%% To be used together with init_cancel_procedure to prevent races during cancel procedure.
%% Warning: Cannot be called from the inside of callback as it results in deadlock.
%% @end
%%--------------------------------------------------------------------
-spec wait_for_pending_callbacks(execution_id()) -> ok.
wait_for_pending_callbacks(ExecutionId) ->
    workflow_execution_state:wait_for_pending_callbacks(ExecutionId).

-spec finish_cancel_procedure(execution_id()) -> ok.
finish_cancel_procedure(ExecutionId) ->
    case workflow_execution_state:finish_cancel(ExecutionId) of
        {ok, EngineId} ->
            trigger_job_scheduling(EngineId);
        ?WF_ERROR_CANCEL_NOT_INITIALIZED ->
            ?warning("Finishing not initialized cancel procedure for execution ~s", [ExecutionId]),
            ok
    end.


-spec abandon(execution_id(), workflow_handler:abrupt_stop_reason()) -> ok.
abandon(ExecutionId, InterruptReason) ->
    workflow_execution_state:abandon(ExecutionId, InterruptReason).


-spec report_async_task_result(execution_id(), workflow_jobs:encoded_job_identifier(), processing_result()) -> ok.
report_async_task_result(ExecutionId, EncodedJobIdentifier, Result) ->
    JobIdentifier = workflow_jobs:decode_job_identifier(EncodedJobIdentifier),
    report_execution_status_update(ExecutionId, ?ASYNC_CALL_ENDED, JobIdentifier, Result).


-spec report_async_task_heartbeat(execution_id(), workflow_jobs:encoded_job_identifier()) -> ok.
report_async_task_heartbeat(ExecutionId, EncodedJobIdentifier) ->
    JobIdentifier = workflow_jobs:decode_job_identifier(EncodedJobIdentifier),
    workflow_timeout_monitor:report_heartbeat(ExecutionId, JobIdentifier).


-spec stream_task_data(workflow_engine:execution_id(), workflow_engine:task_id(), streamed_task_data()) -> ok.
stream_task_data(ExecutionId, TaskId, TaskData) ->
    {ok, EngineId} = workflow_execution_state:report_new_streamed_task_data(ExecutionId, TaskId, TaskData),
    trigger_job_scheduling(EngineId).


-spec report_task_data_streaming_concluded(workflow_engine:execution_id(), workflow_engine:task_id(),
    stream_closing_result()) -> ok.
report_task_data_streaming_concluded(ExecutionId, TaskId, Result) ->
    {ok, EngineId} = workflow_execution_state:mark_all_streamed_task_data_received(ExecutionId, TaskId, Result),
    trigger_job_scheduling(EngineId).


%%%===================================================================
%%% Framework internal API - to be called only by other workflow_engine modules
%%%===================================================================

-spec report_execution_status_update(execution_id(), processing_stage(),
    workflow_jobs:job_identifier(), processing_result()) -> ok.
report_execution_status_update(ExecutionId, ReportType, JobIdentifier, Ans) ->
    StatusUpdateAns = case Ans of
        pause_job -> workflow_execution_state:pause_job_after_cancel(ExecutionId, JobIdentifier);
        _ -> workflow_execution_state:report_execution_status_update(ExecutionId, JobIdentifier, ReportType, Ans)
    end,

    case StatusUpdateAns of
        {ok, EngineId, TaskSpec} ->
            DecrementSlotsUsage = case {ReportType, Ans} of
                {?ASYNC_CALL_ENDED, _} -> true;
                {?ASYNC_CALL_STARTED, error} -> true;
                {?ASYNC_CALL_STARTED, pause_job} -> true;
                _ -> false
            end,

            case DecrementSlotsUsage of
                true ->
                    % TODO VFS-7788 - support multiple pools
                    case get_async_call_pools(TaskSpec) of
                        [CallPoolId] -> workflow_async_call_pool:decrement_slot_usage(CallPoolId);
                        _ -> ok
                    end;
                _ ->
                    ok
            end,

            case ReportType of
                ?ASYNC_CALL_ENDED ->
                    % Asynchronous job finish - it has no slot acquired
                    trigger_job_scheduling(EngineId, ?TAKE_UP_FREE_SLOTS);
                _ ->
                    trigger_job_scheduling(EngineId, ?FOR_CURRENT_SLOT_FIRST)
            end;
        ?WF_ERROR_WRONG_EXECUTION_STATUS ->
            report_execution_status_update(ExecutionId, ReportType, JobIdentifier, error);
        ?WF_ERROR_JOB_NOT_FOUND ->
            ok
    end.

-spec get_async_call_pools(task_spec()) -> [workflow_async_call_pool:id()].
get_async_call_pools(TaskSpec) ->
    maps:get(async_call_pools, TaskSpec, [?DEFAULT_ASYNC_CALL_POOL_ID]).

-spec trigger_job_scheduling(id()) -> ok.
trigger_job_scheduling(EngineId) ->
    trigger_job_scheduling(EngineId, ?TAKE_UP_FREE_SLOTS).

-spec call_handler(execution_id(), execution_context(), workflow_handler:handler(), handler_function(), handler_args()) ->
    handler_result() | error.
call_handler(ExecutionId, Context, Handler, Function, Args) ->
    try
        apply(Handler, Function, [ExecutionId, Context] ++ Args)
    catch
        Error:Reason:Stacktrace  ->
            handle_exception(
                ExecutionId, Handler, Context,
                "~s", [?autoformat([Function, Args])],
                Error, Reason, Stacktrace
            ),
            error
    end.

-spec call_handle_task_execution_stopped_for_all_tasks(
    execution_id(),
    workflow_handler:handler(),
    execution_context(),
    [task_id()]
) -> ok.
call_handle_task_execution_stopped_for_all_tasks(ExecutionId, Handler, Context, TaskIds) ->
    lists:foreach(fun(TaskId) ->
        call_handler(ExecutionId, Context, Handler, handle_task_execution_stopped, [TaskId])
    end, TaskIds).

-spec call_handle_task_results_processed_for_all_items_for_all_tasks(
    execution_id(),
    workflow_handler:handler(),
    execution_context(),
    [task_id()]
) -> ok.
call_handle_task_results_processed_for_all_items_for_all_tasks(ExecutionId, Handler, Context, TaskIds) ->
    lists:foreach(fun(TaskId) ->
        call_handler(ExecutionId, Context, Handler, handle_task_results_processed_for_all_items, [TaskId])
    end, TaskIds).

-spec call_handlers_for_cancelled_lane(
    execution_id(),
    workflow_handler:handler(),
    execution_context(),
    lane_id(),
    [task_id()]
) -> ok.
call_handlers_for_cancelled_lane(ExecutionId, Handler, Context, LaneId, TaskIds) ->
    call_handle_task_execution_stopped_for_all_tasks(ExecutionId, Handler, Context, TaskIds),

    case call_handler(ExecutionId, Context, Handler, handle_lane_execution_stopped, [LaneId]) of
        ?END_EXECUTION ->
            ok;
        Other ->
            ?error("Wrong return of handle_lane_execution_stopped for cancelled lane ~p of execution ~p: ~p",
                [LaneId, ExecutionId, Other])
    end.

-spec handle_exception(execution_id(), workflow_handler:handler(), execution_context(),
    string(), list(), throw | error | exit, term(), list()) -> ok.
handle_exception(ExecutionId, Handler, Context, Message, MessageArgs, Class, Reason, Stacktrace) ->
    try
        ?error_exception(
            "workflow_handler ~w, execution ~s: " ++ Message,
            [Handler, ExecutionId | MessageArgs],
            Class, Reason, Stacktrace
        ),
        workflow_execution_state:handle_exception(ExecutionId, Context, Class, Reason, Stacktrace)
    catch
        Class2:Reason2:Stacktrace2  ->
            ?critical_exception(
                "Unexpected error handling exception for workflow_handler ~w (execution ~s)", [Handler, ExecutionId],
                Class2, Reason2, Stacktrace2
            )
    end.

-spec execute_exception_handler(execution_id(), execution_context(), workflow_handler:handler(),
    throw | error | exit, term(), list()) -> workflow_handler:abrupt_stop_reason() | undefined.
execute_exception_handler(ExecutionId, Context, Handler, Class, Reason, Stacktrace) ->
    try
        apply(Handler, handle_exception, [ExecutionId, Context, Class, Reason, Stacktrace])
    catch
        Class2:Reason2:Stacktrace2  ->
            ?critical_exception(
                "Unexpected error handling exception for workflow_handler ~w (execution ~s)", [Handler, ExecutionId],
                Class2, Reason2, Stacktrace2
            ),
            undefined
    end.


-spec get_enqueuing_timeout(id()) -> time:seconds() | infinity | undefined.
get_enqueuing_timeout(EngineId) ->
    node_cache:get({enqueuing_timeout, EngineId}, undefined).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec init_service(id(), options()) -> ok.
init_service(Id, Options) ->
    SlotsLimit = maps:get(slots_limit, Options, ?DEFAULT_SLOT_COUNT),
    init_pool(Id, SlotsLimit),
    case workflow_engine_state:init(Id, SlotsLimit) of
        ok ->
            case maps:get(default_keepalive_timeout, Options, undefined) of
                undefined -> ok;
                KeepaliveTimeout -> set_default_keepalive_timeout(Id, KeepaliveTimeout)
            end,

            set_enqueuing_timeout(Id, maps:get(enqueuing_timeout, Options, undefined)),

            AsyncCallPools = maps:get(workflow_async_call_pools_to_use, Options,
                [{?DEFAULT_ASYNC_CALL_POOL_ID, ?DEFAULT_CALLS_LIMIT}]),
            lists:foreach(fun({AsyncCallPoolId, AsyncCallPoolSlotsLimit}) ->
                workflow_async_call_pool:init(AsyncCallPoolId, AsyncCallPoolSlotsLimit)
            end, AsyncCallPools),

            case maps:get(init_workflow_timeout_server, Options, ?USE_TIMEOUT_SERVER_DEFAULT) of
                {true, CheckPeriod} -> workflow_timeout_monitor:init(Id, CheckPeriod);
                false -> ok
            end;
        ?ERROR_ALREADY_EXISTS ->
            ok
    end.

-spec takeover_service(id(), options(), node()) -> ok.
takeover_service(_EngineId, _Options, _Node) ->
    % TODO VFS-7788 Restart tasks
    ok.

-spec init_pool(id(), non_neg_integer()) -> ok.
init_pool(EngineId, SlotsLimit) ->
    % TODO VFS-7788 handle params such as ParallelLanesLimit, ParallelSyncItems, ParallelAsyncItems, ParallelReports
    try
        {ok, _} = worker_pool:start_sup_pool(?POOL_ID(EngineId), [{workers, SlotsLimit}]),
        ok
    catch
        error:{badmatch, {error, {already_started, _}}} ->
            throw({error, already_exists})
    end.

-spec trigger_job_scheduling(id(), ?TAKE_UP_FREE_SLOTS | ?FOR_CURRENT_SLOT_FIRST) -> ok.
trigger_job_scheduling(EngineId, ?TAKE_UP_FREE_SLOTS) ->
    case workflow_engine_state:increment_slot_usage(EngineId) of
        ok ->
            case trigger_job_scheduling_for_acquired_slot(EngineId) of
                ok -> trigger_job_scheduling(EngineId, ?TAKE_UP_FREE_SLOTS);
                ?WF_ERROR_NOTHING_TO_START -> ok
            end;
        ?WF_ERROR_ALL_SLOTS_USED ->
            ok
    end;
trigger_job_scheduling(EngineId, ?FOR_CURRENT_SLOT_FIRST) ->
    case trigger_job_scheduling_for_acquired_slot(EngineId) of
        ok ->
            trigger_job_scheduling(EngineId, ?TAKE_UP_FREE_SLOTS);
        ?WF_ERROR_NOTHING_TO_START ->
            ok
    end.

-spec trigger_job_scheduling_for_acquired_slot(id()) -> ok | ?WF_ERROR_NOTHING_TO_START.
trigger_job_scheduling_for_acquired_slot(EngineId) ->
    case schedule_next_job(EngineId) of
        ok ->
            ok;
        ?WF_ERROR_NOTHING_TO_START ->
            workflow_engine_state:decrement_slot_usage(EngineId),
            ?WF_ERROR_NOTHING_TO_START
    end.

-spec schedule_next_job(id()) -> ok | ?WF_ERROR_NOTHING_TO_START.
schedule_next_job(EngineId) ->
    try
        schedule_next_job_insecure(EngineId, [])
    catch
        Error:Reason:Stacktrace  ->
            ?error_stacktrace(
                "Unexpected error scheduling next job for engine ~s~nError was: ~w:~p",
                [EngineId, Error, Reason],
                Stacktrace
            ),
            ?WF_ERROR_NOTHING_TO_START
    end.

-spec schedule_next_job_insecure(id(), [execution_id()]) -> ok | ?WF_ERROR_NOTHING_TO_START.
schedule_next_job_insecure(EngineId, DeferredExecutions) ->
    case workflow_engine_state:poll_next_execution_id(EngineId) of
        {ok, ExecutionId} ->
            case lists:member(ExecutionId, DeferredExecutions) of
                false ->
                    case workflow_execution_state:prepare_next_job(ExecutionId) of
                        {ok, ExecutionSpec} ->
                            case schedule_on_pool(EngineId, ExecutionId, ExecutionSpec) of
                                ok ->
                                    ok;
                                ?WF_ERROR_LIMIT_REACHED ->
                                    schedule_next_job_insecure(EngineId, [ExecutionId | DeferredExecutions])
                            end;
                        ?PREPARE_LANE_EXECUTION(Handler, ExecutionContext, LaneId, PreparationMode, InitType) ->
                            schedule_lane_prepare_on_pool(
                                EngineId, ExecutionId, Handler, ExecutionContext, LaneId, PreparationMode, InitType);
                        #execution_ended{} = ExecutionEndedRecord ->
                            handle_execution_ended(EngineId, ExecutionId, ExecutionEndedRecord),
                            schedule_next_job_insecure(EngineId, DeferredExecutions);
                        ?DEFER_EXECUTION ->
                            % no jobs can be currently scheduled for this execution but new jobs will appear in future
                            schedule_next_job_insecure(EngineId, [ExecutionId | DeferredExecutions]);
                        ?RETRY_EXECUTION ->
                            schedule_next_job_insecure(EngineId, DeferredExecutions);
                        ?ERROR_NOT_FOUND ->
                            % Race with execution deletion
                            schedule_next_job_insecure(EngineId, [ExecutionId | DeferredExecutions])
                    end;
                true ->
                    % no jobs can be currently scheduled for any execution (all executions has been checked and
                    % added to DeferredExecutions) but new jobs will appear in future
                    ?WF_ERROR_NOTHING_TO_START
            end;
        ?ERROR_NOT_FOUND ->
            ?WF_ERROR_NOTHING_TO_START
    end.

-spec handle_execution_ended(id(), execution_id(), execution_ended_info()) -> ok.
handle_execution_ended(EngineId, ExecutionId, #execution_ended{
    handler = Handler,
    context = Context,
    final_callback = FinalCallback,
    lane_callbacks = LaneCallbacks
}) ->
    case workflow_engine_state:remove_execution_id(EngineId, ExecutionId) of
        ok ->
            case LaneCallbacks of
                {true, CancelledLaneId, CancelledLaneContext, TaskIds} ->
                    % TODO VFS-9993 - test if exception handler is called when exception appears here
                    call_handlers_for_cancelled_lane(
                        ExecutionId, Handler, CancelledLaneContext, CancelledLaneId, TaskIds);
                false ->
                    ok
            end,

            execute_final_callback(ExecutionId, Context, Handler, FinalCallback),
            workflow_execution_state:cleanup(ExecutionId);
        ?WF_ERROR_ALREADY_REMOVED ->
            ok
    end.


-spec execute_final_callback(id(), execution_id(), workflow_handler:handler(),
    handle_workflow_execution_stopped | {handle_workflow_abruptly_stopped, workflow_handler:abrupt_stop_reason()}) -> ok.
execute_final_callback(ExecutionId, Context, Handler, FinalCallback) ->
    case {FinalCallback, workflow_execution_state:execute_exception_handler_if_waiting(ExecutionId)} of
        {handle_workflow_execution_stopped, {executed, Reason}} ->
            execute_final_callback(ExecutionId, Context, Handler, {handle_workflow_abruptly_stopped, Reason});
        _ ->
            {FunName, Args} = case FinalCallback of
                handle_workflow_execution_stopped -> {handle_workflow_execution_stopped, []};
                {handle_workflow_abruptly_stopped, Reason} -> {handle_workflow_abruptly_stopped, [Reason]}
            end,
            case call_handler(ExecutionId, Context, Handler, FunName, Args) of
                clean_progress ->
                    workflow_iterator_snapshot:cleanup(ExecutionId);
                save_progress ->
                    workflow_execution_state_dump:dump_workflow_execution_state(ExecutionId);
                save_iterator ->
                    ok; % Iterator is already persisted - simply do not clean it
                error ->
                    case {FinalCallback, workflow_execution_state:execute_exception_handler_if_waiting(ExecutionId)} of
                        {handle_workflow_execution_stopped, {executed, NewReason}} ->
                            execute_final_callback(ExecutionId, Context, Handler, {handle_workflow_abruptly_stopped, NewReason});
                        _ ->
                            ok
                    end
            end
    end.


-spec schedule_on_pool(
    id(),
    execution_id(),
    execution_spec()
) -> ok | ?WF_ERROR_LIMIT_REACHED.
schedule_on_pool(EngineId, ExecutionId, #execution_spec{
    job_identifier = streamed_task_data % stream data processing
} = ExecutionSpec) ->
    CallArgs = {?MODULE, process_streamed_task_data, [EngineId, ExecutionId, ExecutionSpec]},
    ok = worker_pool:cast(?POOL_ID(EngineId), CallArgs);
schedule_on_pool(EngineId, ExecutionId, #execution_spec{
    task_spec = TaskSpec,
    job_identifier = JobIdentifier
} = ExecutionSpec) ->
    CallArgs = {?MODULE, process_job_or_result, [EngineId, ExecutionId, ExecutionSpec]},
    TaskType = maps:get(type, TaskSpec),
    CallPools = get_async_call_pools(TaskSpec),
    ProcessingType = workflow_jobs:get_processing_type(JobIdentifier),
    case {TaskType, CallPools, ProcessingType} of
        {async, [CallPoolId], ?JOB_PROCESSING} -> % TODO VFS-7788 - support multiple pools
            case workflow_async_call_pool:increment_slot_usage(CallPoolId) of
                ok ->
                    ok = worker_pool:cast(?POOL_ID(EngineId), CallArgs);
                ?WF_ERROR_LIMIT_REACHED ->
                    % TODO VFS-7787 - handle case when other tasks can be started (limit of task, not task execution engine is reached)
                    workflow_execution_state:pause_job(ExecutionId, JobIdentifier),
                    ?WF_ERROR_LIMIT_REACHED
            end;
        _ ->
            ok = worker_pool:cast(?POOL_ID(EngineId), CallArgs)
    end.

-spec schedule_lane_prepare_on_pool(
    id(),
    execution_id(),
    workflow_handler:handler(),
    execution_context(),
    lane_id(),
    preparation_mode(),
    workflow_execution_state:init_type()
) -> ok.
schedule_lane_prepare_on_pool(EngineId, ExecutionId, Handler, ExecutionContext, LaneId, PreparationMode, InitType) ->
    CallArgs = {?MODULE, prepare_lane, [EngineId, ExecutionId, Handler, ExecutionContext, LaneId, PreparationMode, InitType]},
    ok = worker_pool:cast(?POOL_ID(EngineId), CallArgs).

-spec get_default_keepalive_timeout(id()) -> time:seconds().
get_default_keepalive_timeout(EngineId) ->
    node_cache:get({default_keepalive_timeout, EngineId}, ?DEFAULT_KEEPALIVE_TIMEOUT_SEC).

-spec set_default_keepalive_timeout(id(), time:seconds()) -> ok.
set_default_keepalive_timeout(Id, Timeout) ->
    put_value_in_cache(Id, default_keepalive_timeout, Timeout).

-spec set_enqueuing_timeout(id(), time:seconds() | infinity | undefined) -> ok.
set_enqueuing_timeout(Id, Timeout) ->
    put_value_in_cache(Id, enqueuing_timeout, Timeout).

-spec put_value_in_cache(id(), atom(), term()) -> ok.
put_value_in_cache(Id, Name, Value) ->
    Nodes = consistent_hashing:get_all_nodes(),
    {Res, BadNodes} = utils:rpc_multicall(Nodes, node_cache, put, [{Name, Id}, Value]),

    case BadNodes of
        [] ->
            ok;
        _ ->
            ?error("Engine ~p: setting value for ~p failed on nodes: ~p (RPC error)", [Id, Name, BadNodes])
    end,

    lists:foreach(fun
        (ok) -> ok;
        ({badrpc, _} = Error) ->
            ?error(
                "Engine ~p: setting value for ~p failed.~n"
                "Reason: ~p", [Id, Name, Error]
            )
    end, Res).


%%%===================================================================
%%% Function executed on pool
%%%===================================================================

-spec process_job_or_result(id(), execution_id(), execution_spec()) -> ok.
process_job_or_result(EngineId, ExecutionId, ExecutionSpec = #execution_spec{
    job_identifier = JobIdentifier
}) ->
    case workflow_jobs:get_processing_type(JobIdentifier) of
        ?JOB_PROCESSING -> process_item(EngineId, ExecutionId, ExecutionSpec);
        ?ASYNC_RESULT_PROCESSING -> process_result(EngineId, ExecutionId, ExecutionSpec)
    end.

-spec process_item(id(), execution_id(), execution_spec()) -> ok.
process_item(EngineId, ExecutionId, ExecutionSpec = #execution_spec{
    handler = Handler,
    context = ExecutionContext,
    task_id = TaskId,
    task_spec = TaskSpec,
    subject_id = ItemId,
    job_identifier = JobIdentifier
}) ->
    try
        #{type := TaskType} = TaskSpec,
        {ReportType, FinalAns} = case TaskType of
            sync ->
                {?SYNC_CALL, process_item(ExecutionId, ExecutionSpec)};
            async ->
                case process_item(ExecutionId, ExecutionSpec) of
                    ok ->
                        DefaultTimeout = get_default_keepalive_timeout(EngineId),
                        AnsWithTimeout = {ok, maps:get(keepalive_timeout, TaskSpec, DefaultTimeout)},
                        {?ASYNC_CALL_STARTED, AnsWithTimeout};
                    Other ->
                        {?ASYNC_CALL_STARTED, Other}
                end
        end,

        report_execution_status_update(ExecutionId, ReportType, JobIdentifier, FinalAns)
    catch
        Error:Reason:Stacktrace  ->
            handle_exception(
                ExecutionId, Handler, ExecutionContext,
                "Unexpected error handling task ~s", [?autoformat([TaskId, ItemId])],
                Error, Reason, Stacktrace
            ),
            trigger_job_scheduling(EngineId, ?FOR_CURRENT_SLOT_FIRST)
    end.

-spec process_item(execution_id(), execution_spec()) -> workflow_handler:handler_execution_result().
process_item(ExecutionId, #execution_spec{
    handler = Handler,
    context = ExecutionContext,
    task_id = TaskId,
    job_identifier = JobIdentifier,
    subject_id = ItemId
}) ->
    Item = workflow_cached_item:get_item(ItemId),
    EncodedJobIdentifier = workflow_jobs:encode_job_identifier(JobIdentifier),
    try
        case Handler:run_task_for_item(ExecutionId, ExecutionContext, TaskId, EncodedJobIdentifier, Item) of
            {error, running_item_failed} -> error;
            {error, task_already_stopping} -> pause_job;
            {error, task_already_stopped} -> pause_job;
            Other -> Other
        end
    catch
        Error:Reason:Stacktrace  ->
            % TODO VFS-7788 - use callbacks to get human readable information about item and task
            handle_exception(
                ExecutionId, Handler, ExecutionContext,
                "Unexpected error handling task ~s", [?autoformat([TaskId, ItemId, Item])],
                Error, Reason, Stacktrace
            ),
            error
    end.

-spec process_result(id(), execution_id(), execution_spec()) -> ok.
process_result(EngineId, ExecutionId, #execution_spec{
    handler = Handler,
    context = ExecutionContext,
    task_id = TaskId,
    subject_id = CachedResultId,
    job_identifier = JobIdentifier
}) ->
    try
        ItemId = workflow_execution_state:get_item_id(ExecutionId, JobIdentifier),
        CachedItem = workflow_cached_item:get_item(ItemId),
        CachedResult = workflow_cached_async_result:take(CachedResultId),

        ProcessedResult = try
            Handler:process_task_result_for_item(ExecutionId, ExecutionContext, TaskId, CachedItem, CachedResult)
        catch
            Error:Reason:Stacktrace  ->
                % TODO VFS-7788 - use callbacks to get human readable information about task
                handle_exception(
                    ExecutionId, Handler, ExecutionContext,
                    "Unexpected error processing task result ~s",
                    [?autoformat([TaskId, CachedResultId, CachedResult, ItemId, CachedItem])],
                    Error, Reason, Stacktrace
                ),
                error
        end,
        report_execution_status_update(ExecutionId, ?ASYNC_RESULT_PROCESSED, JobIdentifier, ProcessedResult)
    catch
        Error2:Reason2:Stacktrace2  ->
            handle_exception(
                ExecutionId, Handler, ExecutionContext,
                "Unexpected error getting item or result to process task result ~s",
                [?autoformat([TaskId, CachedResultId])],
                Error2, Reason2, Stacktrace2
            ),
            trigger_job_scheduling(EngineId, ?FOR_CURRENT_SLOT_FIRST)
    end.

-spec process_streamed_task_data(id(), execution_id(), execution_spec()) -> ok.
process_streamed_task_data(EngineId, ExecutionId, #execution_spec{
    handler = Handler,
    context = ExecutionContext,
    task_id = TaskId,
    subject_id = CachedTaskDataId
}) ->
    try
        Data = workflow_cached_task_data:take(CachedTaskDataId),

        try
            Ans = call_handler(ExecutionId, ExecutionContext, Handler, process_streamed_task_data, [TaskId, Data]),
            workflow_execution_state:report_streamed_task_data_processed(ExecutionId, TaskId, CachedTaskDataId, Ans),
            trigger_job_scheduling(EngineId, ?FOR_CURRENT_SLOT_FIRST)
        catch
            Error:Reason:Stacktrace  ->
                handle_exception(
                    ExecutionId, Handler, ExecutionContext,
                    "Unexpected error processing task data ~s", [?autoformat([TaskId, CachedTaskDataId])],
                    Error, Reason, Stacktrace
                ),
                trigger_job_scheduling(EngineId, ?FOR_CURRENT_SLOT_FIRST)
        end
    catch
        Error2:Reason2:Stacktrace2  ->
            handle_exception(
                ExecutionId, Handler, ExecutionContext,
                "Unexpected error getting data for task ~s", [?autoformat([CachedTaskDataId, TaskId])],
                Error2, Reason2, Stacktrace2
            ),
            trigger_job_scheduling(EngineId, ?FOR_CURRENT_SLOT_FIRST)
    end.

-spec prepare_lane(
    id(),
    execution_id(),
    workflow_handler:handler(),
    execution_context(),
    lane_id(),
    preparation_mode(),
    workflow_execution_state:init_type()
) -> ok.
prepare_lane(EngineId, ExecutionId, Handler, ExecutionContext, LaneId, PreparationMode, InitType) ->
    try
        Callback = case InitType of
            prepare -> prepare_lane;
            ?RESUMING(_, _, _) -> resume_lane
        end,
        Ans = call_handler(ExecutionId, ExecutionContext, Handler, Callback, [LaneId]),
        workflow_execution_state:report_lane_execution_prepared(Handler, ExecutionId, LaneId, PreparationMode, InitType, Ans),
        trigger_job_scheduling(EngineId, ?FOR_CURRENT_SLOT_FIRST)
    catch
        Error:Reason:Stacktrace  ->
            handle_exception(
                ExecutionId, Handler, ExecutionContext,
                "Unexpected error preparing lane ~p", [LaneId],
                Error, Reason, Stacktrace
            ),
            trigger_job_scheduling(EngineId, ?FOR_CURRENT_SLOT_FIRST)
    end.