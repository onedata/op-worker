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
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_engine).
-author("Michal Wrzeszcz").

-include("workflow_engine.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([init/1, init/2, execute_workflow/2, report_execution_status_update/6]).

%% Functions exported for internal_services engine - do not call directly
-export([init_service/2, takeover_service/3]).

%% Function executed by wpool - do not call directly
-export([process_item/3, prepare_execution/4]).

-type id() :: binary(). % Id of an engine
-type execution_id() :: binary().
-type execution_context() :: term().
-type task_id() :: binary().
-type job_execution_spec() :: #job_execution_spec{}.
-type processing_stage() :: ?SYNC_CALL | ?ASYNC_CALL_STARTED | ?ASYNC_CALL_FINISHED.
-type processing_result() :: workflow_handler:callback_execution_result() | {ok, KeepaliveTimeout :: time:seconds()}.

%% @formatter:off
-type options() :: #{
    slots_limit => non_neg_integer(),
    workflow_async_call_pools_to_use => [{workflow_async_call_pool:id(), SlotsLimit :: non_neg_integer()}],
    init_workflow_timeout_server => boolean()
}.

-type execution_spec() :: #{
    id := id(),
    workflow_handler := workflow_handler:handler(),
    execution_context => execution_context(),
    force_clean_execution => boolean()
}.

-type task_type() :: sync | async.
-type task_spec() :: #{
    type := task_type(),
    async_call_pools => [workflow_async_call_pool:id()] | undefined,
    keepalive_timeout => time:seconds()
}.
-type parallel_box_spec() :: #{task_id() => task_spec()}.
-type lane_spec() :: #{
    parallel_boxes := [parallel_box_spec()],
    iterator := iterator:iterator(),
    is_last => boolean()
}.
%% @formatter:on

-export_type([id/0, execution_id/0, execution_context/0, task_id/0,
    job_execution_spec/0, processing_stage/0, processing_result/0,
    task_spec/0, parallel_box_spec/0, lane_spec/0]).

-define(POOL_ID(EngineId), binary_to_atom(EngineId, utf8)).
-define(DEFAULT_SLOT_COUNT, 20).
-define(DEFAULT_CALLS_LIMIT, 1000).
-define(USE_TIMEOUT_SERVER_DEFAULT, true).
-define(DEFAULT_KEEPALIVE_TIMEOUT_SEC, 300).

-define(WF_ERROR_NOTHING_TO_START, {error, nothing_to_start}).
-define(WF_ERROR_ALL_DEFERRED, {error, all_deferred}).

% Job triggering modes (see function trigger_job_scheduling/2)
-define(TAKE_UP_FREE_SLOTS, take_up_free_slots).
-define(FOR_CURRENT_SLOT, for_current_slot).

%%%===================================================================
%%% API
%%%===================================================================

-spec init(id()) -> ok.
init(Id) ->
    init(Id, #{}).

-spec init(id(), options()) -> ok.
init(Id, Options) ->
    % TODO VFS-7551 Implement internal_service HA callbacks
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

-spec execute_workflow(id(), execution_spec()) -> ok.
execute_workflow(EngineId, ExecutionSpec) ->
    ExecutionId = maps:get(id, ExecutionSpec),
    Handler = maps:get(workflow_handler, ExecutionSpec),
    Context = maps:get(execution_context, ExecutionSpec, undefined),

    case ExecutionSpec of
        #{force_clean_execution := true} -> workflow_execution_state:init(ExecutionId, Handler, Context);
        _ -> workflow_execution_state:init_using_snapshot(ExecutionId, Handler, Context)
    end,

    workflow_engine_state:add_execution_id(EngineId, ExecutionId),
    trigger_job_scheduling(EngineId, ?TAKE_UP_FREE_SLOTS),
    ok.

-spec report_execution_status_update(execution_id(), id(), processing_stage(),
    workflow_jobs:job_identifier(), workflow_async_call_pool:id() | undefined, processing_result()) -> ok.
report_execution_status_update(ExecutionId, EngineId, ReportType, JobIdentifier, CallPoolId, Ans) ->
    case CallPoolId of
        undefined ->
            ok;
        _ ->
            workflow_async_call_pool:decrement_slot_usage(CallPoolId)
    end,

    workflow_execution_state:report_execution_status_update(ExecutionId, JobIdentifier, ReportType, Ans),

    case ReportType of
        ?ASYNC_CALL_FINISHED ->
            % Asynchronous job finish - it has no slot acquired
            trigger_job_scheduling(EngineId, ?TAKE_UP_FREE_SLOTS);
        _ ->
            case trigger_job_scheduling(EngineId, ?FOR_CURRENT_SLOT) of
                ok -> ok;
                ?WF_ERROR_NOTHING_TO_START -> ok
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec init_service(id(), options()) -> ok.
init_service(Id, Options) ->
    SlotsLimit = maps:get(slots_limit, Options, ?DEFAULT_SLOT_COUNT),
    init_pool(Id, SlotsLimit),
    case workflow_engine_state:init(Id, SlotsLimit) of
        ok ->
            AsyncCallPools = maps:get(workflow_async_call_pools_to_use, Options,
                [{?DEFAULT_ASYNC_CALL_POOL_ID, ?DEFAULT_CALLS_LIMIT}]),
            lists:foreach(fun({AsyncCallPoolId, AsyncCallPoolSlotsLimit}) ->
                workflow_async_call_pool:init(AsyncCallPoolId, AsyncCallPoolSlotsLimit)
            end, AsyncCallPools),

            case maps:get(init_workflow_timeout_server, Options, ?USE_TIMEOUT_SERVER_DEFAULT) of
                true -> workflow_timeout_monitor:init(Id);
                false -> ok
            end;
        ?ERROR_ALREADY_EXISTS ->
            ok
    end.

-spec takeover_service(id(), options(), node()) -> ok.
takeover_service(_EngineId, _Options, _Node) ->
    % TODO VFS-7551 Restart tasks
    ok.

-spec init_pool(id(), non_neg_integer()) -> ok.
init_pool(EngineId, SlotsLimit) ->
    % TODO VFS-7551 handle params such as ParallelLanesLimit, ParallelSyncItems, ParallelAsyncItems, ParallelReports
    try
        {ok, _} = worker_pool:start_sup_pool(?POOL_ID(EngineId), [{workers, SlotsLimit}]),
        ok
    catch
        error:{badmatch, {error, {already_started, _}}} ->
            throw({error, already_exists})
    end.

-spec trigger_job_scheduling(id(), ?TAKE_UP_FREE_SLOTS | ?FOR_CURRENT_SLOT) -> ok | ?WF_ERROR_NOTHING_TO_START.
trigger_job_scheduling(EngineId, ?TAKE_UP_FREE_SLOTS) ->
    case workflow_engine_state:increment_slot_usage(EngineId) of
        ok ->
            case trigger_job_scheduling(EngineId, ?FOR_CURRENT_SLOT) of
                ok -> trigger_job_scheduling(EngineId, ?TAKE_UP_FREE_SLOTS);
                ?WF_ERROR_NOTHING_TO_START -> ok
            end;
        ?WF_ERROR_ALL_SLOTS_USED ->
            ok
    end;
trigger_job_scheduling(EngineId, ?FOR_CURRENT_SLOT) ->
    case schedule_next_job(EngineId, []) of
        ok ->
            ok;
        ?WF_ERROR_ALL_DEFERRED ->
            workflow_engine_state:decrement_slot_usage(EngineId),
            % TODO VFS-7551 - check without acquire to break spawning loop
            spawn(fun() ->
                timer:sleep(timer:seconds(5)),
                trigger_job_scheduling(EngineId, ?TAKE_UP_FREE_SLOTS)
            end),
            ?WF_ERROR_NOTHING_TO_START;
        ?WF_ERROR_NOTHING_TO_START ->
            workflow_engine_state:decrement_slot_usage(EngineId),
            ?WF_ERROR_NOTHING_TO_START
    end.

-spec schedule_next_job(id(), [execution_id()]) -> ok | ?WF_ERROR_ALL_DEFERRED | ?WF_ERROR_NOTHING_TO_START.
schedule_next_job(EngineId, DeferredExecutions) ->
    case workflow_engine_state:poll_next_execution_id(EngineId) of
        {ok, ExecutionId} ->
            case lists:member(ExecutionId, DeferredExecutions) of
                false ->
                    case workflow_execution_state:prepare_next_job(ExecutionId) of
                        {ok, JobExecutionSpec} ->
                            case schedule_on_pool(EngineId, ExecutionId, JobExecutionSpec) of
                                ok ->
                                    ok;
                                ?WF_ERROR_LIMIT_REACHED ->
                                    schedule_next_job(EngineId, [ExecutionId | DeferredExecutions])
                            end;
                        ?PREPARE_EXECUTION(Handler, ExecutionContext) ->
                            schedule_prepare_on_pool(EngineId, ExecutionId, Handler, ExecutionContext);
                        ?END_EXECUTION_AND_NOTIFY(Handler, Context, LaneIndex) ->
                            case workflow_engine_state:remove_execution_id(EngineId, ExecutionId) of
                                ok ->
                                    Handler:handle_lane_execution_ended(ExecutionId, Context, LaneIndex),
                                    workflow_iterator_snapshot:mark_exhausted(ExecutionId);
                                ?WF_ERROR_ALREADY_REMOVED ->
                                    ok
                            end,
                            schedule_next_job(EngineId, DeferredExecutions);
                        ?END_EXECUTION ->
                            workflow_engine_state:remove_execution_id(EngineId, ExecutionId),
                            schedule_next_job(EngineId, DeferredExecutions);
                        ?DEFER_EXECUTION ->
                            % no jobs can be currently scheduled for this execution but new jobs will appear in future
                            schedule_next_job(EngineId, [ExecutionId | DeferredExecutions])
                    end;
                true ->
                    % no jobs can be currently scheduled for any execution (all executions has been checked and
                    % added to DeferredExecutions) but new jobs will appear in future
                    ?WF_ERROR_ALL_DEFERRED
            end;
        ?ERROR_NOT_FOUND ->
            ?WF_ERROR_NOTHING_TO_START
    end.

-spec schedule_on_pool(
    id(),
    execution_id(),
    job_execution_spec()
) -> ok | ?WF_ERROR_LIMIT_REACHED.
schedule_on_pool(EngineId, ExecutionId, #job_execution_spec{
    task_spec = TaskSpec,
    job_identifier = JobIdentifier
} = JobExecutionSpec) ->
    CallArgs = {?MODULE, process_item, [EngineId, ExecutionId, JobExecutionSpec]},
    TaskType = maps:get(type, TaskSpec),
    CallPools = maps:get(async_call_pools, TaskSpec, [?DEFAULT_ASYNC_CALL_POOL_ID]),
    case {TaskType, CallPools} of
        {sync, _} ->
            ok = worker_pool:cast(?POOL_ID(EngineId), CallArgs);
        {async, undefined} ->
            ok = worker_pool:cast(?POOL_ID(EngineId), CallArgs);
        {async, [CallPoolId]} -> % TODO VFS-7551 - support multiple pools
            case workflow_async_call_pool:increment_slot_usage(CallPoolId) of
                ok ->
                    ok = worker_pool:cast(?POOL_ID(EngineId), CallArgs);
                ?WF_ERROR_LIMIT_REACHED ->
                    % TODO VFS-7551 - handle case when other tasks can be started (limit of task, not task execution engine is reached)
                    workflow_execution_state:report_limit_reached_error(ExecutionId, JobIdentifier),
                    ?WF_ERROR_LIMIT_REACHED
            end
    end.

-spec schedule_prepare_on_pool(
    id(),
    execution_id(),
    workflow_handler:handler(),
    execution_context()
) -> ok.
schedule_prepare_on_pool(EngineId, ExecutionId, Handler, ExecutionContext) ->
    CallArgs = {?MODULE, prepare_execution, [EngineId, ExecutionId, Handler, ExecutionContext]},
    ok = worker_pool:cast(?POOL_ID(EngineId), CallArgs).


%%%===================================================================
%%% Function executed on pool
%%%===================================================================

-spec process_item(id(), execution_id(), job_execution_spec()) -> ok.
process_item(EngineId, ExecutionId, JobExecutionSpec = #job_execution_spec{
    task_id = TaskId,
    task_spec = TaskSpec,
    item_id = ItemId,
    job_identifier = JobIdentifier
}) ->
    try
        #{type := TaskType} = TaskSpec,
        {ReportType, FinalAns} = case TaskType of
            sync ->
                {?SYNC_CALL, process_item(ExecutionId, JobExecutionSpec, <<>>, <<>>)};
            async ->
                FinishCallback = workflow_engine_callback_handler:prepare_finish_callback_id(
                    ExecutionId, EngineId, JobIdentifier, TaskSpec),
                HeartbeatCallback = workflow_engine_callback_handler:prepare_heartbeat_callback_id(
                    ExecutionId, EngineId, JobIdentifier),

                case process_item(ExecutionId, JobExecutionSpec, FinishCallback, HeartbeatCallback) of
                    ok ->
                        Timeout = {ok, maps:get(keepalive_timeout, TaskSpec, ?DEFAULT_KEEPALIVE_TIMEOUT_SEC)},
                        {?ASYNC_CALL_STARTED, Timeout};
                    Other ->
                        {?ASYNC_CALL_STARTED, Other}
                end
        end,

        report_execution_status_update(ExecutionId, EngineId, ReportType, JobIdentifier, undefined, FinalAns)
    catch
        Error2:Reason2  ->
            ?error_stacktrace("Unexpected error handling task ~p for item id ~p: ~p:~p",
                [TaskId, ItemId, Error2, Reason2])
    end.

-spec process_item(
    execution_id(),
    job_execution_spec(),
    workflow_handler:finished_callback_id(),
    workflow_handler:heartbeat_callback_id()
) -> workflow_handler:callback_execution_result().
process_item(ExecutionId, #job_execution_spec{
    handler = Handler,
    context = ExecutionContext,
    task_id = TaskId,
    item_id = ItemId
}, FinishCallback, HeartbeatCallback) ->
    Item = workflow_cached_item:get(ItemId),
    try
        Handler:process_item(ExecutionId, ExecutionContext, TaskId, Item,
            FinishCallback, HeartbeatCallback)
    catch
        Error:Reason  ->
            % TODO VFS-7551 - use callbacks to get human readable information about item and task
            ?error_stacktrace("Unexpected error handling task ~p for item ~p (id ~p): ~p:~p",
                [TaskId, Item, ItemId, Error, Reason]),
            error
    end.

-spec prepare_execution(
    id(),
    execution_id(),
    workflow_handler:handler(),
    execution_context()
) -> ok.
prepare_execution(EngineId, ExecutionId, Handler, ExecutionContext) ->
    try
        Ans = Handler:prepare(ExecutionId, ExecutionContext),
        workflow_execution_state:report_execution_prepared(ExecutionId, Handler, ExecutionContext, Ans),
        case trigger_job_scheduling(EngineId, ?FOR_CURRENT_SLOT) of
            ok -> ok;
            ?WF_ERROR_NOTHING_TO_START -> ok
        end
    catch
        Error:Reason  ->
            ?error_stacktrace("Unexpected error perparing execution ~p: ~p:~p", [ExecutionId, Error, Reason])
    end.