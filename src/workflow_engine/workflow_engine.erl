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
-export([init/1, init/2, execute_workflow/2,
    report_execution_finish/5]).

%% Functions exported for internal_services engine - do not call directly
-export([init_service/2, restart_ongoing/3]).

%% Function executed by wpool - do not call directly
-export([process_item/5]).

-type id() :: binary(). % Id of an engine
-type execution_id() :: binary().

%% @formatter:off
-type options() :: #{
    slots_limit => non_neg_integer(),
    workflow_async_call_pools_to_use => [{workflow_async_call_pool:id(), SlotsLimit :: non_neg_integer()}],
    init_workflow_timeout_server => boolean()
}.
%% @formatter:on

% TODO describe and properly define types
-type task_spec() :: map().
-type parallel_box_spec() :: #{TaskId :: binary() => task_spec()}.
-type lane_spec() :: map().

-export_type([id/0, execution_id/0]).
-export_type([task_spec/0, parallel_box_spec/0, lane_spec/0]).

-define(POOL_ID(EngineId), binary_to_atom(EngineId, utf8)).
-define(DEFAULT_SLOT_COUNT, 20).
-define(DEFAULT_CALLS_LIMIT, 1000).
-define(USE_TIMEOUT_SERVER_DEFAULT, true).

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
        takeover_function => restart_ongoing,
        takeover_function_args => [Id, Options, node()],
        migrate_function => undefined,
        migrate_function_args => [],
        stop_function => stop_service,
        stop_function_args => [Id]
    },
    ok = internal_services_manager:start_service(?MODULE, Id, ServiceOptions).

-spec execute_workflow(id(), workflow_definition:execution_spec()) -> ok.
execute_workflow(EngineId, ExecutionSpec) ->
    ExecutionId = maps:get(id, ExecutionSpec),
    LanesCount = maps:get(lanes_count, ExecutionSpec),

    workflow_execution_state:init(ExecutionId, LanesCount),
    workflow_engine_state:add_execution_id(EngineId, ExecutionId),
    trigger_job_scheduling(EngineId, ?TAKE_UP_FREE_SLOTS),
    ok.

-spec report_execution_finish(execution_id(), id(), task_executor:task_id(),
    workflow_jobs:job_identifier() | task_executor:async_ref(), task_executor:result()) -> ok.
report_execution_finish(ExecutionId, EngineId, Task, RefOrJobIdentifier, Ans) ->
    workflow_execution_state:report_execution_finish(ExecutionId, Task, RefOrJobIdentifier, Ans),

    case workflow_jobs:is_async_job(RefOrJobIdentifier) of
        false ->
            case trigger_job_scheduling(EngineId, ?FOR_CURRENT_SLOT) of
                ok -> ok;
                ?WF_ERROR_NOTHING_TO_START -> ok
            end;
        true ->
            % Asynchronous job finish - it has no slot acquired
            trigger_job_scheduling(EngineId, ?TAKE_UP_FREE_SLOTS)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec init_service(id(), options()) -> ok.
init_service(Id, Options) ->
    SlotsLimit = maps:get(slots_limit, Options, ?DEFAULT_SLOT_COUNT),
    init_pool(Id, SlotsLimit),
    workflow_engine_state:init(Id, SlotsLimit),

    AsyncCallPools = maps:get(workflow_async_call_pools_to_use, Options,
        [{?DEFAULT_ASYNC_CALL_POOL_ID, ?DEFAULT_CALLS_LIMIT}]),
    lists:foreach(fun({Id, SlotsLimit}) ->
        workflow_async_call_pool:init(Id, SlotsLimit)
    end, AsyncCallPools),

    case maps:get(init_workflow_timeout_server, Options, ?USE_TIMEOUT_SERVER_DEFAULT) of
        true -> workflow_timeout_monitor:init(Id);
        false -> ok
    end,

    restart_ongoing(Id, Options, node()).

-spec restart_ongoing(id(), options(), node()) -> ok.
restart_ongoing(_EngineId, _Options, _Node) ->
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
                        {ok, TaskId, ItemId, JobIdentifier} ->
                            schedule_on_pool(EngineId, ExecutionId, TaskId, ItemId, JobIdentifier);
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
    task_executor:task_id(),
    workflow_cached_item:id(),
    workflow_jobs:job_identifier()
) -> ok.
schedule_on_pool(EngineId, ExecutionId, TaskId, ItemId, JobIdentifier) ->
    CallArgs = {?MODULE, process_item, [EngineId, ExecutionId, TaskId, ItemId, JobIdentifier]},
    case task_executor:get_calls_counter_id(ExecutionId, TaskId) of
        undefined ->
            ok = worker_pool:cast(?POOL_ID(EngineId), CallArgs);
        TaskEngineId ->
            case workflow_async_call_pool:increment_slot_usage(TaskEngineId) of
                ok ->
                    ok = worker_pool:cast(?POOL_ID(EngineId), CallArgs);
                ?WF_ERROR_LIMIT_REACHED ->
                    % TODO VFS-7551 - handle case when other tasks can be started (limit of task, not task execution engine is reached)
                    workflow_execution_state:report_limit_reached_error(ExecutionId, JobIdentifier)
            end
    end.

%%%===================================================================
%%% Function executed on pool
%%%===================================================================

-spec process_item(
    id(),
    execution_id(),
    task_executor:task_id(),
    workflow_cached_item:id(),
    workflow_jobs:job_identifier()
) -> ok.
process_item(EngineId, ExecutionId, TaskId, ItemId, JobIdentifier) ->
    try
        Item = workflow_cached_item:get(ItemId),
        Ans = try
            task_executor:process_item(EngineId, ExecutionId, TaskId, Item)
        catch
            Error:Reason  ->
                ?error_stacktrace("Unexpected error handling task ~p for item ~p (id ~p): ~p:~p",
                    [task_executor:get_task_description(TaskId), workflow_store:get_item_description(Item),
                        ItemId, Error, Reason]),
                error
        end,
        report_execution_finish(ExecutionId, EngineId, TaskId, JobIdentifier, Ans)
    catch
        Error2:Reason2  ->
            ?error_stacktrace("Unexpected error handling task ~p for item id ~p: ~p:~p",
                [task_executor:get_task_description(TaskId), ItemId, Error2, Reason2])
    end.