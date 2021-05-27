%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for workflow_execution_state processing information
%%% about jobs currently being processed by workflow_engine. It
%%% also processes information about scheduled jobs.
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_jobs).
-author("Michal Wrzeszcz").

-include("workflow_engine.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([init/0, prepare_next_waiting_job/1, populate_with_jobs_for_item/4,
    pause_job/2, mark_ongoing_job_finished/2, register_failure/2,
    ensure_job_identifier_or_cache_ans/3, prepare_next_parallel_box/4]).
-export([register_async_call/5, check_timeouts/1, reset_keepalive_timer/2]).
-export([is_async_job/1, get_item_id/2, get_task_id/2]).

% Internal record used for scheduled jobs management
-record(job_identifier, {
    item_index :: workflow_execution_state:index(),
    parallel_box_index :: workflow_execution_state:index(),
    task_index :: workflow_execution_state:index()
}).

% Internal record used for management of jobs that are processed asynchronously
-record(async_job, {
    job_identifier :: job_identifier(),
    task_id :: task_executor:task_id(), % TODO VFS-7551 - delete field
    keepalive_timer :: countdown_timer:instance(),
    % max allowed time between heartbeats to assume the async process is still alive
    keepalive_timeout :: time:seconds()
}).

% Internal record that describe information about all jobs that are currently
% known to workflow_execution_state. It does not store information about all jobs
% that have appeared - information about job is deleted when it is no longer needed.
-record(workflow_jobs, {
    waiting = gb_sets:empty() :: jobs_set(),
    ongoing = gb_sets:empty() :: jobs_set(),
    % When any item processing fails, item is stored in `failed_items` set
    % to prevent scheduling next parallel box. When all ongoing jobs for this item ends,
    % item is deleted from the set.
    failed_items = sets:new() :: items_set(),

    pending_async_jobs = #{} :: pending_async_jobs(),
    unidentified_async_refs = #{} :: unidentified_async_refs() % TODO VFS-7551 - clean when they are not needed anymore (after integration with BW)
}).

-type job_identifier() :: #job_identifier{}.
-type jobs_set() :: gb_sets:set(job_identifier()).
-type items_set() :: sets:set(workflow_execution_state:index()).
-type async_job() :: #async_job{}.
-type pending_async_jobs() :: #{task_executor:async_ref() => async_job()}.
-type unidentified_async_refs() :: #{task_executor:async_ref() => task_executor:result()}.
-type jobs() :: #workflow_jobs{}.

-export_type([job_identifier/0, jobs/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec init() -> jobs().
init() ->
    #workflow_jobs{}.

-spec prepare_next_waiting_job(jobs()) ->
    {ok, job_identifier(), jobs()} | ?WF_ERROR_ONGOING_ITEMS_ONLY | ?ERROR_NOT_FOUND.
prepare_next_waiting_job(Jobs = #workflow_jobs{
    waiting = Waiting,
    ongoing = Ongoing
}) ->
    case gb_sets:is_empty(Waiting) of
        false ->
            {JobIdentifier, NewWaiting} = gb_sets:take_smallest(Waiting),
            NewOngoing = gb_sets:insert(JobIdentifier, Ongoing),
            {ok, JobIdentifier, Jobs#workflow_jobs{waiting = NewWaiting, ongoing = NewOngoing}};
        true ->
            case gb_sets:is_empty(Ongoing) of
                true -> ?ERROR_NOT_FOUND;
                false -> ?WF_ERROR_ONGOING_ITEMS_ONLY
            end
    end.

-spec populate_with_jobs_for_item(
    jobs(),
    workflow_execution_state:index(),
    workflow_execution_state:index(),
    workflow_definition:boxes_map()
) -> {jobs(), job_identifier()}.
populate_with_jobs_for_item(
    Jobs = #workflow_jobs{
        ongoing = Ongoing,
        waiting = Waiting
    },
    ItemIndex, ParallelBoxToStartIndex, BoxesSpec) ->
    Tasks = maps:get(1, BoxesSpec),
    [ToStart | ToWait] = lists:map(fun(TaskIndex) ->
        #job_identifier{
            item_index = ItemIndex,
            parallel_box_index = ParallelBoxToStartIndex,
            task_index = TaskIndex
        }
    end, lists:seq(1, maps:size(Tasks))),

    {Jobs#workflow_jobs{
        ongoing = gb_sets:insert(ToStart, Ongoing),
        waiting = gb_sets:union(Waiting, gb_sets:from_list(ToWait))
    }, ToStart}.

-spec pause_job(jobs(), job_identifier()) -> jobs().
pause_job(Jobs = #workflow_jobs{
    waiting = Waiting,
    ongoing = Ongoing
}, JobIdentifier) ->
    Jobs#workflow_jobs{
        waiting = gb_sets:insert(JobIdentifier, Waiting),
        ongoing = gb_sets:delete(JobIdentifier, Ongoing)
    }.

-spec mark_ongoing_job_finished(jobs(), job_identifier()) ->
    {jobs(), ?NO_JOBS_LEFT_FOR_PARALLEL_BOX | ?AT_LEAST_ONE_JOB_LEFT_FOR_PARALLEL_BOX}.
mark_ongoing_job_finished(Jobs = #workflow_jobs{
    ongoing = Ongoing,
    waiting = Waiting
}, JobIdentifier) ->
    NewOngoing = gb_sets:delete(JobIdentifier, Ongoing),
    RemainingForBox = case has_item(JobIdentifier, NewOngoing) orelse has_item(JobIdentifier, Waiting) of
        true -> ?AT_LEAST_ONE_JOB_LEFT_FOR_PARALLEL_BOX;
        false -> ?NO_JOBS_LEFT_FOR_PARALLEL_BOX
    end,
    {Jobs#workflow_jobs{ongoing = NewOngoing}, RemainingForBox}.

-spec register_failure(jobs(), job_identifier()) -> jobs().
register_failure(Jobs = #workflow_jobs{
    failed_items = Failed
}, #job_identifier{item_index = ItemIndex} = JobIdentifier) ->
    {Jobs2, RemainingForBox} = mark_ongoing_job_finished(Jobs, JobIdentifier),

    NewFailed = case RemainingForBox of
        % Keep only one key for failed parallel box
        ?AT_LEAST_ONE_JOB_LEFT_FOR_PARALLEL_BOX -> sets:add_element(ItemIndex, Failed);
        ?NO_JOBS_LEFT_FOR_PARALLEL_BOX -> Failed
    end,

    % TODO VFS-7551 - count errors and stop workflow when errors limit is reached
    Jobs2#workflow_jobs{failed_items = NewFailed}.

-spec ensure_job_identifier_or_cache_ans(jobs(), job_identifier() | task_executor:async_ref(), task_executor:result()) ->
    {{ok, job_identifier()} | ?WF_ERROR_UNKNOWN_REFERENCE, jobs()}.
ensure_job_identifier_or_cache_ans(Jobs, #job_identifier{} = JobIdentifier, _Ans) ->
    {{ok, JobIdentifier}, Jobs};
ensure_job_identifier_or_cache_ans(Jobs = #workflow_jobs{
    pending_async_jobs = AsyncCalls,
    unidentified_async_refs = Unidentified
}, Ref, Ans) ->
    case maps:get(Ref, AsyncCalls, undefined) of
        undefined ->
            {?WF_ERROR_UNKNOWN_REFERENCE, Jobs#workflow_jobs{unidentified_async_refs = Unidentified#{Ref => Ans}}};
        #async_job{job_identifier = JobIdentifier} ->
            {{ok, JobIdentifier}, Jobs#workflow_jobs{pending_async_jobs = maps:remove(Ref, AsyncCalls)}}
    end.

-spec prepare_next_parallel_box(jobs(), job_identifier(), workflow_definition:boxes_map(), non_neg_integer()) ->
    {ok, jobs()} | ?WF_ERROR_ITEM_PROCESSING_FINISHED(workflow_execution_state:index()).
prepare_next_parallel_box(_Jobs,
    #job_identifier{
        parallel_box_index = BoxCount,
        item_index = ItemIndex
    }, _BoxesSpec, BoxCount) ->
    ?WF_ERROR_ITEM_PROCESSING_FINISHED(ItemIndex);
prepare_next_parallel_box(
    Jobs = #workflow_jobs{
        failed_items = Failed,
        waiting = Waiting
    },
    JobIdentifier = #job_identifier{
        item_index = ItemIndex,
        parallel_box_index = BoxIndex
    },
    BoxesSpec, _BoxCount) ->
    case has_item(ItemIndex, Failed) of
        true ->
            {ok, Jobs#workflow_jobs{failed_items = sets:del_element(ItemIndex, Failed)}};
        false ->
            NewBoxIndex = BoxIndex + 1,
            Tasks = maps:get(NewBoxIndex, BoxesSpec),
            NewWaiting = lists:foldl(fun(TaskIndex, TmpWaiting) ->
                gb_sets:insert(
                    JobIdentifier#job_identifier{parallel_box_index = NewBoxIndex, task_index = TaskIndex}, TmpWaiting)
            end, Waiting, lists:seq(1, maps:size(Tasks))),
            {ok, Jobs#workflow_jobs{waiting = NewWaiting}}
    end.

%%%===================================================================
%%% Functions returning/updating pending_async_jobs field
%%%===================================================================

-spec register_async_call(jobs(), job_identifier(), task_executor:task_id(), task_executor:async_ref(), time:seconds()) ->
    {ok | ?WF_ERROR_ALREADY_FINISHED(task_executor:result()), jobs()}.
register_async_call(Jobs = #workflow_jobs{
    pending_async_jobs = AsyncCalls,
    unidentified_async_refs = Unidentified
}, JobIdentifier, TaskId, Ref, KeepaliveTimeout) ->
    case maps:get(Ref, Unidentified, undefined) of
        undefined ->
            NewAsyncCalls = AsyncCalls#{Ref => #async_job{
                job_identifier = JobIdentifier,
                task_id = TaskId,
                keepalive_timer = countdown_timer:start_seconds(KeepaliveTimeout),
                keepalive_timeout = KeepaliveTimeout
            }},
            {ok, Jobs#workflow_jobs{pending_async_jobs = NewAsyncCalls}};
        FinalAns ->
            {?WF_ERROR_ALREADY_FINISHED(FinalAns),
                Jobs#workflow_jobs{unidentified_async_refs = maps:remove(Ref, Unidentified)}}
    end.

-spec check_timeouts(jobs()) -> {ok, jobs()} | ?ERROR_NOT_FOUND.
check_timeouts(Jobs = #workflow_jobs{
    pending_async_jobs = AsyncCalls,
    ongoing = Ongoing
}) ->
    CheckAns = maps:fold(
        fun(Ref, AsyncJob, {ExtendedTimeoutsAcc, ErrorsAcc} = Acc) ->
            #async_job{task_id = TaskId, keepalive_timer = Timer} = AsyncJob,
            case countdown_timer:is_expired(Timer) of
                true ->
                    case task_executor:check_ongoing_item_processing(TaskId, Ref) of
                        ok -> {[Ref | ExtendedTimeoutsAcc], ErrorsAcc};
                        error -> {ExtendedTimeoutsAcc, [Ref | ErrorsAcc]}
                    end;
                false ->
                    Acc
            end
        end, {[], []}, AsyncCalls),

    case CheckAns of
        {[], []} ->
            ?ERROR_NOT_FOUND;
        {UpdatedTimeouts, ErrorRefs} ->
            % TODO VFS-7551 - delete iteration_state step when necessary
            {AsyncCallsWithErrorsDeleted, NewOngoing} = lists:foldl(fun(ErrorRef, {AsyncCallsAcc, OngoingAcc} = AccTuple) ->
                case maps:get(ErrorRef, AsyncCallsAcc, undefined) of
                    undefined ->
                        AccTuple; % Async call ended after timer check
                    #async_job{job_identifier = JobIdentifier} ->
                        {maps:remove(ErrorRef, AsyncCallsAcc), gb_sets:delete(JobIdentifier, OngoingAcc)}
                end
            end, {AsyncCalls, Ongoing}, ErrorRefs),

            FinalAsyncCalls = lists:foldl(fun(Ref, Acc) ->
                case maps:get(Ref, Acc, undefined) of
                    undefined ->
                        Acc; % Async call ended after timer check
                    AsyncJob = #async_job{keepalive_timeout = KeepaliveTimeout} ->
                        Acc#{Ref => AsyncJob#async_job{keepalive_timer = countdown_timer:start_seconds(KeepaliveTimeout)}}
                end
            end, AsyncCallsWithErrorsDeleted, UpdatedTimeouts),

            {ok, Jobs#workflow_jobs{
                pending_async_jobs = FinalAsyncCalls,
                ongoing = NewOngoing
            }}
    end.

-spec reset_keepalive_timer(jobs(), task_executor:async_ref()) -> jobs().
reset_keepalive_timer(Jobs = #workflow_jobs{pending_async_jobs = AsyncCalls}, Ref) ->
    NewAsyncCalls = case maps:get(Ref, AsyncCalls, undefined) of
        undefined ->
            AsyncCalls; % Async call ended after timer check
        AsyncJob = #async_job{keepalive_timeout = KeepaliveTimeout} ->
            AsyncCalls#{Ref => AsyncJob#async_job{keepalive_timer = countdown_timer:start_seconds(KeepaliveTimeout)}}
    end,
    Jobs#workflow_jobs{pending_async_jobs = NewAsyncCalls}.

%%%===================================================================
%%% Functions operating on job_identifier record
%%%===================================================================

-spec is_async_job(job_identifier() | task_executor:async_ref()) -> boolean().
is_async_job(#job_identifier{}) ->
    false;
is_async_job(_) ->
    true.

-spec get_item_id(job_identifier(), workflow_iteration_state:state()) -> workflow_cached_item:id().
get_item_id(#job_identifier{item_index = ItemIndex}, IterationProgress) ->
    workflow_iteration_state:get_item_id(IterationProgress, ItemIndex).

-spec get_task_id(job_identifier(), workflow_definition:boxes_map()) -> task_executor:task_id().
get_task_id(#job_identifier{parallel_box_index = BoxIndex, task_index = TaskIndex}, BoxesSpec) ->
    Tasks = maps:get(BoxIndex, BoxesSpec),
    maps:get(TaskIndex, Tasks).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec has_item(job_identifier() | workflow_execution_state:index(), jobs_set() | items_set()) -> boolean().
has_item(JobIdentifier = #job_identifier{item_index = ItemIndex}, Tree) ->
    case gb_sets:next(gb_sets:iterator_from(JobIdentifier#job_identifier{task_index = 1}, Tree)) of
        {#job_identifier{item_index = ItemIndex}, _NextIterator} -> true;
        _ -> false
    end;
has_item(ItemIndex, Set) ->
    sets:is_element(ItemIndex, Set).