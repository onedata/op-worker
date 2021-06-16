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
    remove_pending_async_job/3, prepare_next_parallel_box/4]).
%% Functions returning/updating pending_async_jobs field
-export([register_async_call/3, check_timeouts/1, reset_keepalive_timer/2]).
%% Functions operating on job_identifier record
-export([job_identifier_to_binary/1, binary_to_job_identifier/1, get_item_id/2,
    get_task_details/2, is_previous/2]).

% Internal record used for scheduled jobs management
-record(job_identifier, {
    item_index :: workflow_execution_state:index(),
    parallel_box_index :: workflow_execution_state:index(),
    task_index :: workflow_execution_state:index()
}).

% Internal record used to control timeouts of jobs that are processed asynchronously
-record(async_job_timer, {
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
    raced_results = #{} :: raced_results() % TODO VFS-7787 - clean when they are not needed anymore (after integration with BW)
}).

-type job_identifier() :: #job_identifier{}.
-type jobs_set() :: gb_sets:set(job_identifier()).
-type items_set() :: sets:set(workflow_execution_state:index()).
-type pending_async_jobs() :: #{job_identifier() => #async_job_timer{}}.
-type raced_results() :: #{job_identifier() => workflow_handler:callback_execution_result()}.
-type jobs() :: #workflow_jobs{}.
-type jobs_for_parallel_box() :: ?NO_JOBS_LEFT_FOR_PARALLEL_BOX | ?AT_LEAST_ONE_JOB_LEFT_FOR_PARALLEL_BOX.
-type item_processing_result() :: ?SUCCESS | ?FAILURE.

-export_type([job_identifier/0, jobs/0, item_processing_result/0]).

-define(SEPARATOR, "_").

%%%===================================================================
%%% API
%%%===================================================================

-spec init() -> jobs().
init() ->
    #workflow_jobs{}.

-spec prepare_next_waiting_job(jobs()) ->
    {ok, job_identifier(), jobs()} | ?WF_ERROR_NO_WAITING_ITEMS | ?ERROR_NOT_FOUND.
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
                false -> ?WF_ERROR_NO_WAITING_ITEMS
            end
    end.

-spec populate_with_jobs_for_item(
    jobs(),
    workflow_execution_state:index(),
    workflow_execution_state:index(),
    workflow_execution_state:boxes_map()
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

-spec mark_ongoing_job_finished(jobs(), job_identifier()) -> {jobs(), jobs_for_parallel_box()}.
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

-spec register_failure(jobs(), job_identifier()) -> {jobs(), jobs_for_parallel_box()}.
register_failure(Jobs = #workflow_jobs{
    failed_items = Failed
}, #job_identifier{item_index = ItemIndex} = JobIdentifier) ->
    {Jobs2, RemainingForBox} = mark_ongoing_job_finished(Jobs, JobIdentifier),
    % TODO VFS-7788 - count errors and stop workflow when errors limit is reached
    {Jobs2#workflow_jobs{failed_items = sets:add_element(ItemIndex, Failed)}, RemainingForBox}.

-spec remove_pending_async_job(jobs(), job_identifier(), workflow_handler:callback_execution_result()) ->
    {ok | ?WF_ERROR_UNKNOWN_JOB, jobs()}.
remove_pending_async_job(Jobs = #workflow_jobs{
    pending_async_jobs = AsyncCalls,
    raced_results = Unidentified
}, JobIdentifier, Ans) ->
    case maps:get(JobIdentifier, AsyncCalls, undefined) of
        undefined ->
            {?WF_ERROR_UNKNOWN_JOB, Jobs#workflow_jobs{raced_results = Unidentified#{JobIdentifier => Ans}}};
        _ ->
            {ok, Jobs#workflow_jobs{pending_async_jobs = maps:remove(JobIdentifier, AsyncCalls)}}
    end.

-spec prepare_next_parallel_box(jobs(), job_identifier(), workflow_execution_state:boxes_map(), non_neg_integer()) ->
    {ok | ?WF_ERROR_ITEM_PROCESSING_FINISHED(workflow_execution_state:index(), item_processing_result()), jobs()}.
prepare_next_parallel_box(Jobs,
    #job_identifier{
        parallel_box_index = BoxCount,
        item_index = ItemIndex
    }, _BoxesSpec, BoxCount) ->
    {?WF_ERROR_ITEM_PROCESSING_FINISHED(ItemIndex, ?SUCCESS), Jobs};
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
            {?WF_ERROR_ITEM_PROCESSING_FINISHED(ItemIndex, ?FAILURE),
                Jobs#workflow_jobs{failed_items = sets:del_element(ItemIndex, Failed)}};
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

-spec register_async_call(jobs(), job_identifier(), time:seconds()) -> 
    {ok | ?WF_ERROR_ALREADY_FINISHED(workflow_handler:callback_execution_result()), jobs()}.
register_async_call(Jobs = #workflow_jobs{
    pending_async_jobs = AsyncCalls,
    raced_results = Unidentified
}, JobIdentifier, KeepaliveTimeout) ->
    case maps:get(JobIdentifier, Unidentified, undefined) of
        undefined ->
            NewAsyncCalls = AsyncCalls#{JobIdentifier => #async_job_timer{
                keepalive_timer = countdown_timer:start_seconds(KeepaliveTimeout),
                keepalive_timeout = KeepaliveTimeout
            }},
            {ok, Jobs#workflow_jobs{pending_async_jobs = NewAsyncCalls}};
        FinalAns ->
            {?WF_ERROR_ALREADY_FINISHED(FinalAns),
                Jobs#workflow_jobs{raced_results = maps:remove(JobIdentifier, Unidentified)}}
    end.

-spec check_timeouts(jobs()) -> {ok, jobs()} | ?ERROR_NOT_FOUND.
check_timeouts(Jobs = #workflow_jobs{
    pending_async_jobs = AsyncCalls,
    ongoing = Ongoing
}) ->
    CheckAns = maps:fold(
        fun(JobIdentifier, AsyncJobTimer, {ExtendedTimeoutsAcc, ErrorsAcc} = Acc) ->
            #async_job_timer{keepalive_timer = Timer} = AsyncJobTimer,
            case countdown_timer:is_expired(Timer) of
                true ->
                    % TODO VFS-7788 - check if task is expired
%%                    case task_executor:check_ongoing_item_processing(TaskId, Ref) of
%%                        ok -> {[Ref | ExtendedTimeoutsAcc], ErrorsAcc};
%%                        error -> {ExtendedTimeoutsAcc, [JobIdentifier | ErrorsAcc]}
%%                    end;
                    {ExtendedTimeoutsAcc, [JobIdentifier | ErrorsAcc]};
                false ->
                    Acc
            end
        end, {[], []}, AsyncCalls),

    case CheckAns of
        {[], []} ->
            ?ERROR_NOT_FOUND;
        {UpdatedTimeouts, Errors} ->
            % TODO VFS-7786 - delete iteration_state step when necessary
            {AsyncCallsWithErrorsDeleted, NewOngoing} = lists:foldl(fun(JobIdentifier, {AsyncCallsAcc, OngoingAcc} = AccTuple) ->
                case maps:get(JobIdentifier, AsyncCallsAcc, undefined) of
                    undefined ->
                        AccTuple; % Async call ended after timer check
                    #async_job_timer{} ->
                        {maps:remove(JobIdentifier, AsyncCallsAcc), gb_sets:delete(JobIdentifier, OngoingAcc)}
                end
            end, {AsyncCalls, Ongoing}, Errors),

            FinalAsyncCalls = lists:foldl(fun(JobIdentifier, Acc) ->
                case maps:get(JobIdentifier, Acc, undefined) of
                    undefined ->
                        Acc; % Async call ended after timer check
                    AsyncJobTimer = #async_job_timer{keepalive_timeout = KeepaliveTimeout} ->
                        Acc#{JobIdentifier => AsyncJobTimer#async_job_timer{
                            keepalive_timer = countdown_timer:start_seconds(KeepaliveTimeout)}}
                end
            end, AsyncCallsWithErrorsDeleted, UpdatedTimeouts),

            {ok, Jobs#workflow_jobs{
                pending_async_jobs = FinalAsyncCalls,
                ongoing = NewOngoing
            }}
    end.

-spec reset_keepalive_timer(jobs(), job_identifier()) -> jobs().
reset_keepalive_timer(Jobs = #workflow_jobs{pending_async_jobs = AsyncCalls}, JobIdentifier) ->
    NewAsyncCalls = case maps:get(JobIdentifier, AsyncCalls, undefined) of
        undefined ->
            AsyncCalls; % Async call ended after timer check
        AsyncJobTimer = #async_job_timer{keepalive_timeout = KeepaliveTimeout} ->
            AsyncCalls#{JobIdentifier => AsyncJobTimer#async_job_timer{
                keepalive_timer = countdown_timer:start_seconds(KeepaliveTimeout)}}
    end,
    Jobs#workflow_jobs{pending_async_jobs = NewAsyncCalls}.

%%%===================================================================
%%% Functions operating on job_identifier record
%%%===================================================================

-spec job_identifier_to_binary(job_identifier()) -> binary().
job_identifier_to_binary(#job_identifier{
    item_index = ItemIndex,
    parallel_box_index = BoxIndex,
    task_index = TaskIndex
}) ->
    <<(integer_to_binary(ItemIndex))/binary, ?SEPARATOR,
        (integer_to_binary(BoxIndex))/binary, ?SEPARATOR,
        (integer_to_binary(TaskIndex))/binary>>.

-spec binary_to_job_identifier(binary()) -> job_identifier().
binary_to_job_identifier(Binary) ->
    [ItemIndexBin, BoxIndexBin, TaskIndexBin] = binary:split(Binary, <<?SEPARATOR>>, [global, trim_all]),
    #job_identifier{
        item_index = binary_to_integer(ItemIndexBin),
        parallel_box_index = binary_to_integer(BoxIndexBin),
        task_index = binary_to_integer(TaskIndexBin)
    }.

-spec get_item_id(job_identifier(), workflow_iteration_state:state()) -> workflow_cached_item:id().
get_item_id(#job_identifier{item_index = ItemIndex}, IterationProgress) ->
    workflow_iteration_state:get_item_id(IterationProgress, ItemIndex).

-spec get_task_details(job_identifier(), workflow_execution_state:boxes_map()) ->
    {workflow_engine:task_id(), workflow_engine:task_spec()}.
get_task_details(#job_identifier{parallel_box_index = BoxIndex, task_index = TaskIndex}, BoxesSpec) ->
    Tasks = maps:get(BoxIndex, BoxesSpec),
    maps:get(TaskIndex, Tasks).

-spec is_previous(job_identifier(), job_identifier()) -> boolean().
is_previous(#job_identifier{item_index = ItemIndex1}, #job_identifier{item_index = ItemIndex2}) ->
    ItemIndex1 < ItemIndex2.

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