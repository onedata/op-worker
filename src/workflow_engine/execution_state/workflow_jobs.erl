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
-include_lib("ctool/include/logging.hrl").

%% API
-export([init/0, prepare_next_waiting_job/1, prepare_next_waiting_result/1, populate_with_jobs_for_item/5,
    schedule_resume_of_job/2, mark_ongoing_job_finished/2, register_failure/2,
    register_async_job_finish/3, prepare_next_parallel_box/4,
    get_identfiers_for_next_parallel_boxes/3, has_ongoing_jobs/1, get_all_async_cached_result_ids/1,
    get_waiting_or_ongoing_tasks_indexes/1, dump/1, from_dump/2, get_dump_struct/0, fix_resumed_indexes/3]).
%% Functions returning/updating pending_async_jobs field
-export([register_async_call/4, check_timeouts/1, reset_keepalive_timer/2]).
%% Functions operating on job_identifier record
-export([encode_job_identifier/1, decode_job_identifier/1, get_item_id/2, get_subject_id/3,
    get_task_details/2, get_processing_type/1]).
%% API used to check which tasks are finished for all items
-export([is_task_finished/2, is_task_finished/3, build_tasks_tree/1]).
%% Test API
-export([is_empty/1, get_results_in_processing_from_dump/1]).

% Internal record used for scheduled jobs management
-record(job_identifier, {
    item_index :: workflow_execution_state:index(),
    parallel_box_index :: workflow_execution_state:index(),
    task_index :: workflow_execution_state:index(),
    processing_type :: processing_type(),
    incarnation_tag :: workflow_execution_state:incarnation_tag()
}).

% Internal record used to control timeouts of jobs that are processed asynchronously
-record(async_job_timer, {
    keepalive_timer :: countdown_timer:instance() | undefined,
    % max allowed time between heartbeats to assume the async process is still alive
    keepalive_timeout :: time:seconds()
}).

% Internal record used to check which tasks are finished for all items
-record(task_identifier, {
    parallel_box_index :: workflow_execution_state:index(),
    task_index :: workflow_execution_state:index()
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
    raced_results = #{} :: async_results_map(), % TODO VFS-7787 - clean when they are not needed anymore (after integration with BW)
    async_cached_results = #{} :: async_results_map(),

    tasks_tree :: tasks_tree(),
    results_iterator :: undefined | results_iterator()
}).

-record(dump, {
    waiting :: [job_identifier()],
    failed :: [workflow_execution_state:index()]
}).


-type job_identifier() :: #job_identifier{}.
-type encoded_job_identifier() :: binary().
-type jobs_set() :: gb_sets:set(job_identifier()).
-type items_set() :: sets:set(workflow_execution_state:index()).
-type pending_async_jobs() :: #{job_identifier() => #async_job_timer{}}.
-type async_results_map() :: #{job_identifier() => workflow_cached_async_result:result_ref()}.
-type tasks_tree() :: gb_trees:tree(job_identifier(), [workflow_execution_state:index()]) | undefined.
-type jobs() :: #workflow_jobs{}.
-type jobs_for_parallel_box() :: ?NO_JOBS_LEFT_FOR_PARALLEL_BOX | ?AT_LEAST_ONE_JOB_LEFT_FOR_PARALLEL_BOX.
-type item_processing_result() :: ?SUCCESS | ?FAILURE.
-type processing_type() :: ?JOB_PROCESSING | ?ASYNC_RESULT_PROCESSING.

-define(ITERATION_FINISHED, iteration_finished).
-type results_iterator() :: gb_sets:iter(job_identifier()) | ?ITERATION_FINISHED.

-type dump() ::  #dump{}.

-export_type([job_identifier/0, encoded_job_identifier/0, jobs/0, dump/0]).

-define(SEPARATOR, "_").
-define(OPERATION_UNSUPPORTED, operation_unsupported).

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
            NewJobs = Jobs#workflow_jobs{waiting = NewWaiting, ongoing = NewOngoing},
            {ok, JobIdentifier, maybe_remove_async_cached_result(NewJobs, JobIdentifier)};
        true ->
            case gb_sets:is_empty(Ongoing) of
                true -> ?ERROR_NOT_FOUND;
                false -> ?WF_ERROR_NO_WAITING_ITEMS
            end
    end.

-spec prepare_next_waiting_result(jobs()) ->
    {{ok, job_identifier()} | ?ERROR_NOT_FOUND, jobs()} | ?WF_ERROR_ITERATION_FINISHED.
prepare_next_waiting_result(Jobs = #workflow_jobs{results_iterator = undefined, waiting = Waiting}) ->
    prepare_next_waiting_result(Jobs#workflow_jobs{results_iterator = gb_sets:iterator(Waiting)});
prepare_next_waiting_result(#workflow_jobs{results_iterator = ?ITERATION_FINISHED}) ->
    ?WF_ERROR_ITERATION_FINISHED;
prepare_next_waiting_result(Jobs = #workflow_jobs{
    waiting = Waiting,
    ongoing = Ongoing,
    results_iterator = Iterator
}) ->
    case gb_sets:next(Iterator) of
        {#job_identifier{processing_type = ?ASYNC_RESULT_PROCESSING} = JobIdentifier, NextIterator} ->
            NewWaiting = gb_sets:delete(JobIdentifier, Waiting),
            NewOngoing = gb_sets:insert(JobIdentifier, Ongoing),
            NewJobs = Jobs#workflow_jobs{waiting = NewWaiting, ongoing = NewOngoing, results_iterator = NextIterator},
            {{ok, JobIdentifier}, maybe_remove_async_cached_result(NewJobs, JobIdentifier)};
        {_, NextIterator} ->
            prepare_next_waiting_result(Jobs#workflow_jobs{results_iterator = NextIterator});
        none ->
            {?ERROR_NOT_FOUND, Jobs#workflow_jobs{results_iterator = ?ITERATION_FINISHED}}
    end.

-spec populate_with_jobs_for_item(
    jobs(),
    workflow_execution_state:index(),
    workflow_execution_state:index(),
    workflow_execution_state:boxes_map(),
    workflow_execution_state:incarnation_tag()
) -> {jobs(), job_identifier()}.
populate_with_jobs_for_item(
    Jobs = #workflow_jobs{
        ongoing = Ongoing,
        waiting = Waiting
    },
    ItemIndex, ParallelBoxToStartIndex, BoxesSpec, IncarnationTag) ->
    Tasks = maps:get(1, BoxesSpec),
    [ToStart | ToWait] = lists:map(fun(TaskIndex) ->
        #job_identifier{
            processing_type = ?JOB_PROCESSING,
            item_index = ItemIndex,
            parallel_box_index = ParallelBoxToStartIndex,
            task_index = TaskIndex,
            incarnation_tag = IncarnationTag
        }
    end, lists:seq(1, maps:size(Tasks))),

    {Jobs#workflow_jobs{
        ongoing = gb_sets:insert(ToStart, Ongoing),
        waiting = gb_sets:union(Waiting, gb_sets:from_list(ToWait))
    }, ToStart}.

-spec schedule_resume_of_job(jobs(), job_identifier()) -> jobs().
schedule_resume_of_job(Jobs = #workflow_jobs{
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
    waiting = Waiting,
    tasks_tree = TasksTree
}, JobIdentifier) ->
    NewOngoing = gb_sets:delete(JobIdentifier, Ongoing),
    RemainingForBox = case has_item(JobIdentifier, NewOngoing) orelse has_item(JobIdentifier, Waiting) of
        true -> ?AT_LEAST_ONE_JOB_LEFT_FOR_PARALLEL_BOX;
        false -> ?NO_JOBS_LEFT_FOR_PARALLEL_BOX
    end,
    {
        Jobs#workflow_jobs{
            ongoing = NewOngoing,
            tasks_tree = remove_job_from_task_tree(TasksTree, JobIdentifier)
        },
        RemainingForBox
    }.

-spec register_failure(jobs(), job_identifier()) -> {jobs(), jobs_for_parallel_box()}.
register_failure(Jobs = #workflow_jobs{
    failed_items = Failed
}, #job_identifier{item_index = ItemIndex} = JobIdentifier) ->
    {Jobs2, RemainingForBox} = mark_ongoing_job_finished(Jobs, JobIdentifier),
    % TODO VFS-7788 - count errors and stop workflow when errors limit is reached
    {Jobs2#workflow_jobs{failed_items = sets:add_element(ItemIndex, Failed)}, RemainingForBox}.

-spec register_async_job_finish(jobs(), job_identifier(), workflow_cached_async_result:result_ref()) ->
    {ok, jobs()} | ?WF_ERROR_JOB_NOT_FOUND.
register_async_job_finish(Jobs = #workflow_jobs{
    ongoing = Ongoing,
    pending_async_jobs = AsyncCalls,
    raced_results = Unidentified
}, JobIdentifier, CachedResultId) ->
    case gb_sets:is_member(JobIdentifier, Ongoing) of
        true ->
            NewJobs = case maps:get(JobIdentifier, AsyncCalls, undefined) of
                undefined ->
                    Jobs#workflow_jobs{raced_results = Unidentified#{JobIdentifier => CachedResultId}};
                _ ->
                    register_async_result_processing(
                        Jobs#workflow_jobs{pending_async_jobs = maps:remove(JobIdentifier, AsyncCalls)},
                        JobIdentifier, CachedResultId)
            end,
            {ok, NewJobs};
        false ->
            ?debug("Result for unknown job ~tp", [JobIdentifier]),
            ?WF_ERROR_JOB_NOT_FOUND
    end.

-spec prepare_next_parallel_box(jobs(), job_identifier(), workflow_execution_state:boxes_map(), non_neg_integer()) ->
    {ok | ?WF_ERROR_ITEM_PROCESSING_ENDED(workflow_execution_state:index(), item_processing_result()), jobs()}.
prepare_next_parallel_box(
    Jobs = #workflow_jobs{
        failed_items = Failed,
        waiting = Waiting,
        tasks_tree = TasksTree
    },
    #job_identifier{
        item_index = ItemIndex,
        parallel_box_index = BoxIndex,
        incarnation_tag = IncarnationTag
    },
    BoxesSpec, BoxCount) ->
    case {has_item(ItemIndex, Failed), BoxIndex} of
        {true, _} ->
            {?WF_ERROR_ITEM_PROCESSING_ENDED(ItemIndex, ?FAILURE),
                Jobs#workflow_jobs{failed_items = sets:del_element(ItemIndex, Failed)}};
        {false, BoxCount} ->
            {?WF_ERROR_ITEM_PROCESSING_ENDED(ItemIndex, ?SUCCESS), Jobs};
        {false, _} ->
            NewBoxIndex = BoxIndex + 1,
            Tasks = maps:get(NewBoxIndex, BoxesSpec),
            NewWaiting = lists:foldl(fun(TaskIndex, TmpWaiting) ->
                [#job_identifier{
                    processing_type = ?JOB_PROCESSING,
                    item_index = ItemIndex,
                    parallel_box_index = NewBoxIndex,
                    task_index = TaskIndex,
                    incarnation_tag = IncarnationTag
                } | TmpWaiting]
            end, [], lists:seq(1, maps:size(Tasks))),
            {ok, Jobs#workflow_jobs{
                waiting = gb_sets:union(Waiting, gb_sets:from_list(NewWaiting)),
                tasks_tree = add_jobs_to_not_empty_task_tree(TasksTree, NewWaiting)
            }}
    end.

-spec get_identfiers_for_next_parallel_boxes(job_identifier(), workflow_execution_state:boxes_map(), non_neg_integer()) ->
    [job_identifier()].
get_identfiers_for_next_parallel_boxes(#job_identifier{parallel_box_index = BoxCount}, _BoxesSpec, BoxCount) ->
    [];
get_identfiers_for_next_parallel_boxes(#job_identifier{
    item_index = ItemIndex,
    parallel_box_index = BoxIndex,
    incarnation_tag = IncarnationTag
} = Identifier, BoxSpecs, BoxCount) ->
    NextBoxIndex = BoxIndex + 1,
    Tasks = maps:get(NextBoxIndex, BoxSpecs),
    Identifiers = lists:map(fun(TaskIndex) ->
        #job_identifier{
            processing_type = ?JOB_PROCESSING,
            item_index = ItemIndex,
            parallel_box_index = NextBoxIndex,
            task_index = TaskIndex,
            incarnation_tag = IncarnationTag
        }
    end, lists:seq(1, maps:size(Tasks))),
    Identifiers ++ get_identfiers_for_next_parallel_boxes(
        Identifier#job_identifier{parallel_box_index = NextBoxIndex}, BoxSpecs, BoxCount).



-spec has_ongoing_jobs(jobs()) -> boolean().
has_ongoing_jobs(#workflow_jobs{ongoing = Ongoing}) ->
    not gb_sets:is_empty(Ongoing).


-spec get_all_async_cached_result_ids(jobs()) -> [workflow_cached_async_result:result_ref()].
get_all_async_cached_result_ids(#workflow_jobs{async_cached_results = Results}) ->
    maps:values(Results).


-spec get_waiting_or_ongoing_tasks_indexes(jobs()) -> #{job_identifier() => [job_identifier()]}.
get_waiting_or_ongoing_tasks_indexes(#workflow_jobs{waiting = Waiting, ongoing = Ongoing}) ->
    gb_sets:fold(fun(#job_identifier{parallel_box_index = BoxIndex, task_index = TaskIndex}, Acc) ->
        BoxTasks = maps:get(BoxIndex, Acc, []),
        Acc#{BoxIndex => [TaskIndex | BoxTasks]}
    end, #{}, gb_sets:union(Waiting, Ongoing)).


-spec dump(jobs()) -> dump().
dump(#workflow_jobs{waiting = Waiting, failed_items = FailedItems}) ->
    #dump{waiting = gb_sets:to_list(Waiting), failed = sets:to_list(FailedItems)}.


-spec from_dump(dump(), workflow_execution_state:incarnation_tag()) -> jobs().
from_dump(#dump{waiting = WaitingList, failed = FailedList}, IncarnationTag) ->
    MappedWaitingList = lists:map(fun(JobIdentifier) ->
        JobIdentifier#job_identifier{processing_type = ?JOB_PROCESSING, incarnation_tag = IncarnationTag}
    end, WaitingList),
    #workflow_jobs{waiting = gb_sets:from_list(MappedWaitingList), failed_items = sets:from_list(FailedList)}.


-spec get_dump_struct() -> tuple().
get_dump_struct() ->
    {record, [
        {waiting, [{record, [
            {item_index, integer},
            {parallel_box_index, integer},
            {task_index, integer},
            {processing_type, atom},
            {incarnation_tag, string}
        ]}]},
        {failed, [integer]}
    ]}.


-spec fix_resumed_indexes(jobs(), workflow_execution_state:boxes_map(),
    workflow_execution_state_dump:task_index_map()) -> jobs().
fix_resumed_indexes(#workflow_jobs{waiting = Waiting}, BoxesSpec, OriginalIndexMap) ->
    TaskIdsToIndexes = maps:fold(fun(BoxIndex, BoxSpec, ExternalAcc) ->
        maps:fold(fun(TaskIndex, {TaskId, _}, InternalAcc) ->
            InternalAcc#{TaskId => {BoxIndex, TaskIndex}}
        end, ExternalAcc, BoxSpec)
    end, #{}, BoxesSpec),

    MappedWaiting = gb_sets:from_ordset(lists:sort(lists:map(
        fun(#job_identifier{parallel_box_index = BoxIndex, task_index = TaskIndex} = Identifier) ->
            {NewBoxIndex, NewTaskIndex} = maps:get(maps:get({BoxIndex, TaskIndex}, OriginalIndexMap), TaskIdsToIndexes),
            Identifier#job_identifier{parallel_box_index = NewBoxIndex, task_index = NewTaskIndex}
        end, gb_sets:to_list(Waiting)))),

    #workflow_jobs{waiting = MappedWaiting}.


%%%===================================================================
%%% Functions returning/updating pending_async_jobs field
%%%===================================================================

-spec register_async_call(workflow_engine:id(), jobs(), job_identifier(), time:seconds()) -> jobs().
register_async_call(EngineId, Jobs = #workflow_jobs{
    pending_async_jobs = AsyncCalls,
    raced_results = Unidentified
}, JobIdentifier, KeepaliveTimeout) ->
    case maps:get(JobIdentifier, Unidentified, undefined) of
        undefined ->
            KeepaliveTimer = case workflow_engine:get_enqueuing_timeout(EngineId) of
                infinity -> undefined;
                undefined -> countdown_timer:start_seconds(KeepaliveTimeout);
                Timeout -> countdown_timer:start_seconds(Timeout)
            end,
            NewAsyncCalls = AsyncCalls#{JobIdentifier => #async_job_timer{
                keepalive_timer = KeepaliveTimer,
                keepalive_timeout = KeepaliveTimeout
            }},
            Jobs#workflow_jobs{pending_async_jobs = NewAsyncCalls};
        CachedResultId ->
            register_async_result_processing(
                Jobs#workflow_jobs{raced_results = maps:remove(JobIdentifier, Unidentified)},
                JobIdentifier, CachedResultId)
    end.

-spec check_timeouts(jobs()) -> {jobs() | ?WF_ERROR_NO_TIMEOUTS_UPDATED, [job_identifier()]} | ?ERROR_NOT_FOUND.
check_timeouts(Jobs = #workflow_jobs{
    pending_async_jobs = AsyncCalls
}) ->
    CheckAns = maps:fold(
        fun(JobIdentifier, AsyncJobTimer, {ExtendedTimeoutsAcc, ExpiredJobsAcc} = Acc) ->
            #async_job_timer{keepalive_timer = Timer} = AsyncJobTimer,
            case Timer =/= undefined andalso countdown_timer:is_expired(Timer) of
                true ->
                    % TODO VFS-7788 - check if task is expired (do it outside tp process)
%%                    case task_executor:check_ongoing_item_processing(TaskId, Ref) of
%%                        ok -> {[Ref | ExtendedTimeoutsAcc], ErrorsAcc};
%%                        error -> {ExtendedTimeoutsAcc, [JobIdentifier | ErrorsAcc]}
%%                    end;
                    {ExtendedTimeoutsAcc, [JobIdentifier | ExpiredJobsAcc]};
                false ->
                    Acc
            end
        end, {[], []}, AsyncCalls),

    case CheckAns of
        {[], ExpiredJobsIdentifiers} ->
            {?WF_ERROR_NO_TIMEOUTS_UPDATED, ExpiredJobsIdentifiers};
        {UpdatedTimeouts, ExpiredJobsIdentifiers} ->
            FinalAsyncCalls = lists:foldl(fun(JobIdentifier, Acc) ->
                case maps:get(JobIdentifier, Acc, undefined) of
                    undefined ->
                        Acc; % Async call ended after timer check
                    AsyncJobTimer = #async_job_timer{keepalive_timeout = KeepaliveTimeout} ->
                        Acc#{JobIdentifier => AsyncJobTimer#async_job_timer{
                            keepalive_timer = countdown_timer:start_seconds(KeepaliveTimeout)}}
                end
            end, AsyncCalls, UpdatedTimeouts),

            {Jobs#workflow_jobs{pending_async_jobs = FinalAsyncCalls}, ExpiredJobsIdentifiers}
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

-spec encode_job_identifier(job_identifier()) -> encoded_job_identifier().
encode_job_identifier(#job_identifier{
    processing_type = ?JOB_PROCESSING,
    item_index = ItemIndex,
    parallel_box_index = BoxIndex,
    task_index = TaskIndex,
    incarnation_tag = IncarnationTag
}) ->
    <<(integer_to_binary(ItemIndex))/binary, ?SEPARATOR,
        (integer_to_binary(BoxIndex))/binary, ?SEPARATOR,
        (integer_to_binary(TaskIndex))/binary, ?SEPARATOR,
        IncarnationTag/binary>>;
encode_job_identifier(#job_identifier{processing_type = ?ASYNC_RESULT_PROCESSING}) ->
    throw(?OPERATION_UNSUPPORTED).

-spec decode_job_identifier(encoded_job_identifier()) -> job_identifier().
decode_job_identifier(Binary) ->
    [ItemIndexBin, BoxIndexBin, TaskIndexBin, IncarnationTag] = binary:split(Binary, <<?SEPARATOR>>, [global, trim_all]),
    #job_identifier{
        processing_type = ?JOB_PROCESSING,
        item_index = binary_to_integer(ItemIndexBin),
        parallel_box_index = binary_to_integer(BoxIndexBin),
        task_index = binary_to_integer(TaskIndexBin),
        incarnation_tag = IncarnationTag
    }.

-spec get_item_id(job_identifier(), workflow_iteration_state:state()) -> workflow_cached_item:id().
get_item_id(#job_identifier{item_index = ItemIndex}, IterationProgress) ->
    workflow_iteration_state:get_item_id(IterationProgress, ItemIndex).

-spec get_subject_id(job_identifier(), jobs(), workflow_iteration_state:state()) -> workflow_engine:subject_id().
get_subject_id(#job_identifier{processing_type = ?JOB_PROCESSING, item_index = ItemIndex}, _Jobs, IterationProgress) ->
    workflow_iteration_state:get_item_id(IterationProgress, ItemIndex);
get_subject_id(
    #job_identifier{processing_type = ?ASYNC_RESULT_PROCESSING} = JobIdentifier,
    #workflow_jobs{async_cached_results = Results},
    _IterationProgress
) ->
    maps:get(JobIdentifier, Results).

-spec get_task_details(job_identifier(), workflow_execution_state:boxes_map()) ->
    {workflow_engine:task_id(), workflow_engine:task_spec()}.
get_task_details(#job_identifier{parallel_box_index = BoxIndex, task_index = TaskIndex}, BoxesSpec) ->
    Tasks = maps:get(BoxIndex, BoxesSpec),
    maps:get(TaskIndex, Tasks).

-spec get_processing_type(job_identifier()) -> processing_type().
get_processing_type(#job_identifier{processing_type = ProcessingType}) ->
    ProcessingType.

%%%===================================================================
%%% API used to check which tasks are finished for all items
%%%
%%% Tasks tree is built when iteration is finished so there is guarantee
%%% that tasks for new items will not appear. Thus, if task tree is
%%% undefined, the iteration is not finished so no task can be finished
%%% for all items. If tasks tree has been built, it shows if task is
%%% finished for all items.
%%%===================================================================

-spec is_task_finished(jobs(), job_identifier()) -> boolean().
is_task_finished(Jobs, #job_identifier{parallel_box_index = BoxIndex, task_index = TaskIndex}) ->
    is_task_finished(Jobs, BoxIndex, TaskIndex).

-spec is_task_finished(jobs(), workflow_execution_state:index(), workflow_execution_state:index()) -> boolean().
is_task_finished(#workflow_jobs{tasks_tree = undefined}, _BoxIndex, _TaskIndex) ->
    false;
is_task_finished(#workflow_jobs{tasks_tree = TasksTree}, BoxIndex, TaskIndex) ->
    case gb_trees:is_empty(TasksTree) of
        true ->
            true;
        false ->
            TaskIdentifier = #task_identifier{parallel_box_index = BoxIndex, task_index = TaskIndex},
            case gb_trees:smallest(TasksTree) of
                {Key, _} when Key > TaskIdentifier ->
                    true;
                {TaskIdentifier, _} ->
                    false;
                {#task_identifier{parallel_box_index = BoxIndex}, _} ->
                    case gb_trees:take_any(TaskIdentifier, TasksTree) of
                        error -> true;
                        _ -> false
                    end;
                _ ->
                    false
            end
    end.

-spec build_tasks_tree(jobs()) -> jobs().
build_tasks_tree(Jobs = #workflow_jobs{tasks_tree = undefined, waiting = Waiting, ongoing = Ongoing}) ->
    Jobs#workflow_jobs{tasks_tree = add_jobs_to_task_tree(
        gb_trees:empty(), gb_sets:to_list(Waiting) ++ gb_sets:to_list(Ongoing))};
build_tasks_tree(Jobs) ->
    Jobs.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec has_item(job_identifier() | workflow_execution_state:index(), jobs_set() | items_set()) -> boolean().
has_item(JobIdentifier = #job_identifier{item_index = ItemIndex}, Tree) ->
    IdentifierToStart = JobIdentifier#job_identifier{task_index = 1, processing_type = ?ASYNC_RESULT_PROCESSING},
    case gb_sets:next(gb_sets:iterator_from(IdentifierToStart, Tree)) of
        {#job_identifier{item_index = ItemIndex}, _NextIterator} -> true;
        _ -> false
    end;
has_item(ItemIndex, Set) ->
    sets:is_element(ItemIndex, Set).

-spec add_jobs_to_task_tree(tasks_tree(), [job_identifier()]) -> tasks_tree().
add_jobs_to_task_tree(InitialTree, JobIdentifiers) ->
    lists:foldl(fun(#job_identifier{
        item_index = ItemIndex,
        parallel_box_index = BoxIndex,
        task_index = TaskIndex
    }, Acc) ->
        TaskIdentifier = #task_identifier{parallel_box_index = BoxIndex, task_index = TaskIndex},
        {TaskItems, AccWithoutKey} = case gb_trees:take_any(TaskIdentifier, Acc) of
            error -> {[], Acc};
            Other -> Other
        end,
        gb_trees:enter(TaskIdentifier, [ItemIndex | TaskItems], AccWithoutKey)
    end, InitialTree, JobIdentifiers).

-spec add_jobs_to_not_empty_task_tree(tasks_tree(), [job_identifier()]) -> tasks_tree().
add_jobs_to_not_empty_task_tree(undefined, _JobIdentifiers) ->
    undefined;
add_jobs_to_not_empty_task_tree(InitialTree, JobIdentifiers) ->
    add_jobs_to_task_tree(InitialTree, JobIdentifiers).

-spec remove_job_from_task_tree(tasks_tree(), job_identifier()) -> tasks_tree().
remove_job_from_task_tree(undefined, _JobIdentifier) ->
    undefined;
remove_job_from_task_tree(TasksTree, #job_identifier{
    item_index = ItemIndex,
    parallel_box_index = BoxIndex,
    task_index = TaskIndex
}) ->
    TaskIdentifier = #task_identifier{parallel_box_index = BoxIndex, task_index = TaskIndex},
    {TaskItems, TasksTreeWithoutKey} = case gb_trees:take_any(TaskIdentifier, TasksTree) of
        error -> {[], TasksTree};
        Other -> Other
    end,
    case TaskItems -- [ItemIndex] of
        [] -> TasksTreeWithoutKey;
        UpdatedTaskItems -> gb_trees:enter(TaskIdentifier, UpdatedTaskItems, TasksTreeWithoutKey)
    end.

-spec register_async_result_processing(jobs(), job_identifier(), workflow_cached_async_result:result_ref()) -> jobs().
register_async_result_processing(
    Jobs = #workflow_jobs{
        waiting = Waiting,
        ongoing = Ongoing,
        async_cached_results = Results
    },
    JobIdentifier,
    CachedResultId
) ->
    NewJobIdentifier = JobIdentifier#job_identifier{processing_type = ?ASYNC_RESULT_PROCESSING},
    Jobs#workflow_jobs{
        waiting = gb_sets:add(NewJobIdentifier, Waiting),
        ongoing = gb_sets:delete(JobIdentifier, Ongoing),
        async_cached_results = Results#{NewJobIdentifier => CachedResultId},
        results_iterator = undefined
    }.

-spec maybe_remove_async_cached_result(jobs(), job_identifier()) -> jobs().
maybe_remove_async_cached_result(#workflow_jobs{async_cached_results = Results} = Jobs,
    #job_identifier{processing_type = ?ASYNC_RESULT_PROCESSING} = JobIdentifier) ->
    Jobs#workflow_jobs{async_cached_results = maps:remove(JobIdentifier, Results)};
maybe_remove_async_cached_result(Jobs, _JobIdentifier) ->
    Jobs.

%%%===================================================================
%%% Test API
%%%===================================================================

-spec is_empty(jobs()) -> boolean().
is_empty(#workflow_jobs{
    ongoing = Ongoing,
    waiting = Waiting,
    failed_items = Failed,
    pending_async_jobs = AsyncCalls,
    raced_results = Raced,
    async_cached_results = AsyncCached
}) ->
    gb_sets:is_empty(Ongoing) andalso gb_sets:is_empty(Waiting) andalso sets:size(Failed) =:= 0 andalso
        maps:size(AsyncCalls) =:= 0 andalso maps:size(Raced) =:= 0 andalso maps:size(AsyncCached) =:= 0.


-spec get_results_in_processing_from_dump(dump()) ->
    #{workflow_execution_state:index() => {workflow_execution_state:index(), [workflow_execution_state:index()]}}.
get_results_in_processing_from_dump(#dump{waiting = WaitingList}) ->
    lists:foldl(fun
        (#job_identifier{
            processing_type = ?ASYNC_RESULT_PROCESSING,
            item_index = ItemIndex,
            parallel_box_index = BoxIndex,
            task_index = TaskIndex
        }, Acc) ->
            {BoxIndex, TaskIndexes} = maps:get(ItemIndex, Acc, {BoxIndex, []}),
            Acc#{ItemIndex => {BoxIndex, [TaskIndex | TaskIndexes]}};
        (_, Acc) ->
            Acc
    end, #{}, WaitingList).