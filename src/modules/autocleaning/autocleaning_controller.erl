%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for performing space cleanup with given
%%% configuration. It deletes files returned from file_popularity_view,
%%% with given constraints, until configured target level of storage
%%% occupancy is reached.
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_controller).
-author("Jakub Kudzia").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").



%% API
-export([start/3, stop_cleaning/1, process_replica_deletion_result/4, check/1,
    notify_processed_file/2, restart/3]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).
-define(SERVER(SpaceId), {global, {?SERVER, SpaceId}}).

-record(state, {
    autocleaning_run_id :: autocleaning_run:id(),
    space_id :: od_space:id(),
    config :: autocleaning:config(),

    files_to_process = 0 :: non_neg_integer(),
    files_processed = 0 :: non_neg_integer(),
    released_bytes = 0 :: non_neg_integer(),
    bytes_to_release = 0 :: non_neg_integer(),

    % queue for caching least popular files that where queried from the file_popularity_view
    queue = queue:new() :: queue:queue(file_ctx:ctx()),
    % erlang's queue:len/1  is O(n) so will keep track of the queue length ourselves
    queue_len = 0 :: non_neg_integer(),

    end_of_view_reached = false :: boolean(),
    cleaning_stopped = false :: boolean(),
    scheduled_cancelling = false :: boolean(),

    next_batch_token :: undefined | file_popularity_view:index_token(),

    batches_counter = 0 :: non_neg_integer(),

    % map storing #batch_counters per each started batch
    batch_counters_map = #{} :: maps:map(non_neg_integer(), batch_counters()),
    batches_tokens = #{} :: maps:map(),
    % set of already finished batches, which tokens can be stored in the #autocleaning_run model
    finished_batches = ordsets:new(),
    % number of the last batch which token was has already been persisted in the autocleaning model
    last_persisted_batch = 0 :: non_neg_integer()
}).

-record(batch_counters, {
    files_processed = 0 :: non_neg_integer(),
    files_to_process = 0 :: non_neg_integer()
}).

-type batch_id() :: binary().
-type run_id() :: autocleaning_run:id().
-type batch_num() :: non_neg_integer().
-type state() :: #state{}.
-type batch_counters() :: #batch_counters{}.

-export_type([batch_id/0]).

% Messages
-define(START_CLEANING, start_cleaning).

-define(CLEANUP_BATCH, cleanup_batch).
-define(CLEANUP_BATCH(StartDocId), {cleanup_batch, StartDocId}).

-define(FILE_RELEASED, file_released).
-define(FILE_RELEASED(Bytes, BatchNum), {?FILE_RELEASED, Bytes, BatchNum}).

-define(FILE_PROCESSED, processed_file).
-define(FILE_PROCESSED(BatchNum), {?FILE_PROCESSED, BatchNum}).

-define(STOP_CLEANING, stop_cleaning).

-define(NAME(SpaceId), {global, {?SERVER, SpaceId}}).

% batch size used to query the file_popularity_view
-define(VIEW_BATCH_SIZE, application:get_env(?APP_NAME,
    autocleaning_view_batch_size, 1000)).

% max number of active tasks being simultaneously processed by the
% replica_deletion_master process
% if more tasks are added, replica_deletion_master's queues them in the memory
-define(REPLICA_DELETION_MAX_ACTIVE_TASKS,
    application:get_env(?APP_NAME, max_active_deletion_tasks, 2000)).

% ratio used to calculate the ?NEXT_BATCH_THRESHOLD which defines
% when autocleaning_controller should schedule next batch of file replicas to
% be deleted to replica_deletion_master
% calculated threshold is proportional to ?REPLICA_DELETION_MAX_ACTIVE_TASKS
-define(AUTOCLEANING_CONTROLLER_NEXT_BATCH_RATIO,
    application:get_env(?APP_NAME, autocleaning_controller_next_batch_ratio, 1.5)).

% if number of files currently processed (which is basically equal to files_to_process - files_processed)
% by replica_deletion_master is less than ?NEXT_BATCH_THRESHOLD, autocleaning_controller
% will schedule deletion of ?DELETION_BATCH_SIZE number of replicas
-define(NEXT_BATCH_THRESHOLD,
    ?AUTOCLEANING_CONTROLLER_NEXT_BATCH_RATIO * ?REPLICA_DELETION_MAX_ACTIVE_TASKS
).

% number of file replicas which will be scheduled for deletion in one batch
-define(DELETION_BATCH_SIZE,
    application:get_env(?APP_NAME, autocleaning_deletion_batch_size, ?REPLICA_DELETION_MAX_ACTIVE_TASKS)).

-define(ID_SEPARATOR, <<"##">>).


-define(run_and_catch_errors(Fun, State),
    try
        Fun()
    catch
        Error:Reason ->
            ?error_stacktrace("autocleaning_controller of run ~p failed unexpectedly due to ~p:~p",
                [State#state.autocleaning_run_id, Error, Reason]
            ),
            autocleaning_run:mark_failed(State#state.autocleaning_run_id),
            {stop, {Error, Reason}, State}
    end
).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Tries to start process responsible for performing auto-cleaning.
%% Returns error if other auto-cleaning run is in progress.
%% @end
%%-------------------------------------------------------------------
-spec start(od_space:id(), autocleaning:config(), non_neg_integer()) ->
    {ok, autocleaning_run:id()} | {error, term()}.
start(SpaceId, Config, CurrentSize) ->
    Target = autocleaning_config:get_target(Config),
    BytesToRelease = CurrentSize - Target,
    case autocleaning_run:create(SpaceId, BytesToRelease) of
        {ok, #document{key = ARId, value = AR}} ->
            {ok, ACDoc} = autocleaning:maybe_mark_current_run(SpaceId, ARId),
            case autocleaning:get_current_run(ACDoc) of
                ARId ->
                    StartTime = autocleaning_run:get_started_at(AR),
                    ok = autocleaning_run_links:add_link(ARId, SpaceId, StartTime),
                    {ok, _Pid} = gen_server2:start({global, {?SERVER, SpaceId}}, ?MODULE,
                        [ARId, SpaceId, AR, Config], []),
                    {ok, ARId};
                OtherARId ->
                    % other auto-cleaning run is in progress
                    autocleaning_run:delete(ARId),
                    {error, {already_started, OtherARId}}
            end;
        Other ->
            Other
    end.

-spec restart(autocleaning_run:id(), od_space:id(), autocleaning:config()) ->
    {ok, autocleaning:run_id()} | ok.
restart(ARId, SpaceId, Config) ->
    case autocleaning_run:get(ARId) of
        {ok, #document{value = AR = #autocleaning_run{
            space_id = SpaceId,
            started_at = StartTime
        }}} ->
            case autocleaning_run:is_finished(AR) of
                false ->
                    % ensure that there is a link for given autocleaning_run
                    ok = autocleaning_run_links:add_link(ARId, SpaceId, StartTime),
                    {ok, _Pid} = gen_server2:start({global, {?SERVER, SpaceId}}, ?MODULE,
                        [ARId, SpaceId, AR, Config], []),
                    ?debug("Restarted auto-cleaning run ~p in space ~p", [ARId, SpaceId]),
                    {ok, ARId};
                true ->
                    ?error("Could not restart auto-cleaning run ~p in space ~p beceause it is already finished",
                        [ARId, SpaceId]),
                    autocleaning:mark_run_finished(SpaceId)
            end;
        Error ->
            ?error("Could not restart auto-cleaning run ~p in space ~p due to ~p",
                [ARId, SpaceId, Error]),
            autocleaning:mark_run_finished(SpaceId)
    end.

-spec stop_cleaning(od_space:id()) -> ok.
stop_cleaning(SpaceId) ->
    gen_server2:cast(?SERVER(SpaceId), ?STOP_CLEANING).

-spec notify_processed_file(od_space:id(), batch_id()) -> ok.
notify_processed_file(SpaceId, BatchId) ->
    {_ARId, BatchNum} = from_batch_id(BatchId),
    gen_server2:cast(?SERVER(SpaceId), ?FILE_PROCESSED(BatchNum)).

%%%===================================================================
%%% Internal API
%%%===================================================================

-spec start_cleaning(od_space:id()) -> ok.
start_cleaning(SpaceId) ->
    gen_server2:cast(?SERVER(SpaceId), ?START_CLEANING).

-spec notify_released_file(od_space:id(), non_neg_integer(), batch_num()) -> ok.
notify_released_file(SpaceId, ReleasedBytes, BatchNum) ->
    gen_server2:cast(?SERVER(SpaceId), ?FILE_RELEASED(ReleasedBytes, BatchNum)).

%%-------------------------------------------------------------------
%% @doc
%% This function is used for debugging.
%% @end
%%-------------------------------------------------------------------
-spec check(od_space:id()) -> any().
check(SpaceId) ->
    gen_server2:cast(?SERVER(SpaceId), check).

%%-------------------------------------------------------------------
%% @doc
%% Posthook executed by replica_deletion_worker after deleting file
%% replica.
%% @end
%%-------------------------------------------------------------------
-spec process_replica_deletion_result(replica_deletion:result(), od_space:id(),
    file_meta:uuid(), autocleaning_run:id()) -> ok.
process_replica_deletion_result({ok, ReleasedBytes}, SpaceId, FileUuid, ReportId) ->
    {ARId, BatchNum} = from_batch_id(ReportId),
    ?debug("Auto-cleaning of file ~p in run ~p released ~p bytes.",
        [FileUuid, ARId, ReleasedBytes]),
    autocleaning_run:mark_released_file(ARId, ReleasedBytes),
    notify_released_file(SpaceId, ReleasedBytes, BatchNum);
process_replica_deletion_result(Error, SpaceId, FileUuid, ReportId) ->
    {ARId, _BatchNum} = from_batch_id(ReportId),
    ?error("Error ~p occured during auto-cleanig of file ~p in run ~p",
        [Error, FileUuid, ARId]),
    notify_processed_file(SpaceId,ReportId).

-spec to_batch_id(autocleaning_run:id(), non_neg_integer()) -> batch_id().
to_batch_id(ARId, Int) ->
    <<ARId/binary, ?ID_SEPARATOR/binary, (integer_to_binary(Int))/binary>>.

-spec from_batch_id(batch_id()) -> {autocleaning_run:id(), non_neg_integer()}.
from_batch_id(BatchId) ->
    [ARId, BatchNum] = binary:split(BatchId, ?ID_SEPARATOR),
    {ARId, binary_to_integer(BatchNum)}.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([ARId, SpaceId, AutocleaningRun, Config]) ->
    BytesToRelease = autocleaning_run:get_bytes_to_release(AutocleaningRun),
    AlreadyReleasedBytes = autocleaning_run:get_released_bytes(AutocleaningRun),
    NextBatchToken = autocleaning_run:get_index_token(AutocleaningRun),
    start_cleaning(SpaceId),
    {ok, #state{
        autocleaning_run_id = ARId,
        space_id = SpaceId,
        config = Config,
        bytes_to_release = BytesToRelease - AlreadyReleasedBytes,
        next_batch_token = NextBatchToken
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, wrong_request, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(check, State = #state{
    files_to_process = FTP,
    files_processed = FP,
    queue_len = QLen
}) ->
    % todo remove this test log after resolving VFS-5128
    % todo this log will not appear on production as it can be triggered only from console
    ?critical(
    "~nFiles to process: ~p~n"
    "Files processed: ~p~n"
    "Queue length: ~p~n", [FTP, FP, QLen]),
    {noreply, State};
handle_cast(?START_CLEANING, State = #state{
    autocleaning_run_id = ARId,
    space_id = SpaceId
}) ->
    ?run_and_catch_errors(fun() ->
        ?debug("Auto-cleaning run ~p of space ~p started", [ARId, SpaceId]),
        State2 = start_cleaning_internal(State),
        State3 = process_updated_state(State2),
        {noreply, State3}
    end, State);
handle_cast(?FILE_RELEASED(ReleasedBytes, BatchNum), State) ->
    ?run_and_catch_errors(fun() ->
        State2 = mark_released_file(ReleasedBytes, BatchNum, State),
        State3 = process_updated_state(State2),
        {noreply, State3}
    end, State);
handle_cast(?FILE_PROCESSED(BatchNum), State) ->
    ?run_and_catch_errors(fun() ->
        State2 = mark_processed_file(BatchNum, State),
        State3 = process_updated_state(State2),
        {noreply, State3}
    end, State);
handle_cast(?STOP_CLEANING, State = #state{
    autocleaning_run_id = ARId,
    space_id = SpaceId
}) ->
    ?run_and_catch_errors(fun() ->
        autocleaning_run:mark_completed(ARId),
        replica_deletion_master:cancelling_finished(ARId, SpaceId),
        ?debug("Auto-cleaning run ~p of space ~p finished", [ARId, SpaceId]),
        {stop, normal, State}
    end, State);
handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info(_Info, State) ->
    ?log_bad_request(_Info),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec start_cleaning_internal(state()) -> state().
start_cleaning_internal(State) ->
    State2 = maybe_refill_queue(State, ?REPLICA_DELETION_MAX_ACTIVE_TASKS),
    maybe_schedule_cleaning(State2).


-spec mark_released_file(non_neg_integer(), batch_num(), state()) -> state().
mark_released_file(FileReleasedBytes, BatchNum, State = #state{
    finished_batches = BatchesToPersist,
    files_processed = FilesProcessed,
    released_bytes = ReleasedBytes,
    batch_counters_map = BatchesCounters
}) ->
    {NewBatchesCounters, NewBatchesToPersist} = case
        increment_and_check_equality(BatchesCounters, BatchNum)
    of
        {false, BatchesCounters2} ->
            {BatchesCounters2, BatchesToPersist};
        {true, BatchesCounters2} ->
            BatchesToPersist2 = ordsets:add_element(BatchNum, BatchesToPersist),
            {BatchesCounters2, BatchesToPersist2}
    end,
    State#state{
        finished_batches = NewBatchesToPersist,
        batch_counters_map = NewBatchesCounters,
        files_processed = FilesProcessed + 1,
        released_bytes = ReleasedBytes + FileReleasedBytes
    }.


-spec mark_processed_file(batch_num(), state()) -> state().
mark_processed_file(BatchNum, State = #state{
    finished_batches = BatchesToPersist,
    files_processed = FilesProcessed,
    batch_counters_map = BatchesCounters
}) ->
    {NewBatchesCounters, NewBatchesToPersist} = case
        increment_and_check_equality(BatchesCounters, BatchNum)
    of
        {false, BatchesCounters2} ->
            {BatchesCounters2, BatchesToPersist};
        {true, BatchesCounters2} ->
            BatchesToPersist2 = ordsets:add_element(BatchNum, BatchesToPersist),
            {BatchesCounters2, BatchesToPersist2}
    end,
    State#state{
        finished_batches = NewBatchesToPersist,
        batch_counters_map = NewBatchesCounters,
        files_processed = FilesProcessed + 1
    }.


-spec process_updated_state(state()) -> state().
process_updated_state(State) ->
    State2 = maybe_stop_cleaning(State),
    State3 = persist_finished_batches(State2),
    State4 = maybe_refill_queue(State3, ?REPLICA_DELETION_MAX_ACTIVE_TASKS),
    maybe_schedule_cleaning(State4).


-spec maybe_stop_cleaning(state()) -> state().
maybe_stop_cleaning(State = #state{
    space_id = SpaceId,
    files_to_process = FilesToProcess,
    files_processed = FilesToProcess
}) ->
    stop_cleaning(SpaceId),
    State#state{
        cleaning_stopped = true
    };
maybe_stop_cleaning(State = #state{
    autocleaning_run_id = ARId,
    space_id = SpaceId,
    bytes_to_release = BytesToRelease,
    released_bytes = ReleasedBytes,
    scheduled_cancelling = false,
    batch_counters_map = BatchesCounters
}) when BytesToRelease =< ReleasedBytes ->

    lists:foreach(fun(BatchNum) ->
        replica_deletion_master:cancel(to_batch_id(ARId, BatchNum), SpaceId)
    end, maps:keys(BatchesCounters)),

    State#state{
        cleaning_stopped = true,
        scheduled_cancelling = true
    };
maybe_stop_cleaning(State) ->
    State.


-spec persist_finished_batches(state()) -> state().
persist_finished_batches(State = #state{
    finished_batches = []
}) ->
    State;
persist_finished_batches(State = #state{
    autocleaning_run_id = ARId,
    finished_batches = BatchesToPersist,
    last_persisted_batch = LastPersistedBatch,
    batches_tokens = BatchesTokens
}) ->
    {BatchNumToPersist, BatchesStripped, BatchesToPersist2} =
        strip_if_continuous(LastPersistedBatch, ordsets:to_list(BatchesToPersist)),
    {BatchToken, BatchesTokens2} = maps:take(BatchNumToPersist, BatchesTokens),
    autocleaning_run:set_index_token(ARId, BatchToken),

    BatchesTokens3 = lists:foldl(fun(StrippedBatchNum, BatchesTokensIn) ->
        maps:remove(StrippedBatchNum, BatchesTokensIn)
    end, BatchesTokens2, BatchesStripped),
    State#state{
        finished_batches = BatchesToPersist2,
        last_persisted_batch = BatchNumToPersist,
        batches_tokens = BatchesTokens3
    }.


-spec maybe_refill_queue(state(), non_neg_integer()) -> state().
maybe_refill_queue(State = #state{end_of_view_reached = true}, _MinLength) ->
    State;
maybe_refill_queue(State = #state{cleaning_stopped = true}, _MinLength) ->
    State;
maybe_refill_queue(State = #state{queue_len = QLen}, MinLength) when QLen < MinLength ->
    State2 = refill_queue_with_one_batch(State),
    maybe_refill_queue(State2, MinLength);
maybe_refill_queue(State = #state{}, _MinLength) ->
    State.


-spec maybe_schedule_cleaning(state()) -> state().
maybe_schedule_cleaning(State = #state{
    cleaning_stopped = false,
    space_id = SpaceId,
    autocleaning_run_id = ARId,
    files_to_process = FilesToProcess,
    files_processed = FilesProcessed,
    queue_len = QLen,
    queue = Queue
}) when QLen > 0 ->
    case (FilesToProcess - FilesProcessed) < ?NEXT_BATCH_THRESHOLD of
        true ->
            QueueList = queue:to_list(Queue),
            ToSchedule = lists:sublist(QueueList, ?DELETION_BATCH_SIZE),
            ToScheduleLen = length(ToSchedule),
            Queue2 = queue:from_list(lists:nthtail(ToScheduleLen, QueueList)),
            ScheduledNum = schedule_file_replicas_deletion(ToSchedule, ARId, SpaceId),
            State#state{
                files_to_process = FilesToProcess + ScheduledNum,
                queue = Queue2,
                queue_len = QLen - ToScheduleLen
            };
        false ->
            State
    end;
maybe_schedule_cleaning(State) ->
    State.


-spec refill_queue_with_one_batch(state()) -> state().
refill_queue_with_one_batch(State = #state{
    space_id = SpaceId,
    config = #autocleaning_config{rules = ACRules},
    next_batch_token = NextBatchToken,
    batches_counter = BatchesCounter,
    batches_tokens = BatchStartTokens,
    batch_counters_map = BatchesFileCounters,
    queue_len = QLen,
    queue = Queue
}) ->
    BatchesCounter2 = BatchesCounter + 1,
    case query(SpaceId, ?VIEW_BATCH_SIZE, NextBatchToken) of
        {[], NextBatchToken} ->
            State#state{
                end_of_view_reached = true
            };
        {PreselectedFileIds, NewNextBatchToken} ->
            FilteredFiles = filter(PreselectedFileIds, ACRules),
            NewFilesToProcess = length(FilteredFiles),
            FilesWithBatchNum = [{F, BatchesCounter2} || F <- FilteredFiles],
            State#state{
                batches_counter = BatchesCounter2,
                next_batch_token = NewNextBatchToken,
                batches_tokens = BatchStartTokens#{BatchesCounter2 => NextBatchToken},
                batch_counters_map = BatchesFileCounters#{BatchesCounter2 => new_batch_counters(NewFilesToProcess)},
                queue_len = QLen + NewFilesToProcess,
                queue = queue:join(Queue, queue:from_list(FilesWithBatchNum))
            }
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Filters files returned from the file-popularity view.
%% Files that do not satisfy auto-cleaning rules are removed from the list.
%% @end
%%-------------------------------------------------------------------
-spec filter([cdmi_id:objectid()], autocleaning_config:rules()) -> [file_ctx:ctx()].
filter(PreselectedFiles, ACRules) ->
    lists:filtermap(fun(FileId) ->
        Guid = cdmi_id:objectid_to_guid(FileId),
        FileCtx = file_ctx:new_by_guid(Guid),
        try
            case autocleaning_rules:are_all_rules_satisfied(FileCtx, ACRules) of
                true -> {true, FileCtx};
                false -> false
            end
        catch
            Error:Reason ->
                Uuid = file_ctx:get_uuid_const(FileCtx),
                SpaceId = file_ctx:get_space_id_const(FileCtx),
                ?error_stacktrace("Filtering preselected file with uuid ~p in space ~p failed due to ~p:~p",
                    [Uuid, SpaceId, Error, Reason]),
                false
        end
    end, PreselectedFiles).

%%-------------------------------------------------------------------
%% @doc
%% Schedules deletion of the replicas of given files.
%% Returns number of schedule deletions.
%% @end
%%-------------------------------------------------------------------
-spec schedule_file_replicas_deletion([{file_ctx:ctx(), non_neg_integer()}],
    run_id(), od_space:id()) -> non_neg_integer().
schedule_file_replicas_deletion(FilesToCleanAndBatchNum, ARId, SpaceId) ->
    lists:foldl(fun({FileCtx, BatchNum}, ScheduledNumIn) ->
        case replica_deletion_master:get_setting_for_deletion_task(FileCtx) of
            undefined ->
                ScheduledNumIn;
            {FileUuid, Provider, Blocks, VV} ->
                BatchId = to_batch_id(ARId, BatchNum),
                schedule_replica_deletion_task(FileUuid, Provider, Blocks, VV, BatchId, SpaceId),
                ScheduledNumIn + 1
        end
    end, 0, FilesToCleanAndBatchNum).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Adds task of replica deletion to replica_deletion_master queue.
%% @end
%%-------------------------------------------------------------------
-spec schedule_replica_deletion_task(file_meta:uuid(), od_provider:id(),
    fslogic_blocks:blocks(), version_vector:version_vector(), batch_id(),
    od_space:id()) -> ok.
schedule_replica_deletion_task(FileUuid, Provider, Blocks, VV, BatchId, SpaceId) ->
    replica_deletion_master:enqueue_task(FileUuid, Provider, Blocks, VV,
        BatchId, autocleaning, SpaceId).


-spec increment_and_check_equality(maps:map(non_neg_integer(), batch_counters()),
    non_neg_integer()) -> {boolean(), maps:map(non_neg_integer(), batch_counters())}.
increment_and_check_equality(BatchesCounters, BatchNum) ->
    BatchCounters = maps:get(BatchNum, BatchesCounters),
    case increment_and_check_equality(BatchCounters) of
        true ->
            {true, maps:remove(BatchNum, BatchesCounters)};
        {false, BatchFileCounters2} ->
            {false, BatchesCounters#{BatchNum => BatchFileCounters2}}
    end.


-spec increment_and_check_equality(batch_counters()) -> true | {false, batch_counters()}.
increment_and_check_equality(#batch_counters{
    files_to_process = FilesToProcess,
    files_processed = FilesProcessed
}) when FilesToProcess == (FilesProcessed + 1) ->
    true;
increment_and_check_equality(BC = #batch_counters{files_processed = FilesProcessed}) ->
    {false, BC#batch_counters{files_processed = FilesProcessed}}.


-spec strip_if_continuous(non_neg_integer(), [non_neg_integer()]) ->
    {non_neg_integer(), [non_neg_integer()], [non_neg_integer()]}.
strip_if_continuous(N, L) ->
    strip_if_continuous(N, [], L).

-spec strip_if_continuous(non_neg_integer(), [non_neg_integer()], [non_neg_integer()]) ->
    {non_neg_integer(), [non_neg_integer()], [non_neg_integer()]}.
strip_if_continuous(N, StrippedReversed, []) ->
    {N, StrippedReversed, []};
strip_if_continuous(N, StrippedReversed, [H | R]) when N + 1 == H ->
    strip_if_continuous(H, [H | StrippedReversed], R);
strip_if_continuous(N, StrippedReversed, L) ->
    {N, StrippedReversed, L}.


-spec new_batch_counters(non_neg_integer()) -> maps:map().
new_batch_counters(FilesToProcess) ->
    #batch_counters{
        files_to_process = FilesToProcess,
        files_processed = 0
    }.


-spec query(od_space:id(), non_neg_integer(), file_popularity_view:index_token()) ->
    {[cdmi_id:objectid()], file_popularity_view:index_token() | undefined}.
query(SpaceId, BatchSize, IndexToken) ->
    file_popularity_api:query(SpaceId, IndexToken, BatchSize).