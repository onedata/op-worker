%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This gen_server is responsible for controlling run of autocleaning
%%% with given configuration.
%%% It is started per specific run and dies after the run is finished.
%%% Only one run can be performed in the space at a time.
%%%
%%% It uses autocleaning_view_traverse to start traverse over file_popularity view
%%% to choose the least popular files (according to the popularity
%%% function, see file_popularity_view.erl) with given constraints.
%%% autocleaning_view_traverse jobs schedule replica_deletion requests.
%%%
%%% If request for deleting replica (see replica_deletion.erl) is granted a support from other
%%% provider, job for deleting the replica is scheduled on replica_deletion_worker pool.
%%% replica_deletion_worker processes call replica_deletion_behaviour callbacks
%%% to communicate with autocleaning_run_controller gen_server:
%%%   * replica_deletion_predicate/2 is called to determine whether the replica should
%%%     be deleted. This is essential as requesting replica_deletion_support might take
%%%     long time and it is possible that in the meantime storage occupancy has reached
%%%     the target level and therefore there is no need for deleting the replica.
%%%   * process_replica_deletion_result/4 is used to notify autocleaning_run_controller
%%%     that the replica has been deleted so that it can update its counters
%%%
%%% Run of autocleaning in given space can be started in 2 ways:
%%%  * by autocleaning_checker gen-server which is responsible for aggregating requests for
%%%    checking whether autocleaning should start in given space.
%%%    Requests are sent from space_quota model posthooks.
%%%  * by periodical checks triggered from fslogic_worker
%%%
%%% Before starting this gen_server, calling process must set
%%% corresponding AutocleaningRunId in #autocleaning.current_run.
%%% This mechanism allows to ensure that just one such gen_server is started per
%%% space in the cluster.
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_run_controller).
-author("Jakub Kudzia").

-behaviour(replica_deletion_behaviour).
-behaviour(gen_server).

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/autocleaning/autocleaning.hrl").
-include("modules/replica_deletion/replica_deletion.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    start/3,
    restart/3,
    cancel_cleaning/2,
    cancel_cleaning_sync/2,
    notify_files_to_process/5,
    notify_finished_traverse/2,
    notify_processed_file/3,
    notify_processed_files/4,
    pack_batch_id/2,
    unpack_batch_id/1,
    batch_no/2]).

%% Exported for RPC
-export([start_internal/4]).

%% replica_deletion_behaviour callbacks
-export([
    replica_deletion_predicate/2,
    process_replica_deletion_result/4
]).

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
    run_id :: run_id(),
    space_id :: od_space:id(),
    config :: autocleaning:config(),
    batch_size :: non_neg_integer(),

    files_to_process = 0 :: non_neg_integer(),
    files_processed = 0 :: non_neg_integer(),
    released_files = 0 :: non_neg_integer(),
    released_bytes = 0 :: non_neg_integer(),
    bytes_to_release = 0 :: non_neg_integer(),

    end_of_view_reached = false :: boolean(),
    run_cancelled = false :: boolean(),
    traverse_cancelled = false :: boolean(),

    next_batch_token :: undefined | view_traverse:token(),

    % map storing #batch_file_counters per each currently processed batch
    batches_counters = #{} :: #{batch_no() => batch_file_counters()},
    batches_tokens = #{} :: #{batch_no() => view_traverse:token()},
    % set of already finished batches, which tokens can be stored in the #autocleaning_run model
    finished_batches = ordsets:new() :: ordsets:ordset(batch_no()),
    % number of the last batch which token has already been persisted in the autocleaning model
    last_persisted_batch = undefined :: undefined | batch_no(),
    last_update_counters_timestamp = 0,

    report_terminate_to :: pid() | undefined
}).

% helper record that stores counters for each batch of files
-record(batch_file_counters, {
    files_processed = 0 :: non_neg_integer(),
    files_to_process = 0 :: non_neg_integer()
}).


% Generic message record
-record(message, {
    type :: message_type(),
    run_id :: run_id()
}).

% Message types
-define(STOP_CLEANING, stop_cleaning).
-define(TRAVERSE_FINISHED, traverse_finished).
-define(SHOULD_CONTINUE, should_continue).
-record(cancel_cleaning, {
    report_to :: pid() | undefined
}).
-record(file_released, {
    bytes :: non_neg_integer(),
    batch_no :: batch_no()
}).
-record(files_processed, {
    files_number :: non_neg_integer(),
    batch_no :: batch_no()
}).
-record(files_to_process, {
    files_number :: non_neg_integer(),
    batch_no :: batch_no(),
    token :: view_traverse:token()
}).


% Types
-type batch_id() :: binary().
-type run_id() :: autocleaning:run_id().
-type batch_no() :: non_neg_integer().
-type state() :: #state{}.
-type batch_file_counters() :: #batch_file_counters{}.
%% @formatter:off
-type message_type() :: #cancel_cleaning{} | ?STOP_CLEANING | ?TRAVERSE_FINISHED | ?SHOULD_CONTINUE |
                        #files_to_process{} | #file_released{} | #files_processed{}.
%% @formatter:on

-export_type([batch_id/0]).


% batch size used to query the file_popularity_view
-define(BATCH_SIZE, application:get_env(?APP_NAME, autocleaning_view_batch_size, 1000)).
-define(ID_SEPARATOR, <<"##">>).

-define(UPDATE_DOC_COUNTERS_MAX_INTERVAL, timer:seconds(1)).

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
    {ok, run_id()} | {error, term()}.
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
                    case start_service(SpaceId, ARId, AR, Config) of
                        ok ->
                            {ok, ARId};
                        aborted ->
                            ?error(
                                "Could not start autocleaning_run_controller in space ~p",
                                [SpaceId]
                            ),
                            ?ERROR_FILE_POPULARITY_DISABLED
                    end;
                OtherARId ->
                    % other auto-cleaning run is in progress
                    autocleaning_run:delete(ARId),
                    {error, {already_started, OtherARId}}
            end;
        Other ->
            Other
    end.

-spec restart(run_id(), od_space:id(), autocleaning:config()) ->
    {ok, run_id()} | ok.
restart(ARId, SpaceId, Config) ->
    case autocleaning_run:get(ARId) of
        {ok, #document{value = AR = #autocleaning_run{
            space_id = SpaceId,
            started_at = StartTime
        }}} ->
            case autocleaning_run:get_status(AR) of
                ?ACTIVE ->
                    % ensure that there is a link for given autocleaning_run
                    ok = autocleaning_run_links:add_link(ARId, SpaceId, StartTime),
                    ok = start_service(SpaceId, ARId, AR, Config),
                    ?debug("Restarted auto-cleaning run ~p in space ~p", [ARId, SpaceId]),
                    {ok, ARId};
                Status
                    when Status =:= ?COMPLETED
                    orelse Status =:= ?FAILED
                    orelse Status =:= ?CANCELLED
                ->
                    autocleaning:mark_run_finished(SpaceId);
                ?CANCELLING ->
                    ?warning("Could not restart auto-cleaning run ~p in space ~p beceause it is stalled in state ~p",
                        [ARId, SpaceId, ?CANCELLING]),
                    autocleaning_run:mark_finished(ARId)
            end;
        Error ->
            ?error("Could not restart auto-cleaning run ~p in space ~p due to ~p",
                [ARId, SpaceId, Error]),
            autocleaning:mark_run_finished(SpaceId)
    end.

-spec notify_files_to_process(od_space:id(), run_id(), non_neg_integer(), batch_no(), view_traverse:token()) -> ok.
notify_files_to_process(SpaceId, AutocleaningRunId, FilesNumber, BatchNo, Token) ->
    gen_server2:cast(?SERVER(SpaceId), #message{
        type = #files_to_process{
            files_number = FilesNumber,
            batch_no = BatchNo,
            token = Token
        },
        run_id = AutocleaningRunId
    }).

-spec notify_processed_file(od_space:id(), run_id(), batch_no()) -> ok.
notify_processed_file(SpaceId, AutocleaningRunId, BatchNo) ->
    notify_processed_files(SpaceId, AutocleaningRunId, 1, BatchNo).

-spec notify_processed_files(od_space:id(), run_id(), non_neg_integer(), batch_no()) -> ok.
notify_processed_files(SpaceId, AutocleaningRunId, FilesNumber, BatchNo) ->
    gen_server2:cast(?SERVER(SpaceId), #message{
        type = #files_processed{
            files_number = FilesNumber,
            batch_no = BatchNo
        },
        run_id = AutocleaningRunId
    }).

-spec notify_finished_traverse(od_space:id(), run_id()) -> ok.
notify_finished_traverse(SpaceId, AutocleaningRunId) ->
    gen_server2:cast(?SERVER(SpaceId), #message{
        type = ?TRAVERSE_FINISHED,
        run_id = AutocleaningRunId
    }).

-spec batch_no(non_neg_integer(), non_neg_integer()) -> batch_no().
batch_no(BatchOffset, BatchSize) ->
    BatchOffset div BatchSize.

-spec pack_batch_id(run_id(), batch_no()) -> batch_id().
pack_batch_id(AutocleaningRunId, Int) ->
    <<AutocleaningRunId/binary, ?ID_SEPARATOR/binary, (integer_to_binary(Int))/binary>>.

-spec unpack_batch_id(batch_id()) -> {run_id(), batch_no()}.
unpack_batch_id(BatchId) ->
    [AutocleaningRunId, BatchNo] = binary:split(BatchId, ?ID_SEPARATOR),
    {AutocleaningRunId, binary_to_integer(BatchNo)}.

%%%===================================================================
%%% Internal API
%%%===================================================================

-spec start_service(od_space:id(), autocleaning:run_id(), autocleaning_run:record(), autocleaning:config()) ->
    ok | aborted.
start_service(SpaceId, AutocleaningRunId, AutocleaningRun, AutocleaningConfig) ->
    ServiceOptions = #{
        start_function => start_internal,
        start_function_args => [SpaceId, AutocleaningRunId, AutocleaningRun, AutocleaningConfig],
        stop_function => cancel_cleaning_sync,
        stop_function_args => [SpaceId, AutocleaningRunId]
    },
    internal_services_manager:start_service(?MODULE, SpaceId, ServiceOptions).

-spec start_internal(od_space:id(), autocleaning:run_id(), autocleaning_run:record(), autocleaning:config()) ->
    ok | abort.
start_internal(SpaceId, AutocleaningRunId, AutocleaningRun, AutocleaningConfig) ->
    case gen_server2:start({global, {?SERVER, SpaceId}}, ?MODULE,
        [AutocleaningRunId, SpaceId, AutocleaningRun, AutocleaningConfig], []) of
        {ok, _} -> ok;
        _ -> abort
    end.

-spec cancel_cleaning(od_space:id(), run_id()) -> ok.
cancel_cleaning(SpaceId, AutocleaningRunId) ->
    gen_server2:cast(?SERVER(SpaceId), #message{
        type = #cancel_cleaning{},
        run_id = AutocleaningRunId
    }).

-spec cancel_cleaning_sync(od_space:id(), run_id()) -> ok.
cancel_cleaning_sync(SpaceId, AutocleaningRunId) ->
    gen_server2:cast(?SERVER(SpaceId), #message{
        type = #cancel_cleaning{report_to = self()},
        run_id = AutocleaningRunId
    }),
    wait_for_terminate(SpaceId).

-spec wait_for_terminate(od_space:id()) -> ok.
wait_for_terminate(SpaceId) ->
    receive
        {controller_terminated, SpaceId} -> ok
    after
        10000 ->
            case global:whereis_name(?SERVER(SpaceId)) of
                undefined ->
                    ok;
                Pid ->
                    case is_process_alive(Pid) of
                        false -> ok;
                        true -> wait_for_terminate(SpaceId)
                    end
            end
    end.

-spec notify_released_file(od_space:id(), run_id(), non_neg_integer(), batch_no()) -> ok.
notify_released_file(SpaceId, ARId, ReleasedBytes, BatchNo) ->
    gen_server2:cast(?SERVER(SpaceId), #message{
        type = #file_released{
            batch_no = BatchNo,
            bytes = ReleasedBytes
        },
        run_id = ARId
    }).

-spec notify_processed_file(od_space:id(), batch_id()) -> ok.
notify_processed_file(SpaceId, BatchId) ->
    {ARId, BatchNo} = unpack_batch_id(BatchId),
    notify_processed_file(SpaceId, ARId, BatchNo).

-spec should_continue(od_space:id(), batch_id()) -> boolean().
should_continue(SpaceId, BatchId) ->
    {AutocleaningRunId, _BatchNo} = unpack_batch_id(BatchId),
    gen_server2:call(?SERVER(SpaceId), #message{
        type = ?SHOULD_CONTINUE,
        run_id = AutocleaningRunId
    }, infinity).

-spec stop(od_space:id(), run_id()) -> ok.
stop(SpaceId, AutocleaningRunId) ->
    gen_server2:cast(?SERVER(SpaceId), #message{
        type = ?STOP_CLEANING,
        run_id = AutocleaningRunId
    }).

%%%===================================================================
%%% replica_deletion_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link replica_deletion_behaviour} callback replica_deletion_predicate/1.
%% @end
%%--------------------------------------------------------------------
-spec replica_deletion_predicate(od_space:id(), batch_id()) -> boolean().
replica_deletion_predicate(SpaceId, BatchId) ->
    try
        should_continue(SpaceId, BatchId)
    catch
        exit:{noproc, _} ->
            false
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link replica_deletion_behaviour} callback process_replica_deletion_result/4.
%% @end
%%--------------------------------------------------------------------
-spec process_replica_deletion_result(replica_deletion:result(), od_space:id(),
    file_meta:uuid(), batch_id()) -> ok.
process_replica_deletion_result({ok, ReleasedBytes}, SpaceId, _FileUuid, BatchId) ->
    {ARId, BatchNo} = unpack_batch_id(BatchId),
    notify_released_file(SpaceId, ARId, ReleasedBytes, BatchNo);
process_replica_deletion_result({error, precondition_not_satisfied}, SpaceId, _FileUuid, BatchId) ->
    notify_processed_file(SpaceId, BatchId);
process_replica_deletion_result({error, canceled}, SpaceId, _FileUuid, BatchId) ->
    notify_processed_file(SpaceId, BatchId);
process_replica_deletion_result({error, opened_file}, SpaceId, _FileUuid, BatchId) ->
    notify_processed_file(SpaceId, BatchId);
process_replica_deletion_result(Error, SpaceId, FileUuid, BatchId) ->
    {ARId, BatchNo} = unpack_batch_id(BatchId),
    {ok, #document{value = #file_meta{name = FileName}}} = file_meta:get_including_deleted(FileUuid),
    ?error("Error ~p occurred during auto-cleaning of file ~p in run ~p",
        [Error, {FileUuid, FileName}, ARId]),
    notify_processed_file(SpaceId, ARId, BatchNo).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> {ok, State :: #state{}} |{stop, Reason :: term()}.
init([ARId, SpaceId, AutocleaningRun, Config]) ->
    BytesToRelease = autocleaning_run:get_bytes_to_release(AutocleaningRun),
    AlreadyReleasedBytes = autocleaning_run:get_released_bytes(AutocleaningRun),
    AlreadyReleasedFiles = autocleaning_run:get_released_files(AutocleaningRun),
    NextBatchToken = autocleaning_run:get_view_traverse_token(AutocleaningRun),
    ACRules = autocleaning_config:get_rules(Config),
    BatchSize = ?BATCH_SIZE,
    case autocleaning_view_traverse:run(SpaceId, ARId, ACRules, BatchSize, NextBatchToken) of
        {ok, _} ->
            {ok, #state{
                run_id = ARId,
                space_id = SpaceId,
                config = Config,
                batch_size = BatchSize,
                bytes_to_release = BytesToRelease,
                released_bytes = AlreadyReleasedBytes,
                released_files = AlreadyReleasedFiles,
                next_batch_token = NextBatchToken
            }};
        {error, not_found} ->
            {stop, not_found}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}}.
handle_call(#message{type = ?SHOULD_CONTINUE, run_id = AutocleaningRunId}, _From,
    State = #state{
        run_id = AutocleaningRunId,
        released_bytes = ReleasedBytes,
        bytes_to_release = BytesToRelease,
        run_cancelled = RunCancelled
}) ->
    {reply, (ReleasedBytes < BytesToRelease) andalso not RunCancelled, State};
handle_call(#message{type = ?SHOULD_CONTINUE, run_id = _OtherAutocleaningRunId}, _From,
    State = #state{run_id = _AutocleaningRunId}
) ->
    % message is associated with an old autocleaning run
    {reply, false, State};
handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, wrong_request, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_cast(#message{type = MessageType, run_id = ARId}, State = #state{
    run_id = ARId,
    space_id = SpaceId,
    batches_counters = BatchesCounters
}) ->
    try
        handle_cast_internal(MessageType, State)
    catch
        E:R ->
            lists:foreach(fun(BatchNo) ->
                cancel_replica_deletion_request(SpaceId, ARId, BatchNo)
            end, maps:keys(BatchesCounters)),
            autocleaning_view_traverse:cancel(SpaceId, ARId),
            {stop, {error, {E, R}}}
    end;
handle_cast(#message{type = _MessageType, run_id = _OtherAutocleaningRunId},
    State = #state{run_id = _AutocleaningRunId}
) ->
    % message is associated with an old autocleaning run, it can be safely ignored
    {noreply, State};
handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}}.
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
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> ok.
terminate(Reason, #state{
    run_id = ARId,
    released_files = ReleasedFiles,
    released_bytes = ReleasedBytes,
    space_id = SpaceId,
    report_terminate_to = ReportTo
}) ->
    case Reason of
        normal ->
            ok;
        {error, Reason} ->
            ?error_stacktrace("autocleaning_run_controller of run ~p failed unexpectedly due to ~p",
                [ARId, Reason]
            )
    end,

    autocleaning_run:mark_finished(ARId, ReleasedFiles, ReleasedBytes),
    ok = internal_services_manager:report_service_stop(?MODULE, SpaceId, SpaceId),

    case ReportTo of
        undefined -> ok;
        _ -> ReportTo ! {controller_terminated, SpaceId}
    end,
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) -> {ok, NewState :: #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec handle_cast_internal(message_type(), state()) ->
    {noreply, state()} | {stop, normal | {error, term()}, state()}.
handle_cast_internal(#cancel_cleaning{report_to = ReportTo}, State = #state{run_id = ARId}) ->
    autocleaning_run:mark_cancelling(ARId),
    {noreply, process_updated_state(State#state{run_cancelled = true, report_terminate_to = ReportTo})};
handle_cast_internal(#files_to_process{
    files_number = FilesNumber,
    batch_no = BatchNo,
    token = Token
}, State = #state{
    files_to_process = FilesToProcess,
    batches_counters = BatchesCounters,
    batches_tokens = BatchesTokens
}) ->
    {noreply, State#state{
        files_to_process = FilesToProcess + FilesNumber,
        batches_counters = BatchesCounters#{BatchNo => init_batch_file_counters(FilesNumber)},
        batches_tokens = BatchesTokens#{BatchNo => Token}
    }};
handle_cast_internal(#file_released{
    bytes = ReleasedBytes,
    batch_no = BatchNo
}, State) ->
    State2 = mark_released_file(State, ReleasedBytes, BatchNo),
    State3 = process_updated_state(State2),
    {noreply, State3};
handle_cast_internal(#files_processed{
    files_number = FilesNumber,
    batch_no = BatchNo
}, State) ->
    State2 = mark_processed_files(State, FilesNumber, BatchNo),
    State3 = process_updated_state(State2),
    {noreply, State3};
handle_cast_internal(?TRAVERSE_FINISHED, State) ->
    State2 = State#state{end_of_view_reached = true},
    {noreply, process_updated_state(State2)};
handle_cast_internal(?STOP_CLEANING, State) ->
    {stop, normal, State};
handle_cast_internal(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.


-spec mark_released_file(state(), non_neg_integer(), batch_no()) -> state().
mark_released_file(State = #state{released_bytes = ReleasedBytes, released_files = ReleasedFiles},
    FileReleasedBytes, BatchNo
) ->
    State2 = State#state{
        released_bytes = ReleasedBytes + FileReleasedBytes,
        released_files = ReleasedFiles + 1
    },
    mark_processed_files(State2, 1, BatchNo).


-spec mark_processed_files(state(), non_neg_integer(), batch_no()) -> state().
mark_processed_files(State = #state{
    finished_batches = BatchesToPersist,
    files_processed = FilesProcessed,
    batches_counters = BatchesCounters
}, NewProcessedFiles, BatchNo) ->
    {NewBatchesCounters, NewBatchesToPersist} = case
        increment_and_check_equality(BatchesCounters, BatchNo, NewProcessedFiles)
    of
        {false, BatchesCounters2} ->
            {BatchesCounters2, BatchesToPersist};
        {true, BatchesCounters2} ->
            BatchesToPersist2 = ordsets:add_element(BatchNo, BatchesToPersist),
            {BatchesCounters2, BatchesToPersist2}
    end,
    State2 = maybe_update_doc_counters(State),
    State2#state{
        finished_batches = NewBatchesToPersist,
        batches_counters = NewBatchesCounters,
        files_processed = FilesProcessed + NewProcessedFiles
    }.


-spec process_updated_state(state()) -> state().
process_updated_state(State) ->
    State2 = maybe_stop_cleaning(State),
    persist_finished_batches(State2).

-spec maybe_stop_cleaning(state()) -> state().
maybe_stop_cleaning(State = #state{
    space_id = SpaceId,
    run_id = ARId,
    files_to_process = FilesToProcess,
    files_processed = FilesToProcess,
    end_of_view_reached = true
}) ->
    stop(SpaceId, ARId),
    State;
maybe_stop_cleaning(State = #state{
    run_id = ARId,
    space_id = SpaceId,
    bytes_to_release = BytesToRelease,
    released_bytes = ReleasedBytes,
    run_cancelled = RunCancelled,
    traverse_cancelled = false,
    batches_counters = BatchesCounters,
    end_of_view_reached = TraverseFinished
}) when (BytesToRelease =< ReleasedBytes) orelse RunCancelled ->
    case TraverseFinished of
        true ->
            ok;
        false ->
            autocleaning_view_traverse:cancel(SpaceId, ARId)
    end,
    lists:foreach(fun(BatchNo) ->
        cancel_replica_deletion_request(SpaceId, ARId, BatchNo)
    end, maps:keys(BatchesCounters)),
    State#state{traverse_cancelled = true};
maybe_stop_cleaning(State) ->
    State.



-spec maybe_update_doc_counters(state()) -> state().
maybe_update_doc_counters(State = #state{
    run_id = ARId,
    last_update_counters_timestamp = PreviousTimestamp,
    released_bytes = ReleasedBytes,
    released_files = ReleasedFiles
}) ->
    Timestamp = time_utils:system_time_millis(),
    case Timestamp - PreviousTimestamp > ?UPDATE_DOC_COUNTERS_MAX_INTERVAL of
        true ->
            autocleaning_run:update_counters(ARId, ReleasedFiles, ReleasedBytes),
            State#state{last_update_counters_timestamp = Timestamp};
        false ->
            State
    end.


-spec persist_finished_batches(state()) -> state().
persist_finished_batches(State = #state{
    finished_batches = []
}) ->
    State;
persist_finished_batches(State = #state{
    run_id = ARId,
    finished_batches = BatchesToPersist,
    last_persisted_batch = LastPersistedBatch,
    batches_tokens = BatchesTokens
}) ->
    {BatchNumToPersist, BatchesStripped, BatchesToPersist2} =
        strip_if_continuous(LastPersistedBatch, ordsets:to_list(BatchesToPersist)),

    case LastPersistedBatch =:= BatchNumToPersist of
        true ->
            % there isn't any new batch to persist
            State;
        false ->
            {BatchToken, BatchesTokens2} = maps:take(BatchNumToPersist, BatchesTokens),
            autocleaning_run:set_view_traverse_token(ARId, BatchToken),

            BatchesTokens3 = lists:foldl(fun(StrippedBatchNum, BatchesTokensIn) ->
                maps:remove(StrippedBatchNum, BatchesTokensIn)
            end, BatchesTokens2, BatchesStripped),
            State#state{
                finished_batches = BatchesToPersist2,
                last_persisted_batch = BatchNumToPersist,
                batches_tokens = BatchesTokens3
            }
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function increments files_processed counter for given BatchNo
%% and return information whether files_to_process is equal to updated
%% files_processed counter.
%% If counters are equal, #batch_file_counter record is removed from
%% BatchesCounters map.
%% @end
%%--------------------------------------------------------------------
-spec increment_and_check_equality(#{non_neg_integer() => batch_file_counters()},
    batch_no(), non_neg_integer()) -> {boolean(), #{non_neg_integer() => batch_file_counters()}}.
increment_and_check_equality(BatchesCounters, BatchNo, NewFilesProcessed) ->
    BatchFileCounters = maps:get(BatchNo, BatchesCounters),
    {AreCountersEqual, BatchFileCounters2} = increment_and_check_equality(BatchFileCounters, NewFilesProcessed),
    BatchesCounters2 = case AreCountersEqual of
        true ->
            maps:remove(BatchNo, BatchesCounters);
        false ->
            BatchesCounters#{BatchNo => BatchFileCounters2}
    end,
    {AreCountersEqual, BatchesCounters2}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function increments files_processed counter and return information
%% whether files_to_process is equal to updated files_processed counter.
%% @end
%%--------------------------------------------------------------------
-spec increment_and_check_equality(batch_file_counters(), non_neg_integer()) -> {boolean(), batch_file_counters()}.
increment_and_check_equality(BFC = #batch_file_counters{
    files_to_process = FilesToProcess,
    files_processed = FilesProcessed
}, NewFilesProcessed) ->
    FilesProcessed2 = FilesProcessed + NewFilesProcessed,
    {FilesProcessed2 == FilesToProcess, BFC#batch_file_counters{files_processed = FilesProcessed2}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function strips an increasing, "continuous" subsequence from
%% the beginning of list L.
%% Stripped subsequence must start from M = N + 1.
%% "Continuous" in this context means that for each
%% term X_{n} = X_{n-1} + 1.
%% The function returns a triple where:
%%  - the 1st element is the last element of stripped subsequence
%%    or N if nothing was stripped,
%%  - the 2nd element is stripped subsequence,
%%  - the 3rd element is remaining part of list L.
%% WARNING!!!
%% This function assumes that L is an increasing sequence of
%% integers.
%% @end
%%--------------------------------------------------------------------
-spec strip_if_continuous(undefined | non_neg_integer(), [non_neg_integer()]) ->
    {undefined | non_neg_integer(), [non_neg_integer()], [non_neg_integer()]}.
strip_if_continuous(N, L) ->
    strip_if_continuous(N, [], L).

-spec strip_if_continuous(undefined | non_neg_integer(), [non_neg_integer()], [non_neg_integer()]) ->
    {undefined | non_neg_integer(), [non_neg_integer()], [non_neg_integer()]}.
strip_if_continuous(undefined, [], [0 | R]) ->
    strip_if_continuous(0, [0], R);
strip_if_continuous(N, StrippedReversed, [H | R]) when N + 1 == H ->
    strip_if_continuous(H, [H | StrippedReversed], R);
strip_if_continuous(N, StrippedReversed, L) ->
    {N, lists:reverse(StrippedReversed), L}.


-spec init_batch_file_counters(non_neg_integer()) -> batch_file_counters().
init_batch_file_counters(FilesToProcess) ->
    #batch_file_counters{
        files_to_process = FilesToProcess,
        files_processed = 0
    }.

-spec cancel_replica_deletion_request(od_space:id(), run_id(), batch_no()) -> ok.
cancel_replica_deletion_request(SpaceId, AutocleaningRunId, BatchNo) ->
    replica_deletion_master:cancel_request(SpaceId, pack_batch_id(AutocleaningRunId, BatchNo), ?AUTOCLEANING_JOB).
