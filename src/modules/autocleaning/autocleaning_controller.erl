%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This gen_server is responsible for controlling run of autocleaning
%%% with given configuration.
%%% It uses autocleaning_view_traverse to start traverse over file_popularity view
%%% to choose the least popular files (according to the popularity
%%% function, see file_popularity_view)
%%% returned from file_popularity_view, with given constraints.
%%% It schedules replica_deletion requests for the files until
%%% configured target level of storage occupancy is reached.
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_controller).
-author("Jakub Kudzia").

-behaviour(replica_deletion_behaviour).
-behaviour(gen_server).

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    start/3,
    restart/3,
    stop/2,
    check/1, % todo usunac
    notify_files_to_process/4,
    notify_finished_traverse/2,
    notify_processed_file/3,
    pack_batch_id/2,
    unpack_batch_id/1
]).

%% replica_deletion_behaviour callbacks
-export([
    process_replica_deletion_result/4,
    replica_deletion_predicate/1
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
    run_id :: batch_id(),
    space_id :: od_space:id(),
    config :: autocleaning:config(),
    batch_size :: non_neg_integer(),

    files_to_process = 0 :: non_neg_integer(),
    files_processed = 0 :: non_neg_integer(),
    released_bytes = 0 :: non_neg_integer(),
    bytes_to_release = 0 :: non_neg_integer(),

    end_of_view_reached = false :: boolean(),
    scheduled_cancelling = false :: boolean(),

    next_batch_token :: undefined | view_traverse:token(),

    % map storing #batch_file_counters per each started batch
    batches_counters = #{} :: #{batch_no() => batch_file_counters()},
    batches_tokens = #{} :: #{batch_no() => view_traverse:token()},
    % set of already finished batches, which tokens can be stored in the #autocleaning_run model
    finished_batches = ordsets:new() :: ordsets:ordset(batch_no()),
    % number of the last batch which token has already been persisted in the autocleaning model
    last_persisted_batch = 0 :: batch_no()
}).

% helper record that stores counters for each batch of files
-record(batch_file_counters, {
    files_processed = 0 :: non_neg_integer(),
    files_to_process = 0 :: non_neg_integer()
}).


-record(message, {
    content :: term(),
    run_id :: batch_id()
}).

% Generic message
-define(MESSAGE(Content, AutocleaningRunId), #message{content = Content, run_id = AutocleaningRunId}).

% Message contents
-define(START_CLEANING, start_cleaning).
-define(START_CLEANING_MSG(AutocleaningRunId), ?MESSAGE(?START_CLEANING, AutocleaningRunId)).

-define(FILE_RELEASED(Bytes, BatchNo), {file_released, Bytes, BatchNo}).
-define(FILE_RELEASED_MSG(AutocleaningRunId, Bytes, BatchNo), ?MESSAGE(?FILE_RELEASED(Bytes, BatchNo), AutocleaningRunId)).

-define(FILE_PROCESSED(BatchNo), {processed_file, BatchNo}).
-define(FILE_PROCESSED_MSG(AutocleaningRunId, BatchNo), ?MESSAGE(?FILE_PROCESSED(BatchNo), AutocleaningRunId)).

-define(FILES_TO_PROCESS(FilesNumber, Token), {files_to_process, FilesNumber, Token}).
-define(FILES_TO_PROCESS_MSG(AutocleaningRunId, FilesNumber, Token), ?MESSAGE(?FILES_TO_PROCESS(FilesNumber, Token), AutocleaningRunId)).

-define(STOP_CLEANING, stop_cleaning).
-define(STOP_CLEANING_MSG(AutocleaningRunId), ?MESSAGE(?STOP_CLEANING, AutocleaningRunId)).

-define(TRAVERSE_FINISHED, traverse_finished).
-define(TRAVERSE_FINISHED_MSG(AutocleaningRunId), ?MESSAGE(?TRAVERSE_FINISHED, AutocleaningRunId)).

-type batch_id() :: binary().
-type run_id() :: autocleaning:run_id().
-type batch_no() :: non_neg_integer().
-type state() :: #state{}.
-type batch_file_counters() :: #batch_file_counters{}.
-type message_content() :: ?START_CLEANING | ?FILE_RELEASED(_, _) | ?FILE_PROCESSED(_) | ?FILES_TO_PROCESS(_, _) |
    ?STOP_CLEANING | ?TRAVERSE_FINISHED.
-export_type([batch_id/0]).


% batch size used to query the file_popularity_view
-define(BATCH_SIZE, application:get_env(?APP_NAME, autocleaning_view_batch_size, 1000)).
-define(ID_SEPARATOR, <<"##">>).

-define(run_and_catch_errors(Fun, State),
    try
        Fun()
    catch
        Error:Reason ->
            ?error_stacktrace("autocleaning_controller of run ~p failed unexpectedly due to ~p:~p",
                [State#state.run_id, Error, Reason]
            ),
            autocleaning_run:mark_failed(State#state.run_id),
            {stop, {Error, Reason}, State}
    end
).

%todo zmierzyc ile trwa listowanie widoku

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Tries to start process responsible for performing auto-cleaning.
%% Returns error if other auto-cleaning run is in progress.
%% @end
%%-------------------------------------------------------------------
-spec start(od_space:id(), autocleaning:config(), non_neg_integer()) -> {ok, batch_id()} | {error, term()}.
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

-spec restart(batch_id(), od_space:id(), autocleaning:config()) ->
    {ok, batch_id()} | ok.
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

-spec stop(od_space:id(), batch_id()) -> ok.
stop(SpaceId, AutocleaningRunId) ->
    gen_server2:cast(?SERVER(SpaceId), ?STOP_CLEANING_MSG(AutocleaningRunId)).

-spec notify_files_to_process(od_space:id(), run_id(), non_neg_integer(), view_traverse:token()) -> ok.
notify_files_to_process(SpaceId, AutocleaningRunId, FilesNumber, Token) ->
    gen_server2:cast(?SERVER(SpaceId), ?FILES_TO_PROCESS_MSG(AutocleaningRunId, FilesNumber, Token)).

-spec notify_processed_file(od_space:id(), run_id(), non_neg_integer()) -> ok.
notify_processed_file(SpaceId, AutocleaningRunId, BatchNo) ->
    gen_server2:cast(?SERVER(SpaceId), ?FILE_PROCESSED_MSG(AutocleaningRunId, BatchNo)).

-spec notify_finished_traverse(od_space:id(), batch_id()) -> ok.
notify_finished_traverse(SpaceId, AutocleaningRunId) ->
    gen_server2:cast(?SERVER(SpaceId), ?TRAVERSE_FINISHED_MSG(AutocleaningRunId)).

-spec pack_batch_id(batch_id(), non_neg_integer()) -> batch_id().
pack_batch_id(AutocleaningRunId, Int) ->
    <<AutocleaningRunId/binary, ?ID_SEPARATOR/binary, (integer_to_binary(Int))/binary>>.

-spec unpack_batch_id(batch_id()) -> {batch_id(), non_neg_integer()}.
unpack_batch_id(BatchId) ->
    [AutocleaningRunId, BatchNo] = binary:split(BatchId, ?ID_SEPARATOR),
    {AutocleaningRunId, binary_to_integer(BatchNo)}.

%%%===================================================================
%%% Internal API
%%%===================================================================

-spec start_cleaning(od_space:id(), batch_id()) -> ok.
start_cleaning(SpaceId, AutocleaningRunId) ->
    gen_server2:cast(?SERVER(SpaceId), ?START_CLEANING_MSG(AutocleaningRunId)).

-spec notify_released_file(od_space:id(), run_id(), non_neg_integer(), batch_no()) -> ok.
notify_released_file(SpaceId, ARId, ReleasedBytes, BatchNo) ->
    gen_server2:cast(?SERVER(SpaceId), ?FILE_RELEASED_MSG(ARId, ReleasedBytes, BatchNo)).

-spec notify_processed_file(od_space:id(), batch_id()) -> ok.
notify_processed_file(SpaceId, BatchId) ->
    {ARId, BatchNo} = unpack_batch_id(BatchId),
    notify_processed_file(SpaceId, ARId, BatchNo).

%%-------------------------------------------------------------------
%% @doc
%% This function is used for debugging.
%% @end
%%-------------------------------------------------------------------
-spec check(od_space:id()) -> any().
check(SpaceId) ->
    % TODO DELETE
    gen_server2:cast(?SERVER(SpaceId), check).

%%%===================================================================
%%% replica_deletion_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link replica_deletion_behaviour} callback replica_deletion_predicate/1.
%% @end
%%--------------------------------------------------------------------
-spec replica_deletion_predicate(batch_id()) -> boolean().
replica_deletion_predicate(BatchId) ->
    {AutocleaningRunId, _BatchNo} = unpack_batch_id(BatchId),
    {ok, #{
        released_bytes := Released,
        bytes_to_release := ToRelease
    }} = autocleaning_api:get_run_report(AutocleaningRunId),
    Released < ToRelease.

%%--------------------------------------------------------------------
%% @doc
%% {@link replica_deletion_behaviour} callback process_replica_deletion_result/4.
%% @end
%%--------------------------------------------------------------------
-spec process_replica_deletion_result(replica_deletion:result(), od_space:id(),
    file_meta:uuid(), batch_id()) -> ok.
process_replica_deletion_result({ok, ReleasedBytes}, SpaceId, FileUuid, BatchId) ->
    {ARId, BatchNo} = unpack_batch_id(BatchId),
    ?debug("Auto-cleaning of file ~p in run ~p released ~p bytes.", [FileUuid, ARId, ReleasedBytes]),
    autocleaning_run:mark_released_file(ARId, ReleasedBytes),
    notify_released_file(SpaceId, ARId, ReleasedBytes, BatchNo);
process_replica_deletion_result({error, precondition_not_satisfied}, SpaceId, _FileUuid, BatchId) ->
    notify_processed_file(SpaceId, BatchId);
process_replica_deletion_result({error, canceled}, SpaceId, _FileUuid, BatchId) ->
    notify_processed_file(SpaceId, BatchId);
process_replica_deletion_result(Error, SpaceId, FileUuid, BatchId) ->
    {ARId, BatchNo} = unpack_batch_id(BatchId),
    ?error("Error ~p occurred during auto-cleanisg of file ~p in run ~p",
        [Error, FileUuid, ARId]),
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
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([ARId, SpaceId, AutocleaningRun, Config]) ->
    BytesToRelease = autocleaning_run:get_bytes_to_release(AutocleaningRun),
    AlreadyReleasedBytes = autocleaning_run:get_released_bytes(AutocleaningRun),
    NextBatchToken = autocleaning_run:get_query_view_token(AutocleaningRun),
    start_cleaning(SpaceId, ARId),
    {ok, #state{
        run_id = ARId,
        space_id = SpaceId,
        config = Config,
        batch_size = ?BATCH_SIZE,
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
    files_processed = FP
}) ->
    % todo remove this test log after resolving VFS-5128
    % todo this log will not appear on production as it can be triggered only from console
    ?critical(
    "~nFiles to process: ~p~n"
    "Files processed: ~p~n", [FTP, FP]),
    {noreply, State};
handle_cast(?MESSAGE(MessageContent, AutocleaningRunId), State = #state{run_id = AutocleaningRunId}) ->
    handle_cast_internal(MessageContent, State);
handle_cast(?MESSAGE(_MsgType, _OldAutocleaningRunId), State = #state{run_id = _AutocleaningRunId}) ->
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

-spec handle_cast_internal(message_content(), state()) ->
    {noreply, state()} | {stop, normal | {error, term()}, state()}.
handle_cast_internal(?START_CLEANING, State = #state{
    run_id = AutocleaningRunId,
    space_id = SpaceId,
    config = #autocleaning_config{rules = ACRules},
    batch_size = BatchSize,
    next_batch_token = NextBatchToken
}) ->
    % todo handle error from run here
    autocleaning_view_traverse:run(SpaceId, AutocleaningRunId, ACRules, BatchSize, NextBatchToken),
    {noreply, State};
handle_cast_internal(?FILES_TO_PROCESS(FilesNumber, Token), State = #state{
    files_to_process = FilesToProcess,
    batches_counters = BatchesCounters,
    batches_tokens = BatchesTokens
}) ->
    TokenOffset = view_traverse:get_offset(Token),
    BatchNo = TokenOffset - FilesNumber,
    {noreply, State#state{
        files_to_process = FilesToProcess + FilesNumber,
        batches_counters = BatchesCounters#{BatchNo => init_batch_file_counters(FilesNumber)},
        batches_tokens = BatchesTokens#{BatchNo => Token}
    }};
handle_cast_internal(?FILE_RELEASED(ReleasedBytes, BatchNo), State) ->
    ?run_and_catch_errors(fun() ->
        State2 = mark_released_file(State, ReleasedBytes, BatchNo),
        State3 = process_updated_state(State2),
        {noreply, State3}
    end, State);
handle_cast_internal(?FILE_PROCESSED(BatchNo), State) ->
    ?run_and_catch_errors(fun() ->
        State2 = mark_processed_file(State, BatchNo),
        State3 = process_updated_state(State2),
        {noreply, State3}
    end, State);
handle_cast_internal(?TRAVERSE_FINISHED, State) ->
    State2 = State#state{end_of_view_reached = true},
    {noreply, process_updated_state(State2)};
handle_cast_internal(?STOP_CLEANING, State = #state{
    run_id = ARId,
    space_id = SpaceId
}) ->
    ?run_and_catch_errors(fun() ->
        autocleaning_run:mark_completed(ARId),
        ?debug("Auto-cleaning run ~p of space ~p finished", [ARId, SpaceId]),
        {stop, normal, State}
    end, State);
handle_cast_internal(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.


-spec mark_released_file(state(), non_neg_integer(), batch_no()) -> state().
mark_released_file(State = #state{released_bytes = ReleasedBytes}, FileReleasedBytes, BatchNo) ->
    State2 = mark_processed_file(State, BatchNo),
    State2#state{released_bytes = ReleasedBytes + FileReleasedBytes}.


-spec mark_processed_file(state(), non_neg_integer()) -> state().
mark_processed_file(State = #state{
    finished_batches = BatchesToPersist,
    files_processed = FilesProcessed,
    batches_counters = BatchesCounters
}, BatchNo) ->
    {NewBatchesCounters, NewBatchesToPersist} = case
        increment_and_check_equality(BatchesCounters, BatchNo)
    of
        {false, BatchesCounters2} ->
            {BatchesCounters2, BatchesToPersist};
        {true, BatchesCounters2} ->
            BatchesToPersist2 = ordsets:add_element(BatchNo, BatchesToPersist),
            {BatchesCounters2, BatchesToPersist2}
    end,
    State#state{
        finished_batches = NewBatchesToPersist,
        batches_counters = NewBatchesCounters,
        files_processed = FilesProcessed + 1
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
    scheduled_cancelling = false,
    batches_counters = BatchesCounters,
    end_of_view_reached = TraverseFinished
}) when BytesToRelease =< ReleasedBytes ->
    case TraverseFinished of
        true -> ok;
        false ->
            autocleaning_view_traverse:cancel(ARId)
    end,
    lists:foreach(fun(BatchNo) ->
        replica_deletion_master:cancel_autocleaning_request(SpaceId, pack_batch_id(ARId, BatchNo))
    end, maps:keys(BatchesCounters)),
    State#state{scheduled_cancelling = true};
maybe_stop_cleaning(State) ->
    State.


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

    case BatchNumToPersist =:= 0 of
        false ->
            {BatchToken, BatchesTokens2} = maps:take(BatchNumToPersist, BatchesTokens),
            autocleaning_run:set_query_view_token(ARId, BatchToken),

            BatchesTokens3 = lists:foldl(fun(StrippedBatchNum, BatchesTokensIn) ->
                maps:remove(StrippedBatchNum, BatchesTokensIn)
            end, BatchesTokens2, BatchesStripped),
            State#state{
                finished_batches = BatchesToPersist2,
                last_persisted_batch = BatchNumToPersist,
                batches_tokens = BatchesTokens3
            };
        true ->
            % batch no. 1 has not been finished yet
            State
    end.


-spec increment_and_check_equality(#{non_neg_integer() => batch_file_counters()},
    non_neg_integer()) -> {boolean(), #{non_neg_integer() => batch_file_counters()}}.
increment_and_check_equality(BatchesCounters, BatchNo) ->
    BatchFileCounters = maps:get(BatchNo, BatchesCounters),
    {AreCountersEqual, BatchFileCounters2} = increment_and_check_equality(BatchFileCounters),
    BatchesCounters2 = case AreCountersEqual of
        true -> maps:remove(BatchNo, BatchesCounters);
        false -> BatchesCounters#{BatchNo => BatchFileCounters2}
    end,
    {AreCountersEqual, BatchesCounters2}.


-spec increment_and_check_equality(batch_file_counters()) -> {boolean(), batch_file_counters()}.
increment_and_check_equality(BFC = #batch_file_counters{
    files_to_process = FilesToProcess,
    files_processed = FilesProcessed
}) ->
    FilesProcessed2 = FilesProcessed + 1,
    {FilesProcessed2 == FilesToProcess, BFC#batch_file_counters{files_processed = FilesProcessed2}}.


%%--------------------------------------------------------------------
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
-spec strip_if_continuous(non_neg_integer(), [non_neg_integer()]) ->
    {non_neg_integer(), [non_neg_integer()], [non_neg_integer()]}.
strip_if_continuous(N, L) ->
    strip_if_continuous(N, [], L).

-spec strip_if_continuous(non_neg_integer(), [non_neg_integer()], [non_neg_integer()]) ->
    {non_neg_integer(), [non_neg_integer()], [non_neg_integer()]}.
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