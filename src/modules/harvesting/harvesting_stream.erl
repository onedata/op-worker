%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines generic behaviour of `harvesting_stream`.
%%% It also implements most of required generic functionality for such stream.
%%% harvesting_stream is responsible for collecting #custom_metadata{} changes
%%% from couchbase_changes_stream and sending them to Onezone which pushes them
%%% to suitable destination.
%%%
%%% Sending changes to Onezone is performed by call to
%%% space_logic:harvest_metadata(SpaceId, Destination, Batch, MaxStreamSeq, MaxSeq)
%%% where:
%%%     * SpaceId      - id of space in which harvesting is performed
%%%     * Destination  - structure which tells Onezone where harvested
%%%                      metadata should be pushed.
%%%                      Please read DESTINATION section below for more details.
%%%     * Batch        - list of objects, where each object is associated
%%%                      with exactly one #custom_metadata{} document.
%%%                      Please read BATCH section below for more details.
%%%     * MaxStreamSeq - maximum sequence number processed by given
%%%                      harvesting_stream. Compared to MaxSeq, it allows to
%%%                      evaluate progress of harvesting in given space.
%%%     * MaxSeq       - max sequence number in given scope (SpaceId).
%%%
%%% Return value from space_logic:harvest_metadata/5 is processed by
%%% harvesting_result:process/2 function which converts it to format
%%% more convenient to handle by harvesting_stream.
%%% Please read the docs in {@link harvesting_result} module for more details.
%%%
%%% DESTINATION
%%% Destination is a simple data structure that represents structure of
%%% Harvesters and Indices associated with them.
%%% Destination for given harvesting_stream is stored in its state.
%%% {@link harvesting_destination} is a helper module which defines functions
%%% for performing operations on this structure.
%%% Please read the docs in {@link harvesting_destination} module for more
%%% details.
%%%
%%% BATCH
%%% Changes of #custom_metadata{} documents are accumulated in
%%% Accumulator :: harvesting_batch:accumulator() structure. It is ensured that
%%% there is maximum one object associated with one file in one batch.
%%% Before sending changes to Onezone, harvesting_batch:prepare_to_send/1
%%% function must be called on Accumulator to convert it to format
%%% accepted by space_logic:harvest_metadata/5 function.
%%% It ensures that batch is sorted by sequence numbers and that stored
%%% metadata are encoded.
%%%
%%% Accumulated changes are pushed to Onezone when one of 3 cases occurs:
%%%   * size of accumulated batch exceeds ?BATCH_SIZE,
%%%   * elapsed time from last harvesting timestamp exceeds ?FLUSH_TIMEOUT
%%%     and Batch is not empty,
%%%   * elapsed time from last harvesting timestamp exceeds ?FLUSH_TIMEOUT
%%%     and Batch is empty and last seen sequence by the harvesting_stream is
%%%     higher than last sent MaxStreamSeq. This case allows to notify Onezone
%%%     that stream is processing changes but there hasn't been any #custom_metadata{}
%%%     changes since last call to space_logic:harvest_metadata/5.
%%%
%%% IMPLEMENTING MODULES
%%% There are 2 modules, that implement this behaviour:
%%%   * main_harvesting_stream - responsible for harvesting metadata changes
%%%                              in given space. It is always the stream that
%%%                              hast the highest processed sequence number in
%%%                              the space, out of all streams harvesting given
%%%                              space. It also reacts on changes in the harvesters'
%%%                              structure by updating its own Destination and by
%%%                              starting aux_harvesting_streams. Another
%%%                              responsibility of main_harvesting_stream is
%%%                              taking over harvesting for given pair
%%%                              {HarvesterId, IndexId} when aux_harvesting_stream
%%%                              catches up with tha main stream.
%%%   * aux_harvesting_stream  - responsible for catching-up with
%%%                              main_harvesting_stream for given pair
%%%                              {HarvesterId, IndexId}. It is started with
%%%                              Until :: couchbase_changes:seq(). After reaching
%%%                              this sequence it tries to relay its
%%%                              {HarvesterId, IndexId} to the main stream.
%%%
%%% Please read the docs in {@link main_harvesting_stream} and
%%% {@link aux_harvesting_stream} modules for more details.
%%%
%%% HARVESTING STATE
%%% State of harvesting in given space is persisted in {@link harvesting_state}
%%% model. For each harvested space, it stores a document in which
%%% the highest seen sequence for given pair
%%% {od_harvester:id(), od_harvester:index()} is persisted.
%%% This model allows to track progress of harvesting and to restart
%%% harvesting from the given point after restart of provider,
%%% re-support of space or re-subscription of space by harvester.
%%%
%%% HARVESTING_STREAM STATE MACHINE
%%%  +---------------+                     +--------------+
%%%  |   streaming   |-----on-failure----->|   retrying   |
%%%  +---------------+                     +--------------+
%%%           ^                                   |
%%            |                                   |
%%%           +------------on-success-------------+
%%%
%%% Modes:
%%%  - streaming - in this mode, harvesting_stream accumulates changes received
%%%                from couchbase_changes_stream. If number of accumulated
%%%                changes exceeds ?BATCH_SIZE or ?FLUSH_TIMEOUT is exceeded
%%%                Batch of changes is pushed (harvested) to Onezone.
%%%                If harvesting succeeds the process remains in the "streaming"
%%%                state. Otherwise, it stops couchbase_changes_stream and
%%%                moves to the "retrying" state.
%%%  - retrying -  in this mode, harvesting_stream retries harvesting of
%%%                given batch of changes using backoff algorithm.
%%%                Before starting the retries, the process ensures that
%%%                couchbase_changes_stream is stopped so that new messages
%%%                will not arrive when performing retries.
%%%                When all changes are successfully pushed, harvesting_stream
%%%                moves to the `streaming` mode.
%%%
%%%
%%% COUCHBASE CHANGES STREAM THROTTLING
%%% It was very important to avoid situation in which harvesting_stream
%%% is flooded by changes from couchbase_changes_stream.
%%% Therefore, callback called by couchbase_changes_stream is
%%% gen_server:call instead of gen_server:cast.
%%% When handling request from couchbase_changes_stream, harvesting_stream
%%% first replies with gen_server:reply(From, ok) and next processes the
%%% request.
%%% This trick ("simulating" gen_server:cast with gen_server:call)
%%% has following advantages:
%%%     * couchbase_changes_stream is not blocked when harvesting_stream is
%%%       processing current change (like in gen_server:cast). It can
%%%       prepare next change that will be processed by harvesting_stream
%%%       immediately after it finishes processing current change.
%%%     * couchbase_changes_stream blocks on sending next change to
%%%       harvesting_stream as it cannot reply because it is processing
%%%       current change right now. This mechanism ensures that
%%%       harvesting_stream won't be flooded.
%%% @end
%%%-------------------------------------------------------------------
-module(harvesting_stream).
-author("Jakub Kudzia").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("modules/harvesting/harvesting.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([start_link/2, enter_streaming_mode/1, enter_retrying_mode/1]).

%% util functions
-export([throw_harvesting_not_found_exception/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% exported for mocking in tests
-export([changes_stream_start_link/4]).

-type state() :: #hs_state{}.
-type mode() :: streaming | retrying.
-type stream_type() :: main_harvesting_stream | aux_harvesting_stream.
-type name() :: {stream_type(), term()}.
-type handling_result() :: {noreply, harvesting_stream:state()} |
{stop, Reason :: term(), harvesting_stream:state()}.


-export_type([state/0, mode/0, name/0, handling_result/0]).

-define(FLUSH, flush).
-define(RETRY, retry).

% Below constant determines how often seen sequence number will be persisted
% per given IndexId. Value 1000 says that seq will be persisted
% when the difference between current and previously persisted seq will
% be greater than 1000.
% This value is not checked when seq is associated with custom_metadata
% document. In such case, the seq is always persisted.
-define(IGNORED_SEQ_REPORTING_FREQUENCY, 1000).

-define(MIN_BACKOFF_INTERVAL, application:get_env(
    ?APP_NAME, harvesting_stream_min_backoff_interval, timer:seconds(5))).
-define(MAX_BACKOFF_INTERVAL, application:get_env(
    ?APP_NAME, harvesting_stream_max_backoff_interval, timer:minutes(5))).

-define(BATCH_SIZE, application:get_env(?APP_NAME, harvesting_batch_size, 1000)).
-define(FLUSH_TIMEOUT_SECONDS, application:get_env(?APP_NAME, harvesting_flush_timeout_seconds, 10)).
-define(FLUSH_TIMEOUT, timer:seconds(?FLUSH_TIMEOUT_SECONDS)).

-define(EXEC_AND_HANDLE_HARVESTING_DOC_NOT_FOUND(Fun),
    try
        Fun()
    catch
        throw:(?HARVESTING_DOC_NOT_FOUND_EXCEPTION(State)) ->
            Mod = State#hs_state.callback_module,
            Mod:on_harvesting_doc_not_found(State)
    end).

%%%===================================================================
%%% Callbacks
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Callback called by ?MODULE:init/1 which allows for type-specific
%% initialisation of harvesting_stream.
%% @end
%%-------------------------------------------------------------------
-callback init([term()]) -> {ok, state()} | {stop, term()}.

%%-------------------------------------------------------------------
%% @doc
%% Callback called to get name of harvesting_stream which will be used
%% to register it.
%% @end
%%-------------------------------------------------------------------
-callback name([term()]) -> name().

%%-------------------------------------------------------------------
%% @doc
%% Callback which allows for harvesting_stream type-specific
%% implementation of gen_server:handle_call/3.  
%% It is called by ?MODULE:handle_call/3 when no function clause
%% matches passed arguments. 
%% @end
%%-------------------------------------------------------------------
-callback handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: state()) -> handling_result().

%%-------------------------------------------------------------------
%% @doc
%% Callback which allows for harvesting_stream type-specific
%% implementation of gen_server:handle_cast/2.  
%% It is called by ?MODULE:handle_cast/2 when no function clause
%% matches passed arguments. 
%% @end
%%-------------------------------------------------------------------
-callback handle_cast(Request :: term(), State :: state()) -> handling_result().

%%-------------------------------------------------------------------
%% @doc
%% Callback called when harvesting errors must be handled
%% accordingly to harvesting_stream type.
%% @end
%%-------------------------------------------------------------------
-callback custom_error_handling(harvesting_stream:state(),
    harvesting_result:result()) -> handling_result().

%%-------------------------------------------------------------------
%% @doc
%% Callback which allows for type-specific actions to be taken on
%% end of couchbase_changes stream.
%% @end
%%-------------------------------------------------------------------
-callback on_end_of_stream(harvesting_stream:state()) -> ok.

%%-------------------------------------------------------------------
%% @doc
%% Callback which allows for type-specific actions to be taken on
%% ?HARVESTING_DOC_NOT_FOUND_EXCEPTION
%% @end
%%-------------------------------------------------------------------
-callback on_harvesting_doc_not_found(harvesting_stream:state()) ->
    handling_result().

-callback terminate(term(), harvesting_stream:state()) -> ok.

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(module(), [term()]) -> {ok, pid()} | {error, Reason :: term()}.
start_link(CallbackModule, Args) ->
    Name = CallbackModule:name(Args),
    gen_server2:start_link({global, Name}, ?MODULE, [CallbackModule | Args], []).

%%%===================================================================
%%% util functions
%%%===================================================================

-spec throw_harvesting_not_found_exception(state()) -> no_return().
throw_harvesting_not_found_exception(State) ->
    throw(?HARVESTING_DOC_NOT_FOUND_EXCEPTION(State)).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init(Args :: term()) -> {ok, State :: state()} | {stop, Reason :: term()}.
init([CallbackModule | OtherArgs]) ->
    try
        init_internal(CallbackModule, OtherArgs)
    catch
        _:Reason ->
            ?error_stacktrace("Unable to start harvesting_stream due to ~p", [Reason]),
            {stop, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}.
handle_call({change, {ok, end_of_stream}}, From, State = #hs_state{
    mode = streaming,
    callback_module = Mod,
    batch = Batch
}) ->
    % all changes sent by changes_stream before stopping it have already arrived
    % so we can start backoff algorithm
    State2 = State#hs_state{stream_pid = undefined},
    ?EXEC_AND_HANDLE_HARVESTING_DOC_NOT_FOUND(fun() ->
        gen_server2:reply(From, ok),
        case harvesting_batch:is_empty(Batch) of
            true ->
                Mod:on_end_of_stream(State2),
                {noreply, State2};
            false ->
                harvest_and_handle_errors(State2)
        end
    end);
handle_call({change, {ok, DocOrDocs}}, From, State = #hs_state{mode = streaming}) ->
    ?EXEC_AND_HANDLE_HARVESTING_DOC_NOT_FOUND(fun() ->
        gen_server2:reply(From, ok),
        State2 = maybe_add_docs_to_batch(State, utils:ensure_list(DocOrDocs)),
        maybe_harvest_batch_and_handle_errors(State2)
    end);
handle_call({change, {ok, end_of_stream}}, _From, State = #hs_state{mode = retrying}) ->
    % all changes sent by changes_stream before stopping it have already arrived
    % so we can start backoff algorithm
    ?EXEC_AND_HANDLE_HARVESTING_DOC_NOT_FOUND(fun() ->
        {reply, ok, schedule_backoff_if_destination_is_not_empty(
            State#hs_state{stream_pid = undefined})
        }
    end);
handle_call({change, {ok, _DocOrDocs}}, _From, #hs_state{mode = retrying} = State) ->
    % changes sent by changes_stream before stopping it, we can ignore them
    {reply, ok, State};
handle_call(Request, From, State = #hs_state{callback_module = Mod}) ->
    ?EXEC_AND_HANDLE_HARVESTING_DOC_NOT_FOUND(fun() ->
        Mod:handle_call(Request, From, State)
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_cast(Request, #hs_state{callback_module = Mod} = State) ->
    ?EXEC_AND_HANDLE_HARVESTING_DOC_NOT_FOUND(fun() ->
        Mod:handle_cast(Request, State)
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState
    :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_info({timeout, _TimerRef, ?RETRY}, State = #hs_state{
    mode = retrying,
    stream_pid = undefined
}) ->
    ?EXEC_AND_HANDLE_HARVESTING_DOC_NOT_FOUND(fun() ->
        harvest_and_handle_errors(State)
    end);
handle_info(?FLUSH, State = #hs_state{mode = streaming}) ->
    ?EXEC_AND_HANDLE_HARVESTING_DOC_NOT_FOUND(fun() ->
        case should_flush(State) of
            true ->
                Return = harvest_and_handle_errors(State),
                schedule_flush(),
                Return;
            false ->
                State2 = maybe_persist_last_seen_seq(State),
                schedule_flush(),
                {noreply, State2}
        end
    end);
handle_info(?FLUSH, State) ->
    {noreply, State};
handle_info(Info, State) ->
    ?log_bad_request(Info),
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
    State :: state()) -> term().
terminate(Reason, State = #hs_state{name = Name}) ->
    ?debug("Stopping harvesting_stream ~p due to reason: ~p", [Name, Reason]),
    terminate(Reason, State),
    ?log_terminate(Reason, State).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: state(),
    Extra :: term()) -> {ok, NewState :: state()} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec init_internal(module(), [term()]) -> {ok, state()} | {stop, term()}.
init_internal(CallbackModule, Args) ->
    case CallbackModule:init(Args) of
        {ok, State = #hs_state{
            destination = Destination,
            name = Name,
            last_seen_seq = LastSeenSeq
        }} ->
            ?debug("Starting harvesting stream ~p", [Name]),
            State2 = State#hs_state{
                provider_id = oneprovider:get_id(),
                last_persisted_seq = LastSeenSeq,
                last_sent_max_stream_seq = LastSeenSeq,
                callback_module = CallbackModule,
                ignoring_deleted = (LastSeenSeq =:= ?DEFAULT_HARVESTING_SEQ)
            },
            case harvesting_destination:is_empty(Destination) of
                true ->
                    ?debug("Not starting harvesting stream ~p due to empty destination", [Name]),
                    {stop, normal};
                false ->
                    {ok, enter_streaming_mode(State2)}
            end;
        Other ->
            Other
    end.

-spec start_changes_stream(od_space:id(), couchbase_changes:since(),
    couchbase_changes:until()) -> {ok, pid()}.
start_changes_stream(SpaceId, Since, Until) ->
    Self = self(),
    Callback = fun(Change) ->
        try
            gen_server2:call(Self, {change, Change}, infinity)
        catch
            _:_ -> ok
        end
    end,
    harvesting_stream:changes_stream_start_link(SpaceId, Callback, Since, Until).

-spec changes_stream_start_link(od_space:id(), couchbase_changes:callback(),
    couchbase_changes:since(), couchbase_changes:until()) ->
    {ok, pid()} | {error, Reason :: term()}.
changes_stream_start_link(SpaceId, Callback, Since, Until) ->
    couchbase_changes_stream:start_link(
        couchbase_changes:design(), SpaceId, Callback,
        [{since, Since}, {until, Until}], [self()]
    ).

-spec stop_changes_stream(state()) -> state().
stop_changes_stream(State = #hs_state{stream_pid = undefined}) ->
    State;
stop_changes_stream(State = #hs_state{stream_pid = StreamPid}) ->
    case is_process_alive(StreamPid) of
        false ->
            State#hs_state{stream_pid = undefined};
        true ->
            couchbase_changes_stream:stop_async(StreamPid),
            State
    end.

-spec maybe_add_docs_to_batch(state(), [datastore:doc()]) -> state().
maybe_add_docs_to_batch(State, []) ->
    State;
maybe_add_docs_to_batch(State, [Doc | Docs]) ->
    maybe_add_docs_to_batch(maybe_add_doc_to_batch(State, Doc), Docs).


-spec maybe_add_doc_to_batch(state(), datastore:doc()) -> state().
maybe_add_doc_to_batch(State = #hs_state{
    ignoring_deleted = true
}, Doc = #document{
    deleted = false,
    value = #custom_metadata{}
}) ->
    maybe_add_doc_to_batch(State#hs_state{ignoring_deleted = false}, Doc);
maybe_add_doc_to_batch(State = #hs_state{
    ignoring_deleted = false,
    batch = Batch,
    provider_id = ProviderId
}, Doc = #document{
    seq = Seq,
    value = #custom_metadata{},
    mutators = [ProviderId | _]
}) ->
    Batch2 = harvesting_batch:accumulate(Doc, Batch),
    State#hs_state{batch = Batch2, last_seen_seq = Seq};
maybe_add_doc_to_batch(State, #document{seq = Seq}) ->
    State#hs_state{last_seen_seq = Seq}.

-spec maybe_persist_last_seen_seq(state()) -> state().
maybe_persist_last_seen_seq(State = #hs_state{
    space_id = SpaceId,
    last_seen_seq = LastSeenSeq,
    last_persisted_seq = LastPersistedSeq,
    destination = Destination
})
    when (LastSeenSeq - LastPersistedSeq) > ?IGNORED_SEQ_REPORTING_FREQUENCY ->
    % If the gap between #state.last_seen_seq and
    % #state.last_persisted_seq is greater than the defined constant
    % (?IGNORED_SEQ_REPORTING_FREQUENCY), the last_seen_seq is persisted.
    case harvesting_state:set_seen_seq(SpaceId, Destination, LastSeenSeq) of
        ok ->
            State#hs_state{last_persisted_seq = LastSeenSeq};
        ?ERROR_NOT_FOUND ->
            throw_harvesting_not_found_exception(State)
    end;
maybe_persist_last_seen_seq(State) ->
    State.

-spec maybe_harvest_batch_and_handle_errors(state()) -> handling_result().
maybe_harvest_batch_and_handle_errors(State = #hs_state{batch = Batch}) ->
    case harvesting_batch:size(Batch) >= ?BATCH_SIZE of
        true ->
            harvest_and_handle_errors(State);
        false ->
            {noreply, State}

    end.

-spec harvest_and_handle_errors(state()) -> handling_result().
harvest_and_handle_errors(State = #hs_state{
    name = Name,
    space_id = SpaceId,
    destination = Destination,
    batch = Batch,
    callback_module = Mod,
    last_seen_seq = LastSeenSeq
}) ->
    MaxSpaceSeq = get_max_seq(SpaceId),
    PreparedBatch = harvesting_batch:prepare_to_send(Batch),
    BatchEntries = harvesting_batch:get_batch_entries(PreparedBatch),
    State2 = State#hs_state{batch = PreparedBatch},
    Result = space_logic:harvest_metadata(SpaceId, Destination, BatchEntries, LastSeenSeq, MaxSpaceSeq),
    ProcessedResult = harvesting_result:process(Result, Destination, PreparedBatch),
    LastBatchSeq = harvesting_batch:get_last_seq(PreparedBatch),
    case harvesting_result:get_summary(ProcessedResult) of
        ?ERROR_NOT_FOUND ->
            ?debug("Space ~p was deleted. Stopping harvesting_stream ~p", [SpaceId, Name]),
            {stop, normal, State2};
        Error = {error, _} ->
            ErrorLog = str_utils:format_bin("Unexpected error ~w occurred.", [Error]),
            {noreply, enter_retrying_mode(State2#hs_state{
                log_level = error,
                error_log = ErrorLog
            })};
        Summary ->
            case maps:keys(Summary) of
                [LastBatchSeq] ->
                    % all indices are associated with LastBatchSeq which means
                    % that harvesting succeeded for all of them
                    on_successful_result(State2);
                _ ->
                    Mod:custom_error_handling(State2, ProcessedResult)
            end
    end.

-spec on_successful_result(state()) -> handling_result().
on_successful_result(State = #hs_state{
    space_id = SpaceId,
    last_seen_seq = LastSeenSeq,
    destination = Destination
}) ->
    case harvesting_state:set_seen_seq(SpaceId, Destination, LastSeenSeq) of
        ok ->
            State2 = State#hs_state{
                batch = harvesting_batch:new_accumulator(),
                last_harvest_timestamp = time_utils:system_time_seconds(),
                last_persisted_seq = LastSeenSeq,
                last_sent_max_stream_seq = LastSeenSeq
            },
            {noreply, enter_streaming_mode(State2)};
        ?ERROR_NOT_FOUND ->
            throw_harvesting_not_found_exception(State)
    end.


%%-------------------------------------------------------------------
%% @doc
%% Ensures that stream is in `streaming` mode.
%% Starts changes stream if process was in different mode.
%% @end
%%-------------------------------------------------------------------
-spec enter_streaming_mode(state()) -> state().
enter_streaming_mode(State = #hs_state{
    until = Until,
    last_seen_seq = LastSeenSeq,
    callback_module = Mod
}) when Until /= infinity andalso LastSeenSeq >= (Until - 1) ->
    State2 = stop_changes_stream(State),
    case State2#hs_state.stream_pid of
        undefined -> Mod:on_end_of_stream(State2);
        _ -> ok
    end,
    State2;
enter_streaming_mode(State = #hs_state{mode = streaming, stream_pid = StreamPid})
    when StreamPid /= undefined ->
    schedule_flush(),
    State#hs_state{batch = harvesting_batch:new_accumulator()};
enter_streaming_mode(State = #hs_state{
    space_id = SpaceId,
    until = Until,
    last_seen_seq = LastSeenSeq,
    callback_module = Mod
}) ->
    Since = LastSeenSeq + 1,
    case stream_limit_exceeded(Since, Until) of
        true ->
            Mod:on_end_of_stream(State),
            State;
        false ->
            schedule_flush(),
            {ok, StreamPid} = start_changes_stream(SpaceId, Since, Until),
            State#hs_state{
                mode = streaming,
                stream_pid = StreamPid,
                backoff = undefined,
                batch = harvesting_batch:new_accumulator()
            }
    end.

-spec enter_retrying_mode(state()) -> state().
enter_retrying_mode(State = #hs_state{stream_pid = undefined}) ->
    %% stream_pid is undefined which means that stream has been stopped and we
    %% can start backoff algorithm
    schedule_backoff_if_destination_is_not_empty(State#hs_state{mode = retrying});
enter_retrying_mode(State) ->
    %% stream_pid is not undefined, we have to ensure it is stopped
    case stop_changes_stream(State) of
        State2 = #hs_state{stream_pid = undefined} ->
            %% stream_pid is undefined now, we can schedule backoff algorithm
            schedule_backoff_if_destination_is_not_empty(State2#hs_state{mode = retrying});
        State2 ->
            %% stream_pid is not undefined which means that
            %% `end_of_stream` message hasn't arrived yet
            State2#hs_state{mode = retrying}
    end.

-spec schedule_backoff_if_destination_is_not_empty(state()) -> state().
schedule_backoff_if_destination_is_not_empty(State = #hs_state{destination = Destination}) ->
    case harvesting_destination:is_empty(Destination) of
        true -> State;
        false -> schedule_backoff(State)
    end.

-spec schedule_backoff(state()) -> state().
schedule_backoff(State = #hs_state{
    space_id = SpaceId,
    backoff = undefined,
    error_log = ErrorLog,
    log_level = LogLevel,
    name = Name
}) ->
    B0 = backoff:init(?MIN_BACKOFF_INTERVAL, ?MAX_BACKOFF_INTERVAL, self(), ?RETRY),
    B1 = backoff:type(B0, jitter),
    backoff_log(Name, SpaceId, ErrorLog, ?MIN_BACKOFF_INTERVAL / 1000, LogLevel),
    backoff:fire(B1),
    State#hs_state{backoff = B1};
schedule_backoff(State = #hs_state{
    space_id = SpaceId,
    backoff = B0,
    error_log = ErrorLog,
    log_level = LogLevel,
    name = Name
}) ->
    {Value, B1} = backoff:fail(B0),
    backoff_log(Name, SpaceId, ErrorLog, Value / 1000, LogLevel),
    backoff:fire(B1),
    State#hs_state{backoff = B1}.

-spec backoff_log(name(), od_space:id(), binary(), float(), atom()) -> ok.
backoff_log(StreamName, SpaceId, ErrorLog, NexRetry, error) ->
    ?error(backoff_log_format(), [StreamName, SpaceId, ErrorLog, NexRetry]);
backoff_log(StreamName, SpaceId, ErrorLog, NexRetry, warning) ->
    ?warning(backoff_log_format(), [StreamName, SpaceId, ErrorLog, NexRetry]);
backoff_log(StreamName, SpaceId, ErrorLog, NexRetry, _) ->
    ?debug(backoff_log_format(), [StreamName, SpaceId, ErrorLog, NexRetry]).

-spec backoff_log_format() -> string().
backoff_log_format() ->
    "Error in harvesting_stream: ~w when harvesting space: ~p.~n"
    "~s~n"
    "Next harvesting retry after: ~p seconds.".

-spec get_max_seq(od_space:id()) -> couchbase_changes:seq().
get_max_seq(SpaceId) ->
    couchbase_changes_stream:get_seq_safe(SpaceId,
        datastore_model_default:get_default_disk_ctx()).

-spec schedule_flush() -> ok.
schedule_flush() ->
    erlang:send_after(?FLUSH_TIMEOUT, self(), ?FLUSH),
    ok.

-spec should_flush(state()) -> boolean().
should_flush(#hs_state{
    last_harvest_timestamp = Timestamp,
    batch = Batch,
    last_seen_seq = LastSeenSeq,
    last_sent_max_stream_seq = LastSentMaxStreamSeq
}) ->
    CurrentTimestamp = time_utils:system_time_seconds(),
    ((CurrentTimestamp - Timestamp) >= ?FLUSH_TIMEOUT_SECONDS)
        andalso
        ((not harvesting_batch:is_empty(Batch)) orelse LastSeenSeq =/= LastSentMaxStreamSeq).

-spec stream_limit_exceeded(couchbase_changes:since(),
    couchbase_changes:until()) -> boolean().
stream_limit_exceeded(_Since, infinity) ->
    false;
stream_limit_exceeded(Since, Until) ->
    Since >= Until.