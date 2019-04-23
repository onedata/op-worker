%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module starts couchbase_changes_stream and processes changes received
%%% from it. It pushes changes of custom_metadata document
%%% to Onezone. One instance is started per triple
%%% {HarvesterId, SpaceId, IndexId}. Streams started for the same
%%% HarvesterId and SpaceId differ only in progress of processing changes
%%% from the scope.
%%% Operation of harvest_stream process is presented on the below
%%% state machine diagram.
%%%
%%% HARVEST_STREAM STATE MACHINE:
%%%
%%%  +--on-success--+                               +--on-failure--+
%%%  |              |                               |              |
%%%  |              |                               V              |
%%%  |   +--------------+                    +-------------+       |
%%%  +-->|    stream    |-----on-failure---->|    retry    |-------+
%%%      +--------------+                    +-------------+
%%%             ^                                   |
%%%             |                                   |
%%%             |                              on success
%%%             |                                   |
%%%             |                                   V
%%%             |                            +-------------+
%%%             +--changes_flushed := true---|   resuming  |
%%%                                          +-------------+
%%% Modes:
%%%  - stream   - in this mode, harvest_stream processes changes received
%%%               from couchbase_changes_stream. If the change is processed
%%%               successfully, the process remains in the "stream" state.
%%%               Otherwise, it stops couchbase_changes_stream and moves to the
%%%               "retry" state.
%%%  - retry    - in this mode, harvest_stream retries processing of
%%%               given change/changes using backoff algorithm.
%%%               It is possible, that new changes arrive in this mode, because
%%%               changes may have been sent before stopping
%%%               couchbase_changes_stream. If `end_of_stream` is received,
%%%               the flag `changes_flushed` is set to `true`. Other new changes
%%%               are ignored. When all changes are successfully processed,
%%%               harvest_stream moves to the `resuming` mode.
%%%  - resuming - this mode is used to ensure, that all changes sent by
%%%               couchbase_changes_stream before stopping it where received.
%%%               When `changes_flushed` flag equals `true`, harvest_stream
%%%               moves to the `stream` mode.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(harvest_stream).
-author("Jakub Kudzia").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("modules/harvest/harvest.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    id :: harvest_stream_state:id(),
    index_id :: od_harvester:index(),
    harvester_id :: od_harvester:id(),
    space_id :: od_space:id(),
    provider_id :: od_provider:id(),
    last_persisted_seq = 0 :: couchbase_changes:seq(),
    stream :: undefined | pid(),
    mode = stream :: mode(),
    % flag that informs whether all changes received in retry mode were flushed
    changes_flushed = false :: boolean(),
    docs_to_retry = [] :: datastore:doc() | [datastore:doc()],
    backoff :: term()
}).

-type state() :: #state{}.
-type mode() :: stream | retry | resuming.


% Below constant determines how often seen sequence number will be persisted
% per given IndexId. Value 1000 says that seq will be persisted
% when the difference between current and previously persisted seq will
% be greater than 1000.
% This value is not checked when seq is associated with custom_metadata
% document. In such case, the seq is always persisted.
-define(IGNORED_SEQ_REPORTING_FREQUENCY, 1000).

-define(RETRY, retry).
-define(MIN_BACKOFF_INTERVAL, timer:seconds(5)).
-define(MAX_BACKOFF_INTERVAL, timer:hours(1)).

% TODO VFS-5352 * aggregate changes pushed to oz per harvester
% TODO VFS-5352  ** main stream and "catching-up" stream
% TODO VFS-5352  ** "catching-up" stream should checks indices' sequences and
% TODO VFS-5352     decide whether given index is up to date with maxSeq or not
% TODO VFS-5357 * improve tracking of harvesting progress

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(od_harvester:id(), od_space:id(), od_harvester:index()) ->
    {ok, pid()} | {error, Reason :: term()}.
start_link(HarvesterId, SpaceId, IndexId) ->
    gen_server:start_link(?MODULE, [HarvesterId, SpaceId, IndexId], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes the worker.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([HarvesterId, SpaceId, IndexId]) ->
    Id = harvest_stream_state:id(HarvesterId, SpaceId),
    State = #state{
        id = Id,
        index_id = IndexId,
        space_id = SpaceId,
        harvester_id = HarvesterId,
        provider_id = oneprovider:get_id()
    },
    State2 = enter_stream_mode(State),
    ?debug("Started harvest_stream: ~p", [{HarvesterId, SpaceId, IndexId}]),
    {ok, State2}.


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
handle_call(Request, _From, #state{} = State) ->
    ?log_bad_request(Request),
    {noreply, State}.


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
handle_cast({change, {ok, end_of_stream}}, State = #state{mode = retry}) ->
    % all changes sent by changes_stream before stopping it have already arrived
    {noreply, State#state{changes_flushed = true}};
handle_cast({change, {ok, end_of_stream}}, State = #state{mode = resuming}) ->
    % all changes sent by changes_stream before stopping it have already arrived
    % and retry is finished, so we can get back to normal operation
    {noreply, enter_stream_mode(State)};
handle_cast({change, {ok, end_of_stream}}, State = #state{mode = stream}) ->
    {stop, normal, State};
handle_cast({change, {ok, DocOrDocs}}, State = #state{mode = stream}) ->
    {noreply, consume_change_and_enter_retry_mode_on_error(State, DocOrDocs)};
handle_cast({change, {error, _Seq, Reason}}, State = #state{mode = stream}) ->
    {stop, Reason, State};
handle_cast(_Request, #state{mode = Mode} = State)
    when Mode =:= retry orelse Mode =:= resuming ->
    % ignore because stream is in retry/resuming mode
    {noreply, State};
handle_cast(Request, #state{} = State) ->
    ?log_bad_request(Request),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_info({timeout, _TimerRef, ?RETRY}, State = #state{
    mode = retry,
    docs_to_retry = DocOrDocs,
    changes_flushed = ChangesFlushed
}) ->
    State2 = consume_change_and_enter_retry_mode_on_error(State, DocOrDocs),
    case ChangesFlushed andalso (State2#state.mode =:= resuming) of
        true ->
            {noreply, enter_stream_mode(State2)};
        false ->
            {noreply, State2}
    end;
handle_info(Info, #state{} = State) ->
    ?log_bad_request(Info),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doca
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: state()) -> term().
terminate(Reason, #state{} = State) ->
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

-spec start_changes_stream(od_space:id(), couchbase_changes:seq()) ->
    {ok, pid()}.
start_changes_stream(SpaceId, Since) ->
    Self = self(),
    Callback = fun(Change) -> gen_server2:cast(Self, {change, Change}) end,

    couchbase_changes_stream:start_link(
        couchbase_changes:design(), SpaceId, Callback,
        [{since, Since}, {until, infinity}], []
    ).

-spec stop_changes_stream(pid()) -> ok.
stop_changes_stream(Stream) ->
    try
        gen_server:stop(Stream)
    catch
        exit:noproc ->
            ok
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Start/restart changes stream and mark mode as `stream`.
%% @end
%%-------------------------------------------------------------------
-spec enter_stream_mode(state()) -> state().
enter_stream_mode(State = #state{id = Id, index_id = IndexId, space_id = SpaceId}) ->
    LastProcessedSeq = harvest_stream_state:get_seen_seq(Id, IndexId),
    {ok, StreamPid} = start_changes_stream(SpaceId, LastProcessedSeq + 1),
    State#state{
        mode = stream,
        stream = StreamPid
    }.


-spec consume_change_and_enter_retry_mode_on_error(state(),
    datastore:doc() | [datastore:doc()]) -> state().
consume_change_and_enter_retry_mode_on_error(State = #state{mode = Mode}, []) ->
    % all docs were successfully handled, ensure we are not in 'retry' mode
    State#state{
        mode = maybe_enter_resuming_mode(Mode),
        backoff = undefined
    };
consume_change_and_enter_retry_mode_on_error(State = #state{
    id = Id,
    harvester_id = HarvesterId,
    space_id = SpaceId,
    stream = Stream
}, Docs = [Doc | RestDocs]) ->
    State2 = try
        consume_change(State, Doc)
    catch
        Error:Reason ->
            ?error_stacktrace("Unexpected error ~p:~p in harvest_stream ~p "
            "handling changes for harvester ~p in space ~p",
                [Error, Reason, Id, HarvesterId, SpaceId]),
            stop_changes_stream(Stream),
            enter_retry_mode(Docs, State)
    end,
    case State2#state.mode of
        retry ->
            State2;
        _ ->
            consume_change_and_enter_retry_mode_on_error(State2, RestDocs)
    end;
consume_change_and_enter_retry_mode_on_error(State, Doc = #document{}) ->
    consume_change_and_enter_retry_mode_on_error(State, [Doc]).


-spec consume_change(state(), datastore:doc()) -> state().
consume_change(State = #state{
    id = Id,
    index_id = IndexId,
    harvester_id = HarvesterId,
    provider_id = ProviderId
}, #document{
    seq = Seq,
    value = #custom_metadata{
        file_objectid = FileId,
        value = #{<<"onedata_json">> := JSON}
    },
    mutators = [ProviderId | _],
    deleted = false
}) when map_size(JSON) > 0 ->
    % it is possible that we will push the same JSON many times as change
    % may have been triggered by modification of key/value or rdf metadata
    % todo VFS-5357 improve tracking progress of harvesting per index
    MaxRelevantSeq = harvest_stream_state:get_max_relevant_seq(Id),
    % todo VFS-5352 handle FailedIndices
    {ok, [] = _FailedIndices} = harvester_logic:submit_entry(HarvesterId, FileId, JSON, [IndexId], Seq, max(Seq, MaxRelevantSeq)),
    ok = harvest_stream_state:set_seq(Id, IndexId, Seq, relevant),
    State#state{last_persisted_seq = Seq};
consume_change(State = #state{
    id = Id,
    index_id = IndexId,
    harvester_id = HarvesterId,
    provider_id = ProviderId
}, #document{
    seq = Seq,
    value = #custom_metadata{file_objectid = FileId},
    mutators = [ProviderId | _]
}) ->
    % delete entry because one of the following happened:
    %   * onedata_json key is missing in #custom_metadat.value map
    %   * custom_metadata document has_been deleted
    % todo VFS-5357 improve tracking progress of harvesting per index
    MaxRelevantSeq = harvest_stream_state:get_max_relevant_seq(Id),
    % todo VFS-5352 handle FailedIndices
    {ok, [] = _FailedIndices} = harvester_logic:delete_entry(HarvesterId, FileId, [IndexId], Seq, max(Seq, MaxRelevantSeq)),
    ok = harvest_stream_state:set_seq(Id, IndexId, Seq, relevant),
    State#state{last_persisted_seq = Seq};
consume_change(State = #state{
    id = Id,
    index_id = IndexId,
    last_persisted_seq = LastSeq
}, #document{seq = Seq})
    when (Seq - LastSeq) > ?IGNORED_SEQ_REPORTING_FREQUENCY ->
    % Only sequence numbers associated with #custom_metadata{} are persisted
    % in harvest_stream_state each time (previous clauses).
    % Other seen sequence numbers are not persisted every time as it would
    % overload the db, they are persisted once per batch.
    % The last persisted seq is cached in #state.last_persisted_seq.
    % If the gap between Seq and #state.last_persisted_seq is greater
    % than the defined constant (?IGNORED_SEQ_REPORTING_FREQUENCY),
    % the Seq is persisted.
    ok = harvest_stream_state:set_seq(Id, IndexId, Seq, ignored),
    State#state{last_persisted_seq = Seq};
consume_change(State, _Doc) ->
    State.


-spec enter_retry_mode(datastore:doc() | [datastore:doc()], state()) -> state().
enter_retry_mode(DocOrDocs, State = #state{backoff = undefined}) ->
    B0 = backoff:init(?MIN_BACKOFF_INTERVAL, ?MAX_BACKOFF_INTERVAL, self(), ?RETRY),
    B1 = backoff:type(B0, jitter),
    ?error("Next retry after: ~p seconds", [?MIN_BACKOFF_INTERVAL /1000]),
    backoff:fire(B1),
    State#state{
        docs_to_retry = DocOrDocs,
        mode = retry,
        backoff = B1
    };
enter_retry_mode(DocOrDocs, State = #state{backoff = B}) ->
    {Value, B2} = backoff:fail(B),
    ?error("Next retry after: ~p seconds", [Value /1000]),
    backoff:fire(B2),
    State#state{
        docs_to_retry = DocOrDocs,
        mode = retry,
        backoff = B2
    }.

-spec maybe_enter_resuming_mode(mode()) -> mode().
maybe_enter_resuming_mode(stream) -> stream;
maybe_enter_resuming_mode(retry) -> resuming.