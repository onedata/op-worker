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
%%%  +---------------+                     +--------------+
%%%  |   streaming   |-----on-failure----->|   retrying   |
%%%  +---------------+                     +--------------+
%%%           ^                                   |
%%            |                                   |
%%%           +------------on-success-------------+
%%%
%%% Modes:
%%%  - streaming - in this mode, harvest_stream processes changes received
%%%                from couchbase_changes_stream. If the change is processed
%%%                successfully, the process remains in the "streaming" state.
%%%                Otherwise, it stops couchbase_changes_stream and moves to the
%%%                "retrying" state.
%%%  - retrying -  in this mode, harvest_stream retries processing of
%%%                given change/changes using backoff algorithm.
%%%                Before starting the retries, the process ensures that
%%%                couchbase_changes_stream is stopped so that no new messages
%%%                will arrive when performing retries.
%%%                When all changes are successfully processed, harvest_stream
%%%                moves to the `streaming` mode.
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
    stream_pid :: undefined | pid(),
    mode = streaming :: mode(),
    docs_to_retry = [] :: [datastore:doc()],
    backoff :: term()
}).

-type state() :: #state{}.
-type mode() :: streaming | retrying.


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
    State2 = enter_streaming_mode(State),
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
handle_call({change, {ok, DocOrDocs}}, From, State = #state{mode = streaming}) ->
    gen_server2:reply(From, ok),
    {noreply, consume_change_and_retry_on_error(State, DocOrDocs)};
handle_call({change, {ok, end_of_stream}}, _From, State = #state{mode = retrying}) ->
    % all changes sent by changes_stream before stopping it have already arrived
    % so we can start backoff algorithm
    {reply, ok, schedule_backoff(State#state{stream_pid = undefined})};
handle_call({change, {ok, DocOrDocs}}, _From, #state{mode = retrying, docs_to_retry = DocsToRetry} = State) ->
    % changes sent by changes_stream before stopping it 
    % add to list of docs to retry
    {reply, ok, State#state{docs_to_retry = DocsToRetry ++ ensure_list(DocOrDocs)}};
handle_call(Request, _From, #state{} = State) ->
    ?log_bad_request(Request),
    {reply, ok, State}.


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
    mode = retrying,
    stream_pid = undefined,
    docs_to_retry = Docs
}) ->
    {noreply, consume_change_and_retry_on_error(State, Docs)};
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
    Callback = fun(Change) -> gen_server2:call(Self, {change, Change}, infinity) end,
    couchbase_changes_stream:start_link(
        couchbase_changes:design(), SpaceId, Callback,
        [{since, Since}, {until, infinity}], []
    ).

-spec stop_changes_stream(state()) -> state().
stop_changes_stream(State = #state{stream_pid = undefined}) ->
    State;
stop_changes_stream(State = #state{stream_pid = StreamPid}) ->
    case process_info(StreamPid) of
        undefined ->
            State#state{stream_pid = undefined};
        _ ->
            couchbase_changes_stream:stop_async(StreamPid),
            State
    end.

-spec consume_change_and_retry_on_error(state(),
    datastore:doc() | [datastore:doc()]) -> state().
consume_change_and_retry_on_error(State = #state{mode = streaming}, []) ->
    % all docs were successfully handled
    State;
consume_change_and_retry_on_error(State = #state{
    mode = retrying,
    stream_pid = undefined
}, []) ->
    % all docs were successfully handled in retrying mode, get back to streaming mode
    enter_streaming_mode(State);
consume_change_and_retry_on_error(State = #state{
    id = Id,
    harvester_id = HarvesterId,
    space_id = SpaceId
}, Docs = [Doc | RestDocs]) ->
    {State3, ConsumingFailed} = try
        State2 = consume_change(State, Doc),
        {State2#state{backoff = undefined}, false}
    catch
        Error:Reason ->
            ?error_stacktrace("Unexpected error ~p:~p in harvest_stream ~p "
            "handling changes for harvester ~p in space ~p",
                [Error, Reason, Id, HarvesterId, SpaceId]),
            {enter_retrying_mode(Docs, stop_changes_stream(State)), true}
    end,
    case ConsumingFailed of
        true ->
            % if consuming change failed, backoff algorithm will trigger retries
            State3;
        _ ->
            consume_change_and_retry_on_error(State3, RestDocs)
    end;
consume_change_and_retry_on_error(State, Doc = #document{}) ->
    consume_change_and_retry_on_error(State, [Doc]).


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Start/restart changes stream and mark mode as `stream`.
%% @end
%%-------------------------------------------------------------------
-spec enter_streaming_mode(state()) -> state().
enter_streaming_mode(State = #state{id = Id, index_id = IndexId, space_id = SpaceId}) ->
    LastProcessedSeq = harvest_stream_state:get_seen_seq(Id, IndexId),
    {ok, StreamPid} = start_changes_stream(SpaceId, LastProcessedSeq + 1),
    State#state{
        mode = streaming,
        stream_pid = StreamPid,
        backoff = undefined
    }.

-spec enter_retrying_mode([datastore:doc()], state()) -> state().
enter_retrying_mode(Docs, State = #state{stream_pid = undefined}) ->
    %% stream_pid is undefined which means that stream has been stopped and we
    %% can start backoff algorithm
    schedule_backoff(State#state{docs_to_retry = Docs, mode = retrying});
enter_retrying_mode(Docs, State) ->
    %% stream_pid is not undefined which means that `end_of_stream` message
    %% hasn't arrived yet
    State#state{
        docs_to_retry = Docs,
        mode = retrying
    }.

schedule_backoff(State = #state{backoff = undefined}) ->
    B0 = backoff:init(?MIN_BACKOFF_INTERVAL, ?MAX_BACKOFF_INTERVAL, self(), ?RETRY),
    B1 = backoff:type(B0, jitter),
    ?error("Next retry after: ~p seconds", [?MIN_BACKOFF_INTERVAL /1000]),
    backoff:fire(B1),
    State#state{backoff = B1};
schedule_backoff(State = #state{backoff = B0}) ->
    {Value, B1} = backoff:fail(B0),
    ?error("Next retry after: ~p seconds", [Value /1000]),
    backoff:fire(B1),
    State#state{backoff = B1}.


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

-spec ensure_list(term()) -> [term()].
ensure_list(List) when is_list(List) -> List;
ensure_list(Element) -> [Element].