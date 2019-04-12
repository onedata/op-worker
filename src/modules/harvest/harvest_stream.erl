%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for receiving changes from changes stream and
%%% processing them. It pushes changes of custom_metadata document
%%% to Onezone. One instance is started per triple
%%% {HarvesterId, SpaceId, IndexId}. Streams started for the same
%%% HarvesterId and SpaceId differ only in level of processing changes
%%% from the scope.
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
    stream :: pid(),
    retry_status = off :: retry_status(),
    % flag that informs whether all changes received in retry mode were flushed
    changes_flushed = false :: boolean(),
    docs_to_retry = [] :: datastore:doc() | [datastore:doc()],
    backoff :: term()
}).

-type state() :: #state{}.
-type retry_status() :: on | off | done.


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
    LastProcessedSeq = harvest_stream_state:get_seq(Id, IndexId),
    {ok, StreamPid} = start_changes_stream(SpaceId, LastProcessedSeq + 1),
    ?debug("Started harvest_stream: ~p", [{HarvesterId, SpaceId, IndexId}]),
    {ok, #state{
        id = Id,
        index_id = IndexId,
        space_id = SpaceId,
        harvester_id = HarvesterId,
        provider_id = oneprovider:get_id(),
        stream = StreamPid
    }}.


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
handle_cast(_Request, #state{retry_status = RetryStatus} = State)
    when RetryStatus =:= on orelse RetryStatus =:= done ->
    % ignore because stream is in retry mode
    {noreply, State};
handle_cast({change, {ok, end_of_stream}}, State = #state{retry_status = on}) ->
    % all changes sent by changes_stream before stopping it has already arrived
    {noreply, State#state{changes_flushed = true}};
handle_cast({change, {ok, end_of_stream}}, State = #state{retry_status = done}) ->
    % all changes sent by changes_stream before stopping it has already arrived
    % and retry is finished, so we can get back to normal operation
    {noreply, restore_normal_operation(State)};
handle_cast({change, {ok, end_of_stream}}, State = #state{retry_status = off}) ->
    {stop, normal, State};
handle_cast({change, {ok, DocOrDocs}}, State = #state{retry_status = off}) ->
    {noreply, handle_change_and_schedule_retry_on_error(State, DocOrDocs)};
handle_cast({change, {error, _Seq, Reason}}, State = #state{retry_status = off}) ->
    {stop, Reason, State};
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
    retry_status = on,
    docs_to_retry = DocOrDocs,
    changes_flushed = ChangesFlushed
}) ->
    State2 = handle_change_and_schedule_retry_on_error(State, DocOrDocs),
    case ChangesFlushed andalso (State2#state.retry_status =:= done) of
        true ->
            {noreply, restore_normal_operation(State2)};
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

restore_normal_operation(State = #state{id = Id, index_id = IndexId, space_id = SpaceId}) ->
    LastProcessedSeq = harvest_stream_state:get_seq(Id, IndexId),
    {ok, StreamPid} = start_changes_stream(SpaceId, LastProcessedSeq + 1),
    State#state{
        retry_status = off,
        stream = StreamPid
    }.


-spec handle_change_and_schedule_retry_on_error(state(),
    datastore:doc() | [datastore:doc()]) -> state().
handle_change_and_schedule_retry_on_error(State = #state{retry_status = RetryStatus}, []) ->
    % all docs were successfully handled, ensure we are not in_retry_mode
    State#state{
        retry_status = maybe_mark_status_done(RetryStatus),
        backoff = undefined
    };
handle_change_and_schedule_retry_on_error(State = #state{
    id = Id,
    harvester_id = HarvesterId,
    space_id = SpaceId,
    stream = Stream
}, Docs = [Doc | RestDocs]) ->
    try
        State2 = handle_change(State, Doc),
        handle_change_and_schedule_retry_on_error(State2, RestDocs)
    catch
        Error:Reason ->
            ?error_stacktrace("Unexpected error ~p:~p in harvest_stream ~p "
            "handling changes for harvester ~p in space ~p",
                [Error, Reason, Id, HarvesterId, SpaceId]),
            catch gen_server:stop(Stream),
            schedule_retry(Docs, State)
    end;
handle_change_and_schedule_retry_on_error(State = #state{
    id = Id,
    harvester_id = HarvesterId,
    space_id = SpaceId,
    stream = Stream,
    retry_status = RetryStatus
}, Doc) ->
    try
        State2 = handle_change(State, Doc),
        State2#state{
            retry_status = maybe_mark_status_done(RetryStatus),
            backoff = undefined
        }
    catch
        Error:Reason ->
            ?error_stacktrace("Unexpected error ~p:~p in harvest_stream ~p "
            "handling changes for harvester ~p in space ~p",
                [Error, Reason, Id, HarvesterId, SpaceId]),
            catch gen_server:stop(Stream),
            schedule_retry(Doc, State)
    end.


-spec handle_change(state(), datastore:doc()) -> state().
handle_change(State = #state{
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
handle_change(State = #state{
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
handle_change(State = #state{
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
handle_change(State, _Doc) ->
    State.


-spec schedule_retry(datastore:doc() | [datastore:doc()], state()) -> state().
schedule_retry(DocOrDocs, State = #state{backoff = undefined}) ->
    B0 = backoff:init(?MIN_BACKOFF_INTERVAL, ?MAX_BACKOFF_INTERVAL, self(), ?RETRY),
    B1 = backoff:type(B0, jitter),
    ?error("Next retry after: ~p seconds", [?MIN_BACKOFF_INTERVAL /1000]),
    backoff:fire(B1),
    State#state{
        docs_to_retry = DocOrDocs,
        retry_status = on,
        backoff = B1
    };
schedule_retry(DocOrDocs, State = #state{backoff = B}) ->
    {Value, B2} = backoff:fail(B),
    ?error("Next retry after: ~p seconds", [Value /1000]),
    backoff:fire(B2),
    State#state{
        docs_to_retry = DocOrDocs,
        retry_status = on,
        backoff = B2
    }.

-spec maybe_mark_status_done(retry_status()) -> retry_status().
maybe_mark_status_done(off) -> off;
maybe_mark_status_done(on) -> done.