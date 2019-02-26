%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for receiving changes from changes stream and
%%% processing them. One instance is started per pair {space, harvester}.
%%% @end
%%%-------------------------------------------------------------------
-module(harvest_stream).
-author("Jakub Kudzia").

-behaviour(gen_server).

-include("modules/harvest/harvest.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    id :: harvest_stream_state:id(),
    harvester_id :: od_harvester:id(),
    space_id :: od_space:id(),
    provider_id :: od_provider:id(),
    last_persisted_seq = 0
}).

-type state() :: #state{}.

-define(MAX_NOT_PERSISTED_SEQ_GAP, 1000).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(od_harvester:id(), od_space:id()) ->
    {ok, pid()} | {error, Reason :: term()}.
start_link(HarvesterId, SpaceId) ->
    gen_server:start_link(?MODULE, [HarvesterId, SpaceId], []).

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
init([HarvesterId, SpaceId]) ->
    Stream = self(),
    Id = harvest_stream_state:id(HarvesterId, SpaceId),
    Since = harvest_stream_state:get_seq(Id),
    Callback = fun(Change) -> gen_server2:cast(Stream, {change, Change}) end,
    {ok, _} = couchbase_changes_stream:start_link(
        couchbase_changes:design(), SpaceId, Callback,
        [{since, Since}, {until, infinity}], []
    ),
    {ok, #state{
        id = Id,
        space_id = SpaceId,
        harvester_id = HarvesterId,
        provider_id = oneprovider:get_id()
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
handle_cast({change, {ok, end_of_stream}}, State) ->
    {stop, normal, State};
handle_cast({change, {ok, DocOrDocs}}, State) ->
    handle_change_and_stop_on_error(State, DocOrDocs);
handle_cast({change, {error, _Seq, Reason}}, State) ->
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
handle_info(Info, #state{} = State) ->
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

-spec handle_change_and_stop_on_error(state(), datastore:doc() | [datastore:doc()]) ->
    {noreply, state()} | {stop, term(), state()}.
handle_change_and_stop_on_error(State = #state{
    id = Id, harvester_id = HarvesterId, space_id = SpaceId
}, DocOrDocs) ->
    try
        State2 = handle_change(State, DocOrDocs),
        {noreply, State2}
    catch
        Error:Reason ->
            ?error_stacktrace("Unexpected error ~p:~p in harvest_stream ~p "
            "handling changes for harvester ~p in space ~p",
                [Error, Reason, Id, HarvesterId, SpaceId]),
            {stop, {Error, Reason}, State}
    end.

-spec handle_change(state(), datastore:doc() | [datastore:doc()]) -> state().
handle_change(State, Docs) when is_list(Docs) ->
    lists:foldl(fun(Doc, StateIn) ->
        handle_change(StateIn, Doc)
    end, State, Docs);
handle_change(State = #state{
    id = Id,
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
    ok = harvester_logic:harvest(HarvesterId, FileId, JSON),
    ok = harvest_stream_state:set_seq(Id, Seq),
    State#state{last_persisted_seq = Seq};
handle_change(State = #state{
    id = Id,
    harvester_id = HarvesterId,
    provider_id = ProviderId
}, #document{
    seq = Seq,
    value = #custom_metadata{file_objectid = FileId},
    mutators = [ProviderId | _]
}) ->
    % delete entry because one of the following happened:
    %   * onedata_json key is missing
    %   * onedata_json map is empty
    %   * custom_metadata document has_been deleted
    ok = harvester_logic:delete_entry(HarvesterId, FileId),
    ok = harvest_stream_state:set_seq(Id, Seq),
    State#state{last_persisted_seq = Seq};
handle_change(State = #state{id = Id, last_persisted_seq = LastSeq}, #document{seq = Seq})
    when (Seq - LastSeq) > ?MAX_NOT_PERSISTED_SEQ_GAP ->
    ok = harvest_stream_state:set_seq(Id, Seq),
    State#state{last_persisted_seq = Seq};
handle_change(State, _Doc) ->
    State.