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


-define(DEFAULT_BUCKET, <<"onedata">>).

%% API
-export([start_link/3, id/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {
    id :: id(),
    harvester_id :: od_harvester:id(),
    space_id :: od_space:id(),
    provider_id :: od_provider:id()
}).

-type state() :: #state{}.
-type id() :: binary().

-export_type([id/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(id(), od_harvester:id(), od_space:id()) -> {ok, pid()} | {error, Reason :: term()}.
start_link(Id, HarvesterId, SpaceId) ->
    gen_server:start_link({local, binary_to_atom(Id, latin1)}, ?MODULE, [Id, HarvesterId, SpaceId], []).

-spec id(od_harvester:id(), od_space:id()) -> id().
id(HarvesterId, SpaceId) ->
    harvest_stream_state:id(HarvesterId, SpaceId).

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
init([Id, HarvesterId, SpaceId]) ->
    Stream = self(),
    Since = harvest_stream_state:get_seq(Id),
    Callback = fun(Change) -> gen_server2:cast(Stream, {change, Change}) end,
    {ok, _} = couchbase_changes_stream:start_link(
        ?DEFAULT_BUCKET, SpaceId, Callback,
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
handle_cast({change, {ok, Docs}}, State = #state{
    id = Id,
    harvester_id = HarvesterId,
    provider_id = ProviderId
})
    when is_list(Docs)
->
    [handle_change(Id, HarvesterId, ProviderId, Doc) || Doc <- Docs],
    {noreply, State};
handle_cast({change, {ok, #document{} = Doc}}, State = #state{
    id = Id,
    harvester_id = HarvesterId,
    provider_id = ProviderId
}) ->
    handle_change(Id, HarvesterId, ProviderId, Doc),
    {noreply, State};
handle_cast({change, {ok, end_of_stream}}, State) ->
    {stop, normal, State};
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

-spec handle_change(id(), od_harvester:id(), od_provider:id(), datastore:document()) -> ok.
handle_change(Id, HarvesterId, ProviderId, Doc = #document{
    seq = Seq,
    value = #custom_metadata{},
    mutators = [ProviderId | _ ]
}) ->
    ToSend = doc_to_map(Doc),
    harvester_logic:submit(?ROOT_SESS_ID, HarvesterId, ToSend),
    ok = harvest_stream_state:set_seq(Id, Seq);
handle_change(Id, _, _, #document{seq = Seq}) ->
    ok = harvest_stream_state:set_seq(Id, Seq).

-spec doc_to_map(datastore:document()) -> maps:map().
doc_to_map(#document{
    value = #custom_metadata{
        file_objectid = FileId,
        value = Metadata
    },
    scope = SpaceId,
    deleted = Deleted
}) ->
    ToSubmit = #{
        <<"id">> => FileId,
        <<"deleted">> => json_utils:encode(Deleted),
        <<"spaceId">> => SpaceId
    },
    maps:fold(fun
        (<<"onedata_json">>, JSON, AccIn) -> AccIn#{<<"json">> => json_utils:encode(JSON)};
        (<<"onedata_rdf">>, RDF, AccIn) -> AccIn#{<<"rdf">> => json_utils:encode(RDF)};
        (Key, Value, AccIn) -> AccIn#{Key => Value}
    end, ToSubmit, Metadata).
