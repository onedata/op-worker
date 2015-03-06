%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Session management model, frequently invoked by incomming tcp
%%% connections in connection
%%% @end
%%%-------------------------------------------------------------------
-module(session).
-author("Tomasz Lichon").
-behaviour(model_behaviour).

-include("modules/datastore/datastore.hrl").
-include("modules/datastore/models/session.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, list/0, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).

%% API
-export([get_session_supervisor_and_node/1, get_event_manager/1,
    get_sequencer_manager/1, get_communicator/1]).

-export_type([id/0, credentials/0]).

-type id() :: binary().
-type credentials() :: #credentials{}.

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) -> {ok, datastore:key()} | datastore:generic_error().
save(Document) ->
    datastore:save(global_only, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(global_only, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    datastore:create(global_only, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(global_only, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error().
list() ->
    mnesia_cache_driver:list(model_init()).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(global_only, ?MODULE, Key, ?PRED_ALWAYS).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> true | false | datastore:generic_error().
exists(Key) ->
    datastore:exists(global_only, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(session_bucket, []).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns session supervisor and node on which supervisor is running.
%% @end
%%--------------------------------------------------------------------
-spec get_session_supervisor_and_node(SessId :: id()) ->
    {ok, {SessSup :: pid(), Node :: node()}} | {error, Reason :: term()}.
get_session_supervisor_and_node(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{session_sup = undefined}}} ->
            {error, {not_found, missing}};
        {ok, #document{value = #session{session_sup = SessSup, node = Node}}} ->
            {ok, {SessSup, Node}};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns event manager associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_event_manager(SessId :: id()) ->
    {ok, EvtMan :: pid()} | {error, Reason :: term()}.
get_event_manager(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{event_manager = undefined}}} ->
            {error, {not_found, missing}};
        {ok, #document{value = #session{event_manager = EvtMan}}} ->
            {ok, EvtMan};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns sequencer manager associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_sequencer_manager(SessId :: id()) ->
    {ok, SeqMan :: pid()} | {error, Reason :: term()}.
get_sequencer_manager(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{sequencer_manager = undefined}}} ->
            {error, {not_found, missing}};
        {ok, #document{value = #session{sequencer_manager = SeqMan}}} ->
            {ok, SeqMan};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns communicator associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_communicator(SessId :: id()) ->
    {ok, Comm :: pid()} | {error, Reason :: term()}.
get_communicator(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{communicator = undefined}}} ->
            {error, {not_found, missing}};
        {ok, #document{value = #session{communicator = Comm}}} ->
            {ok, Comm};
        {error, Reason} ->
            {error, Reason}
    end.

