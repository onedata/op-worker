%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Session management model, frequently invoked by incomming tcp
%%% connections in protocol_handler
%%% @end
%%%-------------------------------------------------------------------
-module(session).
-author("Tomasz Lichon").
-behaviour(model_behaviour).

-include("workers/datastore/datastore.hrl").
-include("workers/datastore/models/session.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, model_init/0, 'after'/5, before/4]).

%% API
-export([remove_connection/2, create_or_reuse_session/3]).

-export_type([session_id/0]).

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
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(global_only, ?MODULE, Key).


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
%% Creates new session or reuses existing one. The input args are user
%% credentials and connection pid.
%% @end
%%--------------------------------------------------------------------
-spec create_or_reuse_session(Cred :: #credentials{}, Con :: pid(),
    SessionIdToReuse :: undefined | session_id()) ->
    {ok, SessionId :: session_id()} | datastore:create_error() | datastore:update_error().
create_or_reuse_session(Cred, Con, undefined) ->
    session:create(#document{value = #session{credentials = Cred, connections = [Con]}});
create_or_reuse_session(Cred, Con, SessionIdToReuse) ->
    {ok, #document{value = #session{credentials = Cred}}} = session:get(SessionIdToReuse),
    session:update(SessionIdToReuse, fun(#session{connections = Cons} = Sess) ->
        Sess#session{connections = [Con | Cons]}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Remove connection from session
%% @end
%%--------------------------------------------------------------------
-spec remove_connection(Con :: pid(), SessionId :: session_id()) ->
    {ok, SessionId :: session_id()} | datastore:generic_error() | datastore:update_error().
remove_connection(Con, SessionId) ->
    session:update(SessionId, fun(#session{connections = Cons} = Sess) ->
        Sess#session{connections = Cons -- [Con]}
    end). %todo atomically delete session when there are no connections left