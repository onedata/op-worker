%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Globalregistry user data cache
%%% @end
%%%-------------------------------------------------------------------
-module(onedata_user).
-author("Tomasz Lichon").
-behaviour(model_behaviour).

-include("modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_internal.hrl").
-include("proto_internal/oneclient/handshake_messages.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).

%% API
-export([fetch/1, get_or_fetch/2]).

-export_type([id/0]).

-type id() :: binary().

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
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(global_only, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(onedata_user_bucket, []).

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
%% Fetch user from globalregistry and save it in cache.
%% @end
%%--------------------------------------------------------------------
-spec fetch(Token :: #token{}) -> {ok, datastore:document()} | datastore:get_error().
fetch(#token{value = Token}) ->
    case gr_users:get_details({user, Token}) of
        {ok, #user_details{id = Id, name = Name}} ->
            NewDoc = #document{key = Id, value = #onedata_user{name = Name}},
            case onedata_user:save(NewDoc) of
                {ok, _} -> {ok, NewDoc};
                Error -> Error
            end;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get user from cache or fetch from globalregistry and save in cache.
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch(datastore:key(), #token{}) ->
    {ok, datastore:document()} | datastore:get_error().
get_or_fetch(Key, Token) ->
    case onedata_user:get(Key) of
        {ok, Doc} -> {ok, Doc};
        {error, {not_found, missing_or_deleted}} -> fetch(Token);
        Error -> Error
    end.