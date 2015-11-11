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

-include("modules/datastore/datastore_specific_model.hrl").

-include("proto/oneclient/handshake_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
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
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(?ROOT_USER_ID) ->
    {ok, #document{key = ?ROOT_USER_ID, value = #onedata_user{name = <<"root">>}}};
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(onedata_user_bucket, [], ?GLOBAL_ONLY_LEVEL).

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
-spec fetch(Auth :: #auth{}) -> {ok, datastore:document()} | {error, Reason :: term()}.
fetch(#auth{macaroon = SrlzdMacaroon, disch_macaroons = SrlzdDMacaroons}) ->
    try
        {ok, #user_details{id = Id, name = Name}} =
            gr_users:get_details({user, {SrlzdMacaroon, SrlzdDMacaroons}}),
        {ok, #user_spaces{ids = SpaceIds, default = DefaultSpaceId}} =
            gr_users:get_spaces({user, {SrlzdMacaroon, SrlzdDMacaroons}}),
        OnedataUser = #onedata_user{
            name = Name, space_ids = [DefaultSpaceId | SpaceIds -- [DefaultSpaceId]]
        },
        OnedataUserDoc = #document{key = Id, value = OnedataUser},
        {ok, _} = onedata_user:save(OnedataUserDoc),
        {ok, OnedataUserDoc}
    catch
        _:Reason ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get user from cache or fetch from globalregistry and save in cache.
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch(datastore:key(), #auth{}) ->
    {ok, datastore:document()} | datastore:get_error().
get_or_fetch(Key, Token) ->
    case onedata_user:get(Key) of
        {ok, Doc} -> {ok, Doc};
        {error, {not_found, _}} -> fetch(Token);
        Error -> Error
    end.