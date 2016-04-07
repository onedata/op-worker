%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Cache for space details fetched from Global Registry.
%%% @end
%%%-------------------------------------------------------------------
-module(space_info).
-author("Krzysztof Trzepla").
-behaviour(model_behaviour).

-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").

%% API
-export([get/2, fetch/1, fetch/2, create_or_update/2]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, model_init/0,
    'after'/5, before/4]).

-type id() :: binary().

-export_type([id/0]).

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) -> {ok, datastore:ext_key()} | datastore:generic_error().
save(Document) ->
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:ext_key()} | datastore:create_error().
create(Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:ext_key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(space_info_bucket, [], ?GLOBALLY_CACHED_LEVEL).

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
%% Updates document with using ID from document. If such object does not exist,
%% it initialises the object with the document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
create_or_update(Doc, Diff) ->
    datastore:create_or_update(?STORE_LEVEL, Doc, Diff).

%%--------------------------------------------------------------------
%% @doc
%% Gets space details from the database in user context.
%% @end
%%--------------------------------------------------------------------
-spec get(SpaceId :: binary(), SessId :: session:id()) ->
    {ok, datastore:document()} | datastore:get_error().
get(SpaceId, SessId) ->
    Key = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} =
        session:get(SessId),
    case datastore:fetch_link(?LINK_STORE_LEVEL, Key, ?MODEL_NAME, UserId) of
        {ok, {LinkKey, _}} -> space_info:get(LinkKey);
        {error, link_not_found} -> {error, {not_found, ?MODEL_NAME}};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Fetches space details from Global Registry in provider context and stores them
%% in the database.
%% @end
%%--------------------------------------------------------------------
-spec fetch(SpaceId :: binary()) -> {ok, datastore:document()} | datastore:get_error().
fetch(SpaceId) ->
    Key = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    {ok, #space_details{id = Id, name = Name}} = oz_spaces:get_details(provider, SpaceId),
    Doc = #document{key = Key, value = #space_info{id = Id, name = Name}},
    {ok, _} = space_info:save(Doc),
    {ok, Doc}.

%%--------------------------------------------------------------------
%% @doc
%% Fetches space details from Global Registry in user context and stores them
%% in the database.
%% @end
%%--------------------------------------------------------------------
-spec fetch(SpaceId :: binary(), SessId :: session:id()) ->
    {ok, datastore:document()} | datastore:get_error().
fetch(SpaceId, ?ROOT_SESS_ID) ->
    {ok, SpaceDetails} = oz_spaces:get_details(provider, SpaceId),
    fetch(SpaceId, ?ROOT_SESS_ID, ?ROOT_USER_ID, SpaceDetails);
fetch(SpaceId, SessId) ->
    {ok, #document{value = #session{
        auth = #auth{macaroon = Macaroon, disch_macaroons = DischMacaroons},
        identity = #identity{user_id = UserId}
    }}} = session:get(SessId),
    {ok, SpaceDetails} = oz_spaces:get_details(
        {user, {Macaroon, DischMacaroons}}, SpaceId),
    fetch(SpaceId, SessId, UserId, SpaceDetails).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates space details in the database in user context.
%% @end
%%--------------------------------------------------------------------
-spec fetch(SpaceId :: binary(), SessId :: session:id(), UserId :: onedata_user:id(),
    SpaceDetails :: #space_details{}) -> {ok, datastore:document()} | datastore:get_error().
fetch(SpaceId, SessId, UserId, #space_details{id = Id, name = Name}) ->
    case space_info:get(SpaceId, SessId) of
        {ok, #document{value = SpaceInfo} = Doc} ->
            NewDoc = Doc#document{value = SpaceInfo#space_info{id = Id, name = Name}},
            {ok, _} = space_info:save(NewDoc),
            {ok, NewDoc};
        {error, {not_found, _}} ->
            {ok, #document{key = ParentKey}} = fetch(SpaceId),
            Doc = #document{value = #space_info{id = Id, name = Name}},
            {ok, Key} = space_info:save(Doc),
            ok = datastore:add_links(?LINK_STORE_LEVEL, ParentKey, ?MODEL_NAME, {UserId, {Key, ?MODEL_NAME}}),
            {ok, Doc};
        {error, Reason} ->
            {error, Reason}
    end.