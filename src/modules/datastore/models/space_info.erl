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

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").

%% API
-export([fetch/2, create_or_update/2, get_or_fetch/2]).

%% model_behaviour callbacks
-export([save/1, get/1, list/0, exists/1, delete/1, update/2, create/1, model_init/0,
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
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    datastore:list(?STORE_LEVEL, ?MODEL_NAME, ?GET_ALL, []).

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
    ?MODEL_CONFIG(space_info_bucket, [], ?GLOBAL_ONLY_LEVEL).

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
%% Get user from cache or fetch from OZ and save in cache.
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch(Client :: oz_endpoint:client(), datastore:key()) ->
    {ok, datastore:document()} | datastore:get_error().
get_or_fetch(Client, Key) ->
    case space_info:get(Key) of
        {ok, Doc} -> {ok, Doc};
        {error, {not_found, _}} -> fetch(Client, Key);
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Fetches space details from Global Registry and stores them in database.
%% @end
%%--------------------------------------------------------------------
-spec fetch(Client :: oz_endpoint:client(), SpaceId :: binary()) ->
    {ok, datastore:document()} | datastore:get_error().
fetch(Client, SpaceId) ->
    {ok, #space_details{id = Id, name = Name, size = Size}} =
        oz_spaces:get_details(Client, SpaceId),
    {ok, GroupIds} = oz_spaces:get_groups(Client, Id),
    {ok, UserIds} = oz_spaces:get_users(Client, Id),

    GroupsWithPrivileges = utils:pmap(fun(GID) ->
        {ok, Privileges} = oz_spaces:get_group_privileges(Client, Id, GID),
        {GID, Privileges}
    end, GroupIds),
    UsersWithPrivileges = utils:pmap(fun(UID) ->
        {ok, Privileges} = oz_spaces:get_user_privileges(Client, Id, UID),
        {UID, Privileges}
    end, UserIds),

    Doc = #document{key = Id, value = #space_info{
        users = UsersWithPrivileges,
        groups = GroupsWithPrivileges,
        providers_supports = Size,
        name = Name}},
    {ok, _} = space_info:create(Doc),
    {ok, Doc}.