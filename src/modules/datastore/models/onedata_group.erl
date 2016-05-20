%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% OZ group data cache
%%% @end
%%%-------------------------------------------------------------------
-module(onedata_group).
-author("Tomasz Lichon").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/common/credentials.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/oz/oz_groups.hrl").
-include_lib("ctool/include/logging.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, list/0, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).

%% API
-export([fetch/2, get_or_fetch/2, create_or_update/2]).

-export_type([id/0, type/0]).

-type type() :: 'organization' | 'unit' | 'team' | 'role'.
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
    % TODO migrate to GLOBALLY_CACHED_LEVEL
    ?MODEL_CONFIG(onedata_group_bucket, [], ?DISK_ONLY_LEVEL).

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
%% Fetch group from OZ and save it in cache.
%% @end
%%--------------------------------------------------------------------
-spec fetch(Client :: oz_endpoint:client(), GroupId :: id()) ->
    {ok, datastore:document()} | {error, Reason :: term()}.
fetch(Client, GroupId) ->
    ?dump(fetch_start),
    try
        {ok, #group_details{id = Id, name = Name, type = Type}} =
            oz_groups:get_details(Client, GroupId),
        {ok, SpaceIds} = oz_groups:get_spaces(Client, Id),
        {ok, ParentIds} = oz_groups:get_parents(Client, Id),

        % nested groups
        {ok, NestedIds} = oz_groups:get_nested(Client, Id),
        NestedGroupsWithPrivileges = utils:pmap(fun(UID) ->
            {ok, Privileges} = oz_groups:get_nested_privileges(Client, Id, UID),
            {UID, Privileges}
        end, NestedIds),

        % users
        {ok, UserIds} = oz_groups:get_users(Client, Id),
        UsersWithPrivileges = utils:pmap(fun(UID) ->
            {ok, Privileges} = oz_groups:get_user_privileges(Client, Id, UID),
            {UID, Privileges}
        end, UserIds),

        % effective users
        {ok, EffectiveUserIds} = oz_groups:get_effective_users(Client, Id),
        EffectiveUsersWithPrivileges = utils:pmap(fun(UID) ->
            {ok, Privileges} = oz_groups:get_effective_user_privileges(Client, Id, UID),
            {UID, Privileges}
        end, EffectiveUserIds),

        %todo consider getting user_details for each group member and storing it as onedata_user
        OnedataGroupDoc = #document{key = Id, value = #onedata_group{
            users = UsersWithPrivileges, spaces = SpaceIds, name = Name,
            effective_users = EffectiveUsersWithPrivileges, type = Type,
            parent_groups = ParentIds, nested_groups = NestedGroupsWithPrivileges}},
        {ok, _} = onedata_user:save(OnedataGroupDoc),
        ?dump({fetch_end, OnedataGroupDoc}),
        {ok, OnedataGroupDoc}
    catch
        _:Reason ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get group from cache or fetch from OZ and save in cache.
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch(Client :: oz_endpoint:client(), GroupId :: onedata_group:id()) ->
    {ok, datastore:document()} | datastore:get_error().
get_or_fetch(Client, GroupId) ->
    case onedata_group:get(GroupId) of
        {ok, Doc} -> {ok, Doc};
        {error, {not_found, _}} -> fetch(Client, GroupId);
        Error -> Error
    end.
