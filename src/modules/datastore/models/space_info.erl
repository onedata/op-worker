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
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").

%% API
-export([create_or_update/2, get/2, get_or_fetch/3, get_or_fetch/2]).

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
    case datastore:get(?STORE_LEVEL, ?MODULE, Key) of
        {error, Reason} ->
            {error, Reason};
        {ok, D = #document{value = S = #space_info{providers_supports = Supports}}} when is_list(Supports) ->
            {ProviderIds, _} = lists:unzip(Supports),
            {ok, D#document{value = S#space_info{providers = ProviderIds}}};
        {ok, Doc} ->
            {ok, Doc}
    end .

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
    ?MODEL_CONFIG(space_info_bucket, [{space_info, create}, {space_info, save},
        {space_info, delete}, {space_info, create_or_update}], ?DISK_ONLY_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(space_info, create, _, _, {ok, SpaceId}) ->
    worker_proxy:cast(monitoring_worker, {start, space, SpaceId, storage_used}),
    worker_proxy:cast(monitoring_worker, {start, space, SpaceId, storage_quota});
'after'(space_info, create_or_update, _, _, {ok, SpaceId}) ->
    worker_proxy:cast(monitoring_worker, {start, space, SpaceId, storage_used}),
    worker_proxy:cast(monitoring_worker, {start, space, SpaceId, storage_quota});
'after'(space_info, save, _, _, {ok, SpaceId}) ->
    worker_proxy:cast(monitoring_worker, {start, space, SpaceId, storage_used}),
    worker_proxy:cast(monitoring_worker, {start, space, SpaceId, storage_quota});
'after'(space_info, delete, _, _, SpaceId) ->
    worker_proxy:cast(monitoring_worker, {stop, space, SpaceId, storage_used}),
    worker_proxy:cast(monitoring_worker, {stop, space, SpaceId, storage_quota});
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
%% Gets space info from the database in user context.
%% @end
%%--------------------------------------------------------------------
-spec get(SpaceId :: binary(), UserId :: onedata_user:id()) ->
    {ok, datastore:document()} | datastore:get_error().
get(SpaceId, ?ROOT_USER_ID) ->
    case space_info:get(SpaceId) of
        {ok, Doc} -> {ok, Doc};
        {error, Reason} -> {error, Reason}
    end;
get(SpaceId, UserId) ->
    case get(SpaceId, ?ROOT_USER_ID) of
        {ok, #document{value = SpaceInfo} = Doc} ->
            case onedata_user:get(UserId) of
                {ok, #document{value = #onedata_user{spaces = Spaces}}} ->
                    {_, SpaceName} = lists:keyfind(SpaceId, 1, Spaces),
                    {ok, Doc#document{value = SpaceInfo#space_info{name = SpaceName}}};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets space info from the database in user context. If space info is not found
%% fetches it from onezone and stores it in the database.
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch(session:id(), SpaceId :: binary()) ->
    {ok, datastore:document()} | datastore:get_error().
get_or_fetch(SessionId, SpaceId) ->
    Client = fslogic_utils:session_to_rest_client(SessionId),
    {ok, UserId} = session:get_user_id(SessionId),
    get_or_fetch(Client, SpaceId, UserId).


%%--------------------------------------------------------------------
%% @doc
%% Gets space info from the database in user context. If space info is not found
%% fetches it from onezone and stores it in the database.
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch(Client :: oz_endpoint:client(), SpaceId :: binary(),
    UserId :: onedata_user:id()) -> {ok, datastore:document()} | datastore:get_error().
get_or_fetch(Client, SpaceId, ?ROOT_USER_ID) ->
    case get(SpaceId, ?ROOT_USER_ID) of
        {ok, Doc} -> {ok, Doc};
        {error, {not_found, _}} -> fetch(Client, SpaceId);
        {error, Reason} -> {error, Reason}
    end;
get_or_fetch(Client, SpaceId, UserId) ->
    case get_or_fetch(Client, SpaceId, ?ROOT_USER_ID) of
        {ok, #document{value = SpaceInfo} = Doc} ->
            case onedata_user:get_or_fetch(Client, UserId) of
                {ok, #document{value = #onedata_user{spaces = Spaces}}} ->
                    case lists:keyfind(SpaceId, 1, Spaces) of
                        false ->
                            {ok, Doc};
                        {_, SpaceName} ->
                            {ok, Doc#document{value = SpaceInfo#space_info{name = SpaceName}}}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Fetches space info from onezone and stores it in the database.
%% @end
%%--------------------------------------------------------------------
-spec fetch(Client :: oz_endpoint:client(), SpaceId :: binary()) ->
    {ok, datastore:document()} | datastore:get_error().
fetch(Client, SpaceId) ->
    {ok, #space_details{name = Name, providers_supports = Supports}} =
        oz_spaces:get_details(Client, SpaceId),
    {ok, GroupIds} = oz_spaces:get_groups(Client, SpaceId),
    {ok, UserIds} = oz_spaces:get_users(Client, SpaceId),

    {ok, ProviderIds} = oz_spaces:get_providers(Client, SpaceId),

    GroupsWithPrivileges = utils:pmap(fun(GroupId) ->
        {ok, Privileges} =
            oz_spaces:get_group_privileges(Client, SpaceId, GroupId),
        {GroupId, Privileges}
    end, GroupIds),
    UsersWithPrivileges = utils:pmap(fun(UserId) ->
        {ok, Privileges} =
            oz_spaces:get_user_privileges(Client, SpaceId, UserId),
        {UserId, Privileges}
    end, UserIds),

    Doc = #document{key = SpaceId, value = #space_info{
        users = UsersWithPrivileges,
        groups = GroupsWithPrivileges,
        providers_supports = Supports,
        name = Name,
        providers = ProviderIds
    }},
    {ok, _} = save(Doc),

    {ok, Doc}.