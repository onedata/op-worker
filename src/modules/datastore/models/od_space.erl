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
-module(od_space).
-author("Krzysztof Trzepla").
-behaviour(model_behaviour).

-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").

-type doc() :: datastore:document().
-type info() :: #od_space{}.
-type id() :: binary().
-export_type([doc/0, info/0, id/0]).

%% API
-export([create_or_update/2, get/2, get_or_fetch/3, get_or_fetch/2]).
-export([record_struct/1]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, model_init/0,
    'after'/5, before/4]).

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) -> datastore_json:record_struct().
record_struct(1) ->
    {record, [
        {name, string},
        {providers_supports, [{string, integer}]},
        {providers, [string]},
        {users, [{string, [atom]}]},
        {groups, [{string, [atom]}]},
        {shares, [string]},
        {eff_users, [{string, [atom]}]},
        {eff_groups, [{string, [atom]}]},
        {revision_history, [term]}
    ]}.

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
        {ok, D = #document{value = S = #od_space{providers_supports = Supports}}} when is_list(Supports) ->
            {ProviderIds, _} = lists:unzip(Supports),
            {ok, D#document{value = S#od_space{providers = ProviderIds}}};
        {ok, Doc} ->
            {ok, Doc}
    end.

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
    ?MODEL_CONFIG(od_space_bucket, [{?MODULE, create}, {?MODULE, save},
        {?MODULE, create_or_update}, {?MODULE, update}], ?GLOBALLY_CACHED_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(?MODULE, create, ?GLOBAL_ONLY_LEVEL, _, {ok, SpaceId}) ->
    ok = permissions_cache:invalidate_permissions_cache(),
    emit_monitoring_event(SpaceId);
'after'(?MODULE, create_or_update, ?GLOBAL_ONLY_LEVEL, _, {ok, SpaceId}) ->
    ok = permissions_cache:invalidate_permissions_cache(),
    emit_monitoring_event(SpaceId);
'after'(?MODULE, save, ?GLOBAL_ONLY_LEVEL, _, {ok, SpaceId}) ->
    ok = permissions_cache:invalidate_permissions_cache(),
    emit_monitoring_event(SpaceId);
'after'(?MODULE, update, ?GLOBAL_ONLY_LEVEL, _, {ok, SpaceId}) ->
    ok = permissions_cache:invalidate_permissions_cache(),
    emit_monitoring_event(SpaceId);
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
-spec create_or_update(datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
create_or_update(Doc, Diff) ->
    datastore:create_or_update(?STORE_LEVEL, Doc, Diff).

%%--------------------------------------------------------------------
%% @doc
%% Gets space info from the database in user context.
%% @end
%%--------------------------------------------------------------------
-spec get(SpaceId :: binary(), UserId :: od_user:id()) ->
    {ok, datastore:document()} | datastore:get_error().
get(SpaceId, SpecialUser) when SpecialUser =:= ?ROOT_USER_ID orelse SpecialUser =:= ?GUEST_USER_ID ->
    case od_space:get(SpaceId) of
        {ok, Doc} -> {ok, Doc};
        {error, Reason} -> {error, Reason}
    end;
get(SpaceId, UserId) ->
    case get(SpaceId, ?ROOT_USER_ID) of
        {ok, #document{value = SpaceInfo} = Doc} ->
            case od_user:get(UserId) of
                {ok, #document{value = #od_user{space_aliases = Spaces}}} ->
                    {_, SpaceName} = lists:keyfind(SpaceId, 1, Spaces),
                    {ok, Doc#document{value = SpaceInfo#od_space{name = SpaceName}}};
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
    {ok, UserId} = session:get_user_id(SessionId),
    get_or_fetch(SessionId, SpaceId, UserId).


%%--------------------------------------------------------------------
%% @doc
%% Gets space info from the database in user context. If space info is not found
%% fetches it from onezone and stores it in the database.
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch(Auth :: oz_endpoint:auth(), SpaceId :: binary(),
    UserId :: od_user:id()) -> {ok, datastore:document()} | datastore:get_error().
get_or_fetch(Auth, SpaceId, SpecialUser) when SpecialUser =:= ?ROOT_USER_ID orelse SpecialUser =:= ?GUEST_USER_ID ->
    case get(SpaceId, SpecialUser) of
        {ok, Doc} -> {ok, Doc};
        {error, {not_found, _}} -> fetch(Auth, SpaceId);
        {error, Reason} -> {error, Reason}
    end;
get_or_fetch(Auth, SpaceId, UserId) ->
    case get_or_fetch(Auth, SpaceId, ?ROOT_USER_ID) of
        {ok, #document{value = SpaceInfo} = Doc} ->
            case od_user:get_or_fetch(Auth, UserId) of
                {ok, #document{value = #od_user{space_aliases = Spaces}}} ->
                    case lists:keyfind(SpaceId, 1, Spaces) of
                        false ->
                            {ok, Doc};
                        {_, SpaceName} ->
                            {ok, Doc#document{value = SpaceInfo#od_space{name = SpaceName}}}
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
-spec fetch(Auth :: oz_endpoint:auth(), SpaceId :: binary()) ->
    {ok, datastore:document()} | datastore:get_error().
fetch(Auth, SpaceId) ->
    {ok, #space_details{
        name = Name,
        providers_supports = Supports,
        shares = Shares
    }} = oz_spaces:get_details(Auth, SpaceId),

    {ok, GroupIds} = oz_spaces:get_groups(Auth, SpaceId),
    {ok, UserIds} = oz_spaces:get_users(Auth, SpaceId),

    {ok, ProviderIds} = oz_spaces:get_providers(Auth, SpaceId),

    GroupsWithPrivileges = utils:pmap(fun(GroupId) ->
        {ok, Privileges} =
            oz_spaces:get_group_privileges(Auth, SpaceId, GroupId),
        {GroupId, Privileges}
    end, GroupIds),
    UsersWithPrivileges = utils:pmap(fun(UserId) ->
        {ok, Privileges} =
            oz_spaces:get_user_privileges(Auth, SpaceId, UserId),
        {UserId, Privileges}
    end, UserIds),

    Doc = #document{key = SpaceId, value = #od_space{
        name = Name,
        users = UsersWithPrivileges,
        groups = GroupsWithPrivileges,
        providers_supports = Supports,
        providers = ProviderIds,
        shares = Shares
    }},

    case create(Doc) of
        {ok, _} -> ok;
        {error, already_exists} -> ok
    end,

    {ok, Doc}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends event informing about od_space update if provider supports space.
%% @end
%%--------------------------------------------------------------------
-spec emit_monitoring_event(datastore:id()) -> no_return().
emit_monitoring_event(SpaceId) ->
    case od_space:get(SpaceId) of
        {ok, #document{value = #od_space{providers = Providers}}} ->
            case lists:member(oneprovider:get_provider_id(), Providers) of
                true ->
                    monitoring_event:emit_od_space_updated(SpaceId);
                _ -> ok
            end;
        _ -> ok
    end.