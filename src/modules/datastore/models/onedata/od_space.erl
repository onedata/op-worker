%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Cache for space details fetched from onezone.
%%% @end
%%%-------------------------------------------------------------------
-module(od_space).
-author("Krzysztof Trzepla").

-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").

-type id() :: binary().
-type info() :: #od_space{}.
-type doc() :: datastore_doc:doc(info()).
-type diff() :: datastore_doc:diff(info()).
-type alias() :: binary().
-type name() :: binary().
-export_type([doc/0, info/0, id/0, alias/0, name/0]).

%% API
-export([create_or_update/2, get/2, get_or_fetch/3, get_or_fetch/2]).
-export([save/1, get/1, exists/1, delete/1, update/2, create/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_posthooks/0, get_record_struct/1]).

-define(CTX, #{model => ?MODULE}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves space.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, id()} | {error, term()}.
save(Doc) ->
    ?extract_key(datastore_model:save(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Updates space.
%% @end
%%--------------------------------------------------------------------
-spec update(id(), diff()) -> {ok, id()} | {error, term()}.
update(Key, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff)).

%%--------------------------------------------------------------------
%% @doc
%% Creates space.
%% @end
%%--------------------------------------------------------------------
-spec create(doc()) -> {ok, id()} | {error, term()}.
create(Doc) ->
    ?extract_key(datastore_model:create(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Returns space.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:doc()} | {error, term()}.
get(Key) ->
    case datastore_model:get(?CTX, Key) of
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
%% Deletes space.
%% @end
%%--------------------------------------------------------------------
-spec delete(id()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether group exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(id()) -> boolean().
exists(Key) ->
    {ok, Exists} = datastore_model:exists(?CTX, Key),
    Exists.

%%--------------------------------------------------------------------
%% @doc
%% Updates document with using ID from document. If such object does not exist,
%% it initialises the object with the document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(doc(), diff()) -> {ok, id()} | {error, term()}.
create_or_update(#document{key = Key, value = Default}, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff, Default)).

%%--------------------------------------------------------------------
%% @doc
%% Gets space info from the database in user context.
%% @end
%%--------------------------------------------------------------------
-spec get(SpaceId :: binary(), UserId :: od_user:id()) ->
    {ok, datastore:doc()} | {error, term()}.
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
    {ok, datastore:doc()} | {error, term()}.
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
    UserId :: od_user:id()) -> {ok, datastore:doc()} | {error, term()}.
get_or_fetch(Auth, SpaceId, SpecialUser) when SpecialUser =:= ?ROOT_USER_ID orelse SpecialUser =:= ?GUEST_USER_ID ->
    case get(SpaceId, SpecialUser) of
        {ok, Doc} -> {ok, Doc};
        {error, not_found} -> fetch(Auth, SpaceId);
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
    {ok, datastore:doc()} | {error, term()}.
fetch(Auth, SpaceId) ->
    {ok, #space_details{
        name = Name,
        providers_supports = Supports,
        shares = Shares
    }} = oz_spaces:get_details(Auth, SpaceId),

    {ok, GroupIds} = oz_spaces:get_groups(Auth, SpaceId),
    {ok, UserIds} = oz_spaces:get_users(Auth, SpaceId),

    {ok, ProviderIds} = oz_spaces:get_providers(Auth, SpaceId),

    GroupPrivileges = utils:pmap(fun(GroupId) ->
        oz_spaces:get_group_privileges(Auth, SpaceId, GroupId)
    end, GroupIds),
    GroupsWithPrivileges = lists:zipwith(fun(GroupId, {ok, Privileges}) ->
        {GroupId, Privileges}
    end, GroupIds, GroupPrivileges),

    UserPrivileges = utils:pmap(fun(UserId) ->
        oz_spaces:get_user_privileges(Auth, SpaceId, UserId)
    end, UserIds),
    UsersWithPrivileges = lists:zipwith(fun(UserId, {ok, Privileges}) ->
        {UserId, Privileges}
    end, UserIds, UserPrivileges),

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
-spec emit_monitoring_event(datastore:key()) -> no_return().
emit_monitoring_event(SpaceId) ->
    case od_space:get(SpaceId) of
        {ok, #document{value = #od_space{providers = Providers}}} ->
            case lists:member(oneprovider:get_provider_id(), Providers) of
                true -> monitoring_event:emit_od_space_updated(SpaceId);
                _ -> ok
            end;
        _ -> ok
    end,
    {ok, SpaceId}.

%%--------------------------------------------------------------------
%% @doc
%% Space create/update posthook.
%% @end
%%--------------------------------------------------------------------
-spec run_after(atom(), list(), term()) -> term().
run_after(create, _, {ok, #document{key = SpaceId}}) ->
    space_strategies:create(space_strategies:new(SpaceId)),
    ok = permissions_cache:invalidate(),
    emit_monitoring_event(SpaceId);
run_after(update, [_, _, _, _], {ok, #document{key = SpaceId}}) ->
    space_strategies:create(space_strategies:new(SpaceId)),
    ok = permissions_cache:invalidate(),
    emit_monitoring_event(SpaceId);
run_after(save, _, {ok, #document{key = SpaceId}}) ->
    space_strategies:create(space_strategies:new(SpaceId)),
    ok = permissions_cache:invalidate(),
    emit_monitoring_event(SpaceId);
run_after(update, _, {ok, #document{key = SpaceId}}) ->
    ok = permissions_cache:invalidate(),
    emit_monitoring_event(SpaceId);
run_after(_Function, _Args, Result) ->
    Result.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of callbacks which will be called after each operation
%% on datastore model.
%% @end
%%--------------------------------------------------------------------
-spec get_posthooks() -> [datastore_hooks:posthook()].
get_posthooks() ->
    [fun run_after/3].

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
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