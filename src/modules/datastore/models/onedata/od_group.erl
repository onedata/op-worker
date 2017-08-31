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
-module(od_group).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/oz/oz_groups.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([save/1, get/1, list/0, exists/1, delete/1, update/2, create/1]).
-export([fetch/2, get_or_fetch/2, create_or_update/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-type id() :: binary().
-type record() :: #od_group{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type type() :: 'organization' | 'unit' | 'team' | 'role'.

-export_type([id/0, type/0]).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves group.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, id()} | {error, term()}.
save(Doc) ->
    ?extract_key(datastore_model:save(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Updates group.
%% @end
%%--------------------------------------------------------------------
-spec update(id(), diff()) -> {ok, id()} | {error, term()}.
update(Key, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff)).

%%--------------------------------------------------------------------
%% @doc
%% Creates group.
%% @end
%%--------------------------------------------------------------------
-spec create(doc()) -> {ok, id()} | {error, term()}.
create(Doc) ->
    ?extract_key(datastore_model:create(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Updates document with using ID from document. If such object does not exist,
%% it initialises the object with the document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(doc(), diff()) ->
    {ok, id()} | {error, term()}.
create_or_update(#document{key = Key, value = Default}, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff, Default)).

%%--------------------------------------------------------------------
%% @doc
%% Returns group.
%% @end
%%--------------------------------------------------------------------
-spec get(id()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes group.
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
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [id()]} | {error, term()}.
list() ->
    datastore_model:fold_keys(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Fetch group from OZ and save it in cache.
%% @end
%%--------------------------------------------------------------------
-spec fetch(Auth :: oz_endpoint:auth(), GroupId :: id()) ->
    {ok, datastore:doc()} | {error, Reason :: term()}.
fetch(Auth, GroupId) ->
    try
        {ok, #group_details{id = Id, name = Name, type = Type}} =
            oz_groups:get_details(Auth, GroupId),
        case Type of
            public ->
                % Only public info about this group was discoverable
                OnedataGroupDoc = #document{
                    key = Id, value = #od_group{
                        name = Name,
                        type = Type
                    }},
                case od_group:create(OnedataGroupDoc) of
                    {ok, _} -> ok;
                    {error, already_exists} -> ok
                end,
                {ok, OnedataGroupDoc};
            _ ->
                {ok, SpaceIds} = oz_groups:get_spaces(Auth, Id),
                {ok, ParentIds} = oz_groups:get_parents(Auth, Id),

                % nested groups
                {ok, NestedIds} = oz_groups:get_nested(Auth, Id),
                NestedGroupsWithPrivileges = utils:pmap(fun(UID) ->
                    {ok, Privileges} = oz_groups:get_nested_privileges(Auth, Id, UID),
                    {UID, Privileges}
                end, NestedIds),

                % users
                {ok, UserIds} = oz_groups:get_users(Auth, Id),
                UsersWithPrivileges = utils:pmap(fun(UID) ->
                    {ok, Privileges} = oz_groups:get_user_privileges(Auth, Id, UID),
                    {UID, Privileges}
                end, UserIds),

                % effective users
                {ok, EffectiveUserIds} = oz_groups:get_effective_users(Auth, Id),
                EffectiveUsersWithPrivileges = utils:pmap(fun(UID) ->
                    {ok, Privileges} = oz_groups:get_effective_user_privileges(Auth, Id, UID),
                    {UID, Privileges}
                end, EffectiveUserIds),

                %todo consider getting user_details for each group member and storing it as od_user
                OnedataGroupDoc = #document{key = Id, value = #od_group{
                    users = UsersWithPrivileges, spaces = SpaceIds, name = Name,
                    eff_users = EffectiveUsersWithPrivileges, type = Type,
                    parents = ParentIds, children = NestedGroupsWithPrivileges}},

                case od_group:create(OnedataGroupDoc) of
                    {ok, _} -> ok;
                    {error, already_exists} -> ok
                end,
                {ok, OnedataGroupDoc}
        end
    catch
        _:Reason ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get group from cache or fetch from OZ and save in cache.
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch(Auth :: oz_endpoint:auth(), GroupId :: od_group:id()) ->
    {ok, datastore:doc()} | {error, term()}.
get_or_fetch(Auth, GroupId) ->
    case od_group:get(GroupId) of
        {ok, Doc} -> {ok, Doc};
        {error, not_found} -> fetch(Auth, GroupId);
        Error -> Error
    end.

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
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {name, string},
        {type, atom},
        {parents, [string]},
        {children, [{string, [atom]}]},
        {eff_parents, [string]},
        {eff_children, [{string, [atom]}]},
        {users, [{string, [atom]}]},
        {spaces, [string]},
        {handle_services, [string]},
        {handles, [string]},
        {eff_users, [{string, [atom]}]},
        {eff_spaces, [string]},
        {eff_shares, [string]},
        {eff_providers, [string]},
        {eff_handle_services, [string]},
        {eff_handles, [string]},
        {revision_history, [term]}
    ]}.