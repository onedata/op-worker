%%%-------------------------------------------------------------------
%%% @clientor Michal Zmuda
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Interface to provider's space cache.
%%% Operations may involve interactions with OZ api
%%% or cached records from the datastore.
%%% @end
%%%-------------------------------------------------------------------
-module(space_logic).
-author("Michal Zmuda").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").

% Todo move create join leave to user logic
-export([get/3, create_user_space/2, delete/2]).
-export([join_space/2, leave_space/2]).
-export([set_name/3, set_user_privileges/4, set_group_privileges/4]).
-export([get_invite_user_token/2, get_invite_group_token/2,
    get_invite_provider_token/2]).
-export([has_effective_user/2, has_effective_privilege/3]).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves space document.
%% Returned document contains parameters tied to given user
%% (as space name may differ from user to user).
%% Provided client should be authorised to access user details.
%% @end
%%--------------------------------------------------------------------
-spec get(oz_endpoint:auth(), SpaceId :: binary(), UserId :: binary()) ->
    {ok, datastore:document()} | datastore:get_error().
get(Auth, SpaceId, UserId) ->
    od_space:get_or_fetch(Auth, SpaceId, UserId).


%%--------------------------------------------------------------------
%% @doc
%% Creates space in context of an user.
%% User identity is determined using provided auth.
%% @end
%%--------------------------------------------------------------------
-spec create_user_space(oz_endpoint:auth(), #od_space{}) ->
    {ok, SpaceId :: binary()} | {error, Reason :: term()}.
create_user_space(Auth, Record) ->
    Name = Record#od_space.name,
    oz_users:create_space(Auth, [{<<"name">>, Name}]).


%%--------------------------------------------------------------------
%% @doc
%% Deletes space from the system.
%% @end
%%--------------------------------------------------------------------
-spec delete(oz_endpoint:auth(), SpaceId :: binary()) ->
    ok | {error, Reason :: term()}.
delete(Auth, SpaceId) ->
    oz_spaces:remove(Auth, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Removes a user (owner of auth) from space users list.
%% @end
%%--------------------------------------------------------------------
-spec leave_space(oz_endpoint:auth(), SpaceId :: binary()) ->
    ok | {error, Reason :: term()}.
leave_space(Auth, SpaceId) ->
    oz_users:leave_space(Auth, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Joins a user to a space based on token.
%% @end
%%--------------------------------------------------------------------
-spec join_space(oz_endpoint:auth(), Token :: binary()) ->
    ok | {error, Reason :: term()}.
join_space(Auth, Token) ->
    case oz_users:join_space(Auth, [{<<"token">>, Token}]) of
        {ok, SpaceId} ->
            {ok, SpaceId};
        {error, {
            400,
            <<"invalid_request">>,
            <<"invalid 'token' value: ", _/binary>>
        }} ->
            {error, invalid_token_value}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Sets name for an user.
%% User identity is determined using provided auth.
%% @end
%%--------------------------------------------------------------------
-spec set_name(oz_endpoint:auth(), SpaceId :: binary(), Name :: binary()) ->
    ok | {error, Reason :: term()}.
set_name(Auth, SpaceId, Name) ->
    oz_spaces:modify_details(Auth, SpaceId, [{<<"name">>, Name}]).


%%--------------------------------------------------------------------
%% @doc
%% Sets space privileges for an user.
%% User identity is determined using provided auth.
%% @end
%%--------------------------------------------------------------------
-spec set_user_privileges(oz_endpoint:auth(), SpaceId :: binary(),
    UserId :: binary(), Privileges :: [atom()]) ->
    ok | {error, Reason :: term()}.
set_user_privileges(Auth, SpaceId, UserId, PrivilegesAtoms) ->
    Privileges = [atom_to_binary(P, utf8) || P <- PrivilegesAtoms],
    oz_spaces:set_user_privileges(Auth, SpaceId, UserId, [
        {<<"privileges">>, Privileges}
    ]).


%%--------------------------------------------------------------------
%% @doc
%% Sets space privileges for an user.
%% User identity is determined using provided auth.
%% @end
%%--------------------------------------------------------------------
-spec set_group_privileges(oz_endpoint:auth(), SpaceId :: binary(),
    GroupId :: binary(), Privileges :: [atom()]) ->
    ok | {error, Reason :: term()}.
set_group_privileges(Auth, SpaceId, GroupId, PrivilegesAtoms) ->
    Privileges = [atom_to_binary(P, utf8) || P <- PrivilegesAtoms],
    oz_spaces:set_group_privileges(Auth, SpaceId, GroupId, [
        {<<"privileges">>, Privileges}
    ]).


%%--------------------------------------------------------------------
%% @doc
%% Returns a user invitation token for given space.
%% @end
%%--------------------------------------------------------------------
-spec get_invite_user_token(oz_endpoint:auth(), SpaceId :: binary()) ->
    {ok, binary()} | {error, Reason :: term()}.
get_invite_user_token(Auth, SpaceId) ->
    oz_spaces:get_invite_user_token(Auth, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Returns a group invitation token for given space.
%% @end
%%--------------------------------------------------------------------
-spec get_invite_group_token(oz_endpoint:auth(), SpaceId :: binary()) ->
    {ok, binary()} | {error, Reason :: term()}.
get_invite_group_token(Auth, SpaceId) ->
    oz_spaces:get_invite_group_token(Auth, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Returns a provider invitation token (to get support) for given space.
%% @end
%%--------------------------------------------------------------------
-spec get_invite_provider_token(oz_endpoint:auth(), SpaceId :: binary()) ->
    {ok, binary()} | {error, Reason :: term()}.
get_invite_provider_token(Auth, SpaceId) ->
    oz_spaces:get_invite_provider_token(Auth, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Predicate telling if given user belongs to given space directly
%% or via his groups.
%% @end
%%--------------------------------------------------------------------
-spec has_effective_user(SpaceId :: od_space:id(),
    UserId :: od_user:id()) -> boolean().
has_effective_user(SpaceId, UserId) ->
    case od_user:get(UserId) of
        {error, {not_found, _}} ->
            false;
        {ok, #document{value = UserInfo}} ->
            #od_user{space_aliases = SpaceNames} = UserInfo,
            proplists:is_defined(SpaceId, SpaceNames)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Predicate telling if given user has a privilege in given space, directly
%% or via his groups.
%% @end
%%--------------------------------------------------------------------
-spec has_effective_privilege(SpaceId :: od_space:id(),
    UserId :: od_user:id(), Privilege :: privileges:space_privilege()) ->
    boolean().
has_effective_privilege(SpaceId, UserId, Privilege) ->
    case has_effective_user(SpaceId, UserId) of
        false ->
            false;
        true ->
            {ok, UserPrivileges} = get_effective_privileges(SpaceId, UserId),
            ordsets:is_element(Privilege, UserPrivileges)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retrieves effective user privileges taking into account any groups
%% he is a member of that also are members of the Space.
%% Throws exception when call to the datastore fails,
%% or space/user doesn't exist.
%% @end
%%--------------------------------------------------------------------
-spec get_effective_privileges(SpaceId :: od_space:id(),
    UserId :: od_user:id()) ->
    {ok, ordsets:ordset(privileges:space_privilege())}.
get_effective_privileges(SpaceId, UserId) ->
    {ok, #document{
        value = #od_user{
            eff_groups = UGroups
        }}} = od_user:get(UserId),
    {ok, #document{
        value = #od_space{
            users = UserTuples,
            groups = SGroupTuples
        }}} = od_space:get(SpaceId),

    UserGroups = sets:from_list(UGroups),

    PrivilegesSets = lists:filtermap(fun({GroupId, Privileges}) ->
        case sets:is_element(GroupId, UserGroups) of
            true -> {true, ordsets:from_list(Privileges)};
            false -> false
        end
    end, SGroupTuples),

    UserPrivileges =
        case lists:keyfind(UserId, 1, UserTuples) of
            {UserId, Privileges} -> ordsets:from_list(Privileges);
            false -> ordsets:new()
        end,

    {ok, ordsets:union([UserPrivileges | PrivilegesSets])}.
