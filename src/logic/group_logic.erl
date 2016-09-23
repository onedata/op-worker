%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Interface to provider's group cache.
%%% Operations may involve interactions with OZ api
%%% or cached records from the datastore.
%%% @end
%%%-------------------------------------------------------------------
-module(group_logic).
-author("Michal Zmuda").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

-export([get/1, get/2, create/2, set_name/3, delete/2]).
-export([leave_space/3, join_space/3]).
-export([join_group/3, leave_group/3]).
-export([set_user_privileges/4, set_group_privileges/4]).
-export([get_invite_user_token/2, get_invite_group_token/2,
    get_create_space_token/2]).
-export([has_effective_user/2, has_effective_privilege/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves group document as provider.
%% @end
%%--------------------------------------------------------------------
-spec get(GroupId :: binary()) ->
    {ok, datastore:document()} | {error, Reason :: term()}.
get(GroupId) ->
    onedata_group:get(GroupId).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves group document as a user.
%% Provided client should be authorised to access group details.
%% @end
%%--------------------------------------------------------------------
-spec get(oz_endpoint:auth(), GroupId :: binary()) ->
    {ok, datastore:document()} | {error, Reason :: term()}.
get(Auth, GroupId) ->
    onedata_group:get_or_fetch(Auth, GroupId).


%%--------------------------------------------------------------------
%% @doc
%% Creates group in context of an user.
%% User identity is determined using provided auth.
%% @end
%%--------------------------------------------------------------------
-spec create(oz_endpoint:auth(), #onedata_group{}) ->
    {ok, GroupId :: binary()} | {error, Reason :: term()}.
create(Auth, Record) ->
    Name = Record#onedata_group.name,
    oz_users:create_group(Auth, [
        {<<"name">>, Name},
        % Use default group type
        {<<"type">>, <<"role">>}
    ]).


%%--------------------------------------------------------------------
%% @doc
%% Deletes group from the system.
%% @end
%%--------------------------------------------------------------------
-spec delete(oz_endpoint:auth(), GroupId :: binary()) ->
    ok | {error, Reason :: term()}.
delete(Auth, GroupId) ->
    oz_groups:remove(Auth, GroupId).


%%--------------------------------------------------------------------
%% @doc
%% Joins a group to a space based on invite token.
%% @end
%%--------------------------------------------------------------------
-spec join_space(oz_endpoint:auth(), GroupId :: binary(),
    Token :: binary()) -> ok | {error, Reason :: term()}.
join_space(Auth, GroupId, Token) ->
    case oz_groups:join_space(Auth, GroupId, [{<<"token">>, Token}]) of
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
%% Removes a group from space.
%% @end
%%--------------------------------------------------------------------
-spec leave_space(oz_endpoint:auth(), GroupId :: binary(),
    SpaceId :: binary()) -> ok | {error, Reason :: term()}.
leave_space(Auth, GroupId, SpaceId) ->
    oz_groups:leave_space(Auth, GroupId, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Joins a group to a space based on invite token.
%% @end
%%--------------------------------------------------------------------
-spec join_group(oz_endpoint:auth(), GroupId :: binary(),
    Token :: binary()) -> ok | {error, Reason :: term()}.
join_group(Auth, ChildGroupId, Token) ->
    case oz_groups:join_group(Auth, ChildGroupId, [{<<"token">>, Token}]) of
        {ok, ParentGroupId} ->
            {ok, ParentGroupId};
        {error, {
            400,
            <<"invalid_request">>,
            <<"invalid 'token' value: ", _/binary>>
        }} ->
            {error, invalid_token_value}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Removes a subgroup from a group.
%% @end
%%--------------------------------------------------------------------
-spec leave_group(oz_endpoint:auth(), ParentGroupId :: binary(),
    ChildGroupId :: binary()) -> ok | {error, Reason :: term()}.
leave_group(Auth, ParentGroupId, ChildGroupId) ->
    oz_groups:leave_group(Auth, ParentGroupId, ChildGroupId).


%%--------------------------------------------------------------------
%% @doc
%% Sets name for a group.
%% User identity is determined using provided auth.
%% @end
%%--------------------------------------------------------------------
-spec set_name(oz_endpoint:auth(), GroupId :: binary(), Name :: binary()) ->
    ok | {error, Reason :: term()}.
set_name(Auth, GroupId, Name) ->
    oz_groups:modify_details(Auth, GroupId, [
        {<<"name">>, Name},
        % Use default group type
        {<<"type">>, <<"role">>}
    ]).


%%--------------------------------------------------------------------
%% @doc
%% Sets group privileges for an user.
%% User identity is determined using provided auth.
%% @end
%%--------------------------------------------------------------------
-spec set_user_privileges(oz_endpoint:auth(), SpaceId :: binary(),
    UserId :: binary(), Privileges :: [atom()]) ->
    ok | {error, Reason :: term()}.
set_user_privileges(Auth, GroupId, UserId, PrivilegesAtoms) ->
    Privileges = [atom_to_binary(P, utf8) || P <- PrivilegesAtoms],
    oz_groups:set_user_privileges(Auth, GroupId, UserId, [
        {<<"privileges">>, Privileges}
    ]).


%%--------------------------------------------------------------------
%% @doc
%% Sets group privileges for a group.
%% User identity is determined using provided auth.
%% @end
%%--------------------------------------------------------------------
-spec set_group_privileges(oz_endpoint:auth(), ParentGroupId :: binary(),
    ChildGroupId :: binary(), Privileges :: [atom()]) ->
    ok | {error, Reason :: term()}.
set_group_privileges(Auth, ParentGroupId, ChildGroupId, PrivilegesAtoms) ->
    Privileges = [atom_to_binary(P, utf8) || P <- PrivilegesAtoms],
    oz_groups:set_nested_privileges(Auth, ParentGroupId, ChildGroupId, [
        {<<"privileges">>, Privileges}
    ]).


%%--------------------------------------------------------------------
%% @doc
%% Returns a user invitation token to group.
%% @end
%%--------------------------------------------------------------------
-spec get_invite_user_token(oz_endpoint:auth(), GroupId :: binary()) ->
    {ok, binary()} | {error, Reason :: term()}.
get_invite_user_token(Auth, GroupId) ->
    oz_groups:get_invite_user_token(Auth, GroupId).


%%--------------------------------------------------------------------
%% @doc
%% Returns a group invitation token to group.
%% @end
%%--------------------------------------------------------------------
-spec get_invite_group_token(oz_endpoint:auth(), GroupId :: binary()) ->
    {ok, binary()} | {error, Reason :: term()}.
get_invite_group_token(Auth, GroupId) ->
    oz_groups:get_invite_group_token(Auth, GroupId).


%%--------------------------------------------------------------------
%% @doc
%% Returns a create space token (for provider to create a space for group).
%% @end
%%--------------------------------------------------------------------
-spec get_create_space_token(oz_endpoint:auth(), GroupId :: binary()) ->
    {ok, binary()} | {error, Reason :: term()}.
get_create_space_token(Auth, GroupId) ->
    oz_groups:get_create_space_token(Auth, GroupId).


%%--------------------------------------------------------------------
%% @doc
%% Predicate telling if given user belongs to given space directly
%% or via his groups.
%% @end
%%--------------------------------------------------------------------
-spec has_effective_user(GroupId :: onedata_group:id(),
    UserId :: onedata_user:id()) -> boolean().
has_effective_user(GroupId, UserId) ->
    case onedata_user:get(UserId) of
        {error, {not_found, _}} ->
            false;
        {ok, #document{value = UserInfo}} ->
            #onedata_user{effective_group_ids = Groups} = UserInfo,
            lists:member(GroupId, Groups)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Predicate telling if given user has a privilege in given group.
%% @end
%%--------------------------------------------------------------------
-spec has_effective_privilege(GroupId :: onedata_group:id(),
    UserId :: onedata_user:id(), Privilege :: privileges:group_privilege()) ->
    boolean().
has_effective_privilege(GroupId, UserId, Privilege) ->
    case has_effective_user(GroupId, UserId) of
        false ->
            false;
        true ->
            {ok, #document{
                value = #onedata_group{
                    users = UserTuples
                }}} = onedata_group:get(GroupId),
            UserPrivileges = proplists:get_value(UserId, UserTuples, []),
            lists:member(Privilege, UserPrivileges)
    end.
