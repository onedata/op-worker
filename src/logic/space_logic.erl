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
-clientor("Michal Zmuda").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").

-export([get/3, create_user_space/2, delete/2, leave_space/2]).
-export([set_name/3, set_user_privileges/4, set_group_privileges/4]).
-export([get_invite_user_token/2, get_invite_group_token/2,
    get_invite_provider_token/2]).


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
-spec get(oz_endpoint:client(), SpaceId :: binary(), UserId :: binary()) ->
    {ok, datastore:document()} | datastore:get_error().
get(Client, SpaceId, UserId) ->
    space_info:get_or_fetch(Client, SpaceId, UserId).


%%--------------------------------------------------------------------
%% @doc
%% Creates space in context of an user.
%% User identity is determined using provided client.
%% @end
%%--------------------------------------------------------------------
-spec create_user_space(oz_endpoint:client(), #space_info{}) ->
    {ok, SpaceId :: binary()} | {error, Reason :: term()}.
create_user_space(Client = {user, _}, Record) ->
    Name = Record#space_info.name,
    oz_users:create_space(Client, [{<<"name">>, Name}]).


%%--------------------------------------------------------------------
%% @doc
%% Deletes space from the system.
%% @end
%%--------------------------------------------------------------------
-spec delete(oz_endpoint:client(), SpaceId :: binary()) ->
    ok | {error, Reason :: term()}.
delete(Client, SpaceId) ->
    oz_spaces:remove(Client, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Sets name for an user.
%% User identity is determined using provided client.
%% @end
%%--------------------------------------------------------------------
-spec set_name(oz_endpoint:client(), SpaceId :: binary(), Name :: binary()) ->
    ok | {error, Reason :: term()}.
set_name(Client, SpaceId, Name) ->
    oz_spaces:modify_details(Client, SpaceId, [{<<"name">>, Name}]).


%%--------------------------------------------------------------------
%% @doc
%% Sets space privileges for an user.
%% User identity is determined using provided client.
%% @end
%%--------------------------------------------------------------------
-spec set_user_privileges(oz_endpoint:client(), SpaceId :: binary(),
    UserId :: binary(), Privileges :: [atom()]) ->
    ok | {error, Reason :: term()}.
set_user_privileges(Client, SpaceId, UserId, PrivilegesAtoms) ->
    Privileges = [atom_to_binary(P, utf8) || P <- PrivilegesAtoms],
    oz_spaces:set_user_privileges(Client, SpaceId, UserId, [
        {<<"privileges">>, Privileges}
    ]).


%%--------------------------------------------------------------------
%% @doc
%% Sets space privileges for an user.
%% User identity is determined using provided client.
%% @end
%%--------------------------------------------------------------------
-spec set_group_privileges(oz_endpoint:client(), SpaceId :: binary(),
    GroupId :: binary(), Privileges :: [atom()]) ->
    ok | {error, Reason :: term()}.
set_group_privileges(Client, SpaceId, GroupId, PrivilegesAtoms) ->
    Privileges = [atom_to_binary(P, utf8) || P <- PrivilegesAtoms],
    oz_spaces:set_group_privileges(Client, SpaceId, GroupId, [
        {<<"privileges">>, Privileges}
    ]).


%%--------------------------------------------------------------------
%% @doc
%% Removes a user (owner of auth) from space users list.
%% @end
%%--------------------------------------------------------------------
-spec leave_space(oz_endpoint:client(), SpaceId :: binary()) ->
    ok | {error, Reason :: term()}.
leave_space(Client, SpaceId) ->
    oz_users:leave_space(Client, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Returns a user invitation token for given space.
%% @end
%%--------------------------------------------------------------------
-spec get_invite_user_token(oz_endpoint:client(), SpaceId :: binary()) ->
    {ok, binary()} | {error, Reason :: term()}.
get_invite_user_token(Client, SpaceId) ->
    oz_spaces:get_invite_user_token(Client, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Returns a group invitation token for given space.
%% @end
%%--------------------------------------------------------------------
-spec get_invite_group_token(oz_endpoint:client(), SpaceId :: binary()) ->
    {ok, binary()} | {error, Reason :: term()}.
get_invite_group_token(Client, SpaceId) ->
    oz_spaces:get_invite_group_token(Client, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Returns a provider invitation token (to get support) for given space.
%% @end
%%--------------------------------------------------------------------
-spec get_invite_provider_token(oz_endpoint:client(), SpaceId :: binary()) ->
    {ok, binary()} | {error, Reason :: term()}.
get_invite_provider_token(Client, SpaceId) ->
    oz_spaces:get_invite_provider_token(Client, SpaceId).


