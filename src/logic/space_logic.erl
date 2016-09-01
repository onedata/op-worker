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
-export([get_share/3, create_share/4]).


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
    space_info:get_or_fetch(Auth, SpaceId, UserId).


%%--------------------------------------------------------------------
%% @doc
%% Creates space in context of an user.
%% User identity is determined using provided auth.
%% @end
%%--------------------------------------------------------------------
-spec create_user_space(oz_endpoint:auth(), #space_info{}) ->
    {ok, SpaceId :: binary()} | {error, Reason :: term()}.
create_user_space(Auth, Record) ->
    Name = Record#space_info.name,
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
%% Retrieves share document.
%% Returned document contains parameters tied to given user
%% (as space name may differ from user to user).
%% Provided client should be authorised to access user details.
%% @end
%%--------------------------------------------------------------------
-spec get_share(oz_endpoint:auth(), SpaceId :: binary(), ShareId :: binary()) ->
    {ok, datastore:document()} | datastore:get_error().
get_share(Auth, SpaceId, ShareId) ->
    share_info:get_or_fetch(Auth, SpaceId, ShareId).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new share.
%% @end
%%--------------------------------------------------------------------
-spec create_share(oz_endpoint:auth(), Name :: binary(), RootFileId :: binary(), SpaceId :: binary()) ->
    {ok, binary()} | {error, Reason :: term()}.
create_share(Auth, ParentSpaceId, Name, RootFileId) ->
    Parameters = [
        {<<"name">>, Name},
        {<<"root_file_id">>, RootFileId}
    ],
    oz_spaces:create_share(Auth, ParentSpaceId, Parameters).
