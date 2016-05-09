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
-include_lib("ctool/include/oz/oz_spaces.hrl").

-export([get/2, create/2, set_name/3, delete/2]).
-export([set_user_privileges/4, set_group_privileges/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves group document.
%% Provided client should be authorised to access group details.
%% @end
%%--------------------------------------------------------------------
-spec get(oz_endpoint:client(), GroupId :: binary()) ->
    {ok, datastore:document()} | {error, Reason :: term()}.
get({user, {Macaroon, DischMacaroons}}, GroupId) ->
    onedata_group:get_or_fetch({user, {Macaroon, DischMacaroons}}, GroupId).


%%--------------------------------------------------------------------
%% @doc
%% Creates space in context of an user.
%% User identity is determined using provided client.
%% @end
%%--------------------------------------------------------------------
-spec create(oz_endpoint:client(), #onedata_group{}) ->
    {ok, GroupId :: binary()} | {error, Reason :: term()}.
create(Client = {user, _}, Record) ->
    Name = Record#onedata_group.name,
    oz_users:create_group(Client, [{<<"name">>, Name}]).


%%--------------------------------------------------------------------
%% @doc
%% Deletes space from the system.
%% @end
%%--------------------------------------------------------------------
-spec delete(oz_endpoint:client(), GroupId :: binary()) ->
    ok | {error, Reason :: term()}.
delete(Client, GroupId) ->
    oz_groups:remove(Client, GroupId).


%%--------------------------------------------------------------------
%% @doc
%% Sets name for an user.
%% User identity is determined using provided client.
%% @end
%%--------------------------------------------------------------------
-spec set_name(oz_endpoint:client(), GroupId :: binary(), Name :: binary()) ->
    ok | {error, Reason :: term()}.
set_name(Client, GroupId, Name) ->
    oz_groups:modify_details(Client, GroupId, [{<<"name">>, Name}]).


%%--------------------------------------------------------------------
%% @doc
%% Sets space privileges for an user.
%% User identity is determined using provided client.
%% @end
%%--------------------------------------------------------------------
-spec set_user_privileges(oz_endpoint:client(), SpaceId :: binary(),
    UserId :: binary(), Privileges :: [atom()]) ->
    ok | {error, Reason :: term()}.
set_user_privileges(Client, GroupId, UserId, PrivilegesAtoms) ->
    Privileges = [atom_to_binary(P, utf8) || P <- PrivilegesAtoms],
    oz_groups:set_user_privileges(Client, GroupId, UserId, [
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
set_group_privileges(Client, GroupId, PrivsGroupId, PrivilegesAtoms) ->
    {error, not_implemented}.
%%    Privileges = [atom_to_binary(P, utf8) || P <- PrivilegesAtoms],
%%    oz_groups:set_group_privileges(Client, GroupId, PrivsGroupId, [
%%        {<<"privileges">>, Privileges}
%%    ]).




