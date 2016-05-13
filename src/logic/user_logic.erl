%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Interface to provider's user cache.
%%% Operations may involve interactions with OZ api
%%% or cached records from the datastore.
%%% @end
%%%-------------------------------------------------------------------
-module(user_logic).
-author("Michal Zmuda").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_models_def.hrl").

-export([get/2]).
-export([get_spaces/2, get_spaces/1, get_default_space/2, set_default_space/2]).
-export([join_group/2, leave_group/2, get_groups/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves user document.
%% Provided client should be authorised to access user details.
%% @end
%%--------------------------------------------------------------------
-spec get(oz_endpoint:client(), UserId :: binary()) ->
    {ok, datastore:document()} | datastore:get_error().
get({user, {Macaroon, DischMacaroons}}, UserId) ->
    onedata_user:get_or_fetch({user, {Macaroon, DischMacaroons}}, UserId).


%%--------------------------------------------------------------------
%% @doc
%% Returns list of user space IDs.
%% @end
%%--------------------------------------------------------------------
-spec get_spaces(oz_endpoint:client(), UserId :: onedata_user:id()) ->
    {ok, [{SpaceId :: binary(), SpaceName :: binary()}]} |
    {error, Reason :: term()}.
get_spaces({user, {Macaroon, DischMacaroons}}, UserId) ->
    case get({user, {Macaroon, DischMacaroons}}, UserId) of
        {ok, #document{value = #onedata_user{spaces = Spaces}}} ->
            {ok, Spaces};
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns list of user space IDs.
%% @todo this should be removed in favour of get_spaces/2
%% @end
%%--------------------------------------------------------------------
-spec get_spaces(UserId :: onedata_user:id()) ->
    {ok, [{SpaceId :: binary(), SpaceName :: binary()}]} |
    {error, Reason :: term()}.
get_spaces(UserId) ->
    case onedata_user:get(UserId) of
        {ok, #document{value = #onedata_user{spaces = Spaces}}} ->
            {ok, Spaces};
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves default space for given user.
%% @end
%%--------------------------------------------------------------------
-spec get_default_space(oz_endpoint:client(), UserId :: binary()) ->
    {ok, SpaceId :: space_info:id()} | datastore:get_error().
get_default_space({user, {Macaroon, DischMacaroons}}, UserId) ->
    case get({user, {Macaroon, DischMacaroons}}, UserId) of
        {ok, Doc} ->
            #document{
                value = #onedata_user{
                    default_space = DefaultSpace
                }} = Doc,
            DefaultSpace;
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Sets space as default for an user.
%% User identity is determined using provided client.
%% @end
%%--------------------------------------------------------------------
-spec set_default_space(oz_endpoint:client(), SpaceId :: binary()) ->
    ok | {error, Reason :: term()}.
set_default_space({user, {Macaroon, DischMacaroons}}, SpaceId) ->
    oz_users:set_default_space({user, {Macaroon, DischMacaroons}},
        [{<<"spaceId">>, SpaceId}]).


%%--------------------------------------------------------------------
%% @doc
%% Removes a user (owner of auth) from space users list.
%% @end
%%--------------------------------------------------------------------
-spec join_group(oz_endpoint:client(), GroupId :: binary()) ->
    ok | {error, Reason :: term()}.
join_group(Client, Token) ->
    oz_users:join_group(Client, [{<<"token">>, Token}]).


%%--------------------------------------------------------------------
%% @doc
%% Removes a user (owner of auth) from space users list.
%% @end
%%--------------------------------------------------------------------
-spec leave_group(oz_endpoint:client(), GroupId :: binary()) ->
    ok | {error, Reason :: term()}.
leave_group(Client, GroupId) ->
    oz_users:leave_group(Client, GroupId).


%%--------------------------------------------------------------------
%% @doc
%% Returns list of user space IDs.
%% @end
%%--------------------------------------------------------------------
-spec get_groups(oz_endpoint:client(), UserId :: onedata_user:id()) ->
    {ok, GroupsIds :: [binary()]} |  {error, Reason :: term()}.
get_groups({user, {Macaroon, DischMacaroons}}, UserId) ->
    case get({user, {Macaroon, DischMacaroons}}, UserId) of
        {ok, #document{value = #onedata_user{group_ids = GroupsIds}}} ->
            {ok, GroupsIds};
        {error, Reason} ->
            {error, Reason}
    end.