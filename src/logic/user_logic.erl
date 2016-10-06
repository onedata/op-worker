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
-export([join_group/2, leave_group/2, get_groups/2, get_effective_groups/2]).
-export([get_effective_handle_services/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves user document.
%% Provided client should be authorised to access user details.
%% @end
%%--------------------------------------------------------------------
-spec get(oz_endpoint:auth(), UserId :: binary()) ->
    {ok, datastore:document()} | datastore:get_error().
get(Auth, UserId) ->
    od_user:get_or_fetch(Auth, UserId).


%%--------------------------------------------------------------------
%% @doc
%% Returns list of user space IDs.
%% @end
%%--------------------------------------------------------------------
-spec get_spaces(oz_endpoint:auth(), UserId :: od_user:id()) ->
    {ok, [{SpaceId :: binary(), SpaceName :: binary()}]} |
    {error, Reason :: term()}.
get_spaces(Auth, UserId) ->
    case get(Auth, UserId) of
        {ok, #document{value = #od_user{space_aliases = Spaces}}} ->
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
-spec get_spaces(UserId :: od_user:id()) ->
    {ok, [{SpaceId :: binary(), SpaceName :: binary()}]} |
    {error, Reason :: term()}.
get_spaces(UserId) ->
    case od_user:get(UserId) of
        {ok, #document{value = #od_user{space_aliases = Spaces}}} ->
            {ok, Spaces};
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves default space for given user.
%% @end
%%--------------------------------------------------------------------
-spec get_default_space(oz_endpoint:auth(), UserId :: binary()) ->
    {ok, SpaceId :: od_space:id()} | datastore:get_error().
get_default_space(Auth, UserId) ->
    case get(Auth, UserId) of
        {ok, Doc} ->
            #document{
                value = #od_user{
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
-spec set_default_space(oz_endpoint:auth(), SpaceId :: binary()) ->
    ok | {error, Reason :: term()}.
set_default_space(Auth, SpaceId) ->
    oz_users:set_default_space(Auth, [{<<"spaceId">>, SpaceId}]).


%%--------------------------------------------------------------------
%% @doc
%% Adds a user (owner of auth) to a group.
%% @end
%%--------------------------------------------------------------------
-spec join_group(oz_endpoint:auth(), GroupId :: binary()) ->
    {ok, GroupId :: binary()} | {error, Reason :: term()}.
join_group(Auth, Token) ->
    oz_users:join_group(Auth, [{<<"token">>, Token}]).


%%--------------------------------------------------------------------
%% @doc
%% Removes a user (owner of auth) from group users list.
%% @end
%%--------------------------------------------------------------------
-spec leave_group(oz_endpoint:auth(), GroupId :: binary()) ->
    ok | {error, Reason :: term()}.
leave_group(Auth, GroupId) ->
    oz_users:leave_group(Auth, GroupId).


%%--------------------------------------------------------------------
%% @doc
%% Returns list of user group IDs.
%% @end
%%--------------------------------------------------------------------
-spec get_groups(oz_endpoint:auth(), UserId :: od_user:id()) ->
    {ok, GroupsIds :: [binary()]} |  {error, Reason :: term()}.
get_groups(Auth, UserId) ->
    case get(Auth, UserId) of
        {ok, #document{value = #od_user{group_ids = GroupsIds}}} ->
            {ok, GroupsIds};
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns list of user effective group IDs.
%% @end
%%--------------------------------------------------------------------
-spec get_effective_groups(oz_endpoint:auth(), UserId :: od_user:id()) ->
    {ok, GroupsIds :: [binary()]} |  {error, Reason :: term()}.
get_effective_groups(Auth, UserId) ->
    case get(Auth, UserId) of
        {ok, #document{value = #od_user{
            effective_group_ids = GroupsIds}}} ->
            {ok, GroupsIds};
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns list of user effective group IDs.
%% @end
%%--------------------------------------------------------------------
-spec get_effective_handle_services(oz_endpoint:auth(), UserId :: od_user:id()) ->
    {ok, GroupsIds :: [binary()]} |  {error, Reason :: term()}.
get_effective_handle_services(Auth, UserId) ->
    case get(Auth, UserId) of
        {error, Reason} ->
            {error, Reason};
        {ok, Doc} ->
            #document{
                value = #od_user{
                    handle_services = UserHandleServices,
                    effective_group_ids = EffectiveGroupIds
                }} = Doc,
            GroupHandleServices = lists:flatmap(
                fun(GroupId) ->
                    {ok, #document{
                        value = #od_group{
                            handle_services = GroupHS
                        }}} = group_logic:get(Auth, GroupId),
                    GroupHS
                end, EffectiveGroupIds),
            {ok, UserHandleServices ++ GroupHandleServices}
    end.
