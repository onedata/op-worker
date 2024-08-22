%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for reading and manipulating od_group records synchronized
%%% via Graph Sync. Requests are delegated to gs_client_worker, which decides
%%% if they should be served from cache or handled by Onezone.
%%% NOTE: This is the only valid way to interact with od_group records, to
%%% ensure consistency, no direct requests to datastore or OZ REST should
%%% be performed.
%%% @end
%%%-------------------------------------------------------------------
-module(group_logic).
-author("Lukasz Opiola").

-include("graph_sync/provider_graph_sync.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([get/2, get_shared_data/3]).
-export([get_name/1, get_name/3]).
-export([has_eff_privilege/3, has_eff_privileges/3]).
-export([can_view_user_through_group/3, can_view_user_through_group/4]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Retrieves group doc private data by given GroupId.
%% @end
%%--------------------------------------------------------------------
-spec get(gs_client_worker:client(), od_group:id()) ->
    {ok, od_group:doc()} | errors:error().
get(SessionId, GroupId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_group, id = GroupId, aspect = instance, scope = private},
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves group doc restricted to shared data by given GroupId and AuthHint.
%% @end
%%--------------------------------------------------------------------
-spec get_shared_data(gs_client_worker:client(), od_group:id(), gs_protocol:auth_hint()) ->
    {ok, od_group:doc()} | errors:error().
get_shared_data(SessionId, GroupId, AuthHint) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_group, id = GroupId, aspect = instance, scope = shared},
        auth_hint = AuthHint,
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves group name.
%% @end
%%--------------------------------------------------------------------
-spec get_name(od_group:id()) -> {ok, od_group:name()} | errors:error().
get_name(GroupId) ->
    get_name(?ROOT_SESS_ID, GroupId, ?THROUGH_PROVIDER(oneprovider:get_id())).

-spec get_name(gs_client_worker:client(), od_group:id(), gs_protocol:auth_hint()) ->
    {ok, od_group:name()} | errors:error().
get_name(SessionId, GroupId, AuthHint) ->
    case get_shared_data(SessionId, GroupId, AuthHint) of
        {ok, #document{value = #od_group{name = Name}}} ->
            {ok, Name};
        {error, _} = Error ->
            Error
    end.


-spec has_eff_privilege(od_group:record(), od_user:id(), privileges:group_privilege()) ->
    boolean().
has_eff_privilege(GroupRecord, UserId, Privilege) ->
    has_eff_privileges(GroupRecord, UserId, [Privilege]).


-spec has_eff_privileges(od_group:record(), od_user:id(), [privileges:group_privilege()]) ->
    boolean().
has_eff_privileges(#od_group{eff_users = EffUsers}, UserId, Privileges) ->
    UserPrivileges = maps:get(UserId, EffUsers, []),
    lists_utils:is_subset(Privileges, UserPrivileges).


-spec can_view_user_through_group(gs_client_worker:client(), od_group:id(),
    ClientUserId :: od_user:id(), TargetUserId :: od_user:id()) -> boolean().
can_view_user_through_group(SessionId, GroupId, ClientUserId, TargetUserId) ->
    case get(SessionId, GroupId) of
        {ok, GroupDoc = #document{}} ->
            can_view_user_through_group(GroupDoc, ClientUserId, TargetUserId);
        _ ->
            false
    end.

-spec can_view_user_through_group(od_group:doc(), ClientUserId :: od_user:id(),
    TargetUserId :: od_user:id()) -> boolean().
can_view_user_through_group(GroupDoc, ClientUserId, TargetUserId) ->
    has_eff_privilege(GroupDoc#document.value, ClientUserId, ?GROUP_VIEW) andalso
        has_eff_user(GroupDoc, TargetUserId).


%% @private
-spec has_eff_user(od_group:doc(), od_user:id()) -> boolean().
has_eff_user(#document{value = #od_group{eff_users = EffUsers}}, UserId) ->
    maps:is_key(UserId, EffUsers).
