%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for reading and manipulating od_user records synchronized
%%% via Graph Sync. Requests are delegated to gs_client_worker, which decides
%%% if they should be served from cache or handled by Onezone.
%%% NOTE: This is the only valid way to interact with od_user records, to
%%% ensure consistency, no direct requests to datastore or OZ REST should
%%% be performed.
%%% @end
%%%-------------------------------------------------------------------
-module(user_logic).
-author("Lukasz Opiola").

-include("graph_sync/provider_graph_sync.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/aai/aai.hrl").

-export([get/2, get_protected_data/2, get_shared_data/3]).
-export([get_full_name/1, get_full_name/3]).
-export([fetch_idp_access_token/3]).
-export([has_eff_group/2, has_eff_group/3]).
-export([get_eff_spaces/1, get_eff_spaces/2]).
-export([has_eff_space/2, has_eff_space/3]).
-export([get_space_by_name/3]).
-export([get_eff_handle_services/1]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Retrieves user doc by given UserId.
%% @end
%%--------------------------------------------------------------------
-spec get(gs_client_worker:client(), od_user:id()) ->
    {ok, od_user:doc()} | errors:error().
get(_, ?ROOT_USER_ID) ->
    {ok, #document{key = ?ROOT_USER_ID, value = #od_user{full_name = <<"root">>}}};
get(_, ?GUEST_USER_ID) ->
    {ok, #document{key = ?GUEST_USER_ID, value = #od_user{full_name = <<"nobody">>}}};
get(Client, UserId) ->
    gs_client_worker:request(Client, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_user, id = UserId, aspect = instance, scope = private},
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves user doc restricted to protected data by given UserId.
%% @end
%%--------------------------------------------------------------------
-spec get_protected_data(gs_client_worker:client(), od_user:id()) ->
    {ok, od_user:doc()} | errors:error().
get_protected_data(_, ?ROOT_USER_ID) ->
    {ok, #document{key = ?ROOT_USER_ID, value = #od_user{full_name = <<"root">>}}};
get_protected_data(_, ?GUEST_USER_ID) ->
    {ok, #document{key = ?GUEST_USER_ID, value = #od_user{full_name = <<"nobody">>}}};
get_protected_data(?ROOT_SESS_ID, UserId) ->
    get_protected_data(?ROOT_SESS_ID, UserId, ?THROUGH_PROVIDER(oneprovider:get_id()));
get_protected_data(Client, UserId) ->
    get_protected_data(Client, UserId, undefined).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retrieves user doc restricted to protected data by given UserId. Allows
%% to provide AuthHint.
%% @end
%%--------------------------------------------------------------------
-spec get_protected_data(gs_client_worker:client(), od_user:id(), gs_protocol:auth_hint()) ->
    {ok, od_user:doc()} | errors:error().
get_protected_data(Client, UserId, AuthHint) ->
    gs_client_worker:request(Client, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_user, id = UserId, aspect = instance, scope = protected},
        subscribe = true,
        auth_hint = AuthHint
    }).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves user doc restricted to shared data by given UserId and AuthHint.
%% @end
%%--------------------------------------------------------------------
-spec get_shared_data(gs_client_worker:client(), od_user:id(), gs_protocol:auth_hint()) ->
    {ok, od_user:doc()} | errors:error().
get_shared_data(_, ?ROOT_USER_ID, _) ->
    {ok, #document{key = ?ROOT_USER_ID, value = #od_user{full_name = <<"root">>}}};
get_shared_data(_, ?GUEST_USER_ID, _) ->
    {ok, #document{key = ?GUEST_USER_ID, value = #od_user{full_name = <<"nobody">>}}};
get_shared_data(Client, UserId, AuthHint) ->
    gs_client_worker:request(Client, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_user, id = UserId, aspect = instance, scope = shared},
        auth_hint = AuthHint,
        subscribe = true
    }).


-spec get_full_name(od_user:id()) -> {ok, od_user:full_name()} | errors:error().
get_full_name(UserId) ->
    get_full_name(?ROOT_SESS_ID, UserId, ?THROUGH_PROVIDER(oneprovider:get_id())).


-spec get_full_name(gs_client_worker:client(), od_user:id(), gs_protocol:auth_hint()) ->
    od_user:full_name() | errors:error().
get_full_name(Client, UserId, AuthHint) ->
    case get_shared_data(Client, UserId, AuthHint) of
        {ok, #document{value = #od_user{full_name = FullName}}} ->
            {ok, FullName};
        {error, Reason} ->
            {error, Reason}
    end.


-spec fetch_idp_access_token(gs_client_worker:client(), od_user:id(), IdP :: binary()) ->
    {ok, {AccessToken :: binary(), Ttl :: non_neg_integer()}} | errors:error().
fetch_idp_access_token(Client, UserId, IdP) ->
    Result = gs_client_worker:request(Client, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_user, id = UserId, aspect = {idp_access_token, IdP}}
    }),
    case Result of
        {ok, #{<<"token">> := Token, <<"ttl">> := Ttl}} ->
            {ok, {Token, Ttl}};
        {error, Reason} ->
            {error, Reason}
    end.


-spec has_eff_group(od_user:doc(), od_group:id()) -> boolean().
has_eff_group(#document{value = #od_user{eff_groups = EffGroups}}, GroupId) ->
    lists:member(GroupId, EffGroups).


-spec has_eff_group(gs_client_worker:client(), od_user:id(), od_group:id()) -> boolean().
has_eff_group(Client, UserId, GroupId) when is_binary(UserId) ->
    case get(Client, UserId) of
        {ok, UserDoc = #document{}} ->
            has_eff_group(UserDoc, GroupId);
        {error, _} ->
            false
    end.


-spec get_eff_spaces(od_user:doc() | od_user:record()) ->
    {ok, [od_space:id()]} | errors:error().
get_eff_spaces(#od_user{eff_spaces = EffSpaces}) ->
    {ok, EffSpaces};
get_eff_spaces(#document{value = User}) ->
    get_eff_spaces(User).


-spec get_eff_spaces(gs_client_worker:client(), od_user:id()) ->
    {ok, [od_space:id()]} | errors:error().
get_eff_spaces(Client, UserId) ->
    case get(Client, UserId) of
        {ok, Doc} ->
            get_eff_spaces(Doc);
        {error, Reason} ->
            {error, Reason}
    end.


-spec has_eff_space(od_user:doc(), od_space:id()) -> boolean().
has_eff_space(#document{value = #od_user{eff_spaces = EffSpaces}}, SpaceId) ->
    lists:member(SpaceId, EffSpaces).


-spec has_eff_space(gs_client_worker:client(), od_user:id(), od_space:id()) ->
    boolean().
has_eff_space(Client, UserId, SpaceId) when is_binary(UserId) ->
    case get(Client, UserId) of
        {ok, UserDoc = #document{}} ->
            has_eff_space(UserDoc, SpaceId);
        {error, _} ->
            false
    end.


-spec get_space_by_name(gs_client_worker:client(), od_user:id() | od_user:doc(),
    od_space:name()) -> {true, od_space:id()} | false.
get_space_by_name(Client, UserDoc = #document{}, SpaceName) ->
    {ok, Spaces} = get_eff_spaces(UserDoc),
    get_space_by_name_internal(Client, SpaceName, Spaces);
get_space_by_name(Client, UserId, SpaceName) ->
    case get(Client, UserId) of
        {error, _} ->
            false;
        {ok, UserDoc} ->
            get_space_by_name(Client, UserDoc, SpaceName)
    end.


%% @private
-spec get_space_by_name_internal(gs_client_worker:client(), od_space:name(),
    [od_space:id()]) -> {true, od_space:id()} | false.
get_space_by_name_internal(_Client, _SpaceName, []) ->
    false;
get_space_by_name_internal(Client, SpaceName, [SpaceId | Rest]) ->
    case space_logic:get_name(Client, SpaceId) of
        {ok, SpaceName} ->
            {true, SpaceId};
        _ ->
            get_space_by_name_internal(Client, SpaceName, Rest)
    end.


-spec get_eff_handle_services(od_user:doc() | od_user:record()) ->
    {ok, [od_handle_service:id()]} | errors:error().
get_eff_handle_services(#od_user{eff_handle_services = HServices}) ->
    {ok, HServices};
get_eff_handle_services(#document{value = User}) ->
    get_eff_handle_services(User).
