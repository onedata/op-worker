%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for reading and manipulating od_space records synchronized
%%% via Graph Sync. Requests are delegated to gs_client_worker, which decides
%%% if they should be served from cache or handled by Onezone.
%%% NOTE: This is the only valid way to interact with od_space records, to
%%% ensure consistency, no direct requests to datastore or OZ REST should
%%% be performed.
%%% @end
%%%-------------------------------------------------------------------
-module(space_logic).
-author("Lukasz Opiola").

-include("modules/fslogic/fslogic_common.hrl").
-include("graph_sync/provider_graph_sync.hrl").
-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/errors.hrl").

-export([get/2, get_protected_data/2]).
-export([get_name/2]).
-export([get_eff_users/2, has_eff_user/2, has_eff_user/3]).
-export([has_eff_privilege/3, has_eff_privileges/3]).
-export([get_eff_groups/2, get_shares/2, get_local_storage_ids/1, get_local_storage_id/1]).
-export([get_provider_ids/2]).
-export([is_supported/2, is_supported/3]).
-export([is_supported_by_storage/2]).
-export([can_view_user_through_space/3, can_view_user_through_space/4]).
-export([can_view_group_through_space/3, can_view_group_through_space/4]).
-export([harvest_metadata/5]).
-export([get_harvesters/1]).

-define(HARVEST_METADATA_TIMEOUT, application:get_env(
    ?APP_NAME, graph_sync_harvest_metadata_request_timeout, 120000
)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves space doc by given SpaceId.
%% @end
%%--------------------------------------------------------------------
-spec get(gs_client_worker:client(), od_space:id()) ->
    {ok, od_space:doc()} | errors:error().
get(SessionId, SpaceId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_space, id = SpaceId, aspect = instance, scope = private},
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves space doc restricted to protected data by given SpaceId.
%% @end
%%--------------------------------------------------------------------
-spec get_protected_data(gs_client_worker:client(), od_space:id()) ->
    {ok, od_space:doc()} | errors:error().
get_protected_data(SessionId, SpaceId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_space, id = SpaceId, aspect = instance, scope = protected},
        subscribe = true
    }).


-spec get_name(gs_client_worker:client(), od_space:id()) ->
    {ok, od_space:name()} | errors:error().
get_name(SessionId, SpaceId) ->
    case get_protected_data(SessionId, SpaceId) of
        {ok, #document{value = #od_space{name = Name}}} ->
            {ok, Name};
        {error, _} = Error ->
            Error
    end.


-spec get_eff_users(gs_client_worker:client(), od_space:id()) ->
    {ok, #{od_user:id() => [privileges:space_privilege()]}} | errors:error().
get_eff_users(SessionId, SpaceId) ->
    case get(SessionId, SpaceId) of
        {ok, #document{value = #od_space{eff_users = EffUsers}}} ->
            {ok, EffUsers};
        {error, _} = Error ->
            Error
    end.


-spec has_eff_user(od_space:doc(), od_user:id()) -> boolean().
has_eff_user(#document{value = #od_space{eff_users = EffUsers}}, UserId) ->
    maps:is_key(UserId, EffUsers).


-spec has_eff_user(gs_client_worker:client(), od_space:id(), od_user:id()) ->
    boolean().
has_eff_user(SessionId, SpaceId, UserId) ->
    case get(SessionId, SpaceId) of
        {ok, SpaceDoc = #document{}} ->
            has_eff_user(SpaceDoc, UserId);
        _ ->
            false
    end.


-spec has_eff_privilege(od_space:doc() | od_space:id(), od_user:id(),
    privileges:space_privilege()) -> boolean().
has_eff_privilege(SpaceDocOrId, UserId, Privilege) ->
    has_eff_privileges(SpaceDocOrId, UserId, [Privilege]).


-spec has_eff_privileges(od_space:doc() | od_space:id(), od_user:id(),
    [privileges:space_privilege()]) -> boolean().
has_eff_privileges(#document{value = #od_space{eff_users = EffUsers}}, UserId, Privileges) ->
    UserPrivileges = maps:get(UserId, EffUsers, []),
    lists:all(fun(Privilege) ->
        lists:member(Privilege, UserPrivileges)
    end, Privileges);
has_eff_privileges(SpaceId, UserId, Privileges) ->
    case get(?ROOT_SESS_ID, SpaceId) of
        {ok, #document{} = SpaceDoc} ->
            has_eff_privileges(SpaceDoc, UserId, Privileges);
        _ ->
            false
    end.


-spec get_eff_groups(gs_client_worker:client(), od_space:id()) ->
    {ok, #{od_group:id() => [privileges:space_privilege()]}} | errors:error().
get_eff_groups(SessionId, SpaceId) ->
    case get(SessionId, SpaceId) of
        {ok, #document{value = #od_space{eff_groups = EffGroups}}} ->
            {ok, EffGroups};
        {error, _} = Error ->
            Error
    end.


-spec has_eff_group(od_space:doc(), od_group:id()) -> boolean().
has_eff_group(#document{value = #od_space{eff_groups = EffGroups}}, GroupId) ->
    maps:is_key(GroupId, EffGroups).


-spec get_shares(gs_client_worker:client(), od_space:id()) ->
    {ok, [od_share:id()]} | errors:error().
get_shares(SessionId, SpaceId) ->
    case get(SessionId, SpaceId) of
        {ok, #document{value = #od_space{shares = Shares}}} ->
            {ok, Shares};
        {error, _} = Error ->
            Error
    end.

%%-------------------------------------------------------------------
%% @doc
%% @TODO VFS-5497 Remove after allowing to support one space with many storages on one provider
%% This function returns StorageId for given SpaceId.
%% @end
%%-------------------------------------------------------------------
-spec get_local_storage_id(od_space:id()) -> {ok, od_storage:id()} | errors:error().
get_local_storage_id(SpaceId) ->
    case get_local_storage_ids(SpaceId) of
        {ok, []} -> {error, space_not_supported};
        {ok, [StorageId | _]} -> {ok, StorageId};
        Other -> Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of storage ids supporting given space under this provider.
%% @end
%%--------------------------------------------------------------------
-spec get_local_storage_ids(od_space:id()) -> {ok, [od_storage:id()]} | errors:error().
get_local_storage_ids(SpaceId) ->
    case get(?ROOT_SESS_ID, SpaceId) of
        {ok, #document{value = #od_space{local_storages = LocalStorages}}} ->
            {ok, LocalStorages};
        {error, _} = Error ->
            Error
    end.


-spec get_all_storage_ids(od_space:id()) -> {ok, [od_storage:id()]} | errors:error().
get_all_storage_ids(SpaceId) ->
    case get(?ROOT_SESS_ID, SpaceId) of
        {ok, #document{value = #od_space{storages = AllStorages}}} ->
            {ok, maps:keys(AllStorages)};
        {error, _} = Error ->
            Error
    end.


-spec get_provider_ids(gs_client_worker:client(), od_space:id()) ->
    {ok, [od_provider:id()]} | errors:error().
get_provider_ids(SessionId, SpaceId) ->
    case get(SessionId, SpaceId) of
        {ok, #document{value = #od_space{providers = Providers}}} ->
            {ok, maps:keys(Providers)};
        {error, _} = Error ->
            Error
    end.


-spec is_supported(od_space:doc() | od_space:record(), od_provider:id()) ->
    boolean().
is_supported(#od_space{providers = Providers}, ProviderId) ->
    maps:is_key(ProviderId, Providers);
is_supported(#document{value = Space}, ProviderId) ->
    is_supported(Space, ProviderId).


-spec is_supported(gs_client_worker:client(), od_space:id(), od_provider:id()) ->
    boolean().
is_supported(SessionId, SpaceId, ProviderId) ->
    case get(SessionId, SpaceId) of
        {ok, SpaceDoc = #document{}} ->
            is_supported(SpaceDoc, ProviderId);
        _ ->
            false
    end.


-spec is_supported_by_storage(od_space:id(), od_storage:id()) -> boolean().
is_supported_by_storage(SpaceId, StorageId) ->
    case get_all_storage_ids(SpaceId) of
        {ok, AllStorageIds} -> lists:member(StorageId, AllStorageIds);
        _ -> false
    end.


-spec can_view_user_through_space(gs_client_worker:client(), od_space:id(),
    ClientUserId :: od_user:id(), TargetUserId :: od_user:id()) -> boolean().
can_view_user_through_space(SessionId, SpaceId, ClientUserId, TargetUserId) ->
    case get(SessionId, SpaceId) of
        {ok, SpaceDoc = #document{}} ->
            can_view_user_through_space(SpaceDoc, ClientUserId, TargetUserId);
        _ ->
            false
    end.


-spec can_view_user_through_space(od_space:doc(), ClientUserId :: od_user:id(),
    TargetUserId :: od_user:id()) -> boolean().
can_view_user_through_space(SpaceDoc, ClientUserId, TargetUserId) ->
    has_eff_privilege(SpaceDoc, ClientUserId, ?SPACE_VIEW) andalso
        has_eff_user(SpaceDoc, TargetUserId).


-spec can_view_group_through_space(gs_client_worker:client(), od_space:id(),
    ClientUserId :: od_user:id(), od_group:id()) -> boolean().
can_view_group_through_space(SessionId, SpaceId, ClientUserId, GroupId) ->
    case get(SessionId, SpaceId) of
        {ok, SpaceDoc = #document{}} ->
            can_view_group_through_space(SpaceDoc, ClientUserId, GroupId);
        _ ->
            false
    end.


-spec can_view_group_through_space(od_space:doc(), ClientUserId :: od_user:id(),
    od_group:id()) -> boolean().
can_view_group_through_space(SpaceDoc, ClientUserId, GroupId) ->
    has_eff_privilege(SpaceDoc, ClientUserId, ?SPACE_VIEW) andalso
        has_eff_group(SpaceDoc, GroupId).

%%--------------------------------------------------------------------
%% @doc
%% Pushes batch of metadata changes to Onezone.
%% Metadata are harvested for given SpaceId and Destination.
%% Destination is the structure that describes target Harvesters and
%% associated Indices.
%% MaxStreamSeq and MaxSeq are sent to allow for tracking progress of
%% harvesting. MaxStreamSeq is the highest sequence number processed by the
%% calling harvesting_stream. MaxSeq is the highest sequence number in given
%% space.
%% This function can return following values:
%%     * {ok, FailureMap :: #{od_harvester:id() =>
%%           #{od_harvester:index() => FirstFailedSeq :: couchbase_changes:seq()}
%%        }}
%%        FirstFailedSeq is the first sequence number on which harvesting
%%        failed for given index.
%%        If harvesting succeeds for whole Destination, the map is empty.
%%     * {error, _} :: errors:error()
%%
%% NOTE!!!
%% If you introduce any changes in this function, please ensure that
%% docs in {@link harvesting_stream} module are up to date.
%% @end
%%--------------------------------------------------------------------
-spec harvest_metadata(od_space:id(), harvesting_destination:destination(),
    harvesting_batch:batch_entries(), couchbase_changes:seq(),
    couchbase_changes:seq()) -> {ok, harvesting_result:failure_map()} | errors:error().
harvest_metadata(SpaceId, Destination, Batch, MaxStreamSeq, MaxSeq)->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_space, id = SpaceId,
            aspect = harvest_metadata, scope = private
        },
        data = #{
            <<"destination">> => Destination,
            <<"maxSeq">> => MaxSeq,
            <<"maxStreamSeq">> => MaxStreamSeq,
            <<"batch">> => Batch
        }
    }, ?HARVEST_METADATA_TIMEOUT).

-spec get_harvesters(od_space:doc() | od_space:id()) ->
    {ok, [od_harvester:id()]} | errors:error().
get_harvesters(#document{value = #od_space{harvesters = Harvesters}}) ->
    {ok, Harvesters};
get_harvesters(SpaceId) ->
    case space_logic:get(?ROOT_SESS_ID, SpaceId) of
        {ok, Doc} ->
            get_harvesters(Doc);
        {error, _} = Error ->
            ?error("space_logic:get_harvesters(~p) failed due to ~p", [SpaceId, Error]),
            Error
    end.