%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for reading and manipulating od_storage records synchronized
%%% via Graph Sync. Requests are delegated to gs_client_worker, which decides
%%% if they should be served from cache or handled by Onezone.
%%% NOTE: This is the only valid way to interact with od_storage records, to
%%% ensure consistency, no direct requests to datastore or OZ REST should
%%% be performed.
%%%
%%% NOTE: Functions from this module should not be called directly.
%%% Use module `storage` instead.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_logic).
-author("Michal Stanisz").

-include("graph_sync/provider_graph_sync.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").

-export([get/1]).
-export([create_in_zone/1, create_in_zone/2, delete_in_zone/1]).
-export([create_support/3]).
-export([update_space_support_size/3]).
-export([revoke_support/2]).
-export([set_qos_parameters/2, get_qos_parameters/1, get_qos_parameters/2]).
-export([get_spaces/1]).
-export([upgrade_legacy_support/2]).

-compile({no_auto_import, [get/1]}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv create_in_zone(StorageName, undefined)
%%--------------------------------------------------------------------
-spec create_in_zone(storage:qos_parameters()) -> {ok, od_storage:id()} | errors:error().
create_in_zone(QosParameters) ->
    create_in_zone(QosParameters, undefined).

-spec create_in_zone(storage:qos_parameters(), od_storage:id() | undefined) ->
    {ok, od_storage:id()} | errors:error().
create_in_zone(QosParameters, StorageId) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance},
        data = #{<<"qos_parameters">> => QosParameters}
    }),
    ?CREATE_RETURN_ID(?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id())
    end)).


-spec get(od_storage:id()) -> {ok, od_storage:doc()} | errors:error().
get(StorageId) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance},
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retrieves storage data shared between providers through given SpaceId.
%% @end
%%--------------------------------------------------------------------
-spec get_shared_data(od_storage:id(), od_space:id()) -> {ok, od_storage:doc()} | errors:error().
get_shared_data(StorageId, SpaceId) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance, scope = shared},
        subscribe = true,
        auth_hint = ?THROUGH_SPACE(SpaceId)
    }).


-spec delete_in_zone(od_storage:id()) -> ok | errors:error().
delete_in_zone(StorageId) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = delete,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id()),
        % only storage not supporting any space can be deleted
        % so no need to invalidate any od_space cache
        gs_client_worker:invalidate_cache(od_storage, StorageId)
    end).

-spec create_support(od_storage:id(), tokens:serialized(), integer()) -> {ok, od_space:id()}.
create_support(StorageId, SerializedToken, SupportSize) ->
    Data = #{<<"token">> => SerializedToken, <<"size">> => SupportSize},
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_storage, id = StorageId, aspect = support},
        data = Data
    }),

    ?ON_SUCCESS(?CREATE_RETURN_ID(Result), fun({ok, SpaceId}) ->
        gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id()),
        gs_client_worker:invalidate_cache(od_space, SpaceId),
        gs_client_worker:invalidate_cache(od_storage, StorageId)
    end).


-spec update_space_support_size(od_storage:id(), od_space:id(), NewSupportSize :: integer()) ->
    ok | errors:error().
update_space_support_size(StorageId, SpaceId, NewSupportSize) ->
    Data = #{<<"size">> => NewSupportSize},
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = update, data = Data,
        gri = #gri{type = od_storage, id = StorageId, aspect = {space, SpaceId}}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_space, SpaceId),
        gs_client_worker:invalidate_cache(od_storage, StorageId),
        gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id())
    end).


-spec revoke_support(od_storage:id(), od_space:id()) -> ok | errors:error().
revoke_support(StorageId, SpaceId) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = delete,
        gri = #gri{type = od_storage, id = StorageId, aspect = {space, SpaceId}}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_space, SpaceId),
        gs_client_worker:invalidate_cache(od_storage, StorageId),
        gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id())
    end).


-spec set_qos_parameters(od_storage:id(), od_storage:qos_parameters()) -> ok | errors:error().
set_qos_parameters(StorageId, QosParameters) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = update,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance},
        data = #{<<"qos_parameters">> => QosParameters}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_storage, StorageId)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Get own storage QoS parameters.
%% @end
%%--------------------------------------------------------------------
-spec get_qos_parameters(od_storage:id()) ->
    {ok, od_storage:qos_parameters()} | errors:error().
get_qos_parameters(StorageId) ->
    case get(StorageId) of
        {ok, #document{value = #od_storage{qos_parameters = QosParameters}}} ->
            {ok, QosParameters};
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get QoS parameters of storage supporting given space.
%% @end
%%--------------------------------------------------------------------
-spec get_qos_parameters(od_storage:id(), od_space:id()) ->
    {ok, od_storage:qos_parameters()} | errors:error().
get_qos_parameters(StorageId, SpaceId) ->
    case get_shared_data(StorageId, SpaceId) of
        {ok, #document{value = #od_storage{qos_parameters = QosParameters}}} ->
            {ok, QosParameters};
        Error -> Error
    end.


-spec get_spaces(od_storage:id()) -> {ok, [od_space:id()]} | errors:error().
get_spaces(StorageId) ->
    case get(StorageId) of
        {ok, #document{value = #od_storage{spaces = Spaces}}} ->
            {ok, Spaces};
        Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Upgrades legacy space support in Onezone to model with new storages.
%% Dedicated for upgrading Oneprovider from 19.02.* to the next major release.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_legacy_support(od_storage:id(), od_space:id()) -> ok | errors:error().
upgrade_legacy_support(StorageId, SpaceId) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_storage, id = StorageId, aspect = {upgrade_legacy_support, SpaceId}}
    }).
