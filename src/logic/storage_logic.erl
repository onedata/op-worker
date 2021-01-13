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
%%% Record `od_storage` contains storage public data that can be shared between providers.
%%% Storage private information is stored using `storage_config` model.
%%%
%%% Module `storage` is an overlay to this module and `storage_config`.
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
-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").

-export([create_in_zone/4, create_in_zone/5, delete_in_zone/1]).
-export([get/1, get_shared_data/2]).
-export([support_space/3]).
-export([update_space_support_size/3]).
-export([revoke_space_support/2]).
-export([get_name_of_local_storage/1, get_name_of_remote_storage/2]).
-export([get_qos_parameters_of_local_storage/1, get_qos_parameters_of_remote_storage/2]).
-export([get_provider/2]).
-export([get_spaces/1]).
-export([is_imported/1, is_local_storage_readonly/1, is_storage_readonly/2]).
-export([is_local_storage_supporting_space/2]).
-export([supports_access_type/3]).
-export([update_name/2]).
-export([set_qos_parameters/2]).
-export([set_imported/2, update_readonly_and_imported/3]).
-export([upgrade_legacy_support/2]).

-compile({no_auto_import, [get/1]}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv create_in_zone(Name, ImportedStorage, undefined)
%%--------------------------------------------------------------------
-spec create_in_zone(od_storage:name(), storage:imported(), storage:readonly(), storage:qos_parameters()) -> 
    {ok, storage:id()} | errors:error().
create_in_zone(Name, ImportedStorage, Readonly, QosParameters) ->
    create_in_zone(Name, ImportedStorage, Readonly, QosParameters, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Creates document containing storage public information in Onezone.
%% @end
%%--------------------------------------------------------------------
-spec create_in_zone(od_storage:name(), storage:imported() | unknown, storage:readonly(), 
    storage:qos_parameters(), storage:id() | undefined) -> {ok, storage:id()} | errors:error().
create_in_zone(Name, ImportedStorage, Readonly, QosParameters, StorageId) ->
    PartialData = case ImportedStorage of
        unknown-> #{};
        _ -> #{<<"imported">> => ImportedStorage}
    end,
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance},
        data = PartialData#{
            <<"name">> => Name,
            <<"readonly">> => Readonly,
            <<"qosParameters">> => QosParameters
        }
    }),
    ?CREATE_RETURN_ID(?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id())
    end)).


-spec delete_in_zone(storage:id()) -> ok | errors:error().
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


-spec get(storage:id()) -> {ok, od_storage:doc()} | errors:error().
get(StorageId) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance},
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves storage details shared between providers through given space.
%% @end
%%--------------------------------------------------------------------
-spec get_shared_data(storage:id(), od_space:id()) -> {ok, od_storage:doc()} | errors:error().
get_shared_data(StorageId, SpaceId) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance, scope = shared},
        subscribe = true,
        auth_hint = ?THROUGH_SPACE(SpaceId)
    }).


-spec support_space(storage:id(), tokens:serialized(), od_space:support_size()) ->
    {ok, od_space:id()} | errors:error().
support_space(StorageId, SpaceSupportToken, SupportSize) ->
    Data = #{<<"token">> => SpaceSupportToken, <<"size">> => SupportSize},
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


-spec update_space_support_size(storage:id(), od_space:id(), NewSupportSize :: integer()) ->
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


-spec revoke_space_support(storage:id(), od_space:id()) -> ok | errors:error().
revoke_space_support(StorageId, SpaceId) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = delete,
        gri = #gri{type = od_storage, id = StorageId, aspect = {space, SpaceId}}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_space, SpaceId),
        gs_client_worker:invalidate_cache(od_storage, StorageId),
        gs_client_worker:invalidate_cache(od_provider, oneprovider:get_id())
    end).


-spec get_name_of_local_storage(storage:id() | od_storage:doc()) -> {ok, storage:name()} | errors:error().
get_name_of_local_storage(#document{value = #od_storage{name = Name}}) ->
    {ok, Name};
get_name_of_local_storage(StorageId) ->
    case get(StorageId) of
        {ok, Doc} -> get_name_of_local_storage(Doc);
        {error, _} = Error -> Error
    end.


-spec get_name_of_remote_storage(storage:id(), od_space:id()) -> 
    {ok, storage:name()} | errors:error().
get_name_of_remote_storage(StorageId, SpaceId) ->
    case get_shared_data(StorageId, SpaceId) of
        {ok, #document{value = #od_storage{name = Name}}} -> {ok, Name};
        {error, _} = Error -> Error
    end.


-spec get_qos_parameters_of_local_storage(storage:id() | od_storage:doc()) ->
    {ok, od_storage:qos_parameters()} | errors:error().
get_qos_parameters_of_local_storage(#document{value = #od_storage{qos_parameters = QosParameters}}) ->
    {ok, QosParameters};
get_qos_parameters_of_local_storage(StorageId) ->
    case get(StorageId) of
        {ok, Doc} -> get_qos_parameters_of_local_storage(Doc);
        {error, _} = Error -> Error
    end.


-spec get_qos_parameters_of_remote_storage(storage:id(), od_space:id()) ->
    {ok, od_storage:qos_parameters()} | errors:error().
get_qos_parameters_of_remote_storage(StorageId, SpaceId) ->
    case get_shared_data(StorageId, SpaceId) of
        {ok, #document{value = #od_storage{qos_parameters = QosParameters}}} ->
            {ok, QosParameters};
        Error -> Error
    end.


-spec get_provider(storage:id(), od_space:id()) -> {ok, od_provider:id()} | errors:error().
get_provider(StorageId, SpaceId) ->
    case get_shared_data(StorageId, SpaceId) of
        {ok, #document{value = #od_storage{provider = Provider}}} -> {ok, Provider};
        Error -> Error
    end.

-spec get_spaces(storage:id()) -> {ok, [od_space:id()]} | errors:error().
get_spaces(StorageId) ->
    case get(StorageId) of
        {ok, #document{value = #od_storage{spaces = Spaces}}} ->
            {ok, Spaces};
        Error -> Error
    end.


-spec is_imported(storage:id()) -> {ok, boolean()} | errors:error().
is_imported(StorageId) ->
    case get(StorageId) of
        {ok, #document{value = #od_storage{imported = ImportedStorage}}} ->
            {ok, ImportedStorage};
        Error -> Error
    end.


-spec is_local_storage_readonly(storage:id()) -> {ok, boolean()} | errors:error().
is_local_storage_readonly(StorageId) ->
    case storage_logic:get(StorageId) of
        {ok, #document{value = #od_storage{readonly = Readonly}}} ->
            {ok, Readonly};
        Error -> Error
    end.


-spec is_storage_readonly(storage:id(), od_space:id()) -> {ok, boolean()} | errors:error().
is_storage_readonly(StorageId, SpaceId) ->
    case get_shared_data(StorageId, SpaceId) of
        {ok, #document{value = #od_storage{readonly = Readonly}}} ->
            {ok, Readonly};
        Error -> Error
    end.


-spec is_local_storage_supporting_space(storage:id(), od_space:id()) -> boolean().
is_local_storage_supporting_space(StorageId, SpaceId) ->
    case space_logic:get_local_storage_ids(SpaceId) of
        {ok, LocalStorageIds} -> lists:member(StorageId, LocalStorageIds);
        _ -> false
    end.


-spec supports_access_type(storage:id(), od_space:id(), SufficientAccessType :: storage:access_type()) ->
    boolean().
supports_access_type(_StorageId, _SpaceId, ?READONLY) ->
    true;
supports_access_type(StorageId, SpaceId, ?READWRITE) ->
    not storage:is_storage_readonly(StorageId, SpaceId).


-spec update_name(storage:id(), od_storage:name()) -> ok | errors:error().
update_name(StorageId, NewName) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = update,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance},
        data = #{<<"name">> => NewName}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_storage, StorageId)
    end).


-spec set_qos_parameters(storage:id(), od_storage:qos_parameters()) -> ok | errors:error().
set_qos_parameters(StorageId, QosParameters) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = update,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance},
        data = #{<<"qosParameters">> => QosParameters}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_storage, StorageId)
    end).


-spec set_imported(storage:id(), boolean()) -> ok | errors:error().
set_imported(StorageId, Imported) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = update,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance},
        data = #{<<"imported">> => Imported}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_storage, StorageId)
    end).


update_readonly_and_imported(StorageId, Readonly, Imported) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = update,
        gri = #gri{type = od_storage, id = StorageId, aspect = instance},
        data = #{
            <<"imported">> => Imported,
            <<"readonly">> => Readonly
        }
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_storage, StorageId)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Upgrades legacy space support in Onezone to model with new storages.
%% This adds relation between given storage and given space and removes
%% this space from virtual storage (with id equal to that of provider) in Onezone.
%% Can be only used by providers already supporting given space.
%%
%% Dedicated for upgrading Oneprovider from 19.02.* to 19.09.*.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_legacy_support(storage:id(), od_space:id()) -> ok | errors:error().
upgrade_legacy_support(StorageId, SpaceId) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_storage, id = StorageId, aspect = {upgrade_legacy_support, SpaceId}}
    }),
    ?ON_SUCCESS(Result, fun(_) ->
        gs_client_worker:invalidate_cache(od_space, SpaceId)
    end).
