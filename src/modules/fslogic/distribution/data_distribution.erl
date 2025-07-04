%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module responsible for performing operations on file distribution.
%%% @end
%%%--------------------------------------------------------------------
-module(data_distribution).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").
-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/data_distribution.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("proto/oneprovider/provider_rpc_messages.hrl").

%% API
-export([
    gather/2,
    gather_storage_locations/2]).

% `undefined` physical size means that file was not yet created on this storage.
-type dir_physical_size() :: undefined | non_neg_integer().

-type provider_dir_distribution() :: #provider_dir_distribution{}.
-type dir_distribution() :: #dir_distribution_gather_result{}.

-type provider_reg_distribution() :: #provider_reg_distribution_get_result{}.
-type reg_distribution() :: #reg_distribution_gather_result{}.

-type symlink_distribution() :: #symlink_distribution_gather_result{}.

-type get_request() :: #data_distribution_gather_request{}.
-type get_result() :: #data_distribution_gather_result{}.

-type locations_per_storage() :: #{
    storage:id() => file_location:storage_file_id()
}.
-type storage_locations_per_provider() :: #{
    od_provider:id() => locations_per_storage()
}.

-export_type([
    dir_physical_size/0,
    provider_dir_distribution/0, dir_distribution/0,
    provider_reg_distribution/0, reg_distribution/0,
    symlink_distribution/0,
    get_request/0, get_result/0
]).

-export_type([storage_locations_per_provider/0, locations_per_storage/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec gather(user_ctx:ctx(), file_ctx:ctx()) ->
    {ok, get_result()}.
gather(UserCtx, FileCtx0) ->
    FileCtx1 = file_ctx:assert_file_exists(FileCtx0),
    FileCtx2 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx1,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?read_metadata_mask)]
    ),

    {FileType, FileCtx3} = file_ctx:get_type(FileCtx2),

    {ok, #data_distribution_gather_result{distribution = case FileType of
        ?DIRECTORY_TYPE -> gather_dir_distribution(FileCtx3);
        ?SYMLINK_TYPE -> build_symlink_distribution(FileCtx3);
        _ -> gather_reg_distribution(FileCtx3)
    end}}.


-spec gather_storage_locations(user_ctx:ctx(), file_ctx:ctx()) ->
    {ok, storage_locations_per_provider()} | errors:error().
gather_storage_locations(UserCtx, FileCtx0) ->
    FileCtx1 = file_ctx:assert_file_exists(FileCtx0),
    FileCtx2 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx1,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?read_metadata_mask)]
    ),
    {FileType, FileCtx3} = file_ctx:get_type(FileCtx2),
    
    case FileType of
        ?DIRECTORY_TYPE -> {ok, gather_dir_storage_locations(FileCtx3)};
        ?SYMLINK_TYPE -> ?ERROR_NOT_SUPPORTED;
        _ -> {ok, gather_reg_storage_locations(FileCtx3)}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec gather_dir_distribution(file_ctx:ctx()) -> dir_distribution().
gather_dir_distribution(FileCtx) ->
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),

    RequestsPerProvider = build_dir_distribution_provider_requests(FileCtx),
    SizeStatsPerProvider = provider_rpc:gather(FileGuid, RequestsPerProvider),

    DistributionPerProvider = maps:map(fun 
        (_ProviderId, {ok, ProviderDistributionResult}) ->
            build_provider_dir_distribution(ProviderDistributionResult);
        (ProviderId, ?ERROR_NOT_SUPPORTED) ->
            % peer provider is in older version, maybe it understands older request
            % @TODO VFS-12867 remove in next major release after 22.02.*
            case provider_rpc:call(ProviderId, FileGuid,
                (maps:get(ProviderId, RequestsPerProvider))#provider_dir_distribution_get_request.stats_request)
            of
                {ok, CurrentDirStats} ->
                    build_provider_dir_distribution(#provider_dir_distribution_get_result{
                        current_dir_size_stats = CurrentDirStats
                    });
                {error, _} = E ->
                    E
            end;
        (_ProviderId, {error, _} = Error) ->
            Error
    end, SizeStatsPerProvider),

    #dir_distribution_gather_result{distribution_per_provider = DistributionPerProvider}.


%% @private
-spec build_dir_distribution_provider_requests(file_ctx:ctx()) ->
    #{oneprovider:id() => #provider_dir_distribution_get_request{}}.
build_dir_distribution_provider_requests(FileCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {ok, StoragesByProvider} = space_logic:get_storages_by_provider(SpaceId),

    maps:map(fun(_ProviderId, SupportingStorages) ->
        #provider_dir_distribution_get_request{
            stats_request = #provider_current_dir_size_stats_browse_request{
                stat_names = maps:fold(fun(StorageId, _, Acc) ->
                    [?PHYSICAL_SIZE(StorageId) | Acc]
                end, [?VIRTUAL_SIZE, ?LOGICAL_SIZE], SupportingStorages)
            }
        }
    end, StoragesByProvider).


%% @private
-spec build_provider_dir_distribution(#provider_dir_distribution_get_result{}) ->
    provider_dir_distribution().
build_provider_dir_distribution(#provider_dir_distribution_get_result{
    current_dir_size_stats = #provider_current_dir_size_stats_browse_result{status = ok, result = ProviderDirStats},
    locations_per_storage = LocationsPerStorage
}) ->
    #provider_dir_distribution{
        virtual_size = maps:get(?VIRTUAL_SIZE, ProviderDirStats),
        logical_size = maps:get(?LOGICAL_SIZE, ProviderDirStats),
        physical_size_per_storage = maps:fold(fun
            (?PHYSICAL_SIZE(StorageId), Value, Acc) ->
                Acc#{StorageId => Value};
            (_, _, Acc) ->
                Acc
        end, #{}, ProviderDirStats),
        locations_per_storage = LocationsPerStorage
    };
build_provider_dir_distribution(#provider_dir_distribution_get_result{
    current_dir_size_stats = #provider_current_dir_size_stats_browse_result{status = error, result = #{
        <<"error">> := Error, <<"storage_list">> := StorageList}},
    locations_per_storage = LocationsPerStorage
}) ->
    #provider_dir_distribution{
        virtual_size = undefined,
        logical_size = undefined,
        physical_size_per_storage = maps:from_list([{S, Error} || S <- StorageList]),
        locations_per_storage = LocationsPerStorage
    }.


%% @private
-spec build_symlink_distribution(file_ctx:ctx()) -> symlink_distribution().
build_symlink_distribution(FileCtx) ->
    {ok, StoragesByProvider} = space_logic:get_storages_by_provider(file_ctx:get_space_id_const(FileCtx)),
    #symlink_distribution_gather_result{
        storages_per_provider = maps:map(fun(_ProviderId, ProviderStorages) ->
            maps:keys(ProviderStorages)
        end, StoragesByProvider)
    }.


%% @private
-spec gather_reg_distribution(file_ctx:ctx()) -> reg_distribution().
gather_reg_distribution(FileCtx) ->
    %% @TODO VFS-9498 - Compile file distribution knowledge based on version vectors
    ProviderDistributions = provider_rpc:gather_from_cosupporting_providers(
        file_ctx:get_logical_guid_const(FileCtx),
        #provider_reg_distribution_get_request{}
    ),
    #reg_distribution_gather_result{
        distribution_per_provider = maps:map(fun(_ProviderId, Result) ->
            case Result of
                {ok, Distribution} ->
                    Distribution;
                {error, _} = Error ->
                    Error
            end
        end, ProviderDistributions)
    }.


%% @private
-spec gather_reg_storage_locations(file_ctx:ctx()) -> storage_locations_per_provider().
gather_reg_storage_locations(FileCtx) ->
    gather_storage_locations(FileCtx,
        #provider_reg_storage_locations_get_request{},
        fun(#provider_reg_storage_locations_result{locations_per_storage = LocationsPerStorage}) ->
            LocationsPerStorage
        end
    ).


%% @private
-spec gather_dir_storage_locations(file_ctx:ctx()) -> storage_locations_per_provider().
gather_dir_storage_locations(FileCtx) ->
    gather_storage_locations(FileCtx,
        #provider_dir_distribution_get_request{stats_request = #provider_current_dir_size_stats_browse_request{}},
        fun(#provider_dir_distribution_get_result{locations_per_storage = LocationsPerStorage}) ->
            LocationsPerStorage
        end
    ).


%% @private
-spec gather_storage_locations(file_ctx:ctx(),
    #provider_reg_storage_locations_get_request{} | #provider_dir_distribution_get_request{},
    fun((any()) -> locations_per_storage())
) -> storage_locations_per_provider().
gather_storage_locations(FileCtx, Req, LocationsPerStorageFun) ->
    GatheredStorageLocations = provider_rpc:gather_from_cosupporting_providers(
        file_ctx:get_logical_guid_const(FileCtx),
        Req
    ),
    maps:map(
        fun (_ProviderId, {ok, OkResult}) ->
                LocationsPerStorageFun(OkResult);
            (_ProviderId, {error, _} = Error) ->
                Error
        end, GatheredStorageLocations).
