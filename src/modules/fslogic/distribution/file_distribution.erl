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
-module(file_distribution).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").
-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/file_distribution.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("proto/oneprovider/mi_interprovider_messages.hrl").

%% API
-export([gather/2]).
-export([get_deprecated_summary/2]).


-type storage_dir_size() :: #storage_dir_size{}.
-type provider_dir_distribution() :: #provider_dir_distribution{}.
-type dir_distribution() :: #dir_distribution{}.

-type storage_reg_distribution() :: #storage_reg_distribution{}.
-type provider_reg_distribution() :: #provider_reg_distribution{}.
-type reg_distribution() :: #reg_distribution{}.

-type symlink_distribution() :: #symlink_distribution{}.

-type get_request() :: #file_distribution_gather_request{}.
-type get_result() :: #file_distribution_gather_result{}.

-export_type([
    storage_dir_size/0, provider_dir_distribution/0, dir_distribution/0,
    storage_reg_distribution/0, provider_reg_distribution/0, reg_distribution/0,
    symlink_distribution/0,
    get_request/0, get_result/0
]).


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

    {ok, #file_distribution_gather_result{distribution = case FileType of
        ?DIRECTORY_TYPE -> gather_dir_distribution(FileCtx3);
        ?SYMLINK_TYPE -> gather_symlink_distribution(FileCtx3);
        _ -> gather_reg_distribution(FileCtx3)
    end}}.


%% NOTE: used by oneclient
-spec get_deprecated_summary(user_ctx:ctx(), file_ctx:ctx()) -> fslogic_worker:provider_response().
get_deprecated_summary(UserCtx, FileCtx) ->
    {ok, #file_distribution_gather_result{distribution = Distribution}} = gather(UserCtx, FileCtx),
    Res = case Distribution of
        #reg_distribution{distribution_per_provider = DistributionPerProvider} ->
            #file_distribution_summary{provider_file_distributions = 
                maps:fold(fun(ProviderId, ProviderRegDistribution, Acc) ->
                    % Currently only one storage per provider is possible
                    #provider_reg_distribution{
                        blocks_per_storage = [#storage_reg_distribution{blocks = Blocks}]
                    } = ProviderRegDistribution,
                    
                    [#provider_distribution_summary{provider_id = ProviderId, blocks = Blocks} | Acc]
                end, [], DistributionPerProvider)
            };
        _ -> 
            #file_distribution_summary{}
    end,
    #provider_response{
        status = #status{code = ?OK},
        provider_response = Res
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec gather_dir_distribution(file_ctx:ctx()) -> dir_distribution().
gather_dir_distribution(FileCtx) ->
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    
    SizeStatsPerProvider = mi_interprovider_communicator:gather(
        FileGuid, build_dir_size_stat_provider_operations(FileCtx)),

    DistributionPerProvider = maps:map(fun(_ProviderId, Result) ->
        case Result of
            {ok, ProviderStats} ->
                build_provider_dir_distribution(ProviderStats);
            {error, _} = Error ->
                Error
        end
    end, SizeStatsPerProvider),

    #dir_distribution{distribution_per_provider = DistributionPerProvider}.


%% @private
-spec build_dir_size_stat_provider_operations(file_ctx:ctx()) -> 
    #{oneprovider:id() => #browse_local_current_dir_size_stats{}}.
build_dir_size_stat_provider_operations(FileCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {ok, StoragesByProvider} = space_logic:get_storages_by_provider(SpaceId),

    maps:map(fun(_ProviderId, SupportingStorages) ->
        maps:fold(fun(StorageId, _, Acc) ->
            #browse_local_current_dir_size_stats{stat_names = [?SIZE_ON_STORAGE(StorageId) | Acc]}
        end, [<<"total_size">>], SupportingStorages)
    end, StoragesByProvider).


%% @private
-spec build_provider_dir_distribution(#local_current_dir_size_stats{}) ->
    provider_dir_distribution().
build_provider_dir_distribution(#local_current_dir_size_stats{stats = ProviderDirStats}) ->
    #provider_dir_distribution{
        logical_size = maps:get(?TOTAL_SIZE, ProviderDirStats),
        physical_size_per_storage = lists:foldl(fun
            (TSName = ?SIZE_ON_STORAGE(StorageId), Acc) ->
                Acc#{StorageId => maps:get(TSName, ProviderDirStats)};
            (_, Acc) ->
                Acc
        end, #{}, maps:keys(ProviderDirStats))
    }.


%% @private
-spec gather_symlink_distribution(file_ctx:ctx()) -> symlink_distribution().
gather_symlink_distribution(FileCtx) ->
    {ok, StoragesByProvider} = space_logic:get_storages_by_provider(file_ctx:get_space_id_const(FileCtx)),
    #symlink_distribution{
        storages_per_provider = maps:map(fun(_ProviderId, ProviderStorages) ->
            maps:keys(ProviderStorages)
        end, StoragesByProvider)
    }.


%% @private
-spec gather_reg_distribution(file_ctx:ctx()) -> reg_distribution().
gather_reg_distribution(FileCtx) ->
    %% @TODO VFS-9498 - Compile file distribution knowledge based on version vectors
    ProviderDistributions = mi_interprovider_communicator:gather_from_cosupporting_providers(
        file_ctx:get_logical_guid_const(FileCtx),
        #local_reg_file_distribution_get_request{}
    ),
    #reg_distribution{
        distribution_per_provider = maps:map(fun(_ProviderId, Result) ->
            case Result of
                {ok, Distribution} ->
                    Distribution;
                {error, _} = Error ->
                    Error
            end
        end, ProviderDistributions)
    }.
