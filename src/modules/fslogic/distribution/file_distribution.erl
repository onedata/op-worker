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

-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/file_distribution.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").

%% API
-export([get_file_distribution/2]).


-type storage_dir_size() :: #storage_dir_size{}.
-type provider_dir_distribution() :: #provider_dir_distribution{}.
-type provider_reg_distribution() :: #file_distribution{}.

-type dir_distribution() :: #dir_distribution{}.
-type symlink_distribution() :: #symlink_distribution{}.
-type reg_distribution() :: #reg_distribution{}.

-type get_request() :: #file_distribution_get_request{}.
-type get_result() :: #file_distribution_get_result{}.

-export_type([
    provider_dir_distribution/0, dir_distribution/0,
    symlink_distribution/0,
    provider_reg_distribution/0, reg_distribution/0,
    get_request/0, get_result/0
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_file_distribution(user_ctx:ctx(), file_ctx:ctx()) ->
    {ok, get_result()}.
get_file_distribution(UserCtx, FileCtx0) ->
    FileCtx1 = file_ctx:assert_file_exists(FileCtx0),
    FileCtx2 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx1,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?read_metadata_mask)]
    ),

    {FileType, FileCtx3} = file_ctx:get_type(FileCtx2),

    {ok, #file_distribution_get_result{distribution = case FileType of
        ?DIRECTORY_TYPE -> get_dir_distribution(UserCtx, FileCtx3);
        ?SYMLINK_TYPE -> get_symlink_distribution(FileCtx3);
        _ -> get_reg_distribution(UserCtx, FileCtx3)
    end}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_dir_distribution(user_ctx:ctx(), file_ctx:ctx()) -> dir_distribution().
get_dir_distribution(UserCtx, FileCtx0) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    FileRef = ?FILE_REF(file_ctx:get_logical_guid_const(FileCtx0)),

    DistributionPerProvider = maps:map(fun(ProviderId, StatNames) ->
        case lfm:browse_dir_current_stats(SessionId, FileRef, ProviderId, StatNames) of
            {ok, ProviderStats} ->
                build_provider_dir_distribution(ProviderStats);
            {error, Errno} ->
                ?ERROR_POSIX(Errno)
        end
    end, build_get_dir_stat_names(FileCtx0)),

    #dir_distribution{distribution_per_provider = DistributionPerProvider}.


%% @private
-spec build_get_dir_stat_names(file_ctx:ctx()) -> dir_stats_collection:stats_selector().
build_get_dir_stat_names(FileCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {ok, StoragesByProvider} = space_logic:get_storages_by_provider(SpaceId),

    maps:map(fun(_ProviderId, SupportingStorages) ->
        maps:fold(fun(StorageId, _, Acc) ->
            [?SIZE_ON_STORAGE(StorageId) | Acc]
        end, [<<"total_size">>], SupportingStorages)
    end, StoragesByProvider).


%% @private
-spec build_provider_dir_distribution(time_series_collection:slice()) ->
    provider_dir_distribution().
build_provider_dir_distribution(ProviderDirStats) ->
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
-spec get_symlink_distribution(file_ctx:ctx()) -> symlink_distribution().
get_symlink_distribution(FileCtx) ->
    {ok, StoragesByProvider} = space_logic:get_storages_by_provider(file_ctx:get_space_id_const(FileCtx)),
    #symlink_distribution{
        storages_per_provider = maps:map(fun(_ProviderId, ProviderStorages) ->
            maps:keys(ProviderStorages)
        end, StoragesByProvider)
    }.


%% @private
-spec get_reg_distribution(user_ctx:ctx(), file_ctx:ctx()) -> reg_distribution().
get_reg_distribution(UserCtx, FileCtx) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    FileRef = ?FILE_REF(file_ctx:get_logical_guid_const(FileCtx)),
    {ok, Providers} = space_logic:get_provider_ids(file_ctx:get_space_id_const(FileCtx)),
    
    %% @TODO VFS-9498 - Compile file distribution knowledge based on version vectors
    #reg_distribution{blocks_per_provider = lists:foldl(fun(ProviderId, Acc) ->
        Distribution = case lfm:get_local_file_distribution(SessionId, FileRef, ProviderId) of
            {ok, ProviderDistribution} ->
                ProviderDistribution;
            {error, _} = Error ->
                Error
        end,
        Acc#{ProviderId => Distribution}
    end, #{}, Providers)}.
