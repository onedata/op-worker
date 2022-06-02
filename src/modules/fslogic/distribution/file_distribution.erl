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


-type provider_dir_distribution() :: #provider_dir_distribution{}.
-type dir_distribution() :: #dir_distribution{}.

-type symlink_distribution() :: #symlink_distribution{}.
-type reg_distribution() :: #reg_distribution{}.

-type get_request() :: #file_distribution_get_request{}.
-type get_result() :: #file_distribution_get_result{}.

-export_type([
    provider_dir_distribution/0, dir_distribution/0,
    symlink_distribution/0,
    reg_distribution/0,
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
        _ -> get_reg_distribution(FileCtx3)
    end}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_dir_distribution(user_ctx:ctx(), file_ctx:ctx()) -> dir_distribution().
get_dir_distribution(UserCtx, FileCtx0) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    FileRef = ?FILE_REF(file_ctx:get_logical_guid_const(FileCtx0)),

    DistributionPerProvider = maps:map(fun(ProviderId, DirStatsGetReq) ->
        case lfm:browse_dir_stats(SessionId, FileRef, ProviderId, DirStatsGetReq) of
            {ok, #time_series_slice_result{slice = ProviderStats}} ->
                build_provider_dir_distribution(ProviderStats);
            {error, Errno} ->
                ?ERROR_POSIX(Errno)
        end
    end, build_get_dir_stats_requests(FileCtx0)),

    #dir_distribution{distribution_per_provider = DistributionPerProvider}.


%% @private
-spec build_get_dir_stats_requests(file_ctx:ctx()) ->
    #{oneprovider:id() => ts_browse_request:record()}.
build_get_dir_stats_requests(FileCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {ok, StoragesByProvider} = space_logic:get_storages_by_provider(SpaceId),

    maps:map(fun(_ProviderId, SupportingStorages) ->
        ProviderDirStatsLayout = maps:fold(fun(StorageId, _, LayoutAcc) ->
            LayoutAcc#{?SIZE_ON_STORAGE(StorageId) => [?MONTH_METRIC]}
        end, #{?TOTAL_SIZE => [?MONTH_METRIC]}, SupportingStorages),

        #time_series_get_slice_request{
            layout = ProviderDirStatsLayout,
            window_limit = 1
        }
    end, StoragesByProvider).


%% @private
-spec build_provider_dir_distribution(time_series_collection:slice()) ->
    provider_dir_distribution().
build_provider_dir_distribution(ProviderDirStats) ->
    #provider_dir_distribution{
        logical_size = get_dir_size(?TOTAL_SIZE, ProviderDirStats),
        physical_size_per_storage = lists:foldl(fun
            (TSName = ?SIZE_ON_STORAGE(StorageId), Acc) ->
                Acc#{StorageId => get_dir_size(TSName, ProviderDirStats)};

            (_, Acc) ->
                Acc
        end, #{}, maps:keys(ProviderDirStats))
    }.


%% @private
-spec get_dir_size(
    time_series_collection:time_series_name(),
    time_series_collection:slice()
) ->
    undefined | non_neg_integer().
get_dir_size(DirSizeTSName, ProviderDirStats) ->
    case maps:get(DirSizeTSName, ProviderDirStats) of
        #{?MONTH_METRIC := [{_Timestamp, Value}]} -> Value;
        _ -> undefined
    end.


%% @private
-spec get_symlink_distribution(file_ctx:ctx()) -> symlink_distribution().
get_symlink_distribution(_FileCtx) ->
    %% TODO what should be part of symlink distribution? logical_size = 0 always? list of storage ids supporting space?
    #symlink_distribution{}.


%% @private
-spec get_reg_distribution(file_ctx:ctx()) -> reg_distribution().
get_reg_distribution(FileCtx0) ->
    {FileSize, FileCtx1} = file_ctx:get_file_size(FileCtx0),
    {FileLocationDocs, FileCtx2} = file_ctx:get_file_location_docs(FileCtx1),

    %% @TODO VFS-9204 ultimately, location for each file should be created in each provider
    %% and the list of storages in the distribution should always be complete -
    %% for now, add placeholders with zero blocks for missing storages
    SpaceId = file_ctx:get_space_id_const(FileCtx2),
    {ok, StorageIdsByProvider} = space_logic:get_storages_by_provider(SpaceId),
    EmptyFileBlocksPerProvider = maps:map(fun(_ProviderId, ProviderStorages) ->
        maps:map(fun(_StorageId, _) -> [] end, ProviderStorages)
    end, StorageIdsByProvider),

    ActualFileBlocksPerProvider = lists:foldl(fun(
        FileLocationDoc = #document{value = #file_location{
            provider_id = ProviderId,
            storage_id = StorageId
        }},
        FileBlockPerProviderAcc
    ) ->
        FileBlocks = fslogic_location_cache:get_blocks(FileLocationDoc),

        maps:update_with(
            ProviderId,
            fun(FileBlocksPerStorage) -> FileBlocksPerStorage#{StorageId => FileBlocks} end,
            FileBlockPerProviderAcc
        )
    end, EmptyFileBlocksPerProvider, FileLocationDocs),

    #reg_distribution{
        logical_size = FileSize,
        blocks_per_provider = ActualFileBlocksPerProvider
    }.
