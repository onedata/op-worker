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


-type dir_distribution() :: #dir_distribution{}.
-type reg_distribution() :: #reg_distribution{}.

-type get_request() :: #file_distribution_get_request{}.
-type get_result() :: #file_distribution_get_result{}.

-export_type([
    dir_distribution/0, reg_distribution/0,
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

    {ok, #file_distribution_get_result{distribution = case file_ctx:get_type(FileCtx2) of
        {?DIRECTORY_TYPE, FileCtx3} ->
            get_dir_distribution(UserCtx, FileCtx3);
        {?SYMLINK_TYPE, FileCtx3} ->
            %% TODO what about symlinks ?
            get_dir_distribution(UserCtx, FileCtx3);
        {_, FileCtx3} ->
            get_reg_distribution(FileCtx3)
    end}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_dir_distribution(user_ctx:ctx(), file_ctx:ctx()) -> dir_distribution().
get_dir_distribution(UserCtx, FileCtx0) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    FileRef = ?FILE_REF(file_ctx:get_logical_guid_const(FileCtx0)),

    maps:fold(fun(ProviderId, DirStatsGetReq, DirDistributionAcc) ->
        case lfm:browse_dir_stats(SessionId, FileRef, ProviderId, DirStatsGetReq) of
            {ok, #time_series_slice_result{slice = ProviderStats}} ->
                maps:fold(fun update_dir_distribution/3, DirDistributionAcc, ProviderStats);
            {error, _} ->
                % TODO VFS-9435 add information about errors
                DirDistributionAcc
        end
    end, #dir_distribution{}, build_get_dir_stats_requests(FileCtx0)).


%% @private
-spec build_get_dir_stats_requests(file_ctx:ctx()) ->
    #{oneprovider:id() => ts_browse_request:record()}.
build_get_dir_stats_requests(FileCtx) ->
    ThisProviderId = oneprovider:get_id(),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {ok, StoragesByProvider} = space_logic:get_storages_by_provider(SpaceId),

    maps:fold(fun(ProviderId, SupportingStorages, Acc) ->
        ProviderDirStatsLayout = maps:fold(fun(StorageId, _, LayoutAcc) ->
            LayoutAcc#{?SIZE_ON_STORAGE(StorageId) => [?MONTH_METRIC]}
        end, #{}, SupportingStorages),

        Acc#{ProviderId => #time_series_get_slice_request{
            layout = case ProviderId of
                ThisProviderId -> ProviderDirStatsLayout#{?TOTAL_SIZE => [?MONTH_METRIC]};
                _ -> ProviderDirStatsLayout
            end,
            window_limit = 1
        }}
    end, #{}, StoragesByProvider).


%% @private
-spec update_dir_distribution(
    time_series_collection:time_series_name(),
    [non_neg_integer()],  % TODO ŁO/MW Is it possible to receive other format?
    dir_distribution()
) ->
    dir_distribution().
update_dir_distribution(?TOTAL_SIZE, [TotalSize], DirDistribution = #dir_distribution{
    logical_size = LogicalSize
}) ->
    DirDistribution#dir_distribution{logical_size = max(TotalSize, LogicalSize)};

update_dir_distribution(?SIZE_ON_STORAGE(StorageId), [PhysicalSize], DirDistribution = #dir_distribution{
    logical_size = LogicalSize,
    physical_size_per_storage = PhysicalSizePerStorage
}) ->
    DirDistribution#dir_distribution{
        logical_size = max(PhysicalSize, LogicalSize),
        physical_size_per_storage = PhysicalSizePerStorage#{StorageId => PhysicalSize}
    }.


%% @private
-spec get_reg_distribution(file_ctx:ctx()) -> reg_distribution().
get_reg_distribution(FileCtx0) ->
    {FileSize, FileCtx1} = file_ctx:get_file_size(FileCtx0),
    {FileLocationDocs, _FileCtx2} = file_ctx:get_file_location_docs(FileCtx1),

    FileBlocksPerStorage = lists:foldl(fun(FileLocationDoc, Acc) ->
        StorageId = FileLocationDoc#document.value#file_location.storage_id,
        Acc#{StorageId => fslogic_location_cache:get_blocks(FileLocationDoc)}
    end, #{}, FileLocationDocs),

    #reg_distribution{
        logical_size = FileSize,
        blocks_per_storage = FileBlocksPerStorage
    }.
