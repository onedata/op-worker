%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module handling translations between protobuf and internal protocol.
%%% @end
%%%-------------------------------------------------------------------
-module(clproto_provider_rpc_translator).
-author("Michal Stanisz").

-include("proto/oneprovider/provider_rpc_messages.hrl").
-include("modules/fslogic/data_distribution.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").

%% API
-export([
    from_protobuf/1, to_protobuf/1
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec from_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
from_protobuf(#'ProviderRpcCall'{
    file_guid = FileGuid,
    operation = {_, Request}
}) ->
    #provider_rpc_call{
        file_guid = FileGuid,
        request = from_protobuf(Request)
    };
from_protobuf(#'ProviderHistoricalDirSizeStatsBrowseRequest'{
    request = {_, Request}
}) ->
    #provider_historical_dir_size_stats_browse_request{
        request = from_protobuf(Request)
    };
from_protobuf(#'TimeSeriesLayoutGetRequest'{}) ->
    #time_series_layout_get_request{};
from_protobuf(#'TimeSeriesSliceGetRequest'{
    layout_as_json = EncodedLayout,
    start_timestamp = StartTimestamp,
    window_limit = WindowLimit,
    stop_timestamp = StopTimestamp
}) ->
    #time_series_slice_get_request{
        layout = json_utils:decode(EncodedLayout),
        start_timestamp = StartTimestamp,
        window_limit = WindowLimit,
        stop_timestamp = StopTimestamp
    };
from_protobuf(#'ProviderCurrentDirSizeStatsBrowseRequest'{
    stat_names = StatNames
}) ->
    #provider_current_dir_size_stats_browse_request{
        stat_names = StatNames
    };
from_protobuf(#'ProviderRegDistributionGetRequest'{}) ->
    #provider_reg_distribution_get_request{};
from_protobuf(#'ProviderRegStorageLocationsGetRequest'{}) ->
    #provider_reg_storage_locations_get_request{};
from_protobuf(#'ProviderQosStatusGetRequest'{qos_entry_id = QosEntryId}) ->
    #provider_qos_status_get_request{
        qos_entry_id = QosEntryId
    };

from_protobuf(#'ProviderRpcResponse'{
    status = ok,
    result = {_, Result}
}) ->
    #provider_rpc_response{
        status = ok,
        result = from_protobuf(Result)
    };
from_protobuf(#'ProviderRpcResponse'{
    status = error,
    result = {error_json, ErrorAsJson}
}) ->
    #provider_rpc_response{
        status = error,
        result = errors:from_json(json_utils:decode(ErrorAsJson))
};
from_protobuf(#'ProviderCurrentDirSizeStatsBrowseResult'{
    stats_as_json = StatsAsJson
}) ->
    #provider_current_dir_size_stats_browse_result{
        stats = json_utils:decode(StatsAsJson)
    };
from_protobuf(#'TimeSeriesLayoutGetResult'{
    layout_as_json = LayoutAsJson
}) ->
    #time_series_layout_get_result{
        layout = json_utils:decode(LayoutAsJson)
    };
from_protobuf(#'TimeSeriesSliceGetResult'{
    slice_as_json = EncodedSliceGetResult
}) ->
    ts_browse_result:from_json(json_utils:decode(EncodedSliceGetResult));
from_protobuf(#'ProviderRegDistributionGetResult'{
    logical_size = LogicalSize,
    distribution_per_storage = DistributionPerStorage
}) ->
    {BlocksPerStorage, LocationsPerStorage} = lists:foldl(
        fun(#'StorageRegDistributionGetResult'{
            storage_id = StorageId,
            blocks = Blocks,
            location = Location
        }, {BlocksAcc, LocationsAcc}) ->
            {
                BlocksAcc#{StorageId => lists:map(fun clproto_common_translator:from_protobuf/1, Blocks)},
                LocationsAcc#{StorageId => Location}
            }
        end,
    {#{}, #{}}, DistributionPerStorage),
    #provider_reg_distribution_get_result{
        logical_size = LogicalSize,
        blocks_per_storage = BlocksPerStorage,
        locations_per_storage = LocationsPerStorage
    };
from_protobuf(#'ProviderRegStorageLocationsResult'{
    locations = Locations
}) ->
    #provider_reg_storage_locations_result{
        locations_per_storage = maps_utils:generate_from_list(
            fun(#'StorageLocation'{storage_id = StorageId, location = Location}) ->
                {StorageId, Location}
            end, Locations)
    };
from_protobuf(#'ProviderQosStatusGetResult'{
    status = Status
}) ->
    #provider_qos_status_get_result{
        status = Status
    };

%% OTHER
from_protobuf(undefined) -> undefined.


-spec to_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
to_protobuf(#provider_rpc_call{
    file_guid = FileGuid,
    request = Request
}) ->
    {provider_rpc_call, #'ProviderRpcCall'{
        file_guid = FileGuid,
        operation = to_protobuf(Request)
    }};
to_protobuf(#provider_current_dir_size_stats_browse_request{
    stat_names = StatNames
}) ->
    {provider_current_dir_size_stats_browse_request, #'ProviderCurrentDirSizeStatsBrowseRequest'{
        stat_names = StatNames
    }};
to_protobuf(#provider_historical_dir_size_stats_browse_request{
    request = Request
}) ->
    {provider_historical_dir_size_stats_browse_request, #'ProviderHistoricalDirSizeStatsBrowseRequest'{
        request = to_protobuf(Request)
    }};
to_protobuf(#time_series_layout_get_request{}) ->
    {time_series_layout_get_request, #'TimeSeriesLayoutGetRequest'{}};
to_protobuf(#time_series_slice_get_request{
    layout = Layout,
    start_timestamp = StartTimestamp,
    window_limit = WindowLimit,
    stop_timestamp = StopTimestamp
}) ->
    {time_series_slice_get_request, #'TimeSeriesSliceGetRequest'{
        layout_as_json = json_utils:encode(Layout),
        start_timestamp = StartTimestamp,
        window_limit = WindowLimit,
        stop_timestamp = StopTimestamp
    }};
to_protobuf(#provider_reg_distribution_get_request{}) ->
    {provider_reg_distribution_get_request, #'ProviderRegDistributionGetRequest'{}};
to_protobuf(#provider_reg_storage_locations_get_request{}) ->
    {provider_reg_storage_locations_get_request, #'ProviderRegStorageLocationsGetRequest'{}};
to_protobuf(#provider_qos_status_get_request{
    qos_entry_id = QosEntryId
}) ->
    {provider_qos_status_get_request, #'ProviderQosStatusGetRequest'{
        qos_entry_id = QosEntryId
    }};

to_protobuf(#provider_rpc_response{
    status = ok,
    result = Result
}) ->
    {provider_rpc_response, #'ProviderRpcResponse'{
        status = ok,
        result = to_protobuf(Result)
    }};
to_protobuf(#provider_rpc_response{
    status = error,
    result = Error
}) ->
    {provider_rpc_response, #'ProviderRpcResponse'{
        status = error,
        result = {error_json, json_utils:encode(errors:to_json(Error))}
    }};
to_protobuf(#provider_current_dir_size_stats_browse_result{
    stats = Stats
}) ->
    {provider_current_dir_size_stats_browse_result, #'ProviderCurrentDirSizeStatsBrowseResult'{
        stats_as_json = json_utils:encode(Stats)
    }};
to_protobuf(#time_series_layout_get_result{
    layout = Layout
}) ->
    {time_series_layout_get_result, #'TimeSeriesLayoutGetResult'{
        layout_as_json = json_utils:encode(Layout)
    }};
to_protobuf(#time_series_slice_get_result{} = SliceGetResult) ->
    {time_series_slice_get_result, #'TimeSeriesSliceGetResult'{
        slice_as_json = json_utils:encode(ts_browse_result:to_json(SliceGetResult))
    }};
to_protobuf(#provider_reg_distribution_get_result{
    logical_size = LogicalSize,
    blocks_per_storage = BlocksPerStorage,
    locations_per_storage = LocationsPerStorage
}) ->
    {provider_reg_distribution_get_result, #'ProviderRegDistributionGetResult'{
        logical_size = LogicalSize,
        distribution_per_storage = maps:fold(fun(StorageId, StorageBlocks, Acc) ->
            [#'StorageRegDistributionGetResult'{
                storage_id = StorageId,
                blocks = lists:map(fun(Block) -> clproto_common_translator:to_protobuf(Block) end, StorageBlocks),
                location = maps:get(StorageId, LocationsPerStorage)
            } | Acc]
        end, [], BlocksPerStorage)
    }};
to_protobuf(#provider_reg_storage_locations_result{
    locations_per_storage = LocationsPerStorageMap
}) ->
    {provider_reg_storage_locations_result, #'ProviderRegStorageLocationsResult'{
        locations = maps:fold(fun(StorageId, Location, Acc) ->
            [#'StorageLocation'{storage_id = StorageId, location = Location} | Acc]
        end, [], LocationsPerStorageMap)
    }};
to_protobuf(#provider_qos_status_get_result{
    status = Status
}) ->
    {provider_qos_status_get_result, #'ProviderQosStatusGetResult'{
        status = Status
    }};


%% OTHER
to_protobuf(undefined) -> undefined.
