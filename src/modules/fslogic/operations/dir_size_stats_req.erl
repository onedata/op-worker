%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on dir size stats.
%%% @TODO VFS-9435 - come up with a consistent way of handling disabled stats collecting on some providers
%%% @end
%%%--------------------------------------------------------------------
-module(dir_size_stats_req).
-author("Michal Stanisz").

-include("modules/fslogic/data_access_control.hrl").
-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include("proto/oneprovider/provider_rpc_messages.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").

%% API
-export([gather_historical/3]).


%%%===================================================================
%%% API
%%%===================================================================

-spec gather_historical(user_ctx:ctx(), file_ctx:ctx(), ts_browse_request:record()) ->
    ts_browse_result:record().
gather_historical(UserCtx, FileCtx0, BrowseRequest) ->
    FileCtx1 = file_ctx:assert_file_exists(FileCtx0),
    FileCtx2 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx1,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?read_metadata_mask)]
    ),
    
    gather_insecure(FileCtx2, BrowseRequest).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec gather_insecure(file_ctx:ctx(), ts_browse_request:record()) ->
    ts_browse_result:record().
gather_insecure(FileCtx, BrowseRequest) ->
    Guid = file_ctx:get_logical_guid_const(FileCtx),
    ProviderRequests = split_ts_browse_request_between_providers(
        file_id:guid_to_space_id(Guid), BrowseRequest),
    ResultsPerProvider = provider_rpc:gather(Guid, maps:map(fun(_ProviderId, Req) ->
        #provider_historical_dir_size_stats_browse_request{request = Req}
    end, ProviderRequests)),
    
    maps:fold(fun
        (_ProviderId, {ok, Result}, Acc) ->
            merge_ts_browse_results(Acc, Result);
        (_ProviderId, {error, _}, Acc) ->
            %% @TODO VFS-9435 - Add information about errors
            Acc
    end, gen_empty_ts_browse_result(BrowseRequest), ResultsPerProvider).


%% @private
-spec split_ts_browse_request_between_providers(od_space:id(), ts_browse_request:record()) ->
    #{oneprovider:id() => ts_browse_request:record()}.
split_ts_browse_request_between_providers(SpaceId, #time_series_layout_get_request{} = Req) ->
    {ok, Providers} = space_logic:get_provider_ids(SpaceId),
    maps_utils:generate_from_list(fun(P) -> {P, Req} end, Providers);
split_ts_browse_request_between_providers(SpaceId, #time_series_slice_get_request{layout = Layout} = Req) ->
    maps:fold(fun(TimeSeriesName, Metrics, Acc) ->
        case choose_provider_storing_time_series_data(SpaceId, TimeSeriesName) of
            unknown ->
                Acc;
            ProviderId ->
                #time_series_slice_get_request{layout = ProviderLayout} =
                    maps:get(ProviderId, Acc, #time_series_slice_get_request{layout = #{}}),
                Acc#{ProviderId => Req#time_series_slice_get_request{
                    layout = ProviderLayout#{TimeSeriesName => Metrics}
                }}
        end
    end, #{}, Layout).


%% @private
-spec choose_provider_storing_time_series_data(od_space:id(), dir_stats_collection:stat_name()) ->
    oneprovider:id() | unknown.
choose_provider_storing_time_series_data(SpaceId, ?SIZE_ON_STORAGE(StorageId)) ->
    case storage_logic:get_provider(StorageId, SpaceId) of
        {ok, ProviderId} -> ProviderId;
        {error, _} -> unknown
    end;
choose_provider_storing_time_series_data(_SpaceId, _TimeSeriesName) ->
    oneprovider:get_id().


%% @private
-spec merge_ts_browse_results(ts_browse_result:record(), ts_browse_result:record()) -> ts_browse_result:record().
merge_ts_browse_results(#time_series_layout_get_result{layout = L1}, #time_series_layout_get_result{layout = L2}) ->
    #time_series_layout_get_result{layout = maps:merge(L1, L2)};
merge_ts_browse_results(#time_series_slice_get_result{slice = S1}, #time_series_slice_get_result{slice = S2}) ->
    #time_series_slice_get_result{slice = maps:merge(S1, S2)}.


%% @private
-spec gen_empty_ts_browse_result(ts_browse_request:record()) -> ts_browse_result:record().
gen_empty_ts_browse_result(#time_series_layout_get_request{}) -> #time_series_layout_get_result{layout = #{}};
gen_empty_ts_browse_result(#time_series_slice_get_request{}) -> #time_series_slice_get_result{slice = #{}}.
