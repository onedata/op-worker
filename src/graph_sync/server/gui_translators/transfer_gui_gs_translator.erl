%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of op logic results concerning
%%% transfer entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_gui_gs_translator).
-author("Bartosz Walkowicz").

-include("op_logic.hrl").

%% API
-export([translate_resource/2]).

-define(PROVIDER_GRI_ID(__PROVIDER_ID), gri:serialize(#gri{
    type = op_provider,
    id = __PROVIDER_ID,
    aspect = instance,
    scope = protected
})).


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{aspect = instance, scope = private}, #transfer{
    replicating_provider = ReplicatingProviderId,
    evicting_provider = EvictingProviderId,
    file_uuid = FileUuid,
    path = Path,
    user_id = UserId,
    space_id = SpaceId,
    schedule_time = ScheduleTime,
    start_time = StartTime,
    finish_time = FinishTime,
    index_name = ViewName,
    query_view_params = QueryViewParams
} = Transfer) ->
    {DataSource, DataSourceName} = case ViewName of
        undefined ->
            {gri:serialize(#gri{
                type = op_file,
                id = file_id:pack_guid(FileUuid, SpaceId),
                aspect = instance,
                scope = private
            }), Path};
        _ ->
            case view_links:get_view_id(ViewName, SpaceId) of
                {ok, ViewId} ->
                    {gri:serialize(#gri{
                        type = op_view,
                        id = ViewId,
                        aspect = instance,
                        scope = private
                    }), ViewName};
                _ ->
                    {null, ViewName}
            end
    end,
    QueryParams = case QueryViewParams of
        undefined -> null;
        _ -> {QueryViewParams}
    end,
    IsOngoing = transfer:is_ongoing(Transfer),

    EvictingProvider = case EvictingProviderId of
        undefined -> null;
        _ -> ?PROVIDER_GRI_ID(EvictingProviderId)
    end,
    ReplicatingProvider = case ReplicatingProviderId of
        undefined -> null;
        _ -> ?PROVIDER_GRI_ID(ReplicatingProviderId)
    end,

    #{
        <<"replicatingProvider">> => ReplicatingProvider,
        <<"evictingProvider">> => EvictingProvider,
        <<"isOngoing">> => IsOngoing,
        <<"dataSource">> => DataSource,
        <<"dataSourceName">> => DataSourceName,
        <<"queryParams">> => QueryParams,
        <<"user">> => gri:serialize(#gri{
            type = op_user,
            id = UserId,
            aspect = instance,
            scope = shared
        }),
        <<"startTime">> => StartTime,
        <<"scheduleTime">> => ScheduleTime,
        <<"finishTime">> => case IsOngoing of
            true -> null;
            false -> FinishTime
        end
    };
translate_resource(#gri{aspect = progress, scope = private}, ProgressInfo) ->
    ProgressInfo;
translate_resource(#gri{aspect = {throughput_charts, _}, scope = private}, Charts) ->
    Charts.
