%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% transfer entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_gui_gs_translator).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    translate_value/2,
    translate_resource/2
]).

-define(PROVIDER_GRI_ID(__PROVIDER_ID), gri:serialize(#gri{
    type = op_provider,
    id = __PROVIDER_ID,
    aspect = instance,
    scope = protected
})).


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_value(gri:gri(), Value :: term()) -> gs_protocol:data().
translate_value(#gri{aspect = rerun}, TransferId) ->
    #{<<"transferId">> => TransferId};
translate_value(#gri{aspect = throughput_charts}, Charts) ->
    Charts.


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

    QueryParams = case QueryViewParams of
        undefined -> null;
        _ -> maps:from_list(QueryViewParams)
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

    fun(?USER(_UserId, SessionId)) ->
        {DataSourceType, DataSourceId, DataSourceName} = case ViewName of
            undefined ->
                FileGuid = file_id:pack_guid(FileUuid, SpaceId),
                FileType = case lfm:stat(SessionId, ?FILE_REF(FileGuid)) of
                    {ok, #file_attr{type = ?DIRECTORY_TYPE}} -> <<"dir">>;
                    {ok, _} -> <<"file">>;
                    {error, ?ENOENT} -> <<"deleted">>;
                    {error, _} -> <<"unknown">>
                end,
                {FileType, FileGuid, Path};
            _ ->
                case view_links:get_view_id(ViewName, SpaceId) of
                    {ok, IndexId} ->
                        {<<"view">>, IndexId, ViewName};
                    _ ->
                        {<<"view">>, null, ViewName}
                end
        end,

        #{
            <<"type">> => transfer:type(Transfer),
            <<"replicatingProvider">> => ReplicatingProvider,
            <<"evictingProvider">> => EvictingProvider,
            <<"isOngoing">> => IsOngoing,
            <<"dataSourceType">> => DataSourceType,
            <<"dataSourceId">> => DataSourceId,
            <<"dataSourceName">> => DataSourceName,
            <<"queryParams">> => QueryParams,
            <<"userId">> => UserId,
            <<"scheduleTime">> => ScheduleTime,
            <<"startTime">> => StartTime,
            <<"finishTime">> => case IsOngoing of
                true -> null;
                false -> FinishTime
            end
        }
    end;
translate_resource(#gri{aspect = progress, scope = private}, ProgressInfo) ->
    ProgressInfo;
translate_resource(#gri{aspect = {throughput_charts, _}, scope = private}, Charts) ->
    Charts.
