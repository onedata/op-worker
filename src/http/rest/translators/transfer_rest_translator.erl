%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% transfer entities into REST responses.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_rest_translator).
-author("Bartosz Walkowicz").

-include("http/rest.hrl").
-include("middleware/middleware.hrl").

-export([create_response/4, get_response/2]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback create_response/4.
%% @end
%%--------------------------------------------------------------------
-spec create_response(gri:gri(), middleware:auth_hint(),
    middleware:data_format(), Result :: term() | {gri:gri(), term()} |
    {gri:gri(), middleware:auth_hint(), term()}) -> #rest_resp{}.
create_response(#gri{aspect = instance}, _, resource, {#gri{id = TransferId}, _}) ->
    PathTokens = [<<"transfers">>, TransferId],
    ?CREATED_REPLY(PathTokens, #{<<"transferId">> => TransferId});

create_response(#gri{aspect = rerun}, _, value, TransferId) ->
    PathTokens = [<<"transfers">>, TransferId],
    ?CREATED_REPLY(PathTokens, #{<<"transferId">> => TransferId}).


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback get_response/2.
%% @end
%%--------------------------------------------------------------------
-spec get_response(gri:gri(), Resource :: term()) -> #rest_resp{}.
get_response(#gri{aspect = instance, id = TransferId}, #transfer{
    file_uuid = FileUuid,
    space_id = SpaceId,
    index_name = ViewName,
    query_view_params = QueryViewParams,
    user_id = UserId,
    rerun_id = RerunId,
    path = Path,
    replication_status = ReplicationStatus,
    eviction_status = EvictionStatus,
    evicting_provider = EvictingProviderId,
    replicating_provider = ReplicatingProviderId,
    callback = Callback,
    files_to_process = FilesToProcess,
    files_processed = FilesProcessed,
    failed_files = FailedFiles,
    files_replicated = FilesReplicated,
    bytes_replicated = BytesReplicated,
    files_evicted = FilesEvicted,
    schedule_time = ScheduleTime,
    start_time = StartTime,
    finish_time = FinishTime,
    last_update = LastUpdate,
    min_hist = MinHist,
    hr_hist = HrHist,
    dy_hist = DyHist,
    mth_hist = MthHist
} = Transfer) ->
    {EffJobTransferId, EffJobTransfer} = case RerunId of
        undefined ->
            {TransferId, Transfer};
        _ ->
            {ok, #document{
                key = EffTransferId,
                value = EffTransfer
            }} = transfer:get_effective(RerunId),

            {EffTransferId, EffTransfer}
    end,

    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    DataSourceType = transfer:data_source_type(Transfer),
    DataSourceTypeDependentInfo = case DataSourceType of
        file ->
            #{
                <<"fileId">> => FileObjectId,
                <<"filePath">> => Path
            };
        view ->
            #{
                <<"viewName">> => ViewName,
                <<"queryViewParams">> => maps:from_list(QueryViewParams)
            }
    end,

    ?OK_REPLY(DataSourceTypeDependentInfo#{
        <<"type">> => transfer:type(Transfer),
        <<"dataSourceType">> => DataSourceType,

        <<"userId">> => UserId,
        <<"rerunId">> => utils:undefined_to_null(RerunId),
        <<"spaceId">> => SpaceId,
        <<"callback">> => utils:undefined_to_null(Callback),

        <<"replicatingProviderId">> => utils:undefined_to_null(ReplicatingProviderId),
        <<"evictingProviderId">> => utils:undefined_to_null(EvictingProviderId),

        <<"transferStatus">> => transfer:status(Transfer),
        <<"replicationStatus">> => ReplicationStatus,
        <<"evictionStatus">> => EvictionStatus,

        <<"effectiveJobStatus">> => transfer:status(EffJobTransfer),
        <<"effectiveJobTransferId">> => EffJobTransferId,

        <<"filesToProcess">> => FilesToProcess,
        <<"filesProcessed">> => FilesProcessed,
        <<"filesReplicated">> => FilesReplicated,
        <<"bytesReplicated">> => BytesReplicated,
        <<"filesEvicted">> => FilesEvicted,
        <<"filesFailed">> => FailedFiles,

        <<"scheduleTime">> => ScheduleTime,
        <<"startTime">> => StartTime,
        <<"finishTime">> => FinishTime,

        % It is possible that there is no last update, if 0 bytes were
        % transferred, in this case take the start time.
        <<"lastUpdate">> => lists:max([StartTime | maps:values(LastUpdate)]),
        <<"minHist">> => MinHist,
        <<"hrHist">> => HrHist,
        <<"dyHist">> => DyHist,
        <<"mthHist">> => MthHist
    }).
