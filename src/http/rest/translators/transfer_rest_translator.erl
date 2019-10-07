%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module handles translation of op logic results concerning
%%% transfer entities into REST responses.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_rest_translator).
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include("http/rest.hrl").

-export([create_response/4, get_response/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback create_response/4.
%% @end
%%--------------------------------------------------------------------
-spec create_response(op_logic:gri(), op_logic:auth_hint(),
    op_logic:data_format(), Result :: term() | {op_logic:gri(), term()} |
    {op_logic:gri(), op_logic:auth_hint(), term()}) -> #rest_resp{}.
create_response(#gri{aspect = rerun}, _, value, TransferId) ->
    PathTokens = [<<"transfers">>, TransferId],
    ?CREATED_REPLY(PathTokens, #{<<"transferId">> => TransferId}).


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback get_response/2.
%% @end
%%--------------------------------------------------------------------
-spec get_response(op_logic:gri(), Resource :: term()) -> #rest_resp{}.
get_response(_, #transfer{
    file_uuid = FileUuid,
    space_id = SpaceId,
    user_id = UserId,
    rerun_id = RerunId,
    path = Path,
    replication_status = ReplicationStatus,
    eviction_status = EvictionStatus,
    evicting_provider = EvictingProvider,
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
}) ->
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    NullableCallback = utils:ensure_defined(Callback, undefined, null),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    ReplicationStatusBin = atom_to_binary(ReplicationStatus, utf8),
    ReplicatingProvider = utils:ensure_defined(
        ReplicatingProviderId, undefined, null
    ),

    ?OK_REPLY(#{
        <<"fileId">> => FileObjectId,
        <<"userId">> => UserId,
        <<"rerunId">> => utils:ensure_defined(RerunId, undefined, null),
        <<"path">> => Path,
        <<"transferStatus">> => ReplicationStatusBin,
        <<"replicationStatus">> => ReplicationStatusBin,
        <<"invalidationStatus">> => atom_to_binary(EvictionStatus, utf8),
        <<"replicaEvictionStatus">> => atom_to_binary(EvictionStatus, utf8),
        <<"targetProviderId">> => ReplicatingProvider,
        <<"replicatingProviderId">> => ReplicatingProvider,
        <<"evictingProviderId">> => utils:ensure_defined(
            EvictingProvider, undefined, null
        ),
        <<"callback">> => NullableCallback,
        <<"filesToProcess">> => FilesToProcess,
        <<"filesProcessed">> => FilesProcessed,
        <<"filesTransferred">> => FilesReplicated,
        <<"filesReplicated">> => FilesReplicated,
        <<"failedFiles">> => FailedFiles,
        <<"filesInvalidated">> => FilesEvicted,
        <<"fileReplicasEvicted">> => FilesEvicted,
        <<"bytesTransferred">> => BytesReplicated,
        <<"bytesReplicated">> => BytesReplicated,
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
