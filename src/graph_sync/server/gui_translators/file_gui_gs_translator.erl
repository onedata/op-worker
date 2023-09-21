%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% file entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(file_gui_gs_translator).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").
-include("modules/fslogic/data_distribution.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").

%% API
-export([
    translate_value/2,
    translate_resource/2,

    translate_dataset_summary/1
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_value(gri:gri(), Value :: term()) -> gs_protocol:data().
translate_value(#gri{aspect = children}, {ChildrenAttrsJson, IsLast, _PaginationToken}) ->
    #{
        <<"children">> => lists:map(fun map_file_attr_fields_for_gui/1, ChildrenAttrsJson),
        <<"isLast">> => IsLast
    };

translate_value(#gri{aspect = As}, Metadata) when
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata
->
    #{<<"metadata">> => Metadata};

translate_value(#gri{aspect = transfers}, TransfersForFile) ->
    TransfersForFile;

translate_value(#gri{aspect = download_url}, URL) ->
    #{<<"fileUrl">> => URL};

translate_value(#gri{aspect = api_samples, scope = public}, ApiSamples) ->
    ApiSamples;
translate_value(#gri{aspect = api_samples, scope = private}, ApiSamples) ->
    ApiSamples;

translate_value(#gri{aspect = dir_size_stats_collection_schema}, TimeSeriesCollectionSchema) ->
    jsonable_record:to_json(TimeSeriesCollectionSchema);

translate_value(#gri{aspect = {dir_size_stats_collection, _}}, TSBrowseResult) ->
    ts_browse_result:to_json(TSBrowseResult).


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{aspect = instance}, FileAttrsJson) ->
    map_file_attr_fields_for_gui(FileAttrsJson);

translate_resource(#gri{aspect = distribution, scope = private, id = Guid}, Distribution) ->
    data_distribution_translator:gather_result_to_json(gs, Distribution, Guid);

translate_resource(#gri{aspect = storage_locations, scope = private}, StorageLocations) ->
    data_distribution_translator:storage_locations_to_json(StorageLocations);

translate_resource(#gri{aspect = acl, scope = private}, Acl) ->
    try
        #{
            <<"list">> => acl:to_json(Acl, gui)
        }
    catch throw:{error, Errno} ->
        throw(?ERROR_POSIX(Errno))
    end;

translate_resource(#gri{aspect = hardlinks, scope = private}, References) ->
    #{
        <<"hardlinks">> => lists:map(fun(FileGuid) ->
            gri:serialize(#gri{
                type = op_file, id = FileGuid,
                aspect = instance, scope = private
            })
        end, References)
    };

translate_resource(#gri{aspect = {hardlinks, _}, scope = private}, Result) ->
    Result;

translate_resource(#gri{aspect = symlink_target}, FileAttrsJson) ->
    map_file_attr_fields_for_gui(FileAttrsJson);

translate_resource(#gri{aspect = shares, scope = private}, ShareIds) ->
    #{
        <<"list">> => lists:map(fun(ShareId) ->
            gri:serialize(#gri{
                type = op_share,
                id = ShareId,
                aspect = instance,
                scope = private
            })
        end, ShareIds)
    };

translate_resource(#gri{aspect = qos_summary, scope = private}, QosSummaryResponse) ->
    maps:without([<<"status">>], QosSummaryResponse);

translate_resource(#gri{aspect = dataset_summary, scope = private}, DatasetSummary) ->
    translate_dataset_summary(DatasetSummary);

translate_resource(#gri{aspect = archive_recall_details, scope = private}, ArchiveRecallDetails) ->
    translate_archive_recall_details(ArchiveRecallDetails);

translate_resource(#gri{aspect = archive_recall_progress, scope = private}, ArchiveRecallProgress) ->
    ArchiveRecallProgress;

translate_resource(#gri{aspect = archive_recall_log, scope = private}, ArchiveRecallLog) ->
    ArchiveRecallLog.


-spec translate_dataset_summary(dataset_api:file_eff_summary()) -> map().
translate_dataset_summary(#file_eff_dataset_summary{
    direct_dataset = DatasetId,
    eff_ancestor_datasets = EffAncestorDatasets,
    eff_protection_flags = EffProtectionFlags
}) ->
    #{
        <<"directDataset">> => case DatasetId of
            undefined ->
                null;
            _ ->
                gri:serialize(#gri{
                    type = op_dataset, id = DatasetId,
                    aspect = instance, scope = private
                })
        end,
        <<"effAncestorDatasets">> => lists:map(fun(AncestorId) ->
            gri:serialize(#gri{
                type = op_dataset, id = AncestorId, aspect = instance, scope = private
            })
        end, EffAncestorDatasets),
        <<"effProtectionFlags">> => file_meta:protection_flags_to_json(EffProtectionFlags)
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec translate_archive_recall_details(archive_recall:record()) -> map().
translate_archive_recall_details(#archive_recall_details{
    recalling_provider_id = RecallingProviderId,
    archive_id = ArchiveId,
    dataset_id = DatasetId,
    start_timestamp = StartTimestamp,
    finish_timestamp = FinishTimestamp,
    cancel_timestamp = CancelTimestamp,
    total_file_count = TargetFileCount,
    total_byte_size = TargetByteSize,
    last_error = LastError
}) ->
    #{
        <<"recallingProvider">> => gri:serialize(#gri{
            type = op_provider, id = RecallingProviderId, aspect = instance, scope = protected}),
        <<"archive">> => gri:serialize(#gri{
            type = op_archive, id = ArchiveId, aspect = instance, scope = private}),
        <<"dataset">> => gri:serialize(#gri{
            type = op_dataset, id = DatasetId, aspect = instance, scope = private}),
        <<"startTime">> => utils:undefined_to_null(StartTimestamp),
        <<"finishTime">> => utils:undefined_to_null(FinishTimestamp),
        <<"cancelTime">> => utils:undefined_to_null(CancelTimestamp),
        <<"totalFileCount">> => TargetFileCount,
        <<"totalByteSize">> => TargetByteSize,
        <<"lastError">> => utils:undefined_to_null(LastError)
    }.


%% @private
-spec map_file_attr_fields_for_gui(json_utils:json_map()) -> json_utils:json_map().
map_file_attr_fields_for_gui(#{<<"fileId">> := ObjectId} = FileAttrJson) ->
    map_file_attr_parent_for_gui(FileAttrJson#{<<"fileId">> => ensure_guid(ObjectId)});
%% @TODO VFS-11377 deprecated, remove when possible
map_file_attr_fields_for_gui(#{<<"file_id">> := ObjectId} = FileAttrJson) ->
    map_file_attr_parent_for_gui(FileAttrJson#{<<"file_id">> => ensure_guid(ObjectId)});
map_file_attr_fields_for_gui(FileAttrJson) ->
    map_file_attr_parent_for_gui(FileAttrJson).


%% @private
-spec map_file_attr_parent_for_gui(json_utils:json_map()) -> json_utils:json_map().
map_file_attr_parent_for_gui(#{<<"parentId">> := ParentObjectId} = FileAttrJson) ->
    FileAttrJson#{<<"parentId">> => ensure_guid(ParentObjectId)};
%% @TODO VFS-11377 deprecated, remove when possible
map_file_attr_parent_for_gui(#{<<"parent_id">> := ParentObjectId} = FileAttrJson) ->
    FileAttrJson#{<<"parent_id">> => ensure_guid(ParentObjectId)};
map_file_attr_parent_for_gui(FileAttrJson) ->
    FileAttrJson.


%% @private
-spec ensure_guid(null | file_id:objectid()) -> null | file_id:file_guid().
ensure_guid(null) -> null;
ensure_guid(ObjectId) ->
    {ok, Guid} = file_id:objectid_to_guid(ObjectId),
    Guid.
