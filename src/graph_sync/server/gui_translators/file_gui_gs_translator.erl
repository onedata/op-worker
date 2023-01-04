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
-include("modules/fslogic/file_details.hrl").
-include("modules/fslogic/file_distribution.hrl").
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
translate_value(#gri{aspect = children_details, scope = Scope}, {ChildrenDetails, IsLast}) ->
    #{
        <<"children">> => lists:map(fun(ChildDetails) ->
            translate_file_details(ChildDetails, Scope)
        end, ChildrenDetails),
        <<"isLast">> => IsLast
    };

translate_value(#gri{aspect = attrs}, Attrs) ->
    #{<<"attributes">> => Attrs};

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

translate_value(#gri{aspect = dir_size_stats_collection}, TSBrowseResult) ->
    ts_browse_result:to_json(TSBrowseResult).


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{aspect = instance, scope = Scope}, FileDetails) ->
    translate_file_details(FileDetails, Scope);

translate_resource(#gri{aspect = distribution, scope = private, id = Guid}, Distribution) ->
    file_distribution_translator:gather_result_to_json(gs, Distribution, Guid);

translate_resource(#gri{aspect = storage_locations, scope = private}, StorageLocations) ->
    file_distribution_translator:storage_locations_to_json(StorageLocations);

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

translate_resource(#gri{aspect = symlink_target, scope = Scope}, FileDetails) ->
    translate_file_details(FileDetails, Scope);

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
-spec translate_file_details(#file_details{}, gri:scope()) -> map().
translate_file_details(#file_details{
    has_metadata = HasMetadata,
    eff_qos_membership = EffQosMembership,
    active_permissions_type = ActivePermissionsType,
    eff_dataset_membership = EffDatasetMembership,
    eff_protection_flags = EffFileProtectionFlags,
    eff_dataset_protection_flags = EffDatasetProtectionFlags,
    recall_root_id = RecallRootId,
    symlink_value = SymlinkValue,
    file_attr = #file_attr{
        guid = FileGuid,
        name = FileName,
        mode = Mode,
        parent_guid = ParentGuid,
        mtime = MTime,
        type = TypeAttr,
        size = SizeAttr,
        shares = Shares,
        provider_id = ProviderId,
        owner_id = OwnerId,
        nlink = NLink,
        index = ListingIndex
    },
    conflicting_name = ConflictingName
}, Scope) ->
    PosixPerms = list_to_binary(string:right(integer_to_list(Mode, 8), 3, $0)),
    {Type, Size} = case TypeAttr of
        ?DIRECTORY_TYPE -> {<<"DIR">>, utils:undefined_to_null(SizeAttr)};
        ?REGULAR_FILE_TYPE -> {<<"REG">>, SizeAttr};
        ?SYMLINK_TYPE -> {<<"SYMLNK">>, SizeAttr}
    end,
    IsRootDir = case file_id:guid_to_share_id(FileGuid) of
        undefined -> fslogic_file_id:is_space_dir_guid(FileGuid);
        ShareId -> lists:member(ShareId, Shares)
    end,
    ParentId = case IsRootDir of
        true -> null;
        false -> ParentGuid
    end,
    BasicPublicFields = #{
        <<"hasMetadata">> => HasMetadata,
        <<"guid">> => FileGuid,
        <<"name">> => FileName,
        <<"index">> => file_listing:encode_index(ListingIndex),
        <<"posixPermissions">> => PosixPerms,
        <<"parentId">> => ParentId,
        <<"mtime">> => MTime,
        <<"type">> => Type,
        <<"size">> => Size,
        <<"shares">> => Shares,
        <<"activePermissionsType">> => ActivePermissionsType
    },
    PublicFields = case TypeAttr of
        ?SYMLINK_TYPE ->
            BasicPublicFields#{<<"targetPath">> => SymlinkValue};
        _ ->
            BasicPublicFields
    end,
    PublicFields2 = maps_utils:put_if_defined(PublicFields, <<"archiveId">>, 
        archivisation_tree:uuid_to_archive_id(file_id:guid_to_uuid(FileGuid))),
    PublicFields3 = maps_utils:put_if_defined(PublicFields2, <<"conflictingName">>, ConflictingName),
    case {Scope, EffQosMembership} of
        {public, _} ->
            PublicFields3;
        {private, undefined} -> % all or none effective fields are undefined
            PublicFields3#{
                <<"hardlinksCount">> => utils:undefined_to_null(NLink),
                <<"effProtectionFlags">> => [],
                <<"providerId">> => ProviderId,
                <<"ownerId">> => OwnerId
            };
        {private, _} ->
            PublicFields3#{
                <<"hardlinksCount">> => utils:undefined_to_null(NLink),
                <<"effProtectionFlags">> => file_meta:protection_flags_to_json(EffFileProtectionFlags),
                <<"effDatasetProtectionFlags">> => file_meta:protection_flags_to_json(EffDatasetProtectionFlags),
                <<"providerId">> => ProviderId,
                <<"ownerId">> => OwnerId,
                <<"effQosMembership">> => translate_membership(EffQosMembership),
                <<"effDatasetMembership">> => translate_membership(EffDatasetMembership),
                <<"recallRootId">> => utils:undefined_to_null(RecallRootId)
            }
    end.


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
-spec translate_membership(file_qos:membership() | dataset:membership()) -> binary().
translate_membership(?NONE_MEMBERSHIP) -> <<"none">>;
translate_membership(?DIRECT_MEMBERSHIP) -> <<"direct">>;
translate_membership(?ANCESTOR_MEMBERSHIP) -> <<"ancestor">>;
translate_membership(?DIRECT_AND_ANCESTOR_MEMBERSHIP) -> <<"directAndAncestor">>.
