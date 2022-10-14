%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia, Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for performing dataset archivisation during archivisation traverse
%%% (see `archivisation_traverse` for more details).
%%% @end
%%%-------------------------------------------------------------------
-module(archivisation_traverse_logic).
-author("Michal Stanisz").
-author("Jakub Kudzia").

-include("tree_traverse.hrl").
-include("modules/dataset/archive.hrl").
-include("modules/logical_file_manager/lfm.hrl").


%% API
-export([
    mark_building_if_first_job/1, 
    mark_finished_and_propagate_up/8, 
    handle_file/4,
    initialize_archive_dir/3,
    save_dir_checksum_metadata/3,
    info_to_archive_docs/1
]).

-type id() :: archivisation_traverse:id().
-type info() :: archivisation_traverse:info().
-type docs_map() :: archivisation_traverse:docs_map().
-type ctx() :: archivisation_traverse_ctx:ctx().

-define(COPY_OPTIONS(ArchiveId), #{
    recursive => false,
    on_write_callback => fun(_BytesCopied) ->
        %% @TODO VFS-8937 - Update archive bytes copied continuously
        case archive:get(ArchiveId) of
            {ok, #document{value = #archive{state = ?ARCHIVE_CANCELLING}}} -> abort;
            _ -> continue
        end
    end
}).


%%%===================================================================
%%% API functions
%%%===================================================================

-spec mark_building_if_first_job(tree_traverse:master_job()) -> ok.
mark_building_if_first_job(Job = #tree_traverse{traverse_info = #{
    aip_ctx := AipArchiveCtx,
    dip_ctx := DipArchiveCtx
}}) ->
    case is_first_job(Job) of
        true ->
            ok = archive:mark_building(archivisation_traverse_ctx:get_archive_doc(AipArchiveCtx)),
            case archivisation_traverse_ctx:get_archive_doc(DipArchiveCtx) of
                undefined -> ok;
                DipArchiveDoc ->
                    ok = archive:mark_building(DipArchiveDoc)
            end;
        false ->
            ok
    end.


-spec mark_finished_and_propagate_up(file_ctx:ctx(), user_ctx:ctx(), info(), id(), file_meta:uuid(), 
    time:millis(), file_meta:path(), {failed, Reason :: any()} | completed) -> ok.
mark_finished_and_propagate_up(
    CurrentFileCtx, UserCtx, TraverseInfo, TaskId, SourceMasterUuid, StartTimestamp, FilePath, Status
) ->
    report_to_audit_log(CurrentFileCtx, TraverseInfo, StartTimestamp, FilePath, Status, UserCtx),
    NextTraverseInfo = mark_finished_if_current_archive_is_rooted_in_current_file(
        CurrentFileCtx, UserCtx, TraverseInfo),
    propagate_finished_up(CurrentFileCtx, UserCtx, NextTraverseInfo, TaskId, SourceMasterUuid, FilePath).


-spec handle_file(file_ctx:ctx(), file_meta:path(), user_ctx:ctx(),
    info()) -> info().
handle_file(FileCtx, ResolvedFilePath, UserCtx, TraverseInfo = #{
    aip_ctx := AipArchiveCtx,
    dip_ctx := DipArchiveCtx,
    base_archive_doc := BaseArchiveDoc,
    initial_archive_docs := InitialArchiveDocs
}) ->
    AipArchiveDoc = archivisation_traverse_ctx:get_archive_doc(AipArchiveCtx),
    {ok, Config} = archive:get_config(AipArchiveDoc),
    % check if there is dataset attached to current directory
    % if true, create a nested archive associated with the dataset
    NestedDatasetId = case archive_config:is_nested_archives_creation_enabled(Config) of
        true -> get_nested_dataset_id_if_attached(FileCtx, AipArchiveDoc);
        false -> undefined
    end,
    ShouldCreateNestedArchive = NestedDatasetId =/= undefined,
    
    {AipArchiveCtx2, DipArchiveCtx2, FinalBaseArchiveDoc} = case ShouldCreateNestedArchive of
        true -> initialize_nested_archive(NestedDatasetId, UserCtx, TraverseInfo);
        false -> {AipArchiveCtx, DipArchiveCtx, BaseArchiveDoc}
    end,
    {FinalAipArchiveCtx, FinalDipArchiveCtx} = do_archive(FileCtx, ResolvedFilePath, AipArchiveCtx2, DipArchiveCtx2,
        FinalBaseArchiveDoc, InitialArchiveDocs, UserCtx),
    
    ShouldCreateNestedArchive andalso symlink_nested_archive(AipArchiveCtx, FinalAipArchiveCtx, UserCtx),
    ShouldCreateNestedArchive andalso symlink_nested_archive(DipArchiveCtx, FinalDipArchiveCtx, UserCtx),
    TraverseInfo#{
        aip_ctx => FinalAipArchiveCtx,
        dip_ctx => FinalDipArchiveCtx,
        base_archive_doc => FinalBaseArchiveDoc
    }.


-spec initialize_archive_dir(archive:doc() | archive:id(), dataset:id(), user_ctx:ctx()) ->
    {ok, archive:doc()}.
initialize_archive_dir(ArchiveId, DatasetId, UserCtx) when is_binary(ArchiveId) ->
    {ok, ArchiveDoc} = archive:get(ArchiveId),
    initialize_archive_dir(ArchiveDoc, DatasetId, UserCtx);
initialize_archive_dir(ArchiveDoc, DatasetId, UserCtx) ->
    {ok, ArchiveDoc2} = create_archive_root_dir(ArchiveDoc, DatasetId, UserCtx),
    create_archive_data_dir(ArchiveDoc2, UserCtx).


-spec save_dir_checksum_metadata(file_id:file_guid() | undefined, user_ctx:ctx(), non_neg_integer()) ->
    ok.
save_dir_checksum_metadata(undefined, _, _) ->
    ok;
save_dir_checksum_metadata(FileGuid, UserCtx, TotalChildrenCount) ->
    FileCtx = file_ctx:new_by_guid(FileGuid),
    archivisation_checksum:dir_calculate_and_save(FileCtx, UserCtx, TotalChildrenCount).


-spec info_to_archive_docs(info()) -> [archive:doc()].
info_to_archive_docs(#{aip_ctx := AipCtx, dip_ctx := DipCtx}) ->
    lists:filtermap(fun(ArchivisationCtx) ->
        case archivisation_traverse_ctx:get_archive_doc(ArchivisationCtx) of
            undefined -> false;
            ArchiveDoc -> {true, ArchiveDoc}
        end
    end, [AipCtx, DipCtx]).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec create_archive_root_dir(archive:doc(), dataset:id(), user_ctx:ctx()) -> {ok, archive:doc()}.
create_archive_root_dir(ArchiveDoc, DatasetId, UserCtx) ->
    {ok, ArchiveId} = archive:get_id(ArchiveDoc),
    {ok, SpaceId} = archive:get_space_id(ArchiveDoc),
    UserId = user_ctx:get_user_id(UserCtx),
    {ok, ArchiveRootDirUuid} = archivisation_tree:create_archive_dir(
        ArchiveId, DatasetId, SpaceId, UserId),
    ArchiveRootDirGuid = file_id:pack_guid(ArchiveRootDirUuid, SpaceId),
    archive:set_root_dir_guid(ArchiveId, ArchiveRootDirGuid).


-spec create_archive_data_dir(archive:doc(), user_ctx:ctx()) -> {ok, archive:doc()}.
create_archive_data_dir(ArchiveDoc, UserCtx) ->
    {ok, ArchiveRootDirGuid} = archive:get_root_dir_guid(ArchiveDoc),
    ArchiveRootDirCtx = file_ctx:new_by_guid(ArchiveRootDirGuid),
    DataDirGuid = case is_bagit(ArchiveDoc) of
        true ->
            {ok, DataDirCtx} = bagit_archive:prepare(ArchiveRootDirCtx, UserCtx),
            DirGuid = file_ctx:get_logical_guid_const(DataDirCtx),
            save_dir_checksum_metadata(DirGuid, UserCtx, 1),
            DirGuid;
        false ->
            save_dir_checksum_metadata(ArchiveRootDirGuid, UserCtx, 1),
            ArchiveRootDirGuid
    end,
    archive:set_data_dir_guid(ArchiveDoc, DataDirGuid).


-spec get_nested_dataset_id_if_attached(file_ctx:ctx(), archive:doc()) -> dataset:id() | undefined.
get_nested_dataset_id_if_attached(FileCtx, ParentArchiveDoc) ->
    {FileDoc, _FileCtx2} = file_ctx:get_file_doc(FileCtx),
    {ok, ParentDatasetId} = archive:get_dataset_id(ParentArchiveDoc),
    case file_meta_dataset:get_id_if_attached(FileDoc) of
        undefined -> undefined;
        ParentDatasetId -> undefined;
        DatasetId -> DatasetId
    end.


-spec initialize_nested_archive(dataset:id(), user_ctx:ctx(), info()) -> 
    {Aip :: ctx(), Dip :: ctx(), archive:doc() | undefined}.
initialize_nested_archive(NestedDatasetId, UserCtx, #{
    aip_ctx := AipArchiveCtx,
    dip_ctx := DipArchiveCtx,
    scheduled_dataset_base_archive_doc := ScheduledDatasetBaseArchiveDoc
}) ->
    AipNestedArchiveCtx = create_and_prepare_nested_archive_dir(
        NestedDatasetId, AipArchiveCtx, UserCtx, aip),
    DipNestedArchiveCtx = create_and_prepare_nested_archive_dir(
        NestedDatasetId, DipArchiveCtx, UserCtx, dip),
    
    NestedBaseArchiveDoc = case ScheduledDatasetBaseArchiveDoc /= undefined of
        true ->
            incremental_archive:find_base_for_nested_archive(
                archivisation_traverse_ctx:get_archive_doc(AipNestedArchiveCtx), ScheduledDatasetBaseArchiveDoc, UserCtx);
        false ->
            undefined
    end,
    {ok, NestedAipArchiveDoc2} = archive:set_base_archive_id(archivisation_traverse_ctx:get_archive_doc(
        AipNestedArchiveCtx), NestedBaseArchiveDoc),
    {ok, NestedDipArchiveDoc2} = case archivisation_traverse_ctx:get_archive_doc(DipNestedArchiveCtx) of
        undefined -> {ok, undefined};
        NestedDipDoc -> archive:set_base_archive_id(NestedDipDoc, NestedBaseArchiveDoc)
    end,
    AipNestedArchiveCtx2 =
        archivisation_traverse_ctx:set_archive_doc(AipNestedArchiveCtx, NestedAipArchiveDoc2),
    DipNestedArchiveCtx2 =
        archivisation_traverse_ctx:set_archive_doc(DipNestedArchiveCtx, NestedDipArchiveDoc2),
    
    {FinalAipArchiveCtx, FinalDipArchiveCtx} = set_aip_dip_relation(
        AipNestedArchiveCtx2, DipNestedArchiveCtx2),
    {FinalAipArchiveCtx, FinalDipArchiveCtx, NestedBaseArchiveDoc}.


-spec symlink_nested_archive(Parent :: ctx(), Nested :: ctx(), user_ctx:ctx()) -> ok.
symlink_nested_archive(ParentArchiveCtx, NestedArchiveCtx, UserCtx) ->
    case archivisation_traverse_ctx:get_target_parent(ParentArchiveCtx) of
        undefined -> ok;
        TargetParentFileGuid -> make_symlink(
            archivisation_traverse_ctx:get_target_parent(NestedArchiveCtx), TargetParentFileGuid, UserCtx)
    end.


-spec create_and_prepare_nested_archive_dir(dataset:id(), ctx(), user_ctx:ctx(), dip | aip) ->
    ctx().
create_and_prepare_nested_archive_dir(DatasetId, ParentArchiveCtx, UserCtx, Type) ->
    case archivisation_traverse_ctx:get_archive_doc(ParentArchiveCtx) of
        undefined ->
            ParentArchiveCtx;
        ParentArchiveDoc ->
            {ok, SpaceId} = archive:get_space_id(ParentArchiveDoc),
            {ok, ParentArchiveId} = archive:get_id(ParentArchiveDoc),
            {ok, NestedArchiveDoc} = archive:create_nested(DatasetId, ParentArchiveDoc),
            {ok, ArchiveId} = archive:get_id(NestedArchiveDoc),
            {ok, Timestamp} = archive:get_creation_time(NestedArchiveDoc),
            case Type of
                dip -> ok;
                aip -> archives_list:add(DatasetId, SpaceId, ArchiveId, Timestamp)
            end,
            archives_forest:add(ParentArchiveId, SpaceId, ArchiveId),
            {ok, NestedArchiveDoc2} = initialize_archive_dir(NestedArchiveDoc, DatasetId, UserCtx),
            {ok, NestedArchiveDataDirGuid} = archive:get_data_dir_guid(NestedArchiveDoc2),
            archivisation_traverse_ctx:init_for_nested_archive(NestedArchiveDoc2, NestedArchiveDataDirGuid)
    end.


-spec set_aip_dip_relation(Aip :: ctx(), Dip :: ctx()) -> {Aip :: ctx(), Dip :: ctx()}.
set_aip_dip_relation(AipArchiveCtx, DipArchiveCtx) ->
    {ok, NestedAipArchiveId} = archive:get_id(archivisation_traverse_ctx:get_archive_doc(AipArchiveCtx)),
    {ok, NestedDipArchiveId} = case archivisation_traverse_ctx:get_archive_doc(DipArchiveCtx) of
        undefined -> {ok, undefined};
        NestedDipArchiveDoc -> archive:get_id(NestedDipArchiveDoc)
    end,
    {ok, UpdatedAipArchiveDoc} = archive:set_related_dip(
        archivisation_traverse_ctx:get_archive_doc(AipArchiveCtx), NestedDipArchiveId),
    {ok, UpdatedDipArchiveDoc} = case archivisation_traverse_ctx:get_archive_doc(DipArchiveCtx) of
        undefined -> {ok, undefined};
        DipNestedArchiveDoc -> archive:set_related_aip(DipNestedArchiveDoc, NestedAipArchiveId)
    end,
    {
        archivisation_traverse_ctx:set_archive_doc(AipArchiveCtx, UpdatedAipArchiveDoc),
        archivisation_traverse_ctx:set_archive_doc(DipArchiveCtx, UpdatedDipArchiveDoc)
    }.


-spec propagate_finished_up(
    file_ctx:ctx(), user_ctx:ctx(), info(), id(), file_meta:uuid(), file_meta:path()
) -> ok.
propagate_finished_up(FileCtx, UserCtx, TraverseInfo, TaskId, SourceMasterUuid, FilePath) ->
    #{scheduled_dataset_root_guid := ScheduledDatasetRootGuid} = TraverseInfo,
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    case FileGuid =:= ScheduledDatasetRootGuid of
        true ->
            ok;
        false ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            ParentStatus = tree_traverse:report_child_processed(TaskId, SourceMasterUuid),
            case ParentStatus of
                ?SUBTREE_PROCESSED(NextSubtreeRoot, StartTimestamp) ->
                    SourceMasterFileCtx = file_ctx:new_by_uuid(SourceMasterUuid, SpaceId),
                    mark_finished_and_propagate_up(SourceMasterFileCtx, UserCtx, TraverseInfo, 
                        TaskId, NextSubtreeRoot, StartTimestamp, filename:dirname(FilePath), completed);
                ?SUBTREE_NOT_PROCESSED ->
                    ok
            end
    end.


-spec mark_finished_if_current_archive_is_rooted_in_current_file(file_ctx:ctx(), user_ctx:ctx(),
    info()) -> info().
mark_finished_if_current_archive_is_rooted_in_current_file(CurrentFileCtx, UserCtx, #{
    aip_ctx := AipArchiveCtx,
    dip_ctx := DipArchiveCtx
} = TraverseInfo) ->
    case is_archive_rooted_in_current_file(CurrentFileCtx, TraverseInfo) of
        true ->
            TraverseInfo#{
                aip_ctx => finalize_archive(AipArchiveCtx, UserCtx),
                dip_ctx => finalize_archive(DipArchiveCtx, UserCtx)
            };
        false ->
            TraverseInfo
    end.


-spec finalize_archive(ctx(), user_ctx:ctx()) -> ctx().
finalize_archive(ArchiveCtx, UserCtx) ->
    case archivisation_traverse_ctx:get_archive_doc(ArchiveCtx) of
        undefined -> 
            ArchiveCtx;
        CurrentDoc ->
            NestedArchiveStats = archive_api:get_nested_archives_stats(CurrentDoc),
            mark_finished(CurrentDoc, UserCtx, NestedArchiveStats),
            {ok, ParentDocOrUndefined} = archive:get_parent_doc(CurrentDoc),
            archivisation_traverse_ctx:set_archive_doc(ArchiveCtx, ParentDocOrUndefined)
    end.


-spec mark_finished(archive:doc(), user_ctx:ctx(), archive_stats:record()) -> ok.
mark_finished(ArchiveDoc, UserCtx, NestedArchiveStats) ->
    case is_bagit(ArchiveDoc) of
        true ->
            case archive_traverses_common:is_cancelled(ArchiveDoc) of
                true -> ok;
                false ->
                    {ok, ArchiveRootDirCtx} = archive:get_root_dir_ctx(ArchiveDoc),
                    bagit_archive:finalize(ArchiveRootDirCtx, UserCtx)
            end;
        false -> ok
    end,
    ok = archive:mark_creation_finished(ArchiveDoc, NestedArchiveStats).


-spec do_archive(file_ctx:ctx(), file_meta:path(), ctx(), ctx(),
    archive:doc() | undefined, docs_map(), user_ctx:ctx()) -> {ctx(), ctx()}.
do_archive(FileCtx, ResolvedFilePath, AipArchiveCtx, DipArchiveCtx, BaseArchiveDoc,
    InitialArchiveDocs, UserCtx
) ->
    {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),
    {FinalAipTargetParent, FinalDipTargetParent} = case IsDir of
        true ->
            {
                archive_dir(AipArchiveCtx, FileCtx2, ResolvedFilePath, UserCtx),
                archive_dir(DipArchiveCtx, FileCtx2, ResolvedFilePath, UserCtx)
            };
        false ->
            archive_file_and_mark_finished(FileCtx2, AipArchiveCtx, DipArchiveCtx,
                BaseArchiveDoc, InitialArchiveDocs, ResolvedFilePath, UserCtx)
    end,
    {
        archivisation_traverse_ctx:set_target_parent(AipArchiveCtx, FinalAipTargetParent),
        archivisation_traverse_ctx:set_target_parent(DipArchiveCtx, FinalDipTargetParent)
    }.


-spec archive_dir(ctx(), file_ctx:ctx(), file_meta:path(), user_ctx:ctx()) ->
    file_id:file_guid().
archive_dir(ArchiveCtx, FileCtx, ResolvedFilePath, UserCtx) ->
    case archivisation_traverse_ctx:get_archive_doc(ArchiveCtx) of
        undefined -> 
            undefined;
        ArchiveDoc ->
            DirName = filename:basename(ResolvedFilePath),
            DirGuid = file_ctx:get_logical_guid_const(FileCtx),
            TargetParentGuid = archivisation_traverse_ctx:get_target_parent(ArchiveCtx),
            % only directory is copied therefore recursive=false is passed to copy function
            {ok, CopyGuid, _} = file_copy:copy(user_ctx:get_session_id(UserCtx), DirGuid, TargetParentGuid,
                DirName, #{recursive => false}),
            case is_bagit(ArchiveDoc) of
                false -> ok;
                true -> bagit_archive:archive_dir(
                    ArchiveDoc, ResolvedFilePath, file_ctx:new_by_guid(CopyGuid), UserCtx)
            end,
            CopyGuid
    end.


-spec archive_file_and_mark_finished(file_ctx:ctx(), ctx(), ctx(),
    archive:doc() | undefined, docs_map(), file_meta:path(), user_ctx:ctx()) ->
    {file_id:file_guid(), file_id:file_guid() | undefined}.
archive_file_and_mark_finished(FileCtx, AipArchiveCtx, DipArchiveCtx, BaseArchiveDoc,
    InitialArchiveDocs, ResolvedFilePath, UserCtx
) ->
    {ok, ArchiveFileCtx} = archive_reg_file(FileCtx, AipArchiveCtx, BaseArchiveDoc, 
        maps:get(aip, InitialArchiveDocs), ResolvedFilePath, UserCtx
    ),
    {ok, DipArchiveFileGuid} = dip_archive_reg_file(FileCtx, ArchiveFileCtx, DipArchiveCtx,
        maps:get(dip, InitialArchiveDocs), UserCtx),
    {FileSize, _} = file_ctx:get_file_size(ArchiveFileCtx),
    ok = archive:mark_file_archived(archivisation_traverse_ctx:get_archive_doc(AipArchiveCtx), FileSize),
    case archivisation_traverse_ctx:get_archive_doc(DipArchiveCtx) of
        undefined -> ok;
        DipArchiveDoc -> archive:mark_file_archived(DipArchiveDoc, FileSize)
    end,
    {file_ctx:get_logical_guid_const(ArchiveFileCtx), DipArchiveFileGuid}.


-spec archive_reg_file(file_ctx:ctx(), ctx(), archive:doc(), archive:doc(), file_meta:path(), user_ctx:ctx()) -> 
    {ok, file_ctx:ctx()}.
archive_reg_file(FileCtx, ArchiveCtx, BaseArchiveDoc, InitialAipDoc, ResolvedFilePath, UserCtx) ->
    CurrentArchiveDoc = archivisation_traverse_ctx:get_archive_doc(ArchiveCtx),
    TargetParentCtx = file_ctx:new_by_guid(archivisation_traverse_ctx:get_target_parent(ArchiveCtx)),
    case {file_ctx:is_symlink_const(FileCtx), is_bagit(CurrentArchiveDoc)} of
        {false, false} ->
            plain_archive:archive_regular_file(CurrentArchiveDoc, FileCtx, TargetParentCtx,
                BaseArchiveDoc, ResolvedFilePath, UserCtx, ?COPY_OPTIONS(CurrentArchiveDoc#document.key));
        {false, true} ->
            bagit_archive:archive_file(CurrentArchiveDoc, FileCtx, TargetParentCtx, BaseArchiveDoc,
                ResolvedFilePath, UserCtx, ?COPY_OPTIONS(CurrentArchiveDoc#document.key));
        {true, _} ->
            % bagit archive does not store any additional data for symlinks
            plain_archive:archive_symlink(FileCtx, TargetParentCtx, InitialAipDoc, UserCtx)
    end.


-spec dip_archive_reg_file(file_ctx:ctx(), file_ctx:ctx(), ctx(), archive:doc(), user_ctx:ctx()) ->
    {ok, file_id:file_guid() | undefined}.
dip_archive_reg_file(OriginalFileCtx, ArchivedFileCtx, DipArchiveCtx, InitialDipDoc, UserCtx) ->
    case {archivisation_traverse_ctx:get_target_parent(DipArchiveCtx), file_ctx:is_symlink_const(OriginalFileCtx)} of
        {undefined, _} ->
            {ok, undefined};
        {DipTargetParentGuid, true} ->
            plain_archive:archive_symlink(
                OriginalFileCtx, file_ctx:new_by_guid(DipTargetParentGuid), InitialDipDoc, UserCtx);
        {DipTargetParentGuid, false} ->
            {FileName, _} = file_ctx:get_aliased_name(ArchivedFileCtx, UserCtx),
            {ok, #file_attr{guid = LinkGuid}} = lfm:make_link(
                user_ctx:get_session_id(UserCtx),
                #file_ref{guid = file_ctx:get_logical_guid_const(ArchivedFileCtx)},
                #file_ref{guid = DipTargetParentGuid},
                FileName
            ),
            {ok, LinkGuid}
    end.


-spec make_symlink(file_id:file_guid(), file_id:file_guid(), user_ctx:ctx()) -> ok.
make_symlink(TargetGuid, ParentGuid, UserCtx) ->
    TargetCtx = file_ctx:new_by_guid(TargetGuid),
    SpaceId = file_ctx:get_space_id_const(TargetCtx),
    SpaceIdPrefix = ?SYMLINK_SPACE_ID_ABS_PATH_PREFIX(SpaceId),
    {FileName, TargetCtx2} = file_ctx:get_aliased_name(TargetCtx, UserCtx),
    {TargetCanonicalPath, _} = file_ctx:get_canonical_path(TargetCtx2),
    [_Sep, _SpaceId | Rest] = filename:split(TargetCanonicalPath),
    SymlinkValue = filename:join([SpaceIdPrefix | Rest]),
    {ok, _} = lfm:make_symlink(user_ctx:get_session_id(UserCtx), ?FILE_REF(ParentGuid),
        FileName, SymlinkValue),
    ok.


-spec is_bagit(archive:doc()) -> boolean().
is_bagit(ArchiveDoc) ->
    {ok, Config} = archive:get_config(ArchiveDoc),
    archive_config:get_layout(Config) =:= ?ARCHIVE_BAGIT_LAYOUT.


-spec is_dataset_root(tree_traverse:job()) -> boolean().
is_dataset_root(#tree_traverse{
    file_ctx = FileCtx,
    traverse_info = #{scheduled_dataset_root_guid := ScheduledDatasetRootGuid}
}) ->
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    FileGuid =:= ScheduledDatasetRootGuid.


-spec is_first_job(tree_traverse:master_job()) -> boolean().
is_first_job(Job = #tree_traverse{
    file_ctx = FileCtx,
    pagination_token = PaginationToken
}) ->
    {IsDir, _} = file_ctx:is_dir(FileCtx),
    is_dataset_root(Job) andalso (
        (IsDir andalso (PaginationToken =:= undefined))
            orelse not IsDir
    ).


-spec is_archive_rooted_in_current_file(file_ctx:ctx(), info()) -> boolean().
is_archive_rooted_in_current_file(CurrentFileCtx, #{
    scheduled_dataset_root_guid := ScheduledDatasetRootGuid,
    aip_ctx := AipArchiveCtx
}) ->
    CurrentAipDoc = archivisation_traverse_ctx:get_archive_doc(AipArchiveCtx),
    {ok, CurrentArchiveRootGuid} = archive:get_dataset_root_file_guid(CurrentAipDoc),
    {ok, Config} = archive:get_config(CurrentAipDoc),
    CreateNestedArchives = archive_config:is_nested_archives_creation_enabled(Config),
    CurrentFileGuid = file_ctx:get_logical_guid_const(CurrentFileCtx),
    CurrentFileGuid =:= ScheduledDatasetRootGuid orelse
        (CurrentFileGuid =:= CurrentArchiveRootGuid andalso CreateNestedArchives).


-spec report_to_audit_log(file_ctx:ctx(), info(), time:millis(), file_meta:path(),
    {failed, Reason :: any()} | completed, user_ctx:ctx()) -> ok.
report_to_audit_log(CurrentFileCtx, TraverseInfo, StartTimestamp, FilePath, Status, UserCtx) ->
    {FileType, _CurrentFileCtx2} = file_ctx:get_effective_type(CurrentFileCtx),
    {ReportFun, AdditionalArgs} = case Status of
        completed ->
            {fun archivisation_audit_log:report_file_archivisation_finished/4, []};
        {failed, Reason} ->
            {fun archivisation_audit_log:report_file_archivisation_failed/5, [Reason]}
    end,
    
    lists:foreach(fun(ArchiveDoc) ->
        {ok, DatasetRootParentPath} = archive:get_dataset_root_parent_path(ArchiveDoc, UserCtx),
        RelativeFilePath = filepath_utils:relative(DatasetRootParentPath, FilePath),
        {ok, ArchiveId} = archive:get_id(ArchiveDoc),
        erlang:apply(ReportFun, [ArchiveId, RelativeFilePath, FileType, StartTimestamp | AdditionalArgs])
    end, info_to_archive_docs(TraverseInfo)).
