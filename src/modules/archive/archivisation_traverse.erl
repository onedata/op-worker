%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements traverse_behaviour.
%%% It is used to traverse tree rooted in root of passed Dataset
%%% and build archive of the dataset.
%%% @end
%%%-------------------------------------------------------------------
-module(archivisation_traverse).
-author("Jakub Kudzia").

-behavior(traverse_behaviour).

-include("global_definitions.hrl").
-include("tree_traverse.hrl").
-include("modules/dataset/archive.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([init_pool/0, stop_pool/0, start/3
]).

%% Traverse behaviour callbacks
-export([
    task_started/2,
    task_finished/2,
    get_sync_info/1,
    get_job/1,
    update_job_progress/5,
    do_master_job/2,
    do_slave_job/2
]).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).

-type id() :: tree_traverse:id().
%% @formatter:off
-type info() :: #{
    current_archive_doc := archive:doc(),
    % base for current_archive_doc
    base_archive_doc := archive:doc(),
    % base for top archive, the one created from dataset on which archivisation was scheduled
    scheduled_dataset_base_archive_doc := archive:doc(),
    target_parent_guid := file_id:file_guid(),
    scheduled_dataset_root_guid := file_id:file_guid()
}.
%% @formatter:on

-export_type([id/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init_pool() -> ok.
init_pool() ->
    MasterJobsLimit = op_worker:get_env(archivisation_traverse_master_jobs_limit, 10),
    SlaveJobsLimit = op_worker:get_env(archivisation_traverse_slave_jobs_limit, 100),
    ParallelismLimit = op_worker:get_env(archivisation_traverse_parallelism_limit, 10),
    tree_traverse:init(?POOL_NAME, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit).


-spec stop_pool() -> ok.
stop_pool() ->
    tree_traverse:stop(?POOL_NAME).


-spec start(archive:doc(), dataset:doc(), user_ctx:ctx()) -> ok | {error, term()}.
start(ArchiveDoc, DatasetDoc, UserCtx) ->
    TaskId = datastore_key:new(),
    case tree_traverse_session:setup_for_task(UserCtx, TaskId) of
        ok ->
            {ok, ArchiveId} = archive:get_id(ArchiveDoc),
            {ok, DatasetId} = archive:get_dataset_id(ArchiveDoc),
            UserId = user_ctx:get_user_id(UserCtx),

            {ok, ArchiveDoc2} = prepare_archive_dir(ArchiveDoc, DatasetId, UserCtx),
            {ok, ArchiveDataDirGuid} = archive:get_data_dir_guid(ArchiveDoc2),
            {ok, Config} = archive:get_config(ArchiveDoc2),
            IsIncremental = archive_config:is_incremental(Config),
            {ok, BaseArchiveId} = archive:get_base_archive_id(ArchiveDoc2),
            BaseArchiveDoc = case IsIncremental andalso BaseArchiveId =/= undefined of
                true ->
                    {ok, Doc} = archive:get(BaseArchiveId),
                    Doc;
                false ->
                    undefined
            end,

            AdditionalData = #{
                <<"archiveId">> => ArchiveId,
                <<"datasetId">> => DatasetId
            },
            {ok, CallbackOrUndefined} = archive:get_preserved_callback(ArchiveDoc2),
            AdditionalData2 = maps_utils:put_if_defined(AdditionalData, <<"callback">>, CallbackOrUndefined),

            DatasetRootCtx = dataset_api:get_associated_file_ctx(DatasetDoc),

            Options = #{
                task_id => TaskId,
                track_subtree_status => true,
                children_master_jobs_mode => async,
                traverse_info => #{
                    current_archive_doc => ArchiveDoc2,
                    base_archive_doc => BaseArchiveDoc,
                    scheduled_dataset_base_archive_doc => BaseArchiveDoc,
                    target_parent_guid => ArchiveDataDirGuid,
                    scheduled_dataset_root_guid => file_ctx:get_logical_guid_const(DatasetRootCtx)
                },
                additional_data => AdditionalData2
            },
            {ok, TaskId} = tree_traverse:run(?POOL_NAME, DatasetRootCtx, UserId, Options),
            ok;
        {error, _} = Error ->
            Error
    end.


%%%===================================================================
%%% Traverse behaviour callbacks
%%%===================================================================

-spec task_started(id(), tree_traverse:pool()) -> ok.
task_started(TaskId, _Pool) ->
    ?debug("Archivisation job ~p started", [TaskId]).


-spec task_finished(id(), tree_traverse:pool()) -> ok.
task_finished(TaskId, _Pool) ->
    ?debug("Archivisation job ~p finished", [TaskId]),
    tree_traverse_session:close_for_task(TaskId),
    {ok, TaskDoc} = traverse_task:get(?POOL_NAME, TaskId),
    {ok, AdditionalData} = traverse_task:get_additional_data(TaskDoc),
    {ok, Description} = traverse_task:get_description(TaskDoc),

    ArchiveId = maps:get(<<"archiveId">>, AdditionalData),
    DatasetId = maps:get(<<"datasetId">>, AdditionalData),
    CallbackUrlOrUndefined = maps:get(<<"callback">>, AdditionalData, undefined),

    SlaveJobsFailed = maps:get(slave_jobs_failed, Description, 0),
    MasterJobsFailed = maps:get(master_jobs_failed, Description, 0),
    case SlaveJobsFailed + MasterJobsFailed =:= 0 of
        true ->
            archivisation_callback:notify_preserved(ArchiveId, DatasetId, CallbackUrlOrUndefined);
        false ->
            % TODO VFS-7662 send more descriptive error description to archivisation callback
            ErrorDescription = <<"Errors occurered during archivisation job.">>,
            archivisation_callback:notify_preservation_failed(ArchiveId, DatasetId, CallbackUrlOrUndefined,
                ErrorDescription)
    end.


-spec get_sync_info(tree_traverse:master_job()) -> {ok, traverse:sync_info()}.
get_sync_info(Job) ->
    tree_traverse:get_sync_info(Job).


-spec get_job(traverse:job_id() | tree_traverse_job:doc()) ->
    {ok, tree_traverse:master_job(), tree_traverse:pool(), id()}  | {error, term()}.
get_job(DocOrId) ->
    tree_traverse:get_job(DocOrId).


-spec update_job_progress(undefined | main_job | traverse:job_id(),
    tree_traverse:master_job(), tree_traverse:pool(), id(),
    traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TaskId, Status, ?MODULE).


-spec do_master_job(tree_traverse:master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_master_job(Job = #tree_traverse{
    user_id = UserId,
    file_ctx = FileCtx,
    traverse_info = TraverseInfo,
    token = ListingToken
},
    MasterJobArgs = #{task_id := TaskId}
) ->
    mark_building_if_first_job(Job),
    IsFirstBatch = ListingToken =:= ?INITIAL_LS_TOKEN,
    {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),

    case IsDir of
        true ->
            {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
            TraverseInfo2 = case IsFirstBatch of
                true -> handle_nested_dataset_and_do_archive(FileCtx, UserCtx, TraverseInfo);
                false -> TraverseInfo
            end,

            NewJobsPreprocessor = fun(_SlaveJobs, _MasterJobs, _ListExtendedInfo, SubtreeProcessingStatus) ->
                case SubtreeProcessingStatus of
                    ?SUBTREE_PROCESSED ->
                        mark_finished_and_propagate_up(FileCtx2, UserCtx, TraverseInfo2, TaskId);
                    ?SUBTREE_NOT_PROCESSED ->
                        ok
                end
            end,

            Job2 = Job#tree_traverse{traverse_info = TraverseInfo2},
            tree_traverse:do_master_job(Job2, MasterJobArgs, NewJobsPreprocessor);
        false ->
            tree_traverse:do_master_job(Job, MasterJobArgs)
    end.


-spec do_slave_job(tree_traverse:slave_job(), id()) -> ok.
do_slave_job(#tree_traverse_slave{
    user_id = UserId,
    file_ctx = FileCtx,
    traverse_info = TraverseInfo
}, TaskId) ->
    {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
    TraverseInfo2 = handle_nested_dataset_and_do_archive(FileCtx, UserCtx, TraverseInfo),
    mark_finished_and_propagate_up(FileCtx, UserCtx, TraverseInfo2, TaskId).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec prepare_archive_dir(archive:doc(), dataset:id(), user_ctx:ctx()) -> {ok, archive:doc()}.
prepare_archive_dir(ArchiveDoc, DatasetId, UserCtx) ->
    {ok, ArchiveDoc2} = create_archive_root_dir(ArchiveDoc, DatasetId, UserCtx),
    create_archive_data_dir(ArchiveDoc2, UserCtx).


-spec create_archive_root_dir(archive:doc(), dataset:id(), user_ctx:ctx()) -> {ok, archive:doc()}.
create_archive_root_dir(ArchiveDoc, DatasetId, UserCtx) ->
    {ok, ArchiveId} = archive:get_id(ArchiveDoc),
    {ok, SpaceId} = archive:get_space_id(ArchiveDoc),
    UserId = user_ctx:get_user_id(UserCtx),
    {ok, ArchiveRootDirUuid} = archivisation_tree:create_archive_dir(ArchiveId, DatasetId, SpaceId, UserId),
    ArchiveRootDirGuid = file_id:pack_guid(ArchiveRootDirUuid, SpaceId),
    archive:set_root_dir_guid(ArchiveId, ArchiveRootDirGuid).


-spec create_archive_data_dir(archive:doc(), user_ctx:ctx()) -> {ok, archive:doc()}.
create_archive_data_dir(ArchiveDoc, UserCtx) ->
    {ok, ArchiveRootDirGuid} = archive:get_root_dir_guid(ArchiveDoc),
    DataDirGuid = case is_bagit(ArchiveDoc) of
        true ->
            ArchiveRootDirCtx = file_ctx:new_by_guid(ArchiveRootDirGuid),
            {ok, DataDirCtx} = bagit_archive:prepare(ArchiveRootDirCtx, UserCtx),
            file_ctx:get_logical_guid_const(DataDirCtx);
        false ->
            ArchiveRootDirGuid
    end,
    archive:set_data_dir_guid(ArchiveDoc, DataDirGuid).


-spec handle_nested_dataset_and_do_archive(file_ctx:ctx(), user_ctx:ctx(), info()) -> info().
handle_nested_dataset_and_do_archive(FileCtx, UserCtx, TraverseInfo = #{
    target_parent_guid := TargetParentGuid,
    current_archive_doc := CurrentArchiveDoc,
    base_archive_doc := BaseArchiveDoc,
    scheduled_dataset_base_archive_doc := ScheduledDatasetBaseArchiveDoc
}) ->
    TargetParentCtx = file_ctx:new_by_guid(TargetParentGuid),
    {ok, Config} = archive:get_config(CurrentArchiveDoc),
    CreateNestedArchives = archive_config:should_create_nested_archives(Config),
    % check if there is dataset attached to current directory
    % if true, create a nested archive associated with the dataset
    NestedDatasetId = case CreateNestedArchives of
        true ->
            get_nested_dataset_id_if_attached(FileCtx, CurrentArchiveDoc);
        false ->
            undefined
    end,
    {FinalArchiveDoc, FinalTargetParentCtx, FinalBaseArchiveDoc} = case NestedDatasetId =/= undefined of
        true ->
            {ok, NestedArchiveDoc} = create_and_prepare_nested_archive_dir(NestedDatasetId, CurrentArchiveDoc, UserCtx),
            {ok, NestedArchiveDataDirGuid} = archive:get_data_dir_guid(NestedArchiveDoc),
            NestedArchiveDataDirCtx = file_ctx:new_by_guid(NestedArchiveDataDirGuid),

            NestedBaseArchiveDoc = case ScheduledDatasetBaseArchiveDoc /= undefined of
                true ->
                    incremental_archive:find_base_for_nested_archive(NestedArchiveDoc, ScheduledDatasetBaseArchiveDoc,
                        UserCtx);
                false ->
                    undefined
            end,
            {ok, NestedArchiveDoc2} = archive:set_base_archive_id(NestedArchiveDoc, NestedBaseArchiveDoc),
            {ok, ArchivedFileCtx} =
                do_archive(FileCtx, NestedArchiveDataDirCtx, NestedArchiveDoc2, NestedBaseArchiveDoc, UserCtx),
            make_symlink(ArchivedFileCtx, TargetParentCtx, UserCtx),
            {NestedArchiveDoc2, ArchivedFileCtx, NestedBaseArchiveDoc};
        false ->
            {ok, ArchivedFileCtx} = do_archive(FileCtx, TargetParentCtx, CurrentArchiveDoc, BaseArchiveDoc, UserCtx),
            {CurrentArchiveDoc, ArchivedFileCtx, BaseArchiveDoc}
    end,
    TraverseInfo#{
        target_parent_guid => file_ctx:get_logical_guid_const(FinalTargetParentCtx),
        current_archive_doc => FinalArchiveDoc,
        base_archive_doc => FinalBaseArchiveDoc
    }.


-spec get_nested_dataset_id_if_attached(file_ctx:ctx(), archive:doc()) -> dataset:id() | undefined.
get_nested_dataset_id_if_attached(FileCtx, ParentArchiveDoc) ->
    {FileDoc, _FileCtx2} = file_ctx:get_file_doc(FileCtx),
    {ok, ParentDatasetId} = archive:get_dataset_id(ParentArchiveDoc),
    case file_meta_dataset:get_id_if_attached(FileDoc) of
        undefined -> undefined;
        ParentDatasetId -> undefined;
        DatasetId -> DatasetId
    end.


-spec create_and_prepare_nested_archive_dir(dataset:id(), archive:doc(), user_ctx:ctx()) -> {ok, archive:doc()} | {error, term()}.
create_and_prepare_nested_archive_dir(DatasetId, ParentArchiveDoc, UserCtx) ->
    {ok, SpaceId} = archive:get_space_id(ParentArchiveDoc),
    {ok, ParentArchiveId} = archive:get_id(ParentArchiveDoc),
    case archive:create_nested(DatasetId, ParentArchiveDoc) of
        {ok, ArchiveDoc} ->
            {ok, ArchiveId} = archive:get_id(ArchiveDoc),
            {ok, Timestamp} = archive:get_creation_time(ArchiveDoc),
            archives_list:add(DatasetId, SpaceId, ArchiveId, Timestamp),
            archives_forest:add(ParentArchiveId, SpaceId, ArchiveId),
            prepare_archive_dir(ArchiveDoc, DatasetId, UserCtx);
        {error, _} = Error ->
            Error
    end.


-spec mark_finished_and_propagate_up(file_ctx:ctx(), user_ctx:ctx(), info(), id()) -> ok.
mark_finished_and_propagate_up(CurrentFileCtx, UserCtx, TraverseInfo, TaskId) ->
    NextArchiveDocOrUndefined = mark_finished_if_current_archive_is_rooted_in_current_file(CurrentFileCtx, UserCtx, TraverseInfo),
    case NextArchiveDocOrUndefined of
        undefined ->
            ok;
        NextArchiveDoc ->
            propagate_up(CurrentFileCtx, UserCtx, TraverseInfo#{current_archive_doc => NextArchiveDoc}, TaskId)
    end.


-spec propagate_up(file_ctx:ctx(), user_ctx:ctx(), info(), id()) -> ok.
propagate_up(FileCtx, UserCtx, TraverseInfo = #{scheduled_dataset_root_guid := ScheduledDatasetRootGuid}, TaskId) ->
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    case FileGuid =:= ScheduledDatasetRootGuid of
        true ->
            ok;
        false ->
            {ParentFileCtx, _} = files_tree:get_parent(FileCtx, UserCtx),
            ParentUuid = file_ctx:get_logical_uuid_const(ParentFileCtx),
            ParentStatus = tree_traverse:report_child_processed(TaskId, ParentUuid),
            case ParentStatus of
                ?SUBTREE_PROCESSED ->
                    mark_finished_and_propagate_up(ParentFileCtx, UserCtx, TraverseInfo, TaskId);
                ?SUBTREE_NOT_PROCESSED ->
                    ok
            end
    end.


-spec mark_building_if_first_job(tree_traverse:master_job()) -> ok.
mark_building_if_first_job(Job = #tree_traverse{traverse_info = #{current_archive_doc := CurrentArchiveDoc}}) ->
    case is_first_job(Job) of
        true -> ok = archive:mark_building(CurrentArchiveDoc);
        false -> ok
    end.


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
    token = ListingToken
}) ->
    {IsDir, _} = file_ctx:is_dir(FileCtx),
    is_dataset_root(Job) andalso (
        (IsDir andalso (ListingToken =:= ?INITIAL_LS_TOKEN))
            orelse not IsDir
    ).


-spec mark_finished_if_current_archive_is_rooted_in_current_file(file_ctx:ctx(), user_ctx:ctx(), info()) ->
    archive:doc() | undefined.
mark_finished_if_current_archive_is_rooted_in_current_file(CurrentFileCtx, UserCtx, #{
    scheduled_dataset_root_guid := ScheduledDatasetRootGuid,
    current_archive_doc := CurrentArchiveDoc
}) ->
    {ok, CurrentArchiveRootGuid} = archive:get_dataset_root_file_guid(CurrentArchiveDoc),
    {ok, Config} = archive:get_config(CurrentArchiveDoc),
    CreateNestedArchives = archive_config:should_create_nested_archives(Config),
    CurrentFileGuid = file_ctx:get_logical_guid_const(CurrentFileCtx),
    case CurrentFileGuid =:= ScheduledDatasetRootGuid orelse
        (CurrentFileGuid =:= CurrentArchiveRootGuid andalso CreateNestedArchives)
    of
        true ->
            calculate_stats_and_mark_finished(CurrentArchiveDoc, UserCtx),
            {ok, ParentDocOrUndefined} = archive:get_parent_doc(CurrentArchiveDoc),
            ParentDocOrUndefined;
        false ->
            CurrentArchiveDoc
    end.


-spec calculate_stats_and_mark_finished(archive:doc(), user_ctx:ctx()) -> ok.
calculate_stats_and_mark_finished(ArchiveDoc, UserCtx) ->
    NestedArchiveStats = archive_api:get_nested_archives_stats(ArchiveDoc),
    {ok, ArchiveRootDirCtx} = archive:get_root_dir_ctx(ArchiveDoc),
    case is_bagit(ArchiveDoc) of
        true -> bagit_archive:finalize(ArchiveRootDirCtx, UserCtx);
        false -> ok
    end,
    ok = archive:mark_finished(ArchiveDoc, NestedArchiveStats).


-spec do_archive(file_ctx:ctx(), file_ctx:ctx(), archive:doc(), archive:doc() | undefined, user_ctx:ctx()) ->
    {ok, file_ctx:ctx()}.
do_archive(FileCtx, TargetParentCtx, CurrentArchiveDoc, BaseArchiveDoc, UserCtx) ->
    {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),
    case IsDir of
        true ->
            archive_dir(FileCtx2, TargetParentCtx, UserCtx);
        false ->
            archive_file_and_mark_finished(FileCtx2, TargetParentCtx, CurrentArchiveDoc, BaseArchiveDoc, UserCtx)
    end.


-spec archive_dir(file_ctx:ctx(), file_ctx:ctx(), user_ctx:ctx()) -> {ok, file_ctx:ctx()} | {error, term()}.
archive_dir(FileCtx, TargetParentCtx, UserCtx) ->
    try
        archive_dir_insecure(FileCtx, TargetParentCtx, UserCtx)
    catch
        Class:Reason:Stacktrace ->
            Guid = file_ctx:get_logical_guid_const(FileCtx),
            ?error_stacktrace(
                "Unexpected error ~p:~p occured during archivisation of directory ~s.",
                [Class, Reason, Guid],
                Stacktrace
            ),
            {error, Reason}
    end.


-spec archive_dir_insecure(file_ctx:ctx(), file_ctx:ctx(), user_ctx:ctx()) -> {ok, file_ctx:ctx()}.
archive_dir_insecure(FileCtx, TargetParentCtx, UserCtx) ->
    {DirName, FileCtx2} = file_ctx:get_aliased_name(FileCtx, UserCtx),
    DirGuid = file_ctx:get_logical_guid_const(FileCtx2),
    TargetParentGuid = file_ctx:get_logical_guid_const(TargetParentCtx),
    % only directory is copied therefore recursive=false is passed to copy function
    {ok, CopyGuid, _} = file_copy:copy(user_ctx:get_session_id(UserCtx), DirGuid, TargetParentGuid, DirName, false),
    {ok, file_ctx:new_by_guid(CopyGuid)}.


-spec archive_file_and_mark_finished(file_ctx:ctx(), file_ctx:ctx(), archive:doc(), archive:doc() | undefined, user_ctx:ctx()) ->
    {ok, file_ctx:ctx()} | {error, term()}.
archive_file_and_mark_finished(FileCtx, TargetParentCtx, CurrentArchiveDoc, BaseArchiveDoc, UserCtx) ->
    case archive_file(FileCtx, TargetParentCtx, CurrentArchiveDoc, BaseArchiveDoc, UserCtx) of
        {ok, ArchiveFileCtx} ->
            {FileSize, _} = file_ctx:get_file_size(ArchiveFileCtx),
            ok = archive:mark_file_archived(CurrentArchiveDoc, FileSize),
            {ok, ArchiveFileCtx};
        {error, _} = Error ->
            archive:mark_file_failed(CurrentArchiveDoc),
            Error
    end.


-spec archive_file(file_ctx:ctx(), file_ctx:ctx(), archive:doc(), archive:doc() | undefined, user_ctx:ctx()) ->
    {ok, file_ctx:ctx()} | {error, term()}.
archive_file(FileCtx, TargetParentCtx, CurrentArchiveDoc, BaseArchiveDoc, UserCtx) ->
    case is_bagit(CurrentArchiveDoc) of
        false ->
            plain_archive:archive_file(CurrentArchiveDoc, FileCtx, TargetParentCtx, BaseArchiveDoc, UserCtx);
        true ->
            bagit_archive:archive_file(CurrentArchiveDoc, FileCtx, TargetParentCtx, BaseArchiveDoc, UserCtx)
    end.


-spec make_symlink(file_ctx:ctx(), file_ctx:ctx(), user_ctx:ctx()) -> ok.
make_symlink(TargetCtx, ParentCtx, UserCtx) ->
    SpaceId = file_ctx:get_space_id_const(TargetCtx),
    SpaceIdPrefix = ?SYMLINK_SPACE_ID_ABS_PATH_PREFIX(SpaceId),
    {FileName, TargetCtx2} = file_ctx:get_aliased_name(TargetCtx, UserCtx),
    {TargetCanonicalPath, _} = file_ctx:get_canonical_path(TargetCtx2),
    [_Sep, _SpaceId | Rest] = filename:split(TargetCanonicalPath),
    SymlinkValue = filename:join([SpaceIdPrefix | Rest]),
    ParentGuid = file_ctx:get_logical_guid_const(ParentCtx),
    {ok, _} = lfm:make_symlink(user_ctx:get_session_id(UserCtx), ?FILE_REF(ParentGuid), FileName, SymlinkValue),
    ok.


-spec is_bagit(archive:doc()) -> boolean().
is_bagit(ArchiveDoc) ->
    {ok, Config} = archive:get_config(ArchiveDoc),
    archive_config:get_layout(Config) =:= ?ARCHIVE_BAGIT_LAYOUT.