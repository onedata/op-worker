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
%%% When archive is configured to include a DIP archive this traverse 
%%% can create 2 archives in parallel (AIP, which is created always, 
%%% and optionally DIP).
%%% TaskId of a traverse is the same as the id of the initial AIP archive.
%%% When an archive is cancelled its related archive (DIP for AIP and AIP for DIP) is 
%%% cancelled as well. In case of nested archives, if one was already started, then 
%%% it is NOT cancelled.
%%% @end
%%%-------------------------------------------------------------------
-module(archivisation_traverse).
-author("Jakub Kudzia").

-behavior(traverse_behaviour).

-include("tree_traverse.hrl").
-include("modules/dataset/archive.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([init_pool/0, stop_pool/0, start/3]).

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

-export([
    do_slave_job_unsafe/3,
    do_dir_master_job_unsafe/2
]).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).

-type id() :: archive:id().

-type docs_map() :: #{
    aip := archive:doc(),
    dip := archive:doc()
}.

%% @formatter:off
-type info() :: #{
    % incremental archive base for current_archive_doc 
    base_archive_doc := archive:doc() | undefined,
    % incremental archive base for top archive, the one created from dataset on which archivisation was scheduled
    scheduled_dataset_base_archive_doc := archive:doc() | undefined,
    scheduled_dataset_root_guid := file_id:file_guid(),
    initial_archive_docs := docs_map(), 
    aip_ctx := archivisation_traverse_ctx:ctx(),
    dip_ctx := archivisation_traverse_ctx:ctx()
}.
%% @formatter:on

-export_type([id/0, info/0, docs_map/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init_pool() -> ok.
init_pool() ->
    MasterJobsLimit = op_worker:get_env(archivisation_traverse_master_jobs_limit, 10),
    SlaveJobsLimit = op_worker:get_env(archivisation_traverse_slave_jobs_limit, 20),
    ParallelismLimit = op_worker:get_env(archivisation_traverse_parallelism_limit, 10),
    tree_traverse:init(?POOL_NAME, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit),
    archive_verification_traverse:init_pool(),
    archive_recall_traverse:init_pool().


-spec stop_pool() -> ok.
stop_pool() ->
    tree_traverse:stop(?POOL_NAME),
    archive_verification_traverse:stop_pool(),
    archive_recall_traverse:stop_pool().


-spec start(archive:doc(), dataset:doc(), user_ctx:ctx()) -> ok | {error, term()}.
start(ArchiveDoc, DatasetDoc, UserCtx) ->
    {ok, ArchiveId} = archive:get_id(ArchiveDoc),
    case tree_traverse_session:setup_for_task(UserCtx, ArchiveId) of
        ok ->
            try
            UserId = user_ctx:get_user_id(UserCtx),
            DatasetRootCtx = dataset_api:get_associated_file_ctx(DatasetDoc),
            {Options, DatasetRootCtx2} = build_traverse_opts(ArchiveDoc, DatasetRootCtx, UserCtx),
            {ok, _} = tree_traverse:run(?POOL_NAME, DatasetRootCtx2, UserId, Options),
            ok
            catch Class:Reason:Stacktrace ->
                ?error_stacktrace("Unexpected error when trying to setup archive: ~p: ~p", [Class, Reason], Stacktrace),
                {error, datastore_runner:normalize_error(Reason)}
            end;
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
            archivisation_callback:notify_preservation_failed(
                ArchiveId, DatasetId, CallbackUrlOrUndefined,
                ErrorDescription)
    end.


-spec get_sync_info(tree_traverse:master_job()) -> {ok, traverse:sync_info()}.
get_sync_info(Job) ->
    tree_traverse:get_sync_info(Job).


-spec get_job(traverse:job_id()) ->
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
do_master_job(InitialJob, MasterJobArgs = #{task_id := TaskId}) ->
    archivisation_traverse_logic:mark_building_if_first_job(InitialJob),
    ErrorHandler = fun(Job, Reason, Stacktrace) ->
        report_error(TaskId, #tree_traverse{user_id = UserId} = Job, Reason, Stacktrace),
        {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
        do_aborted_master_job(Job, MasterJobArgs, UserCtx, {failed, Reason})
    end,
    archive_traverses_common:do_master_job(?MODULE, InitialJob, MasterJobArgs, ErrorHandler).


-spec do_slave_job(tree_traverse:slave_job(), id()) -> ok.
do_slave_job(InitialJob, TaskId) ->
    StartTimestamp = global_clock:timestamp_millis(),
    ErrorHandler = fun(Job, Reason, Stacktrace) ->
        #tree_traverse_slave{
            file_ctx = FileCtx, 
            user_id = UserId, 
            traverse_info = TraverseInfo, 
            master_job_uuid = MasterJobUuid,
            relative_path = RelativePath
        } = Job,
        report_error(TaskId, Job, Reason, Stacktrace),
        {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
        archivisation_traverse_logic:mark_finished_and_propagate_up(
            FileCtx, UserCtx, TraverseInfo, TaskId, MasterJobUuid, StartTimestamp, RelativePath, {failed, Reason})
    end,
    archive_traverses_common:execute_unsafe_job(
        ?MODULE, do_slave_job_unsafe, [TaskId, StartTimestamp], InitialJob, ErrorHandler).

%%%===================================================================
%%% Internal traverse functions
%%%===================================================================

-spec do_dir_master_job_unsafe(tree_traverse:master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_dir_master_job_unsafe(Job = #tree_traverse{
    user_id = UserId,
    file_ctx = FileCtx,
    traverse_info = #{aip_ctx := AipCtx} = TraverseInfo,
    pagination_token = PaginationToken,
    relative_path = RelativePath
},
    MasterJobArgs = #{task_id := TaskId}
) ->
    {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
    case archive_traverses_common:is_cancelling(AipCtx) of
        true ->
            do_aborted_master_job(Job, MasterJobArgs, UserCtx, completed);
        false ->
            IsFirstBatch = PaginationToken =:= undefined,
            TraverseInfo2 = case IsFirstBatch of
                true -> archivisation_traverse_logic:handle_file(
                    FileCtx, RelativePath, UserCtx, TraverseInfo);
                false -> TraverseInfo
            end,
            
            Job2 = Job#tree_traverse{traverse_info = TraverseInfo2},
            tree_traverse:do_master_job(
                Job2,
                MasterJobArgs,
                build_new_jobs_preprocessor_fun(TaskId, FileCtx, TraverseInfo2, UserCtx, RelativePath)
            )
    end.


-spec do_slave_job_unsafe(tree_traverse:slave_job(), id(), time:millis()) -> ok.
do_slave_job_unsafe(#tree_traverse_slave{
    user_id = UserId,
    file_ctx = FileCtx,
    master_job_uuid = MasterJobUuid,
    traverse_info = #{aip_ctx := AipCtx} = TraverseInfo,
    relative_path = RelativePath
}, TaskId, StartTimestamp) ->
    {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
    TraverseInfo2 = case archive_traverses_common:is_cancelling(AipCtx) of
        true -> TraverseInfo;
        false -> archivisation_traverse_logic:handle_file(
            FileCtx, RelativePath, UserCtx, TraverseInfo)
    end,
    archivisation_traverse_logic:mark_finished_and_propagate_up(
        FileCtx, UserCtx, TraverseInfo2, TaskId, MasterJobUuid, StartTimestamp, RelativePath, completed).


-spec do_aborted_master_job(tree_traverse:master_job(), traverse:master_job_extended_args(), 
    user_ctx:ctx(), completed | {failed, Reason :: any()}) -> {ok, traverse:master_job_map()}.
do_aborted_master_job(
    Job = #tree_traverse{file_ctx = FileCtx, traverse_info = TraverseInfo, relative_path = RelativePath},
    MasterJobArgs = #{task_id := TaskId},
    UserCtx,
    Status
) ->
    case tree_traverse:do_aborted_master_job(Job, MasterJobArgs) of
        {ok, MasterJobMap, undefined, _} ->
            {ok, MasterJobMap};
        {ok, MasterJobMap, NextSubtreeRoot, StartTimestamp} ->
            archivisation_traverse_logic:mark_finished_and_propagate_up(FileCtx, UserCtx, 
                TraverseInfo, TaskId, NextSubtreeRoot, StartTimestamp, RelativePath, Status),
            {ok, MasterJobMap}
    end.


-spec build_new_jobs_preprocessor_fun(id(), file_ctx:ctx(), info(), user_ctx:ctx(), file_meta:path()) ->
    tree_traverse:new_jobs_preprocessor().
build_new_jobs_preprocessor_fun(TaskId, FileCtx, TraverseInfo, UserCtx, RelativePath) ->
    fun(SlaveJobs, MasterJobs, ListingToken, SubtreeProcessingStatus) ->
        DirUuid = file_ctx:get_logical_uuid_const(FileCtx),
        ChildrenCount = length(SlaveJobs) + length(MasterJobs),
        ok = archive_traverses_common:update_children_count(
            ?POOL_NAME, TaskId, DirUuid, ChildrenCount),
        case file_listing:is_finished(ListingToken) of
            false -> ok;
            true -> save_dir_checksum(TaskId, DirUuid, UserCtx, TraverseInfo)
        end,
        case SubtreeProcessingStatus of
            ?SUBTREE_PROCESSED(NextSubtreeRoot, StartTimestamp) ->
                archivisation_traverse_logic:mark_finished_and_propagate_up(
                    FileCtx, UserCtx, TraverseInfo, TaskId, NextSubtreeRoot, StartTimestamp, RelativePath, completed);
            ?SUBTREE_NOT_PROCESSED ->
                ok
        end
    end.


%%%===================================================================
%%% Helper functions
%%%===================================================================

-spec report_error(id(), tree_traverse:job(), Reason :: any(), Stacktrace :: list()) -> ok.
report_error(TaskId, Job, Reason, Stacktrace) ->
    {ArchiveDocs, FileGuid} = job_to_error_info(Job),
    lists:foreach(fun(ArchiveDoc) ->
        {ok, ArchiveId} = archive:get_id(ArchiveDoc),
        archive:mark_file_failed(ArchiveDoc),
        ?error_stacktrace("Unexpected error during archivisation(~p) of file ~p in archive ~p:~n~p", 
            [TaskId, FileGuid, ArchiveId, Reason], Stacktrace)
    end, ArchiveDocs).


-spec build_traverse_opts(archive:doc(), file_ctx:ctx(), user_ctx:ctx()) ->
    {tree_traverse:run_options(), file_ctx:ctx()}.
build_traverse_opts(ArchiveDoc, DatasetRootCtx, UserCtx) ->
    {ok, ArchiveId} = archive:get_id(ArchiveDoc),
    {ok, Config} = archive:get_config(ArchiveDoc),
    
    {FileLogicalPath, DatasetRootCtx2} = file_ctx:get_logical_path(DatasetRootCtx, UserCtx),
    FollowSymlinks = archive_config:should_follow_symlinks(Config),
    StartFileCtx = case FollowSymlinks of
        true -> resolve_symlink(DatasetRootCtx2, UserCtx);
        false -> DatasetRootCtx2
    end,
    
    %% @TODO VFS-8882 - use preserve/follow_external in API
    SymlinksResolutionPolicy = case FollowSymlinks of
        true -> follow_external;
        false -> preserve
    end,
    
    {#{
        task_id => ArchiveId,
        track_subtree_status => true,
        children_master_jobs_mode => async,
        traverse_info => build_initial_traverse_info(
            ArchiveDoc, Config, file_ctx:get_logical_guid_const(StartFileCtx), UserCtx),
        symlink_resolution_policy => SymlinksResolutionPolicy,
        initial_relative_path => FileLogicalPath,
        additional_data => build_additional_data(ArchiveDoc)
    }, StartFileCtx}.


-spec build_initial_traverse_info(archive:doc(), archive:config(), file_id:file_guid(), user_ctx:ctx()) ->
    info().
build_initial_traverse_info(ArchiveDoc, Config, StartFileGuid, UserCtx) ->
    BaseArchiveDoc = get_base_archive_doc(ArchiveDoc, Config),
    AipCtx = init_archive(ArchiveDoc, UserCtx),
    {ok, DipArchiveId} = archive:get_related_dip_id(ArchiveDoc),
    DipCtx = init_archive(DipArchiveId, UserCtx),
    #{
        base_archive_doc => BaseArchiveDoc,
        scheduled_dataset_base_archive_doc => BaseArchiveDoc,
        scheduled_dataset_root_guid => StartFileGuid,
        initial_archive_docs => #{
            aip => archivisation_traverse_ctx:get_archive_doc(AipCtx),
            dip => archivisation_traverse_ctx:get_archive_doc(DipCtx)
        },
        aip_ctx => AipCtx,
        dip_ctx => DipCtx
    }.


-spec build_additional_data(archive:doc()) -> traverse:additional_data().
build_additional_data(ArchiveDoc) ->
    {ok, ArchiveId} = archive:get_id(ArchiveDoc),
    {ok, DatasetId} = archive:get_dataset_id(ArchiveDoc),
    AdditionalData = #{
        <<"archiveId">> => ArchiveId,
        <<"datasetId">> => DatasetId
    },
    {ok, CallbackOrUndefined} = archive:get_preserved_callback(ArchiveDoc),
    maps_utils:put_if_defined(AdditionalData, <<"callback">>, CallbackOrUndefined).


-spec save_dir_checksum(id(), file_meta:uuid(), user_ctx:ctx(), info()) -> ok.
save_dir_checksum(TaskId, DirUuid, UserCtx, TraverseInfo) ->
    TotalChildrenCount = archive_traverses_common:take_children_count(
        ?POOL_NAME, TaskId, DirUuid),
    archivisation_traverse_logic:save_dir_checksum_metadata(
        archivisation_traverse_ctx:get_target_parent(maps:get(aip_ctx, TraverseInfo)),
        UserCtx, TotalChildrenCount),
    archivisation_traverse_logic:save_dir_checksum_metadata(
        archivisation_traverse_ctx:get_target_parent(maps:get(dip_ctx, TraverseInfo)),
        UserCtx, TotalChildrenCount).


%% @TODO VFS-7923 Unify all symlinks resolution across op_worker
-spec resolve_symlink(file_ctx:ctx(), user_ctx:ctx()) -> file_ctx:ctx().
resolve_symlink(FileCtx, UserCtx) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    case file_ctx:is_symlink_const(FileCtx) of
        true ->
            {ok, ResolvedGuid} = lfm:resolve_symlink(SessionId, #file_ref{
                guid = file_ctx:get_logical_guid_const(FileCtx)
            }),
            file_ctx:new_by_guid(ResolvedGuid);
        _ ->
            FileCtx
    end.


-spec job_to_error_info(tree_traverse:job()) -> {[archive:doc()], file_id:file_guid()}.
job_to_error_info(#tree_traverse{traverse_info = TraverseInfo, file_ctx = FileCtx}) -> 
    {archivisation_traverse_logic:info_to_archive_docs(TraverseInfo), file_ctx:get_logical_guid_const(FileCtx)};
job_to_error_info(#tree_traverse_slave{traverse_info = TraverseInfo, file_ctx = FileCtx}) ->
    {archivisation_traverse_logic:info_to_archive_docs(TraverseInfo), file_ctx:get_logical_guid_const(FileCtx)}.


-spec get_base_archive_doc(archive:doc(), archive:config()) -> archive:doc() | undefined.
get_base_archive_doc(ArchiveDoc, Config) ->
    IsIncremental = archive_config:is_incremental(Config),
    {ok, BaseArchiveId} = archive:get_base_archive_id(ArchiveDoc),
    case IsIncremental andalso BaseArchiveId =/= undefined of
        true ->
            {ok, Doc} = archive:get(BaseArchiveId),
            Doc;
        false ->
            undefined
    end.


-spec init_archive(archive:doc() | archive:id(), user_ctx:ctx()) -> archivisation_traverse_ctx:ctx().
init_archive(undefined, _UserCtx) ->
    archivisation_traverse_ctx:init_for_new_traverse(undefined);
init_archive(ArchiveId, UserCtx) when is_binary(ArchiveId) ->
    {ok, ArchiveDoc} = archive:get(ArchiveId),
    init_archive(ArchiveDoc, UserCtx);
init_archive(ArchiveDoc, UserCtx) ->
    {ok, DatasetId} = archive:get_dataset_id(ArchiveDoc),
    {ok, ArchiveDoc2} = archivisation_traverse_logic:initialize_archive_dir(
        ArchiveDoc, DatasetId, UserCtx),
    archivisation_traverse_ctx:init_for_new_traverse(ArchiveDoc2).
