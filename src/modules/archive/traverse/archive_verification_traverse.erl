%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements traverse_behaviour. 
%%% It is responsible for verifying files in given archive by comparing 
%%% checksums calculated during archive creation with existing ones.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_verification_traverse).
-author("Michal Stanisz").

-behavior(traverse_behaviour).

-include("tree_traverse.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([init_pool/0, stop_pool/0, start/1
]).
-export([block_archive_modification/1, unblock_archive_modification/1]).

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

-type id() :: archive:id().

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


-spec start(archive:doc()) -> ok | {error, term()}.
start(ArchiveDoc) ->
    {ok, TaskId} = archive:get_id(ArchiveDoc),
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    {ok, RootFileGuid} = archive:get_data_dir_guid(ArchiveDoc),
    case tree_traverse_session:setup_for_task(UserCtx, TaskId) of
        ok ->
            UserId = user_ctx:get_user_id(UserCtx),
            Options = #{
                task_id => TaskId,
                children_master_jobs_mode => async
            },
            {ok, TaskId} = tree_traverse:run(
                ?POOL_NAME, file_ctx:new_by_guid(RootFileGuid), UserId, Options),
            ok;
        {error, _} = Error ->
            Error
    end.


-spec block_archive_modification(archive:doc()) -> ok.
block_archive_modification(#document{value = #archive{root_dir_guid = RootDirGuid}}) -> 
    FlagsToSet = ?set_flags(?DATA_PROTECTION, ?METADATA_PROTECTION),
    % as protection flags work only with datasets, create a dummy dataset on archive root dir
    ?extract_ok(dataset_api:establish(file_ctx:new_by_guid(RootDirGuid), FlagsToSet, internal)).


-spec unblock_archive_modification(archive:doc()) -> ok.
unblock_archive_modification(#document{value = #archive{root_dir_guid = RootDirGuid}}) -> 
    ?extract_ok(dataset_api:remove(file_id:guid_to_uuid(RootDirGuid))).


%%%===================================================================
%%% Traverse behaviour callbacks
%%%===================================================================

-spec task_started(id(), tree_traverse:pool()) -> ok.
task_started(TaskId, _Pool) ->
    ?debug("Archive verification job ~p started", [TaskId]).


-spec task_finished(id(), tree_traverse:pool()) -> ok.
task_finished(TaskId, _Pool) ->
    tree_traverse_session:close_for_task(TaskId),
    archive:mark_preserved(TaskId),
    ?debug("Archive verification job ~p finished", [TaskId]).


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
do_master_job(#tree_traverse{
    file_ctx = FileCtx, 
    user_id = UserId
} = Job, MasterJobArgs = #{task_id := TaskId}) ->
    {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),
    case IsDir of
        true ->
            NewJobsPreprocessor = fun(SlaveJobs, MasterJobs, #{is_last := IsLast}, _SubtreeProcessingStatus) ->
                DirUuid = file_ctx:get_logical_uuid_const(FileCtx2),
                ChildrenCount = length(SlaveJobs) + length(MasterJobs),
                ok = archive_traverse_common:update_children_count(
                    ?POOL_NAME, TaskId, DirUuid, ChildrenCount),
                case IsLast of
                    false ->
                        ok;
                    true ->
                        TotalChildrenCount = archive_traverse_common:take_children_count(
                            ?POOL_NAME, TaskId, DirUuid),
                        {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
                        case archivisation_checksum:has_dir_changed(
                            FileCtx2, FileCtx2, UserCtx, TotalChildrenCount) 
                        of
                            false -> 
                                ok;
                            true ->
                                archive:mark_verification_failed(TaskId),
                                tree_traverse:cancel(?POOL_NAME, TaskId),
                                ?warning("Invalid checksum for dir ~p in archive ~p",
                                    [file_ctx:get_logical_guid_const(FileCtx), TaskId])
                        end
                end
            end,
            tree_traverse:do_master_job(Job, MasterJobArgs, NewJobsPreprocessor);
        false ->
            tree_traverse:do_master_job(Job, MasterJobArgs)
    end.


-spec do_slave_job(tree_traverse:slave_job(), id()) -> ok.
do_slave_job(#tree_traverse_slave{
    file_ctx = FileCtx,
    user_id = UserId
}, TaskId) ->
    case file_ctx:is_symlink_const(FileCtx) of
        true ->
            ok;
        false ->
            {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
            case archivisation_checksum:has_file_changed(FileCtx, FileCtx, UserCtx) of
                false -> 
                    ok;
                _ ->
                    archive:mark_verification_failed(TaskId),
                    tree_traverse:cancel(?POOL_NAME, TaskId),
                    ?warning("Invalid checksum for file ~p in archive ~p",
                        [file_ctx:get_logical_guid_const(FileCtx), TaskId])
            end
    end.
