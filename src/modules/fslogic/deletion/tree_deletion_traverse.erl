%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements traverse_behaviour.
%%% It is used to traverse tree rooted in filed associated with
%%% passed RootCtx and deletes whole tree.
%%% @end
%%%-------------------------------------------------------------------
-module(tree_deletion_traverse).
-author("Jakub Kudzia").

-behavior(traverse_behaviour).

-include("global_definitions.hrl").
-include("tree_traverse.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([init_pool/0, stop_pool/0, start/4]).

%% Traverse behaviour callbacks
-export([
    task_started/2,
    task_canceled/2,
    task_finished/2,
    get_job/1,
    update_job_progress/5,
    do_master_job/2,
    do_slave_job/2
]).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).

-type id() :: tree_traverse:id().
%@formatter:off
-type info() :: #{
    root_guid := file_id:file_guid(),
    emit_events := boolean(),
    root_original_parent_uuid := file_meta:uuid()
}.
%@formatter:on

-export_type([id/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init_pool() -> ok.
init_pool() ->
    MasterJobsLimit = application:get_env(?APP_NAME, tree_deletion_traverse_master_jobs_limit, 10),
    SlaveJobsLimit = application:get_env(?APP_NAME, tree_deletion_traverse_slave_jobs_limit, 10),
    ParallelismLimit = application:get_env(?APP_NAME, tree_deletion_traverse_parallelism_limit, 10),
    tree_traverse:init(?POOL_NAME, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit).


-spec stop_pool() -> ok.
stop_pool() ->
    tree_traverse:stop(?POOL_NAME).


-spec start(file_ctx:ctx(), user_ctx:ctx(), boolean(), file_meta:uuid()) -> {ok, id()} | {error, term()}.
start(RootDirCtx, UserCtx, EmitEvents, RootOriginalParentUuid) ->
    TaskId = datastore_key:new(),
    Options = #{
        task_id => TaskId,
        track_subtree_status => true,
        children_master_jobs_mode => async,
        use_listing_token => false,
        traverse_info => #{
            root_guid => file_ctx:get_guid_const(RootDirCtx),
            emit_events => EmitEvents,
            % TODO VFS-7133 after extending file_meta with field for storing source parent
            % there will be no need to store below 2 values
            root_original_parent_uuid => RootOriginalParentUuid
        }
    },
    case tree_traverse_session:setup_for_task(UserCtx, TaskId) of
        ok ->
            tree_traverse:run(?POOL_NAME, RootDirCtx, user_ctx:get_user_id(UserCtx), Options);
        {error, _} = Error ->
            Error
    end.


%%%===================================================================
%%% Traverse callbacks
%%%===================================================================

-spec task_started(id(), tree_traverse:pool()) -> ok.
task_started(TaskId, _Pool) ->
    ?debug("dir deletion job ~p started", [TaskId]).

-spec task_canceled(traverse:id(), traverse:pool()) -> ok.
task_canceled(TaskId, _PoolName) ->
    tree_traverse_session:close_for_task(TaskId),
    ?debug("dir deletion job ~p cancelled", [TaskId]).

-spec task_finished(id(), tree_traverse:pool()) -> ok.
task_finished(TaskId, _Pool) ->
    tree_traverse_session:close_for_task(TaskId),
    ?debug("dir deletion job ~p finished", [TaskId]).

-spec get_job(traverse:job_id() | tree_traverse_job:doc()) ->
    {ok, tree_traverse:master_job(), traverse:pool(), id()}  | {error, term()}.
get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).

-spec update_job_progress(undefined | main_job | traverse:job_id(),
    tree_traverse:master_job(), traverse:pool(), id(),
    traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TaskId, Status, ?MODULE).


-spec do_master_job(tree_traverse:master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_master_job(Job = #tree_traverse{
    file_ctx = FileCtx,
    user_id = UserId,
    traverse_info = TraverseInfo
},
    MasterJobArgs = #{task_id := TaskId}
) ->
    BatchProcessingPrehook = fun(_SlaveJobs, _MasterJobs, _ListExtendedInfo, SubtreeProcessingStatus) ->
        delete_dir_if_subtree_processed(SubtreeProcessingStatus, FileCtx, UserId, TaskId, TraverseInfo)
    end,
    tree_traverse:do_master_job(Job, MasterJobArgs, BatchProcessingPrehook).


-spec do_slave_job(tree_traverse:slave_job(), id()) -> ok.
do_slave_job(#tree_traverse_slave{
    file_ctx = FileCtx,
    user_id = UserId,
    traverse_info = TraverseInfo
}, TaskId) ->
   delete_file(FileCtx, UserId, TaskId, TraverseInfo).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec delete_dir_if_subtree_processed(tree_traverse_progress:status(), file_ctx:ctx(), od_user:id(),
    id(), info()) -> ok.
delete_dir_if_subtree_processed(?SUBTREE_PROCESSED, FileCtx, UserId, TaskId, TraverseInfo) ->
    delete_dir(FileCtx, UserId, TaskId, TraverseInfo);
delete_dir_if_subtree_processed(?SUBTREE_NOT_PROCESSED, _FileCtx, _UserId, _TaskId, _TraverseInfo) ->
    ok.


%% @private
-spec delete_dir(file_ctx:ctx(), od_user:id(), id(), info()) -> ok.
delete_dir(FileCtx, UserId, TaskId, TraverseInfo = #{
    root_guid := RootGuid,
    emit_events := EmitEvents,
    root_original_parent_uuid := RootOriginalParentUuid
}) ->
    case tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId) of
        {ok, UserCtx} ->
            try
                % TODO VFS-7133 after extending file_meta with field for storing source parent
                % there will be no need to delete deletion_marker here, it may be deleted in fslogic_delete
                {IsStorageFileCreated, FileCtx2} = file_ctx:is_storage_file_created(FileCtx),
                % get StorageFileId before location is deleted as it's stored in dir_location doc
                {StorageFileId, FileCtx3} = file_ctx:get_storage_file_id(FileCtx2),
                delete_req:delete(UserCtx, FileCtx3, not EmitEvents),
                tree_traverse:delete_subtree_status_doc(TaskId, file_ctx:get_uuid_const(FileCtx3)),
                case file_ctx:get_guid_const(FileCtx3) =:= RootGuid of
                    true ->
                        case IsStorageFileCreated of
                            true -> deletion_marker:remove_by_name(RootOriginalParentUuid, filename:basename(StorageFileId));
                            false -> ok
                        end;
                    false ->
                        file_processed(FileCtx, UserCtx, TaskId, TraverseInfo)
                end
            catch
                throw:?EACCES ->
                    ok
            end;
        {error, ?EACCES} ->
            ok
    end.


-spec delete_file(file_ctx:ctx(), od_user:id(), id(), info()) -> ok.
delete_file(FileCtx, UserId, TaskId, TraverseInfo = #{emit_events := EmitEvents}) ->
    case tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId) of
        {ok, UserCtx} ->
            try
                delete_req:delete(UserCtx, FileCtx, not EmitEvents),
                file_processed(FileCtx, UserCtx, TaskId, TraverseInfo)
            catch
                throw:?EACCES ->
                    ok
            end;
        {error, ?EACCES} ->
            ok
    end.


%% @private
-spec file_processed(file_ctx:ctx(), user_ctx:ctx(), id(), info()) -> ok.
file_processed(FileCtx, UserCtx, TaskId, TraverseInfo = #{root_original_parent_uuid := RootOriginalParentUuid}) ->
    {ParentFileCtx, FileCtx1} = files_tree:get_parent(FileCtx, UserCtx),
    case file_qos:get_effective(RootOriginalParentUuid) of
        {ok, #effective_file_qos{qos_entries = EffectiveQosEntries}} ->
            SpaceId = file_ctx:get_space_id_const(FileCtx1),
            OriginalRootParentCtx = file_ctx:new_by_uuid(RootOriginalParentUuid, SpaceId),
            lists:foreach(fun(EffectiveQosEntryId) ->
                qos_status:report_file_deleted(FileCtx1, EffectiveQosEntryId, OriginalRootParentCtx)
            end, EffectiveQosEntries);
        _ ->
            ok
    end,
    ParentUuid = file_ctx:get_uuid_const(ParentFileCtx),
    ParentStatus = tree_traverse:report_child_processed(TaskId, ParentUuid),
    delete_dir_if_subtree_processed(ParentStatus, ParentFileCtx, user_ctx:get_user_id(UserCtx), TaskId, TraverseInfo).