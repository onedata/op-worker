%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains functions responsible for traversing tree and
%%% performing actions related to QoS management.
%%% Traverse is started for each storage that given QoS requires files
%%% to be. Traverse is run on provider, that given storage belongs to.
%%%
%%% @end
%%%--------------------------------------------------------------------
-module(qos_traverse).
-author("Michal Cwiertnia").

-behavior(traverse_behaviour).

-include("global_definitions.hrl").
-include("modules/datastore/qos.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("tree_traverse.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([reconcile_file_for_qos_entries/2, start_initial_traverse/3, report_entry_deleted/1]).
-export([init_pool/0, stop_pool/0]).

%% Traverse behaviour callbacks
-export([do_master_job/2, do_slave_job/2, task_finished/2, task_canceled/2, 
    get_job/1, update_job_progress/5]).

-type id() :: qos_traverse_req:id().

-export_type([id/0]).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(TRAVERSE_BATCH_SIZE, op_worker:get_env(qos_traverse_batch_size, 40)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates initial traverse task to fulfill requirements defined in qos_entry.
%% @end
%%--------------------------------------------------------------------
-spec start_initial_traverse(file_ctx:ctx(), qos_entry:id(), id()) -> ok.
start_initial_traverse(FileCtx, QosEntryId, TaskId) ->
    Options = #{
        task_id => TaskId,
        batch_size => ?TRAVERSE_BATCH_SIZE,
        children_master_jobs_mode => sync,
        additional_data => #{
            <<"qos_entry_id">> => QosEntryId,
            <<"space_id">> => file_ctx:get_space_id_const(FileCtx),
            <<"uuid">> => file_ctx:get_referenced_uuid_const(FileCtx),
            <<"task_type">> => <<"traverse">>
        }
    },
    {ok, FileCtx2} = qos_status:report_traverse_start(TaskId, FileCtx),
    {ok, _} = tree_traverse:run(?POOL_NAME, FileCtx2, Options),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Creates traverse task to fulfill requirements defined in qos_entries for
%% single file, after its change was synced.
%% @end
%%--------------------------------------------------------------------
-spec reconcile_file_for_qos_entries(file_ctx:ctx(), [qos_entry:id()]) -> ok.
reconcile_file_for_qos_entries(_FileCtx, []) ->
    ok;
reconcile_file_for_qos_entries(FileCtx, QosEntries) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    TaskId = datastore_key:new(),
    FileUuid = file_ctx:get_referenced_uuid_const(FileCtx),
    Options = #{
        task_id => TaskId,
        batch_size => ?TRAVERSE_BATCH_SIZE,
        children_master_jobs_mode => sync,
        additional_data => #{
            <<"space_id">> => SpaceId,
            <<"uuid">> => FileUuid,
            <<"task_type">> => <<"reconcile">>
        }
    },
    ok = qos_status:report_reconciliation_started(TaskId, FileCtx, QosEntries),
    {ok, _} = tree_traverse:run(?POOL_NAME, FileCtx, Options),
    ok.
    

-spec report_entry_deleted(qos_entry:id() | qos_entry:doc()) -> ok.
report_entry_deleted(QosEntryId) when is_binary(QosEntryId) ->
    case qos_entry:get(QosEntryId) of
        {ok, QosDoc} -> report_entry_deleted(QosDoc);
        ?ERROR_NOT_FOUND -> ok;
        {error, _} = Error -> 
            ?error("Error in qos_traverse:report_entry_deleted: ~p", [Error])
    end;
report_entry_deleted(#document{key = QosEntryId} = QosEntryDoc) ->
    ok = cancel_local_traverses(QosEntryDoc),
    ok = qos_entry:apply_to_all_transfers(QosEntryId, fun replica_synchronizer:cancel/1).


-spec init_pool() -> ok  | no_return().
init_pool() ->
    % Get pool limits from app.config
    MasterJobsLimit = op_worker:get_env(qos_traverse_master_jobs_limit, 10),
    SlaveJobsLimit = op_worker:get_env(qos_traverse_slave_jobs_limit, 20),
    ParallelismLimit = op_worker:get_env(qos_traverse_parallelism_limit, 20),

    tree_traverse:init(?MODULE, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit).


-spec stop_pool() -> ok.
stop_pool() ->
    tree_traverse:stop(?POOL_NAME).

%%%===================================================================
%%% Traverse callbacks
%%%===================================================================

-spec get_job(traverse:job_id() | tree_traverse_job:doc()) ->
    {ok, tree_traverse:master_job(), traverse:pool(), tree_traverse:id()}  | {error, term()}.
get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).

-spec task_finished(id(), traverse:pool()) -> ok.
task_finished(TaskId, _PoolName) ->
    {ok, #{
        <<"space_id">> := SpaceId,
        <<"uuid">> := FileUuid,
        <<"task_type">> := TaskType
    } = AdditionalData} = traverse_task:get_additional_data(?POOL_NAME, TaskId),
    FileCtx = file_ctx:new_by_uuid(FileUuid, SpaceId),
    case TaskType of
        <<"traverse">> ->
            #{<<"qos_entry_id">> := QosEntryId} = AdditionalData,
            ok = qos_entry:remove_traverse_req(QosEntryId, TaskId),
            ok = qos_status:report_traverse_finished(TaskId, FileCtx);
        <<"reconcile">> ->
            ok = qos_status:report_reconciliation_finished(TaskId, FileCtx)
    end.

-spec task_canceled(id(), traverse:pool()) -> ok.
task_canceled(TaskId, PoolName) ->
    task_finished(TaskId, PoolName).

-spec update_job_progress(undefined | main_job | traverse:job_id(),
    tree_traverse:master_job(), traverse:pool(), id(),
    traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TaskId, Status, ?MODULE).

-spec do_master_job(tree_traverse:master_job() | tree_traverse:slave_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_master_job(Job = #tree_traverse_slave{}, #{task_id := TaskId}) ->
    do_slave_job(Job, TaskId);
do_master_job(Job = #tree_traverse{file_ctx = FileCtx}, MasterJobArgs = #{task_id := TaskId}) ->
    BatchProcessingPrehook = fun(SlaveJobs, MasterJobs, ListExtendedInfo, _SubtreeProcessingStatus) ->
        ChildrenFiles = lists:map(fun(#tree_traverse_slave{file_ctx = ChildFileCtx}) ->
            file_ctx:get_logical_uuid_const(ChildFileCtx)
        end, SlaveJobs),
        ChildrenDirs = lists:map(fun(#tree_traverse{file_ctx = ChildDirCtx}) ->
            file_ctx:get_logical_uuid_const(ChildDirCtx)
        end, MasterJobs),
        BatchLastFilename = maps:get(last_name, ListExtendedInfo, undefined),
        ok = qos_status:report_next_traverse_batch(
            TaskId, FileCtx, ChildrenDirs, ChildrenFiles, BatchLastFilename),

        case maps:get(is_last, ListExtendedInfo) of
            true ->
                ok = qos_status:report_traverse_finished_for_dir(TaskId, FileCtx);
            false ->
                ok
        end

    end,
    tree_traverse:do_master_job(Job, MasterJobArgs, BatchProcessingPrehook).


%%--------------------------------------------------------------------
%% @doc
%% Performs slave job for traverse task responsible for scheduling replications
%% to fulfill QoS requirements.
%% @end
%%--------------------------------------------------------------------
-spec do_slave_job(tree_traverse:slave_job(), id()) -> ok.
do_slave_job(#tree_traverse_slave{file_ctx = FileCtx}, TaskId) ->
    % TODO VFS-6137: add space check and optionally choose other storage
    UserCtx = user_ctx:new(?ROOT_SESS_ID),

    {ok, #{<<"task_type">> := TaskType} = AdditionalData} = 
        traverse_task:get_additional_data(?POOL_NAME, TaskId),
    
    case TaskType of
        <<"traverse">> -> 
            slave_job_traverse(TaskId, UserCtx, FileCtx, AdditionalData);
        <<"reconcile">> ->
            slave_job_reconcile(TaskId, UserCtx, FileCtx)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec slave_job_traverse(id(), user_ctx:ctx(), file_ctx:ctx(), traverse:additional_data()) -> ok.
slave_job_traverse(TaskId, UserCtx, FileCtx, AdditionalData) ->
    #{<<"qos_entry_id">> := QosEntryId} = AdditionalData,
    % start transfer only for existing entries
    QosEntries = case qos_entry:get(QosEntryId) of
        {ok, _} -> [QosEntryId];
        ?ERROR_NOT_FOUND -> []
    end,
    ok = synchronize_file_for_entries(TaskId, UserCtx, FileCtx, QosEntries),
    {ParentFileCtx, FileCtx2} = files_tree:get_parent(FileCtx, undefined),
    ok = qos_status:report_traverse_finished_for_file(TaskId, FileCtx2, ParentFileCtx).


%% @private
-spec slave_job_reconcile(id(), user_ctx:ctx(), file_ctx:ctx()) -> ok.
slave_job_reconcile(TaskId, UserCtx, FileCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {FileDoc, FileCtx1} = file_ctx:get_file_doc(FileCtx),
    case file_qos:get_effective(FileDoc) of
        undefined -> ok;
        {ok, EffectiveFileQos} ->
            QosEntries = case file_qos:is_in_trash(EffectiveFileQos) of
                true ->
                    [];
                false ->
                    % start transfer only for existing entries
                    {ok, [StorageId | _]} = space_logic:get_local_storages(SpaceId),
                    file_qos:get_assigned_entries_for_storage(EffectiveFileQos, StorageId)
            end,
            ok = synchronize_file_for_entries(TaskId, UserCtx, FileCtx1, QosEntries)
    end.


%% @private
-spec synchronize_file_for_entries(id(), user_ctx:ctx(), file_ctx:ctx(), [qos_entry:id()]) -> 
    ok.
synchronize_file_for_entries(_TaskId, _UserCtx, _FileCtx, []) -> 
    ok;
synchronize_file_for_entries(TaskId, UserCtx, FileCtx, QosEntries) ->
    {Size, FileCtx2} = file_ctx:get_file_size(FileCtx),
    FileBlock = #file_block{offset = 0, size = Size},
    Uuid = file_ctx:get_logical_uuid_const(FileCtx),
    TransferId = datastore_key:new_from_digest([TaskId, Uuid]),
    IsSymlink = fslogic_uuid:is_symlink_uuid(Uuid),
    
    lists:foreach(fun(QosEntry) -> 
        qos_entry:add_transfer_to_list(QosEntry, TransferId) 
    end, QosEntries),
    SyncResult = case IsSymlink of
        true -> 
            ok;
        false ->
            Res = replica_synchronizer:synchronize(UserCtx, FileCtx2, FileBlock, 
                false, TransferId, ?QOS_SYNCHRONIZATION_PRIORITY),
            case file_popularity:increment_open(FileCtx) of
                ok -> ok;
                {error, not_found} -> ok
            end,
            Res
    end,
    lists:foreach(fun(QosEntry) ->
        qos_entry:remove_transfer_from_list(QosEntry, TransferId)
    end, QosEntries),
    
    case SyncResult of
        ok -> ok;
        {ok, _} -> ok;
        {error, cancelled} -> 
            ?debug("QoS file synchronization failed due to cancellation");
        {error, _} = Error ->
            qos_status:report_file_transfer_failure(FileCtx2, QosEntries),
            ?error("Error during QoS file synchronization: ~p", [Error])
    end.


%% @private
-spec cancel_local_traverses(qos_entry:doc()) -> ok.
cancel_local_traverses(QosEntryDoc) ->
    {ok, TraverseReqs} = qos_entry:get_traverse_reqs(QosEntryDoc),
    {LocalTraverseIds, _} = qos_traverse_req:split_local_and_remote(TraverseReqs),
    lists:foreach(fun(TaskId) ->
        case tree_traverse:cancel(?POOL_NAME, TaskId) of
            ok -> ok;
            Error -> ?error("Error when cancelling traverse: ~p", [Error])
        end
    end, LocalTraverseIds).
