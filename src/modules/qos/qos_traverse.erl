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
-behaviour(transfer_stats_callback_behaviour).

-include("global_definitions.hrl").
-include("modules/datastore/datastore_runner.hrl").
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

%% transfer_stats_callback_behaviour
-export([flush_stats/3]).

-type id() :: qos_traverse_req:id().
-type transfer_id() :: binary().

-export_type([id/0]).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(TRAVERSE_BATCH_SIZE, op_worker:get_env(qos_traverse_batch_size, 40)).

-define(SEPARATOR, <<"#">>).
-define(QOS_TRANSFER_ID(TaskId, FileUuid), <<TaskId/binary, (?SEPARATOR)/binary, FileUuid/binary>>).

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

    % set parallelism limit equal to master jobs limit
    tree_traverse:init(?MODULE, MasterJobsLimit, SlaveJobsLimit, MasterJobsLimit).


-spec stop_pool() -> ok.
stop_pool() ->
    tree_traverse:stop(?POOL_NAME).

%%%===================================================================
%%% Traverse callbacks
%%%===================================================================

-spec get_job(traverse:job_id()) ->
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
    BatchProcessingPrehook = fun(SlaveJobs, MasterJobs, ListingToken, _SubtreeProcessingStatus) ->
        ChildrenFiles = lists:map(fun(#tree_traverse_slave{file_ctx = ChildFileCtx}) ->
            file_ctx:get_logical_uuid_const(ChildFileCtx)
        end, SlaveJobs),
        ChildrenDirs = lists:map(fun(#tree_traverse{file_ctx = ChildDirCtx}) ->
            file_ctx:get_logical_uuid_const(ChildDirCtx)
        end, MasterJobs),
        ok = qos_status:report_next_traverse_batch(
            TaskId, FileCtx, ChildrenDirs, ChildrenFiles, file_listing:get_last_listed_filename(ListingToken)),

        case file_listing:is_finished(ListingToken) of
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
%%% transfer_stats_callback_behaviour
%%%===================================================================

-spec flush_stats(od_space:id(), transfer_id(), #{od_provider:id() => non_neg_integer()}) ->
    ok | {error, term()}.
flush_stats(SpaceId, TransferId, BytesPerProvider) ->
    case transfer_id_to_file_uuid(TransferId) of
        {ok, FileUuid} ->       
            QosEntries = get_file_local_qos_entries(SpaceId, FileUuid),
            BytesPerStorage = maps:fold(fun(ProviderId, Value, AccMap) ->
                {ok, StoragesMap} = space_logic:get_provider_storages(SpaceId, ProviderId),
                %% @TODO VFS-5497 No longer true after allowing to support one space with many storages on one provider
                [StorageId | _] = maps:keys(StoragesMap),
                AccMap#{StorageId => Value}
            end, #{}, BytesPerProvider),
            report_transfer_stats(QosEntries, ?BYTES_STATS, BytesPerStorage);
        error ->
            % File UUID of legacy transfer cannot be retrieved, ignore such stats
            ok
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec get_file_local_qos_entries(od_space:id(), file_meta:uuid() | file_meta:doc()) -> 
    [qos_entry:id()].
get_file_local_qos_entries(SpaceId, FileUuidOrDoc) ->
    case file_qos:get_effective(FileUuidOrDoc) of
        {ok, EffectiveFileQos} ->
            case file_qos:is_in_trash(EffectiveFileQos) of
                true ->
                    [];
                false ->
                    {ok, [StorageId | _]} = space_logic:get_local_storages(SpaceId),
                    file_qos:get_assigned_entries_for_storage(EffectiveFileQos, StorageId)
            end;
        _ -> 
            []
    end.

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
    {ParentFileCtx, FileCtx2} = file_tree:get_parent(FileCtx, undefined),
    ok = qos_status:report_traverse_finished_for_file(TaskId, FileCtx2, ParentFileCtx).


%% @private
-spec slave_job_reconcile(id(), user_ctx:ctx(), file_ctx:ctx()) -> ok.
slave_job_reconcile(TaskId, UserCtx, FileCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {FileDoc, FileCtx1} = file_ctx:get_file_doc(FileCtx),
    QosEntries = get_file_local_qos_entries(SpaceId, FileDoc),
    ok = synchronize_file_for_entries(TaskId, UserCtx, FileCtx1, QosEntries).


%% @private
-spec synchronize_file_for_entries(id(), user_ctx:ctx(), file_ctx:ctx(), [qos_entry:id()]) ->
    ok.
synchronize_file_for_entries(_TaskId, _UserCtx, _FileCtx, []) ->
    ok;
synchronize_file_for_entries(TaskId, UserCtx, FileCtx, QosEntries) ->
    try
        synchronize_file_for_entries_insecure(TaskId, UserCtx, FileCtx, QosEntries)
    catch Class:Reason:Stacktrace ->
        ?error_stacktrace("Unexpected error during QoS synchronization for file ~p: ~p", 
            [file_ctx:get_logical_uuid_const(FileCtx), {Class, Reason}], Stacktrace),
        ok = qos_status:report_file_transfer_failure(FileCtx, QosEntries),
        ok = report_file_failed_for_entries(QosEntries, FileCtx, Reason)
    end.


%% @private
-spec synchronize_file_for_entries_insecure(id(), user_ctx:ctx(), file_ctx:ctx(), [qos_entry:id()]) -> 
    ok.
synchronize_file_for_entries_insecure(TaskId, UserCtx, FileCtx, QosEntries) ->
    {Size, FileCtx2} = file_ctx:get_file_size(FileCtx),
    FileBlock = #file_block{offset = 0, size = Size},
    Uuid = file_ctx:get_logical_uuid_const(FileCtx),
    TransferId = ?QOS_TRANSFER_ID(TaskId, Uuid),
    IsSymlink = fslogic_file_id:is_symlink_uuid(Uuid),
    
    lists:foreach(fun(QosEntry) -> 
        qos_entry:add_transfer_to_list(QosEntry, TransferId) 
    end, QosEntries),
    ok = report_file_synchronization_started_for_entries(QosEntries, FileCtx),
    SyncResult = case IsSymlink of
        true -> 
            ok;
        false ->
            Res = replica_synchronizer:synchronize(UserCtx, FileCtx2, FileBlock, 
                false, TransferId, ?QOS_SYNCHRONIZATION_PRIORITY, ?MODULE),
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
        ok ->
            ok = report_file_synchronized_for_entries(QosEntries, FileCtx2);
        {ok, _} -> 
            ok = report_file_synchronized_for_entries(QosEntries, FileCtx2);
        {error, cancelled} -> 
            % QoS entry was deleted, so there is no need to report to audit log
            ?debug("QoS file synchronization failed due to cancellation");
        {error, _} = Error ->
            NormalizedError = normalize_error(Error),
            ok = report_file_failed_for_entries(QosEntries, FileCtx2, NormalizedError),
            ok = qos_status:report_file_transfer_failure(FileCtx2, QosEntries),
            ?error("Error during QoS file synchronization: ~p", [NormalizedError])
    end.


%% @private
-spec report_file_synchronization_started_for_entries([qos_entry:id()], file_ctx:ctx()) -> ok.
report_file_synchronization_started_for_entries(QosEntries, FileCtx) ->
    report_to_audit_log(
        QosEntries, FileCtx, [], fun qos_entry_audit_log:report_synchronization_started/2).


%% @private
-spec report_file_synchronized_for_entries([qos_entry:id()], file_ctx:ctx()) -> ok.
report_file_synchronized_for_entries(QosEntries, FileCtx) ->
    {StorageId, FileCtx2} = file_ctx:get_storage_id(FileCtx),
    report_transfer_stats(QosEntries, ?FILES_STATS, #{StorageId => 1}),
    report_to_audit_log(
        QosEntries, FileCtx2, [], fun qos_entry_audit_log:report_file_synchronized/2).


%% @private
-spec report_file_failed_for_entries([qos_entry:id()], file_ctx:ctx(), {error, term()}) -> ok.
report_file_failed_for_entries(QosEntries, FileCtx, Error) ->
    report_to_audit_log(
        QosEntries, FileCtx, [Error], fun qos_entry_audit_log:report_file_synchronization_failed/3).


%% @private
-spec report_to_audit_log([qos_entry:id()], file_ctx:ctx(), [term()], fun((...) -> ok)) -> ok.
report_to_audit_log(QosEntries, FileCtx, Args, ReportFun) ->
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    lists:foreach(fun(QosEntryId) ->
        % ignore not found errors, as audit log could have been deleted along with QoS entry
        ok = ?ok_if_not_found(erlang:apply(ReportFun, [QosEntryId, FileGuid | Args]))
    end, QosEntries).


%% @private
-spec report_transfer_stats([qos_entry:id()], qos_transfer_stats:type(), 
    #{od_storage:id() => non_neg_integer()}) -> ok.
report_transfer_stats(QosEntries, Type, ValuesPerStorage) ->
    lists:foreach(fun(QosEntryId) ->
        ok = qos_transfer_stats:report(QosEntryId, Type, ValuesPerStorage)
    end, QosEntries).


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


%% @private
-spec transfer_id_to_file_uuid(transfer_id()) -> {ok, file_meta:uuid()} | error.
transfer_id_to_file_uuid(TransferId) ->
    case binary:split(TransferId, ?SEPARATOR) of
        [_TaskId, FileUuid] -> {ok, FileUuid};
        _ -> error
    end.


%% @private
-spec normalize_error({error, any()}) -> errors:error().
normalize_error({error, <<"quota exceeded">>}) ->
    ?ERROR_QUOTA_EXCEEDED;
normalize_error(Error) ->
    Error.

