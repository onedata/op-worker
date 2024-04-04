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
-export([start/3, report_entry_deleted/1]).
-export([init_pool/0, stop_pool/0]).
-export([pool_name/0]).

%% Traverse behaviour callbacks
-export([do_master_job/2, do_slave_job/2, task_finished/2, task_canceled/2, 
    get_job/1, update_job_progress/5]).

%% transfer_stats_callback_behaviour
-export([flush_stats/3]).

-type id() :: qos_traverse_req:id().
-type slave_job() :: tree_traverse:slave_job().
-type transfer_id() :: binary().

-export_type([id/0]).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(TRAVERSE_BATCH_SIZE, op_worker:get_env(qos_traverse_batch_size)).

-define(SEPARATOR, <<"#">>).
-define(QOS_TRANSFER_ID(TaskId, FileUuid), <<TaskId/binary, (?SEPARATOR)/binary, FileUuid/binary>>).

% Due to traverse_task only using few first bytes of id in combination with timestamp in seconds there is
% quite a big chance of conflicts when creating new traverse.
% Therefore starting new traverse is repeated on already_exists error up to ?MAX_START_ALREADY_EXISTS_REPEATS,
% with each repeat taking 1 second to ensure new timestamp.
-define(MAX_START_ALREADY_EXISTS_REPEATS, 8).

-define(LISTING_ERRORS_REPEAT_TIMEOUT_SEC, op_worker:get_env(qos_listing_errors_repeat_timeout_sec)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates initial traverse task to fulfill requirements defined in qos_entry.
%% @end
%%--------------------------------------------------------------------
-spec start(file_ctx:ctx(), [qos_entry:id()], id()) -> ok.
start(FileCtx, QosEntries, TaskId) ->
    Options = #{
        task_id => TaskId,
        batch_size => ?TRAVERSE_BATCH_SIZE,
        children_master_jobs_mode => sync,
        listing_errors_handling_policy => propagate,
        additional_data => #{
            <<"encoded_qos_entries">> => json_utils:encode(QosEntries),
            <<"space_id">> => file_ctx:get_space_id_const(FileCtx),
            <<"uuid">> => file_ctx:get_referenced_uuid_const(FileCtx)
        }
    },
    {ok, FileCtx2} = qos_status:report_traverse_started(TaskId, FileCtx, QosEntries),
    start_internal(FileCtx2, Options, ?MAX_START_ALREADY_EXISTS_REPEATS).
    

-spec report_entry_deleted(qos_entry:id() | qos_entry:doc()) -> ok.
report_entry_deleted(QosEntryId) when is_binary(QosEntryId) ->
    ok = cancel_traverses(QosEntryId),
    ok = qos_entry:apply_to_all_transfers(QosEntryId, fun replica_synchronizer:cancel/1);
report_entry_deleted(#document{key = QosEntryId}) ->
    report_entry_deleted(QosEntryId).


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


-spec pool_name() -> traverse:pool().
pool_name() ->
    ?POOL_NAME.

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
        <<"uuid">> := FileUuid
    } = AdditionalData} = traverse_task:get_additional_data(?POOL_NAME, TaskId),
    FileCtx = file_ctx:new_by_uuid(FileUuid, SpaceId),
    qos_status:report_traverse_finished(TaskId, FileCtx, get_traverse_qos_entries(AdditionalData)).


-spec task_canceled(id(), traverse:pool()) -> ok.
task_canceled(TaskId, PoolName) ->
    % call with ?MODULE for mocking in tests
    ?MODULE:task_finished(TaskId, PoolName).


-spec update_job_progress(undefined | main_job | traverse:job_id(),
    tree_traverse:master_job(), traverse:pool(), id(),
    traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TaskId, Status, ?MODULE).


-spec do_master_job(tree_traverse:master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_master_job(Job = #tree_traverse{file_ctx = FileCtx}, MasterJobArgs = #{task_id := TaskId}) ->
    BatchProcessingPrehook = build_batch_processing_prehook(TaskId, FileCtx),
    %% @TODO VFS-10301 Optimize QoS by cutting unnecessary traverses if already traversed
    do_master_jobs_with_repeats(Job, MasterJobArgs, BatchProcessingPrehook, global_clock:timestamp_seconds()).


-spec do_slave_job(tree_traverse:slave_job(), id()) -> ok.
do_slave_job(#tree_traverse_slave{file_ctx = FileCtx} = Job, TaskId) ->
    % TODO VFS-6137: add space check and optionally choose other storage
    {ok, AdditionalData} = traverse_task:get_additional_data(?POOL_NAME, TaskId),
    
    % skip entries deleted after traverse start
    FinalQosEntries = lists:filter(fun(QosEntryId) ->
        case qos_entry:get(QosEntryId) of
            {ok, _} -> true;
            ?ERROR_NOT_FOUND -> false
        end
    end, get_traverse_qos_entries(AdditionalData)),
    ok = synchronize_file_for_entries(TaskId, Job, FinalQosEntries),
    {ParentFileCtx, FileCtx2} = file_tree:get_parent(FileCtx, undefined),
    ok = qos_status:report_traverse_finished_for_file(TaskId, FileCtx2, ParentFileCtx).


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
-spec start_internal(file_ctx:ctx(), tree_traverse:run_options(), non_neg_integer()) -> ok.
start_internal(_FileCtx, #{task_id := TaskId}, 0) ->
    ?error("Reached max repeats on QoS traverse task id conflict: ~tp", [TaskId]);
start_internal(FileCtx, #{task_id := TaskId} = Options, Repeats) ->
    try tree_traverse:run(?POOL_NAME, FileCtx, Options) of
        {ok, _} -> ok
    catch _:{badmatch, [{error,already_exists}]} ->
        timer:sleep(timer:seconds(1)), % sleep to ensure different timestamp for traverse link
        ?warning("Conflict on QoS traverse task id ~tp. Reapeting...", [TaskId]),
        start_internal(FileCtx, Options, Repeats - 1)
    end.


%% @private
-spec build_batch_processing_prehook(id(), file_ctx:ctx()) -> tree_traverse:new_jobs_preprocessor().
build_batch_processing_prehook(TaskId, FileCtx) ->
    fun(SlaveJobs, MasterJobs, ListingToken, _SubtreeProcessingStatus) ->
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
    end.


%% @private
-spec do_master_jobs_with_repeats(tree_traverse:master_job(), traverse:master_job_extended_args(),
    tree_traverse:new_jobs_preprocessor(), time:seconds()) -> {ok, traverse:master_job_map()}.
do_master_jobs_with_repeats(Job = #tree_traverse{file_ctx = FileCtx}, MasterJobArgs = #{task_id := TaskId},
    BatchProcessingPrehook, FirstListingTimestamp
) ->
    case tree_traverse:do_master_job(Job, MasterJobArgs, BatchProcessingPrehook) of
        {ok, _} = Result ->
            Result;
        {error, Reason, Stacktrace} ->
            case global_clock:timestamp_seconds() - FirstListingTimestamp > ?LISTING_ERRORS_REPEAT_TIMEOUT_SEC of
                true ->
                    {ok, AdditionalData} = traverse_task:get_additional_data(?POOL_NAME, TaskId),
                    ?error_exception(error, Reason, Stacktrace),
                    report_file_failed_for_entries(get_traverse_qos_entries(AdditionalData), FileCtx, {error, Reason}),
                    {ok, #{}};
                false ->
                    timer:sleep(timer:seconds(1)),
                    do_master_jobs_with_repeats(Job, MasterJobArgs, BatchProcessingPrehook, FirstListingTimestamp)
            end
    end.


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
-spec synchronize_file_for_entries(id(), slave_job(), [qos_entry:id()]) ->
    ok.
synchronize_file_for_entries(_TaskId, _Job, []) ->
    ok;
synchronize_file_for_entries(TaskId, #tree_traverse_slave{file_ctx = FileCtx} = Job, QosEntries) ->
    try
        synchronize_file_for_entries_insecure(TaskId, Job, QosEntries)
    catch Class:Reason:Stacktrace ->
        ?error_exception("Unexpected error during QoS synchronization for file ~tp",
            [file_ctx:get_logical_uuid_const(FileCtx)], Class, Reason, Stacktrace),
        ok = report_file_failed_for_entries(QosEntries, FileCtx, Reason)
    end.


%% @private
-spec synchronize_file_for_entries_insecure(id(), slave_job(), [qos_entry:id()]) -> ok.
synchronize_file_for_entries_insecure(TaskId, #tree_traverse_slave{file_ctx = FileCtx}, QosEntries) ->
    Uuid = file_ctx:get_logical_uuid_const(FileCtx),
    TransferId = ?QOS_TRANSFER_ID(TaskId, Uuid),
    
    lists:foreach(fun(QosEntry) -> 
        qos_entry:add_transfer_to_list(QosEntry, TransferId) 
    end, QosEntries),
    ok = report_file_synchronization_started_for_entries(QosEntries, FileCtx),
    
    case do_synchronize_file(FileCtx, TransferId) of
        {ok, FileCtx2} ->
            ok = report_file_synchronized_for_entries(QosEntries, FileCtx2);
        {{error, cancelled}, _} ->
            % QoS entry was deleted, so there is no need to report to audit log
            ?debug("QoS file synchronization failed due to cancellation");
        {{error, _} = Error, FileCtx2} ->
            NormalizedError = normalize_error(Error),
            ?error("Error during QoS file synchronization: ~tp", [NormalizedError]),
            ok = report_file_failed_for_entries(QosEntries, FileCtx2, NormalizedError)
    end,
    
    lists:foreach(fun(QosEntry) ->
        qos_entry:remove_transfer_from_list(QosEntry, TransferId)
    end, QosEntries).


%% @private
-spec do_synchronize_file(file_ctx:ctx(), transfer:id()) -> {ok | {error, term()}, file_ctx:ctx()}.
do_synchronize_file(FileCtx, TransferId) ->
    case fslogic_file_id:is_symlink_uuid(file_ctx:get_logical_uuid_const(FileCtx)) of
        true -> {ok, FileCtx};
        false -> do_synchronize_reg_file(FileCtx, TransferId)
    end.


%% @private
-spec do_synchronize_reg_file(file_ctx:ctx(), transfer:id()) -> {ok | {error, term()}, file_ctx:ctx()}.
do_synchronize_reg_file(FileCtx, TransferId) ->
    ok = ?ok_if_not_found(file_popularity:increment_open(FileCtx)),
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    {Size, FileCtx2} = file_ctx:get_file_size(FileCtx),
    FileBlock = #file_block{offset = 0, size = Size},
    Res = ?extract_ok(replica_synchronizer:synchronize(UserCtx, FileCtx2, FileBlock,
        false, TransferId, ?QOS_SYNCHRONIZATION_PRIORITY, ?MODULE)),
    {Res, FileCtx2}.


%% @private
-spec report_file_synchronization_started_for_entries([qos_entry:id()], file_ctx:ctx()) -> ok.
report_file_synchronization_started_for_entries(QosEntries, FileCtx) ->
    report_to_audit_log(
        QosEntries, FileCtx, [], fun qos_entry_audit_log:report_synchronization_started/3).


%% @private
-spec report_file_synchronized_for_entries([qos_entry:id()], file_ctx:ctx()) -> ok.
report_file_synchronized_for_entries(QosEntries, FileCtx) ->
    {StorageId, FileCtx2} = file_ctx:get_storage_id(FileCtx),
    report_transfer_stats(QosEntries, ?FILES_STATS, #{StorageId => 1}),
    report_to_audit_log(
        QosEntries, FileCtx2, [], fun qos_entry_audit_log:report_file_synchronized/3).


%% @private
-spec report_file_failed_for_entries([qos_entry:id()], file_ctx:ctx(), {error, term()}) -> ok.
report_file_failed_for_entries(QosEntries, FileCtx, Error) ->
    ok = qos_status:report_file_transfer_failure(FileCtx, QosEntries),
    report_to_audit_log(
        QosEntries, FileCtx, [Error], fun qos_entry_audit_log:report_file_synchronization_failed/4).


%% @private
-spec report_to_audit_log([qos_entry:id()], file_ctx:ctx(), [term()], fun((...) -> ok)) -> ok.
report_to_audit_log(QosEntries, FileCtx, Args, ReportFun) ->
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    {LogicalPath, _} = file_ctx:get_logical_path(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
    lists:foreach(fun(QosEntryId) ->
        % ignore not found errors, as audit log could have been deleted along with QoS entry
        ok = ?ok_if_not_found(erlang:apply(ReportFun, [QosEntryId, FileGuid, LogicalPath | Args]))
    end, QosEntries).


%% @private
-spec report_transfer_stats([qos_entry:id()], qos_transfer_stats:type(), 
    #{od_storage:id() => non_neg_integer()}) -> ok.
report_transfer_stats(QosEntries, Type, ValuesPerStorage) ->
    lists:foreach(fun(QosEntryId) ->
        ok = qos_transfer_stats:report(QosEntryId, Type, ValuesPerStorage)
    end, QosEntries).


%% @private
-spec cancel_traverses(qos_entry:id()) -> ok.
cancel_traverses(QosEntryId) ->
    qos_entry:fold_traverses(QosEntryId, fun({TaskId, _TraverseRootUuid}, _Acc) ->
        case ?ok_if_not_found(tree_traverse:cancel(?POOL_NAME, TaskId)) of
            ok -> ok;
            Error -> ?error("Error when cancelling traverse: ~tp", [Error])
        end
    end, ok).


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
normalize_error({error, {connection,<<"No such file or directory">>}}) ->
    ?ERROR_NOT_FOUND;
normalize_error(Error) ->
    Error.


-spec get_traverse_qos_entries(traverse:additional_data()) -> [qos_entry:id()].
get_traverse_qos_entries(#{<<"encoded_qos_entries">> := EncodeQosEntries}) ->
    json_utils:decode(EncodeQosEntries);
get_traverse_qos_entries(#{<<"qos_entry_id">> := QosEntryId}) ->
    % can happen when traverse was started before upgrade
    [QosEntryId];
get_traverse_qos_entries(_) ->
    % can happen when traverse was started before upgrade
    [].
