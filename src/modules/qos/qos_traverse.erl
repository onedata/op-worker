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
%%% @end
%%%--------------------------------------------------------------------
-module(qos_traverse).
-author("Michal Cwiertnia").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/qos.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([fulfill_qos/4, init_pool/0]).

%% Traverse behaviour callbacks
-export([do_master_job/2, do_slave_job/2,
    task_started/1, task_finished/1,
    get_job/1, get_sync_info/1, update_job_progress/5]).

% For test purpose
-export([schedule_transfers/4]).

-record(add_qos_traverse_args, {
    qos_id :: qos_entry:id(),
    file_path_tokens = [] :: [binary()],
    target_storages = undefined :: [storage:id()] | undefined
}).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(TRAVERSE_BATCH_SIZE, application:get_env(?APP_NAME, qos_traverse_batch_size, 40)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates traverse task to fulfill qos.
%% @end
%%--------------------------------------------------------------------
-spec fulfill_qos(file_ctx:ctx(), qos_entry:id(), [storage:id()] | undefined, traverse:id()) -> ok.
fulfill_qos(FileCtx, QosId, TargetStorages, TaskId) ->
    {ok, #document{value = QosItem, scope = SpaceId}} = qos_entry:get(QosId),
    QosOriginFileUuid = QosItem#qos_entry.file_uuid,
    QosOriginFileGuid = file_id:pack_guid(QosOriginFileUuid, SpaceId),
    {FilePathTokens, _FileCtx} = file_ctx:get_canonical_path_tokens(file_ctx:new_by_guid(QosOriginFileGuid)),
    Options = #{
        task_id => TaskId,
        batch_size => ?TRAVERSE_BATCH_SIZE,
        traverse_info => #add_qos_traverse_args{
            qos_id = QosId,
            file_path_tokens = FilePathTokens,
            target_storages = TargetStorages
        }
    },
    {ok, _} = tree_traverse:run(?POOL_NAME, FileCtx, Options),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Initializes pool for traverse tasks concerning qos management.
%% @end
%%--------------------------------------------------------------------
-spec init_pool() -> ok.
init_pool() ->
    % Get pool limits from app.config
    MasterJobsLimit = application:get_env(?APP_NAME, qos_taverse_master_jobs_limit, 10),
    SlaveJobsLimit = application:get_env(?APP_NAME, qos_taverse_slave_jobs_limit, 20),
    ParallelismLimit = application:get_env(?APP_NAME, qos_traverse_parallelism_limit, 20),

    tree_traverse:init(?MODULE, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit).

%%%===================================================================
%%% Traverse callbacks
%%%===================================================================

-spec get_job(traverse:job_id() | tree_traverse_job:doc()) ->
    {ok, tree_traverse:master_job(), traverse:pool(), traverse:id()}  | {error, term()}.
get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).

-spec get_sync_info(tree_traverse:master_job()) -> {ok, traverse:ctx_sync_info()}.
get_sync_info(Job) ->
    tree_traverse:get_sync_info(Job).

-spec task_started(traverse:id()) -> ok.
task_started(TaskId) ->
    [_, SpaceId, QosId, TaskType] = binary:split(TaskId, <<"#">>, [global]),
    case binary_to_atom(TaskType, utf8) of
        traverse ->
            ok = qos_entry:add_traverse(SpaceId, QosId, TaskId),
            ok = qos_entry:remove_traverse_req(QosId, TaskId);
        restore -> ok
    end.

-spec task_finished(traverse:id()) -> ok.
task_finished(TaskId) ->
    [_, SpaceId, QosId, TaskType] = binary:split(TaskId, <<"#">>, [global]),
    case binary_to_atom(TaskType, utf8) of
        traverse -> ok = qos_entry:remove_traverse(SpaceId, QosId, TaskId);
        restore -> ok
    end.

-spec update_job_progress(undefined | main_job | traverse:job_id(),
    tree_traverse:master_job(), traverse:pool(), traverse:id(),
    traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TaskId, Status, ?MODULE).

-spec do_master_job(tree_traverse:master_job(), traverse:id()) -> {ok, traverse:master_job_map()}.
do_master_job(Job, TaskId) ->
    tree_traverse:do_master_job(Job, TaskId).


%%--------------------------------------------------------------------
%% @doc
%% Performs slave job for traverse task responsible for scheduling replications
%% to fulfill QoS requirements.
%% @end
%%--------------------------------------------------------------------
-spec do_slave_job(traverse:job(), traverse:id()) -> ok.
do_slave_job({#document{key = FileUuid, scope = SpaceId},
    #add_qos_traverse_args{target_storages = TargetStorages} = TraverseArgs}, TaskId) ->
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    RelativePath = qos_status:get_relative_path(TraverseArgs#add_qos_traverse_args.file_path_tokens, FileGuid),
    QosId = TraverseArgs#add_qos_traverse_args.qos_id,
    % TODO: add space check and optionally choose other storage

    OnTransferScheduledFun = fun(TransferId, StorageId) ->
        ok = qos_status:add_status_link(QosId, SpaceId, RelativePath, TaskId, StorageId, TransferId)
    end,
    OnTransferFinishedFun = fun
        (undefined) -> ok;
        (StorageId) -> ok = qos_status:delete_status_link(QosId, SpaceId, RelativePath, TaskId, StorageId)
    end,

    % call using ?MODULE macro for mocking in tests
    TransfersStorageProplist =
        ?MODULE:schedule_transfers(?ROOT_SESS_ID, FileGuid, TargetStorages, OnTransferScheduledFun),
    wait_for_transfers_completion(TransfersStorageProplist, OnTransferFinishedFun).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates replication of file to all storages specified in given list.
%% Adds appropriate status link.
%% @end
%%--------------------------------------------------------------------
-spec schedule_transfers(session:id(), fslogic_worker:file_guid(), [storage:id()],  OnTransferScheduledFun) ->
    [{transfer:id(), storage:id()}] when  OnTransferScheduledFun :: fun((transfer:id(), storage:id()) -> ok).
schedule_transfers(SessId, FileGuid, TargetStorages, OnTransferScheduledFun) ->
    lists:map(fun(StorageId) ->
        % TODO: VFS-5573 use storage qos
        {ok, TransferId} = lfm:schedule_file_replication(
            SessId, {guid, FileGuid}, StorageId, undefined, self()),
        OnTransferScheduledFun(TransferId, StorageId),
        {TransferId, StorageId}
    end, TargetStorages).


% TODO: implement properly when transfer callback will be available
wait_for_transfers_completion([] , _) ->
    ok;
wait_for_transfers_completion(TransfersPropList, OnTransferFinishedFun) ->
    receive
        {completed, TransferId} ->
            OnTransferFinishedFun(proplists:get_value(TransferId, TransfersPropList)),
            wait_for_transfers_completion(proplists:delete(TransferId, TransfersPropList),
                OnTransferFinishedFun)
    end.
