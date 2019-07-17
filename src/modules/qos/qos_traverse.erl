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
-include("modules/datastore/qos.hrl").
-include("proto/oneclient/fuse_messages.hrl").

%% API
-export([remove_qos/2, fulfill_qos/6, init_pool/0, get_space_storages/2,
    schedule_transfers/3]).

%% Pool callbacks
-export([do_master_job/1, do_slave_job/1, task_finished/1, get_job/1,
    get_sync_info/1, get_target_storages/4, list_ongoing_jobs/0,
    update_job_progress/5]).

-record(add_qos_traverse_args, {
    session_id :: session:id(),
    qos_id :: qos_entry:id(),
    qos_expression :: qos_expression:expression(),
    replicas_num :: qos_entry:replicas_num(),
    target_storages = undefined :: file_qos:target_storages() | undefined
}).

-record(remove_qos_traverse_args, {
    qos_id :: qos_entry:id()
}).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(TRAVERSE_BATCH_SIZE, begin
    {ok, BatchSize} = application:get_env(?APP_NAME, qos_traverse_batch_size),
    BatchSize
end).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates traverse task to fulfill qos.
%% @end
%%--------------------------------------------------------------------
-spec fulfill_qos(session:id(), file_ctx:ctx(), qos_entry:id(), qos_expression:expression(),
    pos_integer(), [storage:id()] | undefined) -> ok.
fulfill_qos(SessionId, FileCtx, QosId, QosExpression, ReplicasNum, TargetStorages) ->
    Options = #{
        task_id => QosId,
        batch_size => ?TRAVERSE_BATCH_SIZE,
        traverse_info => #add_qos_traverse_args{
            session_id = SessionId,
            qos_id = QosId,
            qos_expression = QosExpression,
            replicas_num = ReplicasNum,
            target_storages = TargetStorages
        }
    },

    case file_ctx:is_dir(FileCtx) of
        {false, _} ->
            {ok, FileMeta} = file_meta:get(file_ctx:get_uuid_const(FileCtx)),
            tree_traverse:run(?POOL_NAME, FileMeta, Options);
        {true, _} ->
            tree_traverse:run(?POOL_NAME, FileCtx, Options)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates traverse task for removing qos.
%% @end
%%--------------------------------------------------------------------
-spec remove_qos(file_ctx:ctx(), qos_entry:id()) -> ok.
remove_qos(FileCtx, QosId) ->
    Options = #{
        task_id => <<QosId/binary, "#remove">>,
        execute_on_slave_dir => true,
        batch_size => ?TRAVERSE_BATCH_SIZE,
        traverse_info => #remove_qos_traverse_args{qos_id = QosId}
    },
    tree_traverse:run(?POOL_NAME, FileCtx, Options).

%%--------------------------------------------------------------------
%% @doc
%% Initializes pool for traverse tasks concerning qos management.
%% @end
%%--------------------------------------------------------------------
-spec init_pool() -> ok.
init_pool() ->
    % Get pool limits from app.config
    {ok, MasterJobsLimit} = application:get_env(?APP_NAME, qos_taverse_master_jobs_limit),
    {ok, SlaveJobsLimit} = application:get_env(?APP_NAME, qos_taverse_slave_jobs_limit),
    {ok, ParallelismLimit} = application:get_env(?APP_NAME, qos_traverse_parallelism_limit),

    tree_traverse:init(?MODULE, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit).

%%%===================================================================
%%% Traverse callbacks
%%%===================================================================

list_ongoing_jobs() ->
    {ok, []}.

task_finished(TaskId) ->
    case binary:split(TaskId, <<"#">>) of
        [_QosId, <<"remove">>] ->
            ok;
        _ ->
            QosId = TaskId,
            case qos_entry:get_status(QosId) of
                ?IMPOSSIBLE ->
                    ok;
                _ ->
                    {ok, _} = qos_entry:set_status(QosId, ?FULFILLED)
            end
    end,
    ok.

get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).

get_sync_info(Job) ->
    tree_traverse:get_sync_info(Job).

update_job_progress(Id, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TaskId, Status, ?MODULE).

do_master_job(Job) ->
    tree_traverse:do_master_job(Job).

%%--------------------------------------------------------------------
%% @doc
%% Performs slave job for traverse task responsible for scheduling replications
%% to fulfill QoS requirements.
%% For directory target_storages are chosen before starting traverse task and
%% passed to slave job.
%% For file target_storages are chosen in slave job and "undefined" is passed
%% in target_storages argument.
%% @end
%%--------------------------------------------------------------------
do_slave_job({#document{key = FileUuid, scope = SpaceId},
        TraverseArgs = #add_qos_traverse_args{target_storages = undefined}}) ->
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    SessId = TraverseArgs#add_qos_traverse_args.session_id,
    QosId = TraverseArgs#add_qos_traverse_args.qos_id,
    QosExpression = TraverseArgs#add_qos_traverse_args.qos_expression,
    ReplicasNum = TraverseArgs#add_qos_traverse_args.replicas_num,

    case get_target_storages(SessId, {guid, FileGuid}, QosExpression, ReplicasNum) of
        ?CANNOT_FULFILL_QOS ->
            {ok, _} = qos_entry:set_status(QosId, ?IMPOSSIBLE),
            ok;
        TargetStorages ->
            {ok, _} = file_qos:add_to_target_storages(FileGuid, TargetStorages, QosId),
            TransfersList = qos_traverse:schedule_transfers(SessId, FileGuid, TargetStorages),
            wait_for_transfers_completion(TransfersList)
    end;
do_slave_job({#document{key = FileUuid, scope = SpaceId}, TraverseArgs = #add_qos_traverse_args{target_storages = TargetStorages}}) ->
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    SessId = TraverseArgs#add_qos_traverse_args.session_id,

    % TODO: VFS-5574 add check if storage has enough free space
    TransfersList = qos_traverse:schedule_transfers(SessId, FileGuid, TargetStorages),
    wait_for_transfers_completion(TransfersList);

%%--------------------------------------------------------------------
%% @doc
%% Performs slave job for traverse task responsible for removing QoS requirement.
%%--------------------------------------------------------------------
do_slave_job({#document{key = FileUuid, scope = SpaceId}, #remove_qos_traverse_args{qos_id = QosId}}) ->
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    {ok, _} = file_qos:remove_from_target_storages(FileGuid, QosId).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates replication of file to all storages specified in given list.
%% @end
%%--------------------------------------------------------------------
-spec schedule_transfers(session:id(), fslogic_worker:file_guid(),
    [storage:id()]) -> [transfer:id()].
schedule_transfers(SessId, FileGuid, TargetStorages) ->
    lists:foldl(fun(StorageId, TransfersList) ->
        % TODO: VFS-5573 use storage qos
        {ok, TransferId} = lfm:schedule_file_replication(
            SessId, {guid, FileGuid}, StorageId, undefined, self()
        ),
        [TransferId | TransfersList]
    end, [], TargetStorages).


%%--------------------------------------------------------------------
%% @doc
%% Get list of storage id, on which file should be present according to
%% qos requirements and available storage.
%% @end
%%--------------------------------------------------------------------
-spec get_target_storages(session:id(), logical_file_manager:file_key(),
    qos_expression:expression(), pos_integer()) -> [storage:id()].
get_target_storages(SessId, FileKey, Expression, ReplicasNum) ->
    {ok, FileLocations} = lfm:get_file_distribution(SessId, FileKey),

    % TODO: VFS-5574 add check if storage has enough free space
    SpaceStorages = qos_traverse:get_space_storages(SessId, FileKey),
    qos_expression:get_target_storage(Expression, ReplicasNum, SpaceStorages, FileLocations).


%%--------------------------------------------------------------------
%% @doc
%% Get list of storage id supporting space in which given file is stored.
%% @end
%%--------------------------------------------------------------------
-spec get_space_storages(session:id(), logical_file_manager:file_key()) ->
    [storage:id()].
get_space_storages(Auth, FileKey) ->
    {guid, FileGuid} = guid_utils:ensure_guid(Auth, FileKey),
    SpaceId = file_id:guid_to_space_id(FileGuid),

    % TODO: VFS-5573 use storage qos
    {ok, ProvidersId} = space_logic:get_provider_ids(Auth, SpaceId),
    ProvidersId.


% TODO: VFS-5572 use transfers callback
-spec wait_for_transfers_completion([transfer:id()]) -> ok.
wait_for_transfers_completion([]) ->
    ok;
wait_for_transfers_completion(TransfersList) ->
    receive
        {completed, TransferId} ->
            wait_for_transfers_completion(lists:delete(TransferId, TransfersList))
    end.