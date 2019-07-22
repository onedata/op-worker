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
-include_lib("ctool/include/logging.hrl").

%% API
-export([remove_qos/2, fulfill_qos/4, init_pool/0]).

%% Pool callbacks
-export([do_master_job/1, do_slave_job/1, task_finished/1, get_job/1,
    get_sync_info/1, list_ongoing_jobs/0, update_job_progress/5]).

% For test purpose
-export([schedule_transfers/4]).

-record(add_qos_traverse_args, {
    session_id :: session:id(),
    qos_id :: qos_entry:id(),
    qos_entry :: #qos_entry{},
    file_path_tokens = [] :: [binary()],
    target_storages = undefined :: file_qos:target_storages() | undefined
}).

-record(remove_qos_traverse_args, {
    qos_id :: qos_entry:id()
}).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(TRAVERSE_BATCH_SIZE, application:get_env(?APP_NAME, qos_traverse_batch_size, 40)).
-define(TASK_ID(QosId), <<QosId/binary, "#", (datastore_utils:gen_key())/binary>>).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates traverse task to fulfill qos.
%% @end
%%--------------------------------------------------------------------
-spec fulfill_qos(session:id(), file_ctx:ctx(), qos_entry:id(), [storage:id()] | undefined) -> ok.
fulfill_qos(SessionId, FileCtx, QosId, TargetStorages) ->
    {ok, #document{value = QosItem}} = qos_entry:get(QosId),
    QosOriginFileGuid = QosItem#qos_entry.file_guid,
    {FilePathTokens, _FileCtx} = file_ctx:get_canonical_path_tokens(file_ctx:new_by_guid(QosOriginFileGuid)),
    Options = #{
        task_id => ?TASK_ID(QosId),
        batch_size => ?TRAVERSE_BATCH_SIZE,
        traverse_info => #add_qos_traverse_args{
            session_id = SessionId,
            qos_id = QosId,
            file_path_tokens = FilePathTokens,
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
    MasterJobsLimit = application:get_env(?APP_NAME, qos_taverse_master_jobs_limit, 10),
    SlaveJobsLimit = application:get_env(?APP_NAME, qos_taverse_slave_jobs_limit, 20),
    ParallelismLimit = application:get_env(?APP_NAME, qos_traverse_parallelism_limit, 20),

    tree_traverse:init(?MODULE, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit).

%%%===================================================================
%%% Traverse callbacks
%%%===================================================================

list_ongoing_jobs() ->
    {ok, []}.

task_finished(TaskId) ->
    [QosId, _] = binary:split(TaskId, <<"#">>, [global]),
    {ok, _} = qos_entry:set_status(QosId, ?QOS_TRAVERSE_FINISHED_STATUS),
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
%% @end
%%--------------------------------------------------------------------
do_slave_job({#document{key = FileUuid, scope = Scope}, TraverseArgs = #add_qos_traverse_args{target_storages = TargetStorages}}) ->
    create_qos_replicas(FileUuid, Scope, TargetStorages, TraverseArgs);

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
%% Creates file replicas on given storages.
%% @end
%%--------------------------------------------------------------------
-spec create_qos_replicas(file_meta:uuid(), datastore_doc:scope(), file_qos:target_storages(),
    #add_qos_traverse_args{}) -> ok.
create_qos_replicas(FileUuid, SpaceId, TargetStorages, TraverseArgs) ->
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    RelativePath = qos_status:get_relative_path(TraverseArgs#add_qos_traverse_args.file_path_tokens, FileGuid),
    SessId = TraverseArgs#add_qos_traverse_args.session_id,
    QosId = TraverseArgs#add_qos_traverse_args.qos_id,
    % TODO: add space check and optionally choose other storage

    OnTransferScheduledFun = fun(TransferId, StorageId) ->
        qos_status:add_status_link(QosId, SpaceId, RelativePath, StorageId, TransferId)
    end,
    OnTransferFinishedFun = fun(StorageId) ->
        qos_status:delete_status_link(QosId, SpaceId, RelativePath, StorageId)
    end,

    % call using ?MODULE macro for mocking in tests
    TransfersList = ?MODULE:schedule_transfers(SessId, FileGuid, TargetStorages, OnTransferScheduledFun),
    wait_for_transfers_completion(TransfersList, OnTransferFinishedFun).


%%--------------------------------------------------------------------
%% @doc
%% Creates replication of file to all storages specified in given list.
%% Adds appropriate status link.
%% @end
%%--------------------------------------------------------------------
-spec schedule_transfers(session:id(), fslogic_worker:file_guid(), [storage:id()],  OnTransferScheduledFun) ->
    [transfer:id()] when  OnTransferScheduledFun :: fun((transfer:id(), storage:id()) -> ok).
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
