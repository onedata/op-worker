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
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("tree_traverse.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([reconcile_qos_for_entries/2, start_initial_traverse/3, init_pool/0,
    stop_pool/0]).

%% Traverse behaviour callbacks
-export([do_master_job/2, do_slave_job/2,
    task_finished/2, get_job/1, get_sync_info/1, update_job_progress/5]).

-type task_type() :: traverse | reconcile.
-export_type([task_type/0]).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(TRAVERSE_BATCH_SIZE, application:get_env(?APP_NAME, qos_traverse_batch_size, 40)).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates initial traverse task to fulfill requirements defined in qos_entry.
%% @end
%%--------------------------------------------------------------------
-spec start_initial_traverse(file_ctx:ctx(), qos_entry:id(), traverse:id()) -> ok.
start_initial_traverse(FileCtx, QosEntryId, TaskId) ->
    Options = #{
        task_id => TaskId,
        batch_size => ?TRAVERSE_BATCH_SIZE,
        additional_data => #{
            <<"qos_entry_id">> => QosEntryId,
            <<"space_id">> => file_ctx:get_space_id_const(FileCtx),
            <<"uuid">> => file_ctx:get_uuid_const(FileCtx),
            <<"task_type">> => <<"traverse">>
        }
    },
    {ok, FileCtx2} = qos_status:report_traverse_started(TaskId, FileCtx),
    {ok, _} = tree_traverse:run(?POOL_NAME, FileCtx2, Options),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Creates traverse task to fulfill requirements defined in qos_entries for
%% single file, after its change was synced.
%% @end
%%--------------------------------------------------------------------
-spec reconcile_qos_for_entries(file_ctx:ctx(), [qos_entry:id()]) -> ok.
reconcile_qos_for_entries(_FileCtx, []) -> 
    ok;
reconcile_qos_for_entries(FileCtx, QosEntries) ->
    ?notice("reconcile qos for entries"),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    TaskId = datastore_key:new(),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    Options = #{
        task_id => TaskId,
        batch_size => ?TRAVERSE_BATCH_SIZE,
        additional_data => #{
            <<"space_id">> => SpaceId,
            <<"uuid">> => FileUuid,
            <<"task_type">> => <<"reconcile">>
        }
    },
    ok = qos_status:report_file_changed(QosEntries, FileCtx, TaskId),
    {ok, _} = tree_traverse:run(?POOL_NAME, FileCtx, Options),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Initializes pool for traverse tasks concerning QoS management.
%% @end
%%--------------------------------------------------------------------
-spec init_pool() -> ok  | no_return().
init_pool() ->
    % Get pool limits from app.config
    MasterJobsLimit = application:get_env(?APP_NAME, qos_traverse_master_jobs_limit, 10),
    SlaveJobsLimit = application:get_env(?APP_NAME, qos_traverse_slave_jobs_limit, 20),
    ParallelismLimit = application:get_env(?APP_NAME, qos_traverse_parallelism_limit, 20),

    tree_traverse:init(?MODULE, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit).


-spec stop_pool() -> ok.
stop_pool() ->
    traverse:stop_pool(?POOL_NAME).

%%%===================================================================
%%% Traverse callbacks
%%%===================================================================

-spec get_job(traverse:job_id() | tree_traverse_job:doc()) ->
    {ok, tree_traverse:master_job(), traverse:pool(), traverse:id()}  | {error, term()}.
get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).

-spec get_sync_info(tree_traverse:master_job()) -> {ok, traverse:sync_info()}.
get_sync_info(Job) ->
    tree_traverse:get_sync_info(Job).

-spec task_finished(traverse:id(), traverse:pool()) -> ok.
task_finished(TaskId, _PoolName) ->
    {ok, #{
        <<"space_id">> := SpaceId,
        <<"uuid">> := FileUuid,
        <<"task_type">> := TaskType
    }} = AdditionalData = traverse_task:get_additional_data(?POOL_NAME, TaskId),
    case TaskType of
        <<"traverse">> ->
            {ok, #{<<"qos_entry_id">> := QosEntryId}} = AdditionalData,
            ok = qos_status:report_traverse_finished(SpaceId, TaskId, FileUuid),
            ok = qos_entry:remove_traverse_req(QosEntryId, TaskId);
        <<"reconcile">> ->
            ok = qos_status:report_file_reconciled(SpaceId, FileUuid, TaskId)
    end.

-spec update_job_progress(undefined | main_job | traverse:job_id(),
    tree_traverse:master_job(), traverse:pool(), traverse:id(),
    traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TaskId, Status, ?MODULE).

-spec do_master_job(tree_traverse:master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_master_job(Job, MasterJobArgs) ->
    tree_traverse:do_master_job(
        Job, MasterJobArgs, fun next_batch_callback/6, fun master_job_finished_callback/3
    ).


next_batch_callback(TaskId, SlaveJobs, MasterJobs, SpaceId, Uuid, BatchLastFilename) ->
    ChildrenFiles = lists:map(fun({#document{key = ChildFileUuid}, _}) ->
        ChildFileUuid
    end, SlaveJobs),
    ChildrenDirs = lists:map(fun(#tree_traverse{doc = #document{key = ChildDirUuid}}) ->
        ChildDirUuid
    end, MasterJobs),
    ok = qos_status:report_next_traverse_batch(
        SpaceId, TaskId, Uuid, ChildrenDirs, ChildrenFiles, BatchLastFilename
    ).


master_job_finished_callback(TaskId, Uuid, SpaceId) ->
    ok = qos_status:report_traverse_finished_for_dir(TaskId, Uuid, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Performs slave job for traverse task responsible for scheduling replications
%% to fulfill QoS requirements.
%% @end
%%--------------------------------------------------------------------
-spec do_slave_job(traverse:job(), traverse:id()) -> ok.
do_slave_job({#document{key = FileUuid, scope = SpaceId} = FileDoc, _TraverseInfo}, TaskId) ->
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    % TODO: add space check and optionally choose other storage
    ok = synchronize_file(UserCtx, FileCtx),

    {ok, #{
        <<"task_type">> := TaskType
    }} = traverse_task:get_additional_data(?POOL_NAME, TaskId),
    case TaskType of
        <<"traverse">> ->
            ok = qos_status:report_traverse_finished_for_file(
                TaskId, file_ctx:new_by_doc(FileDoc, SpaceId, undefined)
            );
        <<"reconcile">> -> ok
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Synchronizes file to given storage.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_file(user_ctx:ctx(), file_ctx:ctx()) -> ok.
synchronize_file(UserCtx, FileCtx) ->
    {Size, FileCtx2} = file_ctx:get_file_size(FileCtx),
    FileBlock = #file_block{offset = 0, size = Size},
    SyncResult = replica_synchronizer:synchronize(
        UserCtx, FileCtx2, FileBlock, false, undefined, ?QOS_SYNCHRONIZATION_PRIORITY
    ),
    case SyncResult of
        {ok, _} ->
            ok;
        {error, cancelled} ->
            ?debug("QoS file synchronization failed due to cancelation");
        {error, Reason} = Error ->
            % TODO: VFS-5737 handle failures properly
            ?error("Error during file synchronization: ~p", [Reason]),
            Error
    end,
    ok.
