%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handling space unsupport procedure. 
%%% 
%%% Unsupport procedure consists of following activities: 
%%%     * blocking all operations, that modify local files replicas (@TODO VFS-6135 NYI)
%%%     * replicating all local replicas to other providers 
%%%     * cleaning up local storage and local metadata 
%%%     * waiting for all remote providers to be up to date with this provider dbsync changes (@TODO VFS-7164 NYI)
%%%     * cleaning up database from documents synced in space scope 
%%%     * cleaning up database from local documents related to space support 
%%% 
%%% Forced strategy indicates that the provider has been forcefully removed 
%%% from the space supporters. In such case, only necessary cleanup is performed 
%%% and stages such as data replication are omitted.
%%% @end
%%%--------------------------------------------------------------------
-module(space_unsupport_engine).
-author("Michal Stanisz").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/qos.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

-behaviour(traverse_behaviour).

%% API
-export([init_pools/0]).
-export([schedule_start/2, schedule_start/3]).
-export([report_cleanup_traverse_finished/2]).
-export([clean_local_documents/2]).
-export([get_all_stages/0]).

%% traverse behaviour callbacks
-export([get_job/1, update_job_progress/5]).
-export([do_master_job/2, do_slave_job/2]).
-export([task_finished/2]).

-type stage() :: init | replicate | cleanup_traverse | wait_for_dbsync 
    | delete_synced_documents | delete_local_documents.
-type job() :: space_unsupport_job:record().
% Id of task that was created in slave job (e.g. QoS entry id or cleanup traverse id). 
-type subtask_id() :: qos_entry:id() | unsupport_cleanup_traverse:id().
-type strategy() :: forced | graceful.

-export_type([stage/0, subtask_id/0, strategy/0]).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(DOCUMENT_EXPIRY_TIME, 60). % in seconds


%%%===================================================================
%%% API
%%%===================================================================

-spec init_pools() -> ok.
init_pools() ->
    MasterJobsLimit = application:get_env(?APP_NAME, space_unsupport_master_jobs_limit, 1),
    SlaveJobsLimit = application:get_env(?APP_NAME, space_unsupport_slave_jobs_limit, 5),
    ParallelismLimit = application:get_env(?APP_NAME, space_unsupport_parallelism_limit, 5),
    traverse:init_pool(?POOL_NAME, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit),
    unsupport_cleanup_traverse:init_pool().


-spec schedule_start(od_space:id(), storage:id()) -> ok.
schedule_start(SpaceId, StorageId) ->
    schedule_start(SpaceId, StorageId, graceful).

-spec schedule_start(od_space:id(), storage:id(), strategy()) -> ok.
schedule_start(SpaceId, StorageId, Strategy) ->
    %% @TODO VFS-7167 handle last unsupport
    % ensure that file_meta doc for space is created, so it is not needed to always 
    % check if it exists when doing operations on files
    file_meta:make_space_exist(SpaceId),
    ?ok_if_exists(traverse:run(?POOL_NAME, datastore_key:new_from_digest([SpaceId, StorageId]),
        #space_unsupport_job{space_id = SpaceId, storage_id = StorageId, stage = init, 
            strategy = Strategy})).


-spec report_cleanup_traverse_finished(od_space:id(), storage:id()) -> ok.
report_cleanup_traverse_finished(SpaceId, StorageId) ->
    {ok, #space_unsupport_job{slave_job_pid = Pid}} = 
        space_unsupport_job:get(SpaceId, StorageId, cleanup_traverse),
    Pid ! cleanup_traverse_finished,
    ok.


-spec clean_local_documents(od_space:id(), storage:id()) -> ok.
clean_local_documents(SpaceId, StorageId) ->
    file_popularity_api:disable(SpaceId),
    file_popularity_api:delete_config(SpaceId),
    autocleaning_api:disable(SpaceId),
    autocleaning_api:delete_config(SpaceId),
    storage_import:clean_up(SpaceId),
    space_quota:delete(SpaceId),
    luma:clear_db(StorageId, SpaceId).


-spec get_all_stages() -> [stage()].
get_all_stages() ->
    [
        init,
        replicate,
        cleanup_traverse,
        wait_for_dbsync,
        delete_synced_documents,
        delete_local_documents
    ].

%%%===================================================================
%%% Traverse behaviour callbacks
%%%===================================================================


-spec get_job(traverse:job_id()) ->
    {ok, job(), traverse:pool(), traverse:id()}  | {error, term()}.
get_job(Id) ->
    {ok, Job} = space_unsupport_job:get(Id),
    {ok, Job, ?POOL_NAME, Id}.

-spec update_job_progress(undefined | main_job | traverse:job_id(),
    job(), traverse:pool(), traverse:id(), traverse:job_status()) ->
    {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, _Job, _PoolName, _TaskId, Status) when
    Status =:= ended;
    Status =:= failed;
    Status =:= canceled ->
    ok = space_unsupport_job:delete(Id),
    {ok, Id};
update_job_progress(Id, Job, _PoolName, TaskId, _Status) ->
    {ok, _} = space_unsupport_job:save(Id, Job, TaskId).

-spec do_master_job(job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()} | {error, term()}.
do_master_job(#space_unsupport_job{stage = Stage} = Job, _MasterJobArgs) ->
    NextMasterJobsBase = get_next_jobs_base(Stage),
    NextMasterJobs = lists:map(fun(NextStage) -> 
        Job#space_unsupport_job{stage = NextStage, subtask_id = undefined}
    end, NextMasterJobsBase),
    {ok, #{slave_jobs => [Job], master_jobs => NextMasterJobs}}.

-spec do_slave_job(job(), traverse:id()) -> ok | {ok, traverse:description()} | {error, term()}.
do_slave_job(Job, _TaskId) ->
    execute_stage(Job).

-spec task_finished(traverse:id(), traverse:pool()) -> ok.
task_finished(TaskId, Pool) ->
    %% @TODO Sleep not needed after resolving VFS-6212
    spawn(fun() -> timer:sleep(timer:seconds(2)), traverse_task:delete_ended(Pool, TaskId) end),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec get_next_jobs_base(stage()) -> [stage()].
get_next_jobs_base(init) -> [replicate];
get_next_jobs_base(replicate) -> [cleanup_traverse];
get_next_jobs_base(cleanup_traverse) -> [wait_for_dbsync];
get_next_jobs_base(wait_for_dbsync) -> [delete_synced_documents];
get_next_jobs_base(delete_synced_documents) -> [delete_local_documents];
get_next_jobs_base(delete_local_documents) -> [].


%% @private
-spec execute_stage(space_unsupport_job:record()) -> ok.
execute_stage(#space_unsupport_job{stage = init, space_id = SpaceId, storage_id = StorageId}) ->
    ok = space_support:init_unsupport(StorageId, SpaceId),
    ok = supported_spaces:remove(SpaceId, StorageId),
    main_harvesting_stream:space_unsupported(SpaceId),
    storage_import:stop_auto_scan(SpaceId),
    auto_storage_import_worker:notify_space_unsupported(SpaceId),
    %% @TODO VFS-6133 Stop all incoming transfers
    %% @TODO VFS-6134 Close all open file handles
    %% @TODO VFS-6135 Block all modifying file operations
    %% @TODO VFS-6208 Cancel sync and auto-cleaning traverse and clean up ended tasks when unsupporting
    ok;

execute_stage(#space_unsupport_job{stage = replicate, strategy = forced}) ->
    % can not replicate data as space is no longer supported
    ok;
execute_stage(#space_unsupport_job{stage = replicate, subtask_id = undefined} = Job) ->
    #space_unsupport_job{space_id = SpaceId, storage_id = StorageId} = Job,
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    Expression = <<?QOS_ANY_STORAGE, "\\ storageId = ", StorageId/binary>>,
    {ok, QosEntryId} = lfm:add_qos_entry(?ROOT_SESS_ID, {guid, SpaceGuid}, Expression, 1, internal),
    NewJob = Job#space_unsupport_job{subtask_id = QosEntryId},
    {ok, _} = space_unsupport_job:save(NewJob),
    execute_stage(NewJob);
execute_stage(#space_unsupport_job{stage = replicate} = Job) ->
    #space_unsupport_job{space_id = SpaceId, storage_id = StorageId, subtask_id = QosEntryId} = Job,
    %% @TODO Use subscription after resolving VFS-5647
    wait(fun() -> lfm:check_qos_status(?ROOT_SESS_ID, QosEntryId) == {ok, ?FULFILLED} end),
    ok = space_support:complete_unsupport_resize(StorageId, SpaceId),
    lfm:remove_qos_entry(?ROOT_SESS_ID, QosEntryId);

execute_stage(#space_unsupport_job{stage = cleanup_traverse, subtask_id = undefined} = Job) ->
    #space_unsupport_job{space_id = SpaceId, storage_id = StorageId} = Job,
    {ok, TaskId} = unsupport_cleanup_traverse:start(SpaceId, StorageId),
    NewJob = Job#space_unsupport_job{subtask_id = TaskId, slave_job_pid = self()},
    {ok, _} = space_unsupport_job:save(NewJob),
    execute_stage(NewJob);
execute_stage(#space_unsupport_job{stage = cleanup_traverse} = Job) ->
    % This clause can be run after provider restart so update slave_job_pid if needed
    maybe_update_slave_job_pid(Job),
    
    #space_unsupport_job{space_id = SpaceId, storage_id = StorageId, subtask_id = TraverseId} = Job,
    
    case unsupport_cleanup_traverse:is_finished(TraverseId) of
        true -> ok;
        false ->
            receive cleanup_traverse_finished ->
                ok
            end
    end,
    ok = space_support:complete_unsupport_purge(StorageId, SpaceId);

execute_stage(#space_unsupport_job{stage =  wait_for_dbsync, strategy = forced}) ->
    % no need to wait for dbsync as this space is no longer supported by this provider
    ok;
execute_stage(#space_unsupport_job{stage = wait_for_dbsync} = _Job) ->
    %% @TODO VFS-6164 wait for all documents to be saved on disc
    %% @TODO VFS-7164 wait until all other providers are up to date with dbsync changes
    %% @TODO VFS-7164 Stop dbsync
    timer:sleep(timer:seconds(60)),
    ok;

execute_stage(#space_unsupport_job{stage = delete_synced_documents} = Job) ->
    #space_unsupport_job{space_id = SpaceId} = Job,
    start_changes_stream(SpaceId),
    receive end_of_stream ->
        ok
    end;

execute_stage(#space_unsupport_job{stage = delete_local_documents} = Job) ->
    #space_unsupport_job{space_id = SpaceId, storage_id = StorageId} = Job,
    %% @TODO VFS-6241 Properly clean up users root dirs
    clean_local_documents(SpaceId, StorageId),
    qos_entry:apply_to_all_impossible_in_space(SpaceId, fun(QosEntryId) -> 
        qos_entry:remove_from_impossible_list(SpaceId, QosEntryId)
    end),
    qos_entry:apply_to_all_in_failed_files_list(SpaceId, fun(FileUuid) ->
        qos_entry:remove_from_failed_files_list(SpaceId, FileUuid)
    end),
    unsupport_cleanup_traverse:delete_ended(SpaceId, StorageId),
    ok = space_support:finalize_unsupport(StorageId, SpaceId).


%%%===================================================================
%%% Stage internals
%%%===================================================================

%% @private
-spec wait(fun(() -> {ok, boolean()})) -> ok.
wait(Fun) ->
    try
        case Fun() of
            true -> ok;
            false ->
                timer:sleep(timer:seconds(10)),
                wait(Fun)
        end
    catch Error:Reason ->
        ?warning("Error in space_unsupport_engine:wait/1 ~p:~p", [Error, Reason]),
        timer:sleep(timer:seconds(10)),
        wait(Fun)
    end.

%% @private
-spec maybe_update_slave_job_pid(job()) -> ok.
maybe_update_slave_job_pid(#space_unsupport_job{slave_job_pid = Pid} = Job) ->
    case self() of
        Pid -> ok;
        _ -> 
            {ok, _} = space_unsupport_job:save(Job#space_unsupport_job{slave_job_pid = self()}),
            ok
    end.

%% @private
-spec start_changes_stream(od_space:id()) -> ok.
start_changes_stream(SpaceId) ->
    Until = dbsync_state:get_seq(SpaceId, oneprovider:get_id()),
    Pid = self(),
    Callback = fun 
        ({ok, end_of_stream}) ->
            Pid ! end_of_stream,
            ok;
        ({ok, Batch}) ->
            lists:foreach(fun expire_docs/1, Batch)
    end,
    couchbase_changes_stream:start_link(
        couchbase_changes:design(), SpaceId, Callback,
        [{since, 0}, {until, Until}], [self()]
    ),
    ok.


%% @private
-spec expire_docs(datastore:doc()) -> ok.
expire_docs(#document{value = Value} = Doc) ->
    case Value of
        #links_forest{model = Model, key = Key} -> expire_links(Model, Key, Doc);
        #links_node{model = Model, key = Key} -> expire_links(Model, Key, Doc);
        _ ->
            Model  = element(1, Value),
            Ctx = Model:get_ctx(),
            Ctx1 = Ctx#{
                hooks_disabled => true,
                expiry => ?DOCUMENT_EXPIRY_TIME
            },
            datastore_model:save(Ctx1, Doc)
    end,
    ok.


%% @private
-spec expire_links(datastore_model:model(), datastore:key(), datastore:doc()) -> ok.
expire_links(Model, RoutingKey, Doc) ->
    Ctx = Model:get_ctx(),
    Ctx1 = Ctx#{
        expiry => ?DOCUMENT_EXPIRY_TIME,
        routing_key => RoutingKey
    },
    datastore_model:save_with_routing_key(Ctx1, Doc),
    ok.
