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
%%%     * blocking all operations, that modify local files replicas 
%%%     * replicating all local replicas to other providers 
%%%     * cleaning up local storage and local metadata 
%%%     * waiting for all remote providers to be up to date with this provider dbsync changes
%%%     * cleaning up database from documents synced in space scope 
%%%     * cleaning up database from local documents related to space support
%%% @end
%%%--------------------------------------------------------------------
-module(space_unsupport).
-author("Michal Stanisz").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/qos.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

-behaviour(traverse_behaviour).

%% API
-export([init_pools/0, run/2]).
-export([report_traverse_finished/2]).
-export([get_all_stages/0]).

%% traverse behaviour callbacks
-export([get_job/1, update_job_progress/5]).
-export([do_master_job/2, do_slave_job/2]).
-export([task_finished/2]).
%% @TODO VFS-6132 Do not export when space_unsupport is used in storage:revoke_space_support
-export([cleanup_local_documents/2]).

-type stage() :: init | replicate | cleanup_traverse | wait_for_dbsync 
    | delete_synced_documents | delete_local_documents.
-type job() :: space_unsupport_job:record().
% Id of task that was created in slave job (e.g. QoS entry id or cleanup traverse id). 
-type subtask_id() :: qos_entry:id() | unsupport_cleanup_traverse:id().

-export_type([stage/0, subtask_id/0]).

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

-spec run(od_space:id(), storage:id()) -> ok.
run(SpaceId, StorageId) ->
    file_meta:make_space_exist(SpaceId),
    ?ok_if_exists(traverse:run(?POOL_NAME, datastore_key:new_from_digest([SpaceId, StorageId]), 
        #space_unsupport_job{space_id = SpaceId, storage_id = StorageId, stage = init})).

-spec report_traverse_finished(od_space:id(), storage:id()) -> ok.
report_traverse_finished(SpaceId, StorageId) ->
    {ok, #space_unsupport_job{slave_job_pid = Pid}} = 
        space_unsupport_job:get(SpaceId, StorageId, cleanup_traverse),
    Pid ! cleanup_traverse_finished,
    ok.

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
    space_unsupport_job:save(Id, Job, TaskId).

-spec do_master_job(job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()} | {error, term()}.
do_master_job(#space_unsupport_job{stage = Stage} = Job, _MasterJobArgs) ->
    NextStageList = get_next_stage(Stage),
    NextMasterJobs = lists:map(fun(NextStage) -> 
        Job#space_unsupport_job{stage = NextStage, subtask_id = undefined}
    end, NextStageList),
    {ok, #{slave_jobs => [Job], master_jobs => NextMasterJobs}}.

-spec do_slave_job(job(), traverse:id()) -> ok | {ok, traverse:description()} | {error, term()}.
do_slave_job(Job, _TaskId) ->
    execute_stage(Job).

-spec task_finished(traverse:id(), traverse:pool()) -> ok.
task_finished(TaskId, Pool) ->
    spawn(fun() -> timer:sleep(timer:seconds(2)), traverse_task:delete_ended(Pool, TaskId) end),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec get_next_stage(stage()) -> [stage()].
get_next_stage(init) -> [replicate];
get_next_stage(replicate) -> [cleanup_traverse];
get_next_stage(cleanup_traverse) -> [wait_for_dbsync];
get_next_stage(wait_for_dbsync) -> [delete_synced_documents];
get_next_stage(delete_synced_documents) -> [delete_local_documents];
get_next_stage(delete_local_documents) -> [].


%% @private
-spec execute_stage(space_unsupport_job:record()) -> ok.
execute_stage(#space_unsupport_job{stage = init}) ->
    %% @TODO VFS-6133 Stop all incoming transfers
    %% @TODO VFS-6134 Close all open file handles
    %% @TODO VFS-6135 Block all modifying file operations
    %% @TODO VFS-6136 Inform onezone that unsupport started
    ok;

execute_stage(#space_unsupport_job{stage = replicate, subtask_id = undefined} = Job) ->
    #space_unsupport_job{space_id = SpaceId, storage_id = StorageId} = Job,
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    Expression = <<?QOS_ANY_STORAGE/binary, " - storageId = ", StorageId/binary>>,
    {ok, QosEntryId} = lfm:add_qos_entry(?ROOT_SESS_ID, {guid, SpaceGuid}, Expression, 1, internal),
    NewJob = Job#space_unsupport_job{subtask_id = QosEntryId},
    space_unsupport_job:save(NewJob),
    execute_stage(NewJob);
execute_stage(#space_unsupport_job{stage = replicate, subtask_id = QosEntryId} = _Job) ->
    wait(fun() -> lfm:check_qos_fulfilled(?ROOT_SESS_ID, QosEntryId) end),
    lfm:remove_qos_entry(?ROOT_SESS_ID, QosEntryId);

execute_stage(#space_unsupport_job{stage = cleanup_traverse, subtask_id = undefined} = Job) ->
    #space_unsupport_job{space_id = SpaceId, storage_id = StorageId} = Job,
    %% @TODO VFS-6175 Do not clean up storage when import is on
    {ok, TaskId} = unsupport_cleanup_traverse:start(SpaceId, StorageId),
    NewJob = Job#space_unsupport_job{subtask_id = TaskId, slave_job_pid = self()},
    {ok, _} = space_unsupport_job:save(NewJob),
    execute_stage(NewJob);
execute_stage(#space_unsupport_job{stage = cleanup_traverse} = Job) ->
    #space_unsupport_job{
        space_id = SpaceId, slave_job_pid = Pid, subtask_id = TraverseId
    } = Job,
    
    case self() of
        Pid -> ok;
        _ -> space_unsupport_job:save(Job#space_unsupport_job{slave_job_pid = self()})
    end, 
    case unsupport_cleanup_traverse:is_finished(TraverseId) of
        true -> ok;
        false ->
            receive cleanup_traverse_finished ->
                ok
            end
    end,
    
    %% @TODO VFS-6165 Not needed after modifying cleanup traverse to delete dirs 
    %% after all its children have been deleted
    FileGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    sd_utils:recursive_delete(FileCtx, UserCtx);

execute_stage(#space_unsupport_job{stage = wait_for_dbsync} = _Job) ->
    %% @TODO VFS-6164 wait for all documents to be saved on disc
    %% @TODO VFS-6136 wait until all other providers are up to date with dbsync changes
    %% @TODO VFS-6135 Stop dbsync
    timer:sleep(timer:seconds(60)),
    ok;

execute_stage(#space_unsupport_job{stage = delete_synced_documents} = Job) ->
    #space_unsupport_job{space_id = SpaceId, storage_id = StorageId} = Job,
    %% @TODO VFS-6136 Inform onezone that unsupport started in init
    storage_logic:revoke_space_support(StorageId, SpaceId),
    start_changes_stream(SpaceId),
    receive end_of_stream ->
        ok
    end;

execute_stage(#space_unsupport_job{stage = delete_local_documents} = Job) ->
    #space_unsupport_job{space_id = SpaceId, storage_id = StorageId} = Job,
    cleanup_local_documents(SpaceId, StorageId),
    unsupport_cleanup_traverse:delete_ended(SpaceId, StorageId),
    ok.


%%%===================================================================
%%% Stage internals
%%%===================================================================

%% @private
-spec wait(fun(() -> {ok, boolean()})) -> ok.
wait(Fun) ->
    try
        case Fun() of
            {ok, true} -> ok;
            {ok, false} ->
                timer:sleep(timer:seconds(10)),
                wait(Fun)
        end
    catch Error:Reason ->
        ?warning("Error in space_unsupport:wait/1 ~p:~p", [Error, Reason]),
        timer:sleep(timer:seconds(10)),
        wait(Fun)
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
            Ctx = #{
                model => element(1, Value),
                hooks_disabled => true,
                expiry => ?DOCUMENT_EXPIRY_TIME
            },
            datastore_model:save(Ctx, Doc)
    end,
    ok.


%% @private
-spec expire_links(datastore_model:model(), datastore:key(), datastore:doc()) -> ok.
expire_links(Model, RoutingKey, Doc = #document{key = Key}) ->
    Ctx = #{
        model => Model,
        expiry => ?DOCUMENT_EXPIRY_TIME,
        routing_key => RoutingKey
    },
    Ctx2 = datastore_model_default:set_defaults(Ctx),
    Ctx3 = datastore_multiplier:extend_name(RoutingKey, Ctx2),
    datastore_router:route(save, [Ctx3, Key, Doc]),
    ok.


%% @private
-spec cleanup_local_documents(od_space:id(), storage:id()) -> ok.
cleanup_local_documents(SpaceId, StorageId) ->
    file_popularity_api:disable(SpaceId),
    file_popularity_api:delete_config(SpaceId),
    autocleaning_api:disable(SpaceId),
    autocleaning_api:delete_config(SpaceId),
    storage_sync:space_unsupported(SpaceId, StorageId),
    space_quota:delete(SpaceId),
    dbsync_state:delete(SpaceId),
    main_harvesting_stream:space_unsupported(SpaceId).
