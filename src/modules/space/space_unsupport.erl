%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% fixme
%%% @end
%%%--------------------------------------------------------------------
-module(space_unsupport).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/qos.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

-behaviour(traverse_behaviour).

%% API
-export([init/0, run/2]).

%% traverse behaviour callbacks
-export([get_job/1, update_job_progress/5]).
-export([do_master_job/2, do_slave_job/2]).
-export([task_finished/2]).

% fixme better names (start | replicate | ... | cleanup_database | after? )
-type stage() :: init | qos | clear_storage | wait_for_dbsync | clean_database | finished.
-type job() :: space_unsupport_job:record().

-export_type([stage/0]).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(EXPIRY, 60).

%%%===================================================================
%%% API
%%%===================================================================

init() ->
    % fixme
    traverse:init_pool(?POOL_NAME, 1, 5, 5),
    unsupport_traverse:init_pool().

run(SpaceId, StorageId) ->
    file_meta:make_space_exist(SpaceId),
    ?ok_if_exists(traverse:run(?POOL_NAME, datastore_key:new_from_digest([SpaceId, StorageId]), 
        #space_unsupport_job{space_id = SpaceId, storage_id = StorageId, stage = init})).

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
do_master_job(#space_unsupport_job{stage = finished} = Job, _MasterJobArgs) ->
    {ok, #{slave_jobs => [Job], master_jobs => []}};
do_master_job(#space_unsupport_job{stage = Stage} = Job, _MasterJobArgs) ->
    NextMasterJob = Job#space_unsupport_job{stage = get_next_stage(Stage), slave_job_id = undefined},
    {ok, #{slave_jobs => [Job], master_jobs => [NextMasterJob]}}.

-spec do_slave_job(job(), traverse:id()) -> ok | {ok, traverse:description()} | {error, term()}.
do_slave_job(#space_unsupport_job{stage = Stage} = Job, _TaskId) ->
    execute_stage(Job).

-spec task_finished(traverse:id(), traverse:pool()) -> ok.
task_finished(TaskId, Pool) ->
    spawn(fun() -> timer:sleep(timer:seconds(2)), traverse_task:delete_ended(Pool, TaskId) end),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec get_next_stage(stage()) -> stage().
get_next_stage(init) -> qos;
get_next_stage(qos) -> clear_storage;
get_next_stage(clear_storage) -> wait_for_dbsync;
get_next_stage(wait_for_dbsync) -> clean_database;
get_next_stage(clean_database) -> finished.

%% @private
-spec execute_stage(space_unsupport_job:record()) -> ok.
execute_stage(#space_unsupport_job{stage = init}) ->
    %% @TODO VFS-6133 Stop all incoming transfers
    %% @TODO VFS-6134 Close all open file handles
    %% @TODO VFS-6135 Block all modifying file operations
    %% @TODO VFS-6136 Inform onezone that unsupport started
    ok;

execute_stage(#space_unsupport_job{stage = qos, slave_job_id = undefined} = Job) ->
    #space_unsupport_job{space_id = SpaceId, storage_id = StorageId} = Job,
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    Expression = <<?QOS_ALL_STORAGES/binary, " - storage_id = ", StorageId/binary>>,
    {ok, QosEntryId} = lfm:add_qos_entry(?ROOT_SESS_ID, {guid, SpaceGuid}, Expression, 1, internal),
    NewJob = Job#space_unsupport_job{slave_job_id = QosEntryId},
    space_unsupport_job:save(NewJob),
    execute_stage(NewJob);
execute_stage(#space_unsupport_job{stage = qos, slave_job_id = QosEntryId} = _Job) ->
    wait(fun() -> lfm:check_qos_fulfilled(?ROOT_SESS_ID, QosEntryId) end),
    lfm:remove_qos_entry(?ROOT_SESS_ID, QosEntryId, true);

execute_stage(#space_unsupport_job{stage = clear_storage, slave_job_id = undefined} = Job) ->
    #space_unsupport_job{space_id = SpaceId, storage_id = StorageId} = Job,
    % fixme do not clean storage if import is on
    {ok, TaskId} = unsupport_traverse:start(SpaceId, StorageId),
    NewJob = Job#space_unsupport_job{slave_job_id = TaskId},
    {ok, _} = space_unsupport_job:save(NewJob),
    execute_stage(NewJob);
execute_stage(#space_unsupport_job{stage = clear_storage, space_id = SpaceId, slave_job_id = TaskId} = _Job) ->
    wait(fun() -> unsupport_traverse:is_finished(TaskId) end),
    %% @TODO VFS-6165
    FileGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    sd_utils:recursive_delete(FileCtx, UserCtx);

execute_stage(#space_unsupport_job{stage = wait_for_dbsync, slave_job_id = undefined} = _Job) ->
    %% @TODO VFS-6136 wait until all dbsync changes are sent
    %% @TODO VFS-6164 wait for all documents to be saved on disc
    timer:sleep(timer:seconds(60)),
    %% @TODO VFS-6135 Stop dbsync
    ok;

execute_stage(#space_unsupport_job{stage = clean_database, slave_job_id = undefined} = Job) ->
    #space_unsupport_job{space_id = SpaceId, storage_id = StorageId} = Job,
    storage_logic:revoke_space_support(StorageId, SpaceId),
    start_changes_stream(SpaceId),
    receive end_of_stream ->
        ok
    end;

execute_stage(#space_unsupport_job{stage = finished} = Job) ->
    #space_unsupport_job{space_id = SpaceId, storage_id = StorageId} = Job,
    storage:on_space_unsupported(SpaceId, StorageId),
    unsupport_traverse:delete_ended(SpaceId, StorageId),
    ok.


%%%===================================================================
%%% Stage internals
%%%===================================================================

%% @private
-spec wait(fun(() -> {ok, boolean()})) -> ok.
wait(Fun) ->
    case Fun() of
        {ok, true} -> ok;
        {ok, false} ->
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
                expiry => ?EXPIRY
            },
            datastore_model:save(Ctx, Doc)
    end,
    ok.


%% @private
-spec expire_links(datastore:model(), datastore:key(), datastore:doc()) -> ok.
expire_links(Model, RoutingKey, Doc = #document{key = Key}) ->
    Ctx = Model:get_ctx(),
    Ctx2 = Ctx#{
        expiry => ?EXPIRY,
        routing_key => RoutingKey
    },
    Ctx3 = datastore_model_default:set_defaults(Ctx2),
    Ctx4 = datastore_multiplier:extend_name(RoutingKey, Ctx3),
    datastore_router:route(save, [Ctx4, Key, Doc]),
    ok.
