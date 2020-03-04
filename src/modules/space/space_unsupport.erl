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
-export([run/2]).

-export([do_master_job/2, do_slave_job/2]).
-export([task_finished/2]).
-export([get_job/1, update_job_progress/5]).
-export([init_pool/0]).
-export([delete_ended/2]).

% for tests
-export([execute_stage/1]).

% fixme better names
-type stage() :: init | qos | clear_storage | wait_for_dbsync | clean_database | finished.

-export_type([stage/0]).


-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).

run(SpaceId, StorageId) ->
    file_meta:make_space_exist(SpaceId),
    ?ok_if_exists(traverse:run(?POOL_NAME, datastore_key:new_from_digest([SpaceId, StorageId]), 
        #space_unsupport_job{space_id = SpaceId, storage_id = StorageId, stage = init})).

init_pool() ->
    % fixme
    traverse:init_pool(?POOL_NAME, 1, 5, 5).

execute_stage(#space_unsupport_job{stage = init}) ->
    % fixme inform zone space unsupported
    % fixme start blockades
    % fixme stop all incoming transfers
    % fixme kill handles
    ok;
execute_stage(#space_unsupport_job{stage = qos, slave_job_id = <<"">>} = Job) ->
    #space_unsupport_job{space_id = SpaceId, storage_id = StorageId} = Job,
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    % fixme if preferred provider replace AllStorages with provider Id (not really if other providers still allowed)
    Expression = <<?QOS_ALL_STORAGES/binary, " - storage_id = ", StorageId/binary>>,
    {ok, QosEntryId} = lfm:add_qos_entry(?ROOT_SESS_ID, {guid, SpaceGuid}, Expression, 1, ?MODULE),
    ?notice("QosEntry: ~p", [QosEntryId]),
    NewJob = Job#space_unsupport_job{slave_job_id = QosEntryId},
    space_unsupport_job:save(NewJob),
    execute_stage(NewJob);
execute_stage(#space_unsupport_job{stage = qos, slave_job_id = QosEntryId} = _Job) ->
    wait_for_qos(QosEntryId),
    lfm:remove_qos_entry(?ROOT_SESS_ID, QosEntryId, true);
execute_stage(#space_unsupport_job{stage = clear_storage, slave_job_id = <<"">>} = Job) ->
    #space_unsupport_job{space_id = SpaceId, storage_id = StorageId} = Job,
    % fixme do not clean storage if import is on
    {ok, TaskId} = unsupport_traverse:start(SpaceId, StorageId),
    NewJob = Job#space_unsupport_job{slave_job_id = TaskId},
    {ok, _} = space_unsupport_job:save(NewJob),
    execute_stage(NewJob);
execute_stage(#space_unsupport_job{stage = clear_storage, slave_job_id = TaskId} = Job) ->
    wait_for_traverse(TaskId);
execute_stage(#space_unsupport_job{stage = wait_for_dbsync, slave_job_id = <<"">>} = _Job) ->
    % fixme better stage name (we are waiting for all docs to be persisted)
    % fixme wait for all docs to be saved on disc
    timer:sleep(timer:seconds(60)),
    % fixme stop dbsync
    ok;
execute_stage(#space_unsupport_job{stage = clean_database, slave_job_id = <<"">>} = Job) ->
    #space_unsupport_job{space_id = SpaceId, storage_id = StorageId} = Job,
    % fixme do it in init (inform zone, that space is being unsupported)
    storage_logic:revoke_space_support(StorageId, SpaceId),
    xd(SpaceId),
    % fixme  no receive
    receive end_of_stream ->
        ok
    end;
execute_stage(#space_unsupport_job{stage = finished} = Job) ->
    #space_unsupport_job{space_id = SpaceId, storage_id = StorageId} = Job,
    % fixme do not call storage (move it here?)
    % fixme file_handles
    storage:on_space_unsupported(SpaceId, StorageId),
    space_quota:delete(SpaceId),
    unsupport_traverse:delete_ended(SpaceId, StorageId),
    ok.

get_next_stage(init) -> qos;
get_next_stage(qos) -> clear_storage;
get_next_stage(clear_storage) -> wait_for_dbsync;
get_next_stage(wait_for_dbsync) -> clean_database;
get_next_stage(clean_database) -> finished.

do_master_job(#space_unsupport_job{stage = finished} = Job, _MasterJobArgs) -> 
    {ok, #{slave_jobs => [Job], master_jobs => []}};
do_master_job(#space_unsupport_job{stage = Stage} = Job, _MasterJobArgs) -> 
    ?warning("master_job in stage: ~p", [Stage]),
    NextMasterJob = Job#space_unsupport_job{stage = get_next_stage(Stage), slave_job_id = <<>>},
    {ok, #{slave_jobs => [Job], master_jobs => [NextMasterJob]}}.
        
do_slave_job(#space_unsupport_job{stage = Stage, space_id = SpaceId, storage_id = StorageId}, _TaskId) ->
    ?notice("starting stage: ~p", [Stage]),
    {ok, Job} = space_unsupport_job:get(SpaceId, StorageId, Stage),
    ?MODULE:execute_stage(Job),
    % call using module for tests
    ?notice("finished stage: ~p", [Stage]).

task_finished(TaskId, Pool) ->
    % fixme
    spawn(fun() -> timer:sleep(timer:seconds(2)), traverse_task:delete_ended(Pool, TaskId) end),
    ok.
    

% fixme
get_job(Id) ->
    {ok, Job} = space_unsupport_job:get(Id),
    {ok, Job, ?POOL_NAME, Id}.

update_job_progress(Id, _Job, _PoolName, _TaskId, Status) when 
    Status =:= ended;
    Status =:= failed;
    Status =:= canceled ->
    space_unsupport_job:delete(Id);
update_job_progress(Id, Job, _PoolName, TaskId, _Status) ->
    space_unsupport_job:save(Id, Job, TaskId).


delete_ended(SpaceId, StorageId) ->
    traverse_task:delete_ended(?POOL_NAME, datastore_key:new_from_digest([SpaceId, StorageId])).


wait_for_qos(QosEntryId) ->
    case lfm:check_qos_fulfilled(?ROOT_SESS_ID, QosEntryId) of
        {ok, true} -> ok;
        {ok, false} ->
            timer:sleep(timer:seconds(1)),
            wait_for_qos(QosEntryId)
    end.


% fixme merge with above
wait_for_traverse(TaskId) ->
    case unsupport_traverse:is_finished(TaskId) of
        true -> ok;
        false ->
            timer:sleep(timer:seconds(1)),
            wait_for_traverse(TaskId)
    end.

% fixme
-define(EXPIRY, 30).

% fixme name
xd(SpaceId) ->
    Until = dbsync_state:get_seq(SpaceId, oneprovider:get_id()),
    Pid = self(),
    Callback =
        fun ({ok, end_of_stream}) ->
                ?notice("end of stream"),
                Pid ! end_of_stream,
                ok;
            ({ok, Batch}) ->
                lists:foreach(fun(#document{key = Ads, value = Value} = Doc) ->
                    ?critical("xd: ~p", [Ads]),
                    {ok, _} = case Value of
                        #links_forest{model = Model, key = Key} ->
                            links_save(Model, Key, Doc);
                        #links_node{model = Model, key = Key} ->
                            links_save(Model, Key, Doc);
                        _ ->
                            datastore_model:save(#{
                                model => element(1, Value),
                                hooks_disabled => true,
                                expiry => ?EXPIRY
                            }, Doc)
                    end
                end, Batch);
            (Dupa) ->
                ?critical("~p", [Dupa])
        end,
    couchbase_changes_stream:start_link(
        couchbase_changes:design(), SpaceId, Callback,
        % fixme listing from 0 generates errors if documents do not exist 
        [{since, 0}, {until, Until}], [self()]
    ).

links_save(Model, RoutingKey, Doc = #document{key = Key}) ->
    Ctx = Model:get_ctx(),
    Ctx2 = Ctx#{
        expiry => ?EXPIRY,
        routing_key => RoutingKey
    },
    Ctx3 = datastore_model_default:set_defaults(Ctx2),
    Ctx4 = datastore_multiplier:extend_name(RoutingKey, Ctx3),
    datastore_router:route(save, [Ctx4, Key, Doc]).
