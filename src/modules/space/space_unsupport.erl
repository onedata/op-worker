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

-include("modules/datastore/qos.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

-behaviour(traverse_behaviour).

%% API
-export([run/2]).

-export([do_master_job/2, do_slave_job/2]).
-export([get_job/1, update_job_progress/5]).
-export([init_pool/0]).
-export([delete_ended/2]).

-type stage() :: init | qos | clear_storage | wait_for_dbsync | clean_database.

-export_type([stage/0]).


-record(job, {
    stage = init :: stage(),
    space_id :: od_space:id(),
    storage_id :: storage:id(),
    id = undefined :: undefined | traverse:id() | qos_entry:id()
}).


-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).

run(SpaceId, StorageId) ->
    file_meta:make_space_exist(SpaceId),
    % fixme ok if already exists
    traverse:run(?POOL_NAME, datastore_key:new_from_digest([SpaceId, StorageId]), 
        #job{space_id = SpaceId, storage_id = StorageId, stage = init}).

init_pool() ->
    traverse:init_pool(?POOL_NAME, 1, 5, 5).

execute_stage(init, _SpaceId, _StorageId) ->
    % fixme inform zone space unsupported
    % fixme start blockades
    % fixme stop all incoming transfers
    % fixme kill handles
    % fixme mozna zrownoleglić (+ qos) -> uzyc traversu 
    ok;
execute_stage(qos, SpaceId, StorageId) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    % fixme if preferred provider replace AllStorages with provider Id (not really if other providers still allowed)
    Expression = <<?ALL_STORAGES/binary, " - storage_id = ", StorageId/binary>>,
    {ok, QosEntryId} = lfm:add_qos_entry(?ROOT_SESS_ID, {guid, SpaceGuid}, Expression, 1, ?MODULE),
    ?notice("QosEntry: ~p", [QosEntryId]),
    wait_for_qos(QosEntryId),
    lfm:remove_qos_entry(?ROOT_SESS_ID, QosEntryId, true);
execute_stage(clear_storage, SpaceId, StorageId) ->
    % fixme do not clean storage if import is on
    {ok, TaskId} = unsupport_traverse:start(SpaceId, StorageId),
    ?notice("TaskId: ~p", [TaskId]),
    % fixme  no receive
    receive end_of_traverse ->
        ok
        % fixme delete space root dir
    end;
execute_stage(wait_for_dbsync, _SpaceId, _StorageId) ->
    % fixme wait for all docs to be saved on disc
    timer:sleep(timer:seconds(60)),
    % fixme stop dbsync
    ok;
execute_stage(clean_database, SpaceId, StorageId) ->
    % fixme do it in init (inform zone, that space is being unsupported)
    storage_logic:revoke_space_support(StorageId, SpaceId),
    xd(SpaceId),
    % fixme  no receive
    receive end_of_stream ->
        % fixme do not call storage (move it here?)
        % fixme file_handles
        storage:on_space_unsupported(SpaceId, StorageId),
        space_quota:delete(SpaceId),
        unsupport_traverse:delete_ended(SpaceId, StorageId)
    end,
    ok.

get_next_stage(init) -> qos;
get_next_stage(qos) -> clear_storage;
get_next_stage(clear_storage) -> wait_for_dbsync;
get_next_stage(wait_for_dbsync) -> clean_database;
get_next_stage(clean_database) -> finished.

do_master_job(#job{stage = Stage} = Job, _MasterJobArgs) -> 
    ?warning("master_job in stage: ~p", [Stage]),
    MasterJobs = case get_next_stage(Stage) of
        finished -> [];
        NextStage -> [Job#job{stage = NextStage}]
    end,
    {ok, #{slave_jobs => [Job], master_jobs => MasterJobs}}.
        
do_slave_job(#job{stage = Stage, space_id = SpaceId, storage_id = StorageId}, _TaskId) ->
    ?notice("starting stage: ~p", [Stage]),
    execute_stage(Stage, SpaceId, StorageId),
    ?notice("finished stage: ~p", [Stage]).

% fixme
get_job(Id) ->
    {ok, #job{}, ?POOL_NAME, Id}.

update_job_progress(main_job,_,_,_,_) ->
    {ok, datastore_key:new()};
update_job_progress(undefined,_,_,_,_) ->
    {ok, datastore_key:new()};
update_job_progress(Id,_,_,_,_) when is_binary(Id) ->
    {ok, Id}.


delete_ended(SpaceId, StorageId) ->
    traverse_task:delete_ended(?POOL_NAME, datastore_key:new_from_digest([SpaceId, StorageId])).


wait_for_qos(QosEntryId) ->
    case lfm:check_qos_fulfilled(?ROOT_SESS_ID, QosEntryId) of
        {ok, true} -> ok;
        {ok, false} ->
            timer:sleep(timer:seconds(1)),
            wait_for_qos(QosEntryId)
    end.

% fixme
-define(EXPIRY, 30).

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
