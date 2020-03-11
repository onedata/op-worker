%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for traversing files tree in order to 
%%% perform necessary clean up after space was unsupported, i.e foreach file: 
%%%     * deletes it on storage, 
%%%     * deletes file_qos document
%%%     * deletes local file_location or dir_location document
%%% @end
%%%--------------------------------------------------------------------
-module(unsupport_cleanup_traverse).
-author("Michal Stanisz").

-behavior(traverse_behaviour).

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([
    start/2,
    init_pool/0, stop_pool/0,
    delete_ended/2,
    is_finished/1
]).

%% Traverse behaviour callbacks
-export([
    task_finished/2,
    get_job/1, 
    update_job_progress/5,
    do_master_job/2, do_slave_job/2
]).

-type id() :: traverse:id().
-export_type([id/0]).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(TRAVERSE_BATCH_SIZE, application:get_env(?APP_NAME, unsupport_cleanup_traverse_batch_size, 40)).


%%%===================================================================
%%% API
%%%===================================================================

-spec start(od_space:id(), storage:id()) -> {ok, id()}.
start(SpaceId, StorageId) ->
    Options = #{
        task_id => gen_id(SpaceId, StorageId),
        batch_size => ?TRAVERSE_BATCH_SIZE,
        %% @TODO VFS-6165 execute on dir after subtree
        execute_slave_on_dir => true,
        additional_data => #{
            <<"space_id">> => SpaceId,
            <<"storage_id">> => StorageId
        }
    },
    SpaceDirGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    {ok, _} = tree_traverse:run(?POOL_NAME, file_ctx:new_by_guid(SpaceDirGuid), Options).


-spec init_pool() -> ok  | no_return().
init_pool() ->
    MasterJobsLimit = application:get_env(?APP_NAME, unsupport_cleanup_traverse_master_jobs_limit, 10),
    SlaveJobsLimit = application:get_env(?APP_NAME, unsupport_cleanup_traverse_slave_jobs_limit, 20),
    ParallelismLimit = application:get_env(?APP_NAME, unsupport_cleanup_traverse_parallelism_limit, 20),
    
    tree_traverse:init(?MODULE, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit).


-spec stop_pool() -> ok.
stop_pool() ->
    traverse:stop_pool(?POOL_NAME).

-spec delete_ended(od_space:id(), storage:id()) -> ok.
delete_ended(SpaceId, StorageId) ->
    traverse_task:delete_ended(?POOL_NAME, gen_id(SpaceId, StorageId)).

-spec is_finished(id()) -> boolean().
is_finished(TaskId) ->
    case traverse_task:get(?POOL_NAME, TaskId) of
        {ok, #document{value = #traverse_task{status = finished}}} -> true;
        _ -> false
    end.

%%%===================================================================
%%% Traverse callbacks
%%%===================================================================

-spec task_finished(traverse:id(), traverse:pool()) -> ok.
task_finished(TaskId, Pool) ->
    {ok, #{
        <<"space_id">> := SpaceId,
        <<"storage_id">> := StorageId
    }} = traverse_task:get_additional_data(Pool, TaskId),
    space_unsupport:report_traverse_finished(SpaceId, StorageId).

-spec get_job(traverse:job_id() | tree_traverse_job:doc()) ->
    {ok, tree_traverse:master_job(), traverse:pool(), id()}  | {error, term()}.
get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).

-spec update_job_progress(undefined | main_job | traverse:job_id(),
    tree_traverse:master_job(), traverse:pool(), id(),
    traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TaskId, Status, ?MODULE).

-spec do_master_job(tree_traverse:master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_master_job(Job, MasterJobArgs) ->
    tree_traverse:do_master_job(Job, MasterJobArgs).

-spec do_slave_job(traverse:job(), id()) -> ok.
do_slave_job({#document{key = FileUuid, scope = SpaceId}, _TraverseInfo}, _TaskId) ->
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    {FileLocation, FileCtx1} = file_ctx:get_local_file_location_doc(FileCtx, false),
    ok = file_qos:delete(FileUuid),
    case file_ctx:is_dir(FileCtx1) of
        {true, FC} ->
            sd_utils:delete_storage_dir(FC, UserCtx); % this function also deletes dir_location
        {false, FC} ->
            case file_location:is_storage_file_created(FileLocation) of
                false -> ok;
                true -> sd_utils:delete_storage_file(FC, UserCtx)
            end,
            LocationId = file_location:local_id(FileUuid),
            fslogic_location_cache:clear_blocks(FC, LocationId),
            fslogic_location_cache:delete_location(FileUuid, LocationId)
    end,
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec gen_id(od_space:id(), storage:id()) -> id().
gen_id(SpaceId, StorageId) ->
    datastore_key:new_from_digest([SpaceId, StorageId]).
