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
% sth like clear storage traverse?
-module(unsupport_traverse).
-author("Michal Stanisz").

-behavior(traverse_behaviour).

-include("global_definitions.hrl").
-include("modules/datastore/qos.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start/2, init_pool/0,
    stop_pool/0]).

%% Traverse behaviour callbacks
-export([do_master_job/2, do_slave_job/2, task_finished/2,
    get_job/1, update_job_progress/5]).

-type task_type() :: traverse | reconcile.
-export_type([task_type/0]).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(TRAVERSE_BATCH_SIZE, application:get_env(?APP_NAME, qos_traverse_batch_size, 40)).


%%%===================================================================
%%% API
%%%===================================================================

% fixme
start(SpaceId, StorageId) ->
    Options = #{
        task_id => datastore_key:new_from_digest([SpaceId, StorageId]),
        batch_size => ?TRAVERSE_BATCH_SIZE,
        execute_slave_on_dir => true,
        additional_data => #{
            <<"space_id">> => SpaceId,
            <<"storage_id">> => StorageId
        }
    },
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    {ok, _} = tree_traverse:run(?POOL_NAME, file_ctx:new_by_guid(SpaceGuid), Options).

%%--------------------------------------------------------------------
%% @doc
%% Initializes pool for traverse tasks concerning QoS management.
%% @end
%%--------------------------------------------------------------------
-spec init_pool() -> ok  | no_return().
init_pool() ->
    % Get pool limits from app.config
    % fixme 
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

-spec update_job_progress(undefined | main_job | traverse:job_id(),
    tree_traverse:master_job(), traverse:pool(), traverse:id(),
    traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TaskId, Status, ?MODULE).

-spec task_finished(traverse:id(), traverse:pool()) -> ok.
task_finished(TaskId, _PoolName) ->
    {ok, #{
        <<"space_id">> := SpaceId,
        <<"storage_id">> := StorageId
    }} = traverse_task:get_additional_data(?POOL_NAME, TaskId),
    space_unsupport_worker:next_stage(SpaceId, StorageId),
    traverse_task:delete_ended(?POOL_NAME, TaskId),
    ok.

-spec do_master_job(tree_traverse:master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_master_job(Job, MasterJobArgs) ->
    tree_traverse:do_master_job(Job, MasterJobArgs).

-spec do_slave_job(traverse:job(), traverse:id()) -> ok.
do_slave_job({#document{key = FileUuid, scope = SpaceId}, _TraverseInfo}, _TaskId) ->
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    case file_ctx:is_dir(FileCtx) of
        {true, FC} ->
            sd_utils:delete_storage_dir(FC, UserCtx);
        {false, FC} ->
            sd_utils:delete_storage_file(FC, UserCtx)
    end,
    ok.

