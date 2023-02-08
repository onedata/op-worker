%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module responsible for traversing file tree and performing QoS and file meta posthooks upgrade:
%%%   - executes old style file_meta posthooks
%%%   - adds files with QoS entries to QoS structure @TODO VFS-10301
%%% @TODO VFS-6767 Remove in next major release after 21.02.*.
%%% @end
%%%--------------------------------------------------------------------
-module(qos_upgrade_traverse).
-author("Michal Stanisz").

-behavior(traverse_behaviour).

-include("global_definitions.hrl").
-include("tree_traverse.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([start/0]).

%% Traverse behaviour callbacks
-export([do_master_job/2, do_slave_job/2, task_started/2, task_finished/2,
    get_job/1, update_job_progress/5]).

-type id() :: od_space:id().

-define(POOL_NAME, qos_traverse:pool_name()).
-define(TRAVERSE_BATCH_SIZE, op_worker:get_env(qos_traverse_batch_size, 40)).

%%%===================================================================
%%% API
%%%===================================================================

-spec start() -> ok.
start() ->
    {ok, Spaces} = provider_logic:get_spaces(),
    lists:foreach(fun(SpaceId) ->
        FileCtx = file_ctx:new_by_guid(fslogic_file_id:spaceid_to_space_dir_guid(SpaceId)),
        {ok, _} = tree_traverse:run(?POOL_NAME, FileCtx, #{batch_size => ?TRAVERSE_BATCH_SIZE, task_id => SpaceId})
    end, Spaces).


%%%===================================================================
%%% Traverse callbacks
%%%===================================================================

-spec get_job(traverse:job_id()) ->
    {ok, tree_traverse:master_job(), traverse:pool(), tree_traverse:id()}  | {error, term()}.
get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).


-spec task_started(id(), traverse:pool()) -> ok.
task_started(TaskId, _PoolName) ->
    ?notice("QoS and posthooks upgrade traverse in space ~p started.", [TaskId]).


-spec task_finished(id(), traverse:pool()) -> ok.
task_finished(TaskId, _PoolName) ->
    ?notice("QoS and posthooks upgrade traverse in space ~p finished.", [TaskId]).


-spec update_job_progress(undefined | main_job | traverse:job_id(),
    tree_traverse:master_job(), traverse:pool(), id(),
    traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TaskId, Status, ?MODULE).


-spec do_master_job(tree_traverse:master_job() | tree_traverse:slave_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_master_job(Job = #tree_traverse_slave{}, #{task_id := TaskId}) ->
    do_slave_job(Job, TaskId);
do_master_job(Job, MasterJobArgs) ->
    tree_traverse:do_master_job(Job, MasterJobArgs).


-spec do_slave_job(tree_traverse:slave_job(), id()) -> ok.
do_slave_job(#tree_traverse_slave{file_ctx = FileCtx}, _TaskId) ->
    %% @TODO VFS-10301 add to qos structure if needed
    Uuid = file_ctx:get_logical_uuid_const(FileCtx),
    file_meta_posthooks:execute_hooks_deprecated(Uuid, doc),
    file_meta_posthooks:execute_hooks_deprecated(Uuid, link).