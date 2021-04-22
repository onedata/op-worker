%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module responsible for tree traverse during archive download.
%%% Starts new traverse for each directory on the list. Files without access are ignored.
%%% Uses user's offline session to ensure that download may progress even when client 
%%% disconnects from provider. Traverse is started with single master job mode, 
%%% so at most one file is being processed at a time.
%%% @end
%%%--------------------------------------------------------------------
-module(bulk_download_traverse).
-author("Michal Stanisz").

-behavior(traverse_behaviour).

-include("modules/bulk_download/bulk_download.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("tree_traverse.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([start/3]).
-export([init_pool/0, stop_pool/0]).
-export([get_pool_name/0]).

%% Traverse behaviour callbacks
-export([do_master_job/2, do_slave_job/2, task_finished/2, task_canceled/2, 
    get_job/1, update_job_progress/5]).

-type id() :: tree_traverse:id().

-export_type([id/0]).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).

%%%===================================================================
%%% API
%%%===================================================================

start(TaskId, UserCtx, Guid) ->
    %% @TODO VFS-6212 start traverse with cleanup option
    traverse_task:delete_ended(?POOL_NAME, TaskId),
    Options = #{
        task_id => TaskId,
        batch_size => 1,
        children_master_jobs_mode => sync,
        child_dirs_job_generation_policy => generate_slave_and_master_jobs,
        additional_data => #{<<"main_pid">> => transfer_utils:encode_pid(self())},
        master_job_mode => single
    },
    {ok, _} = tree_traverse:run(
        ?POOL_NAME, file_ctx:new_by_guid(Guid), user_ctx:get_user_id(UserCtx), Options).

-spec init_pool() -> ok  | no_return().
init_pool() ->
    MasterJobsLimit = op_worker:get_env(tarball_streaming_traverse_master_jobs_limit, 50),
    SlaveJobsLimit = op_worker:get_env(tarball_streaming_traverse_slave_jobs_limit, 50),
    ParallelismLimit = op_worker:get_env(tarball_streaming_traverse_parallelism_limit, 50),

    ok = tree_traverse:init(?MODULE, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit).


-spec stop_pool() -> ok.
stop_pool() ->
    tree_traverse:stop(?POOL_NAME).

get_pool_name() ->
    ?POOL_NAME.

%%%===================================================================
%%% Traverse callbacks
%%%===================================================================

-spec get_job(traverse:job_id() | tree_traverse_job:doc()) ->
    {ok, tree_traverse:master_job(), traverse:pool(), tree_traverse:id()}  | {error, term()}.
get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).


-spec task_finished(id(), traverse:pool()) -> ok.
task_finished(TaskId, _PoolName) ->
    Pid = get_main_pid(TaskId),
    Pid ! done,
    ok.


-spec task_canceled(id(), traverse:pool()) -> ok.
task_canceled(TaskId, PoolName) ->
    task_finished(TaskId, PoolName).


-spec update_job_progress(undefined | main_job | traverse:job_id(),
    tree_traverse:master_job(), traverse:pool(), id(),
    traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TaskId, Status, ?MODULE).


-spec do_master_job(tree_traverse:master_job() | tree_traverse:slave_job(), 
    traverse:master_job_extended_args()) -> {ok, traverse:master_job_map()}.
do_master_job(Job, MasterJobArgs) ->
    tree_traverse:do_master_job(Job, MasterJobArgs).


-spec do_slave_job(tree_traverse:slave_job(), id()) -> ok.
do_slave_job(#tree_traverse_slave{file_ctx = FileCtx, user_id = UserId}, TaskId) ->
    {ok, UserCtx} = tree_traverse_session:acquire_for_task(UserId, ?POOL_NAME, TaskId),
    #fuse_response{status = #status{code = ?OK}, fuse_response = FileAttrs} = 
        attr_req:get_file_attr(UserCtx, FileCtx, false, false),
    Pid = get_main_pid(TaskId),
    Pid ! ?MSG_NEXT_FILE(FileAttrs, self()),
    case slave_job_loop(Pid) of
        ok -> 
            ok;
        error -> 
            ?debug("Canceling dir streaming traverse ~p due to unexpected exit "
                   "of download process ~p.", [TaskId, Pid]),
            ok = traverse:cancel(?POOL_NAME, TaskId)
    end.


%%%===================================================================
%%% Internal functions run by traverse pool processes
%%%===================================================================

%% @private
-spec slave_job_loop(pid()) -> ok | error.
slave_job_loop(Pid) ->
    receive
        ?MSG_DONE -> ok
    after timer:seconds(5) ->
        case is_process_alive(Pid) of
            true -> slave_job_loop(Pid);
            false -> error
        end
    end.


%% @private
-spec get_main_pid(id()) -> pid().
get_main_pid(TaskId) ->
    {ok, #{ <<"main_pid">> := EncodedPid }} =
        traverse_task:get_additional_data(?POOL_NAME, TaskId),
    transfer_utils:decode_pid(EncodedPid).
