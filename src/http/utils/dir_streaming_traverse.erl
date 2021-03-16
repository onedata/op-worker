%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module responsible for tree traverse during archive download.
%%% @end
%%%--------------------------------------------------------------------
-module(dir_streaming_traverse).
-author("Michal Stanisz").

-behavior(traverse_behaviour).

-include("global_definitions.hrl").
-include("tree_traverse.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([start/3]).
-export([init_pool/0, stop_pool/0]).

%% Traverse behaviour callbacks
-export([do_master_job/2, do_slave_job/2, task_finished/2, task_canceled/2, 
    get_job/1, update_job_progress/5]).

-type id() :: tree_traverse:id().

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).

%%%===================================================================
%%% API
%%%===================================================================

start(FileCtx, SessionId, Pid) ->
    Options = #{
        batch_size => 1,
        children_master_jobs_mode => sync,
        children_dirs_handling_mode => generate_slave_and_master_jobs,
        additional_data => #{<<"connection_pid">> => transfer_utils:encode_pid(Pid)}
    },
    {ok, _} = tree_traverse:run(?POOL_NAME, FileCtx, {session, SessionId}, Options).


-spec init_pool() -> ok  | no_return().
init_pool() ->
    MasterJobsLimit = application:get_env(?APP_NAME, dir_streaming_traverse_master_jobs_limit, 100),
    SlaveJobsLimit = application:get_env(?APP_NAME, dir_streaming_traverse_slave_jobs_limit, 100),
    ParallelismLimit = application:get_env(?APP_NAME, dir_streaming_traverse_parallelism_limit, 100),

    tree_traverse:init(?MODULE, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit).


-spec stop_pool() -> ok.
stop_pool() ->
    tree_traverse:stop(?POOL_NAME).

%%%===================================================================
%%% Traverse callbacks
%%%===================================================================

-spec get_job(traverse:job_id() | tree_traverse_job:doc()) ->
    {ok, tree_traverse:master_job(), traverse:pool(), tree_traverse:id()}  | {error, term()}.
get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).

-spec task_finished(id(), traverse:pool()) -> ok.
task_finished(TaskId, PoolName) ->
    {ok, #{ <<"connection_pid">> := EncodedPid }} = traverse_task:get_additional_data(PoolName, TaskId),
    Pid = transfer_utils:decode_pid(EncodedPid),
    Pid ! done,
    ok.

-spec task_canceled(id(), traverse:pool()) -> ok.
task_canceled(_TaskId, _PoolName) ->
    ok.

-spec update_job_progress(undefined | main_job | traverse:job_id(),
    tree_traverse:master_job(), traverse:pool(), id(),
    traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TaskId, Status, ?MODULE).

-spec do_master_job(tree_traverse:master_job() | tree_traverse:slave_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_master_job(Job, MasterJobArgs) ->
    tree_traverse:do_master_job(Job, MasterJobArgs).


-spec do_slave_job(tree_traverse:slave_job(), id()) -> ok.
do_slave_job(#tree_traverse_slave{file_ctx = FileCtx}, TaskId) ->
    {ok, #{ <<"connection_pid">> := EncodedPid }} = traverse_task:get_additional_data(?POOL_NAME, TaskId),
    Pid = transfer_utils:decode_pid(EncodedPid),
    Pid ! {file_ctx, FileCtx, self()},
    case slave_job_loop(Pid) of
        ok -> ok;
        error -> 
            ?debug("Canceling dir streaming traverse ~p due to unexpected exit of connection process ~p.",
                [TaskId, Pid]),
            ok = traverse:cancel(?POOL_NAME, TaskId)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec slave_job_loop(pid()) -> ok | error.
slave_job_loop(Pid) ->
    receive
        done -> ok
    after timer:seconds(5) ->
        case is_process_alive(Pid) of
            true -> slave_job_loop(Pid);
            false -> error
        end
    end.
