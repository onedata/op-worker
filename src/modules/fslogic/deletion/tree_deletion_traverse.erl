%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(tree_deletion_traverse).
-author("Jakub Kudzia").

-behavior(traverse_behaviour).

-include("global_definitions.hrl").
-include("tree_traverse.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([init_pool/0, stop_pool/0, start/2]).

%% Traverse behaviour callbacks
-export([
    task_started/2,
    task_finished/2,
    get_job/1,
    update_job_progress/5,
    do_master_job/2,
    do_slave_job/2
]).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).

-type id() :: tree_traverse:id().
%@formatter:off
-type info() :: #{
    user_ctx := user_ctx:ctx(),
    root_guid := file_id:file_guid()
}.
%@formatter:on

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init_pool() -> ok.
init_pool() ->
    MasterJobsLimit = application:get_env(?APP_NAME, tree_deletion_traverse_master_jobs_limit, 10),
    SlaveJobsLimit = application:get_env(?APP_NAME, tree_deletion_traverse_slave_jobs_limit, 10),
    ParallelismLimit = application:get_env(?APP_NAME, tree_deletion_traverse_parallelism_limit, 10),
    tree_traverse:init(?POOL_NAME, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit).


-spec stop_pool() -> ok.
stop_pool() ->
    tree_traverse:stop(?POOL_NAME).


-spec start(file_ctx:ctx(), user_ctx:ctx()) -> {ok, id()}.
start(RootDirCtx, UserCtx) ->
    Options = #{
        track_subtree_status => true,
        children_master_jobs_mode => async,
        use_token => false,
        traverse_info => #{
            user_ctx => UserCtx,
            root_guid => file_ctx:get_guid_const(RootDirCtx)
        }
    },
    % todo trzeba ogarnac rozne runy dla tego samego katalopgu
    % todo moze w addistional_info trzymac sciezke do katalogu, zeby w razie czego wiedziec gdzie był?
    tree_traverse:run(?POOL_NAME, RootDirCtx, Options).


%%%===================================================================
%%% Traverse callbacks
%%%===================================================================

-spec task_started(id(), tree_traverse:pool()) -> ok.
task_started(TaskId, _Pool) ->
    ?debug("dir deletion job ~p started", [TaskId]).

-spec task_finished(id(), tree_traverse:pool()) -> ok.
task_finished(TaskId, _Pool) ->
    ?debug("dir deletion job ~p finished", [TaskId]).

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
do_master_job(Job = #tree_traverse{
    file_ctx = FileCtx,
    traverse_info = TraverseInfo
},
    MasterJobArgs = #{task_id := TaskId}
) ->
    BatchProcessingPrehook = fun(_SlaveJobs, _MasterJobs, _ListExtendedInfo, SubtreeProcessingStatus) ->
        maybe_delete_dir(SubtreeProcessingStatus, FileCtx, TaskId, TraverseInfo)
    end,
    tree_traverse:do_master_job(Job, MasterJobArgs, BatchProcessingPrehook).


-spec do_slave_job(tree_traverse:slave_job(), id()) -> ok.
do_slave_job(#tree_traverse_slave{
    file_ctx = FileCtx,
    traverse_info = TraverseInfo = #{
        user_ctx := UserCtx
}}, TaskId) ->
    fslogic_delete:delete_file_locally(UserCtx, FileCtx, false),
    file_processed(FileCtx, TaskId, TraverseInfo).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec file_processed(file_ctx:ctx(), id(), info()) -> ok.
file_processed(FileCtx, TaskId, TraverseInfo = #{user_ctx := UserCtx}) ->
    {ParentFileCtx, _FileCtx} = file_ctx:get_parent(FileCtx, UserCtx),
    ParentUuid = file_ctx:get_uuid_const(ParentFileCtx),
    ParentStatus = tree_traverse:report_child_processed(TaskId, ParentUuid),
    maybe_delete_dir(ParentStatus, ParentFileCtx, TaskId, TraverseInfo).


%% @private
-spec maybe_delete_dir(tree_traverse_progress:status(), file_ctx:ctx(), id(), info()) -> ok.
maybe_delete_dir(?SUBTREE_PROCESSED, FileCtx, TaskId, TraverseInfo) ->
    delete_dir(FileCtx, TaskId, TraverseInfo);
maybe_delete_dir(?SUBTREE_NOT_PROCESSED, _FileCtx, _TaskId, _TraverseInfo) ->
    ok.


%% @private
-spec delete_dir(file_ctx:ctx(), id(), info()) -> ok.
delete_dir(FileCtx, TaskId, TraverseInfo = #{
    user_ctx := UserCtx,
    root_guid := RootGuid
}) ->
    fslogic_delete:delete_file_locally(UserCtx, FileCtx, false),
    tree_traverse:delete_subtree_status_doc(TaskId, file_ctx:get_uuid_const(FileCtx)),
    case file_ctx:get_guid_const(FileCtx) =:= RootGuid of
        true ->
            ok;
        false ->
            file_processed(FileCtx, TaskId, TraverseInfo)
    end.