%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tree traverse that initializes statistics for all directories in
%%% space.
%%% @end
%%%-------------------------------------------------------------------
-module(dir_stats_initialization_traverse).
-author("Michal Wrzeszcz").


-behaviour(traverse_behaviour).


-include_lib("ctool/include/logging.hrl").


%% API
-export([init_pool/0, run/2, cancel/2]).
%% Pool callbacks
-export([do_master_job/2, do_slave_job/2, update_job_progress/5, get_job/1, task_finished/2, task_canceled/2]).


-define(TASK_ID_SEPARATOR, "#").

%%%===================================================================
%%% API
%%%===================================================================

-spec init_pool() -> ok | no_return().
init_pool() ->
    tree_traverse:init(?MODULE, 10, 0, 5).


-spec run(file_id:space_id(), non_neg_integer()) -> ok.
run(SpaceId, TraverseNum) ->
    Options = #{task_id => gen_task_id(SpaceId, TraverseNum)},
    FileCtx = file_ctx:new_by_guid(fslogic_uuid:spaceid_to_space_dir_guid(SpaceId)),
    {ok, _} = tree_traverse:run(?MODULE, FileCtx, Options),
    ok.


-spec cancel(file_id:space_id(), non_neg_integer()) -> ok.
cancel(SpaceId, TraverseNum) ->
    ok = tree_traverse:cancel(?MODULE, gen_task_id(SpaceId, TraverseNum)).


%%%===================================================================
%%% Pool callbacks
%%%===================================================================

-spec do_master_job(tree_traverse:master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_master_job(#tree_traverse{file_ctx = FileCtx} = Job, #{task_id := TaskId}) ->
    ok = dir_stats_collector:enable_stats_collecting(file_ctx:get_logical_guid_const(FileCtx)),
    NewJobsPreprocessor = fun(_SlaveJobs, MasterJobs, _ListExtendedInfo, _SubtreeProcessingStatus) ->
        {[], MasterJobs}
    end,
    tree_traverse:do_master_job(Job, TaskId, NewJobsPreprocessor).


-spec do_slave_job(tree_traverse:slave_job(), tree_traverse:id()) -> ok.
do_slave_job(_, TaskId) ->
    ?warning("Not expected slave job of ~p for task ~p", [?MODULE, TaskId]),
    ok.


-spec update_job_progress(undefined | main_job | traverse:job_id(), tree_traverse:master_job(),
    traverse:pool(), tree_traverse:id(), traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TaskId, Status, ?MODULE).


-spec get_job(traverse:job_id())->
    {ok, tree_traverse:master_job(), traverse:pool(), tree_traverse:id()}  | {error, term()}.
get_job(DocOrId) ->
    tree_traverse:get_job(DocOrId).


-spec task_finished(tree_traverse:id(), traverse:pool()) -> ok.
task_finished(TaskId, _PoolName) ->
    dir_stats_collector_config:report_enabling_finished(get_space_id(TaskId)).


-spec task_canceled(tree_traverse:id(), traverse:pool()) -> ok.
task_canceled(TaskId, PoolName) ->
    % NOTE - task should be canceled using dir_stats_collector_config:disable_for_space/1 so information about
    % cancellation is already present in config - notification about finish is enough to handle cancellation.
    task_finished(TaskId, PoolName).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec gen_task_id(file_id:space_id(), non_neg_integer()) -> tree_traverse:id().
gen_task_id(SpaceId, TraverseNum) ->
    <<(integer_to_binary(TraverseNum))/binary, ?TASK_ID_SEPARATOR, SpaceId/binary>>.


-spec gen_task_id(tree_traverse:id()) -> file_id:space_id().
get_space_id(TaskId) ->
    [_TraverseNumBinary, SpaceId] = binary:split(TaskId, ?TASK_ID_SEPARATOR),
    SpaceId.