%%%-------------------------------------------------------------------
%%% @author michal
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. Feb 2022 10:18 PM
%%%-------------------------------------------------------------------
-module(dir_stats_initialization_traverse).
-author("michal").


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

init_pool() ->
    tree_traverse:init(?MODULE, 10, 0, 5).


run(SpaceId, TraverseNum) ->
    Options = #{
        task_id => gen_task_id(SpaceId, TraverseNum)
    },
    FileCtx = file_ctx:new_by_guid(fslogic_uuid:spaceid_to_space_dir_guid(SpaceId)),
    {ok, _} = tree_traverse:run(?MODULE, FileCtx, Options),
    ok.


cancel(SpaceId, TraverseNum) ->
    tree_traverse:cancel(?MODULE, gen_task_id(SpaceId, TraverseNum)).


%%%===================================================================
%%% Pool callbacks
%%%===================================================================

do_master_job(#tree_traverse{file_ctx = FileCtx} = Job, TaskID) ->
    ok = dir_stats_collector:enable_stats_collecting(file_ctx:get_logical_guid_const(FileCtx)),
    NewJobsPreprocessor = fun(_SlaveJobs, MasterJobs, _ListExtendedInfo, _SubtreeProcessingStatus) ->
        {[], MasterJobs}
    end,
    tree_traverse:do_master_job(Job, TaskID, NewJobsPreprocessor).


do_slave_job(_, TaskID) ->
    ?warning("Not expected slave job of ~p for task ~p", [?MODULE, TaskID]),
    ok.


update_job_progress(ID, Job, Pool, TaskID, Status) ->
    tree_traverse:update_job_progress(ID, Job, Pool, TaskID, Status, ?MODULE).


get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).


-spec task_finished(id(), traverse:pool()) -> ok.
task_finished(TaskId, _PoolName) ->
    dir_stats_collector_config:report_enabling_finished(get_space_id(TaskId)).


-spec task_canceled(id(), traverse:pool()) -> ok.
task_canceled(TaskId, PoolName) ->
    % NOTE - task should be canceled using dir_stats_collector_config:disable_for_space so information about
    % cancellation is already present in config - notification about finish is enough to handle cancellation.
    task_finished(TaskId, PoolName).


%%%===================================================================
%%% Internal functions
%%%===================================================================

gen_task_id(SpaceId, TraverseNum) ->
    <<(integer_to_binary(TraverseNum))/binary, ?TASK_ID_SEPARATOR, SpaceId/binary>>.


get_space_id(TaskId) ->
    [_, SpaceId] = binary:split(TaskId, ?TASK_ID_SEPARATOR),
    SpaceId.