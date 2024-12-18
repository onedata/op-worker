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
%%%
%%% NOTE: Collections initialization traverses are used by
%%%       dir_stats_service_state to change collecting statuses.
%%%       They should not be used directly by any other module than
%%%       dir_stats_service_state.
%%% @end
%%%-------------------------------------------------------------------
-module(dir_stats_collections_initialization_traverse).
-author("Michal Wrzeszcz").


-behaviour(traverse_behaviour).


-include("tree_traverse.hrl").
-include("modules/dataset/archivisation_tree.hrl").
-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([init_pool/0, stop_pool/0, run/2, cancel/2]).
%% Pool callbacks
-export([do_master_job/2, do_slave_job/2, update_job_progress/5, get_job/1, task_finished/2, task_canceled/2]).

% Export for tests
-export([gen_task_id/2]).


-define(POOL_WORKERS, op_worker:get_env(dir_stats_collections_initialization_traverse_pool_workers, 10)).
-define(TASK_ID_SEPARATOR, "#").

%%%===================================================================
%%% API
%%%===================================================================

-spec init_pool() -> ok | no_return().
init_pool() ->
    tree_traverse:init(?MODULE, ?POOL_WORKERS, 0, 5, [?MODULE]).


-spec stop_pool() -> ok.
stop_pool() ->
    tree_traverse:stop(?MODULE).


-spec run(file_id:space_id(), non_neg_integer()) -> ok | ?ERROR_INTERNAL_SERVER_ERROR.
run(SpaceId, Incarnation) ->
    try
        Options = #{
            task_id => gen_task_id(SpaceId, Incarnation),
            listing_errors_handling_policy => propagate_unknown
        },
        FileCtx = file_ctx:new_by_guid(fslogic_file_id:spaceid_to_space_dir_guid(SpaceId)),
        {ok, _} = tree_traverse:run(?MODULE, FileCtx, Options),
        ok
    catch
        _:{badmatch, {error, not_found}} ->
            % Space dir is not found - traverse is not needed
            dir_stats_service_state:report_collections_initialization_finished(SpaceId);
        Error:Reason:Stacktrace ->
            ?error_stacktrace("Error starting stats initialization traverse for space ~tp (incarnation ~tp): ~tp:~tp",
                [SpaceId, Incarnation, Error, Reason], Stacktrace),
            ?ERROR_INTERNAL_SERVER_ERROR
    end.


-spec cancel(file_id:space_id(), non_neg_integer()) -> ok.
cancel(SpaceId, Incarnation) ->
    case tree_traverse:cancel(?MODULE, gen_task_id(SpaceId, Incarnation)) of
        ok -> ok;
        ?ERROR_NOT_FOUND -> ok
    end.


%%%===================================================================
%%% Pool callbacks
%%%===================================================================

-spec do_master_job(tree_traverse:master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_master_job(#tree_traverse{
    pagination_token = undefined, % Call dir_stats_collector only for first batch
    file_ctx = FileCtx
} = Job, MasterJobExtendedArgs) ->
    ok = dir_stats_collector:initialize_collections(file_ctx:get_logical_guid_const(FileCtx)),
    receive
        initialization_finished -> ok
    end,

    {ok, MasterJobMap} = Ans = try
        do_tree_traverse_master_job(Job, MasterJobExtendedArgs)
    catch
        error:{badmatch, {error, not_found}} ->
            {ok, #{}}
    end,
    case file_ctx:is_space_dir_const(FileCtx) of
        true ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            TrashJob = tree_traverse:get_child_master_job(Job,
                file_ctx:new_by_uuid(fslogic_file_id:spaceid_to_trash_dir_uuid(SpaceId), SpaceId), ?TRASH_DIR_NAME),
            ArchiveJob = tree_traverse:get_child_master_job(Job,
                file_ctx:new_by_uuid(archivisation_tree:get_root_dir_uuid(SpaceId), SpaceId), ?ARCHIVES_ROOT_DIR_NAME),
            TmpDirJob = tree_traverse:get_child_master_job(Job,
                file_ctx:new_by_uuid(fslogic_file_id:spaceid_to_tmp_dir_uuid(SpaceId), SpaceId), ?TMP_DIR_NAME),
            {ok, MasterJobMap#{master_jobs => [TrashJob, ArchiveJob, TmpDirJob | maps:get(master_jobs, MasterJobMap, [])]}};
        false ->
            Ans
    end;
do_master_job(Job, MasterJobExtendedArgs) ->
    do_tree_traverse_master_job(Job, MasterJobExtendedArgs).


-spec do_slave_job(tree_traverse:slave_job(), tree_traverse:id()) -> ok.
do_slave_job(_, TaskId) ->
    ?warning("Not expected slave job of ~tp for task ~tp", [?MODULE, TaskId]),
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
    dir_stats_service_state:report_collections_initialization_finished(get_space_id(TaskId)).


-spec task_canceled(tree_traverse:id(), traverse:pool()) -> ok.
task_canceled(TaskId, PoolName) ->
    % NOTE - task should be canceled using dir_stats_service_state:disable/1 so information about
    % cancellation is already present in config - notification about finish is enough to handle cancellation.
    task_finished(TaskId, PoolName).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec gen_task_id(file_id:space_id(), non_neg_integer()) -> tree_traverse:id().
gen_task_id(SpaceId, Incarnation) ->
    <<(integer_to_binary(Incarnation))/binary, ?TASK_ID_SEPARATOR, SpaceId/binary>>.


-spec get_space_id(tree_traverse:id()) -> file_id:space_id().
get_space_id(TaskId) ->
    [_IncarnationBinary, SpaceId] = binary:split(TaskId, <<?TASK_ID_SEPARATOR>>),
    SpaceId.


-spec do_tree_traverse_master_job(tree_traverse:master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_tree_traverse_master_job(#tree_traverse{file_ctx = FileCtx} = Job, MasterJobExtendedArgs) ->
    NewJobsPreprocessor = fun(_SlaveJobs, MasterJobs, _ListExtendedInfo, _SubtreeProcessingStatus) ->
        {[], MasterJobs}
    end,
    case tree_traverse:do_master_job(Job, MasterJobExtendedArgs, NewJobsPreprocessor) of
        {ok, _} = Res ->
            Res;
        {error, Reason, Stacktrace} ->
            %% @TODO VFS-11151 - log to system audit log
            FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
            ?error_exception(?autoformat_with_msg("Error when listing directory during stats initialization:",
                FileUuid), error, Reason, Stacktrace),
            ok = dir_stats_collector:update_stats_of_dir(
                file_ctx:get_logical_guid_const(FileCtx), dir_size_stats, #{?DIR_ERROR_COUNT => 1}),
            {ok, #{}}
    end.