%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides base functionality for directory tree traversing.
%%% It bases on traverse framework (see traverse.erl in cluster_worker).
%%% The module provides functions for pool and tasks start and implementation of some callbacks.
%%% To use tree traverse, new callback module has to be defined (see traverse_behaviour.erl from cluster_worker) that
%%% uses callbacks defined in this module and additionally provides do_slave_job function implementation. Next,
%%% pool and tasks are started using init and run functions from this module.
%%% The traverse jobs (see traverse.erl for jobs definition) are persisted using tree_traverse_job datastore model
%%% which stores jobs locally and synchronizes main job for each task between providers (to allow tasks execution
%%% on other provider resources).
%%% @end
%%%-------------------------------------------------------------------
-module(tree_traverse).
-author("Michal Wrzeszcz").

% This module is a base for traverse callback
% (other callbacks especially for slave jobs have to be defined)

-include("tree_traverse.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").

%% Main API
-export([init/4, init/5, stop/1, run/3, run/4, cancel/2]).
% Getters API
-export([get_traverse_info/1, set_traverse_info/2, get_task/2, get_sync_info/0]).
%% Behaviour callbacks
-export([do_master_job/2, do_master_job/3, update_job_progress/6, get_job/1, get_sync_info/1, get_timestamp/0]).

%% Tracking subtree progress status API
-export([report_child_processed/2, delete_subtree_status_doc/2]).


-type id() :: traverse:id().
-type pool() :: traverse:pool().
-type master_job() :: #tree_traverse{}.
-type master_jobs() :: [master_job()].
-type slave_job() :: #tree_traverse_slave{}.
-type slave_jobs() :: [slave_job()].
-type execute_slave_on_dir() :: boolean().
-type children_master_jobs_mode() :: sync | async.
-type batch_size() :: non_neg_integer().
-type traverse_info() :: term().
-type run_options() :: #{
    % Options of traverse framework
    task_id => id(),
    callback_module => traverse:callback_module(),
    group_id => traverse:group(),
    additional_data => traverse:additional_data(),
    % Options used to create jobs
    execute_slave_on_dir => execute_slave_on_dir(),
    % flag determining whether token should be used for iterating over file_meta links
    % token shouldn't be used when links may be deleted from tree
    use_listing_token => boolean(),
    % flag determining whether children master jobs are scheduled before slave jobs are processed
    children_master_jobs_mode => children_master_jobs_mode(),
    % With this option enabled, tree_traverse_status will be
    % created for each directory and used to track progress of execution of children jobs.
    track_subtree_status => boolean(),
    batch_size => batch_size(),
    traverse_info => traverse_info(),
    % Provider which should execute task
    target_provider_id => oneprovider:id()
}.


%formatter:off
% This callback is executed after generating children jobs but before executing them (before returning
% traverse:master_job_map()).
-type new_jobs_preprocessor() ::
    fun((
        slave_jobs(),
        master_jobs(),
        file_meta:list_extended_info(),
        SubtreeProcessingStatus :: tree_traverse_progress:status()
    ) -> ok).
%formatter:on


-export_type([id/0, pool/0, master_job/0, slave_job/0, execute_slave_on_dir/0,
    children_master_jobs_mode/0, batch_size/0, traverse_info/0]).

%%%===================================================================
%%% Main API
%%%===================================================================

-spec init(traverse:pool() | atom(), non_neg_integer(), non_neg_integer(), non_neg_integer()) -> ok | no_return().
init(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit) when is_atom(Pool) ->
    init(atom_to_binary(Pool, utf8), MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit);
init(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit) ->
    traverse:init_pool(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit,
        #{executor => oneprovider:get_id_or_undefined()}).

-spec init(traverse:pool() | atom(), non_neg_integer(), non_neg_integer(), non_neg_integer(),
    [traverse:callback_module()]) -> ok  | no_return().
init(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, CallbackModules) when is_atom(Pool) ->
    init(atom_to_binary(Pool, utf8), MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, CallbackModules);
init(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, CallbackModules) ->
    traverse:init_pool(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit,
        #{executor => oneprovider:get_id_or_undefined(), callback_modules => CallbackModules}).

-spec stop(traverse:pool() | atom()) -> ok.
stop(Pool) when is_atom(Pool) ->
    stop(atom_to_binary(Pool, utf8));
stop(Pool) ->
    traverse:stop_pool(Pool).


-spec run(traverse:pool() | atom(), file_meta:doc() | file_ctx:ctx(), run_options()) -> {ok, id()}.
run(Pool, DocOrCtx, Opts)  ->
    run(Pool, DocOrCtx, undefined, Opts).

-spec run(traverse:pool() | atom(), file_meta:doc() | file_ctx:ctx(), user_ctx:ctx() | undefined, run_options()) ->
    {ok, id()}.
run(Pool, DocOrCtx, UserCtx, Opts) when is_atom(Pool) ->
    run(atom_to_binary(Pool, utf8), DocOrCtx, UserCtx, Opts);
run(Pool, FileDoc = #document{scope = SpaceId, value = #file_meta{}}, UserCtx, Opts) ->
    FileCtx = file_ctx:new_by_doc(FileDoc, SpaceId),
    run(Pool, FileCtx, UserCtx, Opts);
run(Pool, FileCtx, UserCtx, Opts) ->
    TaskId = case maps:get(task_id, Opts, undefined) of
        undefined -> datastore_key:new();
        Id -> Id
    end,
    % TODO VFS-7101 use offline access token in tree_traverse
    UserCtx2 = case UserCtx =/= undefined of
        true -> UserCtx;
        false -> user_ctx:new(?ROOT_SESS_ID)
    end,
    BatchSize = maps:get(batch_size, Opts, ?DEFAULT_BATCH_SIZE),
    ExecuteSlaveOnDir = maps:get(execute_slave_on_dir, Opts, ?DEFAULT_EXEC_SLAVE_ON_DIR),
    ChildrenMasterJobsMode = maps:get(children_master_jobs, Opts, ?DEFAULT_CHILDREN_MASTER_JOBS_MODE),
    TrackSubtreeStatus = maps:get(track_subtree_status, Opts, ?DEFAULT_TRACK_SUBTREE_STATUS),
    TraverseInfo = maps:get(traverse_info, Opts, undefined),
    Token = case maps:get(use_listing_token, Opts, true) of
        true -> #link_token{};
        false -> undefined
    end,

    RunOpts = case maps:get(target_provider_id, Opts, undefined) of
        undefined -> #{executor => oneprovider:get_id_or_undefined()};
        TargetId -> #{creator => oneprovider:get_id_or_undefined(), executor => TargetId}
    end,
    RunOpts2 = case maps:get(callback_module, Opts, undefined) of
        undefined -> RunOpts;
        CM -> RunOpts#{callback_module => CM}
    end,
    RunOpts3 = case maps:get(group_id, Opts, undefined) of
        undefined -> RunOpts2;
        Group -> RunOpts2#{group_id => Group}
    end,
    RunOpts4 = case maps:get(additional_data, Opts, undefined) of
        undefined -> RunOpts3;
        AdditionalData -> RunOpts3#{additional_data => AdditionalData}
    end,

    Job = #tree_traverse{
        file_ctx = FileCtx,
        user_ctx = UserCtx2,
        token = Token,
        execute_slave_on_dir = ExecuteSlaveOnDir,
        children_master_jobs_mode = ChildrenMasterJobsMode,
        track_subtree_status = TrackSubtreeStatus,
        batch_size = BatchSize,
        traverse_info = TraverseInfo
    },
    maybe_create_status_doc(Job, TaskId),
    ok = traverse:run(Pool, TaskId, Job, RunOpts4),
    {ok, TaskId}.

-spec cancel(traverse:pool() | atom(), id()) -> ok | {error, term()}.
cancel(Pool, TaskId) when is_atom(Pool) ->
    cancel(atom_to_binary(Pool, utf8), TaskId);
cancel(Pool, TaskId) ->
    traverse:cancel(Pool, TaskId, oneprovider:get_id_or_undefined()).

%%%===================================================================
%%% Getters/Setters API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Provides traverse info from master job record.
%% @end
%%--------------------------------------------------------------------
-spec get_traverse_info(master_job()) -> traverse_info().
get_traverse_info(#tree_traverse{traverse_info = TraverseInfo}) ->
    TraverseInfo.

%%--------------------------------------------------------------------
%% @doc
%% Sets traverse info in master job record.
%% @end
%%--------------------------------------------------------------------
-spec set_traverse_info(master_job(), traverse_info()) -> master_job().
set_traverse_info(TraverseJob, TraverseInfo) ->
    TraverseJob#tree_traverse{traverse_info = TraverseInfo}.


-spec get_task(traverse:pool() | atom(), id()) -> {ok, traverse_task:doc()} | {error, term()}.
get_task(Pool, Id) when is_atom(Pool) ->
    get_task(atom_to_binary(Pool, utf8), Id);
get_task(Pool, Id) ->
    traverse_task:get(Pool, Id).

%%--------------------------------------------------------------------
%% @doc
%% Provides information needed for document synchronization.
%% Warning: if any traverse callback module uses other sync info than one provided by tree_traverse,
%% dbsync_changes:get_ctx function has to be extended to parse #document and get callback module.
%% @end
%%--------------------------------------------------------------------
-spec get_sync_info() -> traverse:sync_info().
get_sync_info() ->
    Provider = oneprovider:get_id_or_undefined(),
    #{
        sync_enabled => true,
        remote_driver => datastore_remote_driver,
        mutator => Provider,
        local_links_tree_id => Provider
    }.

%%%===================================================================
%%% Behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv do_master_job(Job, MasterJobArgs, fun(_,_,_,_,_,_) -> ok end, fun(_,_,_) -> ok end, sync).
%% @end
%%--------------------------------------------------------------------
-spec do_master_job(master_job(), traverse:master_job_extended_args()) -> 
    {ok, traverse:master_job_map()}.
do_master_job(Job, MasterJobArgs) ->
    do_master_job(Job, MasterJobArgs, ?NEW_JOBS_DEFAULT_PREPROCESSOR).

%%--------------------------------------------------------------------
%% @doc
%% Does master job that traverse directory tree. The job lists directory (number of listed children is limited) and
%% returns jobs for listed children and next batch if needed.
%% @end
%%--------------------------------------------------------------------
-spec do_master_job(master_job(), traverse:master_job_extended_args(), new_jobs_preprocessor()) ->
    {ok, traverse:master_job_map()}.
do_master_job(Job = #tree_traverse{
    file_ctx = FileCtx,
    children_master_jobs_mode = ChildrenMasterJobsMode
},
    #{task_id := TaskId},
    NewJobsPreprocessor
) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    Job2 = Job#tree_traverse{file_ctx = FileCtx2},
    case file_meta:get_type(FileDoc) of
        ?REGULAR_FILE_TYPE ->
            {ok, #{slave_jobs => [get_child_slave_job(Job2, FileCtx2)]}};
        ?DIRECTORY_TYPE ->
            {ChildrenCtxs, ListExtendedInfo, FileCtx3, IsLast} = list_children(Job2),
            LastName2 = maps:get(last_name, ListExtendedInfo),
            LastTree2 = maps:get(last_tree, ListExtendedInfo),
            Token2 = maps:get(token, ListExtendedInfo, undefined),
            {SlaveJobs, MasterJobs} = generate_children_jobs(Job2, TaskId, ChildrenCtxs),
            ChildrenCount = length(SlaveJobs) + length(MasterJobs),
            SubtreeProcessingStatus = maybe_report_children_jobs_to_process(Job2, TaskId, ChildrenCount, IsLast),
            NewJobsPreprocessor(SlaveJobs, MasterJobs, ListExtendedInfo, SubtreeProcessingStatus),
            FinalMasterJobs = case IsLast of
                true ->
                    MasterJobs;
                false -> [Job2#tree_traverse{
                    file_ctx = FileCtx3,
                    token = Token2,
                    last_name = LastName2,
                    last_tree = LastTree2
                } | MasterJobs]
            end,

            ChildrenMasterJobsKey = case ChildrenMasterJobsMode of
                sync -> master_jobs;
                async -> async_master_jobs
            end,
            {ok, #{slave_jobs => SlaveJobs, ChildrenMasterJobsKey => FinalMasterJobs}}
        end.


%%--------------------------------------------------------------------
%% @doc
%% Updates information about master jobs saving it in datastore or deleting if it is finished or canceled.
%% @end
%%--------------------------------------------------------------------
-spec update_job_progress(undefined | main_job | traverse:job_id(),
    master_job(), traverse:pool(), id(), traverse:job_status(), traverse:callback_module()) ->
    {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status, CallbackModule) when Status =:= waiting ; Status =:= on_pool ->
    tree_traverse_job:save_master_job(Id, Job, Pool, TaskId, CallbackModule);
update_job_progress(Id, Job = #tree_traverse{file_ctx = FileCtx}, _, _, _, CallbackModule) ->
    Scope = file_ctx:get_space_id_const(FileCtx),
    ok = tree_traverse_job:delete_master_job(Id, Job, Scope, CallbackModule),
    {ok, Id}.


-spec get_job(traverse:job_id() | tree_traverse_job:doc()) ->
    {ok, master_job(), traverse:pool(), id()}  | {error, term()}.
get_job(DocOrId) ->
    tree_traverse_job:get_master_job(DocOrId).


%%--------------------------------------------------------------------
%% @doc
%% Provides information needed for task document synchronization basing on file_meta scope.
%% @end
%%--------------------------------------------------------------------
-spec get_sync_info(master_job()) -> {ok, traverse:sync_info()}.
get_sync_info(#tree_traverse{file_ctx = FileCtx}) ->
    Scope = file_ctx:get_space_id_const(FileCtx),
    Info = get_sync_info(),
    {ok, Info#{scope => Scope}}.


%%--------------------------------------------------------------------
%% @doc
%% Provides timestamp used for tasks listing.
%% @end
%%--------------------------------------------------------------------
-spec get_timestamp() -> {ok, traverse:timestamp()}.
get_timestamp() ->
    {ok, global_clock:timestamp_seconds()}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec report_child_processed(id(), file_meta:uuid()) -> tree_traverse_progress:status().
report_child_processed(TaskId, ParentUuid) ->
    tree_traverse_progress:report_child_processed(TaskId, ParentUuid).


-spec delete_subtree_status_doc(id(), file_meta:uuid()) -> ok | {error, term()}.
delete_subtree_status_doc(TaskId, Uuid) ->
    tree_traverse_progress:delete(TaskId, Uuid).

%%%===================================================================
%% Tracking subtree progress status API
%%%===================================================================

-spec list_children(master_job()) ->
    {
        Children :: [file_ctx:ctx()],
        ListExtendedInfo :: file_meta:list_extended_info(),
        NewFileCtx :: file_ctx:ctx(),
        IsLast :: boolean()
    }.
list_children(#tree_traverse{
    file_ctx = FileCtx,
    user_ctx = UserCtx,
    token = Token,
    last_name = LastName,
    last_tree = LastTree,
    batch_size = BatchSize
}) ->
    dir_req:get_children_ctxs(UserCtx, FileCtx, 0, BatchSize, Token, LastName, LastTree).


-spec generate_children_jobs(master_job(), id(), [file_ctx:ctx()]) -> {[slave_job()], [master_job()]}.
generate_children_jobs(MasterJob = #tree_traverse{execute_slave_on_dir = ExecOnDir}, TaskId, Children) ->
    {SlaveJobsReversed, MasterJobsReversed} = lists:foldl(fun(ChildCtx, {SlavesAcc, MastersAcc} = Acc) ->
        try
            {ChildDoc, ChildCtx2} = file_ctx:get_file_doc(ChildCtx),
            case file_meta:get_type(ChildDoc) of
                ?DIRECTORY_TYPE ->
                    ChildMasterJob = get_child_master_job(MasterJob, ChildCtx2),
                    maybe_create_status_doc(ChildMasterJob, TaskId),
                    case ExecOnDir of
                        true ->
                            ChildSlaveJob = get_child_slave_job(MasterJob, ChildCtx2),
                            {[ChildSlaveJob | SlavesAcc], [ChildMasterJob | MastersAcc]};
                        false ->
                            {SlavesAcc, [ChildMasterJob | MastersAcc]}
                    end;
                ?REGULAR_FILE_TYPE ->
                    {[get_child_slave_job(MasterJob, ChildCtx2) | SlavesAcc], MastersAcc}
            end
        catch
            _:{badmatch, {error, not_found}} ->
                Acc
        end
    end, {[], []}, Children),
    {lists:reverse(SlaveJobsReversed), lists:reverse(MasterJobsReversed)}.


-spec get_child_master_job(master_job(), file_ctx:ctx()) -> master_job().
get_child_master_job(MasterJob, ChildCtx) ->
    MasterJob2 = reset_list_options(MasterJob),
    MasterJob2#tree_traverse{file_ctx = ChildCtx}.


-spec get_child_slave_job(master_job(), file_ctx:ctx()) -> slave_job().
get_child_slave_job(#tree_traverse{
    traverse_info = TraverseInfo,
    track_subtree_status = TrackSubtreeStatus
}, ChildCtx) ->
    #tree_traverse_slave{
        file_ctx = ChildCtx,
        traverse_info = TraverseInfo,
        track_subtree_status = TrackSubtreeStatus
    }.


-spec maybe_create_status_doc(master_job(), id()) -> ok | {error, term()}.
maybe_create_status_doc(#tree_traverse{
    file_ctx = FileCtx,
    track_subtree_status = true
}, TaskId) ->
    Uuid = file_ctx:get_uuid_const(FileCtx),
    tree_traverse_progress:create(TaskId, Uuid);
maybe_create_status_doc(_, _) ->
    ok.


-spec maybe_report_children_jobs_to_process(master_job(), id(), non_neg_integer(), boolean()) ->
    tree_traverse_progress:status() | undefined.
maybe_report_children_jobs_to_process(#tree_traverse{
    file_ctx = FileCtx,
    track_subtree_status = true
}, TaskId, ChildrenCount, AllBatchesListed) ->
    Uuid = file_ctx:get_uuid_const(FileCtx),
    tree_traverse_progress:report_children_to_process(TaskId, Uuid, ChildrenCount, AllBatchesListed);
maybe_report_children_jobs_to_process(_, _, _, _) ->
    undefined.


-spec reset_list_options(master_job()) -> master_job().
reset_list_options(Job) ->
    Job#tree_traverse{
        token = #link_token{},
        last_name = <<>>,
        last_tree = <<>>
    }.