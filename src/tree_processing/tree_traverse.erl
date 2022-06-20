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
%%%
%%% It is possible to perform traverse in context of a ?ROOT_USER or a normal user.
%%% ?ROOT_USER is used by default.
%%% If traverse is scheduled in context of a normal user, its offline session
%%% will be used to ensure that traverse may progress even when client disconnects
%%% from provider.
%%%
%%% NOTE !!!
%%% It is a responsibility of the calling module to init and close
%%% offline access session !!!
%%% @end
%%%-------------------------------------------------------------------
-module(tree_traverse).
-author("Michal Wrzeszcz").

% This module is a base for traverse callback
% (other callbacks especially for slave jobs have to be defined)

-include("tree_traverse.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
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

%% Helper functions
-export([get_child_master_job/3, get_child_slave_job/3]).
-export([resolve_symlink/3]).


-type id() :: traverse:id().
-type pool() :: traverse:pool().
-type master_job() :: #tree_traverse{}.
-type master_jobs() :: [master_job()].
-type slave_job() :: #tree_traverse_slave{}.
-type slave_jobs() :: [slave_job()].
-type job() :: master_job() | slave_job().
-type child_dirs_job_generation_policy () :: generate_master_jobs | generate_slave_and_master_jobs.
-type children_master_jobs_mode() :: sync | async.
-type batch_size() :: file_listing:limit().
-type traverse_info() :: map().
% Symbolic links resolution policy: 
%   * preserve - every symbolic link encountered during traverse is passed as is to the slave job; 
%   * follow_all - every valid symbolic link is resolved and target file is passed to the slave job, 
%                  invalid symbolic links (e.g. infinite loops or targeting non existing files) are ignored; 
%   * follow_external - only symbolic links targeting outside of traverse subtree are resolved, 
%                       invalid symbolic links are ignored
-type symlink_resolution_policy() :: preserve | follow_all | follow_external.
-type run_options() :: #{
    % Options of traverse framework
    task_id => id(),
    callback_module => traverse:callback_module(),
    group_id => traverse:group(),
    additional_data => traverse:additional_data(),
    % Options used to create jobs

    % option determining whether slave jobs should be generated also for child directories.
    % NOTE: slave job for starting directory will never be generated.
    child_dirs_job_generation_policy => child_dirs_job_generation_policy(), 
    % Flag determining whether optimization will be used for iterating over files list (see file_listing for more details).
    tune_for_large_continuous_listing => boolean(),
    % flag determining whether children master jobs are scheduled before slave jobs are processed
    children_master_jobs_mode => children_master_jobs_mode(),
    % With this option enabled, tree_traverse_status will be
    % created for each directory and used to track progress of execution of children jobs.
    track_subtree_status => boolean(),
    batch_size => batch_size(),
    traverse_info => traverse_info(),
    % Provider which should execute task
    target_provider_id => oneprovider:id(),
    % if set to 'single', only one master job is performed in parallel for each task - see master_job_mode type definition
    master_job_mode => traverse:master_job_mode(),
    % if set to `true` all encountered symlinks will be resolved
    symlink_resolution_policy => symlink_resolution_policy(),
    initial_relative_path => file_meta:path()
}.


%formatter:off
% This callback is executed after generating children jobs but before executing them (before returning
% traverse:master_job_map()).
-type new_jobs_preprocessor() ::
    fun((
        slave_jobs(),
        master_jobs(),
        file_listing:pagination_token() | undefined,
        SubtreeProcessingStatus :: tree_traverse_progress:status() | {error, term()}
    ) -> ok | {slave_jobs(), master_jobs()}).

%formatter:on

% Set of encountered files on the path from the traverse root to the currently processed one. 
% It is required to efficiently prevent loops when resolving symlinks.
% Implemented as a map with single possible value (`true`) for performance reason.
-type encountered_files_set() :: #{file_meta:uuid() => true}.

-export_type([id/0, pool/0, job/0, master_job/0, slave_job/0, child_dirs_job_generation_policy/0,
    children_master_jobs_mode/0, batch_size/0, traverse_info/0, symlink_resolution_policy/0, 
    encountered_files_set/0, new_jobs_preprocessor/0]).

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
    run(Pool, DocOrCtx, ?ROOT_USER_ID, Opts).

-spec run(traverse:pool() | atom(), file_meta:doc() | file_ctx:ctx(), od_user:id(), run_options()) ->
    {ok, id()}.
run(Pool, DocOrCtx, UserId, Opts) when is_atom(Pool) ->
    run(atom_to_binary(Pool, utf8), DocOrCtx, UserId, Opts);
run(Pool, FileDoc = #document{scope = SpaceId, value = #file_meta{}}, UserId, Opts) ->
    FileCtx = file_ctx:new_by_doc(FileDoc, SpaceId),
    run(Pool, FileCtx, UserId, Opts);
run(Pool, FileCtx, UserId, Opts) ->
    TaskId = case maps:get(task_id, Opts, undefined) of
        undefined -> datastore_key:new();
        Id -> Id
    end,
    BatchSize = maps:get(batch_size, Opts, ?DEFAULT_BATCH_SIZE),
    ChildDirsJobGenerationPolicy = maps:get(child_dirs_job_generation_policy, Opts, ?DEFAULT_CHILD_DIRS_JOB_GENERATION_POLICY),
    ChildrenMasterJobsMode = maps:get(children_master_jobs_mode, Opts, ?DEFAULT_CHILDREN_MASTER_JOBS_MODE),
    TrackSubtreeStatus = maps:get(track_subtree_status, Opts, ?DEFAULT_TRACK_SUBTREE_STATUS),
    TraverseInfo = maps:get(traverse_info, Opts, #{}),
    TraverseInfo2 = TraverseInfo#{pool => Pool},
    SymlinksResolutionPolicy = maps:get(symlink_resolution_policy, Opts, preserve),
    {Filename, FileCtx2} = file_ctx:get_aliased_name(FileCtx, undefined),
    InitialRelativePath = maps:get(initial_relative_path, Opts, Filename),

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

    {FileDoc, FileCtx3} = file_ctx:get_file_doc(FileCtx2),
    {ok, ParentUuid} = file_meta:get_parent_uuid(FileDoc),
    Job = #tree_traverse{
        file_ctx = FileCtx3,
        user_id = UserId,
        tune_for_large_continuous_listing = maps:get(tune_for_large_continuous_listing , Opts, true),
        pagination_token = undefined,
        child_dirs_job_generation_policy = ChildDirsJobGenerationPolicy,
        children_master_jobs_mode = ChildrenMasterJobsMode,
        track_subtree_status = TrackSubtreeStatus,
        batch_size = BatchSize,
        traverse_info = TraverseInfo2,
        symlink_resolution_policy = SymlinksResolutionPolicy,
        resolved_root_uuids = [file_ctx:get_logical_uuid_const(FileCtx3)],
        relative_path = InitialRelativePath,
        encountered_files = add_to_set_if_symlinks_followed(
            file_ctx:get_logical_uuid_const(FileCtx3), #{}, SymlinksResolutionPolicy)
    },
    maybe_create_status_doc(Job, TaskId, ParentUuid),
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

-spec get_traverse_info(job()) -> traverse_info().
get_traverse_info(#tree_traverse{traverse_info = TraverseInfo}) ->
    TraverseInfo;
get_traverse_info(#tree_traverse_slave{traverse_info = TraverseInfo}) ->
    TraverseInfo.

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
do_master_job(Job, #{task_id := TaskId}, NewJobsPreprocessor) ->
    #tree_traverse{
        file_ctx = FileCtx,
        user_id = UserId,
        traverse_info = TraverseInfo
    } = Job,
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    Job2 = Job#tree_traverse{file_ctx = FileCtx2},
    FileType = file_meta:get_effective_type(FileDoc),
    case tree_traverse_session:acquire_for_task(UserId, maps:get(pool, TraverseInfo), TaskId) of
        {ok, UserCtx} ->
            do_master_job_internal(FileType, Job2, TaskId, NewJobsPreprocessor, UserCtx);
        {error, ?EACCES} -> 
            {ok, #{}}
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

-spec do_master_job_internal(file_meta:type(), master_job(), id(), new_jobs_preprocessor(), user_ctx:ctx()) ->
    {ok, traverse:master_job_map()}.
do_master_job_internal(?DIRECTORY_TYPE, Job, TaskId, NewJobsPreprocessor, UserCtx) ->
    #tree_traverse{children_master_jobs_mode = ChildrenMasterJobsMode} = Job,
    case list_children(Job, UserCtx) of
        {error, ?EACCES} ->
            {ok, #{}};
        {ok, {ChildrenCtxs, ListingPaginationToken, FileCtx3}} ->
            {SlaveJobs, MasterJobs} = generate_children_jobs(Job, TaskId, ChildrenCtxs, UserCtx),
            ChildrenCount = length(SlaveJobs) + length(MasterJobs),
            SubtreeProcessingStatus = maybe_report_children_jobs_to_process(
                Job, TaskId, ChildrenCount, file_listing:is_finished(ListingPaginationToken)),
            {UpdatedSlaveJobs, UpdatedMasterJobs} = case 
                NewJobsPreprocessor(SlaveJobs, MasterJobs, ListingPaginationToken, SubtreeProcessingStatus) 
            of
                ok -> {SlaveJobs, MasterJobs};
                {NewSlaveJobs, NewMasterJobs} -> 
                    {NewSlaveJobs, NewMasterJobs}
            end,
            FinalMasterJobs = case file_listing:is_finished(ListingPaginationToken) of
                true ->
                    UpdatedMasterJobs;
                false -> [Job#tree_traverse{
                    file_ctx = FileCtx3,
                    pagination_token = ListingPaginationToken
                } | UpdatedMasterJobs]
            end,
            
            ChildrenMasterJobsKey = case ChildrenMasterJobsMode of
                sync -> master_jobs;
                async -> async_master_jobs
            end,
            {ok, #{slave_jobs => UpdatedSlaveJobs, ChildrenMasterJobsKey => FinalMasterJobs}}
    end;
do_master_job_internal(?REGULAR_FILE_TYPE, Job = #tree_traverse{file_ctx = FileCtx}, _, _, _) ->
    % correct relative path to this file is already set in Job, so passing <<>> as Filename will not extend it
    {ok, #{slave_jobs => [get_child_slave_job(Job, FileCtx, <<>>)]}};
do_master_job_internal(?SYMLINK_TYPE, Job = #tree_traverse{symlink_resolution_policy = preserve, file_ctx = FileCtx}, _, _, _) ->
    % correct relative path to this file is already set in Job, so passing <<>> as Filename will not extend it
    {ok, #{slave_jobs => [get_child_slave_job(Job, FileCtx, <<>>)]}};
do_master_job_internal(?SYMLINK_TYPE, Job = #tree_traverse{symlink_resolution_policy = follow_all, file_ctx = FileCtx}, TaskId, NewJobsPreprocessor, UserCtx) ->
    case resolve_symlink(Job, FileCtx, UserCtx) of
        {ok, ResolvedCtx} ->
            {FileDoc, ResolvedCtx2} = file_ctx:get_file_doc(ResolvedCtx),
            Job2 = Job#tree_traverse{file_ctx = ResolvedCtx2},
            FileType = file_meta:get_effective_type(FileDoc),
            do_master_job_internal(FileType, Job2, TaskId, NewJobsPreprocessor, UserCtx);
        ignore ->
            {ok, #{}}
    end;
do_master_job_internal(?SYMLINK_TYPE, Job = #tree_traverse{symlink_resolution_policy = follow_external, file_ctx = FileCtx}, TaskId, NewJobsPreprocessor, UserCtx) ->
    case resolve_symlink(Job, FileCtx, UserCtx) of
        {ok, ResolvedCtx} ->
            {Path, ResolvedCtx2} = file_ctx:get_uuid_based_path(ResolvedCtx),
            case is_in_subtree(Job, Path) of
                true ->
                    {ok, #{slave_jobs => [get_child_slave_job(Job, FileCtx, <<>>)]}};
                false ->
                    {FileDoc, ResolvedCtx3} = file_ctx:get_file_doc(ResolvedCtx2),
                    Job2 = append_root_uuid(Job#tree_traverse{file_ctx = ResolvedCtx3}, file_ctx:get_logical_uuid_const(ResolvedCtx3)),
                    FileType = file_meta:get_effective_type(FileDoc),
                    do_master_job_internal(FileType, Job2, TaskId, NewJobsPreprocessor, UserCtx)
            end;
        ignore ->
            {ok, #{}}
    end.


-spec report_child_processed(id(), file_meta:uuid()) -> tree_traverse_progress:status().
report_child_processed(TaskId, ParentUuid) ->
    tree_traverse_progress:report_child_processed(TaskId, ParentUuid).


-spec delete_subtree_status_doc(id(), file_meta:uuid()) -> ok | {error, term()}.
delete_subtree_status_doc(TaskId, Uuid) ->
    tree_traverse_progress:delete(TaskId, Uuid).


%%%===================================================================
%% Tracking subtree progress status API
%%%===================================================================

-spec list_children(master_job(), user_ctx:ctx()) -> 
    {ok, {[file_ctx:ctx()], file_listing:pagination_token(), file_ctx:ctx()}} | {error, term()}.
list_children(#tree_traverse{
    file_ctx = FileCtx,
    pagination_token = PaginationToken,
    tune_for_large_continuous_listing = TuneForLargeContinuousListing,
    batch_size = BatchSize
}, UserCtx) ->
    BaseListingOpts = case PaginationToken of
        undefined -> #{tune_for_large_continuous_listing => TuneForLargeContinuousListing};
        _ -> #{pagination_token => PaginationToken}
    end,
    try
        {ok, dir_req:get_children_ctxs(UserCtx, FileCtx, BaseListingOpts#{limit => BatchSize})}
    catch
        throw:?EACCES ->
            {error, ?EACCES}
    end.


-spec generate_children_jobs(master_job(), id(), [file_ctx:ctx()], user_ctx:ctx()) -> 
    {[slave_job()], [master_job()]}.
generate_children_jobs(MasterJob, TaskId, Children, UserCtx) ->
    {SlaveJobsReversed, MasterJobsReversed} = lists:foldl(fun(ChildCtx, {SlavesAcc, MastersAcc} = Acc) ->
        try
            {ChildDoc, ChildCtx2} = file_ctx:get_file_doc(ChildCtx),
            FileType = file_meta:get_effective_type(ChildDoc),
            {Filename, ChildCtx3} = file_ctx:get_aliased_name(ChildCtx2, undefined),
            {ChildSlaves, ChildMasters} = generate_child_jobs(
                FileType, MasterJob, TaskId, ChildCtx3, Filename, UserCtx),
            {ChildSlaves ++ SlavesAcc, ChildMasters ++ MastersAcc}    
        catch
            _:{badmatch, {error, not_found}} ->
                Acc
        end
    end, {[], []}, Children),
    {lists:reverse(SlaveJobsReversed), lists:reverse(MasterJobsReversed)}.


-spec generate_child_jobs(file_meta:type(), master_job(), id(), file_ctx:ctx(), file_meta:name(), user_ctx:ctx()) -> 
    {[slave_job()], [master_job()]}.
generate_child_jobs(?DIRECTORY_TYPE, MasterJob, TaskId, ChildCtx, Filename, _) ->
    #tree_traverse{
        child_dirs_job_generation_policy = ChildDirsJobGenerationPolicy, 
        file_ctx = ParentFileCtx
    } = MasterJob,
    ChildMasterJob = get_child_master_job(MasterJob, ChildCtx, Filename),
    maybe_create_status_doc(ChildMasterJob, TaskId, file_ctx:get_logical_uuid_const(ParentFileCtx)),
    case ChildDirsJobGenerationPolicy of
        generate_slave_and_master_jobs ->
            ChildSlaveJob = get_child_slave_job(MasterJob, ChildCtx, Filename),
            {[ChildSlaveJob], [ChildMasterJob]};
        generate_master_jobs ->
            {[], [ChildMasterJob]}
    end;
generate_child_jobs(?REGULAR_FILE_TYPE, MasterJob, _TaskId, ChildCtx, Filename, _) ->
    {[get_child_slave_job(MasterJob, ChildCtx, Filename)], []};
generate_child_jobs(?SYMLINK_TYPE, #tree_traverse{symlink_resolution_policy = preserve} = MasterJob, _TaskId, ChildCtx, Filename, _) ->
    {[get_child_slave_job(MasterJob, ChildCtx, Filename)], []};
generate_child_jobs(?SYMLINK_TYPE, #tree_traverse{symlink_resolution_policy = follow_all} = MasterJob, TaskId, ChildCtx, Filename, UserCtx) ->
    case resolve_symlink(MasterJob, ChildCtx, UserCtx) of
        {ok, ResolvedCtx} ->
            {FileDoc, ResolvedCtx2} = file_ctx:get_file_doc(ResolvedCtx),
            FileType = file_meta:get_effective_type(FileDoc),
            generate_child_jobs(FileType, MasterJob, TaskId, ResolvedCtx2, Filename, UserCtx);
        ignore -> 
            {[], []}
    end;
generate_child_jobs(?SYMLINK_TYPE, #tree_traverse{symlink_resolution_policy = follow_external} = MasterJob, TaskId, ChildCtx, Filename, UserCtx) ->
    case resolve_symlink(MasterJob, ChildCtx, UserCtx) of
        {ok, ResolvedCtx} ->
            {Path, ResolvedCtx2} = file_ctx:get_uuid_based_path(ResolvedCtx),
            case is_in_subtree(MasterJob, Path) of
                true ->
                    {[get_child_slave_job(MasterJob, ChildCtx, Filename)], []};
                false ->
                    {FileDoc, ResolvedCtx3} = file_ctx:get_file_doc(ResolvedCtx2),
                    FileType = file_meta:get_effective_type(FileDoc),
                    MasterJob2 = append_root_uuid(MasterJob, file_ctx:get_logical_uuid_const(ResolvedCtx3)),
                    generate_child_jobs(FileType, MasterJob2, TaskId, ResolvedCtx3, Filename, UserCtx)
            end;
        ignore ->
            {[], []}
    end.


-spec maybe_create_status_doc(master_job(), id(), file_meta:uuid()) -> ok | {error, term()}.
maybe_create_status_doc(#tree_traverse{
    file_ctx = FileCtx,
    track_subtree_status = true
}, TaskId, ParentUuid) ->
    Uuid = file_ctx:get_logical_uuid_const(FileCtx),
    tree_traverse_progress:create(TaskId, Uuid, ParentUuid);
maybe_create_status_doc(_, _, _) ->
    ok.


-spec maybe_report_children_jobs_to_process(master_job(), id(), non_neg_integer(), boolean()) ->
    tree_traverse_progress:status() | undefined.
maybe_report_children_jobs_to_process(#tree_traverse{
    file_ctx = FileCtx,
    track_subtree_status = true
}, TaskId, ChildrenCount, AllBatchesListed) ->
    Uuid = file_ctx:get_logical_uuid_const(FileCtx),
    tree_traverse_progress:report_children_to_process(TaskId, Uuid, ChildrenCount, AllBatchesListed);
maybe_report_children_jobs_to_process(_, _, _, _) ->
    undefined.


-spec reset_list_options(master_job()) -> master_job().
reset_list_options(Job) ->
    Job#tree_traverse{
        pagination_token = undefined
    }.


-spec add_to_set_if_symlinks_followed(file_meta:uuid(), encountered_files_set(), 
    symlink_resolution_policy()) -> encountered_files_set().
add_to_set_if_symlinks_followed(_Uuid, EncounteredFilesSet, preserve) ->
    % there is no need to keeping track of encountered files when there is no symlinks following
    EncounteredFilesSet;
add_to_set_if_symlinks_followed(Uuid, EncounteredFilesSet, _) ->
    add_to_set(Uuid, EncounteredFilesSet).


%%%===================================================================
%%% Helper functions
%%%===================================================================

-spec get_child_master_job(master_job(), file_ctx:ctx(), file_meta:name()) -> master_job().
get_child_master_job(MasterJob = #tree_traverse{
    relative_path = ParentRelativePath,
    symlink_resolution_policy = FollowSymlinks,
    encountered_files = PrevEncounteredFilesSet
}, ChildCtx, Filename) ->
    MasterJob2 = reset_list_options(MasterJob),
    MasterJob2#tree_traverse{
        file_ctx = ChildCtx,
        relative_path = filename:join(ParentRelativePath, Filename),
        encountered_files = add_to_set_if_symlinks_followed(
            file_ctx:get_logical_uuid_const(ChildCtx), PrevEncounteredFilesSet, FollowSymlinks)
    }.


-spec get_child_slave_job(master_job(), file_ctx:ctx(), file_meta:name()) -> slave_job().
get_child_slave_job(#tree_traverse{
    user_id = UserId,
    file_ctx = FileCtx,
    traverse_info = TraverseInfo,
    track_subtree_status = TrackSubtreeStatus,
    relative_path = ParentRelativePath
}, ChildCtx, Filename) ->
    #tree_traverse_slave{
        user_id = UserId,
        file_ctx = ChildCtx,
        master_job_uuid = file_ctx:get_logical_uuid_const(FileCtx),
        traverse_info = TraverseInfo,
        track_subtree_status = TrackSubtreeStatus,
        relative_path = filename:join(ParentRelativePath, Filename)
    }.


%% @TODO VFS-7923 Unify all symlinks resolution across op_worker
-spec resolve_symlink(master_job(), file_ctx:ctx(), user_ctx:ctx()) -> {ok, file_ctx:ctx()} | ignore.
resolve_symlink(#tree_traverse{encountered_files = EncounteredFilesSet}, SymlinkCtx, UserCtx) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    SymlinkGuid = file_ctx:get_logical_guid_const(SymlinkCtx),
    case lfm:resolve_symlink(SessionId, #file_ref{guid = SymlinkGuid}) of
        {ok, ResolvedGuid} ->
            case is_set_element(file_id:guid_to_uuid(ResolvedGuid), EncounteredFilesSet) of
                true -> ignore; % this file was already encountered, there is a loop in symlinks
                false -> {ok, file_ctx:new_by_guid(ResolvedGuid)}
            end;
        {error, ?ELOOP} -> ignore;
        {error, ?EPERM} -> ignore;
        {error, ?EACCES} -> ignore;
        {error, ?ENOENT} -> ignore
    end.


-spec is_in_subtree(master_job(), file_meta:uuid_based_path()) -> boolean().
is_in_subtree(#tree_traverse{resolved_root_uuids = ResolvedRootUuids, file_ctx = Ctx}, Path) ->
    lists:any(fun(Uuid) ->
        SpaceId = file_ctx:get_space_id_const(Ctx),
        {RootPath, _} = file_ctx:get_uuid_based_path(file_ctx:new_by_uuid(Uuid, SpaceId)),
        str_utils:binary_starts_with(Path, RootPath)
    end, ResolvedRootUuids).


-spec append_root_uuid(master_job(), file_meta:uuid()) -> master_job().
append_root_uuid(#tree_traverse{resolved_root_uuids = ResolvedRootUuids} = Job, Uuid) ->
    Job#tree_traverse{resolved_root_uuids = lists_utils:union(ResolvedRootUuids, [Uuid])}.

%%%===================================================================
%% Files set API
%%%===================================================================

-spec is_set_element(file_meta:uuid(), encountered_files_set()) -> boolean().
is_set_element(Uuid, EncounteredFiles) ->
    maps:get(Uuid, EncounteredFiles, false).


-spec add_to_set(file_meta:uuid(), encountered_files_set()) -> encountered_files_set().
add_to_set(Uuid, PrevEncounteredFiles) ->
    PrevEncounteredFiles#{Uuid => true}.