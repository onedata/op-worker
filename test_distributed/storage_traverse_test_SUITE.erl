%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module tests storage_traverse mechanism.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_traverse_test_SUITE).
-author("Jakub Kudzia").

-behaviour(traverse_behaviour).

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/traverse/storage_traverse.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/logging.hrl").


%% exported for CT
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    posix_files_only_test/1,
    posix_files_only_imported_storage_test/1,
    canonical_s3_files_only_test/1,
    canonical_s3_files_only_imported_storage_test/1,
    posix_files_and_dirs_test/1,
    posix_files_and_dirs_imported_storage_test/1,
    posix_files_only_async_master_jobs_test/1,
    posix_files_only_async_master_jobs_imported_storage_test/1,
    canonical_s3_files_only_async_master_jobs_test/1,
    canonical_s3_files_only_async_master_jobs_imported_storage_test/1,
    posix_files_and_dirs_async_master_jobs_test/1,
    posix_files_and_dirs_async_master_jobs_imported_storage_test/1,
    posix_files_only_max_depth0_test/1,
    posix_files_only_max_depth1_test/1,
    posix_files_only_max_depth2_test/1,
    posix_files_only_max_depth3_test/1,
    posix_files_only_imported_storage_max_depth0_test/1,
    posix_files_only_imported_storage_max_depth1_test/1,
    posix_files_only_imported_storage_max_depth2_test/1,
    posix_files_only_imported_storage_max_depth3_test/1,
    canonical_s3_files_only_max_depth0_test/1,
    canonical_s3_files_only_max_depth1_test/1,
    canonical_s3_files_only_max_depth2_test/1,
    canonical_s3_files_only_max_depth3_test/1,
    canonical_s3_files_only_imported_storage_max_depth0_test/1,
    canonical_s3_files_only_imported_storage_max_depth1_test/1,
    canonical_s3_files_only_imported_storage_max_depth2_test/1,
    canonical_s3_files_only_imported_storage_max_depth3_test/1,
    posix_files_and_dirs_max_depth0_test/1,
    posix_files_and_dirs_max_depth1_test/1,
    posix_files_and_dirs_max_depth2_test/1,
    posix_files_and_dirs_max_depth3_test/1,
    posix_files_and_dirs_imported_storage_max_depth0_test/1,
    posix_files_and_dirs_imported_storage_max_depth1_test/1,
    posix_files_and_dirs_imported_storage_max_depth2_test/1,
    posix_files_and_dirs_imported_storage_max_depth3_test/1,
    posix_files_only_synchronous_next_batch_test/1,
    posix_files_only_synchronous_next_batch_imported_storage_test/1,
    canonical_s3_files_only_synchronous_next_batch_test/1,
    canonical_s3_files_only_synchronous_next_batch_imported_storage_test/1,
    posix_files_and_dirs_synchronous_next_batch_test/1,
    posix_files_and_dirs_synchronous_next_batch_imported_storage_test/1,
    posix_custom_compute_test/1,
    posix_custom_compute_max_depth0_test/1,
    posix_custom_compute_max_depth1_test/1,
    canonical_s3_custom_compute_test/1,
    canonical_s3_custom_compute_max_depth0_test/1,
    canonical_s3_custom_compute_max_depth1_test/1
]).


%% Pool callbacks
-export([do_master_job/2, do_slave_job/2, update_job_progress/5, get_job/1]).

all() -> ?ALL([
    posix_files_only_test,
    posix_files_only_imported_storage_test,
    canonical_s3_files_only_test,
    canonical_s3_files_only_imported_storage_test,
    posix_files_and_dirs_test,
    posix_files_and_dirs_imported_storage_test,
    posix_files_only_async_master_jobs_test,
    posix_files_only_async_master_jobs_imported_storage_test,
    canonical_s3_files_only_async_master_jobs_test,
    canonical_s3_files_only_async_master_jobs_imported_storage_test,
    posix_files_and_dirs_async_master_jobs_test,
    posix_files_and_dirs_async_master_jobs_imported_storage_test,
    posix_files_only_max_depth0_test,
    posix_files_only_max_depth1_test,
    posix_files_only_max_depth2_test,
    posix_files_only_max_depth3_test,
    posix_files_only_imported_storage_max_depth0_test,
    posix_files_only_imported_storage_max_depth1_test,
    posix_files_only_imported_storage_max_depth2_test,
    posix_files_only_imported_storage_max_depth3_test,
    canonical_s3_files_only_max_depth0_test,
    canonical_s3_files_only_max_depth1_test,
    canonical_s3_files_only_max_depth2_test,
    canonical_s3_files_only_max_depth3_test,
    canonical_s3_files_only_imported_storage_max_depth0_test,
    canonical_s3_files_only_imported_storage_max_depth1_test,
    canonical_s3_files_only_imported_storage_max_depth2_test,
    canonical_s3_files_only_imported_storage_max_depth3_test,
    posix_files_and_dirs_max_depth0_test,
    posix_files_and_dirs_max_depth1_test,
    posix_files_and_dirs_max_depth2_test,
    posix_files_and_dirs_max_depth3_test,
    posix_files_and_dirs_imported_storage_max_depth0_test,
    posix_files_and_dirs_imported_storage_max_depth1_test,
    posix_files_and_dirs_imported_storage_max_depth2_test,
    posix_files_and_dirs_imported_storage_max_depth3_test,
    posix_files_only_synchronous_next_batch_test,
    posix_files_only_synchronous_next_batch_imported_storage_test,
    canonical_s3_files_only_synchronous_next_batch_test,
    canonical_s3_files_only_synchronous_next_batch_imported_storage_test,
    posix_files_and_dirs_synchronous_next_batch_test,
    posix_files_and_dirs_synchronous_next_batch_imported_storage_test,
    posix_custom_compute_test,
    posix_custom_compute_max_depth0_test,
    posix_custom_compute_max_depth1_test,
    canonical_s3_custom_compute_test,
    canonical_s3_custom_compute_max_depth0_test,
    canonical_s3_custom_compute_max_depth1_test
]).

-define(SPACES, [<<"space1">>, <<"space2">>, <<"space3">>, <<"space1">>, <<"space4">>]).
-define(TIMEOUT, timer:seconds(60)).
-define(INF_MAX_DEPTH, 99999999999999999999999999999999999999999).

%%%===================================================================
%%% Test functions
%%%===================================================================

posix_files_only_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space1">>, #{}).

posix_files_only_imported_storage_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space2">>, #{}).

canonical_s3_files_only_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space3">>, #{}).

canonical_s3_files_only_imported_storage_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space4">>, #{}).

posix_files_and_dirs_test(Config) ->
    traverse_and_execute_jobs_on_files_and_dirs_test_base(Config, <<"space1">>, #{}).

posix_files_and_dirs_imported_storage_test(Config) ->
    traverse_and_execute_jobs_on_files_and_dirs_test_base(Config, <<"space2">>, #{}).


posix_files_only_async_master_jobs_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space1">>, #{async_children_master_jobs => true}).

posix_files_only_async_master_jobs_imported_storage_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space2">>, #{async_children_master_jobs => true}).

canonical_s3_files_only_async_master_jobs_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space3">>, #{async_children_master_jobs => true}).

canonical_s3_files_only_async_master_jobs_imported_storage_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space4">>, #{async_children_master_jobs => true}).

posix_files_and_dirs_async_master_jobs_test(Config) ->
    traverse_and_execute_jobs_on_files_and_dirs_test_base(Config, <<"space1">>, #{async_children_master_jobs => true}).

posix_files_and_dirs_async_master_jobs_imported_storage_test(Config) ->
    traverse_and_execute_jobs_on_files_and_dirs_test_base(Config, <<"space2">>, #{async_children_master_jobs => true}).


posix_files_only_max_depth0_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space1">>, #{max_depth => 0}).

posix_files_only_max_depth1_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space1">>, #{max_depth => 1}).

posix_files_only_max_depth2_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space1">>, #{max_depth => 2}).

posix_files_only_max_depth3_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space1">>, #{max_depth => 3}).

posix_files_only_imported_storage_max_depth0_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space2">>, #{max_depth => 0}).

posix_files_only_imported_storage_max_depth1_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space2">>, #{max_depth => 1}).

posix_files_only_imported_storage_max_depth2_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space2">>, #{max_depth => 2}).

posix_files_only_imported_storage_max_depth3_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space2">>, #{max_depth => 3}).

canonical_s3_files_only_max_depth0_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space3">>, #{max_depth => 0}).

canonical_s3_files_only_max_depth1_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space3">>, #{max_depth => 1}).

canonical_s3_files_only_max_depth2_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space3">>, #{max_depth => 2}).

canonical_s3_files_only_max_depth3_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space3">>, #{max_depth => 3}).

canonical_s3_files_only_imported_storage_max_depth0_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space4">>, #{max_depth => 0}).

canonical_s3_files_only_imported_storage_max_depth1_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space4">>, #{max_depth => 1}).

canonical_s3_files_only_imported_storage_max_depth2_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space4">>, #{max_depth => 2}).

canonical_s3_files_only_imported_storage_max_depth3_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space4">>, #{max_depth => 3}).

posix_files_and_dirs_max_depth0_test(Config) ->
    traverse_and_execute_jobs_on_files_and_dirs_test_base(Config, <<"space1">>, #{max_depth => 0}).

posix_files_and_dirs_max_depth1_test(Config) ->
    traverse_and_execute_jobs_on_files_and_dirs_test_base(Config, <<"space1">>, #{max_depth => 1}).

posix_files_and_dirs_max_depth2_test(Config) ->
    traverse_and_execute_jobs_on_files_and_dirs_test_base(Config, <<"space1">>, #{max_depth => 2}).

posix_files_and_dirs_max_depth3_test(Config) ->
    traverse_and_execute_jobs_on_files_and_dirs_test_base(Config, <<"space1">>, #{max_depth => 3}).

posix_files_and_dirs_imported_storage_max_depth0_test(Config) ->
    traverse_and_execute_jobs_on_files_and_dirs_test_base(Config, <<"space2">>, #{max_depth => 0}).

posix_files_and_dirs_imported_storage_max_depth1_test(Config) ->
    traverse_and_execute_jobs_on_files_and_dirs_test_base(Config, <<"space2">>, #{max_depth => 1}).

posix_files_and_dirs_imported_storage_max_depth2_test(Config) ->
    traverse_and_execute_jobs_on_files_and_dirs_test_base(Config, <<"space2">>, #{max_depth => 2}).

posix_files_and_dirs_imported_storage_max_depth3_test(Config) ->
    traverse_and_execute_jobs_on_files_and_dirs_test_base(Config, <<"space2">>, #{max_depth => 3}).


posix_files_only_synchronous_next_batch_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space1">>, #{async_next_batch_job => false}).

posix_files_only_synchronous_next_batch_imported_storage_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space2">>, #{async_next_batch_job => false}).

canonical_s3_files_only_synchronous_next_batch_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space3">>, #{async_next_batch_job => false}).

canonical_s3_files_only_synchronous_next_batch_imported_storage_test(Config) ->
    traverse_and_execute_jobs_only_on_files_test_base(Config, <<"space4">>, #{async_next_batch_job => false}).

posix_files_and_dirs_synchronous_next_batch_test(Config) ->
    traverse_and_execute_jobs_on_files_and_dirs_test_base(Config, <<"space1">>, #{async_next_batch_job => false}).

posix_files_and_dirs_synchronous_next_batch_imported_storage_test(Config) ->
    traverse_and_execute_jobs_on_files_and_dirs_test_base(Config, <<"space2">>, #{async_next_batch_job => false}).

posix_custom_compute_test(Config) ->
    custom_compute_test_base(Config, <<"space1">>, #{}, 120).

posix_custom_compute_max_depth0_test(Config) ->
    custom_compute_test_base(Config, <<"space1">>, #{max_depth => 0}, 0).

posix_custom_compute_max_depth1_test(Config) ->
    custom_compute_test_base(Config, <<"space1">>, #{max_depth => 1}, 20).

canonical_s3_custom_compute_test(Config) ->
    custom_compute_test_base(Config, <<"space3">>, #{}, 110).

canonical_s3_custom_compute_max_depth0_test(Config) ->
    custom_compute_test_base(Config, <<"space3">>, #{max_depth => 0}, 0).

canonical_s3_custom_compute_max_depth1_test(Config) ->
    custom_compute_test_base(Config, <<"space3">>, #{max_depth => 1}, 10).

%%%===================================================================
%%% Test bases
%%%===================================================================

traverse_and_execute_jobs_only_on_files_test_base(Config, SpaceId, Opts) ->
    [W | _] = ?config(op_worker_nodes, Config),
    StorageId = initializer:get_supporting_storage_id(W, SpaceId),
    SpaceDir = space_dir(W, SpaceId, StorageId),
    Handle = sd_test_utils:new_handle(W, SpaceId, SpaceDir, StorageId),
    {ok, CSPid} = countdown_server:start_link(self(), W),
    TestFilesStructure = [{10, 10}, {0, 10}],
    sd_test_utils:setup_test_files_structure(W, Handle, TestFilesStructure),
    MaxDepth = maps:get(max_depth, Opts, ?INF_MAX_DEPTH),
    StrippedTestFilesStructure = lists:sublist(TestFilesStructure, MaxDepth),
    % generate names of files taking max_depth into consideration
    {_CreatedDirs, CreatedFiles} = sd_test_utils:setup_test_files_structure(W, Handle,
        StrippedTestFilesStructure, true),
    {_DirsNum, FilesNum} = count_files_and_dirs(TestFilesStructure, MaxDepth),
    FilesCounterRef = countdown_server:init_counter(W, FilesNum),
    StartTime = time_utils:timestamp_millis(),
    run_traverse(W, SpaceId, StorageId, {CSPid, undefined, FilesCounterRef}, Opts),
    ReceivedFiles = countdown_server:await(W, FilesCounterRef, ?TIMEOUT),
    EndTime = time_utils:timestamp_millis(),
    ct:pal("Traverse took ~p seconds.", [(EndTime - StartTime) / 1000]),
    ?assertEqual(lists:usort(CreatedFiles), lists:usort(ReceivedFiles)).

traverse_and_execute_jobs_on_files_and_dirs_test_base(Config, SpaceId, Opts) ->
    [W | _] = ?config(op_worker_nodes, Config),
    StorageId = initializer:get_supporting_storage_id(W, SpaceId),
    SpaceDir = space_dir(W, SpaceId, StorageId),
    Handle = sd_test_utils:new_handle(W, SpaceId, SpaceDir, StorageId),
    {ok, CSPid} = countdown_server:start_link(self(), W),
    TestFilesStructure = [{10, 10}, {10, 10}],
    sd_test_utils:setup_test_files_structure(W, Handle, TestFilesStructure),
    MaxDepth = maps:get(max_depth, Opts, ?INF_MAX_DEPTH),
    StrippedTestFilesStructure = lists:sublist(TestFilesStructure, MaxDepth),
    % generate names of files taking max_depth into consideration
    {CreatedDirs, CreatedFiles} = sd_test_utils:setup_test_files_structure(W, Handle,
        StrippedTestFilesStructure, true),
    CreatedDirs2 = [space_dir(W, SpaceId, StorageId) | CreatedDirs],
    {DirsNum, FilesNum} = count_files_and_dirs(TestFilesStructure, MaxDepth),
    FilesCounterRef = countdown_server:init_counter(W, FilesNum),
    DirsCounterRef = countdown_server:init_counter(W, DirsNum + 1), % job will be executed also on space directory
    StartTime = time_utils:timestamp_millis(),
    run_traverse(W, SpaceId, StorageId, {CSPid, DirsCounterRef, FilesCounterRef},
        Opts#{execute_slave_on_dir => true}),
    ReceivedFiles = countdown_server:await(W, FilesCounterRef, ?TIMEOUT),
    ReceivedDirs = countdown_server:await(W, DirsCounterRef, ?TIMEOUT),
    EndTime = time_utils:timestamp_millis(),
    ct:pal("Traverse took ~p seconds.", [(EndTime - StartTime) / 1000]),
    ?assertEqual(lists:usort(CreatedFiles), lists:usort(ReceivedFiles)),
    ?assertEqual(lists:usort(CreatedDirs2), lists:usort(ReceivedDirs)).

custom_compute_test_base(Config, SpaceId, Opts, ExpectedComputeValue) ->
    [W | _] = ?config(op_worker_nodes, Config),
    StorageId = initializer:get_supporting_storage_id(W, SpaceId),
    SpaceDir = space_dir(W, SpaceId, StorageId),
    Handle = sd_test_utils:new_handle(W, SpaceId, SpaceDir, StorageId),
    {ok, CSPid} = countdown_server:start_link(self(), W),
    TestFilesStructure = [{10, 10}, {0, 10}],
    sd_test_utils:setup_test_files_structure(W, Handle, TestFilesStructure),
    MaxDepth = maps:get(max_depth, Opts, ?INF_MAX_DEPTH),
    StrippedTestFilesStructure = lists:sublist(TestFilesStructure, MaxDepth),
    % generate names of files taking max_depth into consideration
    {_CreatedDirs, CreatedFiles} = sd_test_utils:setup_test_files_structure(W, Handle,
        StrippedTestFilesStructure, true),
    {_DirsNum, FilesNum} = count_files_and_dirs(TestFilesStructure, MaxDepth),
    FilesCounterRef = countdown_server:init_counter(W, FilesNum),
    ComputeCounterRef = countdown_server:init_counter(W, ExpectedComputeValue),
    StartTime = time_utils:timestamp_millis(),
    run_traverse(W, SpaceId, StorageId, {CSPid, undefined, FilesCounterRef, ComputeCounterRef},
        Opts#{
            fold_children_fun => fun(StorageFileCtx, _Info, Acc) -> {Acc + 1, StorageFileCtx} end,
            fold_children_init => 0
        }),
    ReceivedFiles = countdown_server:await(W, FilesCounterRef, ?TIMEOUT),
    countdown_server:await(W, ComputeCounterRef, ?TIMEOUT),
    EndTime = time_utils:timestamp_millis(),
    ct:pal("Traverse took ~p seconds.", [(EndTime - StartTime) / 1000]),
    ?assertEqual(lists:usort(CreatedFiles), lists:usort(ReceivedFiles)).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        ssl:start(),
        hackney:start(),
        initializer:disable_quota_limit(NewConfig),
        initializer:mock_provider_ids(NewConfig),
        initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), NewConfig)
    end,
    [{?LOAD_MODULES, [initializer, sd_test_utils, ?MODULE, countdown_server]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    initializer:unmock_provider_ids(Config),
    ssl:stop().

init_per_testcase(_Case, Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    init_pool(W),
    Config.

end_per_testcase(_Case, Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    lists:foreach(fun(SpaceId) ->
        StorageId = initializer:get_supporting_storage_id(W, SpaceId),
        SpaceDir = space_dir(W, SpaceId, StorageId),
        Handle = sd_test_utils:new_handle(W, SpaceId, SpaceDir, StorageId),
        sd_test_utils:recursive_rm(W, Handle, true)
    end, ?SPACES),
    stop_pool(W).

%===================================================================
% Pool callbacks
%===================================================================

do_master_job(TraverseJob = #storage_traverse_master{fold_children_fun = undefined}, TaskId) ->
    storage_traverse:do_master_job(TraverseJob, TaskId);
do_master_job(TraverseJob = #storage_traverse_master{info = {Pid, _, _, ComputeCounterRef}}, TaskId) ->
    {ok, MasterJobMap, ComputeResult} = storage_traverse:do_master_job(TraverseJob, TaskId),
    case ComputeResult > 0 of
        true -> countdown_server:decrease_by_value(Pid, ComputeCounterRef, ComputeResult);
        false -> ok
    end,
    {ok, MasterJobMap}.

do_slave_job(#storage_traverse_slave{
    storage_file_ctx = StorageFileCtx,
    info = {Pid, DirsCounterRef, FilesCounterRef}
}, _TaskId) ->
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    {#statbuf{st_mode = Mode}, _} = storage_file_ctx:stat(StorageFileCtx),
    case file_meta:type(Mode) of
        ?REGULAR_FILE_TYPE when FilesCounterRef =/= undefined ->
            countdown_server:decrease(Pid, FilesCounterRef, StorageFileId);
        ?DIRECTORY_TYPE when DirsCounterRef =/= undefined ->
            countdown_server:decrease(Pid, DirsCounterRef, StorageFileId);
        _ ->
            ok
    end;
do_slave_job(#storage_traverse_slave{
    storage_file_ctx = StorageFileCtx,
    info = {Pid, DirsCounterRef, FilesCounterRef, _ComputeCounterRef}
}, _TaskId) ->
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    {#statbuf{st_mode = Mode}, _} = storage_file_ctx:stat(StorageFileCtx),
    case file_meta:type(Mode) of
        ?REGULAR_FILE_TYPE when FilesCounterRef =/= undefined ->
            countdown_server:decrease(Pid, FilesCounterRef, StorageFileId);
        ?DIRECTORY_TYPE when DirsCounterRef =/= undefined ->
            countdown_server:decrease(Pid, DirsCounterRef, StorageFileId);
        _ ->
            ok
    end.

update_job_progress(ID, Job, Pool, TaskID, Status) ->
    storage_traverse:update_job_progress(ID, Job, Pool, TaskID, Status).

get_job(DocOrID) ->
    storage_traverse:get_job(DocOrID).

%===================================================================
% Internal functions
%===================================================================

init_pool(Worker) ->
    rpc:call(Worker, storage_traverse, init, [?MODULE, 10, 10, 10]).

stop_pool(Worker) ->
    rpc:call(Worker, storage_traverse, stop, [?MODULE]).

run_traverse(Worker, SpaceId, StorageId, TraversInfo, TraverseOpts) ->
    rpc:call(Worker, storage_traverse, run, [?MODULE, SpaceId, StorageId, TraversInfo, TraverseOpts]).

space_dir(Worker, SpaceId, StorageId) ->
    rpc:call(Worker, storage_file_id, space_dir_id, [SpaceId, StorageId]).

count_files_and_dirs(FilesStructure) ->
    count_files_and_dirs(FilesStructure, 0, 0, 1).

count_files_and_dirs(FilesStructure, MaxDepth) ->
    count_files_and_dirs(lists:sublist(FilesStructure, MaxDepth)).

count_files_and_dirs([], DirsSum, FilesSum, _PrevLevelDirsNum) ->
    {DirsSum, FilesSum};
count_files_and_dirs([{0, 0} | _Rest], DirsSum, FilesSum, _PrevLevelDirsNum) ->
    {DirsSum, FilesSum};
count_files_and_dirs([{DirsNum, FilesNum} | Rest], DirsSum, FilesSum, PrevLevelDirsNum) ->
    CurrentLevelDirsNum = DirsNum * PrevLevelDirsNum,
    count_files_and_dirs(Rest, DirsSum + CurrentLevelDirsNum, FilesSum + FilesNum * PrevLevelDirsNum, CurrentLevelDirsNum).