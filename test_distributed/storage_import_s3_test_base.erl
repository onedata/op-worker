%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module stores test bases of storage import on S3.
%%% @end
%%%--------------------------------------------------------------------
-module(storage_import_s3_test_base).
-author("Jakub Kudzia").

-include("storage_import_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    create_subfiles_and_delete_before_import_is_finished_test/1
]).

%%%==================================================================
%%% Test functions
%%%===================================================================


create_subfiles_and_delete_before_import_is_finished_test(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_DIR),
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, ?DEFAULT_DIR_PERMS),
    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),
    storage_import_test_base:assertInitialScanFinished(W1, ?SPACE_ID),

    %% Create nested tree structure
    DirStructure = [10, 10, 10],
    storage_import_test_base:create_nested_directory_tree(W1, DirStructure, SDHandle),
    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),

    ?assertEqual(true, rpc:call(W1, storage_import_monitoring, is_scan_in_progress, [?SPACE_ID]), ?ATTEMPTS),

    ok = sd_test_utils:recursive_rm(W1, SDHandle),
    ?assertMatch({ok, {?END_OF_LISTING_MARKER, []}}, sd_test_utils:listobjects(W1, SDHandle,  ?INITIAL_LISTING_MARKER, 0, 100)),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W1, SessId,  {path, ?SPACE_TEST_DIR_PATH}), 10 * ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 100), 2 * ?ATTEMPTS).


%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        ssl:start(),
        application:ensure_all_started(hackney),
        initializer:disable_quota_limit(NewConfig),
        initializer:mock_provider_ids(NewConfig),
        NewConfig2 = multi_provider_file_ops_test_base:init_env(NewConfig),
        [W1 | _] = ?config(op_worker_nodes, NewConfig2),
        rpc:call(W1, auto_storage_import_worker, notify_connection_to_oz, []),
        NewConfig2
    end,
    {ok, _} = application:ensure_all_started(worker_pool),
    {ok, _} = worker_pool:start_sup_pool(?VERIFY_POOL, [{workers, 8}]),
    [{?LOAD_MODULES, [initializer, storage_import_test_base, sd_test_utils, ?MODULE]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    ok = wpool:stop_sup_pool(?VERIFY_POOL),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    initializer:unmock_provider_ids(?config(op_worker_nodes, Config)),
    ssl:stop().

init_per_testcase(Case, Config) ->
    storage_import_test_base:init_per_testcase(Case, Config).

end_per_testcase(Case, Config) ->
    storage_import_test_base:end_per_testcase(Case, Config).