%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains base test functions for testing storage import
%%% continuous (update) scans.
%%%
%%% While storage_import_initial_oct_test_base covers the first import of a
%%% storage, this module covers what subsequent (continuous) scans detect and
%%% how the scanning engine behaves: modifications to already-imported files,
%%% idempotency (unchanged entries are not reprocessed), retrying after a failed
%%% scan, conflict/suffix resolution for recreated files, scan configuration
%%% (max_depth, force start/stop, batch handling) and protection flags. The
%%% generic machinery (creating and mutating the tree on the storage, enabling
%%% and awaiting scans, verifying the imported tree, asserting the monitoring
%%% counters) lives in storage_import_test_utils.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_update_oct_test_base).
-author("Bartosz Walkowicz").

-include("storage_import_oct_test.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/fslogic/acl.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/fuse_messages.hrl").


% API
-export([
    clean_up_after_previous_run/2,
    init_per_testcase/1,
    end_per_testcase/3
]).

%% tests
-export([
    %% --- modifications ---
    append_file_update_test/1

    %% --- idempotency ---

    %% --- retry ---

    %% --- suffixes ---

    %% --- config ---

    %% --- protection ---

    %% --- not reimported ---
]).

-define(ATTEMPTS, 30).


%%%===================================================================
%%% API
%%%===================================================================


-spec clean_up_after_previous_run([atom()], storage_import_test_utils:suite_ctx()) -> ok.
clean_up_after_previous_run(AllTestCases, SuiteCtx) ->
    storage_import_test_utils:clean_up_after_previous_run(AllTestCases, SuiteCtx).


init_per_testcase(Config) ->
    lfm_proxy:init(Config).


end_per_testcase(_Case, _TestSuiteCtx, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Tests
%%%===================================================================


%% --- modifications ---


%% A file imported by the initial scan is appended to on the storage; the next
%% (continuous) scan detects the change and the appended bytes become readable
%% through the logical filesystem on both providers.
append_file_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    FileName = ?RAND_STR(),
    InitialContent = ?RAND_STR(),
    AppendedContent = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = InitialContent}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    %% the file was imported by the initial scan with its initial content
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:verify_dir_stats(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }),

    %% append to the file on the storage and let the next continuous scan detect it
    storage_file_setup_utils:write_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId,
        byte_size(InitialContent), AppendedContent
    ),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% the appended content is now readable on both providers
    UpdatedFileSpec = #file_spec{
        name = FileName, content = <<InitialContent/binary, AppendedContent/binary>>
    },
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, UpdatedFileSpec),
    storage_import_test_utils:verify_dir_stats(TestCaseCtx, UpdatedFileSpec),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"unmodified">> => 1
    }).


%% --- idempotency ---


%% --- retry ---


%% --- suffixes ---


%% --- config ---


%% --- protection ---


%% --- not reimported ---
