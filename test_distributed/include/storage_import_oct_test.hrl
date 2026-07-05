%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Macros used in tests of storage import.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(STORAGE_IMPORT_OCT_TEST_HRL).
-define(STORAGE_IMPORT_OCT_TEST_HRL, 1).


-include("onenv_test_utils.hrl").
-include("space_setup_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


-define(TEST_UID, 2000).
-define(TEST_GID, 2000).

% Default attempts (the assertion macros poll every 1s, so Attempts ~ seconds)
% for eventually-consistent assertions (scan completion, dbsync propagation).
-define(ATTEMPTS, 30).

% Generous attempts (the await macro polls every 1s) for awaiting a scan over a
% large tree (hundreds/thousands of files), which may take much longer than the
% default ?ATTEMPTS. Passed explicitly per-test so that small tests still fail fast.
-define(LARGE_IMPORT_SCAN_ATTEMPTS, 300).

% Attempts for asserting state on the non-importing provider when the file is NOT
% awaited via verify_imported_tree first (which retries until the tree propagates).
% Such an assertion must itself tolerate the dbsync propagation lag, which can
% exceed the default ?ATTEMPTS on a loaded environment.
-define(CROSS_PROVIDER_PROPAGATION_ATTEMPTS, 60).

% Evaluates Expr only when StorageType is posix, no-op otherwise. Shorthand for
% the "do X on POSIX, nothing on S3" branches that recur throughout the storage
% import test suites (see their module docs for why POSIX and object storages
% diverge so often here). For a multi-statement Expr, wrap it in begin...end at
% the call site.
-define(IF_POSIX(StorageType, Expr), StorageType =:= posix andalso (Expr)).


%% Declares a FIFO (named pipe) to be created directly on the storage. A FIFO is a
%% special (unsupported) file type that storage import must ignore - it is created
%% on the storage but never imported into the logical filesystem. Used to test that
%% such entries are skipped (see import_ignores_fifo_test). Kept local to the storage
%% import tests, as FIFOs cannot be expressed via the generic onenv file specs.
-record(storage_fifo_spec, {
    name = undefined :: undefined | binary()
}).

-record(storage_import_test_suite_ctx, {
    storage_type :: posix | s3,
    importing_provider_selector :: oct_background:entity_selector(),
    non_importing_provider_selector :: oct_background:entity_selector(),
    space_owner_selector :: oct_background:entity_selector()
}).

-record(provider_ctx, {
    selector :: oct_background:entity_selector(),
    node :: oct_background:node(),
    session_id :: session:id()
}).

%% Context of a single storage import test case, built by
%% storage_import_test_utils:init_testcase/3 and consumed by the generic
%% verification/assertion utils.
-record(storage_import_test_case_ctx, {
    suite_ctx :: #storage_import_test_suite_ctx{},
    imported_storage_id :: storage:id(),
    other_storage_id :: storage:id(),
    space_id :: od_space:id(),
    space_path :: file_meta:path(),
    % concretized file tree spec (all names filled in) that was created on the
    % imported storage; undefined when the storage was left empty
    file_tree_spec :: undefined | onenv_file_test_utils:object_spec()
        | [onenv_file_test_utils:object_spec()],
    importing_provider_ctx :: #provider_ctx{},
    non_importing_provider_ctx :: #provider_ctx{}
}).


-endif.