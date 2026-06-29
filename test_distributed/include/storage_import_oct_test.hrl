%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
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