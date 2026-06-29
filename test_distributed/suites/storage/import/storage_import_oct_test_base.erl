%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains base test functions for testing storage import.
%%%
%%% The tests are declarative: each test declares the file tree to be created on
%%% the storage (via #dir_spec{}/#file_spec{} records), enables import by setting
%%% up a space supported by an imported storage and, once the scan finishes,
%%% declares the expected outcome. The heavy lifting (creating the tree on the
%%% storage, verifying the imported logical tree on both providers, asserting the
%%% monitoring counters) is done generically by storage_import_test_utils.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_oct_test_base).
-author("Katarzyna Such").

-include("storage_import_oct_test.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("modules/logical_file_manager/lfm.hrl").


% API
-export([
    clean_up_after_previous_run/2,
    init_per_testcase/1,
    end_per_testcase/3
]).

%% tests
-export([
    import_empty_storage_test/1,
    import_empty_directory_test/1,
    import_directory_error_test/1,
    import_directory_check_user_id_test/1,

    import_empty_file_test/1,
    import_file_with_content_test/1,

    import_file_in_directory_test/1,
    import_many_subfiles_test/1
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


end_per_testcase(Case = import_directory_error_test, TestSuiteCtx, Config) ->
    unmock_import_file_error(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);
end_per_testcase(Case = import_directory_check_user_id_test, TestSuiteCtx, Config) ->
    unmock_luma(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);
end_per_testcase(_Case, _TestSuiteCtx, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Tests
%%%===================================================================


import_empty_storage_test(SuiteCtx) ->
    TestCaseCtx = storage_import_test_utils:init_testcase(?FUNCTION_NAME, undefined, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}).


import_empty_directory_test(SuiteCtx) ->
    DirName = ?RAND_STR(),
    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        other_storage_id = OtherStorageId,
        space_id = SpaceId,
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx,
        non_importing_provider_ctx = NonImportingProviderCtx
    } = storage_import_test_utils:init_testcase(?FUNCTION_NAME, #dir_spec{name = DirName}, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),

    %% Verify directory ownership (provider-specific, hence not covered by the generic verification)
    #provider_ctx{node = ImportingProviderNode} = ImportingProviderCtx,
    #provider_ctx{node = NonImportingProviderNode} = NonImportingProviderCtx,
    SpaceTestDirPath = filepath_utils:join([SpacePath, DirName]),
    StorageSDHandleImportingProvider = sd_test_utils:get_storage_mountpoint_handle(
        ImportingProviderNode, SpaceId, ImportedStorageId
    ),
    StorageSDHandleNonImportingProvider = sd_test_utils:get_storage_mountpoint_handle(
        ImportingProviderNode, SpaceId, OtherStorageId
    ),
    {ok, #statbuf{st_uid = MountUidImportingProvider}} = sd_test_utils:stat(
        ImportingProviderNode, StorageSDHandleImportingProvider
    ),
    {ok, #statbuf{
        st_uid = MountUidNonImportingProvider,
        st_gid = MountGidNonImportingProvider
    }} = sd_test_utils:stat(NonImportingProviderNode, StorageSDHandleNonImportingProvider),

    SpaceOwnerId = ?SPACE_OWNER_ID(SpaceId),
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceTestDirPath, #{
        owner_id => SpaceOwnerId,
        uid => MountUidImportingProvider,
        gid => 0
    }),
    storage_import_test_utils:assert_attrs(NonImportingProviderCtx, SpaceTestDirPath, #{
        owner_id => SpaceOwnerId,
        uid => MountUidNonImportingProvider,
        gid => MountGidNonImportingProvider
    }),

    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }).


import_directory_error_test(SuiteCtx) ->
    DirName = ?RAND_STR(),
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    mock_import_file_error(ImportingProviderSelector, DirName),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_id = SpaceId,
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{
            node = ImportingProviderNode,
            session_id = ImportingProviderSessionId
        },
        non_importing_provider_ctx = #provider_ctx{
            session_id = NonImportingProviderSessionId
        }
    } = storage_import_test_utils:init_testcase(?FUNCTION_NAME, #dir_spec{name = DirName}, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    SpaceTestDirPath = filepath_utils:join([SpacePath, DirName]),

    %% Check that the dir was not imported ...
    ?assertMatch({ok, []},
        lfm_proxy:get_children(ImportingProviderNode, ImportingProviderSessionId, {path, SpacePath}, 0, 1),
        ?ATTEMPTS
    ),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(ImportingProviderNode, ImportingProviderSessionId, {path, SpaceTestDirPath}),
        ?ATTEMPTS
    ),
    ?assertMatch({error, ?EACCES},
        lfm_proxy:stat(ImportingProviderNode, NonImportingProviderSessionId, {path, SpaceTestDirPath}),
        ?ATTEMPTS
    ),

    %% ... but is still present on the storage
    StorageSDHandleImportingProvider = sd_test_utils:get_storage_mountpoint_handle(
        ImportingProviderNode, SpaceId, ImportedStorageId
    ),
    ?assertMatch({ok, [DirName]},
        sd_test_utils:ls(ImportingProviderNode, StorageSDHandleImportingProvider, 0, 1)),

    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"created">> => 0,
        <<"failed">> => 1,
        <<"unmodified">> => 1,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0
    }).


import_directory_check_user_id_test(SuiteCtx) ->
    DirName = ?RAND_STR(),
    #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector,
        space_owner_selector = SpaceOwnerSelector
    } = SuiteCtx,
    mock_uid_and_gid(ImportingProviderSelector, SpaceOwnerSelector),

    TestCaseCtx = #storage_import_test_case_ctx{
        other_storage_id = OtherStorageId,
        space_id = SpaceId,
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx,
        non_importing_provider_ctx = NonImportingProviderCtx
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #dir_spec{name = DirName, uid = ?TEST_UID, gid = ?TEST_GID}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),

    %% Verify directory ownership (mapped via mocked LUMA, provider-specific)
    #provider_ctx{node = ImportingProviderNode} = ImportingProviderCtx,
    #provider_ctx{node = NonImportingProviderNode} = NonImportingProviderCtx,
    SpaceTestDirPath = filepath_utils:join([SpacePath, DirName]),
    SpaceOwnerId = oct_background:to_entity_id(SpaceOwnerSelector),
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceTestDirPath, #{
        owner_id => SpaceOwnerId,
        uid => ?TEST_UID,
        gid => ?TEST_GID
    }),

    GeneratedUid = ?rpc(NonImportingProviderNode, luma_auto_feed:generate_uid(SpaceOwnerId)),
    StorageSDHandleNonImportingProvider = sd_test_utils:get_storage_mountpoint_handle(
        ImportingProviderNode, SpaceId, OtherStorageId
    ),
    {ok, #statbuf{st_gid = Gid2}} = sd_test_utils:stat(NonImportingProviderNode, StorageSDHandleNonImportingProvider),
    storage_import_test_utils:assert_attrs(NonImportingProviderCtx, SpaceTestDirPath, #{
        owner_id => SpaceOwnerId,
        uid => GeneratedUid,
        gid => Gid2
    }),

    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }).


import_empty_file_test(SuiteCtx) ->
    TestCaseCtx = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = ?RAND_STR()}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }).


import_file_with_content_test(SuiteCtx) ->
    TestCaseCtx = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = ?RAND_STR(), content = ?RAND_STR()}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }).


%% Imports a single directory containing a single regular file (old:
%% create_file_in_dir_import_test). The generic verification covers the whole
%% nested structure (dir type + its children + file type + file content) on both
%% the importing and the non-importing provider.
import_file_in_directory_test(SuiteCtx) ->
    FileTreeSpec = #dir_spec{children = [#file_spec{content = ?RAND_STR()}]},
    TestCaseCtx = storage_import_test_utils:init_testcase(?FUNCTION_NAME, FileTreeSpec, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    %% created = 2 (the directory and the file), derived from the declared tree
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }).


%% Imports many directories, each containing a single regular file (old:
%% create_subfiles_import_many_test). Exercises the declarative tree builder and
%% verification at scale; created is derived from the tree (2 per subdirectory).
import_many_subfiles_test(SuiteCtx) ->
    SubdirsCount = 200,
    Content = ?RAND_STR(),
    FileTreeSpec = [
        #dir_spec{children = [#file_spec{content = Content}]}
        || _ <- lists:seq(1, SubdirsCount)
    ],
    TestCaseCtx = storage_import_test_utils:init_testcase(?FUNCTION_NAME, FileTreeSpec, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    %% createdMinHist is skipped - by the time the (large) tree is verified and the
    %% monitoring is read, the creations may have shifted out of the first minute
    %% histogram buckets; the cumulative hour/day histograms still hold the count
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1,
        <<"createdMinHist">> => skip
    }).


%%%===================================================================
%%% Internal functions - test case specific mocks
%%%===================================================================


%% @private
-spec mock_import_file_error(oct_background:node_selector(), binary()) -> ok.
mock_import_file_error(ProviderSelector, ErroneousFile) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, storage_import_engine),
    ok = test_utils:mock_expect(Nodes, storage_import_engine, import_file_unsafe,
        fun(StorageFileCtx, Info) ->
            case storage_file_ctx:get_file_name_const(StorageFileCtx) of
                ErroneousFile -> throw(test_error);
                _ -> meck:passthrough([StorageFileCtx, Info])
            end
        end
    ).


%% @private
-spec unmock_import_file_error(storage_import_test_utils:suite_ctx()) -> ok.
unmock_import_file_error(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_unload(Nodes, storage_import_engine).


%% @private
-spec mock_uid_and_gid(oct_background:node_selector(), oct_background:entity_selector()) -> ok.
mock_uid_and_gid(ProviderSelector, SpaceOwnerSelector) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    SpaceOwnerUserId = oct_background:to_entity_id(SpaceOwnerSelector),
    ok = test_utils:mock_new(Nodes, [luma]),
    ok = test_utils:mock_expect(Nodes, luma, map_uid_to_onedata_user, fun(_, _, _) ->
        {ok, SpaceOwnerUserId}
    end),
    test_utils:mock_expect(Nodes, luma, map_to_display_credentials, fun(_, _, _) ->
        {ok, {?TEST_UID, ?TEST_GID}}
    end).


%% @private
-spec unmock_luma(storage_import_test_utils:suite_ctx()) -> ok.
unmock_luma(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_unload(Nodes, [luma]).
