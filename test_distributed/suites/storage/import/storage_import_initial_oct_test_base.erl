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
-module(storage_import_initial_oct_test_base).
-author("Katarzyna Such").

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
    %% --- structure ---
    import_empty_storage_test/1,
    import_empty_directory_test/1,
    import_empty_file_test/1,
    import_file_with_content_test/1,
    import_file_in_directory_test/1,
    import_many_subfiles_test/1,
    import_many_directories_test/1,
    import_nested_directory_tree_test/1,

    %% --- ownership (LUMA uid/gid) ---
    import_directory_check_user_id_test/1,
    import_file_check_user_id_test/1,
    import_directory_check_user_id_error_test/1,
    import_file_check_user_id_error_test/1,

    %% --- permissions ---
    import_directory_without_read_permission_test/1,

    %% --- acl ---
    import_nfs_acl_test/1,
    import_nfs_acl_with_disabled_luma_should_fail_test/1,

    %% --- ignored entries ---
    import_ignores_fifo_test/1,

    %% --- failure ---
    import_directory_error_test/1
]).

-define(ATTEMPTS, 30).

%% A representative NFS4 ACL containing a named principal ("ala@nfsdomain.org") that
%% requires LUMA mapping - used to test ACL import and its failure when the principal
%% cannot be mapped to a Onedata user.
-define(TEST_NFS4_ACL, [
    #access_control_entity{
        acetype = ?allow_mask, aceflags = ?no_flags_mask,
        identifier = <<"OWNER@">>, acemask = ?read_acl_mask
    },
    #access_control_entity{
        acetype = ?deny_mask, aceflags = ?no_flags_mask,
        identifier = <<"GROUP@">>, acemask = ?write_acl_mask
    },
    #access_control_entity{
        acetype = ?allow_mask, aceflags = ?no_flags_mask,
        identifier = <<"EVERYONE@">>, acemask = ?read_acl_mask
    },
    #access_control_entity{
        acetype = ?deny_mask, aceflags = ?no_flags_mask,
        identifier = <<"ala@nfsdomain.org">>, acemask = ?write_attributes_mask
    }
]).


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

end_per_testcase(Case, TestSuiteCtx, Config) when
    Case =:= import_directory_check_user_id_test;
    Case =:= import_file_check_user_id_test;
    Case =:= import_directory_check_user_id_error_test;
    Case =:= import_file_check_user_id_error_test
->
    unmock_luma(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(Case = import_nfs_acl_test, TestSuiteCtx, Config) ->
    unmock_storage_driver(TestSuiteCtx),
    unmock_luma(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(Case = import_nfs_acl_with_disabled_luma_should_fail_test, TestSuiteCtx, Config) ->
    unmock_storage_driver(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(_Case, _TestSuiteCtx, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Tests
%%%===================================================================


%% --- structure ---


import_empty_storage_test(SuiteCtx) ->
    TestCaseCtx = storage_import_test_utils:init_testcase(?FUNCTION_NAME, undefined, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:verify_dir_stats(TestCaseCtx),
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
    storage_import_test_utils:verify_dir_stats(TestCaseCtx),

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


import_empty_file_test(SuiteCtx) ->
    TestCaseCtx = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = ?RAND_STR()}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:verify_dir_stats(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }).


import_file_with_content_test(SuiteCtx) ->
    TestCaseCtx = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = ?RAND_STR(), content = ?RAND_STR()}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:verify_dir_stats(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }).


import_file_in_directory_test(SuiteCtx) ->
    FileTreeSpec = #dir_spec{children = [#file_spec{content = ?RAND_STR()}]},
    TestCaseCtx = storage_import_test_utils:init_testcase(?FUNCTION_NAME, FileTreeSpec, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:verify_dir_stats(TestCaseCtx),
    %% created = 2 (the directory and the file), derived from the declared tree
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }).


import_many_subfiles_test(SuiteCtx) ->
    SubdirsCount = 200,
    Content = ?RAND_STR(),
    FileTreeSpec = [
        #dir_spec{children = [#file_spec{content = Content}]}
        || _ <- lists:seq(1, SubdirsCount)
    ],
    TestCaseCtx = storage_import_test_utils:init_testcase(?FUNCTION_NAME, FileTreeSpec, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx, ?LARGE_IMPORT_SCAN_ATTEMPTS),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:verify_dir_stats(TestCaseCtx),
    %% createdMinHist is skipped - by the time the (large) tree is verified and the
    %% monitoring is read, the creations may have shifted out of the first minute
    %% histogram buckets; the cumulative hour/day histograms still hold the count
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1,
        <<"createdMinHist">> => skip
    }).


import_many_directories_test(SuiteCtx) ->
    DirsCount = 200,
    FileTreeSpec = [#dir_spec{} || _ <- lists:seq(1, DirsCount)],
    TestCaseCtx = storage_import_test_utils:init_testcase(?FUNCTION_NAME, FileTreeSpec, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx, ?LARGE_IMPORT_SCAN_ATTEMPTS),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:verify_dir_stats(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1,
        <<"createdMinHist">> => skip
    }).


%% Imports a deep, branching directory tree with files at the leaves:
%% [13, 13, 13] => 13 dirs x 13 subdirs x 13 files = 2379 nodes.
import_nested_directory_tree_test(SuiteCtx) ->
    FileTreeSpec = storage_import_test_utils:gen_nested_tree_spec([13, 13, 13], ?RAND_STR()),
    TestCaseCtx = storage_import_test_utils:init_testcase(?FUNCTION_NAME, FileTreeSpec, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx, ?LARGE_IMPORT_SCAN_ATTEMPTS),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:verify_dir_stats(TestCaseCtx),
    %% Min/Hour created histograms are skipped - importing and verifying ~2400 nodes
    %% takes long enough that the creations age out of the finer-grained buckets by
    %% the time the monitoring is read; the cumulative day histogram still holds them
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1,
        <<"createdMinHist">> => skip,
        <<"createdHourHist">> => skip
    }).


%% --- ownership (LUMA uid/gid) ---


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


import_file_check_user_id_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector,
        space_owner_selector = SpaceOwnerSelector
    } = SuiteCtx,
    mock_uid_and_gid(ImportingProviderSelector, SpaceOwnerSelector),

    FileName = ?RAND_STR(),
    TestCaseCtx = #storage_import_test_case_ctx{
        other_storage_id = OtherStorageId,
        space_id = SpaceId,
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx,
        non_importing_provider_ctx = NonImportingProviderCtx
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME,
        #file_spec{name = FileName, content = ?RAND_STR(), uid = ?TEST_UID, gid = ?TEST_GID},
        SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),

    %% Verify file ownership (mapped via mocked LUMA, provider-specific)
    #provider_ctx{node = ImportingProviderNode} = ImportingProviderCtx,
    #provider_ctx{node = NonImportingProviderNode} = NonImportingProviderCtx,
    SpaceTestFilePath = filepath_utils:join([SpacePath, FileName]),
    SpaceOwnerId = oct_background:to_entity_id(SpaceOwnerSelector),
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceTestFilePath, #{
        owner_id => SpaceOwnerId,
        uid => ?TEST_UID,
        gid => ?TEST_GID
    }),

    GeneratedUid = ?rpc(NonImportingProviderNode, luma_auto_feed:generate_uid(SpaceOwnerId)),
    StorageSDHandleNonImportingProvider = sd_test_utils:get_storage_mountpoint_handle(
        ImportingProviderNode, SpaceId, OtherStorageId
    ),
    {ok, #statbuf{st_gid = Gid2}} = sd_test_utils:stat(
        NonImportingProviderNode, StorageSDHandleNonImportingProvider
    ),
    storage_import_test_utils:assert_attrs(NonImportingProviderCtx, SpaceTestFilePath, #{
        owner_id => SpaceOwnerId,
        uid => GeneratedUid,
        gid => Gid2
    }),

    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }).


%% A directory whose owner uid cannot be mapped to a Onedata user (LUMA returns an
%% error) must not be imported - the scan reports it as failed (alongside the space
%% root, which is processed normally and counted as unmodified).
import_directory_check_user_id_error_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    mock_luma_error(ImportingProviderSelector),

    DirName = ?RAND_STR(),
    TestCaseCtx = #storage_import_test_case_ctx{
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{
            node = ImportingProviderNode,
            session_id = ImportingProviderSessionId
        }
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #dir_spec{name = DirName, uid = ?TEST_UID, gid = ?TEST_GID}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    %% The directory must not have been imported - mapping its owner via LUMA failed
    SpaceTestDirPath = filepath_utils:join([SpacePath, DirName]),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(ImportingProviderNode, ImportingProviderSessionId, {path, SpaceTestDirPath}),
        ?ATTEMPTS
    ),

    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"created">> => 0,
        <<"failed">> => 1,
        <<"unmodified">> => 1,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0
    }).


%% Like import_directory_check_user_id_error_test/1 but for a regular file.
import_file_check_user_id_error_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    mock_luma_error(ImportingProviderSelector),

    FileName = ?RAND_STR(),
    TestCaseCtx = #storage_import_test_case_ctx{
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{
            node = ImportingProviderNode,
            session_id = ImportingProviderSessionId
        }
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME,
        #file_spec{name = FileName, content = ?RAND_STR(), uid = ?TEST_UID, gid = ?TEST_GID},
        SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    %% The file must not have been imported - mapping its owner via LUMA failed
    SpaceTestFilePath = filepath_utils:join([SpacePath, FileName]),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(ImportingProviderNode, ImportingProviderSessionId, {path, SpaceTestFilePath}),
        ?ATTEMPTS
    ),

    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"created">> => 0,
        <<"failed">> => 1,
        <<"unmodified">> => 1,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0
    }).


%% --- permissions ---


%% Imports a single directory created on storage with no permissions (mode 8#000).
%% Only the directory's existence is verified - listing its contents is not permitted,
%% hence verify_imported_tree (which asserts the children set) is intentionally not used.
import_directory_without_read_permission_test(SuiteCtx) ->
    DirName = ?RAND_STR(),
    TestCaseCtx = #storage_import_test_case_ctx{
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx,
        non_importing_provider_ctx = NonImportingProviderCtx
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #dir_spec{name = DirName, mode = 8#000}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    SpaceTestDirPath = filepath_utils:join([SpacePath, DirName]),
    ExpAttrs = #{
        type => ?DIRECTORY_TYPE,
        mode => 8#000
    },
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceTestDirPath, ExpAttrs),
    %% This test skips verify_imported_tree (the 8#000 dir is not listable), so it
    %% does not implicitly wait for the dir to propagate to the non-importing
    %% provider - hence the longer attempts to tolerate the dbsync propagation lag.
    storage_import_test_utils:assert_attrs(
        NonImportingProviderCtx, SpaceTestDirPath, ExpAttrs, ?CROSS_PROVIDER_PROPAGATION_ATTEMPTS
    ),

    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }).


%% --- acl ---


%% A file carrying an NFS4 ACL on storage is imported (with sync_acl enabled) and
%% the ACL is applied: its named principal is mapped (via LUMA) to a Onedata user,
%% the ACL is readable as the cdmi_acl xattr and is enforced. Regular users (not the
%% space owner, who may bypass ACL checks) are used so the deny entries take effect.
import_nfs_acl_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    %% user1/user2 are regular members of the test space (from the 2op scenario)
    User1Selector = user1,
    User2Selector = user2,
    User1Id = oct_background:get_user_id(User1Selector),

    FileName = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),
    EncodedAcl = ?rpc(ImportingProviderSelector, storage_import_acl:encode(?TEST_NFS4_ACL)),
    mock_storage_file_acl(ImportingProviderSelector, StorageFileId, EncodedAcl),
    %% the file owner uid and the ACL's named principal both map to the user1
    mock_luma_acl_user(ImportingProviderSelector, User1Id),

    TestCaseCtx = #storage_import_test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{node = ImportingProviderNode}
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = ?RAND_STR()}, SuiteCtx, #{sync_acl => true}
    ),
    %% the imported file is owned by user1; make user1 and user2 space members so
    %% they can access it (membership propagates via dbsync, hence the retries below)
    ozw_test_rpc:add_user_to_space(SpaceId, User1Id),
    ozw_test_rpc:add_user_to_space(SpaceId, oct_background:get_user_id(User2Selector)),

    SpaceTestFilePath = filepath_utils:join([SpacePath, FileName]),
    User1SessId = oct_background:get_user_session_id(User1Selector, ImportingProviderSelector),
    User2SessId = oct_background:get_user_session_id(User2Selector, ImportingProviderSelector),

    %% the file was imported and is accessible to its owner
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(ImportingProviderNode, User1SessId, {path, SpaceTestFilePath}), ?ATTEMPTS),

    %% the owner may read the imported ACL (the named principal resolves to it) ...
    ExpectedAclJson = expected_imported_acl_json(
        oct_background:get_user_fullname(User1Selector), User1Id
    ),
    ?assertEqual({ok, ExpectedAclJson},
        get_cdmi_acl(ImportingProviderNode, User1SessId, SpaceTestFilePath), ?ATTEMPTS),

    %% ... but may neither set the ACL nor modify the file's attributes (ACL denies it)
    ?assertMatch({error, ?EACCES},
        lfm_proxy:set_xattr(ImportingProviderNode, User1SessId, {path, SpaceTestFilePath}, #xattr{})),
    ?assertMatch({error, ?EACCES},
        lfm_proxy:truncate(ImportingProviderNode, User1SessId, {path, SpaceTestFilePath}, 100)),

    %% the second (non-owner) user may also read the ACL (read_acl granted to EVERYONE@)
    ?assertEqual({ok, ExpectedAclJson},
        get_cdmi_acl(ImportingProviderNode, User2SessId, SpaceTestFilePath), ?ATTEMPTS),

    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }).


%% An NFS4 ACL on storage references a named principal that LUMA cannot map to a
%% Onedata user; with sync_acl enabled, importing the file fails and it is not
%% imported (the space root, processed without an ACL, stays unmodified).
import_nfs_acl_with_disabled_luma_should_fail_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    FileName = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),
    EncodedAcl = ?rpc(ImportingProviderSelector, storage_import_acl:encode(?TEST_NFS4_ACL)),
    mock_storage_file_acl(ImportingProviderSelector, StorageFileId, EncodedAcl),

    TestCaseCtx = #storage_import_test_case_ctx{
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{
            node = ImportingProviderNode,
            session_id = ImportingProviderSessionId
        }
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = ?RAND_STR()}, SuiteCtx, #{sync_acl => true}
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    %% The file must not have been imported - mapping the ACL principal via LUMA failed
    SpaceTestFilePath = filepath_utils:join([SpacePath, FileName]),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(ImportingProviderNode, ImportingProviderSessionId, {path, SpaceTestFilePath}),
        ?ATTEMPTS
    ),

    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"created">> => 0,
        <<"failed">> => 1,
        <<"unmodified">> => 1,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0
    }).


%% --- ignored entries ---


%% A FIFO (named pipe) is an unsupported file type that storage import must skip:
%% it is created on the storage but never imported into the logical filesystem
%% The scan still processes it (counted as unmodified, alongside the space root),
%% but it does not appear in the space.
import_ignores_fifo_test(SuiteCtx) ->
    FifoName = ?RAND_STR(),
    TestCaseCtx = #storage_import_test_case_ctx{
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx,
        non_importing_provider_ctx = NonImportingProviderCtx
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #storage_fifo_spec{name = FifoName}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    %% The fifo must not have been imported into the logical filesystem on either provider
    FifoPath = filepath_utils:join([SpacePath, FifoName]),
    lists:foreach(fun(#provider_ctx{node = Node, session_id = SessId}) ->
        ?assertEqual({ok, []},
            lfm_proxy:get_children(Node, SessId, {path, SpacePath}, 0, 1), ?ATTEMPTS),
        ?assertMatch({error, ?ENOENT},
            lfm_proxy:stat(Node, SessId, {path, FifoPath}), ?ATTEMPTS)
    end, [ImportingProviderCtx, NonImportingProviderCtx]),

    %% the space root and the (skipped) fifo are both counted as unmodified
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 2
    }).


%% --- failure ---


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


%% @private
%% Makes LUMA fail to map any storage uid to a Onedata user, so that importing a
%% file/dir owned by that uid fails. Torn down via unmock_luma/1.
-spec mock_luma_error(oct_background:node_selector()) -> ok.
mock_luma_error(ProviderSelector) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, [luma]),
    ok = test_utils:mock_expect(Nodes, luma, map_uid_to_onedata_user, fun(_, _, _) ->
        error(test_error)
    end).


%% @private
%% Mocks the storage driver so that the given (already encoded) NFS4 ACL is reported
%% as the xattr of the given storage file during the scan; all other files (including
%% the space root) keep their real xattrs. Torn down via unmock_storage_driver/1.
-spec mock_storage_file_acl(oct_background:node_selector(), helpers:file_id(), binary()) -> ok.
mock_storage_file_acl(ProviderSelector, StorageFileId, EncodedAcl) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, storage_driver),
    ok = test_utils:mock_expect(Nodes, storage_driver, getxattr, fun
        (#sd_handle{file = FileId}, _Name) when FileId =:= StorageFileId ->
            {ok, EncodedAcl};
        (Handle, Name) ->
            meck:passthrough([Handle, Name])
    end).


%% @private
-spec unmock_storage_driver(storage_import_test_utils:suite_ctx()) -> ok.
unmock_storage_driver(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_unload(Nodes, storage_driver).


%% @private
%% Mocks LUMA so that both the file owner uid and the named ACL principal map to
%% the given Onedata user, letting the imported NFS ACL be applied to that user.
%% Torn down via unmock_luma/1.
-spec mock_luma_acl_user(oct_background:node_selector(), od_user:id()) -> ok.
mock_luma_acl_user(ProviderSelector, UserId) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, [luma]),
    ok = test_utils:mock_expect(Nodes, luma, map_uid_to_onedata_user, fun(_, _, _) ->
        {ok, UserId}
    end),
    ok = test_utils:mock_expect(Nodes, luma, map_acl_user_to_onedata_user, fun(_, _) ->
        {ok, UserId}
    end).


%% @private
-spec get_cdmi_acl(node(), session:id(), file_meta:path()) -> {ok, json_utils:json_term()} | {error, term()}.
get_cdmi_acl(Node, SessId, Path) ->
    case lfm_proxy:get_xattr(Node, SessId, {path, Path}, <<"cdmi_acl">>) of
        {ok, #xattr{value = Value}} -> {ok, Value};
        {error, _} = Error -> Error
    end.


%% @private
%% Builds the expected cdmi_acl JSON for ?TEST_NFS4_ACL after import: the special
%% principals (OWNER@/GROUP@/EVERYONE@) are kept verbatim, while the named principal
%% is rendered as "<full_name>#<user_id>" of the user it was mapped to.
-spec expected_imported_acl_json(od_user:full_name(), od_user:id()) -> json_utils:json_term().
expected_imported_acl_json(PrincipalFullName, PrincipalUserId) ->
    [
        ace_json(?allow_mask, ?no_flags_mask, <<"OWNER@">>, ?read_acl_mask),
        ace_json(?deny_mask, ?no_flags_mask, <<"GROUP@">>, ?write_acl_mask),
        ace_json(?allow_mask, ?no_flags_mask, <<"EVERYONE@">>, ?read_acl_mask),
        ace_json(
            ?deny_mask, ?no_flags_mask,
            <<PrincipalFullName/binary, "#", PrincipalUserId/binary>>, ?write_attributes_mask
        )
    ].


%% @private
-spec ace_json(non_neg_integer(), non_neg_integer(), binary(), non_neg_integer()) -> json_utils:json_map().
ace_json(AceType, AceFlags, Identifier, AceMask) ->
    #{
        <<"acetype">> => ace_mask_hex(AceType),
        <<"aceflags">> => ace_mask_hex(AceFlags),
        <<"identifier">> => Identifier,
        <<"acemask">> => ace_mask_hex(AceMask)
    }.


%% @private
-spec ace_mask_hex(non_neg_integer()) -> binary().
ace_mask_hex(Mask) ->
    <<"0x", (integer_to_binary(Mask, 16))/binary>>.
