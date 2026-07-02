%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains base test functions for testing the initial storage
%%% import scan.
%%%
%%% The tests are declarative: each test declares the file tree to be created on
%%% the storage (via #dir_spec{}/#file_spec{} records), enables import by setting
%%% up a space supported by an imported storage and, once the scan finishes,
%%% declares the expected outcome. The heavy lifting (creating the tree on the
%%% storage, verifying the imported logical tree on both providers, asserting the
%%% monitoring counters) is done generically by storage_import_test_utils - see
%%% its module doc for an overview of the machinery.
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
    init_per_testcase/3,
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


init_per_testcase(_Case, _TestSuiteCtx, Config) ->
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
    storage_import_test_utils:unmock_luma(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(Case = import_nfs_acl_test, TestSuiteCtx, Config) ->
    storage_import_test_utils:unmock_storage_driver(TestSuiteCtx),
    storage_import_test_utils:unmock_luma(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(Case = import_nfs_acl_with_disabled_luma_should_fail_test, TestSuiteCtx, Config) ->
    storage_import_test_utils:unmock_storage_driver(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(_Case, _TestSuiteCtx, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Tests
%%%===================================================================


%% --- structure ---


import_empty_storage_test(SuiteCtx) ->
    import_file_tree_test_base(SuiteCtx, ?FUNCTION_NAME, undefined, #{}).


import_empty_file_test(SuiteCtx) ->
    import_file_tree_test_base(SuiteCtx, ?FUNCTION_NAME, #file_spec{name = ?RAND_STR()}, #{}).


import_file_with_content_test(SuiteCtx) ->
    import_file_tree_test_base(
        SuiteCtx, ?FUNCTION_NAME, #file_spec{name = ?RAND_STR(), content = ?RAND_STR()}, #{}
    ).


import_file_in_directory_test(SuiteCtx) ->
    import_file_tree_test_base(
        SuiteCtx, ?FUNCTION_NAME, #dir_spec{children = [#file_spec{content = ?RAND_STR()}]}, #{}
    ).


import_many_subfiles_test(SuiteCtx) ->
    Content = ?RAND_STR(),
    FileTreeSpec = [
        #dir_spec{children = [#file_spec{content = Content}]}
        || _ <- lists:seq(1, 200)
    ],
    %% importing/verifying a large tree may outlast the Min (or, for even larger
    %% trees, Hour) histogram windows - see assert_storage_import_monitoring_state/2
    import_file_tree_test_base(SuiteCtx, ?FUNCTION_NAME, FileTreeSpec, #{
        await_attempts => ?LARGE_IMPORT_SCAN_ATTEMPTS,
        monitoring_overrides => #{<<"createdMinHist">> => skip}
    }).


import_many_directories_test(SuiteCtx) ->
    FileTreeSpec = [#dir_spec{} || _ <- lists:seq(1, 200)],
    import_file_tree_test_base(SuiteCtx, ?FUNCTION_NAME, FileTreeSpec, #{
        await_attempts => ?LARGE_IMPORT_SCAN_ATTEMPTS,
        monitoring_overrides => #{<<"createdMinHist">> => skip}
    }).


import_nested_directory_tree_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{storage_type = StorageType} = SuiteCtx,
    %% [13, 13, 13] => 13 dirs x 13 subdirs x 13 files = 2379 nodes
    FileTreeSpec = storage_import_test_utils:gen_nested_tree_spec([13, 13, 13], ?RAND_STR()),
    import_file_tree_test_base(SuiteCtx, ?FUNCTION_NAME, FileTreeSpec, #{
        await_attempts => ?LARGE_IMPORT_SCAN_ATTEMPTS,
        monitoring_overrides => #{
            <<"createdMinHist">> => skip,
            <<"createdHourHist">> => skip,
            %% on s3 the space is imported as a single flat listing of all 2197
            %% file objects, read in batches of storage_import_dir_batch_size
            %% (default 1000) - the space root dir gets ceil(2197/1000) = 3
            %% batch passes, each counted towards "unmodified" (on posix the
            %% root has only 13 direct children => a single pass)
            <<"unmodified">> => case StorageType of
                posix -> 1;
                s3 -> 3
            end
        }
    }).


%% Beyond the shared tree verification, also verifies the imported directory's
%% ownership: with no LUMA mappings configured, the importing provider reports
%% the imported storage's mountpoint uid (and gid 0), while on the non-importing
%% provider (where the directory has no imported-storage counterpart) both uid
%% and gid default to that provider's own storage mountpoint ownership.
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

    #provider_ctx{node = ImportingProviderNode} = ImportingProviderCtx,
    #provider_ctx{node = NonImportingProviderNode} = NonImportingProviderCtx,
    {MountUidImportingProvider, _} = get_storage_mountpoint_owner(
        ImportingProviderNode, ImportingProviderNode, SpaceId, ImportedStorageId
    ),
    {MountUidNonImportingProvider, MountGidNonImportingProvider} = get_storage_mountpoint_owner(
        ImportingProviderNode, NonImportingProviderNode, SpaceId, OtherStorageId
    ),

    SpaceOwnerId = ?SPACE_OWNER_ID(SpaceId),
    SpaceTestDirPath = filepath_utils:join([SpacePath, DirName]),
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

    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}).


%% --- ownership (LUMA uid/gid) ---


import_directory_check_user_id_test(SuiteCtx) ->
    DirName = ?RAND_STR(),
    import_check_user_id_test_base(
        SuiteCtx, ?FUNCTION_NAME, DirName,
        #dir_spec{name = DirName, uid = ?TEST_UID, gid = ?TEST_GID}
    ).


import_file_check_user_id_test(SuiteCtx) ->
    FileName = ?RAND_STR(),
    import_check_user_id_test_base(
        SuiteCtx, ?FUNCTION_NAME, FileName,
        #file_spec{name = FileName, content = ?RAND_STR(), uid = ?TEST_UID, gid = ?TEST_GID}
    ).


import_directory_check_user_id_error_test(SuiteCtx) ->
    DirName = ?RAND_STR(),
    import_check_user_id_error_test_base(
        SuiteCtx, ?FUNCTION_NAME, DirName,
        #dir_spec{name = DirName, uid = ?TEST_UID, gid = ?TEST_GID}
    ).


import_file_check_user_id_error_test(SuiteCtx) ->
    FileName = ?RAND_STR(),
    import_check_user_id_error_test_base(
        SuiteCtx, ?FUNCTION_NAME, FileName,
        #file_spec{name = FileName, content = ?RAND_STR(), uid = ?TEST_UID, gid = ?TEST_GID}
    ).


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
    %% with verify_imported_tree skipped, nothing has awaited the dir's propagation
    %% to the non-importing provider yet - use attempts tolerating the dbsync lag
    storage_import_test_utils:assert_attrs(
        NonImportingProviderCtx, SpaceTestDirPath, ExpAttrs, ?CROSS_PROVIDER_PROPAGATION_ATTEMPTS
    ),

    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}).


%% --- acl ---


%% A file carrying an NFS4 ACL on storage is imported (with sync_acl enabled) and
%% the ACL is applied: its named principal is mapped (via LUMA) to a Onedata user,
%% the ACL is readable as the cdmi_acl xattr and is enforced. Regular users (not the
%% space owner, who may bypass ACL checks) are used so the deny entries take effect.
import_nfs_acl_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    %% user1/user2 are regular users of the 2op scenario, not yet members of the space
    User1Selector = user1,
    User2Selector = user2,
    User1Id = oct_background:get_user_id(User1Selector),

    FileName = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),
    EncodedAcl = ?rpc(ImportingProviderSelector, storage_import_acl:encode(?TEST_NFS4_ACL)),
    storage_import_test_utils:mock_storage_file_acl(SuiteCtx, StorageFileId, EncodedAcl),
    %% the file owner uid and the ACL's named principal both map to user1
    storage_import_test_utils:mock_luma_acl_user(SuiteCtx, User1Id),

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
    ExpectedAclJson = storage_import_test_utils:expected_imported_acl_json(
        ?TEST_NFS4_ACL, oct_background:get_user_fullname(User1Selector), User1Id
    ),
    ?assertEqual({ok, ExpectedAclJson},
        storage_import_test_utils:get_cdmi_acl(ImportingProviderNode, User1SessId, SpaceTestFilePath), ?ATTEMPTS),

    %% ... but may neither set the ACL nor modify the file's attributes (ACL denies it)
    ?assertMatch({error, ?EACCES},
        lfm_proxy:set_xattr(ImportingProviderNode, User1SessId, {path, SpaceTestFilePath}, #xattr{})),
    ?assertMatch({error, ?EACCES},
        lfm_proxy:truncate(ImportingProviderNode, User1SessId, {path, SpaceTestFilePath}, 100)),

    %% the second (non-owner) user may also read the ACL (read_acl granted to EVERYONE@)
    ?assertEqual({ok, ExpectedAclJson},
        storage_import_test_utils:get_cdmi_acl(ImportingProviderNode, User2SessId, SpaceTestFilePath), ?ATTEMPTS),

    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}).


%% An NFS4 ACL on storage references a named principal that LUMA cannot map to a
%% Onedata user; with sync_acl enabled, importing the file fails and it is not imported.
import_nfs_acl_with_disabled_luma_should_fail_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    FileName = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),
    EncodedAcl = ?rpc(ImportingProviderSelector, storage_import_acl:encode(?TEST_NFS4_ACL)),
    storage_import_test_utils:mock_storage_file_acl(SuiteCtx, StorageFileId, EncodedAcl),

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

    SpaceTestFilePath = filepath_utils:join([SpacePath, FileName]),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(ImportingProviderNode, ImportingProviderSessionId, {path, SpaceTestFilePath}),
        ?ATTEMPTS
    ),
    assert_monitoring_state_after_failed_import(TestCaseCtx).


%% --- ignored entries ---


%% A FIFO (named pipe) is an unsupported file type that storage import must skip:
%% it is created on the storage but never imported into the logical filesystem.
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

    %% the fifo must not have been imported into the logical filesystem on either provider
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


%% Importing the declared directory fails (via a mock raising an error from the
%% import engine); the directory must not appear in the space, while remaining
%% intact on the storage.
import_directory_error_test(SuiteCtx) ->
    DirName = ?RAND_STR(),
    mock_import_file_error(SuiteCtx, DirName),

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

    %% the dir was not imported ...
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

    assert_monitoring_state_after_failed_import(TestCaseCtx).


%%%===================================================================
%%% Internal functions - shared test bodies
%%%===================================================================


%% @private
%% Shared body for the structure tests: imports the declared tree and runs the
%% full generic verification (imported tree + dir stats + monitoring counters).
%% Opts:
%%   await_attempts - for trees big enough that the scan may outlast ?ATTEMPTS;
%%   monitoring_overrides - assert_storage_import_monitoring_state/2 overrides.
-spec import_file_tree_test_base(storage_import_test_utils:suite_ctx(), atom(), term(), map()) ->
    ok.
import_file_tree_test_base(SuiteCtx, CaseName, FileTreeSpec, Opts) ->
    TestCaseCtx = storage_import_test_utils:init_testcase(CaseName, FileTreeSpec, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(
        TestCaseCtx, maps:get(await_attempts, Opts, ?ATTEMPTS)
    ),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:verify_dir_stats(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(
        TestCaseCtx, maps:get(monitoring_overrides, Opts, #{})
    ).


%% @private
%% Shared body for the ownership tests: the declared node is owned on the storage
%% by ?TEST_UID/?TEST_GID and (mocked) LUMA maps that owner to the space owner
%% with the same display credentials - so the importing provider reports them
%% verbatim. On the non-importing provider the node has no imported-storage
%% counterpart, so its uid falls back to the LUMA auto-feed generated one and its
%% gid to that provider's storage mountpoint gid.
-spec import_check_user_id_test_base(
    storage_import_test_utils:suite_ctx(), atom(), binary(), term()
) ->
    ok.
import_check_user_id_test_base(SuiteCtx, CaseName, NodeName, NodeSpec) ->
    #storage_import_test_suite_ctx{space_owner_selector = SpaceOwnerSelector} = SuiteCtx,
    mock_uid_and_gid(SuiteCtx),

    TestCaseCtx = #storage_import_test_case_ctx{
        other_storage_id = OtherStorageId,
        space_id = SpaceId,
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx,
        non_importing_provider_ctx = NonImportingProviderCtx
    } = storage_import_test_utils:init_testcase(CaseName, NodeSpec, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),

    SpaceTestNodePath = filepath_utils:join([SpacePath, NodeName]),
    SpaceOwnerId = oct_background:to_entity_id(SpaceOwnerSelector),
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceTestNodePath, #{
        owner_id => SpaceOwnerId,
        uid => ?TEST_UID,
        gid => ?TEST_GID
    }),

    #provider_ctx{node = ImportingProviderNode} = ImportingProviderCtx,
    #provider_ctx{node = NonImportingProviderNode} = NonImportingProviderCtx,
    GeneratedUid = ?rpc(NonImportingProviderNode, luma_auto_feed:generate_uid(SpaceOwnerId)),
    {_, MountGidNonImportingProvider} = get_storage_mountpoint_owner(
        ImportingProviderNode, NonImportingProviderNode, SpaceId, OtherStorageId
    ),
    storage_import_test_utils:assert_attrs(NonImportingProviderCtx, SpaceTestNodePath, #{
        owner_id => SpaceOwnerId,
        uid => GeneratedUid,
        gid => MountGidNonImportingProvider
    }),

    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}).


%% @private
%% Shared body for the ownership-error tests: LUMA fails to map the declared
%% node's storage owner uid to any Onedata user, so the node must not be
%% imported and the scan reports one failure.
-spec import_check_user_id_error_test_base(
    storage_import_test_utils:suite_ctx(), atom(), binary(), term()
) ->
    ok.
import_check_user_id_error_test_base(SuiteCtx, CaseName, NodeName, NodeSpec) ->
    mock_luma_error(SuiteCtx),

    TestCaseCtx = #storage_import_test_case_ctx{
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{
            node = ImportingProviderNode,
            session_id = ImportingProviderSessionId
        }
    } = storage_import_test_utils:init_testcase(CaseName, NodeSpec, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    SpaceTestNodePath = filepath_utils:join([SpacePath, NodeName]),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(ImportingProviderNode, ImportingProviderSessionId, {path, SpaceTestNodePath}),
        ?ATTEMPTS
    ),
    assert_monitoring_state_after_failed_import(TestCaseCtx).


%%%===================================================================
%%% Internal functions - shared assertions
%%%===================================================================


%% @private
%% Monitoring expectation for an initial scan that failed to import the single
%% declared node: nothing created, one failure (the space root itself is still
%% processed normally, keeping the default unmodified count).
-spec assert_monitoring_state_after_failed_import(storage_import_test_utils:case_ctx()) -> ok.
assert_monitoring_state_after_failed_import(TestCaseCtx) ->
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"created">> => 0,
        <<"failed">> => 1,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0
    }).


%% @private
%% Stats the storage mountpoint (the sd handle is built on HandleNode, the stat
%% itself runs on StatNode) and returns its {Uid, Gid} - the ownership that
%% files with no LUMA-mapped/storage-derived owner default to.
-spec get_storage_mountpoint_owner(node(), node(), od_space:id(), storage:id()) ->
    {non_neg_integer(), non_neg_integer()}.
get_storage_mountpoint_owner(HandleNode, StatNode, SpaceId, StorageId) ->
    SDHandle = sd_test_utils:get_storage_mountpoint_handle(HandleNode, SpaceId, StorageId),
    {ok, #statbuf{st_uid = Uid, st_gid = Gid}} = sd_test_utils:stat(StatNode, SDHandle),
    {Uid, Gid}.


%%%===================================================================
%%% Internal functions - test case specific mocks
%%%===================================================================


%% @private
%% Mocks a failure of importing the given file/dir: the import engine raises for
%% it, while all other entries import normally. Torn down via unmock_import_file_error/1.
-spec mock_import_file_error(storage_import_test_utils:suite_ctx(), binary()) -> ok.
mock_import_file_error(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}, ErroneousFile) ->
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
%% Mocks LUMA so that any storage uid maps to the space owner, displayed with
%% the ?TEST_UID/?TEST_GID credentials. Torn down via storage_import_test_utils:unmock_luma/1.
-spec mock_uid_and_gid(storage_import_test_utils:suite_ctx()) -> ok.
mock_uid_and_gid(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector,
    space_owner_selector = SpaceOwnerSelector
}) ->
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
%% Makes LUMA fail to map any storage uid to a Onedata user, so that importing a
%% file/dir owned by that uid fails. Torn down via storage_import_test_utils:unmock_luma/1.
-spec mock_luma_error(storage_import_test_utils:suite_ctx()) -> ok.
mock_luma_error(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, [luma]),
    ok = test_utils:mock_expect(Nodes, luma, map_uid_to_onedata_user, fun(_, _, _) ->
        error(test_error)
    end).
