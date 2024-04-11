%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains base test functions for testing storage import.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_oct_test_base).
-author("Katarzyna Such").

-include("storage_import_oct_test.hrl").
-include("modules/logical_file_manager/lfm.hrl").


% API
-export([
    clean_up_after_previous_run/2
]).

%% tests
-export([
    import_empty_storage_test/1,
    import_empty_directory_test/1,
    import_directory_error_test/1,
    import_directory_check_user_id_test/1,

    import_empty_file_test/1,
    import_file_with_content_test/1
]).


-record(provider_ctx, {
    random_node :: oct_background:node(),
    session_id :: oct_background:entity_id()
}).

-type provider_ctx() :: #provider_ctx{}.
-type storage_import_test_suite_ctx() :: #storage_import_test_suite_ctx{}.

-record(storage_import_test_case_ctx, {
    suite_ctx :: storage_import_test_suite_ctx(),
    imported_storage_id :: storage:id(),
    other_storage_id :: storage:id(),
    space_id :: od_space:id(),
    space_path :: file_meta:path(),
    importing_provider_ctx :: provider_ctx(),
    non_importing_provider_ctx :: provider_ctx()
}).

-type storage_import_test_case_ctx() :: #storage_import_test_case_ctx{}.

-export_type([storage_import_test_suite_ctx/0]).

-define(ATTEMPTS, 30).


%%%===================================================================
%%% API
%%%===================================================================


-spec clean_up_after_previous_run([atom()], storage_import_test_suite_ctx()) -> ok.
clean_up_after_previous_run(AllTestCases, SuiteCtx) ->
    lists_utils:pforeach(fun(SpaceId) ->
        delete_space_with_supporting_storages(SpaceId, SuiteCtx)
    end, filter_spaces_from_previous_run(AllTestCases)).


%%%===================================================================
%%% Tests
%%%===================================================================


import_empty_storage_test(TestSuiteCtx = #storage_import_test_suite_ctx{
    importing_provider_selector = ImportingProviderSelector
}) ->
    #storage_import_test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{
            random_node = NodeImportingProvider,
            session_id = SessionIdImportingProvider
        },
        non_importing_provider_ctx = #provider_ctx{
            random_node = NodeNonImportingProvider,
            session_id = SessionIdNonImportingProvider
        }
    } = init_testcase(?FUNCTION_NAME, undefined, TestSuiteCtx),
    await_initial_scan_finished(ImportingProviderSelector, SpaceId),

    % TODO VFS-11902 implement generic verification of imported files structure/data
    SpacePathFileKey = {path, SpacePath},
    ?assertMatch(
        {ok, []},
        lfm_proxy:get_children(NodeImportingProvider, SessionIdImportingProvider, SpacePathFileKey, 0, 10)
    ),
    ?assertMatch(
        {ok, #file_attr{}},
        lfm_proxy:stat(NodeImportingProvider, SessionIdImportingProvider, SpacePathFileKey)
    ),
    ?assertMatch(
        {ok, #file_attr{}},
        lfm_proxy:stat(NodeNonImportingProvider, SessionIdNonImportingProvider, SpacePathFileKey),
        ?ATTEMPTS
    ),

    assert_storage_import_monitoring_state(ImportingProviderSelector, SpaceId, #{
        <<"scans">> => 1,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }).


import_empty_directory_test(TestSuiteCtx = #storage_import_test_suite_ctx{
    importing_provider_selector = ImportingProviderSelector
}) ->
    DirName = ?RAND_STR(),

    #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        other_storage_id = OtherStorageId,
        space_id = SpaceId,
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{
            random_node = NodeImportingProvider,
            session_id = SessionIdImportingProvider
        },
        non_importing_provider_ctx = #provider_ctx{
            random_node = NodeNonImportingProvider,
            session_id = SessionIdNonImportingProvider
        }
    } = init_testcase(?FUNCTION_NAME, #dir_spec{name = DirName}, TestSuiteCtx),
    await_initial_scan_finished(ImportingProviderSelector, SpaceId),

    % TODO VFS-11902 implement generic verification of imported files structure/data
    %% Check if dir was imported
    SpacePathFileKey = {path, SpacePath},
    ?assertMatch(
        {ok, [{_, DirName}]},
        lfm_proxy:get_children(NodeImportingProvider, SessionIdImportingProvider, SpacePathFileKey, 0, 10),
        ?ATTEMPTS
    ),

    SpaceTestDirPath = filename:join([<<"/">>, ?FUNCTION_NAME, DirName]),
    StorageSDHandleImportingProvider = sd_test_utils:get_storage_mountpoint_handle(
        NodeImportingProvider, SpaceId, ImportedStorageId
    ),
    StorageSDHandleNonImportingProvider = sd_test_utils:get_storage_mountpoint_handle(
        NodeImportingProvider, SpaceId, OtherStorageId
    ),
    {ok, #statbuf{st_uid = MountUidImportingProvider}} = sd_test_utils:stat(
        NodeImportingProvider, StorageSDHandleImportingProvider
    ),
    {ok, #statbuf{
        st_uid = MountUidNonImportingProvider,
        st_gid = MountGidNonImportingProvider
    }} = sd_test_utils:stat(NodeNonImportingProvider, StorageSDHandleNonImportingProvider),

    SpaceOwnerId = ?SPACE_OWNER_ID(SpaceId),

    ?assertMatch(
        {ok, #file_attr{
            owner_id = SpaceOwnerId,
            uid = MountUidImportingProvider,
            gid = 0
        }},
        lfm_proxy:stat(NodeImportingProvider, SessionIdImportingProvider, {path, SpaceTestDirPath}),
        ?ATTEMPTS
    ),
    ?assertMatch(
        {ok, #file_attr{
            owner_id = SpaceOwnerId,
            uid = MountUidNonImportingProvider,
            gid = MountGidNonImportingProvider
        }},
        lfm_proxy:stat(NodeNonImportingProvider, SessionIdNonImportingProvider, {path, SpaceTestDirPath}),
        ?ATTEMPTS
    ),

    assert_storage_import_monitoring_state(ImportingProviderSelector, SpaceId, #{
        <<"scans">> => 1,
        <<"created">> => 1,
        <<"modified">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 1,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 0,
        <<"modifiedHourHist">> => 0,
        <<"modifiedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }).


import_directory_error_test(TestSuiteCtx = #storage_import_test_suite_ctx{
    importing_provider_selector = ImportingProviderSelector
}) ->
    DirName = ?RAND_STR(),

    #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_id = SpaceId,
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{
            random_node = NodeImportingProvider,
            session_id = SessionIdImportingProvider
        },
        non_importing_provider_ctx = #provider_ctx{
            random_node = NodeNonImportingProvider,
            session_id = SessionIdNonImportingProvider
        }
    } = init_testcase(?FUNCTION_NAME,  #dir_spec{name = DirName}, TestSuiteCtx),

    SpaceTestDirPath = filename:join([<<"/">>, ?FUNCTION_NAME, DirName]),
    await_initial_scan_finished(ImportingProviderSelector, SpaceId),
    SpacePathFileKey = {path, SpacePath},

    %% Check if dir was not imported
    ?assertMatch({ok, []},
        lfm_proxy:get_children(NodeImportingProvider, SessionIdImportingProvider, SpacePathFileKey, 0, 1),
        ?ATTEMPTS
    ),
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(NodeImportingProvider, SessionIdImportingProvider, {path, SpaceTestDirPath}),
        ?ATTEMPTS
    ),
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(NodeNonImportingProvider, SessionIdNonImportingProvider, {path, SpaceTestDirPath}),
        ?ATTEMPTS
    ),
    StorageSDHandleImportingProvider = sd_test_utils:get_storage_mountpoint_handle(
        NodeImportingProvider, SpaceId, ImportedStorageId
    ),

    %% Check if dir is still on storage
    ?assertMatch({ok, [DirName]}, sd_test_utils:ls(NodeImportingProvider, StorageSDHandleImportingProvider, 0, 1)),

    assert_storage_import_monitoring_state(ImportingProviderSelector, SpaceId, #{
        <<"scans">> => 1,
        <<"created">> => 0,
        <<"modified">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 1,
        <<"unmodified">> => 1,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"modifiedMinHist">> => 0,
        <<"modifiedHourHist">> => 0,
        <<"modifiedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }).


import_directory_check_user_id_test(TestSuiteCtx = #storage_import_test_suite_ctx{
    importing_provider_selector = ImportingProviderSelector,
    space_owner_selector = SpaceOwnerSelector
}) ->
    DirName = ?RAND_STR(),

    #storage_import_test_case_ctx{
        other_storage_id = OtherStorageId,
        space_id = SpaceId,
        importing_provider_ctx = #provider_ctx{
            random_node = NodeImportingProvider,
            session_id = SessionIdImportingProvider
        },
        non_importing_provider_ctx = #provider_ctx{
            random_node = NodeNonImportingProvider,
            session_id = SessionIdNonImportingProvider
        }
    } = init_testcase(?FUNCTION_NAME,  #dir_spec{name = DirName}, TestSuiteCtx),
    await_initial_scan_finished(ImportingProviderSelector, SpaceId),

    SpaceTestDirPath = filename:join([<<"/">>, ?FUNCTION_NAME, DirName]),
    SpaceOwnerId = oct_background:to_entity_id(SpaceOwnerSelector),

    ?assertMatch({ok, #file_attr{
        owner_id = SpaceOwnerId,
        uid = ?TEST_UID,
        gid = ?TEST_GID
    }}, lfm_proxy:stat(NodeImportingProvider, SessionIdImportingProvider, {path, SpaceTestDirPath}), ?ATTEMPTS),

    GeneratedUid = ?rpc(NodeNonImportingProvider, luma_auto_feed:generate_uid(SpaceOwnerId)),
    StorageSDHandleNonImportingProvider = sd_test_utils:get_storage_mountpoint_handle(
        NodeImportingProvider, SpaceId, OtherStorageId
    ),
    {ok, #statbuf{st_gid = Gid2}} = sd_test_utils:stat(NodeNonImportingProvider, StorageSDHandleNonImportingProvider),

    ?assertMatch({ok, #file_attr{
        owner_id = SpaceOwnerId,
        uid = GeneratedUid,
        gid = Gid2
    }}, lfm_proxy:stat(NodeNonImportingProvider, SessionIdNonImportingProvider, {path, SpaceTestDirPath}), ?ATTEMPTS),

    assert_storage_import_monitoring_state(ImportingProviderSelector, SpaceId, #{
        <<"scans">> => 1,
        <<"created">> => 1,
        <<"modified">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 1,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 0,
        <<"modifiedHourHist">> => 0,
        <<"modifiedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }).


import_empty_file_test(TestSuiteCtx = #storage_import_test_suite_ctx{
    importing_provider_selector = ImportingProviderSelector
}) ->
    FileName = ?RAND_STR(),
    #storage_import_test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{
            random_node = NodeImportingProvider,
            session_id = SessionIdImportingProvider
        },
        non_importing_provider_ctx = #provider_ctx{
            random_node = NodeNonImportingProvider,
            session_id = SessionIdNonImportingProvider
        }
    }  = init_testcase(?FUNCTION_NAME, #file_spec{name = FileName}, TestSuiteCtx),
    await_initial_scan_finished(ImportingProviderSelector, SpaceId),

    SpacePathFileKey = {path, filepath_utils:join([SpacePath, FileName])},

    %% Check if file was imported on ImportingProvider
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(NodeImportingProvider, SessionIdImportingProvider, SpacePathFileKey),
    ?ATTEMPTS),

    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(NodeImportingProvider, SessionIdImportingProvider, SpacePathFileKey, read)),
    ?assertMatch({ok, <<>>},
        lfm_proxy:check_size_and_read(NodeImportingProvider, Handle1, 0, 100)),
    lfm_proxy:close(NodeImportingProvider, Handle1),

    assert_storage_import_monitoring_state(ImportingProviderSelector, SpaceId, #{
        <<"scans">> => 1,
        <<"created">> => 1,
        <<"modified">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 1,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 0,
        <<"modifiedHourHist">> => 0,
        <<"modifiedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }),

    %% Check if file was imported on NonImportingProvider
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(NodeNonImportingProvider, SessionIdNonImportingProvider, SpacePathFileKey), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(NodeNonImportingProvider, SessionIdNonImportingProvider, SpacePathFileKey, read), ?ATTEMPTS),
    ?assertMatch({ok, <<>>},
        lfm_proxy:check_size_and_read(NodeNonImportingProvider, Handle2, 0, 1), ?ATTEMPTS).


import_file_with_content_test(TestSuiteCtx = #storage_import_test_suite_ctx{
    importing_provider_selector = ImportingProviderSelector
}) ->
    FileName = ?RAND_STR(),
    FileContent = ?RAND_STR(),

    #storage_import_test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{
            random_node = NodeImportingProvider,
            session_id = SessionIdImportingProvider
        },
        non_importing_provider_ctx = #provider_ctx{
            random_node = NodeNonImportingProvider,
            session_id = SessionIdNonImportingProvider
        }
    } = init_testcase(?FUNCTION_NAME, #file_spec{name = FileName, content = FileContent}, TestSuiteCtx),
    await_initial_scan_finished(ImportingProviderSelector, SpaceId),

    SpacePathFileKey = {path, filepath_utils:join([SpacePath, FileName])},

    %% Check if file was imported on ImportingProvider
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(NodeImportingProvider, SessionIdImportingProvider, SpacePathFileKey),
        ?ATTEMPTS),

    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(NodeImportingProvider, SessionIdImportingProvider, SpacePathFileKey, read)),
    ?assertMatch({ok, FileContent},
        lfm_proxy:check_size_and_read(NodeImportingProvider, Handle1, 0, 100)),
    lfm_proxy:close(NodeImportingProvider, Handle1),

    assert_storage_import_monitoring_state(ImportingProviderSelector, SpaceId, #{
        <<"scans">> => 1,
        <<"created">> => 1,
        <<"modified">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 1,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 0,
        <<"modifiedHourHist">> => 0,
        <<"modifiedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }),

    %% Check if file was imported on NonImportingProvider
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(NodeNonImportingProvider, SessionIdNonImportingProvider, SpacePathFileKey), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(NodeNonImportingProvider, SessionIdNonImportingProvider, SpacePathFileKey, read), ?ATTEMPTS),
    ?assertMatch({ok, FileContent},
        lfm_proxy:check_size_and_read(NodeNonImportingProvider, Handle2, 0, 100), ?ATTEMPTS).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec filter_spaces_from_previous_run([atom()]) -> [od_space:id()].
filter_spaces_from_previous_run(AllTestCases) ->
    lists:filter(fun(SpaceId) ->
        SpaceDetails = ozw_test_rpc:get_space_protected_data(?ROOT, SpaceId),
        SpaceName = maps:get(<<"name">>, SpaceDetails),
        lists:member(binary_to_atom(SpaceName), AllTestCases)
    end, ozw_test_rpc:list_spaces()).


%% @private
-spec delete_space_with_supporting_storages(od_space:id(), storage_import_test_suite_ctx()) ->
    ok.
delete_space_with_supporting_storages(SpaceId, #storage_import_test_suite_ctx{
    importing_provider_selector = ImportingProviderSelector,
    non_importing_provider_selector = NonImportingProviderSelector
}) ->
    [StorageImportingProvider] = opw_test_rpc:get_space_local_storages(
        ImportingProviderSelector, SpaceId
    ),
    [StorageNonImportingProvider] = opw_test_rpc:get_space_local_storages(
        NonImportingProviderSelector, SpaceId
    ),

    ok = ozw_test_rpc:delete_space(SpaceId),

    ok = delete_storage(ImportingProviderSelector, StorageImportingProvider),
    ok = delete_storage(NonImportingProviderSelector, StorageNonImportingProvider).


%% @private
-spec delete_storage(oct_background:node_selector(), storage:id()) -> ok.
delete_storage(NodeSelector, StorageId) ->
    ?assertEqual(ok, opw_test_rpc:call(NodeSelector, storage, delete, [StorageId]), ?ATTEMPTS).


%% @private
-spec init_testcase(
    atom(), undefined | onenv_file_test_utils:object_spec(), storage_import_test_suite_ctx()
) ->
    storage_import_test_case_ctx().
init_testcase(TestCaseName = import_directory_error_test, FileDesc = #dir_spec{name = DirName},
    TestSuiteCtx = #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector}
) ->
    mock_import_file_error(ImportingProviderSelector, DirName),
    init_testcase(default, TestCaseName, FileDesc, TestSuiteCtx);
init_testcase(TestCaseName = import_directory_check_user_id_test, FileDesc,
    TestSuiteCtx = #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector,
        space_owner_selector = SpaceOwnerSelector
    }
) ->
    Nodes = oct_background:get_provider_nodes(ImportingProviderSelector),
    SpaceOwnerUid = oct_background:to_entity_id(SpaceOwnerSelector),
    ok = test_utils:mock_new(Nodes, [luma]),
    ok = test_utils:mock_expect(Nodes, luma, map_uid_to_onedata_user, fun(_, _, _) ->
        {ok, SpaceOwnerUid}
    end),
    ok = test_utils:mock_expect(Nodes, luma, map_to_display_credentials, fun(_, _, _) ->
        {ok, {?TEST_UID, ?TEST_GID}}
    end),
    init_testcase(default, TestCaseName, FileDesc, TestSuiteCtx);
init_testcase(TestCaseName, FileDesc, TestSuiteCtx) ->
    init_testcase(default, TestCaseName, FileDesc, TestSuiteCtx).


%% @private
-spec init_testcase(
    atom(), atom(), undefined | onenv_file_test_utils:object_spec(), storage_import_test_suite_ctx()
) ->
    storage_import_test_case_ctx().
init_testcase(default, TestCaseName, FileDesc, TestSuiteCtx = #storage_import_test_suite_ctx{
    storage_type = StorageType,
    importing_provider_selector = ImportingProviderSelector,
    non_importing_provider_selector = NonImportingProviderSelector,
    space_owner_selector = SpaceOwnerSelector
}) ->
    ImportedStorageId = create_storage(StorageType, ImportingProviderSelector, true),
    FileDesc =/= undefined andalso ?rpc(ImportingProviderSelector, create_file_tree_on_storage(
        TestCaseName, ImportedStorageId, FileDesc
    )),

    OtherStorageId = create_storage(StorageType, NonImportingProviderSelector, false),

    SpaceId = space_setup_utils:set_up_space(#space_spec{
        name = TestCaseName,
        owner = SpaceOwnerSelector,
        users = [],
        supports = [
            #support_spec{
                provider = ImportingProviderSelector,
                storage_spec = ImportedStorageId,
                size = 1000000000
            },
            #support_spec{
                provider = NonImportingProviderSelector,
                storage_spec = OtherStorageId,
                size = 1000000000
            }
        ]
    }),
    SpaceNameBin = str_utils:to_binary(TestCaseName),

    #storage_import_test_case_ctx{
        suite_ctx = TestSuiteCtx,
        imported_storage_id = ImportedStorageId,
        other_storage_id = OtherStorageId,
        space_id = SpaceId,
        space_path = <<"/", SpaceNameBin/binary>>,
        importing_provider_ctx = build_provider_ctx(SpaceOwnerSelector, ImportingProviderSelector),
        non_importing_provider_ctx = build_provider_ctx(SpaceOwnerSelector, NonImportingProviderSelector)
    }.


%% @private
-spec create_storage(posix, oct_background:entity_selector(), boolean()) -> storage:id().
create_storage(posix, ProviderSelector, IsImported) ->
    space_setup_utils:create_storage(ProviderSelector, #posix_storage_params{
        mount_point = <<"/mnt/st_", (generator:gen_name())/binary>>,
        imported_storage = IsImported
    });
create_storage(s3, ProviderSelector, true) ->
    space_setup_utils:create_storage(ProviderSelector, #s3_storage_params{
        storage_path_type = <<"canonical">>,
        imported_storage = true,
        hostname = build_s3_hostname(ProviderSelector),
        bucket_name = ?RAND_STR(15),
        block_size = 0
    });
create_storage(s3, ProviderSelector, false) ->
    space_setup_utils:create_storage(ProviderSelector, #s3_storage_params{
        storage_path_type = <<"flat">>,
        hostname = build_s3_hostname(ProviderSelector),
        bucket_name = ?RAND_STR(15)
    }).


%% @private
-spec build_s3_hostname(oct_background:entity_selector()) -> binary().
build_s3_hostname(ProviderSelector) ->
    <<
        "volume-s3.dev-volume-s3-",
        (atom_to_binary(oct_background:to_entity_placeholder(ProviderSelector)))/binary,
        ".default:9000"
    >>.


%% @private
-spec build_provider_ctx(oct_background:entity_selector(), oct_background:entity_selector()) ->
    provider_ctx().
build_provider_ctx(SpaceOwnerSelector, ProviderSelector) ->
    #provider_ctx{
        random_node = oct_background:get_random_provider_node(ProviderSelector),
        session_id = oct_background:get_user_session_id(SpaceOwnerSelector, ProviderSelector)
    }.


%% @private
-spec create_file_tree_on_storage(atom(), storage:id(), onenv_file_test_utils:object_spec()) ->
    storage:id().
create_file_tree_on_storage(TestCase, StorageId, FileDesc) ->
    Helper = storage:get_helper(StorageId),
    HelperHandle = helpers:get_helper_handle(Helper, Helper#helper.admin_ctx),
    create_file_tree_on_storage(TestCase, HelperHandle, <<"/">>, FileDesc).


%% @private
-spec create_file_tree_on_storage(
    atom(),
    helpers:helper_handle(),
    file_meta:path(),
    onenv_file_test_utils:object_spec()
) ->
    storage:id().
create_file_tree_on_storage(import_directory_check_user_id_test, HelperHandle, ParentPath, #dir_spec{
    name = NameOrUndefined,
    mode = DirMode
}) ->
    DirName = utils:ensure_defined(NameOrUndefined, str_utils:rand_hex(20)),
    StorageDirId = filepath_utils:join([ParentPath, DirName]),
    ok = helpers:mkdir(HelperHandle, StorageDirId, DirMode),
    ok = helpers:chown(HelperHandle, StorageDirId, ?TEST_UID, ?TEST_GID);
create_file_tree_on_storage(_TestCase, HelperHandle, ParentPath, #dir_spec{
    name = NameOrUndefined,
    mode = DirMode
}) ->
    DirName = utils:ensure_defined(NameOrUndefined, str_utils:rand_hex(20)),
    StorageDirId = filepath_utils:join([ParentPath, DirName]),
    ok = helpers:mkdir(HelperHandle, StorageDirId, DirMode);
create_file_tree_on_storage(_TestCase, HelperHandle, ParentPath, #file_spec{
    name = NameOrUndefined,
    mode = FileMode,
    content =  FileContent
}) ->
    FileName = utils:ensure_defined(NameOrUndefined, str_utils:rand_hex(20)),
    StorageFileId = filepath_utils:join([ParentPath, FileName]),
    ok = helpers:mknod(HelperHandle, FileName, FileMode, reg),
    {ok, FileHandle} = helpers:open(HelperHandle, StorageFileId, write),
    {ok, _} = helpers:write(FileHandle, 0, FileContent),
    ok = helpers:release(FileHandle).


%% @private
-spec await_initial_scan_finished(oct_background:node_selector(), od_space:id()) -> true.
await_initial_scan_finished(NodeSelector, SpaceId) ->
    ?assertEqual(
        true,
        catch(?rpc(NodeSelector, storage_import_monitoring:is_initial_scan_finished(SpaceId))),
        ?ATTEMPTS
    ).


%% @private
-spec assert_storage_import_monitoring_state(oct_background:node_selector(), od_space:id(), map()) ->
    ok.
assert_storage_import_monitoring_state(Node, SpaceId, ExpectedSIM) ->
    assert_storage_import_monitoring_state(Node, SpaceId, ExpectedSIM, 1).


%% @private
-spec assert_storage_import_monitoring_state(
    oct_background:node_selector(),
    od_space:id(),
    map(),
    non_neg_integer()
) ->
    ok.
assert_storage_import_monitoring_state(Node, SpaceId, ExpectedSIM, Attempts) ->
    SIM = ?rpc(Node, storage_import_monitoring:describe(SpaceId)),

    try
        assert_storage_import_monitoring_state(ExpectedSIM, flatten_storage_import_histograms(SIM))
    catch
        throw:{assertion_error, _} when Attempts > 0 ->
            timer:sleep(timer:seconds(1)),
            assert_storage_import_monitoring_state(Node, SpaceId, ExpectedSIM, Attempts - 1);

        throw:{assertion_error, {Key, ExpectedValue, Value}}:Stacktrace ->
            {Format, Args} = build_storage_import_monitoring_description(SIM),
            ct:pal(
                "Assertion of field \"~tp\" in storage_import_monitoring for space ~tp failed.~n"
                "    Expected: ~tp~n"
                "    Value: ~tp~n"
                ++ Format ++
                    "~nStacktrace:~n~tp",
                [Key, SpaceId, ExpectedValue, Value] ++ Args ++ [Stacktrace]),
            ct:fail("assertion failed")
    end.


%% @private
-spec mock_import_file_error(oct_background:node_selector(), string()) -> ok.
mock_import_file_error(ProviderSelector, ErroneousFile) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, storage_import_engine),
    ok = test_utils:mock_expect(Nodes, storage_import_engine, import_file_unsafe,
        fun(StorageFileCtx, Info) ->
            FileName = storage_file_ctx:get_file_name_const(StorageFileCtx),
            case FileName of
                ErroneousFile -> throw(test_error);
                _ ->
                    meck:passthrough([StorageFileCtx, Info])
            end
        end
    ).


%% @private
assert_storage_import_monitoring_state(ExpectedSIM, SIM) ->
    maps:foreach(fun(Key, ExpectedValue) ->
        case maps:get(Key, SIM) of
            ExpectedValue -> ok;
            Value -> throw({assertion_error, {Key, ExpectedValue, Value}})
        end
    end, ExpectedSIM).


%% @private
flatten_storage_import_histograms(SIM) ->
    SIM#{
        % flatten beginnings of histograms for assertions
        <<"createdMinHist">> => lists:sum(lists:sublist(maps:get(<<"createdMinHist">>, SIM), 2)),
        <<"modifiedMinHist">> => lists:sum(lists:sublist(maps:get(<<"modifiedMinHist">>, SIM), 2)),
        <<"deletedMinHist">> => lists:sum(lists:sublist(maps:get(<<"deletedMinHist">>, SIM), 2)),
        <<"queueLengthMinHist">> => hd(maps:get(<<"queueLengthMinHist">>, SIM)),

        <<"createdHourHist">> => lists:sum(lists:sublist(maps:get(<<"createdHourHist">>, SIM), 3)),
        <<"modifiedHourHist">> => lists:sum(lists:sublist(maps:get(<<"modifiedHourHist">>, SIM), 3)),
        <<"deletedHourHist">> => lists:sum(lists:sublist(maps:get(<<"deletedHourHist">>, SIM), 3)),
        <<"queueLengthHourHist">> => hd(maps:get(<<"queueLengthHourHist">>, SIM)),

        <<"createdDayHist">> => lists:sum(lists:sublist(maps:get(<<"createdDayHist">>, SIM), 1)),
        <<"modifiedDayHist">> => lists:sum(lists:sublist(maps:get(<<"modifiedDayHist">>, SIM), 1)),
        <<"deletedDayHist">> => lists:sum(lists:sublist(maps:get(<<"deletedDayHist">>, SIM), 1)),
        <<"queueLengthDayHist">> => hd(maps:get(<<"queueLengthDayHist">>, SIM))
    }.


%% @private
build_storage_import_monitoring_description(SIM) ->
    maps:fold(fun(Key, Value, {AccFormat, AccArgs}) ->
        {AccFormat ++ "    ~tp = ~tp~n", AccArgs ++ [Key, Value]}
    end, {"~n#storage_import_monitoring fields values:~n", []}, SIM).
