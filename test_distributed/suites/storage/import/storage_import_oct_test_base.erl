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

% API
-export([
    clean_up_after_previous_run/2
]).

%% tests
-export([
    empty_import_test/1,
    create_directory_import_test/1
]).

-type storage_import_test_suite_ctx() :: #storage_import_test_suite_ctx{}.

-record(storage_import_test_case_ctx, {
    suite_ctx :: storage_import_test_suite_ctx(),
    imported_storage_id :: storage:id(),
    other_storage_id :: storage:id(),
    space_id :: od_space:id(),
    space_path :: file_meta:path()
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


empty_import_test(TestSuiteCtx = #storage_import_test_suite_ctx{
    importing_provider_selector = ImportingProviderSelector,
    non_importing_provider_selector = NonImportingProviderSelector,
    space_owner_selector = SpaceOwnerSelector
}) ->
    TestCaseCtx = init_testcase(?FUNCTION_NAME, undefined, TestSuiteCtx),
    SpaceId = TestCaseCtx#storage_import_test_case_ctx.space_id,
    await_initial_scan_finished(ImportingProviderSelector, SpaceId),

    % TODO VFS-11902 implement generic verification of imported files structure/data
    NodeImportingProvider = oct_background:get_random_provider_node(ImportingProviderSelector),
    NodeNonImportingProvider = oct_background:get_random_provider_node(NonImportingProviderSelector),
    SessionIdImportingProvider = oct_background:get_user_session_id(
        SpaceOwnerSelector, ImportingProviderSelector
    ),
    SessionIdNonImportingProvider = oct_background:get_user_session_id(
        SpaceOwnerSelector, NonImportingProviderSelector
    ),

    SpacePathFileKey = {path, TestCaseCtx#storage_import_test_case_ctx.space_path},
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


create_directory_import_test(TestSuiteCtx = #storage_import_test_suite_ctx{
    importing_provider_selector = ImportingProviderSelector,
    non_importing_provider_selector = NonImportingProviderSelector,
    space_owner_selector = SpaceOwnerSelector
}) ->
    DirName = ?RAND_STR(),

    TestCaseCtx = init_testcase(?FUNCTION_NAME, #dir_spec{name = DirName}, TestSuiteCtx),
    SpaceId = TestCaseCtx#storage_import_test_case_ctx.space_id,
    await_initial_scan_finished(ImportingProviderSelector, SpaceId),

    % TODO VFS-11902 implement generic verification of imported files structure/data
    NodeImportingProvider = oct_background:get_random_provider_node(ImportingProviderSelector),
    NodeNonImportingProvider = oct_background:get_random_provider_node(NonImportingProviderSelector),
    SessionIdImportingProvider = oct_background:get_user_session_id(
        SpaceOwnerSelector, ImportingProviderSelector
    ),
    SessionIdNonImportingProvider = oct_background:get_user_session_id(
        SpaceOwnerSelector, NonImportingProviderSelector
    ),

    %% Check if dir was imported
    SpacePathFileKey = {path, TestCaseCtx#storage_import_test_case_ctx.space_path},
    ?assertMatch(
        {ok, [{_, DirName}]},
        lfm_proxy:get_children(NodeImportingProvider, SessionIdImportingProvider, SpacePathFileKey, 0, 10),
        ?ATTEMPTS
    ),

    SpaceTestDirPath = filename:join([<<"/">>, ?FUNCTION_NAME, DirName]),
    StorageSDHandleImportingProvider = sd_test_utils:get_storage_mountpoint_handle(
        NodeImportingProvider,
        SpaceId,
        TestCaseCtx#storage_import_test_case_ctx.imported_storage_id
    ),
    StorageSDHandleNonImportingProvider = sd_test_utils:get_storage_mountpoint_handle(
        NodeImportingProvider,
        SpaceId,
        TestCaseCtx#storage_import_test_case_ctx.other_storage_id
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

    ozw_test_rpc:delete_space(SpaceId),

    delete_storage(ImportingProviderSelector, StorageImportingProvider),
    delete_storage(NonImportingProviderSelector, StorageNonImportingProvider).


%% @private
-spec delete_storage(oct_background:node_selector(), storage:id()) -> ok.
delete_storage(NodeSelector, StorageId) ->
    ?assertEqual(ok, opw_test_rpc:call(NodeSelector, storage, delete, [StorageId]), ?ATTEMPTS).


%% @private
-spec init_testcase(
    atom(),
    undefined | onenv_file_test_utils:object_spec(),
    storage_import_test_suite_ctx()
) ->
    storage_import_test_case_ctx().
init_testcase(TestCaseName, FileDesc, TestSuiteCtx = #storage_import_test_suite_ctx{
    storage_type = StorageType,
    importing_provider_selector = ImportingProviderSelector,
    non_importing_provider_selector = NonImportingProviderSelector,
    space_owner_selector = SpaceOwnerSelector
}) ->
    ImportedStorageId = create_storage(StorageType, ImportingProviderSelector, true),
    FileDesc =/= undefined andalso ?rpc(ImportingProviderSelector, create_file_tree_on_storage(
        ImportedStorageId, FileDesc
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
        space_path = <<"/", SpaceNameBin/binary>>
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
        bucket_name = <<"test">>,
        block_size = 0
    });
create_storage(s3, ProviderSelector, false) ->
    space_setup_utils:create_storage(ProviderSelector, #s3_storage_params{
        storage_path_type = <<"flat">>,
        hostname = build_s3_hostname(ProviderSelector)
    }).


%% @private
build_s3_hostname(ProviderSelector) ->
    <<
        "dev-volume-s3-",
        (atom_to_binary(oct_background:to_entity_placeholder(ProviderSelector)))/binary,
        ".default:9000"
    >>.


%% @private
-spec create_file_tree_on_storage(storage:id(), onenv_file_test_utils:object_spec()) ->
    storage:id().
create_file_tree_on_storage(StorageId, FileDesc) ->
    Helper = storage:get_helper(StorageId),
    HelperHandle = helpers:get_helper_handle(Helper, Helper#helper.admin_ctx),

    create_file_tree_on_storage(HelperHandle, <<"/">>, FileDesc).


%% @private
-spec create_file_tree_on_storage(
    helpers:helper_handle(),
    file_meta:path(),
    onenv_file_test_utils:object_spec()
) ->
    storage:id().
create_file_tree_on_storage(HelperHandle, ParentPath, #dir_spec{
    name = NameOrUndefined,
    mode = DirMode
}) ->
    DirName = utils:ensure_defined(NameOrUndefined, str_utils:rand_hex(20)),
    StorageDirId = filepath_utils:join([ParentPath, DirName]),
    helpers:mkdir(HelperHandle, StorageDirId, DirMode).


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
