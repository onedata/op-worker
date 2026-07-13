%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Common base for file registration tests (POST data/register), shared by the
%%% thin SUITE modules that differ only in the storage backing the registering
%%% provider (e.g. imported S3 vs imported read-only HTTP).
%%%
%%% The space is supported by the registering provider's imported storage and by
%%% a regular POSIX storage on the other provider, so cross-provider propagation
%%% is also verified. A fresh space with dedicated storages is set up per testcase.
%%% @end
%%%--------------------------------------------------------------------
-module(file_registration_test_base).
-author("Bartosz Walkowicz").

-include("file_registration_test.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").
-include_lib("ctool/include/aai/aai.hrl").

%% API
-export([
    clean_up_after_previous_run/2,
    init_per_testcase/1,
    end_per_testcase/3
]).

%% tests
-export([
    register_file_test/1,
    register_file_with_conflict_test/1,
    register_file_and_create_parents_test/1,
    update_registered_file_test/1,
    update_registered_file_with_not_matching_destination_test/1,
    stat_on_storage_should_not_be_performed_if_automatic_detection_of_attributes_is_disabled/1,
    registration_should_fail_if_size_is_not_passed_and_automatic_detection_of_attributes_is_disabled/1,
    registration_should_fail_if_file_is_missing/1,
    registration_should_succeed_if_size_is_passed/1,
    interrupted_registration_test/1,
    interrupted_registration_nested_file_test/1,
    register_many_files_test/1,
    register_many_nested_files_test/1,
    register_file_with_size_smaller_than_real_test/1,
    register_file_with_size_larger_than_real_test/1,
    registration_should_succeed_if_file_is_missing_and_existence_verification_is_disabled/1,
    registration_should_fail_if_file_is_missing_and_existence_verification_is_enabled/1,
    registration_should_verify_existence_without_detecting_attributes/1,
    read_registered_file_after_source_removed_from_storage_test/1,
    read_registered_file_after_source_modified_on_storage_test/1,
    read_registered_file_when_storage_returns_error_test/1,
    large_registered_file_should_be_correctly_replicated_to_other_provider_test/1,
    register_shared_file_via_public_url_test/1
]).

-define(SUPPORT_SIZE, 1000000000).

%% Size of the file used to verify replication across the rtransfer block boundary.
%% It must exceed rtransfer_link's transfer block size (passed to the native link as
%% single_fetch_max_size, 12 MB by default - see the {rtransfer_link, [{transfer,
%% [{block_size, _}]}]} entry in app.config; note the {rtransfer_block_size, _} op_worker
%% env is NOT used), so that during replication the source provider reads the storage in
%% several sub-ranges (the later ones at a non-zero offset) rather than a single
%% whole-file read.
-define(LARGE_FILE_SIZE, 30 * 1024 * 1024).

-record(provider_ctx, {
    selector :: oct_background:entity_selector(),
    node :: oct_background:node(),
    session_id :: session:id()
}).

%% Describes where (and how) to place the source file that is later registered.
%% It is storage type specific - for an imported S3 storage the file is created
%% directly on storage with helpers, for an HTTP storage it is served by a test
%% HTTP server backing the storage.
-type source_backend() ::
    {s3, oct_background:entity_selector(), storage:id()} |
    {http, oct_background:entity_selector(), http_storage_test_server:handle()}.

-record(test_case_ctx, {
    suite_ctx :: #file_registration_test_suite_ctx{},
    space_id :: od_space:id(),
    space_path :: file_meta:path(),
    imported_storage_id :: storage:id(),
    source_backend :: source_backend(),
    registering_provider_ctx :: #provider_ctx{},
    other_provider_ctx :: #provider_ctx{}
}).


%%%==================================================================
%%% Test functions
%%%===================================================================

% TODO VFS-6509 test conflict with LFM

register_file_test(SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}) ->
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        source_backend = SourceBackend,
        registering_provider_ctx = #provider_ctx{node = RegNode, session_id = RegSessId},
        other_provider_ctx = #provider_ctx{node = OtherNode, session_id = OtherSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),

    FileName = ?FILE_NAME,
    FilePath = filepath_utils:join([SpacePath, FileName]),
    StorageFileId = filename:join(["/", FileName]),
    ok = place_source_file(SourceBackend, StorageFileId, ?TEST_DATA),

    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => FileName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"mtime">> => global_clock:timestamp_seconds(),
        <<"size">> => byte_size(?TEST_DATA),
        <<"mode">> => <<"664">>,
        <<"xattrs">> => ?XATTRS,
        <<"json">> => ?JSON1,
        <<"rdf">> => ?ENCODED_RDF1
    })),

    % check whether file has been properly registered
    ?assertFile(RegNode, RegSessId, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1),

    % check whether file is visible on the other provider
    ?assertFile(OtherNode, OtherSessId, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS).


register_file_with_conflict_test(SuiteCtx = #file_registration_test_suite_ctx{
    registering_storage_type = StorageType,
    test_user_selector = User
}) ->
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        source_backend = SourceBackend,
        registering_provider_ctx = #provider_ctx{selector = RegProvider, node = RegNode, session_id = RegSessId},
        other_provider_ctx = #provider_ctx{node = OtherNode, session_id = OtherSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),

    FileName = ?FILE_NAME,
    FilePath = filepath_utils:join([SpacePath, FileName]),
    StorageFileId = filename:join(["/", <<FileName/binary, "_imported">>]),
    ok = place_source_file(SourceBackend, StorageFileId, ?TEST_DATA),

    % Create a regular file with the same logical name to provoke an import
    % conflict. The registering storage may be readonly (HTTP), in which case the
    % conflicting file must be created on the other provider (writable POSIX); it
    % is then synced to the registering provider, which still resolves the import
    % conflict using its own provider id in the suffix.
    {CreationNode, CreationSessId} = case StorageType of
        http -> {OtherNode, OtherSessId};
        _ -> {RegNode, RegSessId}
    end,
    {ok, {ExistingFileGuid, Handle}} = lfm_proxy:create_and_open(CreationNode, CreationSessId, FilePath),
    lfm_proxy:write(CreationNode, Handle, 0, ?TEST_DATA2),
    lfm_proxy:close(CreationNode, Handle),
    % wait until the conflicting file is visible on the registering provider so
    % that the subsequent registration detects the name conflict
    ?assertInLs(RegNode, RegSessId, FilePath, ?ATTEMPTS),
    ?assertMatch({ok, _}, lfm_proxy:stat(RegNode, RegSessId, ?FILE_REF(ExistingFileGuid)), ?ATTEMPTS),

    {ok, _, _, EncodedBody} = ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => FileName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"mtime">> => global_clock:timestamp_seconds(),
        <<"size">> => byte_size(?TEST_DATA),
        <<"mode">> => <<"664">>,
        <<"xattrs">> => ?XATTRS,
        <<"json">> => ?JSON1,
        <<"rdf">> => ?ENCODED_RDF1
    })),

    % registration should create a new file with import conflict suffix
    RegisteredFileId = maps:get(<<"fileId">>, json_utils:decode(EncodedBody)),
    ?assertNotEqual(ExistingFileGuid, RegisteredFileId),

    ConflictingName = ?IMPORTED_CONFLICTING_FILE_NAME(FileName, oct_background:get_provider_id(RegProvider)),
    RegisteredPath = filepath_utils:join([SpacePath, ConflictingName]),

    % check whether file has been properly registered
    ?assertFile(RegNode, RegSessId, RegisteredPath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1),

    % check whether file is visible on the other provider
    ?assertFile(OtherNode, OtherSessId, RegisteredPath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS).


register_file_and_create_parents_test(SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}) ->
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        source_backend = SourceBackend,
        registering_provider_ctx = #provider_ctx{node = RegNode, session_id = RegSessId},
        other_provider_ctx = #provider_ctx{node = OtherNode, session_id = OtherSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),

    FileName = ?FILE_NAME,
    DestinationPath = filename:join(["/", ?DIR_NAME, ?DIR_NAME, ?DIR_NAME, FileName]),
    FilePath = filepath_utils:join([SpacePath, DestinationPath]),
    StorageFileId = FileName,
    ok = place_source_file(SourceBackend, StorageFileId, ?TEST_DATA),

    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => DestinationPath,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"mtime">> => global_clock:timestamp_seconds(),
        <<"size">> => byte_size(?TEST_DATA),
        <<"mode">> => <<"664">>,
        <<"xattrs">> => ?XATTRS,
        <<"json">> => ?JSON1,
        <<"rdf">> => ?ENCODED_RDF1
    })),

    % check whether file has been properly registered
    ?assertFile(RegNode, RegSessId, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1),

    % check whether file is visible on the other provider
    ?assertFile(OtherNode, OtherSessId, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS).


update_registered_file_test(SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}) ->
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        source_backend = SourceBackend,
        registering_provider_ctx = #provider_ctx{node = RegNode, session_id = RegSessId},
        other_provider_ctx = #provider_ctx{node = OtherNode, session_id = OtherSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),

    FileName = ?FILE_NAME,
    DestinationPath = FileName,
    StorageFileId = FileName,
    FilePath = filepath_utils:join([SpacePath, FileName]),
    ok = place_source_file(SourceBackend, StorageFileId, ?TEST_DATA),

    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => DestinationPath,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"mtime">> => global_clock:timestamp_seconds(),
        <<"size">> => byte_size(?TEST_DATA),
        <<"mode">> => <<"664">>,
        <<"xattrs">> => ?XATTRS,
        <<"json">> => ?JSON1,
        <<"rdf">> => ?ENCODED_RDF1
    })),

    % check whether file has been properly registered
    ?assertFile(RegNode, RegSessId, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1),

    % check whether file is visible on the other provider
    ?assertFile(OtherNode, OtherSessId, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS),

    ok = update_source_file(SourceBackend, StorageFileId, ?TEST_DATA2),

    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => DestinationPath,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"mtime">> => global_clock:timestamp_seconds(),
        <<"size">> => byte_size(?TEST_DATA2),
        <<"mode">> => <<"664">>
    })),

    % check whether file has been properly updated
    ?assertFile(RegNode, RegSessId, FilePath, ?TEST_DATA2, ?XATTRS, ?JSON1, ?RDF1),

    % check whether file was updated on the other provider
    ?assertFile(OtherNode, OtherSessId, FilePath, ?TEST_DATA2, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS),

    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => DestinationPath,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"xattrs">> => ?XATTRS2,
        <<"json">> => ?JSON2,
        <<"rdf">> => ?ENCODED_RDF2,
        <<"size">> => byte_size(?TEST_DATA2),
        <<"mode">> => <<"664">>
    })),

    XATTRS3 = maps:merge(?XATTRS, ?XATTRS2),

    % check whether file has been properly updated
    ?assertFile(RegNode, RegSessId, FilePath, ?TEST_DATA2, XATTRS3, ?JSON2, ?RDF2),

    % check whether file was updated on the other provider
    ?assertFile(OtherNode, OtherSessId, FilePath, ?TEST_DATA2, XATTRS3, ?JSON2, ?RDF2, ?ATTEMPTS).


update_registered_file_with_not_matching_destination_test(
    SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}
) ->
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        source_backend = SourceBackend,
        registering_provider_ctx = #provider_ctx{node = RegNode, session_id = RegSessId},
        other_provider_ctx = #provider_ctx{node = OtherNode, session_id = OtherSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),

    FileName = ?FILE_NAME,
    StorageFileId = filename:join(["/", FileName]),
    ok = place_source_file(SourceBackend, StorageFileId, ?TEST_DATA),

    RegisteredFileName = str_utils:join_binary([FileName, <<"_registered">>]),
    FilePath = filepath_utils:join([SpacePath, RegisteredFileName]),

    RegisterAndCheckFun = fun(Data) ->
        ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, #{
            <<"spaceId">> => SpaceId,
            <<"destinationPath">> => RegisteredFileName,
            <<"storageFileId">> => StorageFileId,
            <<"storageId">> => StorageId,
            <<"mtime">> => global_clock:timestamp_seconds(),
            <<"size">> => byte_size(Data),
            <<"mode">> => <<"664">>,
            <<"xattrs">> => ?XATTRS,
            <<"json">> => ?JSON1,
            <<"rdf">> => ?ENCODED_RDF1
        })),

        % check whether file has been properly registered
        ?assertFile(RegNode, RegSessId, FilePath, Data, ?XATTRS, ?JSON1, ?RDF1),

        % check whether file is visible on the other provider
        ?assertFile(OtherNode, OtherSessId, FilePath, Data, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS)
    end,

    RegisterAndCheckFun(?TEST_DATA),
    ok = update_source_file(SourceBackend, StorageFileId, ?TEST_DATA2),
    RegisterAndCheckFun(?TEST_DATA2).


stat_on_storage_should_not_be_performed_if_automatic_detection_of_attributes_is_disabled(
    SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}
) ->
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        source_backend = SourceBackend,
        registering_provider_ctx = #provider_ctx{node = RegNode, session_id = RegSessId},
        other_provider_ctx = #provider_ctx{node = OtherNode, session_id = OtherSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),

    FileName = ?FILE_NAME,
    FilePath = filepath_utils:join([SpacePath, FileName]),
    StorageFileId = filename:join(["/", FileName]),
    ok = place_source_file(SourceBackend, StorageFileId, ?TEST_DATA),
    Timestamp = global_clock:timestamp_seconds(),

    ok = test_utils:mock_new(RegNode, [storage_driver], [passthrough]),
    % verifyExistence is disabled alongside autoDetectAttributes: the existence
    % check is implemented as storage_driver:exists, which performs a stat under the
    % hood, so leaving it on would make the assertion below (no stat) fail
    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => FileName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"mtime">> => Timestamp,
        <<"atime">> => Timestamp,
        <<"ctime">> => Timestamp,
        <<"uid">> => 0,
        <<"gid">> => 0,
        <<"size">> => byte_size(?TEST_DATA),
        <<"mode">> => <<"664">>,
        <<"xattrs">> => ?XATTRS,
        <<"json">> => ?JSON1,
        <<"rdf">> => ?ENCODED_RDF1,
        <<"autoDetectAttributes">> => false,
        <<"verifyExistence">> => false
    })),

    test_utils:mock_assert_num_calls(RegNode, storage_driver, stat, ['_'], 0),

    % check whether file has been properly registered
    ?assertFile(RegNode, RegSessId, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1),

    % check whether file is visible on the other provider
    ?assertFile(OtherNode, OtherSessId, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS).


registration_should_fail_if_size_is_not_passed_and_automatic_detection_of_attributes_is_disabled(
    SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}
) ->
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        source_backend = SourceBackend,
        registering_provider_ctx = #provider_ctx{node = RegNode, session_id = RegSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),

    FileName = ?FILE_NAME,
    FilePath = filepath_utils:join([SpacePath, FileName]),
    StorageFileId = filename:join(["/", FileName]),
    ok = place_source_file(SourceBackend, StorageFileId, ?TEST_DATA),

    ?assertMatch({ok, ?HTTP_400_BAD_REQUEST, _, _}, register_file(RegNode, User, #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => FileName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"autoDetectAttributes">> => false
    })),

    % file shouldn't have been registered
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(RegNode, RegSessId, {path, FilePath})).


registration_should_fail_if_file_is_missing(
    SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}
) ->
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        registering_provider_ctx = #provider_ctx{node = RegNode, session_id = RegSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),

    FileName = ?FILE_NAME,
    FilePath = filepath_utils:join([SpacePath, FileName]),
    StorageFileId = filename:join(["/", FileName]),

    ?assertMatch({ok, ?HTTP_400_BAD_REQUEST, _, _}, register_file(RegNode, User, #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => FileName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"size">> => 100
    })),

    % file shouldn't have been registered
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(RegNode, RegSessId, {path, FilePath})).


registration_should_succeed_if_size_is_passed(
    SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}
) ->
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        source_backend = SourceBackend,
        registering_provider_ctx = #provider_ctx{node = RegNode, session_id = RegSessId},
        other_provider_ctx = #provider_ctx{node = OtherNode, session_id = OtherSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),

    FileName = ?FILE_NAME,
    FilePath = filepath_utils:join([SpacePath, FileName]),
    StorageFileId = filename:join(["/", FileName]),
    ok = place_source_file(SourceBackend, StorageFileId, ?TEST_DATA),

    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => FileName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"size">> => byte_size(?TEST_DATA)
    })),

    % check whether file has been properly registered
    ?assertFile(RegNode, RegSessId, FilePath, ?TEST_DATA, #{}, #{}, <<>>),

    % check whether file is visible on the other provider
    ?assertFile(OtherNode, OtherSessId, FilePath, ?TEST_DATA, #{}, #{}, <<>>, ?ATTEMPTS).


interrupted_registration_test(SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}) ->
    % this test checks whether subsequent registration can succeed when previous one was interrupted by user
    % e. g. by pressing CTRL + C
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        source_backend = SourceBackend,
        registering_provider_ctx = #provider_ctx{selector = RegProvider, node = RegNode, session_id = RegSessId},
        other_provider_ctx = #provider_ctx{node = OtherNode, session_id = OtherSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),

    FileName = ?FILE_NAME,
    FilePath = filepath_utils:join([SpacePath, FileName]),
    StorageFileId = filename:join(["/", FileName]),
    ok = place_source_file(SourceBackend, StorageFileId, ?TEST_DATA),

    mock_file_meta_save(RegProvider, FileName),

    Body = #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => FileName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"mtime">> => global_clock:timestamp_seconds(),
        <<"size">> => byte_size(?TEST_DATA),
        <<"mode">> => <<"664">>,
        <<"xattrs">> => ?XATTRS,
        <<"json">> => ?JSON1,
        <<"rdf">> => ?ENCODED_RDF1
    },
    Pid = spawn(fun() -> register_file(RegNode, User, Body) end),

    wait_until_saving_file_meta_is_frozen(),

    % kill process that requested the registration, the same things happen when user
    % aborts the REST request with CTRL + C
    exit(Pid, shutdown),

    % file shouldn't have been registered
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(RegNode, RegSessId, {path, FilePath})),

    unmock_file_meta_save(RegProvider),

    % retry registration
    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, Body)),

    % check whether file has been properly registered
    ?assertFile(RegNode, RegSessId, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1),

    % check whether file is visible on the other provider
    ?assertFile(OtherNode, OtherSessId, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS).


interrupted_registration_nested_file_test(
    SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}
) ->
    % this test checks whether subsequent registration can succeed when previous one was interrupted by user
    % e. g. by pressing CTRL + C
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        source_backend = SourceBackend,
        registering_provider_ctx = #provider_ctx{selector = RegProvider, node = RegNode, session_id = RegSessId},
        other_provider_ctx = #provider_ctx{node = OtherNode, session_id = OtherSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),

    DirName = ?DIR_NAME,
    FileName = ?FILE_NAME,
    DestinationPath = filename:join(["/", DirName, FileName]),
    FilePath = filepath_utils:join([SpacePath, DestinationPath]),
    StorageFileId = filename:join(["/", FileName]),
    ok = place_source_file(SourceBackend, StorageFileId, ?TEST_DATA),

    mock_file_meta_save(RegProvider, DirName),

    Body = #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => DestinationPath,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"mtime">> => global_clock:timestamp_seconds(),
        <<"size">> => byte_size(?TEST_DATA),
        <<"mode">> => <<"664">>,
        <<"xattrs">> => ?XATTRS,
        <<"json">> => ?JSON1,
        <<"rdf">> => ?ENCODED_RDF1
    },
    Pid = spawn(fun() ->
        ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, Body))
    end),

    wait_until_saving_file_meta_is_frozen(),

    % kill process that requested the registration, the same things happen when user
    % aborts the REST request with CTRL + C
    exit(Pid, shutdown),

    % parent dir and file shouldn't have been created
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(RegNode, RegSessId, {path, filepath_utils:join([SpacePath, DirName])})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(RegNode, RegSessId, {path, FilePath})),

    unmock_file_meta_save(RegProvider),

    % retry registration
    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, Body)),

    % check whether file has been properly registered
    ?assertFile(RegNode, RegSessId, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1),

    % check whether file is visible on the other provider
    ?assertFile(OtherNode, OtherSessId, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS).


register_many_files_test(SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}) ->
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        source_backend = SourceBackend,
        registering_provider_ctx = #provider_ctx{node = RegNode, session_id = RegSessId},
        other_provider_ctx = #provider_ctx{node = OtherNode, session_id = OtherSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),
    LogicalFilesCount = 40,

    BaseFileName = ?FILE_NAME,
    StorageFileId = filename:join(["/", BaseFileName]),
    % create only 1 file on storage
    ok = place_source_file(SourceBackend, StorageFileId, ?TEST_DATA),

    % but register it as many logical files
    DestinationPaths = lists:map(fun(I) ->
        str_utils:format_bin("/~ts_~tp", [BaseFileName, I])
    end, lists:seq(1, LogicalFilesCount)),

    % register (in parallel) and verify each logical file; a failed registration or
    % verification propagates the real assertion error immediately (pforeach re-raises)
    lists_utils:pforeach(fun(DestinationPath) ->
        LogicalFilePath = filepath_utils:join([SpacePath, DestinationPath]),
        ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, #{
            <<"spaceId">> => SpaceId,
            <<"destinationPath">> => DestinationPath,
            <<"storageFileId">> => StorageFileId,
            <<"storageId">> => StorageId,
            <<"mtime">> => global_clock:timestamp_seconds(),
            <<"size">> => byte_size(?TEST_DATA),
            <<"mode">> => <<"664">>,
            <<"xattrs">> => ?XATTRS,
            <<"json">> => ?JSON1,
            <<"rdf">> => ?ENCODED_RDF1
        })),
        % check whether file has been properly registered
        ?assertFile(RegNode, RegSessId, LogicalFilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1),
        % check whether file is visible on the other provider
        ?assertFile(OtherNode, OtherSessId, LogicalFilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS)
    end, DestinationPaths).


register_many_nested_files_test(SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}) ->
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        source_backend = SourceBackend,
        registering_provider_ctx = #provider_ctx{node = RegNode, session_id = RegSessId},
        other_provider_ctx = #provider_ctx{node = OtherNode, session_id = OtherSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),
    LogicalFilesCount = 40,

    BaseFileName = ?FILE_NAME,
    StorageFileId = filename:join(["/", BaseFileName]),
    % create only 1 file on storage
    ok = place_source_file(SourceBackend, StorageFileId, ?TEST_DATA),

    % but register it as many logical files in the same directory
    Dir1 = ?DIR_NAME,
    Dir2 = ?DIR_NAME,
    Dir3 = ?DIR_NAME,
    ParentPath = filename:join([Dir1, Dir2, Dir3]),
    DestinationPaths = lists:map(fun(I) ->
        FileName = str_utils:format_bin("~ts_~tp", [BaseFileName, I]),
        filename:join(["/", ParentPath, FileName])
    end, lists:seq(1, LogicalFilesCount)),

    % register (in parallel) and verify each logical file; a failed registration or
    % verification propagates the real assertion error immediately (pforeach re-raises)
    lists_utils:pforeach(fun(DestinationPath) ->
        LogicalFilePath = filepath_utils:join([SpacePath, DestinationPath]),
        ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, #{
            <<"spaceId">> => SpaceId,
            <<"destinationPath">> => DestinationPath,
            <<"storageFileId">> => StorageFileId,
            <<"storageId">> => StorageId,
            <<"mtime">> => global_clock:timestamp_seconds(),
            <<"size">> => byte_size(?TEST_DATA),
            <<"mode">> => <<"664">>,
            <<"xattrs">> => ?XATTRS,
            <<"json">> => ?JSON1,
            <<"rdf">> => ?ENCODED_RDF1
        })),
        % check whether file has been properly registered
        ?assertFile(RegNode, RegSessId, LogicalFilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1),
        % check whether file is visible on the other provider
        ?assertFile(OtherNode, OtherSessId, LogicalFilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS)
    end, DestinationPaths).


%%%===================================================================
%%% Edge case tests - declared size mismatch, storage drift and errors
%%%===================================================================
%%
%% NOTE: with autoDetectAttributes = false the registration trusts the caller and
%% does NOT consult the storage (no existence check, no size verification - see
%% file_registration:maybe_verify_existence/2 and fill_in_missing_stat_fields/3).
%% Any inconsistency between the declared metadata and the real storage content
%% therefore surfaces only later, at read time.


register_file_with_size_smaller_than_real_test(
    SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}
) ->
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        source_backend = SourceBackend,
        registering_provider_ctx = #provider_ctx{node = RegNode, session_id = RegSessId},
        other_provider_ctx = #provider_ctx{node = OtherNode, session_id = OtherSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),

    FileName = ?FILE_NAME,
    FilePath = filepath_utils:join([SpacePath, FileName]),
    StorageFileId = filename:join(["/", FileName]),
    DeclaredSize = byte_size(?TEST_DATA) - 3,
    ExpectedData = binary:part(?TEST_DATA, 0, DeclaredSize),
    ok = place_source_file(SourceBackend, StorageFileId, ?TEST_DATA),

    % a declared size smaller than the real file is accepted as is
    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => FileName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"size">> => DeclaredSize,
        <<"autoDetectAttributes">> => false
    })),

    % the logical size is the declared one and a read returns only that prefix
    ?assertMatch({ok, #file_attr{size = DeclaredSize}},
        lfm_proxy:stat(RegNode, RegSessId, {path, FilePath})),
    ?assertRead(RegNode, RegSessId, FilePath, 0, ExpectedData, ?ATTEMPTS),

    % the same (truncated) view is propagated to the other provider
    ?assertMatch({ok, #file_attr{size = DeclaredSize}},
        lfm_proxy:stat(OtherNode, OtherSessId, {path, FilePath}), ?ATTEMPTS),
    ?assertRead(OtherNode, OtherSessId, FilePath, 0, ExpectedData, ?ATTEMPTS).


register_file_with_size_larger_than_real_test(
    SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}
) ->
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        source_backend = SourceBackend,
        registering_provider_ctx = #provider_ctx{node = RegNode, session_id = RegSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),

    FileName = ?FILE_NAME,
    FilePath = filepath_utils:join([SpacePath, FileName]),
    StorageFileId = filename:join(["/", FileName]),
    DeclaredSize = byte_size(?TEST_DATA) + 100,
    ok = place_source_file(SourceBackend, StorageFileId, ?TEST_DATA),

    % a declared size larger than the real file is accepted without verification
    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => FileName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"size">> => DeclaredSize,
        <<"autoDetectAttributes">> => false
    })),

    % the (inflated) declared size is what gets recorded ...
    ?assertMatch({ok, #file_attr{size = DeclaredSize}},
        lfm_proxy:stat(RegNode, RegSessId, {path, FilePath})),
    % ... while a read returns only the bytes actually present on the storage: even
    % though DeclaredSize bytes are requested, op-worker performs a short read of the
    % real content instead of zero-padding up to the (inflated) logical size.
    {ok, Handle} = ?assertMatch({ok, _},
        lfm_proxy:open(RegNode, RegSessId, {path, FilePath}, read), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(RegNode, Handle, 0, DeclaredSize)),
    ?assertEqual(ok, lfm_proxy:close(RegNode, Handle)).


registration_should_succeed_if_file_is_missing_and_existence_verification_is_disabled(
    SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}
) ->
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        registering_provider_ctx = #provider_ctx{node = RegNode, session_id = RegSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),

    FileName = ?FILE_NAME,
    FilePath = filepath_utils:join([SpacePath, FileName]),
    StorageFileId = filename:join(["/", FileName]),
    DeclaredSize = 100,
    % NOTE: the source file is deliberately NOT placed on the storage

    % with both attribute detection and existence verification disabled the storage
    % is not consulted at all, so the registration of a missing file succeeds
    % (contrast with registration_should_fail_if_file_is_missing_and_existence_verification_is_enabled)
    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => FileName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"size">> => DeclaredSize,
        <<"autoDetectAttributes">> => false,
        <<"verifyExistence">> => false
    })),

    % the file is registered (with the declared size) ...
    ?assertMatch({ok, #file_attr{size = DeclaredSize}},
        lfm_proxy:stat(RegNode, RegSessId, {path, FilePath})),
    % ... but its data is not actually available: the file is missing on the storage,
    % which maps to ENOENT (HTTP 404 / S3 NoSuchKey), so a read fails.
    {ok, Handle} = ?assertMatch({ok, _},
        lfm_proxy:open(RegNode, RegSessId, {path, FilePath}, read)),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:read(RegNode, Handle, 0, DeclaredSize), ?ATTEMPTS),
    ?assertEqual(ok, lfm_proxy:close(RegNode, Handle)).


registration_should_fail_if_file_is_missing_and_existence_verification_is_enabled(
    SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}
) ->
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        registering_provider_ctx = #provider_ctx{node = RegNode, session_id = RegSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),

    FileName = ?FILE_NAME,
    FilePath = filepath_utils:join([SpacePath, FileName]),
    StorageFileId = filename:join(["/", FileName]),
    % NOTE: the source file is deliberately NOT placed on the storage

    % existence verification is independent of attribute detection and defaults to
    % true, so even with autoDetectAttributes disabled (caller-provided attributes)
    % registering a file that is missing on the storage fails with ENOENT
    ?assertMatch({ok, ?HTTP_400_BAD_REQUEST, _, _}, register_file(RegNode, User, #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => FileName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"size">> => 100,
        <<"autoDetectAttributes">> => false
    })),

    % the file should not have been registered
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(RegNode, RegSessId, {path, FilePath})).


registration_should_verify_existence_without_detecting_attributes(
    SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}
) ->
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        source_backend = SourceBackend,
        registering_provider_ctx = #provider_ctx{node = RegNode, session_id = RegSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),

    FileName = ?FILE_NAME,
    FilePath = filepath_utils:join([SpacePath, FileName]),
    StorageFileId = filename:join(["/", FileName]),
    DeclaredSize = byte_size(?TEST_DATA) - 3,
    ok = place_source_file(SourceBackend, StorageFileId, ?TEST_DATA),

    % with autoDetectAttributes disabled but verifyExistence enabled the file's
    % presence is confirmed while its attributes are taken from the caller: the
    % declared (smaller) size is recorded rather than the real size read from storage
    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => FileName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"size">> => DeclaredSize,
        <<"autoDetectAttributes">> => false,
        <<"verifyExistence">> => true
    })),

    ?assertMatch({ok, #file_attr{size = DeclaredSize}},
        lfm_proxy:stat(RegNode, RegSessId, {path, FilePath})).


read_registered_file_after_source_removed_from_storage_test(
    SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}
) ->
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        source_backend = SourceBackend,
        registering_provider_ctx = #provider_ctx{node = RegNode, session_id = RegSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),

    FileName = ?FILE_NAME,
    FilePath = filepath_utils:join([SpacePath, FileName]),
    StorageFileId = filename:join(["/", FileName]),
    DataSize = byte_size(?TEST_DATA),
    ok = place_source_file(SourceBackend, StorageFileId, ?TEST_DATA),

    % register normally (the file exists, attributes auto-detected)
    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => FileName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"size">> => DataSize
    })),
    ?assertMatch({ok, #file_attr{size = DataSize}},
        lfm_proxy:stat(RegNode, RegSessId, {path, FilePath})),

    % remove the file from the underlying storage; the data is intentionally NOT read
    % beforehand so that no local replica masks the removal. In MANUAL import mode
    % there is no scan, so the logical metadata is not reconciled.
    ok = remove_source_file(SourceBackend, StorageFileId, DataSize),

    % stat still succeeds (metadata persists) ...
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(RegNode, RegSessId, {path, FilePath})),
    % ... but reading now fails with ENOENT because the data is gone from the storage.
    {ok, Handle} = ?assertMatch({ok, _},
        lfm_proxy:open(RegNode, RegSessId, {path, FilePath}, read)),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:read(RegNode, Handle, 0, DataSize), ?ATTEMPTS),
    ?assertEqual(ok, lfm_proxy:close(RegNode, Handle)).


read_registered_file_after_source_modified_on_storage_test(
    SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}
) ->
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        source_backend = SourceBackend,
        registering_provider_ctx = #provider_ctx{node = RegNode, session_id = RegSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),

    FileName = ?FILE_NAME,
    FilePath = filepath_utils:join([SpacePath, FileName]),
    StorageFileId = filename:join(["/", FileName]),
    OriginalSize = byte_size(?TEST_DATA),
    ShrunkData = binary:part(?TEST_DATA, 0, 2),
    ok = place_source_file(SourceBackend, StorageFileId, ?TEST_DATA),

    % register normally - size is auto-detected as the original size
    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => FileName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"size">> => OriginalSize
    })),
    ?assertMatch({ok, #file_attr{size = OriginalSize}},
        lfm_proxy:stat(RegNode, RegSessId, {path, FilePath})),

    % shrink the source on storage WITHOUT re-registering (no read beforehand, so no
    % local replica). In MANUAL import mode the logical size is not reconciled.
    ok = replace_source_file(SourceBackend, StorageFileId, OriginalSize, ShrunkData),

    % the logical size still reflects the original registration ...
    ?assertMatch({ok, #file_attr{size = OriginalSize}},
        lfm_proxy:stat(RegNode, RegSessId, {path, FilePath})),
    % ... while a read up to the stale size returns only the (now smaller) actual
    % storage content as a short read, instead of padding up to the original size.
    {ok, Handle} = ?assertMatch({ok, _},
        lfm_proxy:open(RegNode, RegSessId, {path, FilePath}, read), ?ATTEMPTS),
    ?assertEqual({ok, ShrunkData}, lfm_proxy:read(RegNode, Handle, 0, OriginalSize)),
    ?assertEqual(ok, lfm_proxy:close(RegNode, Handle)).


read_registered_file_when_storage_returns_error_test(
    SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}
) ->
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        source_backend = SourceBackend,
        registering_provider_ctx = #provider_ctx{node = RegNode, session_id = RegSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),

    FileName = ?FILE_NAME,
    FilePath = filepath_utils:join([SpacePath, FileName]),
    StorageFileId = filename:join(["/", FileName]),
    ok = place_source_file(SourceBackend, StorageFileId, ?TEST_DATA),

    % register normally
    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => FileName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"size">> => byte_size(?TEST_DATA)
    })),

    % make the storage return an error for the file's GET (without reading it first,
    % so no local replica masks the failure). 403 is mapped to EPERM by the HTTP
    % helper and - unlike e.g. 500/EIO - is not retried, so the failure is prompt.
    ok = set_source_file_error(SourceBackend, StorageFileId, ?HTTP_403_FORBIDDEN),

    % reading fails with EPERM (the error the HTTP 403 is mapped to).
    {ok, Handle} = ?assertMatch({ok, _},
        lfm_proxy:open(RegNode, RegSessId, {path, FilePath}, read)),
    ?assertEqual({error, ?EPERM}, lfm_proxy:read(RegNode, Handle, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    ?assertEqual(ok, lfm_proxy:close(RegNode, Handle)).


large_registered_file_should_be_correctly_replicated_to_other_provider_test(
    SuiteCtx = #file_registration_test_suite_ctx{test_user_selector = User}
) ->
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = StorageId,
        source_backend = SourceBackend,
        registering_provider_ctx = #provider_ctx{node = RegNode, session_id = RegSessId},
        other_provider_ctx = #provider_ctx{
            selector = OtherProvider, node = OtherNode, session_id = OtherSessId
        }
    } = init_testcase(?FUNCTION_NAME, SuiteCtx),

    FileName = ?FILE_NAME,
    FilePath = filepath_utils:join([SpacePath, FileName]),
    StorageFileId = filename:join(["/", FileName]),
    Size = ?LARGE_FILE_SIZE,
    Content = crypto:strong_rand_bytes(Size),
    ExpectedHash = crypto:hash(sha256, Content),
    ok = place_source_file(SourceBackend, StorageFileId, Content),

    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => FileName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"size">> => Size
    })),
    ?assertMatch({ok, #file_attr{size = Size}},
        lfm_proxy:stat(RegNode, RegSessId, {path, FilePath})),

    % wait until the file metadata is synced to the other provider; a stat does not pull
    % the data, so no on-the-fly replication masks the explicit transfer scheduled below
    ?assertMatch({ok, #file_attr{size = Size}},
        lfm_proxy:stat(OtherNode, OtherSessId, {path, FilePath}), ?ATTEMPTS),

    % explicitly replicate the whole file to the other provider and wait for completion;
    % as the file is larger than the rtransfer block size, the source reads it from the
    % storage in several sub-ranges (see ?LARGE_FILE_SIZE)
    OtherProviderId = oct_background:get_provider_id(OtherProvider),
    {ok, TransferId} = ?assertMatch({ok, _}, opt_transfers:schedule_file_replication(
        RegNode, RegSessId, ?resolveFileRef(RegNode, RegSessId, FilePath), OtherProviderId
    )),
    ?assertMatch({ok, #document{value = #transfer{
        replication_status = completed,
        files_replicated = 1,
        bytes_replicated = Size
    }}}, rpc:call(RegNode, transfer, get, [TransferId]), 5 * ?ATTEMPTS),

    % the replica on the other provider (now served from its own local POSIX storage)
    % must be byte-for-byte identical to the source - any sub-range read misassembled
    % during replication (e.g. a non-range HTTP server returning the whole file for a
    % request at a non-zero offset) would change the content hash
    ?assertEqual(ExpectedHash, begin
        {ok, Handle} = lfm_proxy:open(OtherNode, OtherSessId, {path, FilePath}, read),
        try
            crypto:hash(sha256, read_full_content(OtherNode, Handle, Size))
        after
            lfm_proxy:close(OtherNode, Handle)
        end
    end, ?ATTEMPTS).


register_shared_file_via_public_url_test(SuiteCtx = #file_registration_test_suite_ctx{
    test_user_selector = User
}) ->
    % A single space (named after the test case, so it is picked up by
    % clean_up_after_previous_run just like every other case here) is supported by the
    % registering provider's imported read-only HTTP storage and by the other provider's
    % writable POSIX storage. The HTTP storage endpoint is set to the registering
    % provider's REST API root - individual files are registered by their full public
    % share URL though (see below), so the endpoint host is largely irrelevant.
    #test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        imported_storage_id = HttpStorageId,
        registering_provider_ctx = #provider_ctx{node = RegNode, session_id = RegSessId},
        other_provider_ctx = #provider_ctx{node = OtherNode, session_id = OtherSessId}
    } = init_testcase(?FUNCTION_NAME, SuiteCtx#file_registration_test_suite_ctx{
        registering_storage_type = http
    }),

    % Create a file with content on the writable (other provider's POSIX) storage and
    % share it, so its content becomes publicly downloadable.
    SourceFilePath = filepath_utils:join([SpacePath, ?FILE_NAME]),
    {ok, {SourceGuid, Handle}} = lfm_proxy:create_and_open(OtherNode, OtherSessId, SourceFilePath),
    {ok, _} = lfm_proxy:write(OtherNode, Handle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(OtherNode, Handle),
    {ok, ShareId} = ?assertMatch({ok, _},
        opt_shares:create(OtherNode, OtherSessId, ?FILE_REF(SourceGuid), <<"share">>)),
    {ok, ShareObjectId} = file_id:guid_to_objectid(file_id:guid_to_share_guid(SourceGuid, ShareId)),

    % The shared file's public content URL, accessed directly on one of the supporting
    % providers (chosen at random). Putting the full URL in the storage file id makes the
    % HTTP helper target it directly, ignoring the storage endpoint.
    %
    % The Onezone public-share redirector root is intentionally NOT exercised yet:
    % registration relies on the helper's getattr (HTTP HEAD) which - unlike its read -
    % did not re-point the request path to the redirect target, so following the Onezone
    % 302 queried the wrong path. This is fixed in the helpers repo (HTTPHelper::getattr);
    % once that fix is pulled in as a dependency, switch to the commented line below to
    % also cover the Onezone redirector root.
    RestApiRoot = onenv_api_test_runner:get_rest_api_root(
        lists_utils:random_element([RegNode, OtherNode])),
%%    RestApiRoot = onenv_api_test_runner:random_share_rest_api_root([RegNode, OtherNode]),
    PublicUrl = <<RestApiRoot/binary, "data/", ShareObjectId/binary, "/content">>,

    % Register the shared file (size intentionally omitted, so it must be detected via
    % HTTP HEAD) and verify it is readable through the public URL. Retries cover the
    % short delay before the freshly created share becomes publicly resolvable.
    RegFileName = ?FILE_NAME,
    RegFilePath = filepath_utils:join([SpacePath, RegFileName]),
    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(RegNode, User, #{
        <<"spaceId">> => SpaceId,
        <<"destinationPath">> => RegFileName,
        <<"storageFileId">> => PublicUrl,
        <<"storageId">> => HttpStorageId
    }), ?ATTEMPTS),

    ExpectedSize = byte_size(?TEST_DATA),
    ?assertMatch({ok, #file_attr{size = ExpectedSize}},
        lfm_proxy:stat(RegNode, RegSessId, {path, RegFilePath}), ?ATTEMPTS),
    ?assertRead(RegNode, RegSessId, RegFilePath, 0, ?TEST_DATA, ?ATTEMPTS).


%%%===================================================================
%%% SetUp and TearDown helpers (called by thin SUITE modules)
%%%===================================================================


-spec init_per_testcase(test_config:config()) -> test_config:config().
init_per_testcase(Config) ->
    ct:timetrap({minutes, 5}),
    lfm_proxy:init(Config).


-spec end_per_testcase(atom(), #file_registration_test_suite_ctx{}, test_config:config()) ->
    test_config:config().
end_per_testcase(_Case, SuiteCtx = #file_registration_test_suite_ctx{
    registering_provider_selector = RegProvider
}, Config) ->
    Nodes = oct_background:get_provider_nodes(RegProvider),
    test_utils:mock_unload(Nodes, storage_driver),
    test_utils:mock_unload(Nodes, file_meta),
    maybe_stop_http_servers(SuiteCtx),
    lfm_proxy:teardown(Config).


%% @private
-spec maybe_stop_http_servers(#file_registration_test_suite_ctx{}) -> ok.
maybe_stop_http_servers(#file_registration_test_suite_ctx{
    registering_storage_type = http,
    registering_provider_selector = RegProvider
}) ->
    http_storage_test_server:stop_all(RegProvider);
maybe_stop_http_servers(_SuiteCtx) ->
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec init_testcase(atom(), #file_registration_test_suite_ctx{}) -> #test_case_ctx{}.
init_testcase(TestCaseName, SuiteCtx = #file_registration_test_suite_ctx{
    registering_storage_type = StorageType,
    registering_provider_selector = RegProvider,
    other_provider_selector = OtherProvider,
    test_user_selector = User
}) ->
    {RegStorageId, SourceBackend} = create_registering_storage(StorageType, RegProvider),
    OtherStorageId = create_posix_storage(OtherProvider),

    SpaceName = str_utils:to_binary(TestCaseName),
    SpaceId = space_setup_utils:set_up_space(#space_spec{
        name = SpaceName,
        owner = User,
        users = [],
        supports = [
            #support_spec{
                provider = RegProvider,
                storage_spec = RegStorageId,
                size = ?SUPPORT_SIZE,
                % support directly in manual import mode - required for HTTP storage,
                % which rejects switching the import mode after support
                storage_import = #{mode => <<"manual">>}
            },
            #support_spec{provider = OtherProvider, storage_spec = OtherStorageId, size = ?SUPPORT_SIZE}
        ]
    }),

    #test_case_ctx{
        suite_ctx = SuiteCtx,
        space_id = SpaceId,
        space_path = <<"/", SpaceName/binary>>,
        imported_storage_id = RegStorageId,
        source_backend = SourceBackend,
        registering_provider_ctx = build_provider_ctx(User, RegProvider),
        other_provider_ctx = build_provider_ctx(User, OtherProvider)
    }.


%% @private
-spec build_provider_ctx(oct_background:entity_selector(), oct_background:entity_selector()) ->
    #provider_ctx{}.
build_provider_ctx(User, ProviderSelector) ->
    #provider_ctx{
        selector = ProviderSelector,
        node = oct_background:get_random_provider_node(ProviderSelector),
        session_id = oct_background:get_user_session_id(User, ProviderSelector)
    }.


%% @private
-spec create_registering_storage(s3 | http, oct_background:entity_selector()) ->
    {storage:id(), source_backend()}.
create_registering_storage(s3, ProviderSelector) ->
    StorageId = storage_import_test_utils:create_storage(s3, ProviderSelector, true),
    {StorageId, {s3, ProviderSelector, StorageId}};
create_registering_storage(http, ProviderSelector) ->
    Server = http_storage_test_server:start(ProviderSelector),
    StorageId = space_setup_utils:create_storage(ProviderSelector, #http_storage_params{
        endpoint = http_storage_test_server:endpoint(Server),
        emulate_range_read = true,
        % the test server does not support native range reads, so all reads (including the
        % per-block reads during replication) rely on range-read emulation, which downloads
        % the whole file. Set the eligibility limit comfortably above ?LARGE_FILE_SIZE so
        % that the largest file used by the suite is still served (the storage default may
        % be smaller and would otherwise make such files unreadable).
        max_emulated_range_read_file_size = 2 * ?LARGE_FILE_SIZE
    }),
    {StorageId, {http, ProviderSelector, Server}}.


%% @private
-spec create_posix_storage(oct_background:entity_selector()) -> storage:id().
create_posix_storage(ProviderSelector) ->
    space_setup_utils:create_storage(ProviderSelector, #posix_storage_params{
        mount_point = <<"/mnt/st_", (?RAND_STR())/binary>>
    }).


%% @private
-spec place_source_file(source_backend(), helpers:file_id(), binary()) -> ok.
place_source_file({s3, ProviderSelector, StorageId}, StorageFileId, Content) ->
    storage_file_setup_utils:create_file(ProviderSelector, StorageId, StorageFileId, Content);
place_source_file({http, ProviderSelector, Server}, StorageFileId, Content) ->
    http_storage_test_server:add_file(ProviderSelector, Server, StorageFileId, Content).


%% @private
-spec update_source_file(source_backend(), helpers:file_id(), binary()) -> ok.
update_source_file({s3, ProviderSelector, StorageId}, StorageFileId, Content) ->
    storage_file_setup_utils:write_file(ProviderSelector, StorageId, StorageFileId, 0, Content);
update_source_file({http, ProviderSelector, Server}, StorageFileId, Content) ->
    http_storage_test_server:add_file(ProviderSelector, Server, StorageFileId, Content).


%% @private
-spec remove_source_file(source_backend(), helpers:file_id(), non_neg_integer()) -> ok.
remove_source_file({s3, ProviderSelector, StorageId}, StorageFileId, CurrentSize) ->
    storage_file_setup_utils:delete_file(ProviderSelector, StorageId, StorageFileId, CurrentSize);
remove_source_file({http, ProviderSelector, Server}, StorageFileId, _CurrentSize) ->
    http_storage_test_server:remove_file(ProviderSelector, Server, StorageFileId).


%% @private
%% @doc Replaces the source file content, actually changing its size (unlike
%% update_source_file/3 which only overwrites from offset 0 - for S3 that would
%% leave trailing bytes, so the file is deleted and recreated instead).
-spec replace_source_file(source_backend(), helpers:file_id(), non_neg_integer(), binary()) -> ok.
replace_source_file({s3, ProviderSelector, StorageId}, StorageFileId, CurrentSize, NewContent) ->
    ok = storage_file_setup_utils:delete_file(ProviderSelector, StorageId, StorageFileId, CurrentSize),
    storage_file_setup_utils:create_file(ProviderSelector, StorageId, StorageFileId, NewContent);
replace_source_file({http, ProviderSelector, Server}, StorageFileId, _CurrentSize, NewContent) ->
    http_storage_test_server:add_file(ProviderSelector, Server, StorageFileId, NewContent).


%% @private
%% @doc Makes the source storage return the given HTTP error for the file (HTTP
%% backend only - used by read_registered_file_when_storage_returns_error_test).
-spec set_source_file_error(source_backend(), helpers:file_id(), non_neg_integer()) -> ok.
set_source_file_error({http, ProviderSelector, Server}, StorageFileId, StatusCode) ->
    http_storage_test_server:set_file_error(ProviderSelector, Server, StorageFileId, StatusCode).


%% @private
-spec register_file(oct_background:node(), oct_background:entity_selector(), map()) ->
    {ok, integer(), map(), binary()} | {error, term()}.
register_file(Node, UserSelector, Body) ->
    Headers = #{
        ?HDR_X_AUTH_TOKEN => oct_background:get_user_access_token(UserSelector),
        ?HDR_CONTENT_TYPE => <<"application/json">>
    },
    rest_test_utils:request(
        Node, <<"data/register">>, post, Headers, json_utils:encode(Body), [{recv_timeout, 30000}]
    ).


%% @private
%% @doc Reads exactly Size bytes of a (locally available) file, accumulating across the
%% individual reads so that a short read at a block boundary does not truncate the result.
-spec read_full_content(oct_background:node(), lfm:handle(), non_neg_integer()) -> binary().
read_full_content(Node, Handle, Size) ->
    read_full_content(Node, Handle, 0, Size, <<>>).

%% @private
-spec read_full_content(oct_background:node(), lfm:handle(), non_neg_integer(),
    non_neg_integer(), binary()) -> binary().
read_full_content(_Node, _Handle, Offset, Size, Acc) when Offset >= Size ->
    Acc;
read_full_content(Node, Handle, Offset, Size, Acc) ->
    {ok, Chunk} = lfm_proxy:read(Node, Handle, Offset, Size - Offset),
    case byte_size(Chunk) of
        0 -> Acc;
        ChunkSize -> read_full_content(Node, Handle, Offset + ChunkSize, Size, <<Acc/binary, Chunk/binary>>)
    end.


%% @private
mock_file_meta_save(ProviderSelector, FileName) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    TestMasterPid = self(),
    ok = test_utils:mock_new(Nodes, file_meta),
    ok = test_utils:mock_expect(Nodes, file_meta, save, fun(Doc = #document{value = FM}) ->
        case FM#file_meta.name =:= FileName of
            true ->
                TestMasterPid ! saving_file_meta_frozen,
                timer:sleep(timer:seconds(5)),
                meck:passthrough([Doc]);
            false ->
                meck:passthrough([Doc])
        end
    end).


%% @private
unmock_file_meta_save(ProviderSelector) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_unload(Nodes, file_meta).


%% @private
wait_until_saving_file_meta_is_frozen() ->
    receive saving_file_meta_frozen -> ok end.


%%%===================================================================
%%% Clean up functions
%%%===================================================================


-spec clean_up_after_previous_run([atom()], #file_registration_test_suite_ctx{}) -> ok.
clean_up_after_previous_run(AllTestCases, #file_registration_test_suite_ctx{
    registering_provider_selector = RegProvider,
    other_provider_selector = OtherProvider
}) ->
    storage_import_test_utils:clean_up_after_previous_run(AllTestCases, RegProvider, OtherProvider).
