%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 ACK CYFRONET AGH
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
-module(file_registration_oct_test_base).
-author("Bartosz Walkowicz").

-include("file_registration_oct_test.hrl").
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
    register_many_nested_files_test/1
]).

-define(SUPPORT_SIZE, 1000000000).

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
        % TODO always create on remote??
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
        <<"autoDetectAttributes">> => false
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

    TestMaster = self(),

    LogicalFilePaths = lists:map(fun(DestinationPath) ->
        LogicalFilePath = filepath_utils:join([SpacePath, DestinationPath]),
        spawn(fun() ->
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
            TestMaster ! {file_registered, LogicalFilePath}
        end),
        LogicalFilePath
    end, DestinationPaths),

    ?assertEqual({message_queue_len, LogicalFilesCount}, process_info(TestMaster, message_queue_len), ?ATTEMPTS),

    verification_loop(LogicalFilePaths, fun(LogicalFilePath) ->
        % check whether file has been properly registered
        ?assertFile(RegNode, RegSessId, LogicalFilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1),

        % check whether file is visible on the other provider
        ?assertFile(OtherNode, OtherSessId, LogicalFilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS)
    end, timer:seconds(60)).


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

    TestMaster = self(),

    LogicalFilePaths = lists:map(fun(DestinationPath) ->
        LogicalFilePath = filepath_utils:join([SpacePath, DestinationPath]),
        spawn(fun() ->
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
            TestMaster ! {file_registered, LogicalFilePath}
        end),
        LogicalFilePath
    end, DestinationPaths),

    ?assertEqual({message_queue_len, LogicalFilesCount}, process_info(TestMaster, message_queue_len), ?ATTEMPTS),

    verification_loop(LogicalFilePaths, fun(LogicalFilePath) ->
        % check whether file has been properly registered
        ?assertFile(RegNode, RegSessId, LogicalFilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1),

        % check whether file is visible on the other provider
        ?assertFile(OtherNode, OtherSessId, LogicalFilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS)
    end, timer:seconds(60)).


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
    StorageId = space_setup_utils:create_storage(ProviderSelector, #s3_storage_params{
        storage_path_type = <<"canonical">>,
        imported_storage = true,
        hostname = build_s3_hostname(ProviderSelector),
        bucket_name = ?RAND_STR(15),
        block_size = 0
    }),
    {StorageId, {s3, ProviderSelector, StorageId}};
create_registering_storage(http, ProviderSelector) ->
    Server = http_storage_test_server:start(ProviderSelector),
    StorageId = space_setup_utils:create_storage(ProviderSelector, #http_storage_params{
        endpoint = http_storage_test_server:endpoint(Server),
        emulate_range_read = true
    }),
    {StorageId, {http, ProviderSelector, Server}}.


%% @private
-spec create_posix_storage(oct_background:entity_selector()) -> storage:id().
create_posix_storage(ProviderSelector) ->
    space_setup_utils:create_storage(ProviderSelector, #posix_storage_params{
        mount_point = <<"/mnt/st_", (?RAND_STR())/binary>>
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


%% @private
verification_loop([], _VerifyFun, _TimeoutMillis) ->
    ok;
verification_loop(FilePaths, _VerifyFun, TimeoutMillis) when TimeoutMillis < 0 ->
    ct:pal(
        "Verification loop timeout.~n"
        "Unverified files: ~tp", [FilePaths]
    ),
    ct:fail(verification_loop_timeout);
verification_loop(FilePaths, VerifyFun, TimeoutMillis) when TimeoutMillis >= 0 ->
    Start = global_clock:timestamp_millis(),
    receive
        {file_registered, FilePath} ->
            End = global_clock:timestamp_millis(),
            VerifyFun(FilePath),
            verification_loop(FilePaths -- [FilePath], VerifyFun, TimeoutMillis - (End - Start))
    after
        TimeoutMillis ->
            ct:pal(
                "Verification loop timeout.~n"
                "Unverified files: ~tp", [FilePaths]
            ),
            ct:fail(verification_loop_timeout)
    end.


%%%===================================================================
%%% Clean up functions
%%%===================================================================


-spec clean_up_after_previous_run([atom()], #file_registration_test_suite_ctx{}) -> ok.
clean_up_after_previous_run(AllTestCases, SuiteCtx) ->
    lists_utils:pforeach(fun(SpaceId) ->
        delete_space_with_supporting_storages(SpaceId, SuiteCtx)
    end, filter_spaces_from_previous_run(AllTestCases)).


%% @private
-spec filter_spaces_from_previous_run([atom()]) -> [od_space:id()].
filter_spaces_from_previous_run(AllTestCases) ->
    lists:filter(fun(SpaceId) ->
        SpaceDetails = ozw_test_rpc:get_space_protected_data(?ROOT, SpaceId),
        SpaceName = maps:get(<<"name">>, SpaceDetails),
        lists:member(binary_to_atom(SpaceName), AllTestCases)
    end, ozw_test_rpc:list_spaces()).


%% @private
-spec delete_space_with_supporting_storages(od_space:id(), #file_registration_test_suite_ctx{}) ->
    ok.
delete_space_with_supporting_storages(SpaceId, #file_registration_test_suite_ctx{
    registering_provider_selector = RegProvider,
    other_provider_selector = OtherProvider
}) ->
    [RegStorage] = opw_test_rpc:get_space_local_storages(RegProvider, SpaceId),
    [OtherStorage] = opw_test_rpc:get_space_local_storages(OtherProvider, SpaceId),

    ok = ozw_test_rpc:delete_space(SpaceId),

    ok = delete_storage(RegProvider, RegStorage),
    ok = delete_storage(OtherProvider, OtherStorage).


%% @private
-spec delete_storage(oct_background:node_selector(), storage:id()) -> ok.
delete_storage(NodeSelector, StorageId) ->
    ?assertEqual(ok, opw_test_rpc:call(NodeSelector, storage, delete, [StorageId]), ?ATTEMPTS).
