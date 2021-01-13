%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests file registration mechanism
%%% @end
%%%--------------------------------------------------------------------
-module(file_registration_test_SUITE).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include("rest_test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").


%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    register_file_test/1,
    register_file_and_create_parents_test/1,
    update_registered_file_test/1,
    stat_on_storage_should_not_be_performed_if_automatic_detection_of_attributes_is_disabled/1,
    registration_should_fail_if_size_is_not_passed_and_automatic_detection_of_attributes_is_disabled/1,
    registration_should_fail_if_file_is_missing/1,
    registration_should_succeed_if_size_is_passed/1,
    interrupted_registration_test/1,
    interrupted_registration_nested_file_test/1,
    register_many_files_test/1,
    register_many_nested_files_test/1
]).

-define(TEST_CASES, [
    register_file_test,
    register_file_and_create_parents_test,
    update_registered_file_test,
    stat_on_storage_should_not_be_performed_if_automatic_detection_of_attributes_is_disabled,
    registration_should_fail_if_size_is_not_passed_and_automatic_detection_of_attributes_is_disabled,
    registration_should_fail_if_file_is_missing,
    registration_should_succeed_if_size_is_passed,
    interrupted_registration_test,
    interrupted_registration_nested_file_test,
    register_many_files_test,
    register_many_nested_files_test
]).

all() -> ?ALL(?TEST_CASES).

%% test data
-define(USER1, <<"user1">>).
-define(USER_1_AUTH_HEADERS(Config), ?USER_1_AUTH_HEADERS(Config, [])).
-define(USER_1_AUTH_HEADERS(Config, OtherHeaders),
    ?USER_AUTH_HEADERS(Config, <<"user1">>, OtherHeaders)).
-define(SPACE_ID, <<"space1">>).
-define(SPACE_NAME, <<"space_name1">>).
-define(FILE_NAME, <<"file_", (?RAND_NAME)/binary>>).
-define(DIR_NAME, <<"dir_", (?RAND_NAME)/binary>>).
-define(RAND_NAME,
    <<(str_utils:to_binary(?FUNCTION))/binary, "_", (integer_to_binary(rand:uniform(?RAND_RANGE)))/binary>>).
-define(RAND_RANGE, 1000000000).
-define(TEST_DATA, <<"abcdefgh">>).
-define(TEST_DATA2, <<"zyxwvut">>).
-define(CANONICAL_PATH(FileRelativePath), filename:join(["/", ?SPACE_ID, FileRelativePath])).
-define(PATH(FileRelativePath), filepath_utils:join([<<"/">>, ?SPACE_NAME, FileRelativePath])).
-define(XATTR_KEY(N), <<"xattrName", (integer_to_binary(N))/binary>>).
-define(XATTR_VALUE(N), <<"xattrValue", (integer_to_binary(N))/binary>>).
-define(XATTRS, #{
    ?XATTR_KEY(1) => 1,
    ?XATTR_KEY(2) => ?XATTR_VALUE(2)
}).
-define(XATTRS2, #{
    ?XATTR_KEY(1) => ?XATTR_VALUE(1),
    ?XATTR_KEY(3) => ?XATTR_VALUE(3)
}).

-define(JSON1, #{
    <<"key1">> => <<"value1">>,
    <<"key2">> => #{
        <<"key21">> => <<"value21">>
    }
}).

-define(JSON2, #{
    <<"key1">> => <<"value1.2">>,
    <<"key3">> => #{
        <<"key31">> => <<"value31">>
    }
}).
-define(RDF1, <<"<rdf>metadata_1</rdf>">>).
-define(RDF2, <<"<rdf>metadata_2</rdf>">>).
-define(ENCODED_RDF(RDF), base64:encode(RDF)).
-define(ENCODED_RDF1, ?ENCODED_RDF(?RDF1)).
-define(ENCODED_RDF2, ?ENCODED_RDF(?RDF2)).

-define(ATTEMPTS, 15).

-define(assertInLs(Worker, SessId, FilePath, Attempts), (
    fun(__Worker, __SessId, __FilePath, __Attempts) ->
        ?assertMatch(true, try
            __DirPath = filename:dirname(__FilePath),
            {ok, __Children} = lfm_proxy:get_children(Worker, SessId, {path, __DirPath}, 0, 1000),
            __ChildrenNames = [_N || {_G, _N} <- __Children],
            lists:member(filename:basename(__FilePath), __ChildrenNames)
        catch
            _:_ ->
                error
        end, __Attempts)
    end)(Worker, SessId, FilePath, Attempts)
).

-define(assertStat(Worker, SessId, FilePath, Attempts),
    __Name = filename:basename(FilePath),
    ?assertMatch({ok, #file_attr{name = __Name}}, lfm_proxy:stat(Worker, SessId, {path, FilePath}), Attempts)
).

-define(assertRead(Worker, SessId, FilePath, Offset, ExpectedData, Attempts), (
    fun(__Worker, __SessId, __FilePath, __Offset, __ExpectedData, __Attempts) ->
        {ok, __H} = ?assertMatch({ok, _},
            lfm_proxy:open(__Worker, __SessId, {path, __FilePath}, read), __Attempts),
        ?assertEqual({ok, __ExpectedData},
            lfm_proxy:read(__Worker, __H, __Offset, byte_size(__ExpectedData)), __Attempts),
        ?assertEqual(ok, lfm_proxy:close(__Worker, __H))
    end)(Worker, SessId, FilePath, Offset, ExpectedData, Attempts)
).

-define(assertWrite(Worker, SessId, FilePath, Offset, Data, Attempts), (
    fun(__Worker, __SessId, __FilePath, __Offset, __Data, __Attempts) ->
        {ok, __H} = ?assertMatch({ok, _},
            lfm_proxy:open(__Worker, __SessId, {path, __FilePath}, rdwr), __Attempts),
        ?assertMatch({ok, _}, lfm_proxy:write(__Worker, __H, __Offset, __Data)),
        ?assertEqual({ok, __Data}, lfm_proxy:read(__Worker, __H, __Offset, byte_size(__Data))),
        ?assertEqual(ok, lfm_proxy:close(__Worker, __H))
    end)(Worker, SessId, FilePath, Offset, Data, Attempts)
).

-define(assertXattrs(Worker, SessId, FilePath, Xattrs, Attempts),
    (fun(__Worker, __SessId, __FilePath, __Xattrs, __Attempts) ->
        ?assertEqual(#{}, maps:fold(fun(__K, __V, __Acc) ->
            ?assertMatch({ok, #xattr{name = __K, value = __V}},
                lfm_proxy:get_xattr(__Worker, __SessId, {path, __FilePath}, __K), __Attempts),
            maps:without([__K], __Acc)
        end, __Xattrs, __Xattrs), __Attempts)
    end)(Worker, SessId, FilePath, Xattrs, Attempts)
).

-define(assertJsonMetadata(Worker, SessId, FilePath, JSON, Attempts),
    (fun
        (__Worker, __SessId, __FilePath, __JSON, __Attempts) when map_size(__JSON) =:= 0 ->
            ?assertMatch({error, ?ENODATA},
                lfm_proxy:get_metadata(__Worker, __SessId, {path, __FilePath}, json, [], false), __Attempts);
        (__Worker, __SessId, __FilePath, __JSON, __Attempts) ->
            ?assertMatch({ok, __JSON},
                lfm_proxy:get_metadata(__Worker, __SessId, {path, __FilePath}, json, [], false), __Attempts)
    end)(Worker, SessId, FilePath, JSON, Attempts)
).

-define(assertRdfMetadata(Worker, SessId, FilePath, RDF, Attempts),
    (fun
        (__Worker, __SessId, __FilePath, <<>>, __Attempts) ->
            ?assertMatch({error, ?ENODATA},
                lfm_proxy:get_metadata(__Worker, __SessId, {path, __FilePath}, rdf, [], false), __Attempts);
        (__Worker, __SessId, __FilePath, __RDF, __Attempts) ->
            ?assertMatch({ok, __RDF},
                lfm_proxy:get_metadata(__Worker, __SessId, {path, __FilePath}, rdf, [], false), __Attempts)
    end)(Worker, SessId, FilePath, RDF, Attempts)
).

-define(assertFile(Worker, SessionId, FilePath, ReadData, Xattrs, JSON, RDF),
    ?assertFile(Worker, SessionId, FilePath, ReadData, Xattrs, JSON, RDF, 1)).
-define(assertFile(Worker, SessionId, FilePath, ReadData, Xattrs, JSON, RDF, Attempts),
    verify_file(Worker, SessionId, FilePath, ReadData, Xattrs, JSON, RDF, Attempts)).

%%%==================================================================
%%% Test functions
%%%===================================================================

% TODO VFS-6509 test conflict with LFM

register_file_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),

    FileName = ?FILE_NAME,
    FilePath = ?PATH(FileName),
    StorageFileId = filename:join(["/", FileName]),
    StorageId = initializer:get_supporting_storage_id(W1, ?SPACE_ID),
    SDFileHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageFileId),
    ok = sd_test_utils:create_file(W1, SDFileHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDFileHandle, 0, ?TEST_DATA),

    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(W1, Config, #{
        <<"spaceId">> => ?SPACE_ID,
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
    ?assertFile(W1, SessId, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1),

    % check whether file is visible on 2nd provider
    ?assertFile(W2, SessId2, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS).

register_file_and_create_parents_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),

    FileName = ?FILE_NAME,
    DestinationPath = filename:join(["/", ?DIR_NAME, ?DIR_NAME, ?DIR_NAME, FileName]),
    FilePath = ?PATH(DestinationPath),
    StorageFileId = FileName,
    StorageId = initializer:get_supporting_storage_id(W1, ?SPACE_ID),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageFileId),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),

    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(W1, Config, #{
        <<"spaceId">> => ?SPACE_ID,
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
    ?assertFile(W1, SessId, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1),

    % check whether file is visible on 2nd provider
    ?assertFile(W2, SessId2, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS).

update_registered_file_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),

    FileName = ?FILE_NAME,
    DestinationPath = FileName,
    StorageFileId = FileName,
    FilePath = ?PATH(FileName),
    StorageId = initializer:get_supporting_storage_id(W1, ?SPACE_ID),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageFileId),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),

    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(W1, Config, #{
        <<"spaceId">> => ?SPACE_ID,
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
    ?assertFile(W1, SessId, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1),

    % check whether file is visible on 2nd provider
    ?assertFile(W2, SessId2, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS),

    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA2),

    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(W1, Config, #{
        <<"spaceId">> => ?SPACE_ID,
        <<"destinationPath">> => DestinationPath,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"mtime">> => global_clock:timestamp_seconds(),
        <<"size">> => byte_size(?TEST_DATA2),
        <<"mode">> => <<"664">>
        })),

    % check whether file has been properly updated
    ?assertFile(W1, SessId, FilePath, ?TEST_DATA2, ?XATTRS, ?JSON1, ?RDF1),

    % check whether file was updated on 2nd provider
    ?assertFile(W2, SessId2, FilePath, ?TEST_DATA2, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS),

    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(W1, Config, #{
        <<"spaceId">> => ?SPACE_ID,
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
    ?assertFile(W1, SessId, FilePath, ?TEST_DATA2, XATTRS3, ?JSON2, ?RDF2),

    % check whether file was updated on 2nd provider
    ?assertFile(W2, SessId2, FilePath, ?TEST_DATA2, XATTRS3, ?JSON2, ?RDF2, ?ATTEMPTS).

stat_on_storage_should_not_be_performed_if_automatic_detection_of_attributes_is_disabled(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),

    FileName = ?FILE_NAME,
    FilePath = ?PATH(FileName),
    StorageFileId = filename:join(["/", FileName]),
    StorageId = initializer:get_supporting_storage_id(W1, ?SPACE_ID),
    SDFileHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageFileId),
    ok = sd_test_utils:create_file(W1, SDFileHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDFileHandle, 0, ?TEST_DATA),
    Timestamp = global_clock:timestamp_seconds(),

    ok = test_utils:mock_new(W1, [storage_driver], [passthrough]),
    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(W1, Config, #{
        <<"spaceId">> => ?SPACE_ID,
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

    test_utils:mock_assert_num_calls(W1, storage_driver, stat, ['_'], 0),

    % check whether file has been properly registered
    ?assertFile(W1, SessId, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1),

    % check whether file is visible on 2nd provider
    ?assertFile(W2, SessId2, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS).

registration_should_fail_if_size_is_not_passed_and_automatic_detection_of_attributes_is_disabled(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),

    FileName = ?FILE_NAME,
    FilePath = ?PATH(FileName),
    StorageFileId = filename:join(["/", FileName]),
    StorageId = initializer:get_supporting_storage_id(W1, ?SPACE_ID),
    SDFileHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageFileId),
    ok = sd_test_utils:create_file(W1, SDFileHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDFileHandle, 0, ?TEST_DATA),

    ?assertMatch({ok, ?HTTP_400_BAD_REQUEST, _, _}, register_file(W1, Config, #{
        <<"spaceId">> => ?SPACE_ID,
        <<"destinationPath">> => FileName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"autoDetectAttributes">> => false
    })),

    % file shouldn't have been registered
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(W1, SessId, {path, FilePath})).

registration_should_fail_if_file_is_missing(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),

    FileName = ?FILE_NAME,
    FilePath = ?PATH(FileName),
    StorageFileId = filename:join(["/", FileName]),
    StorageId = initializer:get_supporting_storage_id(W1, ?SPACE_ID),

    ?assertMatch({ok, ?HTTP_400_BAD_REQUEST, _, _}, register_file(W1, Config, #{
        <<"spaceId">> => ?SPACE_ID,
        <<"destinationPath">> => FileName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"size">> => 100
    })),

    % file shouldn't have been registered
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(W1, SessId, {path, FilePath})).

registration_should_succeed_if_size_is_passed(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),

    FileName = ?FILE_NAME,
    FilePath = ?PATH(FileName),
    StorageFileId = filename:join(["/", FileName]),
    StorageId = initializer:get_supporting_storage_id(W1, ?SPACE_ID),
    SDFileHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageFileId),
    ok = sd_test_utils:create_file(W1, SDFileHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDFileHandle, 0, ?TEST_DATA),

    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(W1, Config, #{
        <<"spaceId">> => ?SPACE_ID,
        <<"destinationPath">> => FileName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"size">> => byte_size(?TEST_DATA)
    })),

    % check whether file has been properly registered
    ?assertFile(W1, SessId, FilePath, ?TEST_DATA, #{}, #{}, <<>>),

    % check whether file is visible on 2nd provider
    ?assertFile(W2, SessId2, FilePath, ?TEST_DATA, #{}, #{}, <<>>, ?ATTEMPTS).

interrupted_registration_test(Config) ->
    % these test checks whether subsequent registration can succeed when previous one was interrupted by used
    % e. g. by pressing CTRL + C
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),

    FileName = ?FILE_NAME,
    FilePath = ?PATH(FileName),
    StorageFileId = filename:join(["/", FileName]),
    StorageId = initializer:get_supporting_storage_id(W1, ?SPACE_ID),
    SDFileHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageFileId),
    ok = sd_test_utils:create_file(W1, SDFileHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDFileHandle, 0, ?TEST_DATA),

    mock_file_meta_save(W1, FileName),

    Pid = spawn(fun() ->
        register_file(W1, Config, #{
            <<"spaceId">> => ?SPACE_ID,
            <<"destinationPath">> => FileName,
            <<"storageFileId">> => StorageFileId,
            <<"storageId">> => StorageId,
            <<"mtime">> => global_clock:timestamp_seconds(),
            <<"size">> => byte_size(?TEST_DATA),
            <<"mode">> => <<"664">>,
            <<"xattrs">> => ?XATTRS,
            <<"json">> => ?JSON1,
            <<"rdf">> => ?ENCODED_RDF1
        })
    end),

    wait_until_saving_file_meta_is_frozen(),

    % kill process that requested the registration, the same things happen when user
    % aborts the REST request with CTRL + C
    exit(Pid, shutdown),

    % file shouldn't have been registered
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W1, SessId, {path, ?PATH(FileName)})),

    unmock_file_meta_save(W1),

    % retry registration
    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(W1, Config, #{
        <<"spaceId">> => ?SPACE_ID,
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
    ?assertFile(W1, SessId, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1),

    % check whether file is visible on 2nd provider
    ?assertFile(W2, SessId2, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS).

interrupted_registration_nested_file_test(Config) ->
    % these test checks whether subsequent registration can succeed when previous one was interrupted by used
    % e. g. by pressing CTRL + C
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),

    DirName = ?DIR_NAME,
    FileName = ?FILE_NAME,
    DestinationPath = filename:join(["/", DirName, FileName]),
    FilePath = ?PATH(DestinationPath),
    StorageFileId = filename:join(["/", FileName]),
    StorageId = initializer:get_supporting_storage_id(W1, ?SPACE_ID),
    SDFileHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageFileId),
    ok = sd_test_utils:create_file(W1, SDFileHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDFileHandle, 0, ?TEST_DATA),

    mock_file_meta_save(W1, DirName),

    Pid = spawn(fun() ->
        ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(W1, Config, #{
            <<"spaceId">> => ?SPACE_ID,
            <<"destinationPath">> => DestinationPath,
            <<"storageFileId">> => StorageFileId,
            <<"storageId">> => StorageId,
            <<"mtime">> => global_clock:timestamp_seconds(),
            <<"size">> => byte_size(?TEST_DATA),
            <<"mode">> => <<"664">>,
            <<"xattrs">> => ?XATTRS,
            <<"json">> => ?JSON1,
            <<"rdf">> => ?ENCODED_RDF1
        }))
    end),

    wait_until_saving_file_meta_is_frozen(),

    % kill process that requested the registration, the same things happen when user
    % aborts the REST request with CTRL + C
    exit(Pid, shutdown),

    % parent dir and file shouldn't have been created
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W1, SessId, {path, ?PATH(DirName)})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W1, SessId, {path, FilePath})),

    unmock_file_meta_save(W1),

    % retry registration
    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(W1, Config, #{
        <<"spaceId">> => ?SPACE_ID,
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
    ?assertFile(W1, SessId, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1),

    % check whether file is visible on 2nd provider
    ?assertFile(W2, SessId2, FilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS).

register_many_files_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    LogicalFilesCount = 100,

    BaseFileName = ?FILE_NAME,
    StorageFileId = filename:join(["/", BaseFileName]),
    StorageId = initializer:get_supporting_storage_id(W1, ?SPACE_ID),
    SDFileHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageFileId),
    % create only 1 file on storage
    ok = sd_test_utils:create_file(W1, SDFileHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDFileHandle, 0, ?TEST_DATA),

    % but register it as many logical files
    DestinationPaths = lists:map(fun(I) ->
        str_utils:format_bin("/~s_~p", [BaseFileName, I])
    end, lists:seq(1, LogicalFilesCount)),

    TestMaster = self(),

    LogicalFilePaths = lists:map(fun(DestinationPath) ->
        LogicalFilePath = ?PATH(DestinationPath),
        spawn(fun() ->
            ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(W1, Config, #{
                <<"spaceId">> => ?SPACE_ID,
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

    verification_loop(LogicalFilePaths, fun(LogicalFilePath) ->
        % check whether file has been properly registered
        ?assertFile(W1, SessId, LogicalFilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1, 60),

        % check whether file is visible on 2nd provider
        ?assertFile(W2, SessId2, LogicalFilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS)
    end, timer:seconds(60)).

register_many_nested_files_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    LogicalFilesCount = 100,

    BaseFileName = ?FILE_NAME,
    StorageFileId = filename:join(["/", BaseFileName]),
    StorageId = initializer:get_supporting_storage_id(W1, ?SPACE_ID),
    SDFileHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageFileId),
    % create only 1 file on storage
    ok = sd_test_utils:create_file(W1, SDFileHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDFileHandle, 0, ?TEST_DATA),

    % but register it as many logical files in the same directory
    Dir1 = ?DIR_NAME,
    Dir2 = ?DIR_NAME,
    Dir3 = ?DIR_NAME,
    ParentPath = filename:join([Dir1, Dir2, Dir3]),
    % but register it as many logical files
    DestinationPaths = lists:map(fun(I) ->
        FileName = str_utils:format_bin("~s_~p", [BaseFileName, I]),
        filename:join(["/", ParentPath, FileName])
    end, lists:seq(1, LogicalFilesCount)),

    TestMaster = self(),

    LogicalFilePaths = lists:map(fun(DestinationPath) ->
        LogicalFilePath = ?PATH(DestinationPath),
        spawn(fun() ->
            ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(W1, Config, #{
                <<"spaceId">> => ?SPACE_ID,
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

    verification_loop(LogicalFilePaths, fun(LogicalFilePath) ->
        % check whether file has been properly registered
        ?assertFile(W1, SessId, LogicalFilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1),

        % check whether file is visible on 2nd provider
        ?assertFile(W2, SessId2, LogicalFilePath, ?TEST_DATA, ?XATTRS, ?JSON1, ?RDF1, ?ATTEMPTS)
    end, timer:seconds(60)).


%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        ssl:start(),
        hackney:start(),
        initializer:disable_quota_limit(NewConfig),
        initializer:mock_provider_ids(NewConfig),
        NewConfig2 = multi_provider_file_ops_test_base:init_env(NewConfig),
        NewConfig3 = sort_workers(NewConfig2),
        [W1 | _] = ?config(op_worker_nodes, NewConfig3),
        ok = rpc:call(W1, storage_import, set_manual_mode, [?SPACE_ID]),
        NewConfig3
    end,
    {ok, _} = application:ensure_all_started(worker_pool),
    [{?LOAD_MODULES, [initializer, sd_test_utils, ?MODULE]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    initializer:unmock_provider_ids(Config),
    ssl:stop().

init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, storage_driver),
    lfm_proxy:teardown(Config).

register_file(Worker, Config, Body) ->
    Headers = ?USER_1_AUTH_HEADERS(Config, [{?HDR_CONTENT_TYPE, <<"application/json">>}]),
    rest_test_utils:request(Worker, <<"data/register">>, post, Headers, json_utils:encode(Body), [{recv_timeout, 30000}]).


verify_file(Worker, SessionId, FilePath, ReadData, Xattrs, JSON, RDF, Attempts) ->
    ?assertInLs(Worker, SessionId, FilePath, Attempts),
    ?assertStat(Worker, SessionId, FilePath, Attempts),
    ?assertRead(Worker, SessionId, FilePath, 0, ReadData, Attempts),
    ?assertXattrs(Worker, SessionId, FilePath, Xattrs, Attempts),
    ?assertJsonMetadata(Worker, SessionId, FilePath, JSON, Attempts),
    ?assertRdfMetadata(Worker, SessionId, FilePath, RDF, Attempts).

sort_workers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:keyreplace(op_worker_nodes, 1, Config, {op_worker_nodes, lists:sort(Workers)}).

mock_file_meta_save(Worker, FileName) ->
    TestMasterPid = self(),
    ok = test_utils:mock_new(Worker, file_meta),
    ok = test_utils:mock_expect(Worker, file_meta, save, fun(Doc = #document{value = FM}) ->
        case FM#file_meta.name =:= FileName of
            true ->
                TestMasterPid ! saving_file_meta_frozen,
                timer:sleep(timer:seconds(5)),
                meck:passthrough([Doc]);
            false ->
                meck:passthrough([Doc])
        end
    end).

unmock_file_meta_save(Worker) ->
    ok = test_utils:mock_unload(Worker, file_meta).

wait_until_saving_file_meta_is_frozen() ->
    receive saving_file_meta_frozen -> ok end.


verification_loop([], _VerifyFun, _TimeoutMillis) ->
    ok;
verification_loop(FilePaths, _VerifyFun, TimeoutMillis) when TimeoutMillis < 0 ->
    ct:pal(
        "Verification loop timeout.~n"
        "Unverified files: ~p", [FilePaths]
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
                "Unverified files: ~p", [FilePaths]
            ),
            ct:fail(verification_loop_timeout)
    end.