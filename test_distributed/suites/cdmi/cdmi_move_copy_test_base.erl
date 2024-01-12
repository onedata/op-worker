%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% CDMI move and copy tests
%%% @end
%%%-------------------------------------------------------------------
-module(cdmi_move_copy_test_base).
-author("Katarzyna Such").

-include("cdmi_test.hrl").
-include("http/cdmi.hrl").
-include("modules/fslogic/metadata.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% Tests
-export([
    copy_file_test/1,
    copy_dir_test/1,
    move_file_test/1,
    move_dir_test/1,
    moved_file_permanently_test/1,
    moved_dir_permanently_test/1,
    moved_dir_with_QS_permanently_test/1,
    move_copy_conflict_test/1
]).


%%%===================================================================
%%% Test functions
%%%===================================================================


copy_file_test(Config) ->
    Xattrs = #{<<"key1">> => <<"value1">>, <<"key2">> => <<"value2">>},
    UserId = oct_background:get_user_id(user2),
    UserName = oct_background:get_user_fullname(user2),
    FileData = <<"data">>,
    JsonMetadata = #{<<"a">> => <<"b">>, <<"c">> => 2, <<"d">> => []},

    % create file to copy
    #object{guid = FileGuid} = onenv_file_test_utils:create_and_sync_file_tree(
        user2,
        node_cache:get(root_dir_guid),
        #file_spec{
            name = atom_to_binary(?FUNCTION_NAME),
            content = FileData,
            metadata = #metadata_spec{
                json = JsonMetadata,
                xattrs = Xattrs
            }
        }, Config#cdmi_test_config.p1_selector
    ),
    FilePath = ?build_test_root_path(Config),
    FileAcl = [#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId,
        name = UserName,
        aceflags = ?no_flags_mask,
        acemask = ?all_object_perms_mask
    }],
    NewFilePath = cdmi_test_utils:build_test_root_path(Config, atom_to_list(?FUNCTION_NAME) ++"1"),
    ok = cdmi_test_utils:set_acl(FilePath, FileAcl, Config),

    % assert source file is created and destination does not exist
    ?assert(cdmi_test_utils:object_exists(FilePath, Config)),
    ?assertNot(cdmi_test_utils:object_exists(NewFilePath, Config)),
    ?assertEqual(FileData, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),
    ?assertEqual({ok, FileAcl}, cdmi_test_utils:get_acl(FilePath, Config), ?ATTEMPTS),

    % copy file using cdmi
    RequestHeaders = [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER, ?CDMI_OBJECT_CONTENT_TYPE_HEADER],
    RequestBody = json_utils:encode(#{<<"copy">> => build_random_src_uri(FilePath, FileGuid)}),
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _, _},
        cdmi_test_utils:do_request(
        ?WORKERS(Config), NewFilePath, put, RequestHeaders, RequestBody
    )),

    % assert new file is created
    ?assert(cdmi_test_utils:object_exists(FilePath, Config), ?ATTEMPTS),
    ?assert(cdmi_test_utils:object_exists(NewFilePath, Config), ?ATTEMPTS),
    ?assertEqual(FileData, cdmi_test_utils:get_file_content(NewFilePath, Config), ?ATTEMPTS),
    ?assertEqual({ok, JsonMetadata}, cdmi_test_utils:get_json_metadata(NewFilePath, Config), ?ATTEMPTS),
    ?assertEqual([
        #xattr{name = <<"key1">>, value = <<"value1">>},
        #xattr{name = <<"key2">>, value = <<"value2">>},
        #xattr{name = ?JSON_METADATA_KEY, value = JsonMetadata}
    ], cdmi_test_utils:get_xattrs(NewFilePath, Config), ?ATTEMPTS
    ),
    ?assertEqual({ok, FileAcl}, cdmi_test_utils:get_acl(NewFilePath, Config), ?ATTEMPTS).


copy_dir_test(Config) ->
    Xattrs = #{<<"key1">> => <<"value1">>, <<"key2">> => <<"value2">>},
    UserId = oct_background:get_user_id(user2),
    UserName = oct_background:get_user_fullname(user2),
    [WorkerP1, _WorkerP2] = ?WORKERS(Config),
    #object{guid = DirGuid} = onenv_file_test_utils:create_and_sync_file_tree(
        user2,
        node_cache:get(root_dir_guid),
        #dir_spec{
            name = atom_to_binary(?FUNCTION_NAME),
            metadata = #metadata_spec{
                xattrs = Xattrs
            },
            children = [
                #dir_spec{
                    name = <<"dir1">>,
                    children = [
                        #file_spec{name = <<"1">>},
                        #file_spec{name = <<"2">>}
                ]},
                #dir_spec{name = <<"dir2">>},
                #file_spec{name = <<"3">>}
            ]
        }, Config#cdmi_test_config.p1_selector
    ),
    DirPath = ?build_test_root_path(Config) ++ "/",
    NewDirPath = cdmi_test_utils:build_test_root_path(
        Config, "new" ++ atom_to_list(?FUNCTION_NAME)
    ) ++ "/",
    DirAcl = [#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId,
        name = UserName,
        aceflags = ?no_flags_mask,
        acemask = ?all_container_perms_mask
    }],
    ?assert(cdmi_test_utils:object_exists(DirPath, Config)),
    cdmi_test_utils:set_acl(DirPath, DirAcl, Config),

    % assert source files are successfully created, and destination file does not exist
    ?assert(cdmi_test_utils:object_exists(DirPath, Config)),
    ?assert(cdmi_test_utils:object_exists(filename:join(DirPath, "dir1"), Config)),
    ?assert(cdmi_test_utils:object_exists(filename:join(DirPath, "dir2"), Config)),
    ?assert(cdmi_test_utils:object_exists(filename:join([DirPath, "dir1", "1"]), Config)),
    ?assert(cdmi_test_utils:object_exists(filename:join([DirPath, "dir1", "2"]), Config)),
    ?assert(cdmi_test_utils:object_exists(filename:join(DirPath, "3"), Config)),
    ?assertNot(cdmi_test_utils:object_exists(NewDirPath, Config)),

    % copy dir using cdmi
    RequestHeaders = [
        cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER, ?CDMI_CONTAINER_CONTENT_TYPE_HEADER
    ],
    RequestBody = json_utils:encode(#{
        <<"copy">> => build_random_src_uri(DirPath, DirGuid)
    }),

    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _, _},
        cdmi_test_utils:do_request(
            WorkerP1, NewDirPath, put, RequestHeaders, RequestBody
        ),
        ?ATTEMPTS
    ),

    % assert source files still exists
    ?assert(cdmi_test_utils:object_exists(DirPath, Config)),
    ?assert(cdmi_test_utils:object_exists(filename:join(DirPath, "dir1"), Config)),
    ?assert(cdmi_test_utils:object_exists(filename:join(DirPath, "dir2"), Config)),
    ?assert(cdmi_test_utils:object_exists(filename:join([DirPath, "dir1", "1"]), Config)),
    ?assert(cdmi_test_utils:object_exists(filename:join([DirPath, "dir1", "2"]), Config)),
    ?assert(cdmi_test_utils:object_exists(filename:join(DirPath, "3"), Config)),

    % assert destination files have been created
    ?assert(cdmi_test_utils:object_exists(NewDirPath, Config), ?ATTEMPTS),
    ?assertEqual([
        #xattr{name = <<"key1">>, value = <<"value1">>},
        #xattr{name = <<"key2">>, value = <<"value2">>}
    ], cdmi_test_utils:get_xattrs(NewDirPath, Config), ?ATTEMPTS),
    ?assertEqual({ok, DirAcl}, cdmi_test_utils:get_acl(NewDirPath, Config), ?ATTEMPTS),
    ?assert(cdmi_test_utils:object_exists(filename:join(NewDirPath, "dir1"), Config)),
    ?assert(cdmi_test_utils:object_exists(filename:join(NewDirPath, "dir2"), Config)),
    ?assert(cdmi_test_utils:object_exists(filename:join([NewDirPath, "dir1", "1"]), Config)),
    ?assert(cdmi_test_utils:object_exists(filename:join([NewDirPath, "dir1", "2"]), Config)),
    ?assert(cdmi_test_utils:object_exists(filename:join(NewDirPath, "3"), Config)).


% tests copy and move operations on dataobjects and containers
move_file_test(Config) ->
    #object{guid = FileGuid} = onenv_file_test_utils:create_and_sync_file_tree(
        user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = atom_to_binary(?FUNCTION_NAME),
            content = <<"data">>
        }, Config#cdmi_test_config.p1_selector
    ),
    FilePath = ?build_test_root_path(Config),
    FileData = <<"data">>,
    NewMoveFilePath = cdmi_test_utils:build_test_root_path(Config, "new" ++ atom_to_list(?FUNCTION_NAME)),

    ?assert(cdmi_test_utils:object_exists(FilePath, Config)),
    ?assertNot(cdmi_test_utils:object_exists(NewMoveFilePath, Config)),
    ?assertEqual(FileData, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),

    RequestHeaders = [cdmi_test_utils:user_2_token_header(),
        ?CDMI_VERSION_HEADER, ?CDMI_OBJECT_CONTENT_TYPE_HEADER],
    RequestBody = json_utils:encode(#{<<"move">> => build_random_src_uri(FilePath, FileGuid)}),
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _Headers3, _Response3},
        cdmi_test_utils:do_request(?WORKERS(Config), NewMoveFilePath, put, RequestHeaders, RequestBody)
    ),
    ?assertNot(cdmi_test_utils:object_exists(FilePath, Config), ?ATTEMPTS),
    ?assert(cdmi_test_utils:object_exists(NewMoveFilePath, Config), ?ATTEMPTS),
    ?assertEqual(FileData, cdmi_test_utils:get_file_content(NewMoveFilePath, Config), ?ATTEMPTS).


move_dir_test(Config) ->
    #object{guid = DirGuid}  = onenv_file_test_utils:create_and_sync_file_tree(
        user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = atom_to_binary(?FUNCTION_NAME)
        }, Config#cdmi_test_config.p1_selector
    ),
    DirPath = ?build_test_root_path(Config) ++ "/",
    NewMoveDirPath = cdmi_test_utils:build_test_root_path(Config, "new" ++ atom_to_list(?FUNCTION_NAME)) ++ "/",

    ?assert(cdmi_test_utils:object_exists(DirPath, Config)),
    ?assertNot(cdmi_test_utils:object_exists(NewMoveDirPath, Config)),

    RequestHeaders = [cdmi_test_utils:user_2_token_header(),
        ?CDMI_VERSION_HEADER, ?CDMI_CONTAINER_CONTENT_TYPE_HEADER
    ],
    RequestBody = json_utils:encode(#{<<"move">> => build_random_src_uri(DirPath, DirGuid)}),
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _Headers2, _Response2},
        cdmi_test_utils:do_request(?WORKERS(Config), NewMoveDirPath, put, RequestHeaders, RequestBody)
    ),

    ?assertNot(cdmi_test_utils:object_exists(DirPath, Config), ?ATTEMPTS),
    ?assert(cdmi_test_utils:object_exists(NewMoveDirPath, Config), ?ATTEMPTS).


%% tests if cdmi returns 'moved permanently' code when we forget about '/' in path
moved_file_permanently_test(Config) ->
    WorkerP1 = oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
    CDMIEndpoint = cdmi_test_utils:get_cdmi_endpoint(Config),
    FilePath = ?build_test_root_path(Config),
    FilePathWithSlash = FilePath ++ "/",

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{name = atom_to_binary(?FUNCTION_NAME)},
    Config#cdmi_test_config.p1_selector),

    RequestHeaders = [
        ?CDMI_OBJECT_CONTENT_TYPE_HEADER,
        ?CDMI_VERSION_HEADER,
        cdmi_test_utils:user_2_token_header()
    ],
    Location3 = list_to_binary(CDMIEndpoint ++ FilePath),
    {ok, _Code3, Headers3, _Response3} = ?assertMatch(
        {ok, ?HTTP_302_FOUND, _, _},
        cdmi_test_utils:do_request(WorkerP1, FilePathWithSlash, get, RequestHeaders, [])
    ),
    ?assertMatch(#{?HDR_LOCATION := Location3}, Headers3).


moved_dir_permanently_test(Config) ->
    WorkerP1 = oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
    CDMIEndpoint = cdmi_test_utils:get_cdmi_endpoint(Config),
    DirPathWithoutSlash = ?build_test_root_path(Config),
    DirPath = DirPathWithoutSlash ++ "/",
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = atom_to_binary(?FUNCTION_NAME),
            children = [
                #file_spec{name = <<"somefile.txt">>}
        ]}
        , Config#cdmi_test_config.p1_selector
    ),

    RequestHeaders = [
        ?CDMI_CONTAINER_CONTENT_TYPE_HEADER,
        ?CDMI_VERSION_HEADER,
        cdmi_test_utils:user_2_token_header()
    ],
    Location = list_to_binary(CDMIEndpoint ++ DirPath),
    {ok, _Code, Headers, _Response} = ?assertMatch(
        {ok, ?HTTP_302_FOUND, _, _},
        cdmi_test_utils:do_request(WorkerP1, DirPathWithoutSlash, get, RequestHeaders, [])
    ),
    ?assertMatch(#{?HDR_LOCATION := Location}, Headers).


moved_dir_with_QS_permanently_test(Config) ->
    WorkerP1 = oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
    CDMIEndpoint = cdmi_test_utils:get_cdmi_endpoint(Config),
    DirPathWithoutSlash = ?build_test_root_path(Config),
    DirPath = DirPathWithoutSlash ++ "/",
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = atom_to_binary(?FUNCTION_NAME),
            children = [
                #file_spec{name = <<"somefile.txt">>}
        ]}
        , Config#cdmi_test_config.p1_selector
    ),

    RequestHeaders = [
        ?CDMI_CONTAINER_CONTENT_TYPE_HEADER,
        ?CDMI_VERSION_HEADER,
        cdmi_test_utils:user_2_token_header()
    ],
    Location = list_to_binary(CDMIEndpoint ++ DirPath ++ "?example_qs=1"),
    {ok, _Code, Headers, _Response} = ?assertMatch(
        {ok, ?HTTP_302_FOUND, _, _},
        cdmi_test_utils:do_request(
            WorkerP1, DirPathWithoutSlash ++ "?example_qs=1", get, RequestHeaders, [])
    ),
    ?assertMatch(#{?HDR_LOCATION := Location}, Headers).


move_copy_conflict_test(Config) ->
    FilePath = ?build_test_root_path(Config),
    FileUri = list_to_binary(filename:join("/", FilePath)),
    FileData = <<"data">>,
    NewMoveFilePath = cdmi_test_utils:build_test_root_path(Config, "new" ++ atom_to_list(?FUNCTION_NAME)),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = atom_to_binary(?FUNCTION_NAME),
            content = <<"data">>
        }, Config#cdmi_test_config.p1_selector
    ),

    ?assertEqual(FileData, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),

    RequestHeaders = [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER, ?CDMI_OBJECT_CONTENT_TYPE_HEADER],
    RequestBody = json_utils:encode(#{<<"move">> => FileUri, <<"copy">> => FileUri}),
    GetResponseErrorFun = fun() ->
        {ok, Code, _Headers, Response} = cdmi_test_utils:do_request(
            ?WORKERS(Config), NewMoveFilePath, put, RequestHeaders, RequestBody
        ),
        {Code, json_utils:decode(Response)}
    end,
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_MALFORMED_DATA),
    ?assertMatch(ExpRestError, GetResponseErrorFun(), ?ATTEMPTS),
    ?assertEqual(FileData, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec build_random_src_uri(list(), file_id:file_guid()) -> binary().
build_random_src_uri(Path, Guid) ->
    PathBin = str_utils:to_binary(Path),
    case rand:uniform(3) of
        1 ->
            PathBin;
        2 ->
            {ok, ObjectId} = file_id:guid_to_objectid(Guid),
            <<"/cdmi_objectid/", ObjectId/binary>>;
        3 ->
            [_SpaceName, RootDir, PathTokens] = filepath_utils:split(PathBin),
            {ok, SpaceObjectId} = file_id:guid_to_objectid(
                fslogic_file_id:spaceid_to_space_dir_guid(file_id:guid_to_space_id(Guid))
            ),

            PathBin2 = filepath_utils:join([SpaceObjectId, RootDir, PathTokens]),
            <<"/cdmi_objectid/", PathBin2/binary>>
    end.
