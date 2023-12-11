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

-include("http/cdmi.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include("onenv_test_utils.hrl").
-include("cdmi_test.hrl").

%% API
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
    UserId2 = oct_background:get_user_id(user2),
    UserName2 = <<"Unnamed User">>,
    FileData2 = <<"data">>,
    JsonMetadata = #{<<"a">> => <<"b">>, <<"c">> => 2, <<"d">> => []},

    %%---------- file cp ----------- (copy file, with xattrs and acl)
    % create file to copy
    #object{guid = FileGuid} = onenv_file_test_utils:create_and_sync_file_tree(
        user2,
        node_cache:get(root_dir_guid),
        #file_spec{
            name = atom_to_binary(?FUNCTION_NAME),
            content = FileData2,
            metadata = #metadata_spec{
                json = JsonMetadata,
                xattrs = Xattrs
            }
        }, Config#cdmi_test_config.p1_selector
    ),
    FileName2 = ?build_test_root_path(Config),
    FileAcl = [#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?all_object_perms_mask
    }],
    NewFileName2 = ?build_test_root_specified_path(Config, atom_to_list(?FUNCTION_NAME) ++"1"),
    ok = cdmi_internal:set_acl(FileName2, FileAcl, Config),

    % assert source file is created and destination does not exist
    ?assert(cdmi_internal:object_exists(FileName2, Config)),
    ?assert(not cdmi_internal:object_exists(NewFileName2, Config)),
    ?assertEqual(FileData2, cdmi_internal:get_file_content(FileName2, Config), ?ATTEMPTS),
    ?assertEqual({ok, FileAcl}, cdmi_internal:get_acl(FileName2, Config), ?ATTEMPTS),

    % copy file using cdmi
    RequestHeaders4 = [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER, ?CDMI_OBJECT_CONTENT_TYPE_HEADER],
    RequestBody4 = json_utils:encode(#{<<"copy">> => build_random_src_uri(FileName2, FileGuid)}),
    {ok, Code4, _Headers4, _Response4} = cdmi_internal:do_request(
        ?WORKERS, NewFileName2, put, RequestHeaders4, RequestBody4
    ),
    ?assertEqual(?HTTP_201_CREATED, Code4),

    % assert new file is created
    ?assert(cdmi_internal:object_exists(FileName2, Config), ?ATTEMPTS),
    ?assert(cdmi_internal:object_exists(NewFileName2, Config), ?ATTEMPTS),
    ?assertEqual(FileData2, cdmi_internal:get_file_content(NewFileName2, Config), ?ATTEMPTS),
    ?assertEqual({ok, JsonMetadata}, cdmi_internal:get_json_metadata(NewFileName2, Config), ?ATTEMPTS),
    ?assertEqual([
        #xattr{name = <<"key1">>, value = <<"value1">>},
        #xattr{name = <<"key2">>, value = <<"value2">>},
        #xattr{name = ?JSON_METADATA_KEY, value = JsonMetadata}
    ], cdmi_internal:get_xattrs(NewFileName2, Config), ?ATTEMPTS
    ),
    ?assertEqual({ok, FileAcl}, cdmi_internal:get_acl(NewFileName2, Config), ?ATTEMPTS).


copy_dir_test(Config) ->
    Xattrs = #{<<"key1">> => <<"value1">>, <<"key2">> => <<"value2">>},
    UserId2 = oct_background:get_user_id(user2),
    UserName2 = <<"Unnamed User">>,
    [WorkerP1, _WorkerP2] = ?WORKERS,
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
                        #file_spec{
                            name = <<"1">>
                        },
                        #file_spec{
                            name = <<"2">>
                        }
                    ]
                },
                #dir_spec{
                    name = <<"dir2">>
                },
                #file_spec{
                    name = <<"3">>
                }
            ]
        }, Config#cdmi_test_config.p1_selector
    ),
    DirName2 = ?build_test_root_path(Config) ++ "/",
    NewDirName2 = ?build_test_root_specified_path(
        Config, "new" ++ atom_to_list(?FUNCTION_NAME)
    ) ++ "/",
    DirAcl = [#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?all_container_perms_mask
    }],
    %%---------- dir cp ------------
    ?assert(cdmi_internal:object_exists(DirName2, Config)),
    cdmi_internal:set_acl(DirName2, DirAcl, Config),

    % assert source files are successfully created, and destination file does not exist
    ?assert(cdmi_internal:object_exists(DirName2, Config)),
    ?assert(cdmi_internal:object_exists(filename:join(DirName2, "dir1"), Config)),
    ?assert(cdmi_internal:object_exists(filename:join(DirName2, "dir2"), Config)),
    ?assert(cdmi_internal:object_exists(filename:join([DirName2, "dir1", "1"]), Config)),
    ?assert(cdmi_internal:object_exists(filename:join([DirName2, "dir1", "2"]), Config)),
    ?assert(cdmi_internal:object_exists(filename:join(DirName2, "3"), Config)),
    ?assert(not cdmi_internal:object_exists(NewDirName2, Config)),

    % copy dir using cdmi
    RequestHeaders5 = [
        cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER, ?CDMI_CONTAINER_CONTENT_TYPE_HEADER
    ],
    RequestBody5 = json_utils:encode(#{
        <<"copy">> => build_random_src_uri(DirName2, DirGuid)
    }),

    {ok, ?HTTP_201_CREATED, _Headers5, _Response5} = ?assertMatch(
        {ok, 201, _, _},
        cdmi_internal:do_request(
            WorkerP1, NewDirName2, put, RequestHeaders5, RequestBody5
        ),
        ?ATTEMPTS
    ),

    % assert source files still exists
    ?assert(cdmi_internal:object_exists(DirName2, Config)),
    ?assert(cdmi_internal:object_exists(filename:join(DirName2, "dir1"), Config)),
    ?assert(cdmi_internal:object_exists(filename:join(DirName2, "dir2"), Config)),
    ?assert(cdmi_internal:object_exists(filename:join([DirName2, "dir1", "1"]), Config)),
    ?assert(cdmi_internal:object_exists(filename:join([DirName2, "dir1", "2"]), Config)),
    ?assert(cdmi_internal:object_exists(filename:join(DirName2, "3"), Config)),

    % assert destination files have been created
    ?assert(cdmi_internal:object_exists(NewDirName2, Config), ?ATTEMPTS),
    ?assertEqual([
        #xattr{name = <<"key1">>, value = <<"value1">>},
        #xattr{name = <<"key2">>, value = <<"value2">>}
    ], cdmi_internal:get_xattrs(NewDirName2, Config), ?ATTEMPTS),
    ?assertEqual({ok, DirAcl}, cdmi_internal:get_acl(NewDirName2, Config), ?ATTEMPTS),
    ?assert(cdmi_internal:object_exists(filename:join(NewDirName2, "dir1"), Config)),
    ?assert(cdmi_internal:object_exists(filename:join(NewDirName2, "dir2"), Config)),
    ?assert(cdmi_internal:object_exists(filename:join([NewDirName2, "dir1", "1"]), Config)),
    ?assert(cdmi_internal:object_exists(filename:join([NewDirName2, "dir1", "2"]), Config)),
    ?assert(cdmi_internal:object_exists(filename:join(NewDirName2, "3"), Config)).


% tests copy and move operations on dataobjects and containers
move_file_test(Config) ->
    #object{guid = FileGuid} = onenv_file_test_utils:create_and_sync_file_tree(
        user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = atom_to_binary(?FUNCTION_NAME),
            content = <<"data">>
        }, Config#cdmi_test_config.p1_selector
    ),
    FileName = ?build_test_root_path(Config),
    FileData = <<"data">>,
    NewMoveFileName = ?build_test_root_specified_path(Config, "new" ++ atom_to_list(?FUNCTION_NAME)),
    %%---------- file mv -----------
    ?assert(cdmi_internal:object_exists(FileName, Config)),
    ?assert(not cdmi_internal:object_exists(NewMoveFileName, Config)),
    ?assertEqual(FileData, cdmi_internal:get_file_content(FileName, Config), ?ATTEMPTS),

    RequestHeaders3 = [cdmi_test_utils:user_2_token_header(),
        ?CDMI_VERSION_HEADER, ?CDMI_OBJECT_CONTENT_TYPE_HEADER],
    RequestBody3 = json_utils:encode(#{<<"move">> => build_random_src_uri(FileName, FileGuid)}),
    ?assertMatch(
        {ok, _Code3, _Headers3, _Response3},
        cdmi_internal:do_request(?WORKERS, NewMoveFileName, put, RequestHeaders3, RequestBody3)
    ),
    ?assert(not cdmi_internal:object_exists(FileName, Config), ?ATTEMPTS),
    ?assert(cdmi_internal:object_exists(NewMoveFileName, Config), ?ATTEMPTS),
    ?assertEqual(FileData, cdmi_internal:get_file_content(NewMoveFileName, Config), ?ATTEMPTS).


move_dir_test(Config) ->
    #object{guid = DirGuid}  = onenv_file_test_utils:create_and_sync_file_tree(
        user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = atom_to_binary(?FUNCTION_NAME)
        }, Config#cdmi_test_config.p1_selector
    ),
    DirName = ?build_test_root_path(Config) ++ "/",
    NewMoveDirName = ?build_test_root_specified_path(Config, "new" ++ atom_to_list(?FUNCTION_NAME)) ++ "/",

    %%----------- dir mv -----------
    ?assert(cdmi_internal:object_exists(DirName, Config)),
    ?assert(not cdmi_internal:object_exists(NewMoveDirName, Config)),

    RequestHeaders2 = [cdmi_test_utils:user_2_token_header(),
        ?CDMI_VERSION_HEADER, ?CDMI_CONTAINER_CONTENT_TYPE_HEADER
    ],
    RequestBody2 = json_utils:encode(#{<<"move">> => build_random_src_uri(DirName, DirGuid)}),
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _Headers2, _Response2},
        cdmi_internal:do_request(?WORKERS, NewMoveDirName, put, RequestHeaders2, RequestBody2)
    ),

    ?assert(not cdmi_internal:object_exists(DirName, Config), ?ATTEMPTS),
    ?assert(cdmi_internal:object_exists(NewMoveDirName, Config), ?ATTEMPTS).


%% tests if cdmi returns 'moved permanently' code when we forget about '/' in path
moved_file_permanently_test(Config) ->
    WorkerP1 = oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
    CDMIEndpoint = cdmi_test_utils:get_cdmi_endpoint(Config),
    FileName = ?build_test_root_path(Config),
    FileNameWithSlash = FileName ++ "/",

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = atom_to_binary(?FUNCTION_NAME)
        },
        Config#cdmi_test_config.p1_selector
    ),
    %%--------- file test ----------
    RequestHeaders3 = [
        ?CDMI_OBJECT_CONTENT_TYPE_HEADER,
        ?CDMI_VERSION_HEADER,
        cdmi_test_utils:user_2_token_header()
    ],
    Location3 = list_to_binary(CDMIEndpoint ++ FileName),
    {ok, Code3, Headers3, _Response3} =
        cdmi_internal:do_request(WorkerP1, FileNameWithSlash, get, RequestHeaders3, []),
    ?assertEqual(?HTTP_302_FOUND, Code3),
    ?assertMatch(#{?HDR_LOCATION := Location3}, Headers3).


moved_dir_permanently_test(Config) ->
    WorkerP1 = oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
    CDMIEndpoint = cdmi_test_utils:get_cdmi_endpoint(Config),
    DirNameWithoutSlash = ?build_test_root_path(Config),
    DirName = DirNameWithoutSlash ++ "/",
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = atom_to_binary(?FUNCTION_NAME),
            children = [
                #file_spec{
                    name = <<"somefile.txt">>
                }
            ]
        }
        , Config#cdmi_test_config.p1_selector
    ),
    %%--------- dir test -----------
    RequestHeaders1 = [
        ?CDMI_CONTAINER_CONTENT_TYPE_HEADER,
        ?CDMI_VERSION_HEADER,
        cdmi_test_utils:user_2_token_header()
    ],
    Location1 = list_to_binary(CDMIEndpoint ++ DirName),
    {ok, Code1, Headers1, _Response1} =
        cdmi_internal:do_request(WorkerP1, DirNameWithoutSlash, get, RequestHeaders1, []),
    ?assertEqual(?HTTP_302_FOUND, Code1),
    ?assertMatch(#{?HDR_LOCATION := Location1}, Headers1).


moved_dir_with_QS_permanently_test(Config) ->
    WorkerP1 = oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
    CDMIEndpoint = cdmi_test_utils:get_cdmi_endpoint(Config),
    DirNameWithoutSlash = ?build_test_root_path(Config),
    DirName = DirNameWithoutSlash ++ "/",
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = atom_to_binary(?FUNCTION_NAME),
            children = [
                #file_spec{
                    name = <<"somefile.txt">>
                }
            ]
        }
        , Config#cdmi_test_config.p1_selector
    ),
    %%--------- dir test with QS-----------
    RequestHeaders2 = [
        ?CDMI_CONTAINER_CONTENT_TYPE_HEADER,
        ?CDMI_VERSION_HEADER,
        cdmi_test_utils:user_2_token_header()
    ],
    Location2 = list_to_binary(CDMIEndpoint ++ DirName ++ "?example_qs=1"),
    {ok, Code2, Headers2, _Response2} =
        cdmi_internal:do_request(
            WorkerP1, DirNameWithoutSlash ++ "?example_qs=1", get, RequestHeaders2, []
        ),
    ?assertEqual(?HTTP_302_FOUND, Code2),
    ?assertMatch(#{?HDR_LOCATION := Location2}, Headers2).


move_copy_conflict_test(Config) ->
    FileName = ?build_test_root_path(Config),
    FileUri = list_to_binary(filename:join("/", FileName)),
    FileData = <<"data">>,
    NewMoveFileName = ?build_test_root_specified_path(Config, "new" ++ atom_to_list(?FUNCTION_NAME)),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = atom_to_binary(?FUNCTION_NAME),
            content = <<"data">>
        }, Config#cdmi_test_config.p1_selector
    ),

    %%--- conflicting mv/cpy ------- (we cannot move and copy at the same time)
    ?assertEqual(FileData, cdmi_internal:get_file_content(FileName, Config), ?ATTEMPTS),

    RequestHeaders1 = [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER, ?CDMI_OBJECT_CONTENT_TYPE_HEADER],
    RequestBody1 = json_utils:encode(#{<<"move">> => FileUri, <<"copy">> => FileUri}),
    {ok, Code1, _Headers1, Response1} = cdmi_internal:do_request(
        ?WORKERS, NewMoveFileName, put, RequestHeaders1, RequestBody1
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_MALFORMED_DATA),
    ?assertMatch(ExpRestError, {Code1, json_utils:decode(Response1)}),
    ?assertEqual(FileData, cdmi_internal:get_file_content(FileName, Config), ?ATTEMPTS).
%%------------------------------

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
