%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This test suite verifies correct behaviour of posix and acl
%%% permissions with corresponding lfm (logical_file_manager) functions
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_permissions_test_SUITE).
-author("Bartosz Walkowicz").

-include("lfm_permissions_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/handshake_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/aai/caveats.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    caveats_test/1,
    mkdir_test/1,
    ls_test/1,
    readdir_plus_test/1,
    get_child_attr_test/1,
    mv_dir_test/1,
    rm_dir_test/1,

    create_file_test/1,
    open_for_read_test/1,
    open_for_write_test/1,
    open_for_rdwr_test/1,
    create_and_open_test/1,
    truncate_test/1,
    mv_file_test/1,
    rm_file_test/1,

    get_parent_test/1,
    get_file_path_test/1,
    get_file_guid_test/1,
    get_file_attr_test/1,
    get_file_distribution_test/1,

    set_perms_test/1,
    check_read_perms_test/1,
    check_write_perms_test/1,
    check_rdwr_perms_test/1,

    create_share_test/1,
    remove_share_test/1,

    get_acl_test/1,
    set_acl_test/1,
    remove_acl_test/1,

    get_transfer_encoding_test/1,
    set_transfer_encoding_test/1,
    get_cdmi_completion_status_test/1,
    set_cdmi_completion_status_test/1,
    get_mimetype_test/1,
    set_mimetype_test/1,

    get_metadata_test/1,
    set_metadata_test/1,
    remove_metadata_test/1,
    get_xattr_test/1,
    list_xattr_test/1,
    set_xattr_test/1,
    remove_xattr_test/1,

    permission_cache_test/1,
    expired_session_test/1
]).

all() ->
    ?ALL([
        caveats_test,
        mkdir_test,
        ls_test,
        readdir_plus_test,
        get_child_attr_test,
        mv_dir_test,
        rm_dir_test,

        create_file_test,
        open_for_read_test,
        open_for_write_test,
        open_for_rdwr_test,
        create_and_open_test,
        truncate_test,
        mv_file_test,
        rm_file_test,

        get_parent_test,
        get_file_path_test,
        get_file_guid_test,
        get_file_attr_test,
        get_file_distribution_test,

        set_perms_test,
        check_read_perms_test,
        check_write_perms_test,
        check_rdwr_perms_test,

        create_share_test,
        remove_share_test,

        get_acl_test,
        set_acl_test,
        remove_acl_test,

        get_transfer_encoding_test,
        set_transfer_encoding_test,
        get_cdmi_completion_status_test,
        set_cdmi_completion_status_test,
        get_mimetype_test,
        set_mimetype_test,

        get_metadata_test,
        set_metadata_test,
        remove_metadata_test,
        get_xattr_test,
        list_xattr_test,
        set_xattr_test,
        remove_xattr_test,

        permission_cache_test,
        expired_session_test
    ]).


-define(rpcCache(W, Function, Args), rpc:call(W, permissions_cache, Function, Args)).


%%%===================================================================
%%% Test functions
%%%===================================================================


caveats_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    Owner = <<"user1">>,
    OwnerUserSessId = ?config({session_id, {Owner, ?GET_DOMAIN(W)}}, Config),

    UserId = <<"user2">>,
    Identity = #user_identity{user_id = UserId},
    UserRootDir = fslogic_uuid:user_root_dir_guid(UserId),
    Space1RootDir = fslogic_uuid:spaceid_to_space_dir_guid(<<"space1">>),
    Space3RootDir = fslogic_uuid:spaceid_to_space_dir_guid(<<"space3">>),

    DirName = atom_to_binary(?FUNCTION_NAME, utf8),
    DirPath = <<"/space1/", DirName/binary>>,
    {ok, DirGuid} = ?assertMatch(
        {ok, _},
        lfm_proxy:mkdir(W, OwnerUserSessId, DirPath)
    ),
    {ok, DirObjectId} = file_id:guid_to_objectid(DirGuid),

    DirName2 = <<(atom_to_binary(?FUNCTION_NAME, utf8))/binary, "2">>,
    DirPath2 = <<"/space1/", DirName2/binary>>,
    {ok, _DirGuid2} = ?assertMatch(
        {ok, _},
        lfm_proxy:mkdir(W, OwnerUserSessId, DirPath2)
    ),

    [
        {Path1, ObjectId1, F1},
        {Path2, ObjectId2, F2},
        {Path3, ObjectId3, F3},
        {Path4, ObjectId4, F4},
        {Path5, ObjectId5, F5}
    ] = lists:map(fun(Num) ->
        FileName = <<"file", ($0 + Num)>>,
        FilePath = <<DirPath/binary, "/", FileName/binary>>,
        {ok, FileGuid} = ?assertMatch(
            {ok, _},
            lfm_proxy:create(W, OwnerUserSessId, FilePath, 8#777)
        ),
        {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
        {FilePath, FileObjectId, {FileGuid, FileName}}
    end, lists:seq(1, 5)),

    {ok, MainToken} = ?assertMatch({ok, _}, tokens:serialize(tokens:construct(#token{
        onezone_domain = <<"zone">>,
        subject = ?SUB(user, UserId),
        nonce = UserId,
        type = ?ACCESS_TOKEN,
        persistent = false
    }, UserId, []))),

    % Whitelisting Dir should result in listing all it's files
    Token1 = tokens:confine(MainToken, #cv_data_path{whitelist = [DirPath]}),
    SessId1 = lfm_permissions_test_utils:create_session(W, Identity, Token1),
    ?assertMatch(
        {ok, [F1, F2, F3, F4, F5]},
        lfm_proxy:ls(W, SessId1, {guid, DirGuid}, 0, 100)
    ),
    Token2 = tokens:confine(MainToken, #cv_data_objectid{whitelist = [DirObjectId]}),
    SessId2 = lfm_permissions_test_utils:create_session(W, Identity, Token2),
    ?assertMatch(
        {ok, [F1, F2, F3, F4, F5]},
        lfm_proxy:ls(W, SessId2, {guid, DirGuid}, 0, 100)
    ),

    % Whitelisting concrete files should result in listing only them
    Token3 = tokens:confine(MainToken, #cv_data_path{whitelist = [Path1, Path3, Path5]}),
    SessId3 = lfm_permissions_test_utils:create_session(W, Identity, Token3),
    ?assertMatch(
        {ok, [F1, F3, F5]},
        lfm_proxy:ls(W, SessId3, {guid, DirGuid}, 0, 100)
    ),
    Token4 = tokens:confine(MainToken, #cv_data_objectid{whitelist = [ObjectId1, ObjectId3, ObjectId5]}),
    SessId4 = lfm_permissions_test_utils:create_session(W, Identity, Token4),
    ?assertMatch(
        {ok, [F1, F3, F5]},
        lfm_proxy:ls(W, SessId4, {guid, DirGuid}, 0, 100)
    ),

    % Using several caveats should result in listing only their intersection
    Token5 = tokens:confine(MainToken, [
        #cv_data_path{whitelist = [Path1, Path3, Path4, Path5]},
        #cv_data_path{whitelist = [Path1, Path2, Path5]},
        #cv_data_path{whitelist = [Path1, Path5]}
    ]),
    SessId5 = lfm_permissions_test_utils:create_session(W, Identity, Token5),
    ?assertMatch(
        {ok, [F1, F5]},
        lfm_proxy:ls(W, SessId5, {guid, DirGuid}, 0, 100)
    ),
    Token6 = tokens:confine(MainToken, [
        #cv_data_objectid{whitelist = [ObjectId1, ObjectId3, ObjectId4, ObjectId5]},
        #cv_data_objectid{whitelist = [ObjectId1, ObjectId2, ObjectId5]},
        #cv_data_objectid{whitelist = [ObjectId1, ObjectId5]}
    ]),
    SessId6 = lfm_permissions_test_utils:create_session(W, Identity, Token6),
    ?assertMatch(
        {ok, [F1, F5]},
        lfm_proxy:ls(W, SessId6, {guid, DirGuid}, 0, 100)
    ),
    Token7 = tokens:confine(MainToken, [
        #cv_data_path{whitelist = [Path1, Path3, Path4, Path5]},
        #cv_data_objectid{whitelist = [ObjectId1, ObjectId2, ObjectId5]},
        #cv_data_path{whitelist = [Path1, Path5]}
    ]),
    SessId7 = lfm_permissions_test_utils:create_session(W, Identity, Token7),
    ?assertMatch(
        {ok, [F1, F5]},
        lfm_proxy:ls(W, SessId7, {guid, DirGuid}, 0, 100)
    ),
    Token8 = tokens:confine(MainToken, [
        #cv_data_objectid{whitelist = [ObjectId3, ObjectId4]},
        #cv_data_path{whitelist = [Path1, Path5]}
    ]),
    SessId8 = lfm_permissions_test_utils:create_session(W, Identity, Token8),
    ?assertMatch(
        {ok, []},
        lfm_proxy:ls(W, SessId8, {guid, DirGuid}, 0, 100)
    ),

    % Using caveat for different directory should result in {error, eacces}
    Token9 = tokens:confine(MainToken, #cv_data_path{whitelist = [<<"/space1/qwe">>]}),
    SessId9 = lfm_permissions_test_utils:create_session(W, Identity, Token9),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:ls(W, SessId9, {guid, DirGuid}, 0, 100)
    ),

    % With no caveats listing user root dir should list all user spaces
    SessId10 = lfm_permissions_test_utils:create_session(W, Identity, MainToken),
    ?assertMatch(
        {ok, [{Space1RootDir, <<"space1">>}, {Space3RootDir, <<"space3">>}]},
        lfm_proxy:ls(W, SessId10, {guid, UserRootDir}, 0, 100)
    ),
    % But with caveats user root dir ls should show only spaces leading to allowed files
    Token11 = tokens:confine(MainToken, #cv_data_path{whitelist = [DirPath]}),
    SessId11 = lfm_permissions_test_utils:create_session(W, Identity, Token11),
    ?assertMatch(
        {ok, [{Space1RootDir, <<"space1">>}]},
        lfm_proxy:ls(W, SessId11, {guid, UserRootDir}, 0, 100)
    ),

    % With no caveats listing space dir should list all space directories
    SessId12 = lfm_permissions_test_utils:create_session(W, Identity, MainToken),
    ?assertMatch(
        {ok, [_ | _]},
        lfm_proxy:ls(W, SessId12, {guid, Space1RootDir}, 0, 100)
    ),
    % And all operations on it and it's children should be allowed
    ?assertMatch(
        {ok, _},
        lfm_proxy:get_acl(W, SessId12, {guid, Space1RootDir})
    ),
    ?assertMatch(
        {ok, _},
        lfm_proxy:get_acl(W, SessId12, {guid, DirGuid})
    ),
    % But with caveats space ls should show only dirs leading to allowed files.
    Token13 = tokens:confine(MainToken, #cv_data_path{whitelist = [Path1]}),
    SessId13 = lfm_permissions_test_utils:create_session(W, Identity, Token13),
    ?assertMatch(
        {ok, [{DirGuid, DirName}]},
        lfm_proxy:ls(W, SessId13, {guid, Space1RootDir}, 0, 100)
    ),
    % On such dirs (ancestor) it should be possible to perform only certain
    % operations like ls, stat, resolve_guid, get_parent and resolve_path.
    ?assertMatch(
        {ok, [F1]},
        lfm_proxy:ls(W, SessId13, {guid, DirGuid}, 0, 100)
    ),
    ?assertMatch(
        {ok, #file_attr{name = DirName, type = ?DIRECTORY_TYPE}},
        lfm_proxy:stat(W, SessId13, {guid, DirGuid})
    ),
    ?assertMatch(
        {ok, DirGuid},
        lfm_proxy:resolve_guid(W, SessId13, DirPath)
    ),
    ?assertMatch(
        {ok, Space1RootDir},
        lfm_proxy:get_parent(W, SessId13, {guid, DirGuid})
    ),
    ?assertMatch(
        {ok, DirPath},
        lfm_proxy:get_file_path(W, SessId13, DirGuid)
    ),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:get_acl(W, SessId13, {guid, DirGuid})
    ),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:create(W, SessId13, DirGuid, <<"file1">>, 8#777)
    ),

    % Test listing with caveats and options (offset, limit)
    Token14 = tokens:confine(MainToken, #cv_data_path{whitelist = [Path1, Path2, Path4, Path5]}),
    SessId14 = lfm_permissions_test_utils:create_session(W, Identity, Token14),
    ?assertMatch(
        {ok, [F1, F2, F4]},
        lfm_proxy:ls(W, SessId14, {guid, DirGuid}, 0, 3)
    ),
    ?assertMatch(
        {ok, [F4, F5]},
        lfm_proxy:ls(W, SessId14, {guid, DirGuid}, 2, 3)
    ),
    ?assertMatch(
        {ok, [F4]},
        lfm_proxy:ls(W, SessId14, {guid, DirGuid}, 2, 1)
    ).


mkdir_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container, ?add_subcontainer]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, _ExtraData) ->
            lfm_proxy:mkdir(W, SessId, <<TestCaseRootDirPath/binary, "/dir1/dir2">>)
        end
    }, Config).


ls_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#dir{
            name = <<"dir1">>,
            perms = [?list_container]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            DirGuid = maps:get(DirPath, ExtraData),
            lfm_proxy:ls(W, SessId, {guid, DirGuid}, 0, 100)
        end
    }, Config).


readdir_plus_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container, ?list_container]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            DirGuid = maps:get(DirPath, ExtraData),
            lfm_proxy:read_dir_plus(W, SessId, {guid, DirGuid}, 0, 100)
        end
    }, Config).


get_child_attr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container],
            children = [#file{name = <<"file1">>}]
        }],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ParentDirGuid = maps:get(ParentDirPath, ExtraData),
            lfm_proxy:get_child_attr(W, SessId, ParentDirGuid, <<"file1">>)
        end
    }, Config).


mv_dir_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [
            #dir{
                name = <<"dir1">>,
                perms = [?traverse_container, ?delete_subcontainer],
                children = [
                    #dir{
                        name = <<"dir11">>,
                        perms = [?delete]
                    }
                ]
            },
            #dir{
                name = <<"dir2">>,
                perms = [?traverse_container, ?add_subcontainer]
            }
        ],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            SrcDirPath = <<TestCaseRootDirPath/binary, "/dir1/dir11">>,
            SrcDirGuid = maps:get(SrcDirPath, ExtraData),
            DstDirPath = <<TestCaseRootDirPath/binary, "/dir2">>,
            DstDirGuid = maps:get(DstDirPath, ExtraData),
            lfm_proxy:mv(W, SessId, {guid, SrcDirGuid}, {guid, DstDirGuid}, <<"dir21">>)
        end
    }, Config).


rm_dir_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [
            #dir{
                name = <<"dir1">>,
                perms = [?traverse_container, ?delete_subcontainer],
                children = [
                    #dir{
                        name = <<"dir2">>,
                        perms = [?delete, ?list_container]
                    }
                ]
            }
        ],
        posix_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1/dir2">>,
            DirGuid = maps:get(DirPath, ExtraData),
            lfm_proxy:unlink(W, SessId, {guid, DirGuid})
        end
    }, Config).


create_file_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container, ?add_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ParentDirGuid = maps:get(ParentDirPath, ExtraData),
            lfm_proxy:create(W, SessId, ParentDirGuid, <<"file1">>, 8#777)
        end
    }, Config).


open_for_read_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?read_object],
            on_create = fun(OwnerSessId, Guid) ->
                % write to file to force its creation on storage. Otherwise it
                % may be not possible during tests without necessary perms.
                fill_file_with_dummy_data(W, OwnerSessId, Guid),
                Guid
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:open(W, SessId, {guid, FileGuid}, read)
        end
    }, Config).


open_for_write_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?write_object],
            on_create = fun(OwnerSessId, Guid) ->
                % write to file to force its creation on storage. Otherwise it
                % may be not possible during tests without necessary perms.
                fill_file_with_dummy_data(W, OwnerSessId, Guid),
                Guid
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:open(W, SessId, {guid, FileGuid}, write)
        end
    }, Config).


open_for_rdwr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?read_object, ?write_object],
            on_create = fun(OwnerSessId, Guid) ->
                % write to file to force its creation on storage. Otherwise it
                % may be not possible during tests without necessary perms.
                fill_file_with_dummy_data(W, OwnerSessId, Guid),
                Guid
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:open(W, SessId, {guid, FileGuid}, rdwr)
        end
    }, Config).


create_and_open_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container, ?add_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ParentDirGuid = maps:get(ParentDirPath, ExtraData),
            lfm_proxy:create_and_open(W, SessId, ParentDirGuid, <<"file1">>, 8#777)
        end
    }, Config).


truncate_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?write_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:truncate(W, SessId, {guid, FileGuid}, 0)
        end
    }, Config).


mv_file_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [
            #dir{
                name = <<"dir1">>,
                perms = [?traverse_container, ?delete_object],
                children = [
                    #file{
                        name = <<"file11">>,
                        perms = [?delete]
                    }
                ]
            },
            #dir{
                name = <<"dir2">>,
                perms = [?traverse_container, ?add_object]
            }
        ],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            SrcFilePath = <<TestCaseRootDirPath/binary, "/dir1/file11">>,
            SrcFileGuid = maps:get(SrcFilePath, ExtraData),
            DstDirPath = <<TestCaseRootDirPath/binary, "/dir2">>,
            DstDirGuid = maps:get(DstDirPath, ExtraData),
            lfm_proxy:mv(W, SessId, {guid, SrcFileGuid}, {guid, DstDirGuid}, <<"file21">>)
        end
    }, Config).


rm_file_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [
            #dir{
                name = <<"dir1">>,
                perms = [?traverse_container, ?delete_object],
                children = [
                    #file{
                        name = <<"file1">>,
                        perms = [?delete]
                    }
                ]
            }
        ],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/dir1/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:unlink(W, SessId, {guid, FileGuid})
        end
    }, Config).


get_parent_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{name = <<"file1">>}],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:get_parent(W, SessId, {guid, FileGuid})
        end
    }, Config).


get_file_path_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{name = <<"file1">>}],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:get_file_path(W, SessId, FileGuid)
        end
    }, Config).


get_file_guid_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{name = <<"file1">>}],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, _ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            lfm_proxy:resolve_guid(W, SessId, FilePath)
        end
    }, Config).


get_file_attr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{name = <<"file1">>}],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:stat(W, SessId, {guid, FileGuid})
        end
    }, Config).


get_file_distribution_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?read_metadata]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:get_file_distribution(W, SessId, {guid, FileGuid})
        end
    }, Config).


set_perms_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    Owner = <<"user1">>,

    OwnerUserSessId = ?config({session_id, {Owner, ?GET_DOMAIN(W)}}, Config),
    GroupUserSessId = ?config({session_id, {<<"user2">>, ?GET_DOMAIN(W)}}, Config),
    OtherUserSessId = ?config({session_id, {<<"user3">>, ?GET_DOMAIN(W)}}, Config),

    DirPath = <<"/space1/dir1">>,
    {ok, DirGuid} = ?assertMatch(
        {ok, _},
        lfm_proxy:mkdir(W, OwnerUserSessId, DirPath)
    ),

    FilePath = <<"/space1/dir1/file1">>,
    {ok, FileGuid} = ?assertMatch(
        {ok, _},
        lfm_proxy:create(W, OwnerUserSessId, FilePath, 8#777)
    ),

    %% POSIX

    % owner can always change file perms if he has access to it
    lfm_permissions_test_utils:set_modes(W, #{DirGuid => 8#677, FileGuid => 8#777}),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(W, OwnerUserSessId, {guid, FileGuid}, 8#000)
    ),
    lfm_permissions_test_utils:set_modes(W, #{DirGuid => 8#100, FileGuid => 8#000}),
    ?assertMatch(ok, lfm_proxy:set_perms(W, OwnerUserSessId, {guid, FileGuid}, 8#000)),

    % other users from space can't change perms no matter what
    lfm_permissions_test_utils:set_modes(W, #{DirGuid => 8#777, FileGuid => 8#777}),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(W, GroupUserSessId, {guid, FileGuid}, 8#000)
    ),

    % users outside of space shouldn't even see the file
    lfm_permissions_test_utils:set_modes(W, #{DirGuid => 8#777, FileGuid => 8#777}),
    ?assertMatch(
        {error, ?ENOENT},
        lfm_proxy:set_perms(W, OtherUserSessId, {guid, FileGuid}, 8#000)
    ),

    %% ACL

    % owner can always change file perms if he has access to it
    lfm_permissions_test_utils:set_acls(W, #{
        DirGuid => ?ALL_DIR_PERMS -- [?traverse_container],
        FileGuid => ?ALL_FILE_PERMS
    }, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(W, OwnerUserSessId, {guid, FileGuid}, 8#000)
    ),

    lfm_permissions_test_utils:set_acls(W, #{
        DirGuid => [?traverse_container],
        FileGuid => []
    }, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(ok, lfm_proxy:set_perms(W, OwnerUserSessId, {guid, FileGuid}, 8#000)),

    % other users from space can't change perms no matter what
    lfm_permissions_test_utils:set_acls(W, #{
        DirGuid => ?ALL_DIR_PERMS,
        FileGuid => ?ALL_FILE_PERMS
    }, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(W, GroupUserSessId, {guid, FileGuid}, 8#000)
    ),

    % users outside of space shouldn't even see the file
    lfm_permissions_test_utils:set_acls(W, #{
        DirGuid => ?ALL_DIR_PERMS,
        FileGuid => ?ALL_FILE_PERMS
    }, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(
        {error, ?ENOENT},
        lfm_proxy:set_perms(W, OtherUserSessId, {guid, FileGuid}, 8#000)
    ).


check_read_perms_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?read_object]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:check_perms(W, SessId, {guid, FileGuid}, read)
        end
    }, Config).


check_write_perms_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?write_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:check_perms(W, SessId, {guid, FileGuid}, write)
        end
    }, Config).


check_rdwr_perms_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?read_object, ?write_object]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:check_perms(W, SessId, {guid, FileGuid}, rdwr)
        end
    }, Config).


create_share_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#dir{name = <<"dir1">>}],
        posix_requires_space_privs = [?SPACE_MANAGE_SHARES],
        acl_requires_space_privs = [?SPACE_MANAGE_SHARES],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            DirGuid = maps:get(DirPath, ExtraData),
            lfm_proxy:create_share(W, SessId, {guid, DirGuid}, <<"create_share">>)
        end
    }, Config).


remove_share_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#dir{
            name = <<"dir1">>,
            on_create = fun(OwnerSessId, Guid) ->
                {ok, {ShareId, _}} = ?assertMatch({ok, _}, lfm_proxy:create_share(
                    W, OwnerSessId, {guid, Guid}, <<"share_to_remove">>
                )),
                ShareId
            end
        }],
        posix_requires_space_privs = [?SPACE_MANAGE_SHARES],
        acl_requires_space_privs = [?SPACE_MANAGE_SHARES],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ShareId = maps:get(DirPath, ExtraData),
            lfm_proxy:remove_share(W, SessId, ShareId)
        end
    }, Config).


get_acl_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?read_acl]
        }],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:get_acl(W, SessId, {guid, FileGuid})
        end
    }, Config).


set_acl_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?write_acl]
        }],
        posix_requires_space_privs = owner,
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:set_acl(W, SessId, {guid, FileGuid}, [
                ?ALLOW_ACE(
                    ?group,
                    ?no_flags_mask,
                    lfm_permissions_test_utils:perms_to_bitmask(?ALL_FILE_PERMS)
                )
            ])
        end
    }, Config).


remove_acl_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?write_acl]
        }],
        posix_requires_space_privs = owner,
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:remove_acl(W, SessId, {guid, FileGuid})
        end
    }, Config).


get_transfer_encoding_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?read_attributes],
            on_create = fun(OwnerSessId, Guid) ->
                lfm_proxy:set_transfer_encoding(W, OwnerSessId, {guid, Guid}, <<"base64">>),
                Guid
            end
        }],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:get_transfer_encoding(W, SessId, {guid, FileGuid})
        end
    }, Config).


set_transfer_encoding_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?write_attributes]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:set_transfer_encoding(W, SessId, {guid, FileGuid}, <<"base64">>)
        end
    }, Config).


get_cdmi_completion_status_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?read_attributes],
            on_create = fun(OwnerSessId, Guid) ->
                lfm_proxy:set_cdmi_completion_status(W, OwnerSessId, {guid, Guid}, <<"Completed">>),
                Guid
            end
        }],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:get_cdmi_completion_status(W, SessId, {guid, FileGuid})
        end
    }, Config).


set_cdmi_completion_status_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?write_attributes]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:set_cdmi_completion_status(W, SessId, {guid, FileGuid}, <<"Completed">>)
        end
    }, Config).


get_mimetype_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?read_attributes],
            on_create = fun(OwnerSessId, Guid) ->
                lfm_proxy:set_mimetype(W, OwnerSessId, {guid, Guid}, <<"mimetype">>),
                Guid
            end
        }],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:get_mimetype(W, SessId, {guid, FileGuid})
        end
    }, Config).


set_mimetype_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?write_attributes]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:set_mimetype(W, SessId, {guid, FileGuid}, <<"mimetype">>)
        end
    }, Config).


get_metadata_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?read_metadata],
            on_create = fun(OwnerSessId, Guid) ->
                lfm_proxy:set_metadata(W, OwnerSessId, {guid, Guid}, json, <<"VAL">>, []),
                Guid
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:get_metadata(W, SessId, {guid, FileGuid}, json, [], false)
        end
    }, Config).


set_metadata_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?write_metadata]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:set_metadata(W, SessId, {guid, FileGuid}, json, <<"VAL">>, [])
        end
    }, Config).


remove_metadata_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?write_metadata],
            on_create = fun(OwnerSessId, Guid) ->
                lfm_proxy:set_metadata(W, OwnerSessId, {guid, Guid}, json, <<"VAL">>, []),
                Guid
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:remove_metadata(W, SessId, {guid, FileGuid}, json)
        end
    }, Config).


get_xattr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?read_metadata],
            on_create = fun(OwnerSessId, Guid) ->
                Xattr = #xattr{name = <<"myxattr">>, value = <<"VAL">>},
                lfm_proxy:set_xattr(W, OwnerSessId, {guid, Guid}, Xattr),
                Guid
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:get_xattr(W, SessId, {guid, FileGuid}, <<"myxattr">>)
        end
    }, Config).


list_xattr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            on_create = fun(OwnerSessId, Guid) ->
                Xattr = #xattr{name = <<"myxattr">>, value = <<"VAL">>},
                lfm_proxy:set_xattr(W, OwnerSessId, {guid, Guid}, Xattr),
                Guid
            end
        }],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:list_xattr(W, SessId, {guid, FileGuid}, false, false)
        end
    }, Config).


set_xattr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?write_metadata]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:set_xattr(
                W, SessId, {guid, FileGuid},
                #xattr{name = <<"myxattr">>, value = <<"VAL">>}
            )
        end
    }, Config).


remove_xattr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?write_metadata],
            on_create = fun(OwnerSessId, Guid) ->
                Xattr = #xattr{name = <<"myxattr">>, value = <<"VAL">>},
                lfm_proxy:set_xattr(W, OwnerSessId, {guid, Guid}, Xattr),
                Guid
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:remove_xattr(W, SessId, {guid, FileGuid}, <<"myxattr">>)
        end
    }, Config).


permission_cache_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    PermissionCacheStatusUuid = <<"status">>,

    case ?rpcCache(W, get, [PermissionCacheStatusUuid]) of
        {ok, #document{value = #permissions_cache{value = {permissions_cache_helper2, _}}}} ->
            ?assertEqual(ok, ?rpcCache(W, invalidate, [])),
            ?assertMatch({ok, #document{value = #permissions_cache{value = {permissions_cache_helper, permissions_cache_helper2}}}},
                ?rpcCache(W, get, [PermissionCacheStatusUuid]), 3);
        _ ->
            ok
    end,

    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p1])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p2])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p3])),

    ?assertMatch({ok, _}, ?rpcCache(W, cache_permission, [p1, ok])),
    ?assertEqual({ok, ok}, ?rpcCache(W, check_permission, [p1])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p2])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p3])),

    ?assertEqual(ok, ?rpcCache(W, invalidate, [])),
    ?assertMatch({ok, #document{value = #permissions_cache{value = {permissions_cache_helper2, _}}}},
        ?rpcCache(W, get, [PermissionCacheStatusUuid])),
    ?assertMatch({ok, _}, ?rpcCache(W, cache_permission, [p2, ok])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p1])),
    ?assertEqual({ok, ok}, ?rpcCache(W, check_permission, [p2])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p3])),

    ?assertMatch({ok, #document{value = #permissions_cache{value = {permissions_cache_helper2, permissions_cache_helper}}}},
        ?rpcCache(W, get, [PermissionCacheStatusUuid]), 2),
    ?assertEqual(ok, ?rpcCache(W, invalidate, [])),
    ?assertMatch({ok, #document{value = #permissions_cache{value = {permissions_cache_helper, _}}}},
        ?rpcCache(W, get, [PermissionCacheStatusUuid]), 2),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p1])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p2])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p3])),

    for(50, fun() -> ?assertEqual(ok, ?rpcCache(W, invalidate, [])) end),
    CheckFun = fun() ->
        case ?rpcCache(W, get, [PermissionCacheStatusUuid]) of
            {ok, #document{value = #permissions_cache{value = {permissions_cache_helper, permissions_cache_helper2}}}} ->
                ok;
            {ok, #document{value = #permissions_cache{value = {permissions_cache_helper2, permissions_cache_helper}}}} ->
                ok;
            Other ->
                Other
        end
    end,
    ?assertMatch(ok, CheckFun(), 10),
    ?assertMatch({ok, _}, ?rpcCache(W, cache_permission, [p1, xyz])),
    ?assertMatch({ok, _}, ?rpcCache(W, cache_permission, [p3, ok])),
    ?assertEqual({ok, xyz}, ?rpcCache(W, check_permission, [p1])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p2])),
    ?assertEqual({ok, ok}, ?rpcCache(W, check_permission, [p3])).


expired_session_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    {_, GUID} = ?assertMatch(
        {ok, _},
        lfm_proxy:create(W, SessId1, <<"/space1/es_file">>, 8#770)
    ),

    ok = rpc:call(W, session, delete, [SessId1]),

    % Verification
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:open(W, SessId1, {guid, GUID}, write)
    ).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        initializer:create_test_users_and_spaces(
            ?TEST_FILE(NewConfig2, "env_desc.json"),
            NewConfig2
        )
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(Config) ->
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:teardown_storage(Config).


init_per_testcase(Case, Config) when
    Case =:= create_share_test;
    Case =:= remove_share_test
->
    initializer:mock_share_logic(Config),
    init_per_testcase(default, Config);

init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).


end_per_testcase(Case, Config) when
    Case =:= create_share_test;
    Case =:= remove_share_test
->
    initializer:unmock_share_logic(Config),
    end_per_testcase(default, Config);

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec for(pos_integer(), term()) -> term().
for(1, F) ->
    F();
for(N, F) ->
    F(),
    for(N - 1, F).


-spec fill_file_with_dummy_data(node(), session:id(), file_id:file_guid()) -> ok.
fill_file_with_dummy_data(Node, SessId, Guid) ->
    {ok, FileHandle} = ?assertMatch(
        {ok, _},
        lfm_proxy:open(Node, SessId, {guid, Guid}, write)
    ),
    ?assertMatch({ok, 4}, lfm_proxy:write(Node, FileHandle, 0, <<"DATA">>)),
    ?assertMatch(ok, lfm_proxy:fsync(Node, FileHandle)),
    ?assertMatch(ok, lfm_proxy:close(Node, FileHandle)).
