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

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
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


-define(rpc(W, Module, Function, Args), rpc:call(W, Module, Function, Args)).
-define(rpcCache(W, Function, Args), rpc:call(W, permissions_cache, Function, Args)).

-define(ALL_PERMS, [
    ?read_object,
    ?list_container,
    ?write_object,
    ?add_object,
    ?add_subcontainer,
    ?read_metadata,
    ?write_metadata,
    ?traverse_container,
    ?delete_object,
    ?delete_subcontainer,
    ?read_attributes,
    ?write_attributes,
    ?delete,
    ?read_acl,
    ?write_acl
]).
-define(DIR_SPECIFIC_PERMS, [
    ?list_container,
    ?add_object,
    ?add_subcontainer,
    ?traverse_container,
    ?delete_object,
    ?delete_subcontainer
]).
-define(FILE_SPECIFIC_PERMS, [
    ?read_object,
    ?write_object
]).
-define(ALL_FILE_PERMS, (?ALL_PERMS -- ?DIR_SPECIFIC_PERMS)).
-define(ALL_DIR_PERMS, (?ALL_PERMS -- ?FILE_SPECIFIC_PERMS)).

-define(ALL_POSIX_PERMS, [read, write, exec]).


-define(ALLOW_ACE(__IDENTIFIER, __FLAGS, __MASK), #access_control_entity{
    acetype = ?allow_mask,
    identifier = __IDENTIFIER,
    aceflags = __FLAGS,
    acemask = __MASK
}).


-define(DENY_ACE(__IDENTIFIER, __FLAGS, __MASK), #access_control_entity{
    acetype = ?deny_mask,
    aceflags = __FLAGS,
    identifier = __IDENTIFIER,
    acemask = __MASK
}).


-record(file, {
    % name of file
    name :: binary(),
    % permissions needed to perform #test_spec.operation
    perms = [] :: [Perms :: binary()],
    % function called during environment setup. Term returned will be stored in `ExtraData`
    % and can be used during test (described in `operation` of #test_spec{}).
    on_create = undefined :: undefined | fun((OwnerSessId :: session:id(), file_id:file_guid()) -> term())
}).


-record(dir, {
    % name of directory
    name :: binary(),
    % permissions needed to perform #test_spec.operation
    perms = [] :: [Perms :: binary()],
    % function called during environment setup. Term returned will be stored in `ExtraData`
    % and can be used during test (described in `operation` of #test_spec{}).
    on_create = undefined :: undefined | fun((session:id(), file_id:file_guid()) -> term()),
    % children of directory if needed
    children = [] :: [#dir{} | #file{}]
}).


-record(test_spec, {
    % Id of space within which test will be carried
    space_id = <<"space1">> :: binary(),

    % Name of root dir for test
    root_dir :: binary(),

    % Id of user belonging to space specified in `space_id` in context
    % of which all files required for tests will be created. It will be
    % used to test `user` posix bits and `OWNER@` special acl identifier
    owner_user = <<"user1">> :: binary(),

    % Id of user belonging to space specified in `space_id` which aren't
    % the same as `owner_user`. It will be used to test `group` posix bits
    % and acl for his Id.
    space_user = <<"user2">> :: binary(),

    % Id of group to which belongs `space_user` and which itself belong to
    % `space_id`. It will be used to test acl group identifier.
    space_user_group = <<"group2">> :: binary(),

    % Id of user not belonging to space specified in `space_id`. It will be
    % used to test `other` posix bits and `EVERYONE@` special acl identifier.
    other_user = <<"user3">> :: binary(),

    % Tells whether `operation` needs `traverse_ancestors` permission. If so
    % `traverse_container` perm will be added to test root dir as needed perm
    % to perform `operation` (since traverse_ancestors means that one can
    % traverse dirs up to file in question).
    requires_traverse_ancestors = true :: boolean(),

    % Tells which space privileges are needed to perform `operation`
    % in case of posix access mode
    posix_requires_space_privs = [] :: owner | [privileges:space_privilege()],

    % Tells which space privileges are needed to perform `operation`
    % in case of acl access mode
    acl_requires_space_privs = [] :: owner | [privileges:space_privilege()],

    % Description of environment (files and permissions on them) needed to
    % perform `operation`.
    files :: [#dir{} | #file{}],

    % Operation being tested. It will be called for various combinations of
    % either posix or acl permissions. It is expected to fail for combinations
    % not having all perms specified in `files` and space privileges and
    % succeed for combination consisting of only them.
    % It takes following arguments:
    % - OwnerSessId - session id of user which creates files for this test,
    % - SessId - session id of user which should perform operation,
    % - TestCaseRootDirPath - absolute path to root dir of testcase,
    % - ExtraData - mapping of file path (for every file specified in `files`) to
    %               term returned from `on_create` #dir{} or #file{} fun.
    %               If mentioned fun is left undefined then by default GUID will
    %               be used.
    operation :: fun((OwnerSessId :: binary(), SessId :: binary(), TestCaseRootDirPath :: binary(), ExtraData :: map()) ->
        ok |
        {ok, term()} |
        {ok, term(), term()} |
        {ok, term(), term(), term()} |
        {error, term()}
    )
}).


%%%===================================================================
%%% Test functions
%%%===================================================================


mkdir_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{name = <<"file1">>}],
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, _ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            lfm_proxy:resolve_guid(W, SessId, FilePath)
        end
    }, Config).


get_file_attr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, OwnerUserSessId, DirPath)),

    FilePath = <<"/space1/dir1/file1">>,
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, OwnerUserSessId, FilePath, 8#777)),

    %% POSIX

    % owner can always change file perms if he has access to it
    set_modes(W, #{DirGuid => 8#677, FileGuid => 8#777}),
    ?assertMatch({error, ?EACCES}, lfm_proxy:set_perms(W, OwnerUserSessId, {guid, FileGuid}, 8#000)),
    set_modes(W, #{DirGuid => 8#100, FileGuid => 8#000}),
    ?assertMatch(ok, lfm_proxy:set_perms(W, OwnerUserSessId, {guid, FileGuid}, 8#000)),

    % other users from space can't change perms no matter what
    set_modes(W, #{DirGuid => 8#777, FileGuid => 8#777}),
    ?assertMatch({error, ?EACCES}, lfm_proxy:set_perms(W, GroupUserSessId, {guid, FileGuid}, 8#000)),

    % users outside of space shouldn't even see the file
    set_modes(W, #{DirGuid => 8#777, FileGuid => 8#777}),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:set_perms(W, OtherUserSessId, {guid, FileGuid}, 8#000)),

    %% ACL

    % owner can always change file perms if he has access to it
    set_acls(W, #{DirGuid => ?ALL_DIR_PERMS -- [?traverse_container], FileGuid => ?ALL_FILE_PERMS}, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch({error, ?EACCES}, lfm_proxy:set_perms(W, OwnerUserSessId, {guid, FileGuid}, 8#000)),
    set_acls(W, #{DirGuid => [?traverse_container], FileGuid => []}, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(ok, lfm_proxy:set_perms(W, OwnerUserSessId, {guid, FileGuid}, 8#000)),

    % other users from space can't change perms no matter what
    set_acls(W, #{DirGuid => ?ALL_DIR_PERMS, FileGuid => ?ALL_FILE_PERMS}, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch({error, ?EACCES}, lfm_proxy:set_perms(W, GroupUserSessId, {guid, FileGuid}, 8#000)),

    % users outside of space shouldn't even see the file
    set_acls(W, #{DirGuid => ?ALL_DIR_PERMS, FileGuid => ?ALL_FILE_PERMS}, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:set_perms(W, OtherUserSessId, {guid, FileGuid}, 8#000)).


check_read_perms_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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
                ?ALLOW_ACE(?group, ?no_flags_mask, perms_to_bitmask(?ALL_FILE_PERMS))
            ])
        end
    }, Config).


remove_acl_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?read_metadata],
            on_create = fun(OwnerSessId, Guid) ->
                lfm_proxy:set_xattr(W, OwnerSessId, {guid, Guid}, #xattr{name = <<"myxattr">>, value = <<"VAL">>}),
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

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            on_create = fun(OwnerSessId, Guid) ->
                lfm_proxy:set_xattr(W, OwnerSessId, {guid, Guid}, #xattr{name = <<"myxattr">>, value = <<"VAL">>}),
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

    run_tests(W, #test_spec{
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

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#file{
            name = <<"file1">>,
            perms = [?write_metadata],
            on_create = fun(OwnerSessId, Guid) ->
                lfm_proxy:set_xattr(W, OwnerSessId, {guid, Guid}, #xattr{name = <<"myxattr">>, value = <<"VAL">>}),
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


-define(PERMISSION_CACHE_STATUS_UUID, <<"status">>).
permission_cache_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    case ?rpcCache(W, get, [?PERMISSION_CACHE_STATUS_UUID]) of
        {ok, #document{value = #permissions_cache{value = {permissions_cache_helper2, _}}}} ->
            ?assertEqual(ok, ?rpcCache(W, invalidate, [])),
            ?assertMatch({ok, #document{value = #permissions_cache{value = {permissions_cache_helper, permissions_cache_helper2}}}},
                ?rpcCache(W, get, [?PERMISSION_CACHE_STATUS_UUID]), 3);
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
        ?rpcCache(W, get, [?PERMISSION_CACHE_STATUS_UUID])),
    ?assertMatch({ok, _}, ?rpcCache(W, cache_permission, [p2, ok])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p1])),
    ?assertEqual({ok, ok}, ?rpcCache(W, check_permission, [p2])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p3])),

    ?assertMatch({ok, #document{value = #permissions_cache{value = {permissions_cache_helper2, permissions_cache_helper}}}},
        ?rpcCache(W, get, [?PERMISSION_CACHE_STATUS_UUID]), 2),
    ?assertEqual(ok, ?rpcCache(W, invalidate, [])),
    ?assertMatch({ok, #document{value = #permissions_cache{value = {permissions_cache_helper, _}}}},
        ?rpcCache(W, get, [?PERMISSION_CACHE_STATUS_UUID]), 2),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p1])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p2])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p3])),

    for(50, fun() -> ?assertEqual(ok, ?rpcCache(W, invalidate, [])) end),
    CheckFun = fun() ->
        case ?rpcCache(W, get, [?PERMISSION_CACHE_STATUS_UUID]) of
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
    {_, GUID} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/space1/es_file">>, 8#770)),

    ok = rpc:call(W, session, delete, [SessId1]),

    % Verification
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(W, SessId1, {guid, GUID}, write)).


%%%===================================================================
%%% TEST MECHANISM
%%%===================================================================


run_tests(Node, #test_spec{
    space_id = SpaceId,
    owner_user = Owner,
    root_dir = RootDir
} = Spec, Config) ->
    OwnerSessId = ?config({session_id, {Owner, ?GET_DOMAIN(Node)}}, Config),

    {ok, SpaceName} = rpc:call(Node, space_logic, get_name, [?ROOT_SESS_ID, SpaceId]),
    RootDirPath = <<"/", SpaceName/binary, "/", RootDir/binary>>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, OwnerSessId, RootDirPath, 8#777)),

    run_space_priv_tests(Node, RootDirPath, Spec, Config),
    run_posix_tests(Node, RootDirPath, Spec, Config),
    run_acl_tests(Node, RootDirPath, Spec, Config).


%%%===================================================================
%%% POSIX TESTS MECHANISM
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Tests space permissions needed to perform #test_spec.operation.
%% It will setup environment, add full posix or acl permissions and
%% assert that without space priv operation cannot be performed and
%% with it it succeeds.
%% @end
%%--------------------------------------------------------------------
run_space_priv_tests(Node, RootDirPath, #test_spec{
    space_id = SpaceId,
    owner_user = Owner,
    space_user = User,
    requires_traverse_ancestors = RequiresTraverseAncestors,
    posix_requires_space_privs = PosixSpacePrivs,
    acl_requires_space_privs = AclSpacePrivs,
    files = Files,
    operation = Fun
}, Config) ->
    OwnerUserSessId = ?config({session_id, {Owner, ?GET_DOMAIN(Node)}}, Config),

    TestCaseRootDirPerms = case RequiresTraverseAncestors of
        true -> [?traverse_container];
        false -> []
    end,
    lists:foreach(fun({RequiredPriv, Type, TestCaseRootDirName}) ->
        {PermsPerFile, ExtraData} = create_files(
            Node, OwnerUserSessId, RootDirPath, #dir{
                name = TestCaseRootDirName,
                perms = TestCaseRootDirPerms,
                children = Files
            }
        ),
        run_space_priv_test(
            Node, SpaceId, Owner, User, PermsPerFile, Type, Fun,
            <<RootDirPath/binary, "/", TestCaseRootDirName/binary>>,
            ExtraData, RequiredPriv, Config
        )
    end, [
        {PosixSpacePrivs, posix, <<"space_priv_posix">>},
        {AclSpacePrivs, acl, <<"space_priv_acl">>}
    ]).


run_space_priv_test(
    Node, SpaceId, Owner, SpaceUser, PermsPerFile, posix,
    Operation, TestCaseRootDirPath, ExtraData, RequiredPrivs, Config
) ->
    AllPosixPermsGranted = maps:map(fun(_, _) -> 8#777 end, PermsPerFile),
    set_modes(Node, AllPosixPermsGranted),

    run_space_priv_test(
        Node, SpaceId, Owner, SpaceUser, posix, Operation,
        TestCaseRootDirPath, ExtraData, RequiredPrivs, Config
    );

run_space_priv_test(
    Node, SpaceId, Owner, SpaceUser, PermsPerFile, acl,
    Operation, TestCaseRootDirPath, ExtraData, RequiredPrivs, Config
) ->
    AllAclPermsGranted = maps:map(fun(Guid, _) ->
        all_perms(Node, Guid)
    end, PermsPerFile),
    set_acls(Node, AllAclPermsGranted, #{}, ?everyone, ?no_flags_mask),

    run_space_priv_test(
        Node, SpaceId, Owner, SpaceUser, acl, Operation,
        TestCaseRootDirPath, ExtraData, RequiredPrivs, Config
    ).


run_space_priv_test(
    Node, SpaceId, Owner, SpaceUser, TestType, Operation,
    TestCaseRootDirPath, ExtraData, RequiredPrivs, Config
) ->
    try
        check_space_priv_requirement(
            Node, SpaceId, Owner, SpaceUser,
            Operation, TestCaseRootDirPath, ExtraData,
            RequiredPrivs, Config
        )
    catch T:R ->
        ct:pal(
            "SPACE PRIV TEST FAILURE~n"
            "   Type: ~p~n"
            "   User: ~p~n"
            "   Root path: ~p~n"
            "   Required space priv: ~p~n"
            "   Stacktrace: ~p~n",
            [
                TestType, SpaceUser,
                TestCaseRootDirPath,
                RequiredPrivs,
                erlang:get_stacktrace()
            ]
        ),
        erlang:T(R)
    after
        initializer:testmaster_mock_space_user_privileges(
            [Node], SpaceId, Owner, privileges:space_privileges()
        ),
        initializer:testmaster_mock_space_user_privileges(
            [Node], SpaceId, SpaceUser, privileges:space_privileges()
        )
    end.


check_space_priv_requirement(
    Node, SpaceId, OwnerId, _, Operation,
    RootDirPath, ExtraData, owner, Config
) ->
    OwnerSessId = ?config({session_id, {OwnerId, ?GET_DOMAIN(Node)}}, Config),

    % invalidate permission cache as it is not done due to initializer mocks
    rpc:call(Node, permissions_cache, invalidate, []),
    initializer:testmaster_mock_space_user_privileges([Node], SpaceId, OwnerId, []),

    ?assertNotMatch(
        {error, _},
        Operation(OwnerSessId, OwnerSessId, RootDirPath, ExtraData)
    );
check_space_priv_requirement(
    Node, SpaceId, OwnerId, UserId, Operation,
    RootDirPath, ExtraData, [], Config
) ->
    UserSessId = ?config({session_id, {UserId, ?GET_DOMAIN(Node)}}, Config),
    OwnerSessId = ?config({session_id, {OwnerId, ?GET_DOMAIN(Node)}}, Config),

    % invalidate permission cache as it is not done due to initializer mocks
    rpc:call(Node, permissions_cache, invalidate, []),

    initializer:testmaster_mock_space_user_privileges([Node], SpaceId, UserId, []),
    ?assertNotMatch(
        {error, _},
        Operation(OwnerSessId, UserSessId, RootDirPath, ExtraData)
    );
check_space_priv_requirement(
    Node, SpaceId, OwnerId, UserId, Operation,
    RootDirPath, ExtraData, RequiredPrivs, Config
) ->
    AllSpacePrivs = privileges:space_privileges(),
    UserSessId = ?config({session_id, {UserId, ?GET_DOMAIN(Node)}}, Config),
    OwnerSessId = ?config({session_id, {OwnerId, ?GET_DOMAIN(Node)}}, Config),

    % If operation requires space priv it should fail without it and succeed with it
    initializer:testmaster_mock_space_user_privileges(
        [Node], SpaceId, UserId, AllSpacePrivs -- RequiredPrivs
    ),
    ?assertMatch(
        {error, ?EACCES},
        Operation(OwnerSessId, UserSessId, RootDirPath, ExtraData)
    ),

    % invalidate permission cache as it is not done due to initializer mocks
    rpc:call(Node, permissions_cache, invalidate, []),

    initializer:testmaster_mock_space_user_privileges(
        [Node], SpaceId, UserId, RequiredPrivs
    ),
    ?assertNotMatch(
        {error, _},
        Operation(OwnerSessId, UserSessId, RootDirPath, ExtraData)
    ).


%%%===================================================================
%%% POSIX TESTS MECHANISM
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Tests posix permissions needed to perform #test_spec.operation.
%% For each bits group (`owner`, `group`, `other`) it will setup
%% environment and test combination of posix perms. It will assert
%% that combinations not having all required perms fails and that
%% combination consisting of only required perms succeeds.
%% @end
%%--------------------------------------------------------------------
run_posix_tests(Node, RootDirPath, #test_spec{
    owner_user = Owner,
    space_user = User,
    other_user = OtherUser,
    requires_traverse_ancestors = RequiresTraverseAncestors,
    files = Files,
    operation = Operation
}, Config) ->
    OwnerUserSessId = ?config({session_id, {Owner, ?GET_DOMAIN(Node)}}, Config),
    GroupUserSessId = ?config({session_id, {User, ?GET_DOMAIN(Node)}}, Config),
    OtherUserSessId = ?config({session_id, {OtherUser, ?GET_DOMAIN(Node)}}, Config),

    TestCaseRootDirPerms = case RequiresTraverseAncestors of
        true -> [?traverse_container];
        false -> []
    end,
    lists:foreach(fun({SessId, Type, TestCaseRootDirName}) ->
        {PermsPerFile, ExtraData} = create_files(
            Node, OwnerUserSessId, RootDirPath, #dir{
                name = TestCaseRootDirName,
                perms = TestCaseRootDirPerms,
                children = Files
            }
        ),
        PosixPermsPerFile = maps:map(fun(_, Perms) ->
            lists:usort(lists:flatmap(fun perm_to_posix_perms/1, Perms))
        end, PermsPerFile),

        run_posix_tests(
            Node, OwnerUserSessId, SessId,
            <<RootDirPath/binary, "/", TestCaseRootDirName/binary>>,
            Operation, PosixPermsPerFile, ExtraData, Type
        )
    end, [
        {OwnerUserSessId, owner, <<"owner_posix">>},
        {GroupUserSessId, group, <<"group_posix">>},
        {OtherUserSessId, other, <<"other_posix">>}
    ]).


run_posix_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath,
    Operation, PosixPermsPerFile, ExtraData, Type
) ->
    {ComplementaryPosixPermsPerFile, RequiredPosixPerms} = maps:fold(
        fun(FileGuid, FileRequiredPerms, {BasePermsPerFileAcc, RequiredPermsAcc}) ->
            {
                BasePermsPerFileAcc#{FileGuid => ?ALL_POSIX_PERMS -- FileRequiredPerms},
                [{FileGuid, Perm} || Perm <- FileRequiredPerms] ++ RequiredPermsAcc
            }
        end,
        {#{}, []},
        PosixPermsPerFile
    ),

    try
        run_posix_tests(
            Node, OwnerSessId, SessId, TestCaseRootDirPath,
            Operation, ComplementaryPosixPermsPerFile,
            RequiredPosixPerms, ExtraData, Type
        )
    catch T:R ->
        FilePathsToRequiredPerms = maps:fold(fun(Guid, RequiredPerms, Acc) ->
            lfm_proxy:set_perms(Node, ?ROOT_SESS_ID, {guid, Guid}, 8#777),
            case lfm_proxy:get_file_path(Node, OwnerSessId, Guid) of
                {ok, Path} -> Acc#{Path => RequiredPerms};
                _ -> Acc#{Guid => RequiredPerms}
            end
        end, #{}, PosixPermsPerFile),

        ct:pal(
            "POSIX TESTS FAILURE~n"
            "   Type: ~p~n"
            "   Root path: ~p~n"
            "   Required Perms: ~p~n"
            "   Stacktrace: ~p~n",
            [
                Type, TestCaseRootDirPath,
                FilePathsToRequiredPerms,
                erlang:get_stacktrace()
            ]
        ),
        erlang:T(R)
    end.


run_posix_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath, Operation,
    ComplementaryPermsPerFile, AllRequiredPerms, ExtraData, owner
) ->
    RequiredPermsWithoutOwnership = lists:filter(fun({_, Perm}) ->
        Perm == read orelse Perm == write orelse Perm == exec
    end, AllRequiredPerms),

    case RequiredPermsWithoutOwnership of
        [] ->
            % If operation requires only ownership then it should succeed
            % even if all files modes are set to 0
            set_modes(Node, maps:map(fun(_, _) -> 0 end, ComplementaryPermsPerFile)),
            ?assertNotMatch(
                {error, _},
                Operation(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
            );
        _ ->
            run_standard_posix_tests(
                Node, OwnerSessId, SessId, TestCaseRootDirPath,
                Operation, ComplementaryPermsPerFile, RequiredPermsWithoutOwnership,
                ExtraData, owner
            )
    end;

run_posix_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath, Operation,
    ComplementaryPermsPerFile, AllRequiredPerms, ExtraData, group
) ->
    OperationRequiresOwnership = lists:any(fun({_, Perm}) ->
        Perm == owner
    end, AllRequiredPerms),

    case OperationRequiresOwnership of
        true ->
            % If operation requires ownership then for group member it should failed
            % even if all files modes are set to 777
            set_modes(Node, maps:map(fun(_, _) -> 8#777 end, ComplementaryPermsPerFile)),
            ?assertMatch(
                {error, ?EACCES},
                Operation(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
            );
        false ->
            RequiredNormalPosixPerms = lists:filter(fun({_, Perm}) ->
                Perm == read orelse Perm == write orelse Perm == exec
            end, AllRequiredPerms),
            run_standard_posix_tests(
                Node, OwnerSessId, SessId, TestCaseRootDirPath,
                Operation, ComplementaryPermsPerFile, RequiredNormalPosixPerms, ExtraData, group
            )
    end;

% Users not belonging to space or unauthorized should not be able to conduct any operation
run_posix_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath, Operation,
    ComplementaryPermsPerFile, _AllRequiredPerms, ExtraData, other
) ->
    set_modes(Node, maps:map(fun(_, _) -> 8#777 end, ComplementaryPermsPerFile)),
    ?assertMatch(
        {error, _},
        Operation(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
    ),
    ?assertMatch(
        {error, _},
        Operation(OwnerSessId, ?GUEST_SESS_ID, TestCaseRootDirPath, ExtraData)
    ).


run_standard_posix_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath, Operation,
    ComplementaryPermsPerFile, AllRequiredPerms, ExtraData, Type
) ->
    AllRequiredModes = lists:map(fun({Guid, PosixPerm}) ->
        {Guid, posix_perm_to_mode(PosixPerm, Type)}
    end, AllRequiredPerms),
    ComplementaryModesPerFile = maps:map(fun(_, Perms) ->
        lists:foldl(fun(Perm, Acc) ->
            Acc bor posix_perm_to_mode(Perm, Type)
        end, 0, Perms)
    end, ComplementaryPermsPerFile),

    [RequiredModesComb | EaccesModesCombs] = combinations(AllRequiredModes),

    % Granting all modes but required ones should result in eacces
    lists:foreach(fun(EaccessModeComb) ->
        EaccesModesPerFile = lists:foldl(fun({Guid, Mode}, Acc) ->
            Acc#{Guid => Mode bor maps:get(Guid, Acc)}
        end, ComplementaryModesPerFile, EaccessModeComb),
        set_modes(Node, EaccesModesPerFile),
        ?assertMatch(
            {error, ?EACCES},
            Operation(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
        )
    end, EaccesModesCombs),

    % Granting only required modes should result in success
    RequiredModesPerFile = lists:foldl(fun({Guid, Mode}, Acc) ->
        Acc#{Guid => Mode bor maps:get(Guid, Acc, 0)}
    end, #{}, RequiredModesComb),
    set_modes(Node, RequiredModesPerFile),
    ?assertNotMatch(
        {error, _},
        Operation(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
    ).


set_modes(Node, ModePerFile) ->
    maps:fold(fun(Guid, Mode, _) ->
        ?assertEqual(ok, lfm_proxy:set_perms(
            Node, ?ROOT_SESS_ID, {guid, Guid}, Mode
        ))
    end, ok, ModePerFile).


%%%===================================================================
%%% ACL TESTS MECHANISM
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Tests acl permissions needed to perform #test_spec.operation.
%% For each type (`allow`, `deny`) and identifier (`OWNER@`, user_id,
%% group_id, `EVERYONE@`) it will setup environment and test combination
%% of acl perms. It will assert that combinations not having all required
%% perms fails and that combination consisting of only required perms succeeds.
%% @end
%%--------------------------------------------------------------------
run_acl_tests(Node, RootDirPath, #test_spec{
    owner_user = OwnerUser,
    space_user = SpaceUser,
    space_user_group = SpaceUserGroup,
    requires_traverse_ancestors = RequiresTraverseAncestors,
    files = Files,
    operation = Operation
}, Config) ->
    OwnerSessId = ?config({session_id, {OwnerUser, ?GET_DOMAIN(Node)}}, Config),
    UserSessId = ?config({session_id, {SpaceUser, ?GET_DOMAIN(Node)}}, Config),

    TestCaseRootDirPerms = case RequiresTraverseAncestors of
        true -> [?traverse_container];
        false -> []
    end,
    lists:foreach(fun({SessId, Type, TestCaseRootDirName, AceWho, AceFlags}) ->
        {PermsPerFile, ExtraData} = create_files(
            Node, OwnerSessId, RootDirPath, #dir{
                name = TestCaseRootDirName,
                perms = TestCaseRootDirPerms,
                children = Files
            }
        ),
        run_acl_tests(
            Node, OwnerSessId, SessId,
            <<RootDirPath/binary, "/", TestCaseRootDirName/binary>>,
            Operation, PermsPerFile, ExtraData, AceWho, AceFlags, Type
        )
    end, [
        {OwnerSessId, allow, <<"owner_acl_allow">>, ?owner, ?no_flags_mask},
        {UserSessId, allow, <<"user_acl_allow">>, SpaceUser, ?no_flags_mask},
        {UserSessId, allow, <<"user_group_acl_allow">>, SpaceUserGroup, ?identifier_group_mask},
        {UserSessId, allow, <<"everyone_acl_allow">>, ?everyone, ?no_flags_mask},

        {OwnerSessId, deny, <<"owner_acl_deny">>, ?owner, ?no_flags_mask},
        {UserSessId, deny, <<"user_acl_deny">>, SpaceUser, ?no_flags_mask},
        {UserSessId, deny, <<"user_group_acl_deny">>, SpaceUserGroup, ?identifier_group_mask},
        {UserSessId, deny, <<"everyone_acl_deny">>, ?everyone, ?no_flags_mask}
    ]).


run_acl_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath,
    Operation, RequiredPermsPerFile, ExtraData, AceWho, AceFlags, Type
) ->
    {ComplementaryPermsPerFile, AllRequiredPerms} = maps:fold(
        fun(FileGuid, FileRequiredPerms, {BasePermsPerFileAcc, RequiredPermsAcc}) ->
            {
                BasePermsPerFileAcc#{FileGuid => complementary_perms(
                    Node, FileGuid, FileRequiredPerms
                )},
                [{FileGuid, Perm} || Perm <- FileRequiredPerms] ++ RequiredPermsAcc
            }
        end,
        {#{}, []},
        RequiredPermsPerFile
    ),

    try
        run_acl_tests(
            Node, OwnerSessId, SessId, TestCaseRootDirPath,
            Operation, ComplementaryPermsPerFile, AllRequiredPerms,
            ExtraData, AceWho, AceFlags, Type
        )
    catch T:R ->
        RequiredPermsPerFileMap = maps:fold(fun(Guid, RequiredPerms, Acc) ->
            lfm_proxy:set_perms(Node, ?ROOT_SESS_ID, {guid, Guid}, 8#777),
            case lfm_proxy:get_file_path(Node, OwnerSessId, Guid) of
                {ok, Path} -> Acc#{Path => RequiredPerms};
                _ -> Acc#{Guid => RequiredPerms}
            end
        end, #{}, RequiredPermsPerFile),

        ct:pal(
            "ACL TESTS FAILURE~n"
            "   Type: ~p~n"
            "   Root path: ~p~n"
            "   Required Perms: ~p~n"
            "   Identifier: ~p~n"
            "   Is group identifier: ~p~n"
            "   Stacktrace: ~p~n",
            [
                Type, TestCaseRootDirPath,
                RequiredPermsPerFileMap,
                AceWho, AceFlags == ?identifier_group_mask,
                erlang:get_stacktrace()
            ]
        ),
        erlang:T(R)
    end.


run_acl_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath, Operation,
    ComplementaryPermsPerFile, AllRequiredPerms, ExtraData, AceWho, AceFlags, allow
) ->
    [AllRequiredPermsComb | EaccesPermsCombs] = combinations(AllRequiredPerms),

    % Granting all perms without required ones should result in eacces
    lists:foreach(fun(EaccessPermComb) ->
        EaccesPermsPerFile = lists:foldl(fun({Guid, Perm}, Acc) ->
            Acc#{Guid => [Perm | maps:get(Guid, Acc)]}
        end, ComplementaryPermsPerFile, EaccessPermComb),
        set_acls(Node, EaccesPermsPerFile, #{}, AceWho, AceFlags),
        ?assertMatch(
            {error, ?EACCES},
            Operation(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
        )
    end, EaccesPermsCombs),

    % Granting only required perms should result in success
    RequiredPermsPerFile = lists:foldl(fun({Guid, Perm}, Acc) ->
        Acc#{Guid => [Perm | maps:get(Guid, Acc, [])]}
    end, #{}, AllRequiredPermsComb),
    set_acls(Node, RequiredPermsPerFile, #{}, AceWho, AceFlags),
    ?assertNotMatch(
        {error, _},
        Operation(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
    );

run_acl_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath, Operation,
    ComplementaryPermsPerFile, AllRequiredPerms, ExtraData, AceWho, AceFlags, deny
) ->
    AllPermsPerFile = maps:map(fun(Guid, _) ->
        all_perms(Node, Guid)
    end, ComplementaryPermsPerFile),

    % Denying only required perms and granting all others should result in eacces
    lists:foreach(fun({Guid, Perm}) ->
        set_acls(Node, AllPermsPerFile, #{Guid => [Perm]}, AceWho, AceFlags),
        ?assertMatch(
            {error, ?EACCES},
            Operation(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
        )
    end, AllRequiredPerms),

    % Denying all perms but required ones should result in success
    set_acls(Node, #{}, ComplementaryPermsPerFile, AceWho, AceFlags),
    ?assertNotMatch(
        {error, _},
        Operation(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
    ).


set_acls(Node, AllowedPermsPerFile, DeniedPermsPerFile, AceWho, AceFlags) ->
    maps:fold(fun(Guid, Perms, _) ->
        ?assertEqual(ok, lfm_proxy:set_acl(
            Node, ?ROOT_SESS_ID, {guid, Guid},
            [?ALLOW_ACE(AceWho, AceFlags, perms_to_bitmask(Perms))])
        )
    end, ok, maps:without(maps:keys(DeniedPermsPerFile), AllowedPermsPerFile)),

    maps:fold(fun(Guid, Perms, _) ->
        AllPerms = all_perms(Node, Guid),

        ?assertEqual(ok, lfm_proxy:set_acl(
            Node, ?ROOT_SESS_ID, {guid, Guid},
            [
                ?DENY_ACE(AceWho, AceFlags, perms_to_bitmask(Perms)),
                ?ALLOW_ACE(AceWho, AceFlags, perms_to_bitmask(AllPerms))
            ]
        ))
    end, ok, DeniedPermsPerFile).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig2, "env_desc.json"), NewConfig2)
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


-spec create_files(node(), session:id(), file_meta:path(), #dir{} | #file{}) ->
    {PermsPerFile :: map(), ExtraData :: map()}.
create_files(Node, OwnerSessId, ParentDirPath, #file{
    name = FileName,
    perms = FilePerms,
    on_create = HookFun
}) ->
    FilePath = <<ParentDirPath/binary, "/", FileName/binary>>,
    {ok, FileGuid} = ?assertMatch(
        {ok, _},
        lfm_proxy:create(Node, OwnerSessId, FilePath, 8#777)
    ),
    ExtraData = case HookFun of
        undefined ->
            #{FilePath => FileGuid};
        _ when is_function(HookFun, 2) ->
            #{FilePath => HookFun(OwnerSessId, FileGuid)}
    end,
    {#{FileGuid => FilePerms}, ExtraData};
create_files(Node, OwnerSessId, ParentDirPath, #dir{
    name = DirName,
    perms = DirPerms,
    on_create = HookFun,
    children = Children
}) ->
    DirPath = <<ParentDirPath/binary, "/", DirName/binary>>,
    {ok, DirGuid} = ?assertMatch(
        {ok, _},
        lfm_proxy:mkdir(Node, OwnerSessId, DirPath)
    ),
    {PermsPerFile0, ExtraData0} = lists:foldl(fun(Child, {PermsPerFileAcc, ExtraDataAcc}) ->
        {ChildPerms, ChildExtraData} = create_files(Node, OwnerSessId, DirPath, Child),
        {maps:merge(PermsPerFileAcc, ChildPerms), maps:merge(ExtraDataAcc, ChildExtraData)}
    end, {#{}, #{}}, Children),

    ExtraData1 = case HookFun of
        undefined ->
            ExtraData0#{DirPath => DirGuid};
        _ when is_function(HookFun, 2) ->
            ExtraData0#{DirPath => HookFun(OwnerSessId, DirGuid)}
    end,
    {PermsPerFile0#{DirGuid => DirPerms}, ExtraData1}.


-spec is_dir(node(), file_id:file_guid()) -> boolean().
is_dir(Node, Guid) ->
    Uuid = file_id:guid_to_uuid(Guid),
    {ok, #document{value = #file_meta{type = FileType}}} = ?assertMatch(
        {ok, _},
        rpc:call(Node, file_meta, get, [Uuid])
    ),
    FileType == ?DIRECTORY_TYPE.


-spec all_perms(node(), file_id:file_guid()) -> Perms :: [binary()].
all_perms(Node, Guid) ->
    case is_dir(Node, Guid) of
        true -> ?ALL_DIR_PERMS;
        false -> ?ALL_FILE_PERMS
    end.


-spec complementary_perms(node(), file_id:file_guid(), Perms :: [binary()]) ->
    ComplementaryPerms :: [binary()].
complementary_perms(Node, Guid, Perms) ->
    ComplementaryPerms = all_perms(Node, Guid) -- Perms,
    % Special case: because ?delete_object and ?delete_subcontainer translates
    % to the same bitmask if even one of them is present other must also be removed
    case lists:member(?delete_object, Perms) or lists:member(?delete_subcontainer, Perms) of
        true -> ComplementaryPerms -- [?delete_object, ?delete_subcontainer];
        false -> ComplementaryPerms
    end.


%% @private
-spec perms_to_bitmask([binary()]) -> non_neg_integer().
perms_to_bitmask(Permissions) ->
    lists:foldl(fun(Perm, BitMask) ->
        BitMask bor perm_to_bitmask(Perm)
    end, ?no_flags_mask, Permissions).


%% @private
-spec perm_to_bitmask(binary()) -> non_neg_integer().
perm_to_bitmask(?read_object) -> ?read_object_mask;
perm_to_bitmask(?list_container) -> ?list_container_mask;
perm_to_bitmask(?write_object) -> ?write_object_mask;
perm_to_bitmask(?add_object) -> ?add_object_mask;
perm_to_bitmask(?add_subcontainer) -> ?add_subcontainer_mask;
perm_to_bitmask(?read_metadata) -> ?read_metadata_mask;
perm_to_bitmask(?write_metadata) -> ?write_metadata_mask;
perm_to_bitmask(?traverse_container) -> ?traverse_container_mask;
perm_to_bitmask(?delete_object) -> ?delete_child_mask;
perm_to_bitmask(?delete_subcontainer) -> ?delete_child_mask;
perm_to_bitmask(?read_attributes) -> ?read_attributes_mask;
perm_to_bitmask(?write_attributes) -> ?write_attributes_mask;
perm_to_bitmask(?delete) -> ?delete_mask;
perm_to_bitmask(?read_acl) -> ?read_acl_mask;
perm_to_bitmask(?write_acl) -> ?write_acl_mask.


-spec perm_to_posix_perms(Perm :: binary()) -> PosixPerms :: [atom()].
perm_to_posix_perms(?read_object) -> [read];
perm_to_posix_perms(?list_container) -> [read];
perm_to_posix_perms(?write_object) -> [write];
perm_to_posix_perms(?add_object) -> [write, exec];
perm_to_posix_perms(?add_subcontainer) -> [write];
perm_to_posix_perms(?read_metadata) -> [read];
perm_to_posix_perms(?write_metadata) -> [write];
perm_to_posix_perms(?traverse_container) -> [exec];
perm_to_posix_perms(?delete_object) -> [write, exec];
perm_to_posix_perms(?delete_subcontainer) -> [write, exec];
perm_to_posix_perms(?read_attributes) -> [];
perm_to_posix_perms(?write_attributes) -> [write];
perm_to_posix_perms(?delete) -> [owner_if_parent_sticky];
perm_to_posix_perms(?read_acl) -> [];
perm_to_posix_perms(?write_acl) -> [owner].


-spec posix_perm_to_mode(PosixPerm :: atom(), Type :: owner | group) ->
    non_neg_integer().
posix_perm_to_mode(read, owner) -> 8#4 bsl 6;
posix_perm_to_mode(write, owner) -> 8#2 bsl 6;
posix_perm_to_mode(exec, owner) -> 8#1 bsl 6;
posix_perm_to_mode(read, group) -> 8#4 bsl 3;
posix_perm_to_mode(write, group) -> 8#2 bsl 3;
posix_perm_to_mode(exec, group) -> 8#1 bsl 3;
posix_perm_to_mode(_, _) -> 8#0.


-spec combinations([term()]) -> [[term()]].
combinations([]) ->
    [[]];
combinations([Item | Items]) ->
    Combinations = combinations(Items),
    [[Item | Comb] || Comb <- Combinations] ++ Combinations.


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
