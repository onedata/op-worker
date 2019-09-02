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
-module(lfm_permissions2_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/fslogic/fslogic_common.hrl").
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

    create_file_test/1,
    open_for_read_test/1,
    open_for_write_test/1,
    open_for_rdwr_test/1,
    create_and_open_test/1,

    get_parent_test/1,
    get_file_path_test/1,
    get_file_guid_test/1,

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
    remove_xattr_test/1
]).

all() ->
    ?ALL([
        mkdir_test,
        ls_test,
        readdir_plus_test,
        get_child_attr_test,

        create_file_test,
%%        open_for_read_test,       % TODO uncomment after fixing
%%        open_for_write_test,
%%        open_for_rdwr_test,
        create_and_open_test,

        get_parent_test,
        get_file_path_test,
        get_file_guid_test,

        create_share_test,
%%        remove_share_test,    TODO uncomment after fixing

%%        get_acl_test, % TODO uncomment after fixing removal of acl when changing posix mode
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
        remove_xattr_test
    ]).


-define(ALL_PERMS, [
    ?read_object,
    ?list_container,
    ?write_object,
    ?add_object,
    ?append_data,
    ?add_subcontainer,
    ?read_metadata,
    ?write_metadata,
    ?execute,
    ?traverse_container,
    ?delete_object,
    ?delete_subcontainer,
    ?read_attributes,
    ?write_attributes,
    ?delete,
    ?read_acl,
    ?write_acl,
    ?write_owner
]).
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
    name :: binary(),
    perms = [] :: [Perms :: binary()],
    on_create = undefined :: undefined | fun((session:id(), file_id:file_guid()) -> term())
}).


-record(dir, {
    name :: binary(),
    perms = [] :: [Perms :: binary()],
    on_create = undefined :: undefined | fun((session:id(), file_id:file_guid()) -> term()),
    children = [] :: [#dir{} | #file{}]
}).


-record(test_spec, {
    space = <<"space1">> :: binary(),
    root_dir :: binary(),
    owner = <<"user1">> :: binary(),
    user = <<"user2">> :: binary(),
    user_group = <<"group2">> :: binary(),
    user_outside_space = <<"user3">>,
    requires_traverse_ancestors = true :: boolean(),
    files :: map(),
    operation :: fun((UserId :: binary(), Path :: binary()) ->
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


create_file_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container, ?add_object]
        }],
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
%%        requires_traverse_ancestors = false,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_object]
        }],
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
            perms = [?write_object]
        }],
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
            perms = [?read_object, ?write_object]
        }],
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
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ParentDirGuid = maps:get(ParentDirPath, ExtraData),
            lfm_proxy:create_and_open(W, SessId, ParentDirGuid, <<"file1">>, 8#777)
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


create_share_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        files = [#dir{name = <<"dir1">>}],
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
                    W, OwnerSessId, {guid, Guid}, <<"remove_share">>
                )),
                ShareId
            end
        }],
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
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:set_acl(W, SessId, {guid, FileGuid}, [
                ?ALLOW_ACE(?group, ?no_flags_mask, perms_to_bitmask(?ALL_PERMS))
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
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileGuid = maps:get(FilePath, ExtraData),
            lfm_proxy:remove_xattr(W, SessId, {guid, FileGuid}, <<"myxattr">>)
        end
    }, Config).


%%%===================================================================
%%% TEST MECHANISM
%%%===================================================================


run_tests(Node, #test_spec{
    space = Space,
    owner = Owner,
    root_dir = RootDir
} = Spec, Config) ->
    OwnerSessId = ?config({session_id, {Owner, ?GET_DOMAIN(Node)}}, Config),
    RootDirPath = <<"/", Space/binary, "/", RootDir/binary>>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, OwnerSessId, RootDirPath, 8#777)),

    run_posix_tests(Node, RootDirPath, Spec, Config),
    run_acl_tests(Node, RootDirPath, Spec, Config).


%%%===================================================================
%%% POSIX TESTS MECHANISM
%%%===================================================================


run_posix_tests(Node, RootDirPath, #test_spec{
    owner = Owner,
    user = User,
    user_outside_space = OtherUser,
    requires_traverse_ancestors = RequiresTraverseAncestors,
    files = Files,
    operation = Fun
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
            Fun, PosixPermsPerFile, ExtraData, Type
        )
    end, [
        {OwnerUserSessId, owner, <<"owner_posix">>},
        {GroupUserSessId, group, <<"group_posix">>},
        {OtherUserSessId, other, <<"other_posix">>}
    ]).


run_posix_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath,
    Fun, PosixPermsPerFile, ExtraData, Type
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
            Fun, ComplementaryPosixPermsPerFile,
            RequiredPosixPerms, ExtraData, Type
        )
    catch T:R ->
        FilePathsToRequiredPerms = maps:fold(fun(Guid, RequiredPerms, Acc) ->
            lfm_proxy:set_perms(Node, ?ROOT_SESS_ID, {guid, Guid}, 8#777),
            {ok, Path} = lfm_proxy:get_file_path(Node, OwnerSessId, Guid),
            Acc#{Path => RequiredPerms}
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
    Node, OwnerSessId, SessId, TestCaseRootDirPath, Fun,
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
                Fun(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
            );
        _ ->
            run_standard_posix_tests(
                Node, OwnerSessId, SessId, TestCaseRootDirPath,
                Fun, ComplementaryPermsPerFile, RequiredPermsWithoutOwnership,
                ExtraData, owner
            )
    end;

run_posix_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath, Fun,
    ComplementaryPermsPerFile, AllRequiredPerms, ExtraData, group
) ->
    OperationRequiresOwnership = lists:any(fun({_, Perm}) ->
        Perm == owner orelse Perm == owner_if_parent_sticky
    end, AllRequiredPerms),

    case OperationRequiresOwnership of
        true ->
            % If operation requires ownership then for group member it should failed
            % even if all files modes are set to 777
            set_modes(Node, maps:map(fun(_, _) -> 8#777 end, ComplementaryPermsPerFile)),
            ?assertMatch(
                {error, ?EACCES},
                Fun(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
            );
        false ->
            run_standard_posix_tests(
                Node, OwnerSessId, SessId, TestCaseRootDirPath,
                Fun, ComplementaryPermsPerFile, AllRequiredPerms, ExtraData, group
            )
    end;

% Users not belonging to space or unauthorized should not be able to conduct any operation
run_posix_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath, Fun,
    ComplementaryPermsPerFile, _AllRequiredPerms, ExtraData, other
) ->
    set_modes(Node, maps:map(fun(_, _) -> 8#777 end, ComplementaryPermsPerFile)),
    ?assertMatch(
        {error, _},
        Fun(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
    ),
    ?assertMatch(
        {error, _},
        Fun(OwnerSessId, ?GUEST_SESS_ID, TestCaseRootDirPath, ExtraData)
    ).


run_standard_posix_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath, Fun,
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
            Fun(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
        )
    end, EaccesModesCombs),

    % Granting only required modes should result in success
    RequiredModesPerFile = lists:foldl(fun({Guid, Mode}, Acc) ->
        Acc#{Guid => Mode bor maps:get(Guid, Acc, 0)}
    end, #{}, RequiredModesComb),
    set_modes(Node, RequiredModesPerFile),
%%    ct:pal("QWE: ~p", [Fun(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)]),
    ?assertNotMatch(
        {error, _},
        Fun(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
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


run_acl_tests(Node, RootDirPath, #test_spec{
    owner = Owner,
    user = User,
    user_group = UserGroup,
    requires_traverse_ancestors = RequiresTraverseAncestors,
    files = Files,
    operation = Fun
}, Config) ->
    OwnerSessId = ?config({session_id, {Owner, ?GET_DOMAIN(Node)}}, Config),
    UserSessId = ?config({session_id, {User, ?GET_DOMAIN(Node)}}, Config),

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
            Fun, PermsPerFile, ExtraData, AceWho, AceFlags, Type
        )
    end, [
        {OwnerSessId, allow, <<"owner_acl_allow">>, ?owner, ?no_flags_mask},
        {UserSessId, allow, <<"user_acl_allow">>, User, ?no_flags_mask},
        {UserSessId, allow, <<"user_group_acl_allow">>, UserGroup, ?identifier_group_mask},
        {UserSessId, allow, <<"everyone_acl_allow">>, ?everyone, ?no_flags_mask},

        {OwnerSessId, deny, <<"owner_acl_deny">>, ?owner, ?no_flags_mask},
        {UserSessId, deny, <<"user_acl_deny">>, User, ?no_flags_mask},
        {UserSessId, deny, <<"user_group_acl_deny">>, UserGroup, ?identifier_group_mask},
        {UserSessId, deny, <<"everyone_acl_deny">>, ?everyone, ?no_flags_mask}
    ]).


run_acl_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath,
    Fun, RequiredPermsPerFile, ExtraData, AceWho, AceFlags, Type
) ->
    {ComplementaryPermsPerFile, AllRequiredPerms} = maps:fold(
        fun(FileGuid, FileRequiredPerms, {BasePermsPerFileAcc, RequiredPermsAcc}) ->
            {
                BasePermsPerFileAcc#{FileGuid => complementary_perms(FileRequiredPerms)},
                [{FileGuid, Perm} || Perm <- FileRequiredPerms] ++ RequiredPermsAcc
            }
        end,
        {#{}, []},
        RequiredPermsPerFile
    ),

    try
        run_acl_tests(
            Node, OwnerSessId, SessId, TestCaseRootDirPath,
            Fun, ComplementaryPermsPerFile, AllRequiredPerms,
            ExtraData, AceWho, AceFlags, Type
        )
    catch T:R ->
        RequiredPermsPerFileMap = maps:fold(fun(Guid, RequiredPerms, Acc) ->
            lfm_proxy:set_perms(Node, ?ROOT_SESS_ID, {guid, Guid}, 8#777),
            {ok, Path} = lfm_proxy:get_file_path(Node, OwnerSessId, Guid),
            Acc#{Path => RequiredPerms}
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
    Node, OwnerSessId, SessId, TestCaseRootDirPath, Fun,
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
            Fun(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
        )
    end, EaccesPermsCombs),

    % Granting only required perms should result in success
    RequiredPermsPerFile = lists:foldl(fun({Guid, Perm}, Acc) ->
        Acc#{Guid => [Perm | maps:get(Guid, Acc, [])]}
    end, #{}, AllRequiredPermsComb),
    set_acls(Node, RequiredPermsPerFile, #{}, AceWho, AceFlags),
    ?assertNotMatch(
        {error, _},
        Fun(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
    );

run_acl_tests(
    Node, OwnerSessId, SessId, TestCaseRootDirPath, Fun,
    ComplementaryPermsPerFile, AllRequiredPerms, ExtraData, AceWho, AceFlags, deny
) ->
    AllPermsPerFile = maps:map(fun(_, _) -> ?ALL_PERMS end, ComplementaryPermsPerFile),

    % Denying only required perms and granting all others should result in eacces
    lists:foreach(fun({Guid, Perm}) ->
        set_acls(Node, AllPermsPerFile, #{Guid => [Perm]}, AceWho, AceFlags),
        ?assertMatch(
            {error, ?EACCES},
            Fun(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
        )
    end, AllRequiredPerms),

    % Denying all perms but required ones should result in success
    set_acls(Node, #{}, ComplementaryPermsPerFile, AceWho, AceFlags),
    ?assertNotMatch(
        {error, _},
        Fun(OwnerSessId, SessId, TestCaseRootDirPath, ExtraData)
    ).


set_acls(Node, AllowedPermsPerFile, DeniedPermsPerFile, AceWho, AceFlags) ->
    maps:fold(fun(Guid, Perms, _) ->
        ?assertEqual(ok, lfm_proxy:set_acl(
            Node, ?ROOT_SESS_ID, {guid, Guid},
            [?ALLOW_ACE(AceWho, AceFlags, perms_to_bitmask(Perms))])
        )
    end, ok, maps:without(maps:keys(DeniedPermsPerFile), AllowedPermsPerFile)),

    maps:fold(fun(Guid, Perms, _) ->
        ?assertEqual(ok, lfm_proxy:set_acl(
            Node, ?ROOT_SESS_ID, {guid, Guid},
            [
                ?DENY_ACE(AceWho, AceFlags, perms_to_bitmask(Perms)),
                ?ALLOW_ACE(AceWho, AceFlags, perms_to_bitmask(?ALL_PERMS))
            ]
        ))
    end, ok, DeniedPermsPerFile).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(Config) ->
    initializer:teardown_storage(Config).

init_per_testcase(Case, Config) when
    Case =:= create_share_test;
    Case =:= remove_share_test
->
    initializer:mock_share_logic(Config),
    init_per_testcase(default, Config);

init_per_testcase(_Case, Config) ->
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).


end_per_testcase(Case, Config) when
    Case =:= create_share_test;
    Case =:= remove_share_test
->
    initializer:unmock_share_logic(Config),
    end_per_testcase(default, Config);

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config).


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
    PermsPerFile = case FilePerms of
        [] -> #{};
        _ -> #{FileGuid => FilePerms}
    end,
    ExtraData = case HookFun of
        undefined ->
            #{FilePath => FileGuid};
        _ when is_function(HookFun, 2) ->
            #{FilePath => HookFun(OwnerSessId, FileGuid)}
    end,
    {PermsPerFile, ExtraData};
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

    PermsPerFile1 = case DirPerms of
        [] -> PermsPerFile0;
        _ -> PermsPerFile0#{DirGuid => DirPerms}
    end,
    ExtraData1 = case HookFun of
        undefined ->
            ExtraData0#{DirPath => DirGuid};
        _ when is_function(HookFun, 2) ->
            ExtraData0#{DirPath => HookFun(OwnerSessId, DirGuid)}
    end,
    {PermsPerFile1, ExtraData1}.


-spec complementary_perms(Perms :: [binary()]) -> ComplementaryPerms :: [binary()].
complementary_perms(Perms) ->
    ?ALL_PERMS -- lists:usort(lists:flatmap(
        fun
            (?read_object) -> [?read_object, ?list_container];
            (?list_container) -> [?read_object, ?list_container];
            (?write_object) -> [?write_object, ?add_object];
            (?add_object) -> [?write_object, ?add_object];
            (?append_data) -> [?append_data, ?add_subcontainer];
            (?add_subcontainer) -> [?append_data, ?add_subcontainer];
            (?read_metadata) -> [?read_metadata];
            (?write_metadata) -> [?write_metadata];
            (?execute) -> [?execute, ?traverse_container];
            (?traverse_container) -> [?execute, ?traverse_container];
            (?delete_object) -> [?delete_object, ?delete_subcontainer];
            (?delete_subcontainer) -> [?delete_object, ?delete_subcontainer];
            (?read_attributes) -> [?read_attributes];
            (?write_attributes) -> [?write_attributes];
            (?delete) -> [?delete];
            (?read_acl) -> [?read_acl];
            (?write_acl) -> [?write_acl];
            (?write_owner) -> [?write_owner]
        end,
        Perms
    )).


%% @private
-spec perms_to_bitmask([binary()]) -> non_neg_integer().
perms_to_bitmask(Permissions) ->
    lists:foldl(fun(Perm, BitMask) ->
        BitMask bor perm_to_bitmask(Perm)
    end, ?no_flags_mask, Permissions).


%% @private
-spec perm_to_bitmask(binary()) -> non_neg_integer().
perm_to_bitmask(?all_perms) -> ?all_perms_mask;
perm_to_bitmask(?rw) -> ?rw_mask;
perm_to_bitmask(?read) -> ?read_mask;
perm_to_bitmask(?write) -> ?write_mask;
perm_to_bitmask(?read_object) -> ?read_object_mask;
perm_to_bitmask(?list_container) -> ?list_container_mask;
perm_to_bitmask(?write_object) -> ?write_object_mask;
perm_to_bitmask(?add_object) -> ?add_object_mask;
perm_to_bitmask(?append_data) -> ?append_data_mask;
perm_to_bitmask(?add_subcontainer) -> ?add_subcontainer_mask;
perm_to_bitmask(?read_metadata) -> ?read_metadata_mask;
perm_to_bitmask(?write_metadata) -> ?write_metadata_mask;
perm_to_bitmask(?execute) -> ?execute_mask;
perm_to_bitmask(?traverse_container) -> ?traverse_container_mask;
perm_to_bitmask(?delete_object) -> ?delete_object_mask;
perm_to_bitmask(?delete_subcontainer) -> ?delete_subcontainer_mask;
perm_to_bitmask(?read_attributes) -> ?read_attributes_mask;
perm_to_bitmask(?write_attributes) -> ?write_attributes_mask;
perm_to_bitmask(?delete) -> ?delete_mask;
perm_to_bitmask(?read_acl) -> ?read_acl_mask;
perm_to_bitmask(?write_acl) -> ?write_acl_mask;
perm_to_bitmask(?write_owner) -> ?write_owner_mask.


-spec perm_to_posix_perms(Perm :: binary()) -> PosixPerms :: [atom()].
perm_to_posix_perms(?read_object) -> [read];
perm_to_posix_perms(?list_container) -> [read];
perm_to_posix_perms(?write_object) -> [write];
perm_to_posix_perms(?add_object) -> [write, exec];
perm_to_posix_perms(?append_data) -> [write];
perm_to_posix_perms(?add_subcontainer) -> [write];
perm_to_posix_perms(?read_metadata) -> [read];
perm_to_posix_perms(?write_metadata) -> [write];
perm_to_posix_perms(?execute) -> [exec];
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
