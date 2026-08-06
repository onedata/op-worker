%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of the basic lfm file operations - creating, opening, reading,
%%% writing, truncating, statting and removing files and directories.
%%%
%%% The bodies are shared by file_lfm_posix_test_SUITE and file_lfm_s3_test_SUITE.
%%% Each test works within its own, randomly named directory in the space, which
%%% the suites empty between test cases.
%%% @end
%%%-------------------------------------------------------------------
-module(file_lfm_crud_tests).
-author("Bartosz Walkowicz").

% This module indirectly includes eunit.hrl, whose parse transform would
% otherwise auto-export every arity 0 function named *_test - clashing with
% the export list below.
-define(EUNIT_NOAUTO, 1).

-include("file/file_lfm_test.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% tests
-export([
    create_and_unlink_test/0,
    create_and_access_test/0,
    create_under_regular_file_fails_test/0,

    basic_rdwr_test/0,
    rdwr_opens_storage_file_once_test/0,
    rdwr_after_storage_file_delete_test/0,
    write_and_read_all_subranges_test/0,
    write_and_check_test/0,
    file_gap_test/0,
    sequential_writes_from_many_processes_test/0,

    get_attrs_test/0,

    truncate_test/0,
    truncate_and_write_test/0,

    mkdir_and_rmdir_test/0,
    rmdir_of_space_dir_fails_test/0,
    rm_recursive_test/0,
    rm_recursive_of_space_dir_fails_test/0,
    close_deleted_open_files_test/0,

    ensure_dir_test/0,
    create_dir_at_path_test/0
]).

%% performance tests (the parameter specs live here so that both suites share them)
-export([
    echo_loop_test_performance_spec/0, echo_loop_test_base/1
]).

%% suite setup helpers
-export([ensure_storage_driver_unmocked/0]).

-define(REPEATS, 3).
-define(SUCCESS_RATE, 100).

-define(ATTEMPTS, 30).

% Writes are applied asynchronously to the file size, so a stat right after one
% may still report the previous size.
-define(SIZE_UPDATE_ATTEMPTS, 10).


%%%===================================================================
%%% Tests of creating and removing files
%%%===================================================================


create_and_unlink_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    OtherSessId = file_lfm_test_utils:get_session_id(?OTHER_USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    FilePath11 = filename:join([RootDirPath, generator:gen_name()]),
    FilePath12 = filename:join([RootDirPath, generator:gen_name()]),
    FilePath21 = filename:join([RootDirPath, generator:gen_name()]),
    FilePath22 = filename:join([RootDirPath, generator:gen_name()]),

    ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, FilePath11)),
    ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, FilePath12)),
    ?assertMatch({error, ?EEXIST}, lfm_proxy:create(Node, SessId, FilePath11)),

    ?assertMatch({ok, _}, lfm_proxy:create(Node, OtherSessId, FilePath21)),
    ?assertMatch({ok, _}, lfm_proxy:create(Node, OtherSessId, FilePath22)),
    ?assertMatch({error, ?EEXIST}, lfm_proxy:create(Node, OtherSessId, FilePath21)),

    ?assertMatch(ok, lfm_proxy:unlink(Node, SessId, {path, FilePath11})),
    ?assertMatch(ok, lfm_proxy:unlink(Node, OtherSessId, {path, FilePath21})),

    ?assertMatch({error, ?ENOENT}, lfm_proxy:unlink(Node, SessId, {path, FilePath11})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:unlink(Node, OtherSessId, {path, FilePath21})),

    ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, FilePath11)),
    ?assertMatch({ok, _}, lfm_proxy:create(Node, OtherSessId, FilePath21)),

    ok.


create_and_access_test() ->
    Node = file_lfm_test_utils:get_node(),
    OwnerSessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    OtherSessId = file_lfm_test_utils:get_session_id(?OTHER_USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, OwnerSessId),

    NewFile = fun(Mode) ->
        FilePath = filename:join([RootDirPath, generator:gen_name()]),
        ?assertMatch({ok, _}, lfm_proxy:create(Node, OwnerSessId, FilePath, Mode)),
        FilePath
    end,

    % owner: write only
    WriteOnlyPath = NewFile(8#240),
    ?assertMatch({ok, _}, lfm_proxy:open(Node, OwnerSessId, {path, WriteOnlyPath}, write)),
    ?assertMatch({ok, _}, lfm_proxy:open(Node, OtherSessId, {path, WriteOnlyPath}, read)),
    ?assertMatch(ok, lfm_proxy:truncate(Node, OwnerSessId, {path, WriteOnlyPath}, 10)),

    ?assertMatch({error, ?EACCES}, lfm_proxy:open(Node, OwnerSessId, {path, WriteOnlyPath}, read)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(Node, OtherSessId, {path, WriteOnlyPath}, write)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(Node, OwnerSessId, {path, WriteOnlyPath}, rdwr)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(Node, OtherSessId, {path, WriteOnlyPath}, rdwr)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:truncate(Node, OtherSessId, {path, WriteOnlyPath}, 10)),

    % owner: read/write, group: read only
    OwnerRdwrPath = NewFile(8#640),
    ?assertMatch({ok, _}, lfm_proxy:open(Node, OwnerSessId, {path, OwnerRdwrPath}, write)),
    ?assertMatch({ok, _}, lfm_proxy:open(Node, OwnerSessId, {path, OwnerRdwrPath}, read)),
    ?assertMatch({ok, _}, lfm_proxy:open(Node, OwnerSessId, {path, OwnerRdwrPath}, rdwr)),
    ?assertMatch({ok, _}, lfm_proxy:open(Node, OtherSessId, {path, OwnerRdwrPath}, read)),
    ?assertMatch(ok, lfm_proxy:truncate(Node, OwnerSessId, {path, OwnerRdwrPath}, 10)),

    ?assertMatch({error, ?EACCES}, lfm_proxy:open(Node, OtherSessId, {path, OwnerRdwrPath}, write)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(Node, OtherSessId, {path, OwnerRdwrPath}, rdwr)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:truncate(Node, OtherSessId, {path, OwnerRdwrPath}, 10)),

    % owner and group: read/write
    SharedRdwrPath = NewFile(8#670),
    ?assertMatch({ok, _}, lfm_proxy:open(Node, OwnerSessId, {path, SharedRdwrPath}, write)),
    ?assertMatch({ok, _}, lfm_proxy:open(Node, OwnerSessId, {path, SharedRdwrPath}, read)),
    ?assertMatch({ok, _}, lfm_proxy:open(Node, OwnerSessId, {path, SharedRdwrPath}, rdwr)),
    ?assertMatch({ok, _}, lfm_proxy:open(Node, OtherSessId, {path, SharedRdwrPath}, write)),
    ?assertMatch({ok, _}, lfm_proxy:open(Node, OtherSessId, {path, SharedRdwrPath}, read)),
    ?assertMatch({ok, _}, lfm_proxy:open(Node, OtherSessId, {path, SharedRdwrPath}, rdwr)),
    ?assertMatch(ok, lfm_proxy:truncate(Node, OwnerSessId, {path, SharedRdwrPath}, 10)),

    % owner and group: read only
    ReadOnlyPath = NewFile(8#540),
    ?assertMatch({ok, _}, lfm_proxy:open(Node, OwnerSessId, {path, ReadOnlyPath}, read)),
    ?assertMatch({ok, _}, lfm_proxy:open(Node, OtherSessId, {path, ReadOnlyPath}, read)),

    ?assertMatch({error, ?EACCES}, lfm_proxy:open(Node, OwnerSessId, {path, ReadOnlyPath}, write)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(Node, OwnerSessId, {path, ReadOnlyPath}, rdwr)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(Node, OtherSessId, {path, ReadOnlyPath}, write)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:open(Node, OtherSessId, {path, ReadOnlyPath}, rdwr)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:truncate(Node, OwnerSessId, {path, ReadOnlyPath}, 10)),
    ?assertMatch({error, ?EACCES}, lfm_proxy:truncate(Node, OtherSessId, {path, ReadOnlyPath}, 10)),

    ok.


create_under_regular_file_fails_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    % intentionally set mode of the file to one typical for a directory,
    % so that it has the execute right
    ParentPath = filename:join([RootDirPath, generator:gen_name()]),
    ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, ParentPath, ?DEFAULT_DIR_PERMS)),

    ?assertEqual({error, ?ENOTDIR}, lfm_proxy:create(
        Node, SessId, filename:join([ParentPath, generator:gen_name()]))),

    ok.


mkdir_and_rmdir_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    DirPath = filename:join([RootDirPath, generator:gen_name()]),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, DirPath)),
    ?assertMatch(ok, lfm_proxy:unlink(Node, SessId, {path, DirPath})),

    ok.


rmdir_of_space_dir_fails_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),

    ?assertMatch({error, ?ENOTSUP}, lfm_proxy:unlink(
        Node, SessId, {path, file_lfm_test_utils:build_space_path()})),

    ok.


rm_recursive_test() ->
    %% Created file structure (mode in brackets):
    %%
    %%   RootDir (775)
    %%      |
    %%      a (700) ─────┬──────────┬─────────┬────────┐
    %%      |            |          |         |        |
    %%      b (300)      c (700)    d (700)   f (000)  x (500 after creation)
    %%                   |  \       |    \             |
    %%                   g   h      i     e (000)      j (000)
    %%                  (000)(000) (000)
    %%
    %% b, e and x are not removable by the user: b and e cannot be listed, and
    %% x cannot have its children removed.

    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    Path = fun(RelPath) -> filename:join([RootDirPath, RelPath]) end,

    {ok, DirAGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, Path(<<"a">>), 8#700)),
    {ok, _DirBGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, Path(<<"a/b">>), 8#300)),
    {ok, DirCGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, Path(<<"a/c">>), 8#700)),
    {ok, _DirDGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, Path(<<"a/d">>), 8#700)),
    {ok, _DirEGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, Path(<<"a/d/e">>), 8#000)),
    {ok, DirXGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, Path(<<"a/x">>), 8#700)),
    {ok, FileFGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, Path(<<"a/f">>), 8#000)),
    {ok, FileGGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, Path(<<"a/c/g">>), 8#000)),
    {ok, FileHGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, Path(<<"a/c/h">>), 8#000)),
    {ok, FileIGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, Path(<<"a/d/i">>), 8#000)),
    {ok, _FileJGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, Path(<<"a/x/j">>), 8#000)),
    ?assertEqual(ok, lfm_proxy:set_perms(Node, SessId, ?FILE_REF(DirXGuid), 8#500)),

    % rm_recursive returns ok despite the fact that some files will not be deleted,
    % as the user has no permission to remove or even list them - it moves files to
    % the trash and deletes them asynchronously
    ?assertEqual(ok, lfm_proxy:rm_recursive(Node, SessId, ?FILE_REF(DirAGuid))),

    % TODO VFS-7348 assert that DirA, DirB, DirD, DirE, DirX and FileJ survive
    % after scheduling the deletion as the user rather than as root
    lists:foreach(fun(Guid) ->
        ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(Node, SessId, ?FILE_REF(Guid)), ?ATTEMPTS)
    end, [DirCGuid, FileFGuid, FileGGuid, FileHGuid, FileIGuid]),

    ok.


rm_recursive_of_space_dir_fails_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),

    ?assertMatch({error, ?ENOTSUP}, lfm_proxy:rm_recursive(
        Node, SessId, {path, file_lfm_test_utils:build_space_path()})),

    ok.


close_deleted_open_files_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {RootDirGuid, _RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(
        Node, SessId, RootDirGuid, generator:gen_name(), ?DEFAULT_DIR_MODE)),
    {ok, {FileGuid1, Handle1}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(
        Node, SessId, DirGuid, generator:gen_name(), ?DEFAULT_FILE_MODE)),
    {ok, {_FileGuid2, Handle2}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(
        Node, SessId, DirGuid, generator:gen_name(), ?DEFAULT_FILE_MODE)),
    ?assertMatch({ok, _}, lfm_proxy:write(Node, Handle1, 0, <<"some_text">>)),
    ?assertMatch({ok, _}, lfm_proxy:write(Node, Handle2, 0, <<"another_text">>)),

    % one file is deleted directly, the other one along with its parent
    ?assertEqual(ok, lfm_proxy:unlink(Node, SessId, ?FILE_REF(FileGuid1))),
    ?assertEqual(ok, lfm_proxy:rm_recursive(Node, SessId, ?FILE_REF(RootDirGuid))),

    ?assertEqual(ok, lfm_proxy:close(Node, Handle1)),
    ?assertEqual(ok, lfm_proxy:close(Node, Handle2)),

    ok.


%%%===================================================================
%%% Tests of creating directories by path
%%%===================================================================


ensure_dir_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    SpaceDirGuid = file_lfm_test_utils:get_space_dir_guid(),

    % all the concurrent calls must converge on the same directory tree
    DirName = generator:gen_name(),
    RelPath = filename:join([DirName, DirName, DirName]),
    lists_utils:pforeach(fun(_) ->
        ?assertMatch({ok, _}, lfm_proxy:ensure_dir(Node, SessId, SpaceDirGuid, RelPath, ?DEFAULT_DIR_MODE))
    end, lists:seq(1, 100)),

    ok.


create_dir_at_path_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    SpaceDirGuid = file_lfm_test_utils:get_space_dir_guid(),

    RelPath = filename:join(lists:duplicate(8, generator:gen_name())),

    {ok, #file_attr{guid = DirGuid}} = ?assertMatch({ok, _}, lfm_proxy:create_dir_at_path(
        Node, SessId, SpaceDirGuid, RelPath)),
    % repeating the call must return the already existing directory
    ?assertMatch({ok, #file_attr{guid = DirGuid}}, lfm_proxy:create_dir_at_path(
        Node, SessId, SpaceDirGuid, RelPath)),

    ok.


%%%===================================================================
%%% Tests of reading and writing
%%%===================================================================


basic_rdwr_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(
        Node, SessId, filename:join([RootDirPath, generator:gen_name()]))),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), rdwr)),

    ?assertEqual({ok, 9}, lfm_proxy:write(Node, Handle, 0, <<"test_data">>)),
    ?assertEqual({ok, <<"test_data">>}, lfm_proxy:read(Node, Handle, 0, 9)),

    ?assertEqual(ok, lfm_proxy:close(Node, Handle)),

    ok.


rdwr_opens_storage_file_once_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(
        Node, SessId, filename:join([RootDirPath, generator:gen_name()]))),

    % this is about the provider opening the storage file on behalf of the
    % client, which it does not do at all for a client doing direct io
    file_lfm_test_utils:set_direct_io(SessId, false),
    ok = test_utils:mock_new(Node, storage_driver, [passthrough]),
    try
        test_utils:mock_assert_num_calls(Node, storage_driver, open, 2, 0),

        {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), rdwr)),
        test_utils:mock_assert_num_calls(Node, storage_driver, open, 2, 1),

        ?assertEqual({ok, 5}, lfm_proxy:write(Node, Handle, 0, <<"11111">>)),
        ?assertEqual({ok, 5}, lfm_proxy:write(Node, Handle, 5, <<"22222">>)),

        ?assertEqual({ok, <<"1111122222">>}, lfm_proxy:read(Node, Handle, 0, 10)),
        ?assertEqual({ok, <<"11111">>}, lfm_proxy:read(Node, Handle, 0, 5)),

        ?assertEqual(ok, lfm_proxy:close(Node, Handle)),
        % neither the reads nor the writes may have opened the storage file again
        test_utils:mock_assert_num_calls(Node, storage_driver, open, 2, 1)
    after
        % restore first - unlike the mock, which the suite also clears
        % defensively, this would otherwise silently change the io path taken by
        % every remaining test case sharing the session
        file_lfm_test_utils:set_direct_io(SessId, true),
        ok = test_utils:mock_unload(Node, [storage_driver])
    end,

    ok.


rdwr_after_storage_file_delete_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(
        Node, SessId, filename:join([RootDirPath, generator:gen_name()]))),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), rdwr)),
    FileContent = <<"test_data">>,

    % remove the file behind lfm's back, directly on the storage
    FileCtx = rpc:call(Node, file_ctx, new_by_guid, [FileGuid]),
    {SDHandle, _} = rpc:call(Node, storage_driver, new_handle, [SessId, FileCtx]),
    ?assertEqual(ok, rpc:call(Node, storage_driver, unlink, [SDHandle, size(FileContent)])),

    % the already open handle must still be usable
    ?assertEqual({ok, 9}, lfm_proxy:write(Node, Handle, 0, FileContent)),
    ?assertEqual({ok, FileContent}, lfm_proxy:read(Node, Handle, 0, size(FileContent))),
    ?assertEqual(ok, lfm_proxy:close(Node, Handle)),

    ok.


write_and_read_all_subranges_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    OpenFile = fun() ->
        {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(
            Node, SessId, filename:join([RootDirPath, generator:gen_name()]))),
        {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), rdwr)),
        Handle
    end,
    Handle1 = OpenFile(),
    Handle2 = OpenFile(),

    % after every write, read back every subrange of what was just written
    WriteAndReadBack = fun(Handle, Offset, Bytes) ->
        Size = size(Bytes),
        ?assertMatch({ok, Size}, lfm_proxy:write(Node, Handle, Offset, Bytes)),
        lists:foreach(fun(From) ->
            lists:foreach(fun(Len) ->
                ExpectedBytes = binary:part(Bytes, From - Offset, Len),
                ?assertMatch({ok, ExpectedBytes}, lfm_proxy:read(Node, Handle, From, Len))
            end, lists:seq(1, Offset + Size - From))
        end, lists:seq(Offset, Offset + Size - 1))
    end,

    lists:foreach(fun({Offset, Bytes}) ->
        WriteAndReadBack(Handle1, Offset, Bytes),
        WriteAndReadBack(Handle2, Offset, Bytes)
    end, [
        {0, <<"abc">>},
        {3, <<"def">>},
        {2, <<"qwerty">>},
        {8, <<"zxcvbnm">>},
        {6, <<"qwerty">>}
    ]),

    WriteAndReadBack(Handle1, 10, crypto:strong_rand_bytes(40)),

    ok.


write_and_check_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    FilePath = filename:join([RootDirPath, generator:gen_name()]),
    ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, FilePath)),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, {path, FilePath}, rdwr)),

    ?assertMatch({ok, #file_attr{size = 0}}, lfm_proxy:stat(Node, SessId, {path, FilePath})),

    % unlike a plain write, write_and_check returns attrs already reflecting the write
    ?assertMatch({ok, 3, {ok, #file_attr{size = 3}}}, lfm_proxy:write_and_check(Node, Handle, 0, <<"abc">>)),
    ?assertMatch({ok, 3, {ok, #file_attr{size = 6}}}, lfm_proxy:write_and_check(Node, Handle, 3, <<"abc">>)),
    ?assertMatch({ok, 3, {ok, #file_attr{size = 6}}}, lfm_proxy:write_and_check(Node, Handle, 2, <<"abc">>)),
    ?assertMatch({ok, 9, {ok, #file_attr{size = 10}}}, lfm_proxy:write_and_check(Node, Handle, 1, <<"123456789">>)),

    ok.


file_gap_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(
        Node, SessId, filename:join([RootDirPath, generator:gen_name()]))),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), rdwr)),

    % a gap left before the written range must read back as zeros
    ?assertEqual({ok, 3}, lfm_proxy:write(Node, Handle, 3, <<"abc">>)),
    ?assertEqual(ok, lfm_proxy:fsync(Node, Handle)),
    ?assertEqual({ok, <<0, 0, 0, $a, $b, $c>>}, lfm_proxy:read(Node, Handle, 0, 6)),

    % and so must a gap left between two written ranges
    ?assertEqual({ok, 4}, lfm_proxy:write(Node, Handle, 8, <<"defg">>)),
    ?assertEqual(ok, lfm_proxy:fsync(Node, Handle)),
    ?assertEqual({ok, <<0, 0, 0, $a, $b, $c, 0, 0, $d, $e, $f, $g>>},
        lfm_proxy:read(Node, Handle, 0, 12)),

    ok.


sequential_writes_from_many_processes_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    SpaceDirGuid = file_lfm_test_utils:get_space_dir_guid(),

    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(
        Node, SessId, SpaceDirGuid, ?RAND_STR(), ?DEFAULT_FILE_MODE)),

    % every process appends its own key to the json document stored in the file;
    % none of the updates may be lost
    WritersNum = 1024,
    ValueSize = 1024,
    lists_utils:pforeach(fun(Key) ->
        critical_section:run(?FUNCTION_NAME, fun() ->
            {ok, Handle} = lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), rdwr),
            Json = read_json(Node, Handle),
            lfm_proxy:write(Node, Handle, 0, json_utils:encode(
                Json#{integer_to_binary(Key) => ?RAND_STR(ValueSize)})),
            % note: lfm:fsync is called on close
            lfm_proxy:close(Node, Handle)
        end)
    end, lists:seq(1, WritersNum)),

    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), rdwr)),
    FinalJson = read_json(Node, Handle),
    ?assertEqual(WritersNum, maps:size(FinalJson)),
    maps:foreach(fun(_Key, Value) ->
        ?assertEqual(ValueSize, byte_size(Value))
    end, FinalJson),

    ok.


%%%===================================================================
%%% Tests of statting
%%%===================================================================


get_attrs_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),

    SpacePath = file_lfm_test_utils:build_space_path(),
    FileName = generator:gen_name(),
    FilePath = file_lfm_test_utils:build_space_path(FileName),

    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, FilePath)),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, {path, FilePath}, rdwr)),

    SelectedAttrs = [name, size, index, active_permissions_type, has_custom_metadata],

    SpaceName = oct_background:get_space_name(?SPACE_SELECTOR),
    SpaceDirIndex = file_listing:build_index(file_lfm_test_utils:get_space_id()),
    % NOTE: the space dir reports no size because the suites turn dir stats counting off
    ?assertMatch({ok, #file_attr{
        name = SpaceName,
        size = undefined,
        index = SpaceDirIndex,
        active_permissions_type = posix,
        has_custom_metadata = false
    }}, lfm_proxy:stat(Node, SessId, {path, SpacePath}, SelectedAttrs)),

    FileIndex = file_listing:build_index(FileName, oct_background:get_provider_id(?PROVIDER_SELECTOR)),
    ?assertMatch({ok, #file_attr{
        name = FileName,
        size = 0,
        index = FileIndex,
        active_permissions_type = posix,
        has_custom_metadata = false
    }}, lfm_proxy:stat(Node, SessId, {path, FilePath}, SelectedAttrs)),
    ?assertMatch({ok, #file_attr{size = 0}}, lfm_proxy:stat(Node, SessId, {path, FilePath})),

    % writes are reflected in the size, overlapping ones do not extend it twice
    lists:foreach(fun({Offset, Bytes, ExpectedSize}) ->
        WrittenSize = size(Bytes),
        ?assertMatch({ok, WrittenSize}, lfm_proxy:write(Node, Handle, Offset, Bytes)),
        ?assertMatch({ok, #file_attr{size = ExpectedSize}},
            lfm_proxy:stat(Node, SessId, {path, FilePath}), ?SIZE_UPDATE_ATTEMPTS)
    end, [
        {0, <<"abc">>, 3},
        {3, <<"abc">>, 6},
        {2, <<"abc">>, 6},
        {1, <<"123456789">>, 10}
    ]),

    ?assertMatch(ok, lfm_proxy:set_xattr(Node, SessId, ?FILE_REF(FileGuid),
        #xattr{name = <<"123456789">>, value = <<"!@#">>})),
    ?assertMatch({ok, #file_attr{has_custom_metadata = true}},
        lfm_proxy:stat(Node, SessId, {path, FilePath}, [has_custom_metadata]), ?SIZE_UPDATE_ATTEMPTS),

    ok.


%%%===================================================================
%%% Tests of truncating
%%%===================================================================


truncate_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    FilePath = filename:join([RootDirPath, generator:gen_name()]),
    FileKey = {path, FilePath},
    ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, FilePath)),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, FileKey, rdwr)),

    AssertSize = fun(ExpectedSize) ->
        ?assertMatch({ok, #file_attr{size = ExpectedSize}},
            lfm_proxy:stat(Node, SessId, FileKey), ?SIZE_UPDATE_ATTEMPTS)
    end,

    AssertSize(0),
    ?assertEqual({ok, 3}, lfm_proxy:write(Node, Handle, 0, <<"abc">>)),
    AssertSize(3),

    % truncating down drops the tail
    ?assertEqual(ok, lfm_proxy:truncate(Node, SessId, FileKey, 1)),
    AssertSize(1),
    ?assertEqual({ok, <<"a">>}, lfm_proxy:read(Node, Handle, 0, 1)),

    % truncating up leaves the head intact
    ?assertEqual(ok, lfm_proxy:truncate(Node, SessId, FileKey, 10)),
    AssertSize(10),
    ?assertEqual({ok, <<"a">>}, lfm_proxy:read(Node, Handle, 0, 1)),

    % a write within the current size does not change it
    ?assertEqual({ok, 3}, lfm_proxy:write(Node, Handle, 1, <<"abc">>)),
    AssertSize(10),

    ?assertEqual(ok, lfm_proxy:truncate(Node, SessId, FileKey, 5)),
    AssertSize(5),
    ?assertEqual({ok, <<"aabc">>}, lfm_proxy:read(Node, Handle, 0, 4)),

    ok.


truncate_and_write_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    FilePath = filename:join([RootDirPath, generator:gen_name()]),
    FileKey = {path, FilePath},
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, FilePath)),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, FileKey, rdwr)),

    AssertSize = fun(ExpectedSize) ->
        ?assertMatch({ok, #file_attr{size = ExpectedSize}},
            lfm_proxy:stat(Node, SessId, FileKey), ?SIZE_UPDATE_ATTEMPTS)
    end,
    AssertBlocks = fun(ExpectedBlocks, ExpectedTotalSize) ->
        ?assertMatch({ok, [#{<<"blocks">> := ExpectedBlocks, <<"totalBlocksSize">> := ExpectedTotalSize}]},
            opt_file_metadata:get_distribution_deprecated(Node, SessId, ?FILE_REF(FileGuid)),
            ?SIZE_UPDATE_ATTEMPTS)
    end,

    AssertSize(0),
    ?assertEqual({ok, 3}, lfm_proxy:write(Node, Handle, 0, <<"abc">>)),
    AssertSize(3),

    % a write right after a truncate must not be aggregated with it
    % (that aggregation used to yield a wrong file size)
    ?assertEqual(ok, lfm_proxy:truncate(Node, SessId, FileKey, 0)),
    ?assertEqual({ok, 1}, lfm_proxy:write(Node, Handle, 0, <<"a">>)),
    AssertSize(1),
    ?assertEqual({ok, <<"a">>}, lfm_proxy:read(Node, Handle, 0, 1)),
    AssertBlocks([[0, 1]], 1),

    % a truncate between two writes trims the first block
    ?assertEqual({ok, 4}, lfm_proxy:write(Node, Handle, 1, <<"bcde">>)),
    ?assertEqual(ok, lfm_proxy:truncate(Node, SessId, FileKey, 3)),
    ?assertEqual({ok, 3}, lfm_proxy:write(Node, Handle, 5, <<"fgh">>)),
    AssertSize(8),
    ?assertEqual({ok, <<"abc\0\0fgh">>}, lfm_proxy:read(Node, Handle, 0, 8)),
    AssertBlocks([[0, 3], [5, 3]], 6),

    % a truncate between two writes drops the first block entirely
    ?assertEqual({ok, 3}, lfm_proxy:write(Node, Handle, 8, <<"ijk">>)),
    ?assertEqual(ok, lfm_proxy:truncate(Node, SessId, FileKey, 8)),
    ?assertEqual({ok, 2}, lfm_proxy:write(Node, Handle, 9, <<"xy">>)),
    AssertSize(11),
    ?assertEqual({ok, <<"abc\0\0fgh\0xy">>}, lfm_proxy:read(Node, Handle, 0, 11)),
    AssertBlocks([[0, 3], [5, 3], [9, 2]], 8),

    % the same two cases, but with the truncate emitted as an event only, so that
    % events are not flushed and the storage file itself is left untouched -
    % only the metadata is changed
    ?assertEqual({ok, 4}, lfm_proxy:write(Node, Handle, 11, <<"bcde">>)),
    ?assertEqual(ok, emit_truncate_event(Node, SessId, FileKey, 13)),
    ?assertEqual({ok, 3}, lfm_proxy:write(Node, Handle, 15, <<"fgh">>)),
    AssertSize(18),
    ?assertEqual({ok, <<"abc\0\0fgh\0xybcdefgh">>}, lfm_proxy:read(Node, Handle, 0, 18)),
    AssertBlocks([[0, 3], [5, 3], [9, 4], [15, 3]], 13),

    ?assertEqual({ok, 3}, lfm_proxy:write(Node, Handle, 18, <<"ijk">>)),
    ?assertEqual(ok, emit_truncate_event(Node, SessId, FileKey, 18)),
    ?assertEqual({ok, 2}, lfm_proxy:write(Node, Handle, 19, <<"xy">>)),
    AssertSize(21),
    ?assertEqual({ok, <<"abc\0\0fgh\0xybcdefghixy">>}, lfm_proxy:read(Node, Handle, 0, 21)),
    AssertBlocks([[0, 3], [5, 3], [9, 4], [15, 3], [19, 2]], 15),

    ok.


%%%===================================================================
%%% Performance tests
%%%===================================================================


echo_loop_test_performance_spec() -> [
    {repeats, ?REPEATS},
    {success_rate, ?SUCCESS_RATE},
    {parameters, [
        [{name, writes_num}, {value, 1000}, {description, "Number of write operations during "}]
    ]},
    {description, "Simulates loop of echo operations done by client"},
    {config, [{name, performance},
        {parameters, [
            [{name, writes_num}, {value, 10000}]
        ]},
        {description, "Basic performance configuration"}
    ]}
].


echo_loop_test_base(Config) ->
    WritesNum = ?config(writes_num, Config),

    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    FilePath = filename:join([RootDirPath, generator:gen_name()]),
    ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, FilePath)),

    Stopwatch = stopwatch:start(),
    lists:foldl(fun(Num, Offset) ->
        {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, {path, FilePath}, write)),
        Bytes = integer_to_binary(Num),
        BytesSize = size(Bytes),
        ?assertMatch({ok, BytesSize}, lfm_proxy:write(Node, Handle, Offset, Bytes)),
        lfm_proxy:close(Node, Handle),
        Offset + BytesSize
    end, 0, lists:seq(1, WritesNum)),

    #parameter{name = echo_time, value = stopwatch:read_micros(Stopwatch), unit = "us",
        description = "Aggregated time of all operations"}.


%%%===================================================================
%%% Suite setup helpers
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% rdwr_opens_storage_file_once_test mocks storage_driver for the duration of a
%% single case and unloads it itself. A run interrupted in between would leave
%% the mock in place on a reused deployment, so this is called defensively from
%% the init_per_suite posthook (unloading a module that was never mocked is a
%% no-op).
%% @end
%%--------------------------------------------------------------------
-spec ensure_storage_driver_unmocked() -> ok.
ensure_storage_driver_unmocked() ->
    test_utils:mock_unload(oct_background:get_provider_nodes(?PROVIDER_SELECTOR), [storage_driver]).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec read_json(node(), lfm:handle()) -> json_utils:json_map().
read_json(Node, Handle) ->
    {ok, Content} = ?assertMatch({ok, _}, lfm_proxy:check_size_and_read(Node, Handle, 0, 1024 * 1024 * 1024)),
    json_utils:decode(Content).


%% @private
-spec emit_truncate_event(node(), session:id(), lfm:file_key(), file_meta:size()) -> ok.
emit_truncate_event(Node, SessId, FileKey, Size) ->
    {ok, FileGuid} = rpc:call(Node, lfm_file_key, ensure_guid, [SessId, FileKey]),
    rpc:call(Node, lfm_event_emitter, emit_file_truncated, [FileGuid, Size, SessId]).
