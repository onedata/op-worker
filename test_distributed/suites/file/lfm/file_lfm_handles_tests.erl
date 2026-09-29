%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of the lifecycle of an open file handle. Two flavours of it are
%%% covered: the handle the provider keeps on the storage file on behalf of the
%%% client, which it must be able to recreate on demand and must not leak when
%%% opening fails, and the handle bound to the process that opened the file,
%%% which is released when that process dies.
%%%
%%% The bodies are shared by file_lfm_posix_test_SUITE and file_lfm_s3_test_SUITE.
%%% Each test works within its own, randomly named directory in the space, which
%%% the suites empty between test cases.
%%% @end
%%%-------------------------------------------------------------------
-module(file_lfm_handles_tests).
-author("Bartosz Walkowicz").

% This module indirectly includes eunit.hrl, whose parse transform would
% otherwise auto-export every arity 0 function named *_test - clashing with
% the export list below.
-define(EUNIT_NOAUTO, 1).

-include("file/file_lfm_test.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% tests
-export([
    recreate_handle_test/0,
    write_with_creation_handle_without_write_perms_test/0,
    recreate_handle_after_delete_test/0,

    direct_io_open_registers_no_storage_handle_test/0,

    open_failure_test/0,
    create_and_open_failure_test/0,
    open_failure_does_not_affect_other_session_test/0,
    mv_between_spaces_failure_test/0,

    monitored_open_releases_handle_on_process_death_test/0,
    list_process_handles_test/0
]).

-define(TEST_DATA, <<"test_data">>).
-define(ATTEMPTS, 10).


%%%===================================================================
%%% Tests of recreating a handle the provider no longer holds
%%%===================================================================


recreate_handle_test() ->
    recreate_handle_test_base(?DEFAULT_FILE_PERMS, keep_file).


%%--------------------------------------------------------------------
%% @doc
%% The handle obtained while creating a file grants write access regardless of
%% the file mode (see file_handles:get_creation_handle/1), and so must the
%% handle recreated in its place.
%% @end
%%--------------------------------------------------------------------
write_with_creation_handle_without_write_perms_test() ->
    recreate_handle_test_base(8#444, keep_file).


recreate_handle_after_delete_test() ->
    recreate_handle_test_base(?DEFAULT_FILE_PERMS, delete_after_open).


%% @private
-spec recreate_handle_test_base(file_meta:mode(), keep_file | delete_after_open) -> ok.
recreate_handle_test_base(CreateMode, WhatToDoWithFile) ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {RootDirGuid, _RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    {ok, {FileGuid, Handle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(
        Node, SessId, RootDirGuid, generator:gen_name(), CreateMode)),

    case WhatToDoWithFile of
        delete_after_open ->
            ?assertEqual(ok, lfm_proxy:unlink(Node, SessId, ?FILE_REF(FileGuid))),
            ?assertEqual(ok, rpc:call(Node, permissions_cache, invalidate, []));
        keep_file ->
            ok
    end,

    % the session holds no handle to the storage file, so writing through the lfm
    % handle can only work if the provider recreates it on the fly
    ?assertEqual({error, not_found}, get_session_handle(Node, SessId, Handle)),

    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(Node, Handle, 0, ?TEST_DATA)),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(Node, Handle, 0, byte_size(?TEST_DATA))),
    ?assertEqual(ok, lfm_proxy:close(Node, Handle)),

    ?assertEqual(false, is_file_opened(Node, FileGuid)),

    ok.


direct_io_open_registers_no_storage_handle_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(
        Node, SessId, filename:join([RootDirPath, generator:gen_name()]))),

    % a client doing the io itself gets the file registered as open, but the
    % provider opens nothing on the storage on its behalf
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), rdwr)),
    ?assertEqual({error, not_found}, get_session_handle(Node, SessId, Handle)),
    ?assertEqual(true, is_file_used_by_session(Node, FileGuid, SessId)),

    ?assertEqual(ok, lfm_proxy:close(Node, Handle)),

    ok.


%%%===================================================================
%%% Tests of a failing storage open
%%%===================================================================


open_failure_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(
        Node, SessId, filename:join([RootDirPath, generator:gen_name()]))),

    file_lfm_test_utils:with_server_side_io([SessId], fun() ->
        {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), rdwr)),
        ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(Node, Handle, 0, ?TEST_DATA)),
        ?assertEqual(ok, lfm_proxy:close(Node, Handle)),

        mock_failing_storage_open(Node),
        ?assertEqual({error, ?EAGAIN}, lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), rdwr)),
        % the open registered before it failed must have been rolled back
        ?assertEqual(false, is_file_opened(Node, FileGuid)),

        % a later open must still succeed and see everything written before
        ok = test_utils:mock_unload(Node, [storage_driver]),
        {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), rdwr)),
        ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(Node, Handle2, 0, byte_size(?TEST_DATA))),
        ?assertEqual(ok, lfm_proxy:close(Node, Handle2)),

        ?assertEqual(false, is_file_opened(Node, FileGuid))
    end),

    ok.


create_and_open_failure_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    file_lfm_test_utils:with_server_side_io([SessId], fun() ->
        OpenedFilesBefore = list_opened_files(Node),
        mock_failing_storage_open(Node),

        % a file that could not be opened right after creation must not be left behind
        FileName = generator:gen_name(),
        ?assertEqual({error, ?EAGAIN}, lfm_proxy:create_and_open(
            Node, SessId, RootDirGuid, FileName, ?DEFAULT_FILE_PERMS)),
        ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(
            Node, SessId, {path, filename:join([RootDirPath, FileName])})),
        ?assertEqual(OpenedFilesBefore, list_opened_files(Node)),

        % opening a file created beforehand fails just the same, leaving no handle
        {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(
            Node, SessId, filename:join([RootDirPath, FileName]))),
        ?assertEqual({error, ?EAGAIN}, lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), rdwr)),
        ?assertEqual(false, is_file_opened(Node, FileGuid)),
        ?assertEqual(OpenedFilesBefore, list_opened_files(Node))
    end),

    ok.


open_failure_does_not_affect_other_session_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    OtherSessId = file_lfm_test_utils:get_session_id(?OTHER_USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    FilePath = filename:join([RootDirPath, generator:gen_name()]),
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, FilePath)),

    % both sessions must have the provider do the io, so that the mocked storage
    % open is what the other session's open trips over
    file_lfm_test_utils:with_server_side_io([SessId, OtherSessId], fun() ->
        {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), rdwr)),
        ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(Node, Handle, 0, ?TEST_DATA)),

        mock_failing_storage_open(Node),
        ?assertEqual({error, ?EAGAIN}, lfm_proxy:open(Node, OtherSessId, {path, FilePath}, rdwr)),
        ?assertEqual(false, is_file_used_by_session(Node, FileGuid, OtherSessId)),
        ?assertEqual(true, is_file_used_by_session(Node, FileGuid, SessId)),

        % the handle of the session that opened the file successfully must survive
        ok = test_utils:mock_unload(Node, [storage_driver]),
        Tail = <<" and more">>,
        ?assertEqual({ok, byte_size(Tail)}, lfm_proxy:write(
            Node, Handle, byte_size(?TEST_DATA), Tail)),
        ?assertEqual({ok, <<?TEST_DATA/binary, Tail/binary>>}, lfm_proxy:read(
            Node, Handle, 0, byte_size(?TEST_DATA) + byte_size(Tail))),
        ?assertEqual(ok, lfm_proxy:close(Node, Handle)),

        ?assertEqual(false, is_file_opened(Node, FileGuid))
    end),

    ok.


%%--------------------------------------------------------------------
%% @doc
%% A move between spaces is carried out as a copy followed by a delete, so unlike
%% a move within a space it opens the file on the storage - and fails when that
%% open does.
%% @end
%%--------------------------------------------------------------------
mv_between_spaces_failure_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(
        Node, SessId, filename:join([RootDirPath, generator:gen_name()]))),

    OtherSpaceName = oct_background:get_space_name(?OTHER_SPACE_SELECTOR),
    TargetPath = filename:join([<<"/">>, OtherSpaceName, generator:gen_name()]),

    file_lfm_test_utils:with_server_side_io([SessId], fun() ->
        OpenedFilesBefore = list_opened_files(Node),
        mock_failing_storage_open(Node),

        ?assertEqual({error, ?EAGAIN}, lfm_proxy:mv(Node, SessId, ?FILE_REF(FileGuid), TargetPath)),
        ?assertEqual(OpenedFilesBefore, list_opened_files(Node))
    end),

    ok.


%%%===================================================================
%%% Tests of handles bound to the process that opened the file
%%%===================================================================


monitored_open_releases_handle_on_process_death_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {RootDirGuid, _RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    NewFile = fun() ->
        {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(
            Node, SessId, RootDirGuid, generator:gen_name(), ?DEFAULT_FILE_MODE)),
        FileGuid
    end,

    % NOTE: the file stays registered as open, rather than the provider holding a
    % handle to the storage file - the client does the io itself (see
    % direct_io_open_registers_no_storage_handle_test)

    % a file opened plainly stays open after the process that opened it dies
    PlainFileGuid = NewFile(),
    {PlainPid, _} = open_in_new_process(Node, SessId, PlainFileGuid, open),
    ?assertEqual(true, is_file_opened(Node, PlainFileGuid), ?ATTEMPTS),
    ?assertEqual(?ERROR_NOT_FOUND, get_process_handles(Node, PlainPid), ?ATTEMPTS),

    kill_and_await_death(PlainPid),
    ?assertEqual(true, is_file_opened(Node, PlainFileGuid), ?ATTEMPTS),

    % a file opened with monitoring is released along with that process
    MonitoredFileGuid = NewFile(),
    {MonitoredPid, MonitoredHandle} = open_in_new_process(
        Node, SessId, MonitoredFileGuid, monitored_open),
    ?assertEqual(true, is_file_opened(Node, MonitoredFileGuid), ?ATTEMPTS),
    ?assertEqual({ok, [MonitoredHandle]}, get_process_handles(Node, MonitoredPid), ?ATTEMPTS),

    kill_and_await_death(MonitoredPid),
    ?assertEqual(false, is_file_opened(Node, MonitoredFileGuid), ?ATTEMPTS),
    ?assertEqual(?ERROR_NOT_FOUND, get_process_handles(Node, MonitoredPid), ?ATTEMPTS),

    ok.


%%--------------------------------------------------------------------
%% @doc
%% Listing the process handles registered on the provider must span more than a
%% single batch.
%% @end
%%--------------------------------------------------------------------
list_process_handles_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {RootDirGuid, _RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    FilesNum = 230,
    BatchSize = 50,

    % NOTE: a handle refers to the file by its id on the storage, not by its guid
    ExpectedFileIds = lists:sort(lists:map(fun(_) ->
        {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(
            Node, SessId, RootDirGuid, generator:gen_name(), ?DEFAULT_FILE_MODE)),
        open_in_new_process(Node, SessId, FileGuid, monitored_open),
        {ok, #file_location{file_id = FileId}} = ?assertMatch({ok, _},
            lfm_proxy:get_file_location(Node, SessId, ?FILE_REF(FileGuid))),
        FileId
    end, lists:seq(1, FilesNum))),

    ListAll = fun ListAll(StartFromId) ->
        {ok, Docs} = ?assertMatch({ok, _}, rpc:call(
            Node, process_handles, list_docs, [StartFromId, BatchSize])),
        FileIds = lists:map(fun(#document{value = #process_handles{handles = Handles}}) ->
            ?assertEqual(1, map_size(Handles)),
            [FileHandle] = maps:values(Handles),
            lfm_context:get_file_id(FileHandle)
        end, Docs),
        case length(Docs) < BatchSize of
            true -> FileIds;
            false -> FileIds ++ ListAll((lists:last(Docs))#document.key)
        end
    end,

    ?assertEqual(ExpectedFileIds, lists:usort(ListAll(undefined)), ?ATTEMPTS),

    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec mock_failing_storage_open(node()) -> ok.
mock_failing_storage_open(Node) ->
    ok = test_utils:mock_new(Node, storage_driver, [passthrough]),
    % NOTE: the error is raised only after the open has been registered, so that
    % the rollback of that registration is what gets tested
    ok = test_utils:mock_expect(Node, storage_driver, open, fun(SDHandle, Flag) ->
        meck:passthrough([SDHandle, Flag]),
        throw(error)
    end).


%% @private
-spec open_in_new_process(node(), session:id(), file_id:file_guid(), open | monitored_open) ->
    {pid(), lfm:handle()}.
open_in_new_process(Node, SessId, FileGuid, OpenFun) ->
    Master = self(),
    Pid = spawn(Node, fun() ->
        Master ! {self(), lfm:OpenFun(SessId, ?FILE_REF(FileGuid), read)},
        receive _ -> ok end
    end),
    receive
        {Pid, {ok, Handle}} -> {Pid, Handle};
        {Pid, Error} -> ct:fail("Failed to open file: ~tp", [Error])
    after
        timer:seconds(30) -> ct:fail("Timeout awaiting the file to be opened")
    end.


%% @private
-spec kill_and_await_death(pid()) -> ok.
kill_and_await_death(Pid) ->
    MonitorRef = erlang:monitor(process, Pid),
    exit(Pid, kill),
    receive
        {'DOWN', MonitorRef, process, Pid, _} -> ok
    after
        timer:seconds(30) -> ct:fail("Timeout awaiting the process to die")
    end.


%% @private
-spec get_process_handles(node(), pid()) -> {ok, [lfm:handle()]} | errors:error().
get_process_handles(Node, Pid) ->
    rpc:call(Node, process_handles, get_all_process_handles, [Pid]).


%% @private
-spec get_session_handle(node(), session:id(), lfm:handle()) ->
    {ok, storage_driver:handle()} | {error, term()}.
get_session_handle(Node, SessId, TestHandle) ->
    Context = rpc:call(Node, ets, lookup_element, [lfm_handles, TestHandle, 2]),
    rpc:call(Node, session_handles, get, [SessId, lfm_context:get_handle_id(Context)]).


%% @private
-spec is_file_opened(node(), file_id:file_guid()) -> boolean().
is_file_opened(Node, FileGuid) ->
    rpc:call(Node, file_handles, is_file_opened, [file_id:guid_to_uuid(FileGuid)]).


%% @private
-spec is_file_used_by_session(node(), file_id:file_guid(), session:id()) -> boolean().
is_file_used_by_session(Node, FileGuid, SessId) ->
    FileCtx = rpc:call(Node, file_ctx, new_by_guid, [FileGuid]),
    rpc:call(Node, file_handles, is_used_by_session, [FileCtx, SessId]).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% The uuids of all files currently open on the provider. Compared before and
%% after an operation that must fail, this catches a handle leaked by it without
%% assuming that nothing else on the deployment holds a file open.
%% @end
%%--------------------------------------------------------------------
-spec list_opened_files(node()) -> [file_meta:uuid()].
list_opened_files(Node) ->
    {ok, Docs} = ?assertMatch({ok, _}, rpc:call(Node, file_handles, list, [])),
    lists:sort([Key || #document{key = Key} <- Docs]).
