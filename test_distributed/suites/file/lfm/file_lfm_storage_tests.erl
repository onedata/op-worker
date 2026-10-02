%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of how lfm operations relate to the file actually present on the
%%% storage behind them. Creating a file only writes its metadata - the storage
%%% file is created later, when the file is opened - and the operations that may
%%% happen in between must cope with it not being there yet. Ranges never written
%%% to are not stored at all, which makes the file sparse.
%%%
%%% The bodies are shared by file_lfm_posix_test_SUITE and file_lfm_s3_test_SUITE.
%%% Each test works within its own, randomly named directory in the space, which
%%% the suites empty between test cases.
%%% @end
%%%-------------------------------------------------------------------
-module(file_lfm_storage_tests).
-author("Bartosz Walkowicz").

% This module indirectly includes eunit.hrl, whose parse transform would
% otherwise auto-export every arity 0 function named *_test - clashing with
% the export list below.
-define(EUNIT_NOAUTO, 1).

-include("file/file_lfm_test.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% tests
-export([
    storage_file_is_created_on_open_test/0,
    mv_before_storage_file_creation_test/0,
    truncate_before_storage_file_creation_test/0,

    recreate_missing_storage_file_on_open_test/0,

    sparse_files_test/1
]).

-define(TEST_DATA, <<"test_data">>).

% Writes are applied asynchronously to the file size, so a stat right after one
% may still report the previous size.
-define(SIZE_UPDATE_ATTEMPTS, 10).


%%%===================================================================
%%% Tests of the deferred storage file creation
%%%===================================================================


storage_file_is_created_on_open_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(
        Node, SessId, filename:join([RootDirPath, generator:gen_name()]))),

    % creating the file writes its metadata only
    SDHandle = build_storage_driver_handle(Node, SessId, FileGuid),
    ?assertEqual({error, ?ENOENT}, stat_on_storage(Node, SDHandle)),

    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), rdwr)),
    ?assertMatch({ok, _}, stat_on_storage(Node, SDHandle)),

    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(Node, Handle, 0, ?TEST_DATA)),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(Node, Handle, 0, byte_size(?TEST_DATA))),
    ?assertEqual(ok, lfm_proxy:close(Node, Handle)),

    ok.


mv_before_storage_file_creation_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(
        Node, SessId, filename:join([RootDirPath, generator:gen_name()]))),

    ?assertMatch({ok, _}, lfm_proxy:mv(
        Node, SessId, ?FILE_REF(FileGuid), filename:join([RootDirPath, generator:gen_name()]))),

    assert_writable(Node, SessId, FileGuid),

    ok.


truncate_before_storage_file_creation_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(
        Node, SessId, filename:join([RootDirPath, generator:gen_name()]))),

    ?assertEqual(ok, lfm_proxy:truncate(Node, SessId, ?FILE_REF(FileGuid), 10)),
    ?assertEqual(ok, fsync(Node, SessId, FileGuid)),
    ?assertMatch({ok, #file_attr{size = 10}},
        lfm_proxy:stat(Node, SessId, ?FILE_REF(FileGuid)), ?SIZE_UPDATE_ATTEMPTS),

    assert_writable(Node, SessId, FileGuid),

    ok.


recreate_missing_storage_file_on_open_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {RootDirGuid, _RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    % the provider must open the storage file as soon as the file is opened - for a
    % client doing direct io it would do so only upon the first read, which is
    % where the missing storage file would get recreated instead
    file_lfm_test_utils:with_server_side_io([SessId], fun() ->
        % with the storage driver stubbed out, only the metadata of the file is written
        ok = test_utils:mock_new(Node, storage_driver),
        ok = test_utils:mock_expect(Node, storage_driver, create, fun(_SDHandle, _Mode) -> ok end),
        ok = test_utils:mock_expect(Node, storage_driver, open, fun(SDHandle, _Flag) -> {ok, SDHandle} end),
        ok = test_utils:mock_expect(Node, storage_driver, release, fun(_SDHandle) -> ok end),

        {ok, {FileGuid, Handle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(
            Node, SessId, RootDirGuid, generator:gen_name(), undefined)),
        ?assertEqual(ok, lfm_proxy:close(Node, Handle)),
        ok = test_utils:mock_unload(Node, [storage_driver]),

        % opening the file must recreate the missing storage file rather than fail
        {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), read)),
        ?assertEqual({ok, <<>>}, lfm_proxy:read(Node, Handle2, 0, 10)),
        ?assertEqual(ok, lfm_proxy:close(Node, Handle2))
    end),

    ok.


%%%===================================================================
%%% Tests of sparse files
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% ReadFun differs between the suites: on an object storage a plain read is not
%% bounded by the file size, so the s3 suite reads through check_size_and_read.
%% @end
%%--------------------------------------------------------------------
-spec sparse_files_test(read | check_size_and_read) -> ok.
sparse_files_test(ReadFun) ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {_RootDirGuid, RootDirPath} = file_lfm_test_utils:create_test_root_dir(Node, SessId),

    NewFile = fun() ->
        {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(
            Node, SessId, filename:join([RootDirPath, generator:gen_name()]))),
        FileGuid
    end,
    WriteByte = fun(FileGuid, Offset) ->
        file_ops_test_utils:write_byte_to_file(Node, SessId, FileGuid, Offset)
    end,
    EmptyWrite = fun(FileGuid, Offset) ->
        file_ops_test_utils:empty_write_to_file(Node, SessId, FileGuid, Offset)
    end,
    TruncateTo = fun(FileGuid, Size) ->
        ?assertEqual(ok, lfm_proxy:truncate(Node, SessId, ?FILE_REF(FileGuid), Size)),
        ?assertEqual(ok, fsync(Node, SessId, FileGuid))
    end,
    AssertSparse = fun(FileGuid, FileSize, ExpectedBlocks) ->
        assert_sparse_file(ReadFun, Node, SessId, FileGuid, FileSize, ExpectedBlocks)
    end,

    % hole between two written blocks
    FileGuid1 = NewFile(),
    WriteByte(FileGuid1, 0),
    WriteByte(FileGuid1, 10),
    AssertSparse(FileGuid1, 11, [[0, 1], [10, 1]]),

    % hole before the only written block
    FileGuid2 = NewFile(),
    WriteByte(FileGuid2, 10),
    AssertSparse(FileGuid2, 11, [[10, 1]]),

    % an empty write past the end of a non empty file
    FileGuid3 = NewFile(),
    WriteByte(FileGuid3, 0),
    EmptyWrite(FileGuid3, 10),
    AssertSparse(FileGuid3, 10, [[0, 1]]),

    % an empty write past the end of an empty file
    FileGuid4 = NewFile(),
    EmptyWrite(FileGuid4, 10),
    AssertSparse(FileGuid4, 10, []),

    % an empty write in the middle of a non empty file
    FileGuid5 = NewFile(),
    WriteByte(FileGuid5, 10),
    EmptyWrite(FileGuid5, 5),
    AssertSparse(FileGuid5, 11, [[10, 1]]),

    % a hole made by truncating a non empty file up
    FileGuid6 = NewFile(),
    WriteByte(FileGuid6, 0),
    TruncateTo(FileGuid6, 10),
    AssertSparse(FileGuid6, 10, [[0, 1]]),

    % a hole made by truncating an empty file up
    FileGuid7 = NewFile(),
    TruncateTo(FileGuid7, 10),
    AssertSparse(FileGuid7, 10, []),

    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec assert_sparse_file(read | check_size_and_read, node(), session:id(),
    file_id:file_guid(), file_meta:size(), [[non_neg_integer()]]) -> ok.
assert_sparse_file(ReadFun, Node, SessId, FileGuid, FileSize, ExpectedBlocks) ->
    BlocksSize = lists:foldl(fun([_, Size], Acc) -> Acc + Size end, 0, ExpectedBlocks),
    ?assertMatch({ok, [#{<<"blocks">> := ExpectedBlocks, <<"totalBlocksSize">> := BlocksSize}]},
        opt_file_metadata:get_distribution_deprecated(Node, SessId, ?FILE_REF(FileGuid))),
    ?assertMatch({ok, #file_attr{size = FileSize}}, lfm_proxy:stat(Node, SessId, ?FILE_REF(FileGuid))),

    ExpectedContent = file_ops_test_utils:get_sparse_file_content(ExpectedBlocks, FileSize),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), rdwr)),
    ?assertEqual({ok, ExpectedContent}, lfm_proxy:ReadFun(Node, Handle, 0, 100)),
    ?assertEqual(ok, lfm_proxy:close(Node, Handle)),
    ok.


%% @private
-spec assert_writable(node(), session:id(), file_id:file_guid()) -> ok.
assert_writable(Node, SessId, FileGuid) ->
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), rdwr)),
    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(Node, Handle, 0, ?TEST_DATA)),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(Node, Handle, 0, byte_size(?TEST_DATA))),
    ?assertEqual(ok, lfm_proxy:close(Node, Handle)),
    ok.


%% @private
-spec build_storage_driver_handle(node(), session:id(), file_id:file_guid()) ->
    storage_driver:handle().
build_storage_driver_handle(Node, SessId, FileGuid) ->
    FileCtx = rpc:call(Node, file_ctx, new_by_guid, [FileGuid]),
    {SDHandle, _} = rpc:call(Node, storage_driver, new_handle, [SessId, FileCtx]),
    SDHandle.


%% @private
-spec stat_on_storage(node(), storage_driver:handle()) ->
    {ok, file_attr:record()} | {error, term()}.
stat_on_storage(Node, SDHandle) ->
    rpc:call(Node, storage_driver, stat, [SDHandle]).


%% @private
-spec fsync(node(), session:id(), file_id:file_guid()) -> ok | {error, term()}.
fsync(Node, SessId, FileGuid) ->
    lfm_proxy:fsync(Node, SessId, ?FILE_REF(FileGuid),
        oct_background:get_provider_id(?PROVIDER_SELECTOR)).
