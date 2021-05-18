%%%--------------------------------------------------------------------
%%% @author Bartek Kryza
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests archive storage based on S3 helper.
%%% @end
%%%--------------------------------------------------------------------
-module(archivestorage_helper_test_SUITE).
-author("Bartek Kryza").

-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0]).

%% tests
-export([mknod_test/1, truncate_new_test/1, truncate_increase_size_test/1,
         truncate_decrease_size_test/1, sequential_write_test/1,
         read_write_test/1, unlink_test/1]).

-define(TEST_CASES, [
    mknod_test, truncate_new_test, truncate_increase_size_test,
    truncate_decrease_size_test, sequential_write_test, read_write_test, unlink_test
]).

all() -> ?ALL(?TEST_CASES, ?TEST_CASES).

-define(S3_STORAGE_NAME, s3).
-define(S3_BUCKET_NAME, <<"onedata">>).
-define(FILE_ID_SIZE, 30).
-define(KB, 1024).
-define(MB, 1024 * 1024).
-define(TIMEOUT, timer:minutes(5)).

%%%===================================================================
%%% Test functions
%%%===================================================================
mknod_test(Config) ->
    Helper = new_helper(Config),
    FileName = random_file_id(),
    FileId = erlang:iolist_to_binary([<<"/space1/.__onedata__">>, FileName]),
    ArchiveFileId = erlang:iolist_to_binary([<<"/space1/.__onedata_archive/">>, FileName]),
    mknod(Helper, FileId, 438),
    mknod(Helper, ArchiveFileId, 438),

    ?assertMatch({ok, #statbuf{st_size = 0}}, getattr(Helper, ArchiveFileId)),

    delete_helper(Helper).


truncate_new_test(Config) ->
    Helper = new_helper(Config),
    FileName = random_file_id(),
    FileId = erlang:iolist_to_binary([<<"/space1/">>, FileName]),
    ArchiveFileId = erlang:iolist_to_binary([<<"/space1/.__onedata_archive/">>, FileName]),
    FileSize = 21 * ?MB,
    truncate(Helper, FileId, FileSize, 0),
    truncate(Helper, ArchiveFileId, FileSize, 0),

    ?assertMatch({ok, #statbuf{st_size = FileSize}}, getattr(Helper, ArchiveFileId)),

    delete_helper(Helper).


truncate_increase_size_test(Config) ->
    Helper = new_helper(Config),
    FileName = random_file_id(),
    FileId = erlang:iolist_to_binary([<<"/space1/">>, FileName]),
    ArchiveFileId = erlang:iolist_to_binary([<<"/space1/.__onedata_archive/">>, FileName]),
    FileSize = 12 * ?MB,
    truncate(Helper, FileId, FileSize, 0),
    truncate(Helper, ArchiveFileId, FileSize, 0),

    truncate(Helper, FileId, 2*FileSize, FileSize),
    truncate(Helper, ArchiveFileId, 2*FileSize, FileSize),

    TargetFileSize = 2*FileSize,
    ?assertMatch({ok, #statbuf{st_size = TargetFileSize}}, getattr(Helper, ArchiveFileId)),

    delete_helper(Helper).


truncate_decrease_size_test(Config) ->
    Helper = new_helper(Config),
    FileName = random_file_id(),
    FileId = erlang:iolist_to_binary([<<"/space1/">>, FileName]),
    ArchiveFileId = erlang:iolist_to_binary([<<"/space1/.__onedata_archive/">>, FileName]),
    FileSize = 24 * ?MB,
    truncate(Helper, FileId, FileSize, 0),
    truncate(Helper, ArchiveFileId, FileSize, 0),

    HalfSize = FileSize bsr 1,
    truncate(Helper, FileId, HalfSize, FileSize),
    truncate(Helper, ArchiveFileId, HalfSize, FileSize),

    TargetFileSize = HalfSize,
    ?assertMatch({ok, #statbuf{st_size = TargetFileSize}}, getattr(Helper, ArchiveFileId)),

    delete_helper(Helper).


sequential_write_test(Config) ->
    Helper = new_helper(Config),
    FileName = random_file_id(),
    FileId = erlang:iolist_to_binary([<<"/space1/">>, FileName]),
    ArchiveFileId = erlang:iolist_to_binary([<<"/space1/.__onedata_archive/">>, FileName]),
    ChunkSize = 1 * ?MB,

    lists:map(fun(N) ->
        Offset = N * ChunkSize,
        {ok, Handle} = open(Helper, FileId, write),
        write(Handle, Offset, ChunkSize)
    end, lists:seq(0, 24)),

    lists:map(fun(N) ->
        Offset = N * ChunkSize,
        {ok, Handle} = open(Helper, ArchiveFileId, write),
        write(Handle, Offset, ChunkSize)
    end, lists:seq(0, 24)),

    flushbuffer(Helper, ArchiveFileId, ChunkSize*25),

    TargetFileSize = ChunkSize * 25,
    ?assertMatch({ok, #statbuf{st_size = TargetFileSize}}, getattr(Helper, ArchiveFileId)),

    delete_helper(Helper).


read_write_test(Config) ->
    Helper = new_helper(Config),
    FileName = random_file_id(),
    ArchiveFileId = erlang:iolist_to_binary([<<"/space1/.__onedata_archive/">>, FileName]),
    ChunkSize = 1 * ?MB,

    lists:map(fun(N) ->
        Offset = N * ChunkSize,
        {ok, Handle} = open(Helper, ArchiveFileId, write),
        write(Handle, Offset, ChunkSize)
    end, lists:seq(0, 24)),

    {ok, Handle} = open(Helper, ArchiveFileId, write),
    DataBeforeFlush = read(Handle, 6*ChunkSize, ChunkSize),

    flushbuffer(Helper, ArchiveFileId, ChunkSize*25),

    DataAfterFlush = read(Handle, 6*ChunkSize, ChunkSize),

    ?assertMatch(DataBeforeFlush, DataAfterFlush),

    delete_helper(Helper).


unlink_test(Config) ->
    Helper = new_helper(Config),
    FileName = random_file_id(),
    ArchiveFileId = erlang:iolist_to_binary([<<"/space1/.__onedata_archive/">>, FileName]),
    FileSize = 21 * ?MB,
    truncate(Helper, ArchiveFileId, FileSize, 0),

    ?assertMatch({ok, #statbuf{st_size = FileSize}}, getattr(Helper, ArchiveFileId)),

    unlink(Helper, ArchiveFileId, FileSize),

    ?assertMatch({error,enoent}, getattr(Helper, ArchiveFileId)),

    delete_helper(Helper).


%%%===================================================================
%%% Internal functions
%%%===================================================================
new_helper(Config) ->
    process_flag(trap_exit, true),
    [Node | _] = ?config(op_worker_nodes, Config),
    S3Config = ?config(s3, ?config(s3, ?config(storages, Config))),

    UserCtx = #{
        <<"accessKey">> => atom_to_binary(?config(access_key, S3Config), utf8),
        <<"secretKey">> => atom_to_binary(?config(secret_key, S3Config), utf8)
    },
    {ok, Helper} = helper:new_helper(
        <<"s3">>,
        #{
            <<"hostname">> => atom_to_binary(?config(host_name, S3Config), utf8),
            <<"bucketName">> => ?S3_BUCKET_NAME,
            <<"scheme">> => <<"http">>,
            <<"storagePathType">> => ?FLAT_STORAGE_PATH,
            <<"blockSize">> => list_to_binary(integer_to_list(5 * ?MB)),
            <<"skipStorageDetection">> => <<"true">>,
            <<"archiveStorage">> => <<"true">>
        },
        UserCtx
    ),

    spawn(Node, fun() ->
        helper_loop(Helper, UserCtx)
    end).

delete_helper(Helper) ->
    Helper ! exit.

helper_loop(Helper, UserCtx) ->
    Handle = helpers:get_helper_handle(Helper, UserCtx),
    helper_loop(Handle).

helper_loop(Handle) ->
    receive
        exit ->
            ok;

        {Pid, {run_helpers, open, Args}} ->
            {ok, FileHandle} = apply(helpers, open, [Handle | Args]),
            HandlePid = spawn_link(fun() -> helper_handle_loop(FileHandle) end),
            Pid ! {self(), {ok, HandlePid}},
            helper_loop(Handle);

        {Pid, {run_helpers, Method, Args}} ->
            Pid ! {self(), apply(helpers, Method, [Handle | Args])},
            helper_loop(Handle)
    end.

helper_handle_loop(FileHandle) ->
    process_flag(trap_exit, true),
    receive
        {'EXIT', _, _} ->
            helpers:release(FileHandle);

        {Pid, {run_helpers, Method, Args}} ->
            Pid ! {self(), apply(helpers, Method, [FileHandle | Args])},
            helper_handle_loop(FileHandle)
    end.

call(Helper, Method, Args) ->
    Helper ! {self(), {run_helpers, Method, Args}},
    receive_result(Helper).

receive_result(Helper) ->
    receive
        {'EXIT', Helper, normal} -> receive_result(Helper);
        {'EXIT', Helper, Reason} -> {error, Reason};
        {Helper, Ans} -> Ans
    after
        ?TIMEOUT -> {error, timeout}
    end.

random_file_id() ->
    re:replace(http_utils:base64url_encode(crypto:strong_rand_bytes(?FILE_ID_SIZE)),
        "\\W", "", [global, {return, binary}]).

open(Helper, FileId, Flag) ->
    call(Helper, open, [FileId, Flag]).

read(FileHandle, Offset, Size) ->
    {ok, Content} =
        ?assertMatch({ok, _}, call(FileHandle, read, [Offset, Size])),
    Content.

write(FileHandle, Offset, Size) ->
    Content = crypto:strong_rand_bytes(Size),
    ActualSize = size(Content),
    ?assertEqual({ok, ActualSize}, call(FileHandle, write, [Offset, Content])),
    Content.

getattr(Helper, FileId) ->
    call(Helper, getattr, [FileId]).

truncate(Helper, FileId, Size, CurrentSize) ->
    ?assertEqual(ok, call(Helper, truncate, [FileId, Size, CurrentSize])).

mknod(Helper, FileId, Mode) ->
    ?assertEqual(ok, call(Helper, mknod, [FileId, Mode, reg])).

flushbuffer(Helper, FileId, CurrentSize) ->
    ?assertEqual(ok, call(Helper, flushbuffer, [FileId, CurrentSize])).

unlink(Helper, FileId, CurrentSize) ->
    ?assertEqual(ok, call(Helper, unlink, [FileId, CurrentSize])).
