%%%-------------------------------------------------------------------
%%% @author Bartek Kryza
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Tests for GlusterFS helper.
%%% @end
%%%-------------------------------------------------------------------
-module(glusterfs_helper_test_SUITE).
-author("Bartek Kryza").

-include("modules/storage_file_manager/helpers/helpers.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0]).

%% tests
-export([create_test/1, mkdir_test/1, getattr_test/1, rmdir_test/1,
    unlink_test/1, symlink_test/1, rename_test/1, chmod_test/1,
    chown_test/1, flush_test/1, fsync_test/1, setxattr_test/1,
    removexattr_test/1, listxattr_test/1, write_test/1,
    multipart_write_test/1, truncate_test/1, write_read_test/1,
    multipart_read_test/1, write_unlink_test/1,
    write_read_truncate_unlink_test/1]).

%% test_bases
-export([create_test_base/1, write_test_base/1, multipart_write_test_base/1,
    truncate_test_base/1, write_read_test_base/1, multipart_read_test_base/1,
    write_unlink_test_base/1, write_read_truncate_unlink_test_base/1]).

-define(PERF_TEST_CASES, [
    create_test, write_test, multipart_write_test, truncate_test,
    write_read_test, multipart_read_test, write_unlink_test,
    write_read_truncate_unlink_test
]).

-define(TEST_CASES, [
    getattr_test, mkdir_test, rmdir_test, unlink_test, symlink_test,
    rename_test, chmod_test, chown_test, flush_test, fsync_test,
    setxattr_test, listxattr_test, removexattr_test
]).

all() -> ?ALL(?TEST_CASES, ?PERF_TEST_CASES).

-define(GLUSTERFS_PORT, 24007).
-define(GLUSTERFS_VOLUME, <<"data">>).

-define(FILE_ID_SIZE, 20).
-define(KB, 1024).
-define(MB, 1024 * 1024).
-define(TIMEOUT, timer:minutes(5)).

-define(THR_NUM(Value), [
    {name, threads_num}, {value, Value}, {description, "Number of threads."}
]).
-define(OP_NUM(Value), lists:keyreplace(description, 1, ?OP_NUM(op, Value),
    {description, "Number of operations."}
)).
-define(OP_NUM(Name, Value), [
    {name, list_to_atom(atom_to_list(Name) ++ "_num")},
    {value, Value},
    {description, "Number of " ++ atom_to_list(Name) ++ " operations."}
]).
-define(OP_SIZE(Value), lists:keyreplace(description, 1, ?OP_SIZE(op, Value),
    {description, "Size of single operation."}
)).
-define(OP_SIZE(Name, Value), [
    {name, list_to_atom(atom_to_list(Name) ++ "_size")},
    {value, Value},
    {description, "Size of single " ++ atom_to_list(Name) ++ " operation."},
    {unit, "MB"}
]).
-define(OP_BLK_SIZE(Name, Value), [
    {name, list_to_atom(atom_to_list(Name) ++ "_blk_size")},
    {value, Value},
    {description, "Size of " ++ atom_to_list(Name) ++ " operation block."},
    {unit, "KB"}
]).
-define(PERF_CFG(Name, Params), ?PERF_CFG(Name, "", Params)).
-define(PERF_CFG(Name, Description, Params), {config, [
    {name, Name},
    {description, Description},
    {parameters, Params}
]}).

% TODO - change to 10 when seg fault is fixed
-define(REPEATS, 1).
% TODO - change to 5 when seg fault is fixed
-define(TEST_SIZE_BASE, 1).

%%%===================================================================
%%% Test functions
%%%===================================================================
create_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, 100},
        {parameters, [?THR_NUM(1), ?OP_NUM(write, 5), ?OP_SIZE(write, 1)]},
        {description, "Multiple parallel write operations."},
        ?PERF_CFG(small, [?THR_NUM(?TEST_SIZE_BASE), ?OP_NUM(write, 2 * ?TEST_SIZE_BASE), ?OP_SIZE(write, 1)]),
        ?PERF_CFG(medium, [?THR_NUM(2 * ?TEST_SIZE_BASE), ?OP_NUM(write, 2 * ?TEST_SIZE_BASE), ?OP_SIZE(write, 1)]),
        ?PERF_CFG(large, [?THR_NUM(4 * ?TEST_SIZE_BASE), ?OP_NUM(write, 2 * ?TEST_SIZE_BASE), ?OP_SIZE(write, 1)])
    ]).
create_test_base(Config) ->
    run(fun() ->
        Helper = new_helper(Config),
        lists:foreach(fun(_) ->
            FileId = random_file_id(),
            create(Helper, FileId)
        end, lists:seq(1, ?config(write_num, Config))),
        delete_helper(Helper)
    end, ?config(threads_num, Config)).

write_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, 100},
        {parameters, [?THR_NUM(1), ?OP_NUM(write, 5), ?OP_SIZE(write, 1)]},
        {description, "Multiple parallel write operations."},
        ?PERF_CFG(small, [?THR_NUM(?TEST_SIZE_BASE), ?OP_NUM(write, 2 * ?TEST_SIZE_BASE), ?OP_SIZE(write, 1)]),
        ?PERF_CFG(medium, [?THR_NUM(2 * ?TEST_SIZE_BASE), ?OP_NUM(write, 2 * ?TEST_SIZE_BASE), ?OP_SIZE(write, 1)]),
        ?PERF_CFG(large, [?THR_NUM(4 * ?TEST_SIZE_BASE), ?OP_NUM(write, 2 * ?TEST_SIZE_BASE), ?OP_SIZE(write, 1)])
    ]).
write_test_base(Config) ->
    run(fun() ->
        Helper = new_helper(Config),
        lists:foreach(fun(_) ->
            FileId = random_file_id(),
            create(Helper, FileId),
            {ok, Handle} = open(Helper, FileId, write),
            write(Handle, ?config(write_size, Config) * ?MB)
        end, lists:seq(1, ?config(write_num, Config))),
        delete_helper(Helper)
    end, ?config(threads_num, Config)).


multipart_write_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, 100},
        {parameters, [?OP_SIZE(write, 1), ?OP_BLK_SIZE(write, 4)]},
        {description, "Multipart write operation."},
        ?PERF_CFG(small, [?OP_SIZE(write, 2 * ?TEST_SIZE_BASE), ?OP_BLK_SIZE(write, ?TEST_SIZE_BASE)]),
        ?PERF_CFG(medium, [?OP_SIZE(write, 10 * ?TEST_SIZE_BASE), ?OP_BLK_SIZE(write, ?TEST_SIZE_BASE)]),
        ?PERF_CFG(large, [?OP_SIZE(write, 20 * ?TEST_SIZE_BASE), ?OP_BLK_SIZE(write, ?TEST_SIZE_BASE)])
    ]).
multipart_write_test_base(Config) ->
    Helper = new_helper(Config),
    FileId = random_file_id(),
    create(Helper, FileId),
    {ok, Handle} = open(Helper, FileId, write),
    Size = ?config(write_size, Config) * ?MB,
    BlockSize = ?config(write_blk_size, Config) * ?KB,
    multipart(Handle, fun write/3, Size, BlockSize),
    delete_helper(Helper).

truncate_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, 100},
        {parameters, [?THR_NUM(1), ?OP_NUM(truncate, 5)]},
        {description, "Multiple parallel truncate operations."},
        ?PERF_CFG(small, [?THR_NUM(?TEST_SIZE_BASE), ?OP_NUM(truncate, 2 * ?TEST_SIZE_BASE)]),
        ?PERF_CFG(medium, [?THR_NUM(2 * ?TEST_SIZE_BASE), ?OP_NUM(truncate, 20 * ?TEST_SIZE_BASE)]),
        ?PERF_CFG(large, [?THR_NUM(4 * ?TEST_SIZE_BASE), ?OP_NUM(truncate, 20 * ?TEST_SIZE_BASE)])
    ]).
truncate_test_base(Config) ->
    run(fun() ->
        Helper = new_helper(Config),
        lists:foreach(fun(_) ->
            truncate(Helper, 0, 0)
        end, lists:seq(1, ?config(truncate_num, Config))),
        delete_helper(Helper)
    end, ?config(threads_num, Config)).

write_read_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, 100},
        {parameters, [?THR_NUM(1), ?OP_NUM(5), ?OP_SIZE(1)]},
        {description, "Multiple parallel write followed by read operations."},
        ?PERF_CFG(small, [?THR_NUM(?TEST_SIZE_BASE), ?OP_NUM(2 * ?TEST_SIZE_BASE), ?OP_SIZE(1)]),
        ?PERF_CFG(medium, [?THR_NUM(2 * ?TEST_SIZE_BASE), ?OP_NUM(2 * ?TEST_SIZE_BASE), ?OP_SIZE(1)]),
        ?PERF_CFG(large, [?THR_NUM(4 * ?TEST_SIZE_BASE), ?OP_NUM(2 * ?TEST_SIZE_BASE), ?OP_SIZE(1)])
    ]).
write_read_test_base(Config) ->
    run(fun() ->
        Helper = new_helper(Config),
        lists:foreach(fun(_) ->
            FileId = random_file_id(),
            create(Helper, FileId),
            {ok, Handle} = open(Helper, FileId, rdwr),
            Content = write(Handle, 0, ?config(op_size, Config) * ?MB),
            ?assertEqual(Content, read(Handle, size(Content)))
        end, lists:seq(1, ?config(op_num, Config))),
        delete_helper(Helper)
    end, ?config(threads_num, Config)).

multipart_read_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, 100},
        {parameters, [?OP_SIZE(read, 1), ?OP_BLK_SIZE(read, 4)]},
        {description, "Multipart read operation."},
        ?PERF_CFG(small, [?OP_SIZE(read, 2 * ?TEST_SIZE_BASE), ?OP_BLK_SIZE(read, ?TEST_SIZE_BASE)]),
        ?PERF_CFG(medium, [?OP_SIZE(read, 10 * ?TEST_SIZE_BASE), ?OP_BLK_SIZE(read, ?TEST_SIZE_BASE)]),
        ?PERF_CFG(large, [?OP_SIZE(read, 20 * ?TEST_SIZE_BASE), ?OP_BLK_SIZE(read, ?TEST_SIZE_BASE)])
    ]).
multipart_read_test_base(Config) ->
    Helper = new_helper(Config),
    FileId = random_file_id(),
    create(Helper, FileId),
    {ok, Handle} = open(Helper, FileId, read),
    Size = ?config(read_size, Config) * ?MB,
    BlockSize = ?config(read_blk_size, Config) * ?KB,
    write(Handle, 0, 0),
    truncate(Helper, FileId, Size, 0),
    multipart(Handle, fun read/3, 0, Size, BlockSize),
    delete_helper(Helper).

write_unlink_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, 100},
        {parameters, [?THR_NUM(1), ?OP_NUM(5), ?OP_SIZE(1)]},
        {description, "Multiple parallel write followed by unlink operations."},
        ?PERF_CFG(small, [?THR_NUM(?TEST_SIZE_BASE), ?OP_NUM(2 * ?TEST_SIZE_BASE), ?OP_SIZE(1)]),
        ?PERF_CFG(medium, [?THR_NUM(2 * ?TEST_SIZE_BASE), ?OP_NUM(2 * ?TEST_SIZE_BASE), ?OP_SIZE(1)]),
        ?PERF_CFG(large, [?THR_NUM(4 * ?TEST_SIZE_BASE), ?OP_NUM(2 * ?TEST_SIZE_BASE), ?OP_SIZE(1)])
    ]).
write_unlink_test_base(Config) ->
    run(fun() ->
        Helper = new_helper(Config),
        lists:foreach(fun(_) ->
            FileId = random_file_id(),
            Size = ?config(op_size, Config) * ?MB,
            create(Helper, FileId),
            {ok, Handle} = open(Helper, FileId, write),
            write(Handle, 0, Size),
            unlink(Helper, FileId, Size)
        end, lists:seq(1, ?config(op_num, Config))),
        delete_helper(Helper)
    end, ?config(threads_num, Config)).

write_read_truncate_unlink_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, 100},
        {parameters, [?THR_NUM(1), ?OP_NUM(5), ?OP_SIZE(1)]},
        {description, "Multiple parallel sequence of write, read, truncate
        and unlink operations."},
        ?PERF_CFG(small, [?THR_NUM(?TEST_SIZE_BASE), ?OP_NUM(2 * ?TEST_SIZE_BASE), ?OP_SIZE(1)]),
        ?PERF_CFG(medium, [?THR_NUM(2 * ?TEST_SIZE_BASE), ?OP_NUM(2 * ?TEST_SIZE_BASE), ?OP_SIZE(1)]),
        ?PERF_CFG(large, [?THR_NUM(4 * ?TEST_SIZE_BASE), ?OP_NUM(2 * ?TEST_SIZE_BASE), ?OP_SIZE(1)])
    ]).
write_read_truncate_unlink_test_base(Config) ->
    run(fun() ->
        Helper = new_helper(Config),
        lists:foreach(fun(_) ->
            FileId = random_file_id(),
            create(Helper, FileId),
            {ok, Handle} = open(Helper, FileId, rdwr),
            Size = ?config(op_size, Config) * ?MB,
            Content = write(Handle, 0, Size),
            ?assertEqual(Content, read(Handle, size(Content))),
            truncate(Helper, FileId, 0, Size),
            unlink(Helper, FileId, 0)
        end, lists:seq(1, ?config(op_num, Config))),
        delete_helper(Helper)
    end, ?config(threads_num, Config)).

getattr_test(Config) ->
    Helper = new_helper(Config),
    FileId = random_file_id(),
    create(Helper, FileId),
    ?assertMatch({ok, #statbuf{}}, call(Helper, getattr, [FileId])).

mkdir_test(Config) ->
    Helper = new_helper(Config),
    DirId = random_file_id(),
    ?assertMatch(ok, mkdir(Helper, DirId)).

rmdir_test(Config) ->
    Helper = new_helper(Config),
    DirId = random_file_id(),
    ?assertMatch(ok, mkdir(Helper, DirId)),
    ?assertMatch(ok, call(Helper, rmdir, [DirId])).

unlink_test(Config) ->
    Helper = new_helper(Config),
    FileId = random_file_id(),
    create(Helper, FileId),
    ?assertMatch(ok, call(Helper, unlink, [FileId, 0])).

symlink_test(Config) ->
    Helper = new_helper(Config),
    FileId = random_file_id(),
    SymLinkId = random_file_id(),
    create(Helper, FileId),
    ?assertMatch(ok, call(Helper, symlink, [FileId, SymLinkId])).

rename_test(Config) ->
    Helper = new_helper(Config),
    FileId = random_file_id(),
    NewFileId = random_file_id(),
    create(Helper, FileId),
    ?assertMatch(ok, call(Helper, rename, [FileId, NewFileId])),
    ?assertMatch({ok, _}, open(Helper, NewFileId, read)).

chmod_test(Config) ->
    Helper = new_helper(Config),
    FileId = random_file_id(),
    create(Helper, FileId),
    ?assertMatch(ok, call(Helper, chmod, [FileId, 0])).

chown_test(Config) ->
    Helper = new_helper(Config),
    FileId = random_file_id(),
    create(Helper, FileId),
    ?assertMatch(ok, call(Helper, chown, [FileId, -1, -1])).

setxattr_test(Config) ->
    Helper = new_helper(Config),
    FileId = random_file_id(),
    XattrName = str_utils:join_binary([<<"user.">>, random_file_id()]),
    XattrValue = random_file_id(),
    create(Helper, FileId),
    ?assertMatch(ok,
        call(Helper, setxattr, [FileId, XattrName, XattrValue, false, false])),
    ?assertMatch({ok, XattrValue}, call(Helper, getxattr, [FileId, XattrName])).

listxattr_test(Config) ->
    Helper = new_helper(Config),
    FileId = random_file_id(),
    create(Helper, FileId),
    ?assertMatch(ok,
        call(Helper, setxattr,
            [FileId, <<"user.XATTR1">>, random_file_id(), false, false])),
    ?assertMatch(ok,
        call(Helper, setxattr,
            [FileId, <<"user.XATTR2">>, random_file_id(), false, false])),
    ?assertMatch(ok,
        call(Helper, setxattr,
            [FileId, <<"user.XATTR3">>, random_file_id(), false, false])),
    {ok, XattrNames} = call(Helper, listxattr, [FileId]),
    ?assertEqual(3, length(XattrNames)).

removexattr_test(Config) ->
    Helper = new_helper(Config),
    FileId = random_file_id(),
    XattrName = str_utils:join_binary([<<"user.">>, random_file_id()]),
    XattrValue = random_file_id(),
    create(Helper, FileId),
    ?assertMatch(ok,
        call(Helper, setxattr, [FileId, XattrName, XattrValue, false, false])),
    {ok, XattrNames} = call(Helper, listxattr, [FileId]),
    ?assertEqual(1, length(XattrNames)),
    ?assertMatch(ok, call(Helper, removexattr, [FileId, XattrName])),
    ?assertMatch({ok, []}, call(Helper, listxattr, [FileId])).

flush_test(Config) ->
    Helper = new_helper(Config),
    FileId = random_file_id(),
    create(Helper, FileId),
    {ok, Handle} = call(Helper, open, [FileId, write]),
    write(Handle, 0, 1024),
    ?assertMatch(ok, call(Handle, flush, [])).

fsync_test(Config) ->
    Helper = new_helper(Config),
    FileId = random_file_id(),
    create(Helper, FileId),
    {ok, Handle} = call(Helper, open, [FileId, write]),
    write(Handle, 0, 1024),
    ?assertMatch(ok, call(Handle, fsync, [false])).


%%%===================================================================
%%% Internal functions
%%%===================================================================

new_helper(Config) ->
    process_flag(trap_exit, true),
    [Node | _] = ?config(op_worker_nodes, Config),
    GlusterFSConfig = ?config(glusterfs, ?config(glusterfs, ?config(storages, Config))),
    UserCtx = helper:new_glusterfs_user_ctx(0, 0),
    Helper = helper:new_glusterfs_helper(
        ?GLUSTERFS_VOLUME,
        atom_to_binary(?config(host_name, GlusterFSConfig), utf8),
        #{
            <<"port">> => integer_to_binary(?GLUSTERFS_PORT),
            <<"transport">> => atom_to_binary(?config(transport, GlusterFSConfig), utf8),
            <<"mountPoint">> => atom_to_binary(?config(mountpoint, GlusterFSConfig), utf8),
            <<"xlatorOptions">> => <<"cluster.write-freq-threshold=100;">>
        },
        UserCtx,
        true,
        ?CANONICAL_STORAGE_PATH
      ),
    spawn_link(Node, fun() ->
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

run(Fun, ThreadsNum) ->
    Results = utils:pmap(fun(_) ->
        Fun(),
        ok
    end, lists:seq(1, ThreadsNum)),
    ?assert(lists:all(fun(Result) -> Result =:= ok end, Results)).

random_file_id() ->
    http_utils:url_encode(base64:encode(crypto:strong_rand_bytes(?FILE_ID_SIZE))).

create(Helper, FileId) ->
    call(Helper, mknod, [FileId, 8#644, reg]).

mkdir(Helper, FileId) ->
    call(Helper, mkdir, [FileId, 8#755]).

open(Helper, FileId, Flag) ->
    call(Helper, open, [FileId, Flag]).

read(FileHandle, Size) ->
    read(FileHandle, 0, Size).

read(FileHandle, Offset, Size) ->
    {ok, Content} =
        ?assertMatch({ok, _}, call(FileHandle, read, [Offset, Size])),
    Content.

write(FileHandle, Size) ->
    write(FileHandle, 0, Size).

write(FileHandle, Offset, Size) ->
    Content = crypto:strong_rand_bytes(Size),
    ActualSize = size(Content),
    ?assertEqual({ok, ActualSize}, call(FileHandle, write, [Offset, Content])),
    Content.

truncate(Helper, Size, CurrentSize) ->
    FileId = random_file_id(),
    create(Helper, FileId),
    truncate(Helper, FileId, Size, CurrentSize).

truncate(Helper, FileId, Size, CurrentSize) ->
    ?assertEqual(ok, call(Helper, truncate, [FileId, Size, CurrentSize])).

unlink(Helper, FileId, CurrentSize) ->
    ?assertEqual(ok, call(Helper, unlink, [FileId, CurrentSize])).

multipart(Helper, Method, Size, BlockSize) ->
    multipart(Helper, Method, 0, Size, BlockSize).

multipart(_Helper, _Method, _Offset, 0, _BlockSize) ->
    ok;
multipart(Helper, Method, Offset, Size, BlockSize)
    when Size >= BlockSize ->
    Method(Helper, Offset, BlockSize),
    multipart(Helper, Method, Offset + BlockSize, Size - BlockSize, BlockSize);
multipart(Helper, Method, Offset, Size, BlockSize) ->
    Method(Helper, Offset, Size),
    multipart(Helper, Method, Offset + Size, 0, BlockSize).
