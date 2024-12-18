%%%--------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests Swift helper.
%%% @end
%%%--------------------------------------------------------------------
-module(swift_helper_test_SUITE).
-author("Michal Wrona").

-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0]).

%% tests
-export([write_test/1, multipart_write_test/1,
    truncate_test/1, write_read_test/1, multipart_read_test/1,
    write_unlink_test/1, write_read_truncate_unlink_test/1,
    check_storage_availability_test/1]).

%% test_bases
-export([write_test_base/1, multipart_write_test_base/1,
    truncate_test_base/1, write_read_test_base/1, multipart_read_test_base/1,
    write_unlink_test_base/1, write_read_truncate_unlink_test_base/1]).

-define(TEST_CASES, [
    write_test, multipart_write_test, truncate_test,
    write_read_test, multipart_read_test, write_unlink_test,
    write_read_truncate_unlink_test, check_storage_availability_test
]).

all() -> ?ALL(?TEST_CASES, ?TEST_CASES).

-define(SWIFT_STORAGE_NAME, swift).
-define(SWIFT_CONTAINER_NAME, <<"onedata">>).
-define(FILE_ID_SIZE, 30).
-define(KB, 1024).
-define(MB, 1024 * 1024).
-define(TIMEOUT, timer:minutes(5)).

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

-define(REPEATS, 10).
-define(TEST_SIZE_BASE, 5).

%%%===================================================================
%%% Test functions
%%%===================================================================

check_storage_availability_test(Config) ->
    Helper = new_helper(Config),
    ?assertMatch(ok, call(Helper, check_storage_availability, [])).

write_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, 100},
        {parameters, [?OP_NUM(write, 1), ?OP_SIZE(write, 1)]},
        {description, "Multiple write operations."},
        ?PERF_CFG(small, [?OP_NUM(write, 2 * ?TEST_SIZE_BASE), ?OP_SIZE(write, 1)]),
        ?PERF_CFG(medium, [?OP_NUM(write, 4 * ?TEST_SIZE_BASE), ?OP_SIZE(write, 1)]),
        ?PERF_CFG(large, [?OP_NUM(write, 10 * ?TEST_SIZE_BASE), ?OP_SIZE(write, 1)])
    ]).
write_test_base(Config) ->
    Helper = new_helper(Config),
    lists:foreach(fun(_) ->
        FileId = random_file_id(),
        {ok, Handle} = open(Helper, FileId, write),
        write(Handle, ?config(write_size, Config) * ?MB)
    end, lists:seq(1, ?config(write_num, Config))),
    delete_helper(Helper).

multipart_write_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, 100},
        {parameters, [?OP_SIZE(write, 1), ?OP_BLK_SIZE(write, 4)]},
        {description, "Multipart write operation."},
        ?PERF_CFG(small, [?OP_SIZE(write, 1), ?OP_BLK_SIZE(write, ?TEST_SIZE_BASE)]),
        ?PERF_CFG(medium, [?OP_SIZE(write, 2), ?OP_BLK_SIZE(write, ?TEST_SIZE_BASE)]),
        ?PERF_CFG(large, [?OP_SIZE(write, 4), ?OP_BLK_SIZE(write, ?TEST_SIZE_BASE)])
    ]).
multipart_write_test_base(Config) ->
    Helper = new_helper(Config),
    FileId = random_file_id(),
    {ok, Handle} = open(Helper, FileId, write),
    Size = ?config(write_size, Config) * ?MB,
    BlockSize = ?config(write_blk_size, Config) * ?KB,
    multipart(Handle, fun write/3, Size, BlockSize),
    delete_helper(Helper).

truncate_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, 100},
        {parameters, [?OP_NUM(truncate, 1)]},
        {description, "Multiple truncate operations."},
        ?PERF_CFG(small, [?OP_NUM(truncate, 2 * ?TEST_SIZE_BASE)]),
        ?PERF_CFG(medium, [?OP_NUM(truncate, 4 * ?TEST_SIZE_BASE)]),
        ?PERF_CFG(large, [?OP_NUM(truncate, 10 * ?TEST_SIZE_BASE)])
    ]).
truncate_test_base(Config) ->
    Helper = new_helper(Config),
    lists:foreach(fun(_) ->
        truncate(Helper, 0, 0)
    end, lists:seq(1, ?config(truncate_num, Config))),
    delete_helper(Helper).

write_read_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, 100},
        {parameters, [?OP_NUM(1), ?OP_SIZE(1)]},
        {description, "Multiple write followed by read operations."},
        ?PERF_CFG(small, [?OP_NUM(2 * ?TEST_SIZE_BASE), ?OP_SIZE(1)]),
        ?PERF_CFG(medium, [?OP_NUM(4 * ?TEST_SIZE_BASE), ?OP_SIZE(1)]),
        ?PERF_CFG(large, [?OP_NUM(10 * ?TEST_SIZE_BASE), ?OP_SIZE(1)])
    ]).
write_read_test_base(Config) ->
    Helper = new_helper(Config),
    lists:foreach(fun(_) ->
        FileId = random_file_id(),
        {ok, Handle} = open(Helper, FileId, rdwr),
        Content = write(Handle, 0, ?config(op_size, Config) * ?MB),
        ?assertEqual(Content, read(Handle, size(Content)))
    end, lists:seq(1, ?config(op_num, Config))),
    delete_helper(Helper).

multipart_read_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, 100},
        {parameters, [?OP_SIZE(read, 1), ?OP_BLK_SIZE(read, 4)]},
        {description, "Multipart read operation."},
        ?PERF_CFG(small, [?OP_SIZE(read, 1), ?OP_BLK_SIZE(read, ?TEST_SIZE_BASE)]),
        ?PERF_CFG(medium, [?OP_SIZE(read, 2), ?OP_BLK_SIZE(read, ?TEST_SIZE_BASE)]),
        ?PERF_CFG(large, [?OP_SIZE(read, 4), ?OP_BLK_SIZE(read, ?TEST_SIZE_BASE)])
    ]).
multipart_read_test_base(Config) ->
    Helper = new_helper(Config),
    FileId = random_file_id(),
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
        {parameters, [?OP_NUM(1), ?OP_SIZE(1)]},
        {description, "Multiple write followed by unlink operations."},
        ?PERF_CFG(small, [?OP_NUM(2 * ?TEST_SIZE_BASE), ?OP_SIZE(1)]),
        ?PERF_CFG(medium, [?OP_NUM(4 * ?TEST_SIZE_BASE), ?OP_SIZE(1)]),
        ?PERF_CFG(large, [?OP_NUM(10 * ?TEST_SIZE_BASE), ?OP_SIZE(1)])
    ]).
write_unlink_test_base(Config) ->
    Helper = new_helper(Config),
    lists:foreach(fun(_) ->
        FileId = random_file_id(),
        Size = ?config(op_size, Config) * ?MB,
        {ok, Handle} = open(Helper, FileId, write),
        write(Handle, 0, Size),
        unlink(Helper, FileId, Size)
    end, lists:seq(1, ?config(op_num, Config))),
    delete_helper(Helper).

write_read_truncate_unlink_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, ?REPEATS},
        {success_rate, 100},
        {parameters, [?OP_NUM(1), ?OP_SIZE(1)]},
        {description, "Multiple sequences of write, read, truncate and unlink
        operations."},
        ?PERF_CFG(small, [?OP_NUM(2 * ?TEST_SIZE_BASE), ?OP_SIZE(1)]),
        ?PERF_CFG(medium, [?OP_NUM(4 * ?TEST_SIZE_BASE), ?OP_SIZE(1)]),
        ?PERF_CFG(large, [?OP_NUM(10 * ?TEST_SIZE_BASE), ?OP_SIZE(1)])
    ]).
write_read_truncate_unlink_test_base(Config) ->
    Helper = new_helper(Config),
    lists:foreach(fun(_) ->
        FileId = random_file_id(),
        Size = ?config(op_size, Config) * ?MB,
        {ok, Handle} = open(Helper, FileId, rdwr),
        Content = write(Handle, 0, Size),
        ?assertEqual(Content, read(Handle, size(Content))),
        truncate(Helper, FileId, 0, Size),
        unlink(Helper, FileId, 0)
    end, lists:seq(1, ?config(op_num, Config))),
    delete_helper(Helper).

%%%===================================================================
%%% Internal functions
%%%===================================================================

new_helper(Config) ->
    process_flag(trap_exit, true),
    [Node | _] = ?config(op_worker_nodes, Config),
    SwiftConfig = ?config(swift, ?config(swift, ?config(storages, Config))),

    UserCtx = #{
        <<"username">> => atom_to_binary(?config(user_name, SwiftConfig), utf8),
        <<"password">> => atom_to_binary(?config(password, SwiftConfig), utf8)
    },
    {ok, Helper} = helper:new_helper(
        ?SWIFT_HELPER_NAME,
        #{
            <<"authUrl">> => <<"http://", (atom_to_binary(?config(host_name, SwiftConfig), utf8))/binary,
            ":", (integer_to_binary(?config(keystone_port, SwiftConfig)))/binary, "/v2.0/tokens">>,
            <<"containerName">> => ?SWIFT_CONTAINER_NAME,
            <<"tenantName">> => atom_to_binary(?config(tenant_name, SwiftConfig), utf8),
            <<"storagePathType">> => ?FLAT_STORAGE_PATH
        },
        UserCtx
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

random_file_id() ->
    re:replace(http_utils:base64url_encode(crypto:strong_rand_bytes(?FILE_ID_SIZE)),
        "\\W", "", [global, {return, binary}]).

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
