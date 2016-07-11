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

-include("modules/fslogic/helpers.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).

%% tests
-export([set_user_ctx_test/1, write_test/1, multipart_write_test/1,
    truncate_test/1, write_read_test/1, multipart_read_test/1,
    write_unlink_test/1, write_read_truncate_unlink_test/1]).

%% test_bases
-export([set_user_ctx_test_base/1, write_test_base/1, multipart_write_test_base/1,
    truncate_test_base/1, write_read_test_base/1, multipart_read_test_base/1,
    write_unlink_test_base/1, write_read_truncate_unlink_test_base/1]).

-define(TEST_CASES, [
    set_user_ctx_test, write_test, multipart_write_test, truncate_test,
    write_read_test, multipart_read_test, write_unlink_test,
    write_read_truncate_unlink_test
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

%%%===================================================================
%%% Test functions
%%%===================================================================

set_user_ctx_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 10},
        {success_rate, 100},
        {parameters, [?OP_NUM(set_user_ctx, 1)]},
        {description, "Multiple user context changes."},
        ?PERF_CFG(small, [?OP_NUM(set_user_ctx, 10)]),
        ?PERF_CFG(medium, [?OP_NUM(set_user_ctx, 20)]),
        ?PERF_CFG(large, [?OP_NUM(set_user_ctx, 50)])
    ]).
set_user_ctx_test_base(Config) ->
    Helper = new_helper(Config),
    lists:foreach(fun(_) ->
        ?assertEqual(ok, set_user_ctx(Helper, Config))
    end, lists:seq(1, ?config(set_user_ctx_num, Config))),
    delete_helper(Helper).

write_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 10},
        {success_rate, 100},
        {parameters, [?OP_NUM(write, 1), ?OP_SIZE(write, 1)]},
        {description, "Multiple write operations."},
        ?PERF_CFG(small, [?OP_NUM(write, 10), ?OP_SIZE(write, 1)]),
        ?PERF_CFG(medium, [?OP_NUM(write, 20), ?OP_SIZE(write, 1)]),
        ?PERF_CFG(large, [?OP_NUM(write, 50), ?OP_SIZE(write, 1)])
    ]).
write_test_base(Config) ->
    Helper = new_helper(Config),
    lists:foreach(fun(_) ->
        write(Helper, ?config(write_size, Config) * ?MB)
    end, lists:seq(1, ?config(write_num, Config))),
    delete_helper(Helper).

multipart_write_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 10},
        {success_rate, 100},
        {parameters, [?OP_SIZE(write, 1), ?OP_BLK_SIZE(write, 4)]},
        {description, "Multipart write operation."},
        ?PERF_CFG(small, [?OP_SIZE(write, 1), ?OP_BLK_SIZE(write, 4)]),
        ?PERF_CFG(medium, [?OP_SIZE(write, 2), ?OP_BLK_SIZE(write, 4)]),
        ?PERF_CFG(large, [?OP_SIZE(write, 4), ?OP_BLK_SIZE(write, 4)])
    ]).
multipart_write_test_base(Config) ->
    Helper = new_helper(Config),
    Size = ?config(write_size, Config) * ?MB,
    BlockSize = ?config(write_blk_size, Config) * ?KB,
    multipart(Helper, fun write/4, Size, BlockSize),
    delete_helper(Helper).

truncate_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 10},
        {success_rate, 100},
        {parameters, [?OP_NUM(truncate, 1)]},
        {description, "Multiple truncate operations."},
        ?PERF_CFG(small, [?OP_NUM(truncate, 10)]),
        ?PERF_CFG(medium, [?OP_NUM(truncate, 20)]),
        ?PERF_CFG(large, [?OP_NUM(truncate, 50)])
    ]).
truncate_test_base(Config) ->
    Helper = new_helper(Config),
    lists:foreach(fun(_) ->
        truncate(Helper, 0)
    end, lists:seq(1, ?config(truncate_num, Config))),
    delete_helper(Helper).

write_read_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 10},
        {success_rate, 100},
        {parameters, [?OP_NUM(1), ?OP_SIZE(1)]},
        {description, "Multiple write followed by read operations."},
        ?PERF_CFG(small, [?OP_NUM(10), ?OP_SIZE(1)]),
        ?PERF_CFG(medium, [?OP_NUM(20), ?OP_SIZE(1)]),
        ?PERF_CFG(large, [?OP_NUM(50), ?OP_SIZE(1)])
    ]).
write_read_test_base(Config) ->
    Helper = new_helper(Config),
    FileId = random_file_id(),
    lists:foreach(fun(_) ->
        Content = write(Helper, FileId, 0, ?config(op_size, Config) * ?MB),
        ?assertEqual(Content, read(Helper, FileId, size(Content)))
    end, lists:seq(1, ?config(op_num, Config))),
    delete_helper(Helper).

multipart_read_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 10},
        {success_rate, 100},
        {parameters, [?OP_SIZE(read, 1), ?OP_BLK_SIZE(read, 4)]},
        {description, "Multipart read operation."},
        ?PERF_CFG(small, [?OP_SIZE(read, 1), ?OP_BLK_SIZE(read, 4)]),
        ?PERF_CFG(medium, [?OP_SIZE(read, 2), ?OP_BLK_SIZE(read, 4)]),
        ?PERF_CFG(large, [?OP_SIZE(read, 4), ?OP_BLK_SIZE(read, 4)])
    ]).
multipart_read_test_base(Config) ->
    Helper = new_helper(Config),
    FileId = random_file_id(),
    Size = ?config(read_size, Config) * ?MB,
    BlockSize = ?config(read_blk_size, Config) * ?KB,
    write(Helper, FileId, 0, 0),
    truncate(Helper, FileId, Size),
    multipart(Helper, fun read/4, FileId, 0, Size, BlockSize),
    delete_helper(Helper).

write_unlink_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 10},
        {success_rate, 100},
        {parameters, [?OP_NUM(1), ?OP_SIZE(1)]},
        {description, "Multiple write followed by unlink operations."},
        ?PERF_CFG(small, [?OP_NUM(10), ?OP_SIZE(1)]),
        ?PERF_CFG(medium, [?OP_NUM(20), ?OP_SIZE(1)]),
        ?PERF_CFG(large, [?OP_NUM(50), ?OP_SIZE(1)])
    ]).
write_unlink_test_base(Config) ->
    Helper = new_helper(Config),
    FileId = random_file_id(),
    lists:foreach(fun(_) ->
        write(Helper, FileId, 0, ?config(op_size, Config) * ?MB),
        unlink(Helper, FileId)
    end, lists:seq(1, ?config(op_num, Config))),
    delete_helper(Helper).

write_read_truncate_unlink_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 10},
        {success_rate, 100},
        {parameters, [?OP_NUM(1), ?OP_SIZE(1)]},
        {description, "Multiple sequences of write, read, truncate and unlink
        operations."},
        ?PERF_CFG(small, [?OP_NUM(10), ?OP_SIZE(1)]),
        ?PERF_CFG(medium, [?OP_NUM(20), ?OP_SIZE(1)]),
        ?PERF_CFG(large, [?OP_NUM(50), ?OP_SIZE(1)])
    ]).
write_read_truncate_unlink_test_base(Config) ->
    Helper = new_helper(Config),
    FileId = random_file_id(),
    lists:foreach(fun(_) ->
        Content = write(Helper, FileId, 0, ?config(op_size, Config) * ?MB),
        ?assertEqual(Content, read(Helper, FileId, size(Content))),
        truncate(Helper, FileId, 0),
        unlink(Helper, FileId)
    end, lists:seq(1, ?config(op_num, Config))),
    delete_helper(Helper).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

new_helper(Config) ->
    process_flag(trap_exit, true),
    [Node | _] = ?config(op_worker_nodes, Config),
    SwiftConfig = ?config(swift, ?config(swift, ?config(storages, Config))),
    HelperArgs = #{
        <<"tenant_name">> => atom_to_binary(?config(tenant_name, SwiftConfig), utf8),
        <<"auth_url">> => <<"http://", (atom_to_binary(?config(host_name, SwiftConfig), utf8))/binary,
            ":", (integer_to_binary(?config(keystone_port, SwiftConfig)))/binary, "/v2.0/tokens">>,
        <<"container_name">> => ?SWIFT_CONTAINER_NAME
    },
    UserCtx = #swift_user_ctx{
        user_name = atom_to_binary(?config(user_name, SwiftConfig), utf8),
        password = atom_to_binary(?config(password, SwiftConfig), utf8)
    },

    spawn_link(Node, fun() ->
        helper_loop(?SWIFT_HELPER_NAME, HelperArgs, UserCtx)
    end).

delete_helper(Helper) ->
    Helper ! exit.

helper_loop(HelperName, HelperArgs, UserCtx) ->
    Ctx = helpers:new_handle(HelperName, HelperArgs),
    helpers:set_user_ctx(Ctx, UserCtx),
    helper_loop(Ctx).

helper_loop(Ctx) ->
    receive
        exit ->
            exit(normal);
        {Pid, {run_helpers, Method, Args}} ->
            Pid ! {self(), apply(helpers, Method, [Ctx | Args])}
    end,
    helper_loop(Ctx).

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
    re:replace(http_utils:base64url_encode(crypto:rand_bytes(?FILE_ID_SIZE)),
        "\\W", "", [global, {return, binary}]).

set_user_ctx(Helper, Config) ->
    SwiftConfig = ?config(swift, ?config(swift, ?config(storages, Config))),
    SwiftUserCtx = #swift_user_ctx{
        user_name = atom_to_binary(?config(user_name, SwiftConfig), utf8),
        password = atom_to_binary(?config(password, SwiftConfig), utf8)
    },
    call(Helper, set_user_ctx, [SwiftUserCtx]).

read(Helper, FileId, Size) ->
    read(Helper, FileId, 0, Size).

read(Helper, FileId, Offset, Size) ->
    {ok, Content} =
        ?assertMatch({ok, _}, call(Helper, read, [FileId, Offset, Size])),
    Content.

write(Helper, Size) ->
    write(Helper, 0, Size).

write(Helper, Offset, Size) ->
    FileId = random_file_id(),
    write(Helper, FileId, Offset, Size).

write(Helper, FileId, Offset, Size) ->
    Content = crypto:rand_bytes(Size),
    ActualSize = size(Content),
    ?assertEqual({ok, ActualSize},
        call(Helper, write, [FileId, Offset, Content])),
    Content.

truncate(Helper, Size) ->
    FileId = random_file_id(),
    truncate(Helper, FileId, Size).

truncate(Helper, FileId, Size) ->
    ?assertEqual(ok, call(Helper, truncate, [FileId, Size])).

unlink(Helper, FileId) ->
    ?assertEqual(ok, call(Helper, unlink, [FileId])).

multipart(Helper, Method, Size, BlockSize) ->
    multipart(Helper, Method, 0, Size, BlockSize).

multipart(Helper, Method, Offset, Size, BlockSize) ->
    FileId = random_file_id(),
    multipart(Helper, Method, FileId, Offset, Size, BlockSize),
    FileId.

multipart(_Helper, _Method, _FileId, _Offset, 0, _BlockSize) ->
    ok;
multipart(Helper, Method, FileId, Offset, Size, BlockSize)
    when Size >= BlockSize ->
    Method(Helper, FileId, Offset, BlockSize),
    multipart(Helper, Method, FileId, Offset + BlockSize, Size - BlockSize,
        BlockSize);
multipart(Helper, Method, FileId, Offset, Size, BlockSize) ->
    Method(Helper, FileId, Offset, Size),
    multipart(Helper, Method, FileId, Offset + Size, 0, BlockSize).