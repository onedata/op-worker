%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Tests for helpers module.
%%% @end
%%%-------------------------------------------------------------------
-module(helpers_test_SUITE).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("modules/storage_file_manager/helpers/helpers.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

-define(call(N, M, A), ?call(N, helpers, M, A)).
-define(call(N, Mod, M, A), rpc:call(N, Mod, M, A)).

-define(dio_root(Config), ?TEMP_DIR).
-define(path(Config, File), list_to_binary(filename:join(?dio_root(Config), str_utils:to_list(File)))).

-define(CALL_TIMEOUT_MILLIS, timer:minutes(3)).

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).
-export([ctx_server/1, ctx_server/2]).
-export([
    getattr_test/1, access_test/1, mknod_test/1, mkdir_test/1, unlink_test/1, rmdir_test/1, symlink_test/1,
    rename_test/1, chmod_test/1, chown_test/1, truncate_test/1, open_test/1, read_test/1, write_test/1,
    release_test/1, flush_test/1, fsync_test/1
]).

all() ->
    ?ALL([
        getattr_test, access_test, mknod_test, mkdir_test, unlink_test,
        rmdir_test, symlink_test, rename_test, chmod_test, chown_test,
        truncate_test, open_test, read_test, write_test, release_test,
        flush_test, fsync_test
    ]).


%%%===================================================================
%%% Test functions
%%%===================================================================

getattr_test(Config) ->
    File = gen_filename(),

    ok = call(Config, file, make_dir, [?path(Config, File)]),
    ?assertMatch({ok, #statbuf{}}, call(Config, getattr, [File])).

access_test(Config) ->
    File = gen_filename(),

    ok = call(Config, file, make_dir, [?path(Config, File)]),
    ?assertMatch(ok, call(Config, access, [File, 0])).

mknod_test(Config) ->
    lists:foreach(fun({ExpectedType, Type}) ->
        File = gen_filename(),
        ?assertMatch(ok, call(Config, mknod, [File, 8#644, Type])),
        {ok, FileInfo} = ?assertMatch({ok, _}, call(Config, file, read_file_info, [?path(Config, File)])),
        ?assertMatch(ExpectedType, element(3, FileInfo)),
        ?assertMatch(ok, call(Config, file, delete, [?path(Config, File)]))
    end, [{regular, reg}, {device, chr}, {device, blk}, {other, fifo}, {other, sock}]).

mkdir_test(Config) ->
    File = gen_filename(),

    ?assertMatch(ok, call(Config, mkdir, [File, 8#755])),
    ?assertMatch(ok, call(Config, file, del_dir, [?path(Config, File)])).

unlink_test(Config) ->
    File = gen_filename(),

    {ok, _} = call(Config, file, open, [?path(Config, File), write]),
    ?assertMatch(ok, call(Config, unlink, [File])).

rmdir_test(Config) ->
    File = gen_filename(),

    ok = call(Config, file, make_dir, [?path(Config, File)]),
    ?assertMatch(ok, call(Config, rmdir, [File])).

symlink_test(Config) ->
    From = gen_filename(),
    To = gen_filename(),

    ok = call(Config, file, make_dir, [?path(Config, From)]),
    ?assertMatch(ok, call(Config, symlink, [From, To])),
    ?assertMatch(ok, call(Config, file, delete, [?path(Config, To)])),
    ok = call(Config, file, del_dir, [?path(Config, From)]).

rename_test(Config) ->
    From = gen_filename(),
    To = gen_filename(),

    ok = call(Config, file, make_dir, [?path(Config, From)]),
    ?assertMatch(ok, call(Config, rename, [From, To])).

chmod_test(Config) ->
    File = gen_filename(),

    ok = call(Config, file, make_dir, [?path(Config, File)]),
    ?assertMatch(ok, call(Config, chmod, [File, 0])).

chown_test(Config) ->
    File = gen_filename(),

    ok = call(Config, file, make_dir, [?path(Config, File)]),
    ?assertMatch(ok, call(Config, chown, [File, -1, -1])).

truncate_test(Config) ->
    File = gen_filename(),

    {ok, _} = call(Config, file, open, [?path(Config, File), write]),
    ?assertMatch(ok, call(Config, truncate, [File, 10])).

open_test(Config) ->
    File = gen_filename(),

    call(Config, file, open, [?path(Config, File), write]),
    {ok, WriteHandle} = ?assertMatch({ok, _}, call(Config, open, [File, write])),
    ?assertMatch({ok, 4}, call(WriteHandle, write, [0, <<"test">>])),
    {ok, ReadHandle} = ?assertMatch({ok, _}, call(Config, open, [File, read])),
    ?assertMatch({ok, <<"test">>}, call(ReadHandle, read, [0, 4])),
    {ok, RdWrHandle} = ?assertMatch({ok, _}, call(Config, open, [File, rdwr])),
    ?assertMatch({ok, 5}, call(RdWrHandle, write, [0, <<"test2">>])),
    ?assertMatch({ok, <<"test2">>}, call(RdWrHandle, read, [0, 5])).

read_test(Config) ->
    File = gen_filename(),

    {ok, Dev} = call(Config, file, open, [?path(Config, File), write]),
    ok = call(Config, file, write, [Dev, <<"test">>]),

    {ok, Handle} = call(Config, open, [File, read]),

    ?assertMatch({ok, <<"st">>}, call(Handle, read, [2, 10])),
    ?assertMatch({ok, <<"s">>}, call(Handle, read, [2, 1])).

write_test(Config) ->
    File = gen_filename(),

    {ok, _} = call(Config, file, open, [?path(Config, File), write]),

    {ok, Handle} = call(Config, open, [File, write]),

    ?assertMatch({ok, 4}, call(Handle, write, [0, <<"test">>])),
    {ok, Dev1} = call(Config, file, open, [?path(Config, File), [read, binary]]),
    {ok, <<"test">>} = call(Config, file, read, [Dev1, 5]),

    ?assertMatch({ok, 4}, call(Handle, write, [2, <<"test">>])),
    {ok, Dev2} = call(Config, file, open, [?path(Config, File), [read, binary]]),
    {ok, <<"tetest">>} = call(Config, file, read, [Dev2, 6]).

release_test(_Config) ->
    _File = gen_filename(),
    %todo
    ok.

flush_test(Config) ->
    File = gen_filename(),

    {ok, _} = call(Config, file, open, [?path(Config, File), write]),

    {ok, Handle} = call(Config, open, [File, write]),
    ?assertMatch(ok, call(Handle, flush, [])).

fsync_test(Config) ->
    File = gen_filename(),

    {ok, _} = call(Config, file, open, [?path(Config, File), write]),

    {ok, Handle} = call(Config, open, [File, write]),
    ?assertMatch(ok, call(Handle, fsync, [false])).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    ?TEST_STOP(Config).

init_per_testcase(Case, Config) ->
    ?CASE_START(Case),
    [Node | _] = ?config(op_worker_nodes, Config),
    CTXServer = spawn(Node, fun() -> ctx_server(Config) end),
    lists:keystore(ctx_server, 1, Config,
        {ctx_server, CTXServer}).

end_per_testcase(Case, Config) ->
    ?CASE_STOP(Case),
    CTXServer = ?config(ctx_server, Config),
    CTXServer ! exit,

    os:cmd("rm -rf " ++ binary_to_list(?path(Config, <<"helpers_test_*">>))),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

gen_filename() ->
    http_utils:url_encode(<<"helpers_test_", (base64:encode(crypto:rand_bytes(20)))/binary>>).

ctx_server(Config) ->
    CTX = helpers:new_handle(<<"DirectIO">>, #{<<"root_path">> => ?path(Config, "")},
                             #posix_user_ctx{uid = 0, gid = 0}),
    ctx_server(Config, CTX).
ctx_server(Config, CTX) ->
    receive
        {Pid, get} ->
            Pid ! CTX;
        {Pid, {run_helpers, open, Args}} ->
            {ok, FileHandle} = apply(helpers, open, [CTX | Args]),
            HandlePid = spawn_link(fun() -> file_handle_server(FileHandle) end),
            Pid ! {ok, HandlePid};
        {Pid, {run_helpers, Method, Args}} ->
            Pid ! apply(helpers, Method, [CTX | Args]);
        {Pid, {run, Module, Method, Args}} ->
            Pid ! apply(Module, Method, Args);
        exit ->
            exit(normal)
    end,
    ctx_server(Config, CTX).


file_handle_server(FileHandle) ->
    process_flag(trap_exit, true),
    receive
        {'EXIT', _, _} ->
            helpers:release(FileHandle);

        {Pid, {run_helpers, Method, Args}} ->
            Pid ! apply(helpers, Method, [FileHandle | Args]),
            file_handle_server(FileHandle)
    end.


call(Handle, Method, Args) when is_pid(Handle) ->
    Handle ! {self(), {run_helpers, Method, Args}},
    receive
        Resp -> Resp
    after
        ?CALL_TIMEOUT_MILLIS -> {error, timeout}
    end;

call(Config, Method, Args) ->
    CTXServer = ?config(ctx_server, Config),
    CTXServer ! {self(), {run_helpers, Method, Args}},
    receive
        Resp -> Resp
    after
        ?CALL_TIMEOUT_MILLIS -> {error, timeout}
    end.

call(Config, Module, Method, Args) ->
    CTXServer = ?config(ctx_server, Config),
    CTXServer ! {self(), {run, Module, Method, Args}},
    receive
        Resp -> Resp
    after
        ?CALL_TIMEOUT_MILLIS -> {error, timeout}
    end.
