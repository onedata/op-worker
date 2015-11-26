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
-include("modules/fslogic/helpers.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("annotations/include/annotations.hrl").

-define(call(N, M, A), ?call(N, helpers, M, A)).
-define(call(N, Mod, M, A), rpc:call(N, Mod, M, A)).

-define(dio_root(Config), ?TEMP_DIR).
-define(path(Config, File), list_to_binary(filename:join(?dio_root(Config), str_utils:to_list(File)))).

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
    [
        getattr_test, access_test, mknod_test, mkdir_test, unlink_test, rmdir_test, symlink_test, rename_test,
        chmod_test, chown_test, truncate_test, open_test, read_test, write_test, release_test, flush_test, fsync_test
    ].


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
    File = gen_filename(),

    ?assertMatch(ok, call(Config, mknod, [File, 8#644, reg])),
    ?assertMatch(ok, call(Config, file, delete, [?path(Config, File)])).

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

    {ok, _} = call(Config, file, open, [?path(Config, File), write]),
    ?assertMatch({ok, _}, call(Config, open, [File, read])).

read_test(Config) ->
    File = gen_filename(),

    {ok, Dev} = call(Config, file, open, [?path(Config, File), write]),
    ok = call(Config, file, write, [Dev, <<"test">>]),
    ?assertMatch({ok, <<"st">>}, call(Config, read, [File, 2, 10])),
    ?assertMatch({ok, <<"s">>}, call(Config, read, [File, 2, 1])).

write_test(Config) ->
    File = gen_filename(),

    {ok, _} = call(Config, file, open, [?path(Config, File), write]),

    ?assertMatch({ok, 4}, call(Config, write, [File, 0, <<"test">>])),
    {ok, Dev1} = call(Config, file, open, [?path(Config, File), [read, binary]]),
    {ok, <<"test">>} = call(Config, file, read, [Dev1, 5]),

    ?assertMatch({ok, 4}, call(Config, write, [File, 2, <<"test">>])),
    {ok, Dev2} = call(Config, file, open, [?path(Config, File), [read, binary]]),
    {ok, <<"tetest">>} = call(Config, file, read, [Dev2, 6]).

release_test(_Config) ->
    _File = gen_filename(),
    %todo
    ok.

flush_test(Config) ->
    File = gen_filename(),

    {ok, _} = call(Config, file, open, [?path(Config, File), write]),
    ?assertMatch(ok, call(Config, flush, [File])).

fsync_test(Config) ->
    File = gen_filename(),

    {ok, _} = call(Config, file, open, [?path(Config, File), write]),
    ?assertMatch(ok, call(Config, fsync, [File, false])).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    CTXServer = spawn(Node, fun() -> ctx_server(Config) end),
    lists:keystore(ctx_server, 1, Config,
        {ctx_server, CTXServer}).

end_per_testcase(_, Config) ->
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
    CTX = helpers:new_handle(<<"DirectIO">>, [?path(Config, "")]),
    ctx_server(Config, CTX).
ctx_server(Config, CTX) ->
    receive
        {Pid, get} ->
            Pid ! CTX;
        {Pid, {run_helpers, Method, Args}} ->
            Pid ! apply(helpers, Method, [CTX | Args]);
        {Pid, {run, Module, Method, Args}} ->
            Pid ! apply(Module, Method, Args);
        exit ->
            exit(normal)
    end,
    ctx_server(Config, CTX).

call(Config, Method, Args) ->
    CTXServer = ?config(ctx_server, Config),
    CTXServer ! {self(), {run_helpers, Method, Args}},
    receive
        Resp -> Resp
    end.
call(Config, Module, Method, Args) ->
    CTXServer = ?config(ctx_server, Config),
    CTXServer ! {self(), {run, Module, Method, Args}},
    receive
        Resp -> Resp
    end.