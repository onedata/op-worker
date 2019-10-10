%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests cleanup of memory pools
%%% @end
%%%--------------------------------------------------------------------
-module(memory_pools_cleanup_test_SUITE).
-author("Michal Stanisz").

-include_lib("proto/common/credentials.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2,
    end_per_testcase/2, end_per_suite/1]).

-export([
    connection_closed_after_file_create/1,
    connection_closed_after_file_create_write/1,
    connection_closed_after_file_create_read/1,
    connection_closed_after_file_create_write_read/1,
    connection_closed_after_file_create_release/1,
    connection_closed_after_file_create_write_release/1,
    connection_closed_after_file_create_read_release/1,
    connection_closed_after_file_create_write_read_release/1,
    connection_closed_after_file_create_unsub/1,
    connection_closed_after_file_create_write_unsub/1,
    connection_closed_after_file_create_read_unsub/1,
    connection_closed_after_file_create_write_read_unsub/1,
    connection_closed_after_file_create_release_unsub/1,
    connection_closed_after_file_create_write_release_unsub/1,
    connection_closed_after_file_create_read_release_unsub/1,
    connection_closed_after_file_create_write_read_release_unsub/1,
    connection_not_closed_after_file_create_write_read_release_unsub/1,
    connection_closed_after_dir_create_ls/1,
    connection_closed_after_dir_create_ls_unsub/1
]).

all() ->
    ?ALL([
        connection_closed_after_file_create,
        connection_closed_after_file_create_write,
        connection_closed_after_file_create_read,
        connection_closed_after_file_create_write_read,
        connection_closed_after_file_create_release,
        connection_closed_after_file_create_write_release,
        connection_closed_after_file_create_read_release,
        connection_closed_after_file_create_write_read_release,
        connection_closed_after_file_create_unsub,
        connection_closed_after_file_create_write_unsub,
        connection_closed_after_file_create_read_unsub,
        connection_closed_after_file_create_write_read_unsub,
        connection_closed_after_file_create_release_unsub,
        connection_closed_after_file_create_write_release_unsub,
        connection_closed_after_file_create_read_release_unsub,
        connection_closed_after_file_create_write_read_release_unsub,
        connection_not_closed_after_file_create_write_read_release_unsub,
        connection_closed_after_dir_create_ls,
        connection_closed_after_dir_create_ls_unsub
    ]).

%%%===================================================================
%%% Tests
%%%===================================================================

connection_closed_after_file_create(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, []).

connection_closed_after_file_create_write(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [write]).

connection_closed_after_file_create_read(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [read]).

connection_closed_after_file_create_write_read(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [write, read]).

connection_closed_after_file_create_release(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [release]).

connection_closed_after_file_create_write_release(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [write, release]).

connection_closed_after_file_create_read_release(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [read, release]).

connection_closed_after_file_create_write_read_release(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [write, read, release]).

connection_closed_after_file_create_unsub(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [unsub]).

connection_closed_after_file_create_write_unsub(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [write, unsub]).

connection_closed_after_file_create_read_unsub(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [read, unsub]).

connection_closed_after_file_create_write_read_unsub(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [write, read, unsub]).

connection_closed_after_file_create_release_unsub(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [release, unsub]).

connection_closed_after_file_create_write_release_unsub(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [write, release, unsub]).

connection_closed_after_file_create_read_release_unsub(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [read, release, unsub]).

connection_closed_after_file_create_write_read_release_unsub(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [write, read, release, unsub]).

connection_not_closed_after_file_create_write_read_release_unsub(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [write, read, release, unsub], false).

connection_closed_after_dir_create_ls(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [directory]).

connection_closed_after_dir_create_ls_unsub(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [directory, unsub]).

%%%===================================================================
%%% Test base
%%%===================================================================

memory_pools_cleared_after_disconnection_test_base(Config, Args) ->
    memory_pools_cleared_after_disconnection_test_base(Config, Args, true).

memory_pools_cleared_after_disconnection_test_base(Config, Args, Close) ->
    client_simulation_test_base:verify_streams(Config),

    [Worker1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
    Nonce = ?config({session_nonce, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
    Token = ?config({session_token, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
    Auth = #token_auth{token = Token},

    SpaceGuid = client_simulation_test_base:get_guid(Worker1, SessionId, <<"/space_name1">>),

    {ok, {_, RootHandle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(Worker1, <<"0">>, SpaceGuid,
        generator:gen_name(), 8#755)),
    ?assertEqual(ok, lfm_proxy:close(Worker1, RootHandle)),

    {ok, {Sock, SessionId}} = fuse_test_utils:connect_via_token(Worker1, [{active, true}], Nonce, Auth),

    {Before, _SizesBefore} = pool_utils:get_pools_entries_and_sizes(Worker1, memory),

    client_simulation_test_base:simulate_client(Config, Args, Sock, SpaceGuid, Close),
    timer:sleep(timer:seconds(30)),

    [Worker1 | _] = ?config(op_worker_nodes, Config),
    {After, _SizesAfter} = pool_utils:get_pools_entries_and_sizes(Worker1, memory),
    Res = pool_utils:get_documents_diff(Worker1, After, Before, Close),
    ?assertEqual([], Res),

    client_simulation_test_base:verify_streams(Config, Close).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    client_simulation_test_base:init_per_suite(Config).

init_per_testcase(_Case, Config) ->
    client_simulation_test_base:init_per_testcase(Config).

end_per_testcase(_Case, Config) ->
    client_simulation_test_base:end_per_testcase(Config).

end_per_suite(_Case) ->
    ok.
