%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains test of behaviour in case of db failure.
%%% @end
%%%-------------------------------------------------------------------
-module(tmp_db_error_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([all/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).

-export([
    db_error_test/1
]).

all() -> [
    db_error_test
].

-define(FILE_DATA, <<"1234567890abcd">>).

%%%===================================================================
%%% API
%%%===================================================================

db_error_test(Config) ->
    [Worker] = test_config:get_all_op_worker_nodes(Config),
    [ProviderId] = test_config:get_providers(Config),
    [User1] = test_config:get_provider_users(Config, ProviderId),
    SessId = test_config:get_user_session_id_on_provider(Config, User1, ProviderId),
    [SpaceId | _] = test_config:get_provider_spaces(Config, ProviderId),
    SpaceGuid = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),

    % disable op_worker healthcheck in onepanel, so nodes are not started up automatically
    % TODO - delete when framework handles node stop properly
    onenv_test_utils:disable_panel_healthcheck(Config),

    DirsAndFiles = failure_test_utils:create_files_and_dirs(Worker, SessId, SpaceGuid, 20, 50),
    ct:pal("Test data created"),
    disable_db(Config),

    ct:pal("Stopping application"),
    Master = self(),
    spawn(fun() ->
        try
            Master ! {application_stop, rpc:call(Worker, application, stop, [?APP_NAME])}
        catch
            Error:Reason ->
                Master ! {application_stop, {Error, Reason}}
        end
    end),

    timer:sleep(timer:seconds(11)),
    enable_db(Config),

    StopMsg = receive
        {application_stop, StopAns} -> StopAns
    after
        timer:seconds(30) -> timeout
    end,
    ?assertEqual(ok, StopMsg),
    ct:pal("Application stopped"),

    ok = onenv_test_utils:kill_node(Config, Worker),
    ?assertEqual({badrpc, nodedown}, rpc:call(Worker, oneprovider, get_id, []), 10),
    ct:pal("Node killed"),

    ok = onenv_test_utils:start_node(Config, Worker),
    ?assertMatch({ok, _}, rpc:call(Worker, provider_auth, get_provider_id, []), 60),

    RestartSession = fun() ->
        try
            {ok, provider_onenv_test_utils:setup_sessions(proplists:delete(sess_id, Config))}
        catch
            Error:Reason  ->
                {error, {Error, Reason}}
        end
    end,

    % Error appears if provider dit not manage to connect to zone so a few tries are needed
    {ok, UpdatedConfig} = ?assertMatch({ok, _}, RestartSession(), 30),
    RecreatedSessId = test_config:get_user_session_id_on_provider(UpdatedConfig, User1, ProviderId),
    mock_cberl(UpdatedConfig, true),
    ct:pal("Node restarted"),

    disable_db(Config),
    test_read_operations_on_db_error(Worker, RecreatedSessId, DirsAndFiles),
    test_write_operations_on_db_error(Worker, RecreatedSessId, SpaceGuid),
    enable_db(Config),

    ct:pal("Verifying test data"),
    failure_test_utils:verify_files_and_dirs(Worker, RecreatedSessId, DirsAndFiles, 1),

    ok.

%%%===================================================================
%%% Helper Functions
%%%===================================================================

test_write_operations_on_db_error(Worker, SessId, ParentUuid) ->
    Name = generator:gen_name(),
    ?assertEqual({error, ?EAGAIN}, rpc:call(Worker, lfm, mkdir, [SessId, ParentUuid, Name, 8#755], 5000)),
    ?assertMatch({error, ?EAGAIN}, rpc:call(Worker, lfm, create, [SessId, ParentUuid, Name, 8#755], 5000)).

test_read_operations_on_db_error(Worker, SessId, {Dirs, Files}) ->
    lists:foreach(fun(Dir) ->
        ?assertMatch({error, ?EAGAIN}, rpc:call(Worker, lfm, stat, [SessId, {guid, Dir}], 1000)),
        ?assertMatch({error, ?EAGAIN}, rpc:call(Worker, lfm, get_children_attrs, [SessId, {guid, Dir}, 0, 100], 1000))
    end, Dirs),

    lists:foreach(fun(File) ->
        ?assertMatch({error, ?EAGAIN}, rpc:call(Worker, lfm, stat, [SessId, {guid, File}], 1000)),
        ?assertMatch({error, ?EAGAIN}, rpc:call(Worker, lfm, open, [SessId, {guid, File}, rdwr]))
    end, Files).

disable_db(Config) ->
    [Worker] = test_config:get_all_op_worker_nodes(Config),
    rpc:call(Worker, application, set_env, [?APP_NAME, emulate_db_error, true]).


enable_db(Config) ->
    [Worker] = test_config:get_all_op_worker_nodes(Config),
    rpc:call(Worker, application, set_env, [?APP_NAME, emulate_db_error, false]).

mock_cberl(Config, InitMockManager) ->
    [Worker] = Workers = test_config:get_all_op_worker_nodes(Config),
    test_node_starter:load_modules(Workers, [?MODULE]),

    case InitMockManager of
        true -> {ok, _} = rpc:call(Worker, mock_manager, start, []);
        false -> ok
    end,

    test_utils:mock_new(Workers, cberl, [passthrough]),

    GenericMock = fun(ArgsList) ->
        case application:get_env(?APP_NAME, emulate_db_error, false) of
            true -> {error, etmpfail};
            false -> meck:passthrough(ArgsList)
        end
    end,

    test_utils:mock_expect(Workers, cberl, bulk_get, fun(A1, A2, A3) -> GenericMock([A1, A2, A3]) end),
    test_utils:mock_expect(Workers, cberl, bulk_store, fun(A1, A2, A3) -> GenericMock([A1, A2, A3]) end),
    test_utils:mock_expect(Workers, cberl, bulk_remove, fun(A1, A2, A3) -> GenericMock([A1, A2, A3]) end),
    test_utils:mock_expect(Workers, cberl, arithmetic,
        fun(A1, A2, A3, A4, A5, A6) -> GenericMock([A1, A2, A3, A4, A5, A6]) end),
    test_utils:mock_expect(Workers, cberl, bulk_durability,
        fun(A1, A2, A3, A4) -> GenericMock([A1, A2, A3, A4]) end),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    failure_test_utils:init_per_suite(Config, "1op").

init_per_testcase(_Case, Config) ->
    mock_cberl(Config, false),
    Config.


end_per_testcase(_Case, Config) ->
    Workers = test_config:get_all_op_worker_nodes(Config),
    test_utils:mock_unload(Workers, cberl).

end_per_suite(_Config) ->
    ok.