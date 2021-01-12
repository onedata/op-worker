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
-include_lib("onenv_ct/include/oct_background.hrl").

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
    [Worker] = oct_background:get_provider_nodes(krakow),
    ProviderId = oct_background:get_provider_id(krakow),
    [User1] = oct_background:get_provider_eff_users(krakow),
    SessId = oct_background:get_user_session_id(joe, krakow),
    [SpaceId | _] = oct_background:get_provider_supported_spaces(krakow),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    % disable op_worker healthcheck in onepanel, so nodes are not started up automatically
    oct_environment:disable_panel_healthcheck(Config),

    DirsAndFiles = file_ops_test_utils:create_files_and_dirs(Worker, SessId, SpaceGuid, 20, 50),
    enable_db_error_emulation(),
    ct:pal("Test data created"),

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

    timer:sleep(timer:seconds(15)),
    disable_db_error_emulation(),

    StopMsg = receive
        {application_stop, StopAns} -> StopAns
    after
        timer:minutes(2) -> timeout
    end,
    ?assertEqual(ok, StopMsg),
    ct:pal("Application stopped"),

    % Kill node to be sure that nothing is in memory and all documents will be read from db
    failure_test_utils:kill_nodes(Config, Worker),
    ct:pal("Node killed"),

    UpdatedConfig = failure_test_utils:restart_nodes(Config, Worker),
    RecreatedSessId = test_config:get_user_session_id_on_provider(UpdatedConfig, User1, ProviderId),
    mock_cberl(UpdatedConfig),
    ct:pal("Node restarted"),

    enable_db_error_emulation(),
    test_read_operations_on_db_error(Worker, RecreatedSessId, DirsAndFiles),
    TestedName1 = test_write_operations_on_db_error(Worker, RecreatedSessId, SpaceGuid),
    TestedName2 = test_write_operations_on_db_error(Worker, RecreatedSessId, SpaceGuid),
    disable_db_error_emulation(),
    test_write_operations_after_db_error(Worker, RecreatedSessId, SpaceGuid, TestedName1, TestedName2),

    ct:pal("Verifying test data"),
    file_ops_test_utils:verify_files_and_dirs(Worker, RecreatedSessId, DirsAndFiles, 1),

    ok.

%%%===================================================================
%%% Helper Functions
%%%===================================================================

test_write_operations_on_db_error(Worker, SessId, ParentUuid) ->
    Name = generator:gen_name(),
    ?assertEqual({error, ?EAGAIN}, lfm_proxy:mkdir(Worker, SessId, ParentUuid, Name, 8#755)),
    ?assertEqual({error, ?EAGAIN}, lfm_proxy:create(Worker, SessId, ParentUuid, Name, 8#755)),
    Name.

test_write_operations_after_db_error(Worker, SessId, ParentUuid, PreviouslyTestedName1, PreviouslyTestedName2) ->
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId, ParentUuid, PreviouslyTestedName1, 8#755)),
    ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId, ParentUuid, PreviouslyTestedName2, 8#755)).

test_read_operations_on_db_error(Worker, SessId, DirsAndFiles) ->
    file_ops_test_utils:test_read_operations_on_error(Worker, SessId, DirsAndFiles, ?EAGAIN).

enable_db_error_emulation() ->
    application:set_env(?APP_NAME, emulate_db_error, true).


disable_db_error_emulation() ->
    application:set_env(?APP_NAME, emulate_db_error, false).

mock_cberl(Config) ->
    Workers = test_config:get_all_op_worker_nodes(Config),
    test_node_starter:load_modules(Workers, [?MODULE]),
    test_utils:mock_new(Workers, cberl, [passthrough]),

    Node = node(),
    GenericMock = fun(ArgsList) ->
        case rpc:call(Node, application, get_env, [?APP_NAME, emulate_db_error, false]) of
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
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op",
        posthook = fun provider_onenv_test_utils:setup_sessions/1
    }).


init_per_testcase(_Case, Config) ->
    mock_cberl(Config),
    lfm_proxy:init(Config, false).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
    Workers = test_config:get_all_op_worker_nodes(Config),
    test_utils:mock_unload(Workers, cberl).

end_per_suite(_Config) ->
    ok.