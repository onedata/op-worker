%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2026: Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Integration tests for the retry mechanism in dir_stats_service_state.
%%% MAX_RETRIES is set to 1 via env override, so the initial traverse plus
%%% one retry is the full lifecycle.
%%% @end
%%%-------------------------------------------------------------------
-module(dir_stats_initialization_retry_test_SUITE).
-behaviour(ct_suite).

-include("onenv_test_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").
-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/logical_file_manager/lfm.hrl").

-export([all/0]).
-export([init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    all_retries_fail_test/1,
    retry_succeeds_test/1,
    reenable_after_retries_exhausted_test/1,
    disable_during_retry_test/1
]).

-define(ATTEMPTS, 60).

%%%===================================================================
%%% Test list
%%%===================================================================

all() -> [
    all_retries_fail_test,
    retry_succeeds_test,
    reenable_after_retries_exhausted_test,
    disable_during_retry_test
].

%%%===================================================================
%%% Tests
%%%===================================================================

all_retries_fail_test(_Config) ->
    SpaceId = oct_background:get_space_id(space1),
    Nodes = oct_background:get_all_providers_nodes(),
    {_Fail1Guid, _Fail2Guid, _NormalDirGuid} = setup_failing_dirs_fixture(SpaceId, Nodes),

    enable_dir_stats(SpaceId),

    ?assertMatch(enabled,
        opw_test_rpc:call(krakow, dir_stats_service_state, get_extended_status, [SpaceId]),
        ?ATTEMPTS),

    ?assertMatch({ok, #dir_stats_service_state{
        initialization_retry_count = 1,
        incarnation = 2
    }}, opw_test_rpc:call(krakow, dir_stats_service_state, get, [SpaceId])),

    % 2 dirs failed despite retries - the stats are partially counted and the errors are stored;
    % 3 reg files are counted, because only listing during traverse failed, dir init succeeded so its content is counted; 
    % (dir stats init lists dir content twice - once in traverse to list directories to init and second one during this 
    % init to count the number of direct children).
    ?assertMatch({ok, #{?REG_FILE_AND_LINK_COUNT := 3, ?DIR_COUNT := 3, ?DIR_ERROR_COUNT := 2, ?FILE_ERROR_COUNT := 0}},
        opw_test_rpc:call(krakow, dir_size_stats, get_stats, [space_dir:guid(SpaceId)]),
        ?ATTEMPTS).


retry_succeeds_test(_Config) ->
    SpaceId = oct_background:get_space_id(space1),
    Nodes = oct_background:get_all_providers_nodes(),
    {Fail1Guid, Fail2Guid, NormalDirGuid} = setup_failing_dirs_fixture(SpaceId, Nodes),

    Master = self(),
    test_utils:mock_new(Nodes, dir_stats_collections_initialization_traverse, [passthrough]),
    test_utils:mock_expect(Nodes, dir_stats_collections_initialization_traverse, run, fun
        (SId, Inc) when SId =:= SpaceId andalso Inc =:= 2 ->
            Master ! {retry_starting, self()},
            receive proceed -> meck:passthrough([SId, Inc]) end;
        (SId, Inc) ->
            meck:passthrough([SId, Inc])
    end),

    enable_dir_stats(SpaceId),

    WorkerPid = receive {retry_starting, Pid} -> Pid end,

    test_utils:mock_unload(Nodes, file_tree),

    WorkerPid ! proceed,

    ?assertMatch(enabled,
        opw_test_rpc:call(krakow, dir_stats_service_state, get_extended_status, [SpaceId]),
        ?ATTEMPTS),

    ?assertMatch({ok, #{?DIR_ERROR_COUNT := 0}},
        opw_test_rpc:call(krakow, dir_size_stats, get_stats, [Fail1Guid]), ?ATTEMPTS),
    ?assertMatch({ok, #{?DIR_ERROR_COUNT := 0}},
        opw_test_rpc:call(krakow, dir_size_stats, get_stats, [Fail2Guid]), ?ATTEMPTS),

    ?assertMatch({ok, #{?REG_FILE_AND_LINK_COUNT := 1}},
        opw_test_rpc:call(krakow, dir_size_stats, get_stats, [Fail1Guid]), ?ATTEMPTS),
    ?assertMatch({ok, #{?REG_FILE_AND_LINK_COUNT := 1}},
        opw_test_rpc:call(krakow, dir_size_stats, get_stats, [Fail2Guid]), ?ATTEMPTS),
    ?assertMatch({ok, #{?REG_FILE_AND_LINK_COUNT := 1}},
        opw_test_rpc:call(krakow, dir_size_stats, get_stats, [NormalDirGuid]), ?ATTEMPTS),
    
    ?assertMatch({ok, #{?REG_FILE_AND_LINK_COUNT := 3, ?DIR_COUNT := 3, ?DIR_ERROR_COUNT := 0, ?FILE_ERROR_COUNT := 0}},
        opw_test_rpc:call(krakow, dir_size_stats, get_stats, [space_dir:guid(SpaceId)]),
        ?ATTEMPTS).


reenable_after_retries_exhausted_test(_Config) ->
    SpaceId = oct_background:get_space_id(space1),
    Nodes = oct_background:get_all_providers_nodes(),
    {Fail1Guid, Fail2Guid, NormalDirGuid} = setup_failing_dirs_fixture(SpaceId, Nodes),

    enable_dir_stats(SpaceId),

    ?assertMatch(enabled,
        opw_test_rpc:call(krakow, dir_stats_service_state, get_extended_status, [SpaceId]),
        ?ATTEMPTS),

    ?assertMatch({ok, #dir_stats_service_state{initialization_retry_count = 1}},
        opw_test_rpc:call(krakow, dir_stats_service_state, get, [SpaceId])),

    test_utils:mock_unload(Nodes, file_tree),

    disable_dir_stats(SpaceId),

    enable_dir_stats(SpaceId),

    ?assertMatch(enabled,
        opw_test_rpc:call(krakow, dir_stats_service_state, get_extended_status, [SpaceId]),
        ?ATTEMPTS),

    ?assertMatch({ok, #dir_stats_service_state{initialization_retry_count = 0}},
        opw_test_rpc:call(krakow, dir_stats_service_state, get, [SpaceId])),

    ?assertMatch({ok, #{?DIR_ERROR_COUNT := 0, ?REG_FILE_AND_LINK_COUNT := 1}},
        opw_test_rpc:call(krakow, dir_size_stats, get_stats, [Fail1Guid]), ?ATTEMPTS),
    ?assertMatch({ok, #{?DIR_ERROR_COUNT := 0, ?REG_FILE_AND_LINK_COUNT := 1}},
        opw_test_rpc:call(krakow, dir_size_stats, get_stats, [Fail2Guid]), ?ATTEMPTS),
    ?assertMatch({ok, #{?REG_FILE_AND_LINK_COUNT := 1}},
        opw_test_rpc:call(krakow, dir_size_stats, get_stats, [NormalDirGuid]), ?ATTEMPTS),

    ?assertMatch({ok, #{?REG_FILE_AND_LINK_COUNT := 3, ?DIR_COUNT := 3, ?DIR_ERROR_COUNT := 0, ?FILE_ERROR_COUNT := 0}},
        opw_test_rpc:call(krakow, dir_size_stats, get_stats, [space_dir:guid(SpaceId)]),
        ?ATTEMPTS).


disable_during_retry_test(_Config) ->
    SpaceId = oct_background:get_space_id(space1),
    Nodes = oct_background:get_all_providers_nodes(),
    {_Fail1Guid, _Fail2Guid, _NormalDirGuid} = setup_failing_dirs_fixture(SpaceId, Nodes),

    Master = self(),
    test_utils:mock_new(Nodes, dir_stats_collections_initialization_traverse, [passthrough]),
    test_utils:mock_expect(Nodes, dir_stats_collections_initialization_traverse, run, fun
        (SId, Inc) when SId =:= SpaceId andalso Inc =:= 2 ->
            Master ! {retry_starting, self()},
            receive proceed -> meck:passthrough([SId, Inc]) end;
        (SId, Inc) ->
            meck:passthrough([SId, Inc])
    end),

    enable_dir_stats(SpaceId),

    ?assertNotMatch(disabled,
        opw_test_rpc:call(krakow, dir_stats_service_state, get_extended_status, [SpaceId]),
        ?ATTEMPTS),

    WorkerPid = receive {retry_starting, Pid} -> Pid end,

    ?assertNotMatch(disabled,
        opw_test_rpc:call(krakow, dir_stats_service_state, get_extended_status, [SpaceId]),
        ?ATTEMPTS),

    set_dir_stats_status(SpaceId, disabled),

    WorkerPid ! proceed,

    ?assertMatch(disabled,
        opw_test_rpc:call(krakow, dir_stats_service_state, get_extended_status, [SpaceId]),
        ?ATTEMPTS).

%%%===================================================================
%%% SetUp and TearDown
%%%===================================================================

init_per_suite(Config) ->
    opt:init_per_suite([{?LOAD_MODULES, [?MODULE]} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {provider_token_ttl_sec, 24 * 60 * 60},
            {dir_stats_initialization_max_retries, 1}
        ]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    lfm_proxy:init(Config, false).


end_per_testcase(_Case, Config) ->
    Nodes = oct_background:get_all_providers_nodes(),
    SpaceId = oct_background:get_space_id(space1),
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),

    test_utils:mock_unload(Nodes),
    reset_dir_stats_and_files(SpaceId, Node, SessId),
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Helper functions
%%%===================================================================

%% @private
enable_dir_stats(SpaceId) ->
    set_dir_stats_status(SpaceId, enabled),
    % the space first transitions to `initializing` (is_active = true) and only later to
    % `enabled`; wait only for active so this does not deadlock tests that pause the
    % initialization traverse before it can reach `enabled`
    ?assertEqual(true,
        opw_test_rpc:call(krakow, dir_stats_service_state, is_active, [SpaceId]),
        ?ATTEMPTS).


%% @private
disable_dir_stats(SpaceId) ->
    set_dir_stats_status(SpaceId, disabled),
    ?assertMatch(disabled,
        opw_test_rpc:call(krakow, dir_stats_service_state, get_extended_status, [SpaceId]),
        ?ATTEMPTS).


%% @private
-spec set_dir_stats_status(od_space:id(), enabled | disabled) -> ok.
set_dir_stats_status(SpaceId, Status) ->
    ?assertEqual(ok, opw_test_rpc:call(krakow, space_logic, update_support_parameters, [
        SpaceId, #support_parameters{dir_stats_service_enabled = Status =:= enabled}
    ])).


%% @private
%% Creates the standard fixture:
%% two dirs whose listing is mocked to fail (fail1, fail2) and one normal dir, each holding one
%% regular file. Installs the file_tree mock that fails listing for the two fail dirs.
setup_failing_dirs_fixture(SpaceId, Nodes) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),
    reset_dir_stats_and_files(SpaceId, Node, SessId),

    #object{guid = Fail1Guid} = onenv_file_test_utils:create_and_sync_file_tree(
        user1, space1, #dir_spec{children = [#file_spec{}]}, krakow),
    #object{guid = Fail2Guid} = onenv_file_test_utils:create_and_sync_file_tree(
        user1, space1, #dir_spec{children = [#file_spec{}]}, krakow),
    #object{guid = NormalDirGuid} = onenv_file_test_utils:create_and_sync_file_tree(
        user1, space1, #dir_spec{children = [#file_spec{}]}, krakow),

    test_utils:mock_new(Nodes, file_tree, [passthrough]),
    test_utils:mock_expect(Nodes, file_tree, list_children, fun(FileCtx, UserCtx, Opts) ->
        Guid = file_ctx:get_logical_guid_const(FileCtx),
        case Guid =:= Fail1Guid orelse Guid =:= Fail2Guid of
            true -> error(listing_error);
            false -> meck:passthrough([FileCtx, UserCtx, Opts])
        end
    end),
    {Fail1Guid, Fail2Guid, NormalDirGuid}.


%% @private
%% Brings the space to a known-clean baseline: dir stats disabled + state record deleted +
%% space-dir stats deleted + ended initialization-traverse tasks deleted + all user files in the
%% space removed. Safe to run regardless of prior test state.
reset_dir_stats_and_files(SpaceId, Node, SessId) ->
    SpaceDirGuid = opw_test_rpc:call(krakow, fun() -> space_dir:guid(SpaceId) end),

    disable_dir_stats(SpaceId),
    ?assertEqual(ok, opw_test_rpc:call(krakow, dir_stats_service_state, clean, [SpaceId])),
    ?assertEqual(ok, opw_test_rpc:call(krakow, dir_size_stats, delete_stats, [SpaceDirGuid])),
    lists:foreach(fun(Incarnation) ->
        ?assertEqual(ok, opw_test_rpc:call(krakow, traverse_task, delete_ended, [
            <<"dir_stats_collections_initialization_traverse">>,
            dir_stats_collections_initialization_traverse:gen_task_id(SpaceId, Incarnation)
        ]))
    end, lists:seq(1, 10)),

    {ok, Children} = ?assertMatch({ok, _},
        lfm_proxy:get_children(Node, SessId, ?FILE_REF(SpaceDirGuid), 0, 10000)),
    lists:foreach(fun({ChildGuid, _Name}) ->
        ?assertEqual(ok, lfm_proxy:rm_recursive(Node, SessId, ?FILE_REF(ChildGuid)))
    end, Children).
