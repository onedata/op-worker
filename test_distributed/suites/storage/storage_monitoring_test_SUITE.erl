%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of storage monitoring functionality.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_monitoring_test_SUITE).
-author("Michal Stanisz").

-include("onenv_test_utils.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("space_setup_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% API
-export([all/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).

-export([
    storage_monitoring_test/1
]).

all() -> [
    storage_monitoring_test
].

-define(ATTEMPTS, 5).

%%%===================================================================
%%% API
%%%===================================================================

storage_monitoring_test(_Config) ->
    #object{guid = Guid} = onenv_file_test_utils:create_and_sync_file_tree(user1, space1, #file_spec{}),
    KrakowNode = oct_background:get_random_provider_node(krakow),
    User1KrakowSessId = oct_background:get_user_session_id(user1, krakow),
    ?assertMatch({ok, _}, lfm_proxy:stat(KrakowNode, User1KrakowSessId, #file_ref{guid = Guid})),
    mock_storage_detector_error(KrakowNode),
    ?assertMatch({error, ?EAGAIN}, lfm_proxy:stat(KrakowNode, User1KrakowSessId, #file_ref{guid = Guid}), ?ATTEMPTS),
    unmock_storage_detector(KrakowNode),
    ?assertMatch({ok, _}, lfm_proxy:stat(KrakowNode, User1KrakowSessId, #file_ref{guid = Guid}), ?ATTEMPTS).


%%%===================================================================
%%% Helper functions
%%%===================================================================

mock_storage_detector_error(Node) ->
    test_utils:mock_new(Node, storage_detector),
    test_utils:mock_expect(Node, storage_detector, run_diagnostics,
        fun(_, _, _, _) -> {{error, mocked}, mocked} end
    ).
    

unmock_storage_detector(Node) ->
    test_utils:mock_unload(Node, storage_detector).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [
            {storages_check_interval_sec, 1}
        ]}]
    }).

init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config, false).


end_per_testcase(_Case, Config) ->
    test_utils:mock_unload(oct_background:get_all_providers_nodes()),
    lfm_proxy:teardown(Config).

end_per_suite(_Config) ->
    ok.
