%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This test creates many Erlang virtual machines and uses them
%% to test how ccm manages workers and monitors nodes.
%%% @end
%%%--------------------------------------------------------------------
-module(nodes_management_test_SUITE).
-author("Michal Wrzeszcz").

-include("test_utils.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([ccm_and_worker_test/1]).

all() -> [ccm_and_worker_test].

%%%===================================================================
%%% Test function
%% ====================================================================

ccm_and_worker_test(Config) ->
    [CCM] = ?config(op_ccm_nodes, Config),
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    ?assertMatch(ccm, gen_server:call({?NODE_MANAGER_NAME, CCM}, get_node_type)),
    ?assertMatch(worker, gen_server:call({?NODE_MANAGER_NAME, Worker1}, get_node_type)),

    ?assertEqual(pong, rpc:call(CCM, worker_proxy, call, [http_worker, ping])),
    ?assertEqual(pong, rpc:call(CCM, worker_proxy, call, [dns_worker, ping])),
    ?assertEqual(pong, rpc:call(Worker1, worker_proxy, call, [http_worker, ping])),
    ?assertEqual(pong, rpc:call(Worker1, worker_proxy, call, [dns_worker, ping])),
    ?assertEqual(pong, rpc:call(Worker2, worker_proxy, call, [http_worker, ping])),
    ?assertEqual(pong, rpc:call(Worker2, worker_proxy, call, [dns_worker, ping])).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_testcase(ccm_and_worker_test, Config) ->
    try
        test_node_starter:prepare_test_environment(Config,
            ?TEST_FILE(Config, "env_desc.json"), ?MODULE)
    catch
        A:B -> ct:print("~p:~p~n~p", [A, B, erlang:get_stacktrace()])
    end.

end_per_testcase(ccm_and_worker_test, Config) ->
    test_node_starter:clean_environment(Config).