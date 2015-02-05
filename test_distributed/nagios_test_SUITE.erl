%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This test verifies if the nagios endpoint works as expected.
%%% @end
%%%--------------------------------------------------------------------
-module(nagios_test_SUITE).
-author("Lukasz Opiola").

-include("test_utils.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([nagios_test/1]).

%% all() -> [one_node_test, nagios_test].
all() -> [nagios_test].

%%%===================================================================
%%% Test function
%% ====================================================================
nagios_test(Config) ->
    [Ccm] = ?config(op_ccm_nodes, Config),
    [Worker1, Worker2, Worker3] = Workers = ?config(op_worker_nodes, Config),

    ct:print("[DUMP]: ~p~n~n", [{Ccm, Worker1, Worker2, Worker3}]),

    %todo integrate with test_utils
    cluster_state_notifier:cast({subscribe_for_init, self(), length(Workers)}),
    receive
        init_finished -> ok
    after
        50000 -> throw(timeout)
    end,

    ?assertMatch(ccm, gen_server:call({?NODE_MANAGER_NAME, Ccm}, get_node_type)),
    ?assertMatch(worker, gen_server:call({?NODE_MANAGER_NAME, Worker1}, get_node_type)),

    ?assertEqual(pong, rpc:call(Ccm, worker_proxy, call, [http_worker, ping])),
    ?assertEqual(pong, rpc:call(Ccm, worker_proxy, call, [dns_worker, ping])),
    ?assertEqual(pong, rpc:call(Worker1, worker_proxy, call, [http_worker, ping])),
    ?assertEqual(pong, rpc:call(Worker1, worker_proxy, call, [dns_worker, ping])),
    ?assertEqual(pong, rpc:call(Worker2, worker_proxy, call, [http_worker, ping])),
    ?assertEqual(pong, rpc:call(Worker2, worker_proxy, call, [dns_worker, ping])).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================
init_per_testcase(nagios_test, Config) ->
  ?INIT_CODE_PATH(Config),
  test_node_starter:prepare_test_environment(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_testcase(nagios_test, Config) ->
  test_node_starter:clean_environment(Config).
