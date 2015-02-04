%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests protocol handler
%%% @end
%%%--------------------------------------------------------------------
-module(protocol_handler_test_SUITE).
-author("Tomasz Lichon").

-include("test_utils.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([tcp_connection_test/1]).

all() -> [tcp_connection_test].

%%%===================================================================
%%% Test function
%% ====================================================================

tcp_connection_test(_) ->
    {ok, Sock} = gen_tcp:connect("localhost", 5678, [binary, {packet, 0}]),
    ok = gen_tcp:send(Sock, <<"Some Data">>),
    ?assertEqual(<<"Some Data">>, gen_tcp:recv(Sock, 0)),
    ok = gen_tcp:close(Sock).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?INIT_CODE_PATH,
    test_node_starter:prepare_test_environment(Config, ?TEST_FILE("env_desc.json")),
    Workers = ?config(op_worker_nodes, Config),
    %todo integrate with test_utils
    cluster_state_notifier:cast({subscribe_for_init, self(), length(Workers)}),
    receive
        init_finished -> ok
    after
        timer:minutes(1) -> throw(timeout)
    end.

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).
