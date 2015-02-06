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

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([ssl_connection_test/1]).

all() -> [tcp_connection_test].

%%%===================================================================
%%% Test function
%% ====================================================================

ssl_connection_test(Config) ->
    ssl:start(),
    [Worker1, _] = ?config(op_worker_nodes, Config),
    {ok, Sock} = ssl:connect(?GET_HOST(Worker1), 5555, [binary, {packet, 4}, {active, true}]),
    ok = ssl:send(Sock, <<"1">>),
    ?assertMatch({ssl, _, <<"1">>}, receive_msg()),
    ok = ssl:send(Sock, <<"2">>),
    ?assertMatch({ssl, _, <<"2">>}, receive_msg()),
    ok = ssl:close(Sock),
    ssl:stop().

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    try
        test_node_starter:prepare_test_environment(Config, ?TEST_FILE(Config, "env_desc.json"))
    catch A:B -> ct:print("~p:~p~n~p", [A, B, erlang:get_stacktrace()]) end.

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

receive_msg() ->
    receive
        Msg -> Msg
    after
        timer:seconds(5) -> timeout
    end.

