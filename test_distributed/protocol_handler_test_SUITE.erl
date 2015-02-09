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
-include("proto/oneclient/messages.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([cert_connection_test/1, token_connection_test/1, protobuf_msg_test/1]).

all() -> [cert_connection_test, token_connection_test, protobuf_msg_test].

%%%===================================================================
%%% Test function
%% ====================================================================

cert_connection_test(Config) ->
    ssl:start(),
    [Worker1, _] = ?config(op_worker_nodes, Config),
    {ok, Sock} = connect_via_token(Worker1),
    ok = ssl:close(Sock),
    ?assertMatch({error, _}, ssl:connection_info(Sock)),
    ssl:stop().

token_connection_test(Config) ->
    ssl:start(),
    [Worker1, _] = ?config(op_worker_nodes, Config),
    {ok, Sock} = ssl:connect(?GET_HOST(Worker1), 5555, [binary, {packet, 4}, {active, true}]),

    TokenAuthMessage = <<"{\"cert\":\"id\"}">>,
    ok = ssl:send(Sock, TokenAuthMessage),
    ?assertMatch({ok, _}, ssl:connection_info(Sock)),
    ok = ssl:close(Sock),
    ?assertMatch({error, _}, ssl:connection_info(Sock)),
    ssl:stop().

protobuf_msg_test(Config) ->
    ssl:start(),
    [Worker1, _] = ?config(op_worker_nodes, Config),
    {ok, Sock} = connect_via_token(Worker1),

    Msg = #'ClientMessage'{response_id = 0, client_message = {handshake_request, #'HandshakeRequest'{}}},
    RawMsg = client_messages:encode_msg(Msg),
    ok = ssl:send(Sock, RawMsg),
    ?assertMatch({ok, _}, ssl:connection_info(Sock)),
    ok = ssl:send(Sock, <<"non_protobuff">>),
%%     ?assertMatch({error, _}, ssl:connection_info(Sock)),

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

connect_via_token(Node) ->
    {ok, Sock} = ssl:connect(?GET_HOST(Node), 5555, [binary, {packet, 4}, {active, true}]),
    TokenAuthMessage = <<"{\"token\":\"val\"}">>,
    ok = ssl:send(Sock, TokenAuthMessage),
    ?assertMatch({ok, _}, ssl:connection_info(Sock)),
    {ok, Sock}.

receive_msg() ->
    receive
        Msg -> Msg
    after
        timer:seconds(5) -> timeout
    end.

