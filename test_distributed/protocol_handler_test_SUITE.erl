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
-include("proto_internal/oneclient/client_messages.hrl").
-include("proto_internal/oneclient/event_messages.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([cert_connection_test/1, token_connection_test/1, protobuf_msg_test/1]).

all() -> [token_connection_test, cert_connection_test, protobuf_msg_test].

%%%===================================================================
%%% Test function
%% ====================================================================

token_connection_test(Config) ->
    % given
    ssl:start(),
    [Worker1, _] = ?config(op_worker_nodes, Config),

    %then
    {ok, Sock} = connect_via_token(Worker1),
    ok = ssl:close(Sock),
    ?assertMatch({error, _}, ssl:connection_info(Sock)),
    ssl:stop().

cert_connection_test(Config) ->
    % given
    ssl:start(),
    [Worker1, _] = ?config(op_worker_nodes, Config),
    TokenAuthMessage = #'ClientMessage'{client_message =
    {handshake_request, #'HandshakeRequest'{auth_method = #'AuthMethod'{auth_method = {certificate, #'Certificate'{value = <<"VAL">>}}}}}},
    TokenAuthMessageRaw = client_messages:encode_msg(TokenAuthMessage),
    % when
    {ok, Sock} = ssl:connect(?GET_HOST(Worker1), 5555, [binary, {packet, 4}, {active, true}]),
    ok = ssl:send(Sock, TokenAuthMessageRaw),

    % then
    ?assertMatch({ok, _}, ssl:connection_info(Sock)),
    ok = ssl:close(Sock),
    ?assertMatch({error, _}, ssl:connection_info(Sock)),
    ssl:stop().

protobuf_msg_test(Config) ->
    % given
    ssl:start(),
    [Worker1, _] = ?config(op_worker_nodes, Config),
    ok = rpc:call(Worker1, meck, new, [router, [passthrough, non_strict, unstick, no_link]]),
    ok = rpc:call(Worker1, meck, expect, [router, preroute_message,
        fun(
            #client_message{
                credentials = #credentials{},
                client_message = #read_event{}
            }
        ) ->
            ok
        end]),
    Msg = #'ClientMessage'{
        message_id = 0,
        client_message =
        {event, #'Event'{event =
            {read_event, #'ReadEvent'{counter = 1, file_id = <<"id">>, size=1, blocks = []}}}}
    },
    RawMsg = client_messages:encode_msg(Msg),

    % when
    {ok, Sock} = connect_via_token(Worker1),
    ok = ssl:send(Sock, RawMsg),

    %then
    ?assertEqual(true, rpc:call(Worker1, meck, validate, [router])),
    ?assertMatch({ok, _}, ssl:connection_info(Sock)),
    ssl:stop().

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================
init_per_suite(Config) ->
    ?TRY_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

connect_via_token(Node) ->
    {ok, Sock} = ssl:connect(?GET_HOST(Node), 5555, [binary, {packet, 4}, {active, true}]),
    TokenAuthMessage = #'ClientMessage'{client_message =
    {handshake_request, #'HandshakeRequest'{auth_method = #'AuthMethod'{auth_method = {token, #'Token'{value = <<"VAL">>}}}}}},
    TokenAuthMessageRaw = client_messages:encode_msg(TokenAuthMessage),
    ok = ssl:send(Sock, TokenAuthMessageRaw),
    ?assertMatch({ok, _}, ssl:connection_info(Sock)),
    {ok, Sock}.

receive_msg() ->
    receive
        Msg -> Msg
    after
        timer:seconds(5) -> timeout
    end.

