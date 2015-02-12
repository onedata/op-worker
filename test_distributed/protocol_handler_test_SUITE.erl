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

-include("proto/oneclient/messages.hrl").
-include("proto_internal/oneclient/event_messages.hrl").
-include("proto_internal/oneclient/client_messages.hrl").
-include("proto_internal/oneclient/handshake_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([cert_connection_test/1, token_connection_test/1, protobuf_msg_test/1]).

all() -> [token_connection_test, cert_connection_test, protobuf_msg_test].

-define(CLEANUP, false).

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
    HandshakeReq = #'ClientMessage'{client_message = {handshake_request, #'HandshakeRequest'{}}},
    HandshakeReqRaw = client_messages:encode_msg(HandshakeReq),
    Pid = self(),
    test_utils:mock_new(Worker1, serializator),
    test_utils:mock_expect(Worker1, serializator, deserialize_oneproxy_certificate_info_message,
        fun(Data) ->
            Ans = meck:passthrough([Data]),
            Pid ! {certificate_info_message, Ans},
            Ans
    end),
    Cert = ?TEST_FILE(Config, "peer.pem"),

    % when
    {ok, Sock} = ssl:connect(?GET_HOST(Worker1), 5555, [binary, {packet, 4}, {active, true},
        {certfile, Cert}, {cacertfile, Cert}]),
    {ok, {certificate_info_message, {ok, CertInfo}}} = test_utils:receive_any(timer:seconds(5)),
    ok = ssl:send(Sock, HandshakeReqRaw),
    {ok, {ssl, _, RawHandshakeResponse}} = test_utils:receive_any(timer:seconds(5)),

    % then
    ?assertMatch({ok, _}, ssl:connection_info(Sock)),
    ?assertMatch(#certificate_info{}, CertInfo),
    ?assert(is_binary(CertInfo#certificate_info.client_session_id)),
    ?assert(is_binary(CertInfo#certificate_info.client_subject_dn)),
    HandshakeResponse = server_messages:decode_msg(RawHandshakeResponse, 'ServerMessage'),
    #'ServerMessage'{server_message = {handshake_response, #'HandshakeResponse'{session_id =
        SessionId}}} = HandshakeResponse,
    ?assert(is_binary(SessionId)),
    ok = ssl:close(Sock),
    ?assertMatch({error, _}, ssl:connection_info(Sock)),
    ssl:stop().

protobuf_msg_test(Config) ->
    % given
    ssl:start(),
    [Worker1, _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker1, router),
    test_utils:mock_expect(Worker1, router, preroute_message, fun(
        #client_message{
            client_message = #read_event{}
        }) -> ok
    end),
    Msg = #'ClientMessage'{
        message_id = 0,
        client_message =
        {event, #'Event'{event =
        {read_event, #'ReadEvent'{counter = 1, file_id = <<"id">>, size = 1, blocks = []}}}}
    },
    RawMsg = client_messages:encode_msg(Msg),

    % when
    {ok, Sock} = connect_via_token(Worker1),
    ok = ssl:send(Sock, RawMsg),

    %then
    test_utils:mock_validate(Worker1, router),
    ?assertMatch({ok, _}, ssl:connection_info(Sock)),
    ssl:stop().

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================
init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(_) when ?CLEANUP == false ->
    ok;
end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

connect_via_token(Node) ->
    %given
    TokenAuthMessage = #'ClientMessage'{client_message =
    {handshake_request, #'HandshakeRequest'{token = #'Token'{value = <<"VAL">>}}}},
    TokenAuthMessageRaw = client_messages:encode_msg(TokenAuthMessage),

    %when
    {ok, Sock} = ssl:connect(?GET_HOST(Node), 5555, [binary, {packet, 4}, {active, true}]),
    ok = ssl:send(Sock, TokenAuthMessageRaw),

    %then
    ReceiveAnswer = test_utils:receive_any(timer:seconds(5)),
    ?assertMatch({ok, {ssl, _, _}}, ReceiveAnswer),
    {ok, {ssl, _, Data}} = ReceiveAnswer,
    ?assert(is_binary(Data)),
    ServerMsg = server_messages:decode_msg(Data, 'ServerMessage'),
    ?assertMatch(#'ServerMessage'{server_message = {handshake_response, #'HandshakeResponse'{}}}, ServerMsg),
    ?assertMatch({ok, _}, ssl:connection_info(Sock)),
    {ok, Sock}.
