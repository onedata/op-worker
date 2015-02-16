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

-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto_internal/oneclient/event_messages.hrl").
-include("proto_internal/oneclient/client_messages.hrl").
-include("proto_internal/oneclient/handshake_messages.hrl").
-include("proto_internal/oneproxy/oneproxy_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).
-export([cert_connection_test/1, token_connection_test/1, protobuf_msg_test/1,
    multi_message_test/1]).

all() -> [token_connection_test, cert_connection_test, protobuf_msg_test,
    multi_message_test].

-define(CLEANUP, true).

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
    [Worker1, _] = Workers = ?config(op_worker_nodes, Config),
    HandshakeReq = #'ClientMessage'{message_body = {handshake_request, #'HandshakeRequest'{}}},
    HandshakeReqRaw = client_messages:encode_msg(HandshakeReq),
    Pid = self(),
    test_utils:mock_expect(Workers, serializator, deserialize_oneproxy_certificate_info_message,
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
    #'ServerMessage'{message_body = {handshake_response, #'HandshakeResponse'{session_id =
    SessionId}}} = HandshakeResponse,
    ?assert(is_binary(SessionId)),
    ok = ssl:close(Sock),
    ?assertMatch({error, _}, ssl:connection_info(Sock)),
    ssl:stop().

protobuf_msg_test(Config) ->
    % given
    ssl:start(),
    [Worker1, _] = Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_expect(Workers, router, preroute_message,
        fun(#client_message{message_body = #read_event{}}) ->
            ok
        end),
    Msg = #'ClientMessage'{
        message_id = <<"0">>,
        message_body = {event, #'Event'{event =
        {read_event, #'ReadEvent'{counter = 1, file_id = <<"id">>, size = 1, blocks = []}}
        }}
    },
    RawMsg = client_messages:encode_msg(Msg),

    % when
    {ok, Sock} = connect_via_token(Worker1),
    ok = ssl:send(Sock, RawMsg),

    %then
    ?assertMatch({ok, _}, ssl:connection_info(Sock)),
    ssl:stop().

multi_message_test(Config) ->
    % given
    ssl:start(),
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    Self = self(),
    test_utils:mock_expect(Workers, router, route_message,
        fun(#client_message{message_body = #read_event{counter = Counter}}) ->
            Self ! Counter,
            ok
        end),
    MsgNumbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    Events = lists:map(
        fun(N) ->
            #'ClientMessage'{message_body = {event, #'Event'{event =
            {read_event, #'ReadEvent'{counter = N, file_id = <<"id">>, size = 1, blocks = []}}}}}
        end, MsgNumbers),
    RawEvents = lists:map(fun(E) -> client_messages:encode_msg(E) end, Events),

    % when
    {ok, Sock} = connect_via_token(Worker1),
    lists:foreach(fun(E) -> ok = ssl:send(Sock, E) end, RawEvents),

    %then
    ?assertMatch({ok, _}, ssl:connection_info(Sock)),
    lists:foreach(
        fun(N) ->
            ?assertMatch({ok, N}, test_utils:receive_msg(N, timer:seconds(5)))
        end, MsgNumbers),
    ssl:stop().

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================
init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    case ?CLEANUP of
        true -> test_node_starter:clean_environment(Config);
        false -> ok
    end.

init_per_testcase(cert_connection_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, serializator),
    Config;

init_per_testcase(protobuf_msg_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, router),
    Config;

init_per_testcase(multi_message_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, router),
    Config;

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(cert_connection_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate(Workers, serializator),
    test_utils:mock_unload(Workers, serializator),
    Config;

end_per_testcase(protobuf_msg_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate(Workers, router),
    test_utils:mock_unload(Workers, router),
    Config;

end_per_testcase(multi_message_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate(Workers, router),
    test_utils:mock_unload(Workers, router),
    Config;

end_per_testcase(_, Config) ->
    Config.

%%%===================================================================
%%% Internal functions
%%%===================================================================

connect_via_token(Node) ->
    %given
    TokenAuthMessage = #'ClientMessage'{message_body =
    {handshake_request, #'HandshakeRequest'{token = #'Token'{value = <<"VAL">>}}}},
    TokenAuthMessageRaw = client_messages:encode_msg(TokenAuthMessage),

    %when
    {ok, Sock} = ssl:connect(?GET_HOST(Node), 5555, [binary, {packet, 4}, {active, true}]),
    ok = ssl:send(Sock, TokenAuthMessageRaw),

    %then
    ReceiveAnswer = receive
                        {ssl, _, _} = Ans -> Ans
                    after timer:seconds(5) ->
                        {error, timeout}
                    end,
    {ssl, _, Data} = ReceiveAnswer,
    ?assert(is_binary(Data)),
    ServerMsg = server_messages:decode_msg(Data, 'ServerMessage'),
    ?assertMatch(#'ServerMessage'{message_body = {handshake_response, #'HandshakeResponse'{}}}, ServerMsg),
    ?assertMatch({ok, _}, ssl:connection_info(Sock)),
    {ok, Sock}.