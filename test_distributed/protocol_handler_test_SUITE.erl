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
-include("proto_internal/oneclient/common_messages.hrl").
-include("proto_internal/oneclient/event_messages.hrl").
-include("proto_internal/oneclient/server_messages.hrl").
-include("proto_internal/oneclient/client_messages.hrl").
-include("proto_internal/oneclient/handshake_messages.hrl").
-include("proto_internal/oneclient/message_id.hrl").
-include("proto_internal/oneproxy/oneproxy_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("annotations/include/annotations.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).
-export([cert_connection_test/1, token_connection_test/1, protobuf_msg_test/1,
    multi_message_test/1, client_send_test/1, client_communicate_test/1,
    client_communiate_async_test/1, multi_ping_pong_test/1,
    sequential_ping_pong_test/1]).

-perf_test({perf_cases, [multi_message_test, multi_ping_pong_test, sequential_ping_pong_test]}).
all() ->
    [token_connection_test, cert_connection_test, protobuf_msg_test,
        multi_message_test, client_send_test, client_communicate_test,
        client_communiate_async_test, multi_ping_pong_test,
        sequential_ping_pong_test].

-define(CLEANUP, true).

%%%===================================================================
%%% Test function
%% ====================================================================

token_connection_test(Config) ->
    % given
    [Worker1, _] = ?config(op_worker_nodes, Config),

    % then
    {ok, {Sock, _}} = connect_via_token(Worker1),
    ok = ssl:close(Sock),
    ?assertMatch({error, _}, ssl:connection_info(Sock)).

cert_connection_test(Config) ->
    % given
    [Worker1, _] = Workers = ?config(op_worker_nodes, Config),
    HandshakeReq =
        #'ClientMessage'{message_body = {handshake_request, #'HandshakeRequest'{}}},
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
    {ok, Sock} = ssl:connect(?GET_HOST(Worker1), 5555, [binary, {packet, 4},
        {active, true}, {certfile, Cert}, {cacertfile, Cert}]),
    {ok, {certificate_info_message, {ok, CertInfo}}} =
        test_utils:receive_any(timer:seconds(5)),
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
    ?assertMatch({error, _}, ssl:connection_info(Sock)).

protobuf_msg_test(Config) ->
    % given
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
    {ok, {Sock, _}} = connect_via_token(Worker1),
    ok = ssl:send(Sock, RawMsg),

    % then
    ?assertMatch({ok, _}, ssl:connection_info(Sock)),
    ok = ssl:close(Sock).

-perf_test([
    {repeats, 3},
    {perf_configs, [
        [{msg_num, 1000000}]
    ]},
    {ct_config, [{msg_num, 1000}]}
]).
multi_message_test(Config) ->
    % given
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    MsgNum = ?config(msg_num, Config),
    Self = self(),
    MsgNumbers = lists:seq(1, MsgNum),
    Events = lists:map(
        fun(N) ->
            #'ClientMessage'{message_body = {event, #'Event'{event =
            {read_event, #'ReadEvent'{
                counter = N,
                file_id = <<"id">>,
                size = 1,
                blocks = []
            }}}}}
        end, MsgNumbers),
    RawEvents = lists:map(fun(E) -> client_messages:encode_msg(E) end, Events),
    test_utils:mock_expect(Workers, router, route_message,
        fun(#client_message{message_body = #read_event{counter = Counter}}) ->
            Self ! Counter,
            ok
        end),

    % when
    {ok, {Sock, _}} = connect_via_token(Worker1),
    T1 = os:timestamp(),
    lists:foreach(fun(E) -> ok = ssl:send(Sock, E) end, RawEvents),
    T2 = os:timestamp(),

    % then
    ?assertMatch({ok, _}, ssl:connection_info(Sock)),
    lists:foreach(
        fun(N) ->
            ?assertMatch({ok, N}, test_utils:receive_msg(N, timer:seconds(5)))
        end, MsgNumbers),
    T3 = os:timestamp(),
    _SendingTime = {sending_time, timer:now_diff(T2, T1)},
    _ReceivingTime = {receiving_time, timer:now_diff(T3, T2)},
    _FullTime = {full_time, timer:now_diff(T3, T1)},
%%     ct:print("~p ~p ~p", [_SendingTime, _ReceivingTime, _FullTime]),
    ok = ssl:close(Sock).

client_send_test(Config) ->
    % given
    [Worker1, _] = ?config(op_worker_nodes, Config),
    {ok, {Sock, SessionId}} = connect_via_token(Worker1),
    Code = 'VOK',
    Description = <<"desc">>,
    ServerMsgInternal = #server_message{
        message_body = #status{
            code = Code,
            description = Description
        }
    },
    ServerMessageProtobuf = #'ServerMessage'{
        message_id = undefined,
        message_body = {status, #'Status'{
            code = Code,
            description = Description}
        }
    },

    % when
    rpc:call(Worker1, client_communicator, send, [ServerMsgInternal, SessionId]),

    % then
    ReceivedMessage =
        receive
            {ssl, _, Data} ->
                server_messages:decode_msg(Data, 'ServerMessage')
        after timer:seconds(5) ->
            {error, timeout}
        end,
    ?assertEqual(ServerMessageProtobuf, ReceivedMessage),
    ok = ssl:close(Sock).

client_communicate_test(Config) ->
    % given
    [Worker1, _] = ?config(op_worker_nodes, Config),
    Status = #status{
        code = 'VOK',
        description = <<"desc">>
    },
    ServerMsgInternal = #server_message{message_body = Status},

    % when
    {ok, {Sock, SessionId}} = spawn_ssl_echo_client(Worker1),
    CommunicateResult = rpc:call(Worker1, client_communicator, communicate,
        [ServerMsgInternal, SessionId]),

    % then
    ?assertMatch({ok, #client_message{message_body = Status}}, CommunicateResult),
    ok = ssl:close(Sock).

client_communiate_async_test(Config) ->
    % given
    [Worker1, _] = Workers = ?config(op_worker_nodes, Config),
    Status = #status{
        code = 'VOK',
        description = <<"desc">>
    },
    ServerMsgInternal = #server_message{message_body = Status},
    Self = self(),
    {ok, {Sock, SessionId}} = spawn_ssl_echo_client(Worker1),

    % when
    {ok, MsgId} = rpc:call(Worker1, client_communicator, communicate_async,
        [ServerMsgInternal, SessionId, Self]),
    ReceivedMessage =
        receive
            #client_message{} = Msg -> Msg
        after timer:seconds(5) ->
            {error, timeout}
        end,

    % then
    ?assertMatch(#client_message{message_id = MsgId, message_body = Status},
        ReceivedMessage),

    % given
    test_utils:mock_expect(Workers, router, route_message,
        fun(#client_message{message_id = Id = #message_id{issuer = server,
            recipient = undefined}}) ->
            Self ! {router_message_called, Id},
            ok
        end),

    % when
    {ok, MsgId2} = rpc:call(Worker1, client_communicator, communicate_async,
        [ServerMsgInternal, SessionId]),
    RouterNotification = test_utils:receive_any(timer:seconds(5)),

    % then
    ?assertEqual({ok, {router_message_called, MsgId2}}, RouterNotification),
    ok = ssl:close(Sock).

-perf_test([
    {repeats, 3},
    {perf_configs, [
        [{msg_num, 1000000}]
    ]},
    {ct_config, [{msg_num, 1000}]}
]).
multi_ping_pong_test(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    MsgNum = ?config(msg_num, Config),
    MsgNumbers = lists:seq(1, MsgNum),
    MsgNumbersBin = lists:map(fun(N) -> integer_to_binary(N) end, MsgNumbers),
    Pings = lists:map(
        fun(N) ->
            #'ClientMessage'{message_id = N, message_body = {ping, #'Ping'{}}}
        end, MsgNumbersBin),
    RawPings = lists:map(fun(E) -> client_messages:encode_msg(E) end, Pings),

    % when
    {ok, {Sock, _}} = connect_via_token(Worker1),
    T1 = os:timestamp(),
    lists:foreach(fun(E) -> ok = ssl:send(Sock, E) end, RawPings),
    T2 = os:timestamp(),

    % then
    ?assertMatch({ok, _}, ssl:connection_info(Sock)),
    UnknownMessages = lists:foldl(
        fun(N, Queued) ->
            case lists:filter(
                fun(#'ServerMessage'{message_id = Id}) -> Id =:= N end,
                Queued)
            of
                [] ->
                    Data =
                        receive
                            {ssl, _, Binary} -> Binary
                        after
                            timer:seconds(5) -> {error, timeout}
                        end,
                    ?assertNotEqual({error, timeout}, Data),
                    Msg = server_messages:decode_msg(Data, 'ServerMessage'),
                    case N =:= Msg#'ServerMessage'.message_id of
                        true ->
                            ?assertMatch(
                                #'ServerMessage'{message_body = {pong, #'Pong'{}}},
                                Msg
                            ),
                            Queued;
                        false ->
                            [Msg | Queued]
                    end;
                [ExistingMsg] ->
                    ?assertMatch(
                        #'ServerMessage'{message_body = {pong, #'Pong'{}}},
                        ExistingMsg
                    ),
                    Queued -- [ExistingMsg]
            end
        end, [], MsgNumbersBin),
    ?assertEqual([], UnknownMessages),
    T3 = os:timestamp(),
    _SendingTime = {sending_time, timer:now_diff(T2, T1)},
    _ReceivingTime = {receiving_time, timer:now_diff(T3, T2)},
    _FullTime = {full_time, timer:now_diff(T3, T1)},
%%     ct:print("~p ~p ~p", [_SendingTime, _ReceivingTime, _FullTime]),
    ok = ssl:close(Sock).

-perf_test([
    {repeats, 3},
    {perf_configs, [
        [{msg_num, 100000}]
    ]},
    {ct_config, [{msg_num, 1000}]}
]).
sequential_ping_pong_test(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    MsgNum = ?config(msg_num, Config),
    MsgNumbers = lists:seq(1, MsgNum),
    MsgNumbersBin = lists:map(fun(N) -> integer_to_binary(N) end, MsgNumbers),
    Pings = lists:map(
        fun(N) ->
            #'ClientMessage'{message_id = N, message_body = {ping, #'Ping'{}}}
        end, MsgNumbersBin),
    RawPings = lists:map(fun(E) -> client_messages:encode_msg(E) end, Pings),

    % when
    {ok, {Sock, _}} = connect_via_token(Worker1, [{active, true}]),
    T1 = os:timestamp(),
    lists:foldl(fun(E, N) ->
        % send ping & receive pong
        ok = ssl:send(Sock, E),
        Pong =
            receive
                {ssl, _, Data} -> server_messages:decode_msg(Data, 'ServerMessage')
            after
                timer:seconds(5) -> {error, timeout}
            end,

        % validate pong
        BinaryN = integer_to_binary(N),
        ?assertMatch(#'ServerMessage'{message_body = {pong, #'Pong'{}},
            message_id = BinaryN},
            Pong),
        N+1
    end, 1, RawPings),
    T2 = os:timestamp(),

    % then
    ?assertMatch({ok, _}, ssl:connection_info(Sock)),
    _FullTime = {full_time, timer:now_diff(T2, T1)},
%%     ct:print("~p", [_FullTime]),
    ok = ssl:close(Sock).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================
init_per_suite(Config) ->
    test_node_starter:prepare_test_environment(Config,
        ?TEST_FILE(Config, "env_desc.json"), ?MODULE).

end_per_suite(Config) ->
    case ?CLEANUP of
        true -> test_node_starter:clean_environment(Config);
        false -> ok
    end.

init_per_testcase(cert_connection_test, Config) ->
    ssl:start(),
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, serializator),
    Config;

init_per_testcase(Case, Config) when Case =:= protobuf_msg_test
    orelse Case =:= multi_message_test
    orelse Case =:= client_communiate_async_test ->
    ssl:start(),
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, router),
    Config;

init_per_testcase(_, Config) ->
    ssl:start(),
    Config.

end_per_testcase(cert_connection_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate(Workers, serializator),
    test_utils:mock_unload(Workers, serializator),
    ssl:stop();

end_per_testcase(Case, Config) when Case =:= protobuf_msg_test
    orelse Case =:= multi_message_test
    orelse Case =:= client_communiate_async_test ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate(Workers, router),
    test_utils:mock_unload(Workers, router),
    ssl:stop();

end_per_testcase(_, _) ->
    ssl:stop().


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node using token, with default socket_opts
%% @equiv connect_via_token(Node, [{active, true}]).
%% @end
%%--------------------------------------------------------------------
-spec connect_via_token(Node :: node()) ->
    {ok, {Sock :: ssl:sslsocket(), SessId :: session:id()}}.
connect_via_token(Node) ->
    connect_via_token(Node, [{active, true}]).

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node using token, with custom socket opts
%% @end
%%--------------------------------------------------------------------
-spec connect_via_token(Node :: node(), SocketOpts :: list()) ->
    {ok, {Sock :: ssl:sslsocket(), SessId :: session:id()}}.
connect_via_token(Node, SocketOpts) ->
    % given
    TokenAuthMessage = #'ClientMessage'{message_body =
    {handshake_request, #'HandshakeRequest'{token = #'Token'{value = <<"VAL">>}}}},
    TokenAuthMessageRaw = client_messages:encode_msg(TokenAuthMessage),
    ActiveOpt =
        case proplists:get_value(active, SocketOpts) of
            undefined -> [];
            Other -> [{active, Other}]
        end,
    OtherOpts = proplists:delete(active, SocketOpts),

    % when
    {ok, Sock} = ssl:connect(?GET_HOST(Node), 5555, [binary, {packet, 4}, {active, once}] ++ OtherOpts),
    ok = ssl:send(Sock, TokenAuthMessageRaw),

    % then
    Data =
        receive
            {ssl, _, Ans} -> Ans
        after timer:seconds(5) ->
            {error, timeout}
        end,
    ?assert(is_binary(Data)),
    ServerMsg = server_messages:decode_msg(Data, 'ServerMessage'),
    ?assertMatch(#'ServerMessage'{message_body = {handshake_response, #'HandshakeResponse'{}}}, ServerMsg),
    ?assertMatch({ok, _}, ssl:connection_info(Sock)),
    #'ServerMessage'{message_body = {handshake_response,
        #'HandshakeResponse'{session_id = SessionId}}} = ServerMsg,
    ssl:setopts(Sock, ActiveOpt),
    {ok, {Sock, SessionId}}.

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node, and send back each message received from the server
%% @end
%%--------------------------------------------------------------------
-spec spawn_ssl_echo_client(NodeToConnect :: node()) ->
    {ok, {Sock :: ssl:sslsocket(), SessId :: session:id()}}.
spawn_ssl_echo_client(NodeToConnect) ->
    {ok, {Sock, SessionId}} = connect_via_token(NodeToConnect, []),
    SslEchoClient =
        fun Loop() ->
            % receive data from server
            case ssl:recv(Sock, 0) of
                {ok, Data} ->
                    % decode
                    #'ServerMessage'{message_id = Id, message_body = Body} =
                        server_messages:decode_msg(Data, 'ServerMessage'),

                    % respond with the same data to the server
                    ClientAnsProtobuf = #'ClientMessage'{message_id = Id, message_body = Body},
                    ClientAnsRaw = client_messages:encode_msg(ClientAnsProtobuf),
                    ?assertEqual(ok, ssl:send(Sock, ClientAnsRaw)),

                    %loop back
                    Loop();
                {error, closed} -> ok;
                Error -> ?error("ssl_echo_client error: ~p", [Error])
            end
        end,
    spawn_link(SslEchoClient),
    {ok, {Sock, SessionId}}.