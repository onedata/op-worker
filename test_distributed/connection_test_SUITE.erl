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
-module(connection_test_SUITE).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("proto/oneclient/message_id.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/handshake_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%%tests
-export([
    cert_connection_test/1,
    token_connection_test/1,
    protobuf_msg_test/1,
    multi_message_test/1,
    client_send_test/1,
    client_communicate_test/1,
    client_communicate_async_test/1,
    multi_ping_pong_test/1,
    sequential_ping_pong_test/1,
    multi_connection_test/1,
    bandwidth_test/1,
    python_client_test/1,
    proto_version_test/1
]).

%%test_bases
-export([
    multi_message_test_base/1,
    multi_ping_pong_test_base/1,
    sequential_ping_pong_test_base/1,
    multi_connection_test_base/1,
    bandwidth_test_base/1,
    python_client_test_base/1
]).

-define(NORMAL_CASES_NAMES, [
    token_connection_test,
    cert_connection_test,
    protobuf_msg_test,
    multi_message_test,
    client_send_test,
    client_communicate_test,
    client_communicate_async_test,
    multi_ping_pong_test,
    sequential_ping_pong_test,
    multi_connection_test,
    bandwidth_test,
    python_client_test,
    proto_version_test
]).

-define(PERFORMANCE_CASES_NAMES, [
    multi_message_test,
    multi_ping_pong_test,
    sequential_ping_pong_test,
    multi_connection_test,
    bandwidth_test,
    python_client_test
]).

all() -> ?ALL(?NORMAL_CASES_NAMES, ?PERFORMANCE_CASES_NAMES).

-define(MACAROON, <<"DUMMY-MACAROON">>).
-define(TIMEOUT, timer:seconds(5)).

%%%===================================================================
%%% Test functions
%%%===================================================================

token_connection_test(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    % then
    {ok, {Sock, _}} = connect_via_token(Worker1),
    ok = ssl:close(Sock).

% todo VFS-1158 Modify & enable the test after veryfing client certificate.
cert_connection_test(_Config) ->
%%     % given
%%     remove_pending_messages(),
%%     [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
%%     HandshakeReq = #'ClientMessage'{message_body = {handshake_request, #'HandshakeRequest'{
%%         session_id = <<"session_id">>
%%     }}},
%%     HandshakeReqRaw = messages:encode_msg(HandshakeReq),
%%     Cert = ?TEST_FILE(Config, "peer.pem"),
%%
%%     {ok, CertPem} = file:read_file(Cert),
%%     PemEntries = public_key:pem_decode(CertPem),
%%     CertDer = lists:keyfind('Certificate', 1, PemEntries),
%%
%%     Self = self(),
%%     test_utils:mock_expect(Workers, connection, init,
%%         fun(Ref, Socket, Transport, Opts) ->
%%             Self ! self(),
%%             meck:passthrough([Ref, Socket, Transport, Opts])
%%         end),
%%
%%     % when
%%     {ok, Sock} = ssl:connect(utils:get_host_as_atom(Worker1), 5555, [binary, {packet, 4},
%%         {active, true}, {certfile, Cert}, {cacertfile, Cert}]),
%%
%%     {ok, Pid} = ?assertReceivedMatch(_, ?TIMEOUT),
%%     State = sys:get_state(Pid),
%%     #'OTPCertificate'{} = Cert = erlang:element(2, State),
%%
%%     ok = ssl:send(Sock, HandshakeReqRaw),
%%     ?assertReceivedMatch({ssl, _, RawHandshakeResponse}, ?TIMEOUT),
%%
%%     % then
%%     ?assertEqual(CertDer, ssl:peercert(Sock)),
%%     HandshakeResponse = messages:decode_msg(RawHandshakeResponse, 'ServerMessage'),
%%     #'ServerMessage'{message_body = {handshake_response, #'HandshakeResponse'{session_id =
%%     SessionId}}} = HandshakeResponse,
%%     ?assert(is_binary(SessionId)),
%%     ok = ssl:close(Sock),
    ok.

protobuf_msg_test(Config) ->
    % given
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_expect(Workers, router, preroute_message, fun
        (#client_message{message_body = #events{events = [#event{
            type = #file_read_event{}
        }]}}, _) -> ok
    end),
    Msg = #'ClientMessage'{
        message_id = <<"0">>,
        message_body = {events, #'Events'{events = [#'Event'{
            type = {file_read, #'FileReadEvent'{
                counter = 1, file_uuid = <<"id">>, size = 1, blocks = []
            }}
        }]}}
    },
    RawMsg = messages:encode_msg(Msg),

    % when
    {ok, {Sock, _}} = connect_via_token(Worker1),
    ok = ssl:send(Sock, RawMsg),

% then
    ok = ssl:close(Sock).

multi_message_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 5},
        {success_rate, 90},
        {parameters, [
            [{name, msg_num}, {value, 1000}, {description, "Number of messages sent and received."}],
            [{name, transport}, {value, ssl}, {description, "Connection transport type."}]
        ]},
        {config, [{name, ssl},
            {parameters, [
                [{name, msg_num}, {value, 100000}]
            ]}
        ]}
    ]).
multi_message_test_base(Config) ->
    % given
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    MsgNum = ?config(msg_num, Config),
    Self = self(),
    MsgNumbers = lists:seq(1, MsgNum),
    Events = lists:map(fun(N) ->
        #'ClientMessage'{message_body = {events, #'Events'{events = [#'Event'{
            type = {file_read, #'FileReadEvent'{
                counter = N,
                file_uuid = <<"id">>,
                size = 1,
                blocks = []
            }}
        }]}}}
    end, MsgNumbers),
    RawEvents = lists:map(fun(E) -> messages:encode_msg(E) end, Events),
    initializer:remove_pending_messages(),
    test_utils:mock_expect(Workers, router, route_message, fun
        (#client_message{message_body = #events{events = [#event{
            type = #file_read_event{counter = Counter}
        }]}}) ->
            Self ! Counter,
            ok
    end),

    % when
    {ok, {Sock, _}} = connect_via_token(Worker1, [{active, true}]),
    T1 = erlang:monotonic_time(milli_seconds),
    lists:foreach(fun(E) -> ok = ssl:send(Sock, E) end, RawEvents),
    T2 = erlang:monotonic_time(milli_seconds),

    % then
    lists:foreach(fun(N) ->
        ?assertReceivedMatch(N, ?TIMEOUT)
    end, MsgNumbers),
    T3 = erlang:monotonic_time(milli_seconds),
    ok = ssl:close(Sock),
    [
        #parameter{name = sending_time, value = T2 - T1, unit = "ms"},
        #parameter{name = receiving_time, value = T3 - T2, unit = "ms"},
        #parameter{name = full_time, value = T3 - T1, unit = "ms"}
    ].

client_send_test(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    {ok, {Sock, SessionId}} = connect_via_token(Worker1),
    Code = ?OK,
    Description = <<"desc">>,
    ServerMsgInternal = #server_message{message_body = #status{
        code = Code,
        description = Description
    }},
    ServerMessageProtobuf = #'ServerMessage'{
        message_id = undefined,
        message_body = {status, #'Status'{
            code = Code,
            description = Description}
        }
    },

    % when
    ?assertEqual(ok, rpc:call(Worker1, communicator, send, [ServerMsgInternal, SessionId])),

    % then
    ?assertEqual(ServerMessageProtobuf, receive_server_message()),
    ok = ssl:close(Sock).

client_communicate_test(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    Status = #status{code = ?OK, description = <<"desc">>},
    ServerMsgInternal = #server_message{message_body = Status},

    % when
    {ok, {Sock, SessionId}} = spawn_ssl_echo_client(Worker1),
    CommunicateResult = rpc:call(Worker1, communicator, communicate,
        [ServerMsgInternal, SessionId]),

    % then
    ?assertMatch({ok, #client_message{message_body = Status}}, CommunicateResult),
    ok = ssl:close(Sock).

client_communicate_async_test(Config) ->
    % given
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    Status = #status{
        code = ?OK,
        description = <<"desc">>
    },
    ServerMsgInternal = #server_message{message_body = Status},
    Self = self(),
    {ok, {Sock, SessionId}} = spawn_ssl_echo_client(Worker1),

    % when
    {ok, MsgId} = rpc:call(Worker1, communicator, communicate_async,
        [ServerMsgInternal, SessionId, Self]),

    % then
    ?assertReceivedMatch(#client_message{
        message_id = MsgId, message_body = Status
    }, ?TIMEOUT),

    % given
    test_utils:mock_expect(Workers, router, route_message, fun
        (#client_message{message_id = Id = #message_id{issuer = Issuer,
            recipient = undefined}}) ->
            Issuer = oneprovider:get_id(),
            Self ! {router_message_called, Id},
            ok
    end),

    % when
    {ok, MsgId2} = rpc:call(Worker1, communicator, communicate_async,
        [ServerMsgInternal, SessionId]),

    % then
    ?assertReceivedMatch({router_message_called, MsgId2}, ?TIMEOUT),
    ok = ssl:close(Sock).

multi_ping_pong_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 5},
        {success_rate, 90},
        {parameters, [
            [{name, connections_num}, {value, 10}, {description, "Number of connections."}],
            [{name, msg_num}, {value, 1000}, {description, "Number of messages sent and received."}],
            [{name, transport}, {value, ssl}, {description, "Connection transport type."}]
        ]},
        {description, "Opens 'connections_num' connections and for each connection, "
        "then sends 'msg_num' ping messages and finally receives 'msg_num' pong "
        "messages."},
        {config, [{name, ssl},
            {parameters, [
                [{name, msg_num}, {value, 100000}]
            ]}
        ]}
    ]
    ).
multi_ping_pong_test_base(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    ConnNumbers = ?config(connections_num, Config),
    MsgNum = ?config(msg_num, Config),
    ConnNumbersList = [integer_to_binary(N) || N <- lists:seq(1, ConnNumbers)],
    MsgNumbers = lists:seq(1, MsgNum),
    MsgNumbersBin = lists:map(fun(N) -> integer_to_binary(N) end, MsgNumbers),
    Pings = lists:map(fun(N) ->
        #'ClientMessage'{message_id = N, message_body = {ping, #'Ping'{}}}
    end, MsgNumbersBin),
    RawPings = lists:map(fun(E) -> messages:encode_msg(E) end, Pings),
    initializer:remove_pending_messages(),
    Self = self(),

    T1 = erlang:monotonic_time(milli_seconds),
    [
        spawn_link(fun() ->
            % when
            {ok, {Sock, _}} = connect_via_token(Worker1, [{active, true}]),
            lists:foreach(fun(E) ->
                ok = ssl:send(Sock, E)
            end, RawPings),
            Received = lists:map(fun(_) ->
                Pong = receive_server_message(),
                ?assertMatch(#'ServerMessage'{message_body = {pong, #'Pong'{}}}, Pong),
                {binary_to_integer(Pong#'ServerMessage'.message_id), Pong}
            end, MsgNumbersBin),

            % then
            {_, ReceivedInOrder} = lists:unzip(lists:keysort(1, Received)),
            IdToMessage = lists:zip(MsgNumbersBin, ReceivedInOrder),
            lists:foreach(fun({Id, #'ServerMessage'{message_id = MsgId}}) ->
                ?assertEqual(Id, MsgId)
            end, IdToMessage),
            ok = ssl:close(Sock),
            Self ! success
        end) || _ <- ConnNumbersList
    ],
    lists:foreach(fun(_) ->
        ?assertReceivedMatch(success, infinity)
    end, ConnNumbersList),
    T2 = erlang:monotonic_time(milli_seconds),
    #parameter{name = full_time, value = T2 - T1, unit = "ms"}.

sequential_ping_pong_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 5},
        {success_rate, 80},
        {parameters, [
            [{name, msg_num}, {value, 1000}, {description, "Number of messages sent and received."}]
        ]},
        {description, "Opens connection and then sends and receives ping/pong message 'msg_num' times."},
        {config, [{name, sequential_ping_pong},
            {parameters, [
                [{name, msg_num}, {value, 100000}]
            ]}
        ]}
    ]
    ).
sequential_ping_pong_test_base(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    MsgNum = ?config(msg_num, Config),
    MsgNumbers = lists:seq(1, MsgNum),
    MsgNumbersBin = lists:map(fun(N) -> integer_to_binary(N) end, MsgNumbers),
    Pings = lists:map(fun(N) ->
        #'ClientMessage'{message_id = N, message_body = {ping, #'Ping'{}}}
    end, MsgNumbersBin),
    RawPings = lists:map(fun(E) -> messages:encode_msg(E) end, Pings),
    initializer:remove_pending_messages(),

    % when
    {ok, {Sock, _}} = connect_via_token(Worker1),
    T1 = erlang:monotonic_time(milli_seconds),
    lists:foldl(fun(E, N) ->
        % send ping
        ok = ssl:send(Sock, E),

        % receive & validate pong
        BinaryN = integer_to_binary(N),
        ?assertMatch(#'ServerMessage'{message_body = {
            pong, #'Pong'{}
        }, message_id = BinaryN}, receive_server_message()),
        N + 1
    end, 1, RawPings),
    T2 = erlang:monotonic_time(milli_seconds),

    % then
    ok = ssl:close(Sock),

    #parameter{name = full_time, value = T2 - T1, unit = "ms"}.

multi_connection_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 10},
        {success_rate, 90},
        {parameters, [
            [{name, connections_num}, {value, 100}, {description, "Number of connections."}]
        ]},
        {description, "Opens 'connections_num' connections to the server, checks "
        "their state, and closes them."},
        {config, [{name, multi_connection},
            {parameters, [
                [{name, connections_num}, {value, 1000}]
            ]}
        ]}
    ]
    ).
multi_connection_test_base(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    ConnNumbers = ?config(connections_num, Config),
    ConnNumbersList = [integer_to_binary(N) || N <- lists:seq(1, ConnNumbers)],
    initializer:remove_pending_messages(),

    % when
    Connections = lists:map(fun(_) ->
        connect_via_token(Worker1, [])
    end, ConnNumbersList),

    % then
    lists:foreach(fun(ConnectionAns) ->
        ?assertMatch({ok, {_, _}}, ConnectionAns),
        {ok, {_Sock, SessId}} = ConnectionAns,
        ?assert(is_binary(SessId))
    end, Connections),
    lists:foreach(fun({ok, {Sock, _}}) -> ssl:close(Sock) end, Connections).

bandwidth_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 5},
        {success_rate, 80},
        {parameters, [
            [{name, packet_size}, {value, 1024}, {unit, "kB"}, {description, "Size of packet."}],
            [{name, packet_num}, {value, 10}, {description, "Number of packets."}],
            [{name, transport}, {value, ssl}, {description, "Connection transport type."}]
        ]},
        {config, [{name, ssl},
            {parameters, [
                [{name, packet_num}, {value, 1000}]
            ]}
        ]}
    ]
    ).
bandwidth_test_base(Config) ->
    % given
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    PacketSize = ?config(packet_size, Config),
    PacketNum = ?config(packet_num, Config),
    Data = crypto:strong_rand_bytes(PacketSize * 1024),
    Packet = #'ClientMessage'{message_body = {ping, #'Ping'{data = Data}}},
    PacketRaw = messages:encode_msg(Packet),


    initializer:remove_pending_messages(),
    Self = self(),
    test_utils:mock_expect(Workers, router, route_message, fun
        (#client_message{message_body = #ping{}}) ->
            Self ! router_message_called,
            ok
    end),

    % when
    {ok, {Sock, _}} = connect_via_token(Worker1, [{active, true}]),
    T1 = erlang:monotonic_time(milli_seconds),
    lists:foreach(fun(_) ->
        ok = ssl:send(Sock, PacketRaw)
    end, lists:seq(1, PacketNum)),
    T2 = erlang:monotonic_time(milli_seconds),

    % then
    lists:foreach(fun(_) ->
        ?assertReceivedMatch(router_message_called, ?TIMEOUT)
    end, lists:seq(1, PacketNum)),
    T3 = erlang:monotonic_time(milli_seconds),
    ssl:close(Sock),
    [
        #parameter{name = sending_time, value = T2 - T1, unit = "ms"},
        #parameter{name = receiving_time, value = T3 - T2, unit = "ms"},
        #parameter{name = full_time, value = T3 - T1, unit = "ms"}
    ].

python_client_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 5},
        {success_rate, 80},
        {parameters, [
            [{name, packet_size}, {value, 1024}, {unit, "kB"}, {description, "Size of packet."}],
            [{name, packet_num}, {value, 10}, {description, "Number of packets."}]
        ]},
        {description, "Same as bandwidth_test, but with ssl client written in python."},
        {config, [{name, python_client},
            {parameters, [
                [{name, packet_num}, {value, 1000}]
            ]}
        ]}
    ]
    ).
python_client_test_base(Config) ->
    % given
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    PacketSize = ?config(packet_size, Config),
    PacketNum = ?config(packet_num, Config),

    Data = crypto:strong_rand_bytes(PacketSize * 1024),
    Packet = #'ClientMessage'{message_body = {ping, #'Ping'{data = Data}}},
    PacketRaw = messages:encode_msg(Packet),

    HandshakeMessage = #'ClientMessage'{message_body = {client_handshake_request,
        #'ClientHandshakeRequest'{session_id = <<"session_id">>, token = #'Token'{
            value = ?MACAROON
        }}
    }},
    HandshakeMessageRaw = messages:encode_msg(HandshakeMessage),

    initializer:remove_pending_messages(),
    Self = self(),
    test_utils:mock_expect(Workers, router, route_message, fun
        (#client_message{message_body = #ping{}}) ->
            Self ! router_message_called,
            ok;
        (_) ->
            ok
    end),

    ClientPath = ?TEST_FILE(Config, "ssl_client.py"),
    MessagePath = ?TEST_FILE(Config, "message.arg"),
    HandshakeMsgPath = ?TEST_FILE(Config, "handshake.arg"),
    file:write_file(MessagePath, PacketRaw),
    file:write_file(HandshakeMsgPath, HandshakeMessageRaw),
    Host = utils:get_host(Worker1),
    {ok, Port} = test_utils:get_env(Worker1, ?APP_NAME, protocol_handler_port),

    % when
    T1 = erlang:monotonic_time(milli_seconds),
    Args = [
        "--host", Host,
        "--port", integer_to_list(Port),
        "--handshake-message", HandshakeMsgPath,
        "--message", MessagePath,
        "--count", integer_to_list(PacketNum)
    ],
    PythonClient = open_port({spawn_executable, ClientPath}, [{args, Args}]),

    % then
    lists:foreach(fun(_) ->
        ?assertReceivedMatch(router_message_called, timer:seconds(15))
    end, lists:seq(1, PacketNum)),
    T2 = erlang:monotonic_time(milli_seconds),
    catch port_close(PythonClient),
    #parameter{name = full_time, value = T2 - T1, unit = "ms"}.

proto_version_test(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    MsgId = <<"message_id">>,
    GetProtoVersion = #'ClientMessage'{
        message_id = MsgId,
        message_body = {get_protocol_version, #'GetProtocolVersion'{}}
    },
    GetProtoVersionRaw = messages:encode_msg(GetProtoVersion),
    {ok, {Sock, _}} = connect_via_token(Worker1),

    % when
    ok = ssl:send(Sock, GetProtoVersionRaw),

    %then
    #'ServerMessage'{message_body = {_, #'ProtocolVersion'{
        major = Major, minor = Minor
    }}} = ?assertMatch(#'ServerMessage'{
        message_id = MsgId,
        message_body = {protocol_version, #'ProtocolVersion'{}}
    }, receive_server_message()),
    ?assert(is_integer(Major)),
    ?assert(is_integer(Minor)),
    ok = ssl:close(Sock).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [initializer]} | Config].

init_per_testcase(cert_connection_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, serializator),
    init_per_testcase(default, Config);

init_per_testcase(Case, Config) when
    Case =:= protobuf_msg_test;
    Case =:= multi_message_test;
    Case =:= client_communicate_async_test;
    Case =:= bandwidth_test;
    Case =:= python_client_test ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, router),
    init_per_testcase(default, Config);

init_per_testcase(default, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ssl:start(),
    initializer:remove_pending_messages(),
    mock_identity(Workers),
    test_utils:mock_new(Workers, provider_auth),
    test_utils:mock_expect(Workers, provider_auth, get_provider_id, fun() -> <<"providerId">> end),
    Config;

init_per_testcase(_Case, Config) ->
    init_per_testcase(default, Config).


end_per_testcase(cert_connection_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, [serializator]),
    end_per_testcase(default, Config);

end_per_testcase(Case, Config) when
    Case =:= protobuf_msg_test;
    Case =:= multi_message_test;
    Case =:= client_communicate_async_test;
    Case =:= bandwidth_test;
    Case =:= python_client_test ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, [router]),
    end_per_testcase(default, Config);

end_per_testcase(python_client_test, Config) ->
    file:delete(?TEST_FILE(Config, "handshake.arg")),
    file:delete(?TEST_FILE(Config, "message.arg")),
    end_per_testcase(default, Config);

end_per_testcase(default, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, [provider_auth, user_identity]),
    ssl:stop();

end_per_testcase(_Case, Config) ->
    end_per_testcase(default, Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node using token, with default socket_opts
%% @equiv connect_via_token(Node, [{active, true}], ssl)
%% @end
%%--------------------------------------------------------------------
-spec connect_via_token(Node :: node()) ->
    {ok, {Sock :: ssl:socket(), SessId :: session:id()}} | no_return().
connect_via_token(Node) ->
    connect_via_token(Node, [{active, true}]).

connect_via_token(Node, SocketOpts) ->
    connect_via_token(Node, SocketOpts, crypto:strong_rand_bytes(10)).

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node using token, with custom socket opts
%% @end
%%--------------------------------------------------------------------
-spec connect_via_token(Node :: node(), SocketOpts :: list(), session:id()) ->
    {ok, {Sock :: term(), SessId :: session:id()}}.
connect_via_token(Node, SocketOpts, SessId) ->
    % given
    TokenAuthMessage = #'ClientMessage'{message_body = {client_handshake_request,
        #'ClientHandshakeRequest'{session_id = SessId, token = #'Token'{
            value = ?MACAROON
        }}
    }},
    TokenAuthMessageRaw = messages:encode_msg(TokenAuthMessage),
    ActiveOpt = case proplists:get_value(active, SocketOpts) of
        undefined -> [];
        Other -> [{active, Other}]
    end,
    NewSocketOpts = proplists:delete(active, SocketOpts),
    {ok, Port} = test_utils:get_env(Node, ?APP_NAME, protocol_handler_port),

    % when
    {ok, Sock} = (catch ssl:connect(utils:get_host(Node), Port, [binary,
        {packet, 4}, {active, once}, {reuse_sessions, false} | NewSocketOpts
    ], timer:minutes(1))),
    ok = ssl:send(Sock, TokenAuthMessageRaw),

    % then
    #'ServerMessage'{message_body = {handshake_response, #'HandshakeResponse'{
        status = 'OK'
    }}} = ?assertMatch(#'ServerMessage'{message_body = {handshake_response, _}},
        receive_server_message()
    ),
    ssl:setopts(Sock, ActiveOpt),
    {ok, {Sock, SessId}}.

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node, and send back each message received from the server
%% @end
%%--------------------------------------------------------------------
-spec spawn_ssl_echo_client(NodeToConnect :: node()) ->
    {ok, {Sock :: ssl:socket(), SessId :: session:id()}}.
spawn_ssl_echo_client(NodeToConnect) ->
    {ok, {Sock, SessionId}} = connect_via_token(NodeToConnect, []),
    SslEchoClient =
        fun Loop() ->
            % receive data from server
            case ssl:recv(Sock, 0) of
                {ok, Data} ->
                    % decode
                    #'ServerMessage'{message_id = Id, message_body = Body} =
                        messages:decode_msg(Data, 'ServerMessage'),

                    % respond with the same data to the server (excluding stream_reset)
                    case Body of
                        {message_stream_reset, _} -> ok;
                        {subscription, _} -> ok;
                        _ ->
                            ClientAnsProtobuf = #'ClientMessage'{message_id = Id, message_body = Body},
                            ClientAnsRaw = messages:encode_msg(ClientAnsProtobuf),
                            ?assertEqual(ok, ssl:send(Sock, ClientAnsRaw))
                    end,

                    %loop back
                    Loop();
                {error, closed} -> ok;
                Error -> ?error("ssl_echo_client error: ~p", [Error])
            end
        end,
    spawn_link(SslEchoClient),
    {ok, {Sock, SessionId}}.

mock_identity(Workers) ->
    test_utils:mock_new(Workers, user_identity),
    test_utils:mock_expect(Workers, user_identity, get_or_fetch,
        fun(#token_auth{token = ?MACAROON}) ->
            {ok, #document{value = #user_identity{}}}
        end
    ).

receive_server_message() ->
    receive_server_message([message_stream_reset, subscription]).

receive_server_message(IgnoredMsgList) ->
    receive
        {_, _, Data} ->
            % ignore listed messages
            Msg = messages:decode_msg(Data, 'ServerMessage'),
            MsgType = element(1, Msg#'ServerMessage'.message_body),
            case lists:member(MsgType, IgnoredMsgList) of
                true -> receive_server_message(IgnoredMsgList);
                false -> Msg
            end
    after ?TIMEOUT ->
        {error, timeout}
    end.
