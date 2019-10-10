%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests connection module.
%%% @end
%%%--------------------------------------------------------------------
-module(connection_test_SUITE).
-author("Bartosz Walkowicz").

-include("fuse_test_utils.hrl").
-include("global_definitions.hrl").
-include("proto/common/clproto_message_id.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/common/handshake_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/onedata.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    provider_connection_test/1,
    client_connection_test/1,
    python_client_test/1,
    multi_connection_test/1,

    protobuf_message_test/1,
    sequential_ping_pong_test/1,
    multi_ping_pong_test/1,

    bandwidth_test/1,
    bandwidth_test2/1,

    rtransfer_connection_secret_test/1,
    rtransfer_nodes_ips_test/1,

    sending_server_msg_via_incoming_connection_should_succeed/1,
    sending_client_msg_via_incoming_connection_should_fail/1,

    errors_other_then_socket_ones_should_not_terminate_connection/1,
    socket_error_should_terminate_connection/1
]).

%% test_bases
-export([
    python_client_test_base/1,
    multi_connection_test_base/1,
    sequential_ping_pong_test_base/1,
    multi_ping_pong_test_base/1,
    bandwidth_test_base/1,
    bandwidth_test2_base/1
]).

-define(NORMAL_CASES, [
    provider_connection_test,
    client_connection_test,
    python_client_test,
    multi_connection_test,

    protobuf_message_test,
    sequential_ping_pong_test,
    multi_ping_pong_test,

    bandwidth_test,
    bandwidth_test2,

    rtransfer_connection_secret_test,
    rtransfer_nodes_ips_test,

    sending_server_msg_via_incoming_connection_should_succeed,
    sending_client_msg_via_incoming_connection_should_fail,

    errors_other_then_socket_ones_should_not_terminate_connection,
    socket_error_should_terminate_connection
]).

-define(PERFORMANCE_CASES, [
    python_client_test,
    multi_connection_test,
    sequential_ping_pong_test,
    multi_ping_pong_test,
    bandwidth_test,
    bandwidth_test2
]).

all() -> ?ALL(?NORMAL_CASES, ?PERFORMANCE_CASES).

-define(CORRECT_PROVIDER_ID, <<"correct-iden-mac">>).
-define(INCORRECT_PROVIDER_ID, <<"incorrect-iden-mac">>).
-define(CORRECT_TOKEN, <<"correct-token">>).
-define(INCORRECT_TOKEN, <<"incorrect-token">>).

-define(ATTEMPTS, 60).


%%%===================================================================
%%% Test functions
%%%===================================================================


provider_connection_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    lists:foreach(fun({ProviderId, Nonce, ExpStatus}) ->
        ?assertMatch(ExpStatus, handshake_as_provider(Worker1, ProviderId, Nonce))
    end, [
        {?INCORRECT_PROVIDER_ID, ?CORRECT_TOKEN, 'INVALID_PROVIDER'},
        {?INCORRECT_PROVIDER_ID, ?INCORRECT_TOKEN, 'INVALID_MACAROON'},
        {?CORRECT_PROVIDER_ID, ?INCORRECT_TOKEN, 'INVALID_MACAROON'},
        {?CORRECT_PROVIDER_ID, ?CORRECT_TOKEN, 'OK'}
    ]).


client_connection_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    OpVersion = rpc:call(Worker1, oneprovider, get_version, []),
    {ok, [CompatibleVersion | _]} = rpc:call(
        Worker1, compatibility, get_compatible_versions, [?ONEPROVIDER, OpVersion, ?ONECLIENT]
    ),
    Macaroon = #'Macaroon'{
        macaroon = ?TOKEN
    },

    lists:foreach(fun({Macaroon, Version, ExpStatus}) ->
        ?assertMatch(ExpStatus, handshake_as_client(Worker1, Macaroon, Version))
    end, [
        {Macaroon, <<"16.07-rc2">>, 'INCOMPATIBLE_VERSION'},
        {Macaroon, binary_to_list(CompatibleVersion), 'OK'}
    ]).


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
    ]).
python_client_test_base(Config) ->
    % given
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    PacketSize = ?config(packet_size, Config),
    PacketNum = ?config(packet_num, Config),

    Data = crypto:strong_rand_bytes(PacketSize * 1024),
    Packet = #'ClientMessage'{message_body = {ping, #'Ping'{data = Data}}},
    PacketRaw = messages:encode_msg(Packet),

    OpVersion = rpc:call(Worker1, oneprovider, get_version, []),
    {ok, [Version | _]} = rpc:call(
        Worker1, compatibility, get_compatible_versions, [?ONEPROVIDER, OpVersion, ?ONECLIENT]
    ),

    HandshakeMessage = #'ClientMessage'{message_body = {client_handshake_request,
        #'ClientHandshakeRequest'{
            session_id = <<"session_id">>,
            macaroon = #'Macaroon'{macaroon = ?TOKEN},
            version = Version
        }
    }},
    HandshakeMessageRaw = messages:encode_msg(HandshakeMessage),

    Self = self(),
    test_utils:mock_expect(Workers, router, route_message, fun
        (#client_message{message_body = #ping{}}, _) ->
            Self ! router_message_called,
            ok;
        (_, _) ->
            ok
    end),

    ClientPath = ?TEST_FILE(Config, "ssl_client.py"),
    MessagePath = ?TEST_FILE(Config, "message.arg"),
    HandshakeMsgPath = ?TEST_FILE(Config, "handshake.arg"),
    file:write_file(MessagePath, PacketRaw),
    file:write_file(HandshakeMsgPath, HandshakeMessageRaw),
    Host = utils:get_host(Worker1),
    {ok, Port} = test_utils:get_env(Worker1, ?APP_NAME, https_server_port),

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
    ]).
multi_connection_test_base(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    ConnNumbers = ?config(connections_num, Config),
    ConnNumbersList = [integer_to_binary(N) || N <- lists:seq(1, ConnNumbers)],

    % when
    Connections = lists:map(fun(_) ->
        fuse_test_utils:connect_via_token(Worker1, [])
    end, ConnNumbersList),

    % then
    lists:foreach(fun(ConnectionAns) ->
        ?assertMatch({ok, {_, _}}, ConnectionAns),
        {ok, {_Sock, SessId}} = ConnectionAns,
        ?assert(is_binary(SessId))
    end, Connections),
    lists:foreach(fun({ok, {Sock, _}}) -> ssl:close(Sock) end, Connections).


protobuf_message_test(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    {ok, {Sock, _}} = fuse_test_utils:connect_via_token(Worker1),
    {Major, Minor} = fuse_test_utils:get_protocol_version(Sock),
    ?assert(is_integer(Major)),
    ?assert(is_integer(Minor)),
    ok = ssl:close(Sock).


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
    Pings = lists:map(fun(Num) ->
        MsgId = integer_to_binary(Num),
        Msg = #'ClientMessage'{message_id = MsgId, message_body = {ping, #'Ping'{}}},
        {MsgId, messages:encode_msg(Msg)}
    end, lists:seq(1, MsgNum)),

    % when
    {ok, {Sock, _}} = fuse_test_utils:connect_via_token(Worker1),

    T1 = erlang:monotonic_time(milli_seconds),
    lists:foreach(fun({MsgId, Ping}) ->
        % send ping
        ok = ssl:send(Sock, Ping),

        % receive & validate pong
        ?assertMatch(#'ServerMessage'{
            message_id = MsgId,
            message_body = {pong, #'Pong'{}}
        }, fuse_test_utils:receive_server_message())
    end, Pings),
    T2 = erlang:monotonic_time(milli_seconds),

    % then
    ok = ssl:close(Sock),

    #parameter{name = full_time, value = T2 - T1, unit = "ms"}.


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

    Self = self(),

    T1 = erlang:monotonic_time(milli_seconds),
    [
        spawn_link(fun() ->
            % when
            {ok, {Sock, _}} = fuse_test_utils:connect_via_token(Worker1, [{active, true}]),
            lists:foreach(fun(E) ->
                ok = ssl:send(Sock, E)
                          end, RawPings),
            Received = lists:map(fun(_) ->
                Pong = fuse_test_utils:receive_server_message(),
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
    ]).
bandwidth_test_base(Config) ->
    % given
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    PacketSize = ?config(packet_size, Config),
    PacketNum = ?config(packet_num, Config),
    Data = crypto:strong_rand_bytes(PacketSize * 1024),
    Packet = #'ClientMessage'{message_body = {ping, #'Ping'{data = Data}}},
    PacketRaw = messages:encode_msg(Packet),

    Self = self(),
    test_utils:mock_expect(Workers, router, route_message, fun
        (#client_message{message_body = #ping{}}, _) ->
            Self ! router_message_called,
            ok
    end),

    % when
    {ok, {Sock, _}} = fuse_test_utils:connect_via_token(Worker1, [{active, true}]),
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


bandwidth_test2(Config) ->
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
bandwidth_test2_base(Config) ->
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
    test_utils:mock_expect(Workers, router, route_message, fun
        (#client_message{message_body = #events{events = [#event{
            type = #file_read_event{counter = Counter}
        }]}}, _) ->
            Self ! Counter,
            ok
    end),

    % when
    {ok, {Sock, _}} = fuse_test_utils:connect_via_token(Worker1, [{active, true}]),
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


rtransfer_connection_secret_test(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    {ok, Sock} = fuse_test_utils:connect_as_provider(
        Worker1, ?CORRECT_PROVIDER_ID, ?CORRECT_TOKEN
    ),
    ssl:setopts(Sock, [{active, once}, {packet, 4}]),

    Secret = fuse_test_utils:generate_rtransfer_conn_secret(Sock),
    ?assert(is_binary(Secret)),

    ok = ssl:close(Sock).


rtransfer_nodes_ips_test(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    ClusterIPs = rpc:call(Worker1, node_manager, get_cluster_ips, []),

    ExpectedIPs = [list_to_binary(inet:ntoa(IP)) || IP <- ClusterIPs],
    ExpectedPort = proplists:get_value(server_port,
        application:get_env(rtransfer_link, transfer, []), 6665
    ),
    ExpectedNodes = lists:sort(
        [#'IpAndPort'{ip = IP, port = ExpectedPort} || IP <- ExpectedIPs]
    ),

    {ok, Sock} = fuse_test_utils:connect_as_provider(
        Worker1, ?CORRECT_PROVIDER_ID, ?CORRECT_TOKEN
    ),

    ssl:setopts(Sock, [{active, once}, {packet, 4}]),
    RespNodes = fuse_test_utils:get_rtransfer_nodes_ips(Sock),

    ?assertEqual(ExpectedNodes, RespNodes),
    ok = ssl:close(Sock).


sending_server_msg_via_incoming_connection_should_succeed(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    {ok, {Sock, SessionId}} = fuse_test_utils:connect_via_token(Worker1),

    Description = <<"desc">>,
    ServerMsgInternal = #server_message{message_body = #status{
        code = ?OK,
        description = Description
    }},
    ServerMessageProtobuf = #'ServerMessage'{
        message_id = undefined,
        message_body = {status, #'Status'{
            code = ?OK,
            description = Description
        }}
    },

    % when
    ?assertMatch(ok, send_sync_msg(Worker1, SessionId, ServerMsgInternal)),

    % then
    ?assertEqual(ServerMessageProtobuf, fuse_test_utils:receive_server_message()),
    ok = ssl:close(Sock).


sending_client_msg_via_incoming_connection_should_fail(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    {ok, {Sock, SessionId}} = fuse_test_utils:connect_via_token(Worker1),

    ClientMsg = #client_message{message_body = #status{
        code = ?OK,
        description = <<"desc">>
    }},

    % when
    ?assertMatch(
        {error, sending_msg_via_wrong_conn_type},
        send_sync_msg(Worker1, SessionId, ClientMsg)
    ),

    % then
    ?assertEqual({error, timeout}, fuse_test_utils:receive_server_message()),

    ok = ssl:close(Sock).


errors_other_then_socket_ones_should_not_terminate_connection(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    {ok, {Sock, _}} = fuse_test_utils:connect_via_token(Worker1),
    Ping = fuse_test_utils:generate_ping_message(),

    lists:foreach(fun({MockFun, UnMockFun}) ->
        MockFun(Worker1),
        ssl:send(Sock, Ping),
        ?assertEqual({error, timeout}, fuse_test_utils:receive_server_message()),
        UnMockFun(Worker1),
        fuse_test_utils:ping(Sock)
    end, [
        {fun mock_client_msg_decoding/1, fun unmock_serializer/1},
        {fun mock_server_msg_encoding/1, fun unmock_serializer/1},
        {fun mock_route_msg/1, fun unmock_route_msg/1}
    ]),

    ok = ssl:close(Sock).


socket_error_should_terminate_connection(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    {ok, {Sock, _}} = fuse_test_utils:connect_via_token(Worker1),

    mock_ranch_ssl(Worker1),
    Ping = fuse_test_utils:generate_ping_message(),
    ssl:send(Sock, Ping),
    ?assertEqual({error, timeout}, fuse_test_utils:receive_server_message()),
    unmock_ranch_ssl(Worker1),

    ?assertMatch({error, closed}, ssl:send(Sock, Ping)),
    ok = ssl:close(Sock).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(_) ->
    ok.


init_per_testcase(Case, Config) when
    Case =:= provider_connection_test;
    Case =:= rtransfer_connection_secret_test;
    Case =:= rtransfer_nodes_ips_test
->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, token_logic, [passthrough]),

    test_utils:mock_expect(Workers, token_logic, verify_identity, fun
        (?CORRECT_TOKEN) ->
            {ok, ?SUB(?ONEPROVIDER, ?CORRECT_PROVIDER_ID)};
        (?INCORRECT_TOKEN) ->
            ?ERROR_BAD_TOKEN
    end),

    test_utils:mock_expect(Workers, provider_logic, assert_zone_compatibility, fun() ->
        ok
    end),

    init_per_testcase(default, Config);

init_per_testcase(Case, Config) when
    Case =:= bandwidth_test;
    Case =:= bandwidth_test2;
    Case =:= python_client_test
->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, router),
    init_per_testcase(default, Config);

init_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ssl:start(),
    initializer:remove_pending_messages(),
    mock_identity(Workers),

    initializer:mock_provider_id(
        Workers, <<"providerId">>, <<"access-token">>, <<"identity-token">>
    ),
    Config.


end_per_testcase(Case, Config) when
    Case =:= provider_connection_test;
    Case =:= rtransfer_connection_secret_test;
    Case =:= rtransfer_nodes_ips_test
->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, [provider_logic, token_logic]),
    end_per_testcase(default, Config);

end_per_testcase(python_client_test, Config) ->
    file:delete(?TEST_FILE(Config, "handshake.arg")),
    file:delete(?TEST_FILE(Config, "message.arg")),

    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, [router]),
    end_per_testcase(default, Config);

end_per_testcase(Case, Config) when
    Case =:= bandwidth_test;
    Case =:= bandwidth_test2
->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, [router]),
    end_per_testcase(default, Config);

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:unmock_provider_ids(Workers),
    test_utils:mock_validate_and_unload(Workers, [user_identity]),
    ssl:stop().


%%%===================================================================
%%% Internal functions
%%%===================================================================


handshake_as_provider(Node, ProviderId, Nonce) ->
    case fuse_test_utils:connect_as_provider(Node, ProviderId, Nonce) of
        {ok, Sock} ->
            ssl:close(Sock),
            'OK';
        {error, HandshakeResp} ->
            HandshakeResp
    end.


handshake_as_client(Node, Token, Version) ->
    Nonce = crypto:strong_rand_bytes(10),
    case fuse_test_utils:connect_as_client(Node, Nonce, Token, Version) of
        {ok, Sock} ->
            ssl:close(Sock),
            'OK';
        {error, HandshakeResp} ->
            HandshakeResp
    end.


send_sync_msg(Node, SessId, Msg) ->
    {ok, #document{value = #session{connections = [Conn | _]}}} = ?assertMatch(
        {ok, #document{value = #session{connections = [Conn | _]}}},
        rpc:call(Node, session, get, [SessId]),
        ?ATTEMPTS
    ),
    rpc:call(Node, connection, send_msg, [Conn, Msg]).


mock_identity(Workers) ->
    test_utils:mock_new(Workers, user_identity),
    test_utils:mock_expect(Workers, user_identity, get_or_fetch,
        fun(#token_auth{token = ?TOKEN}) ->
            {ok, #document{value = #user_identity{}}}
        end
    ).


mock_client_msg_decoding(Node) ->
    test_utils:mock_new(Node, clproto_serializer, [passthrough]),

    test_utils:mock_expect(Node, clproto_serializer, deserialize_client_message, fun(_, _) ->
        throw(inproper_msg)
    end).


mock_server_msg_encoding(Node) ->
    test_utils:mock_new(Node, clproto_serializer, [passthrough]),

    test_utils:mock_expect(Node, clproto_serializer, serialize_server_message, fun(_, _) ->
        throw(inproper_msg)
    end).


unmock_serializer(Node) ->
    test_utils:mock_unload(Node, [clproto_serializer]).


mock_route_msg(Node) ->
    test_utils:mock_new(Node, router, [passthrough]),

    test_utils:mock_expect(Node, router, route_message, fun(_, _) ->
        throw(you_shall_not_pass)
    end).


unmock_route_msg(Node) ->
    test_utils:mock_unload(Node, [router]).


mock_ranch_ssl(Node) ->
    test_utils:mock_new(Node, ranch_ssl, [passthrough]),

    test_utils:mock_expect(Node, ranch_ssl, send, fun(_, _) ->
        {error, you_shall_not_send}
    end).


unmock_ranch_ssl(Node) ->
    test_utils:mock_unload(Node, [ranch_ssl]).
