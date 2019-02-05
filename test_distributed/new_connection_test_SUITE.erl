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
-module(new_connection_test_SUITE).
-author("Bartosz Walkowicz").

-include("fuse_utils.hrl").
-include("global_definitions.hrl").
-include("proto/oneclient/message_id.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/common/handshake_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/api_errors.hrl").
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
    rtransfer_nodes_ips_test/1

%%    broken_connection_test/1,
%%    client_send_test/1,
%%    client_communicate_test/1,
%%    client_communicate_async_test/1,
%%    timeouts_test/1,
%%    client_keepalive_test/1,
%%    socket_timeout_test/1,
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

-define(NORMAL_CASES_NAMES, [
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
    rtransfer_nodes_ips_test

%%    broken_connection_test,
%%    client_send_test,
%%    client_communicate_test,
%%    client_communicate_async_test
%%    socket_timeout_test,
%%    timeouts_test,
%%    client_keepalive_test,
]).

-define(PERFORMANCE_CASES_NAMES, [
    python_client_test,
    multi_connection_test,
    sequential_ping_pong_test,
    multi_ping_pong_test,
    bandwidth_test,
    bandwidth_test2
]).

all() -> ?ALL(?NORMAL_CASES_NAMES, ?PERFORMANCE_CASES_NAMES).

-define(CORRECT_PROVIDER_ID, <<"correct-iden-mac">>).
-define(INCORRECT_PROVIDER_ID, <<"incorrect-iden-mac">>).
-define(CORRECT_NONCE, <<"correct-nonce">>).
-define(INCORRECT_NONCE, <<"incorrect-nonce">>).

-define(req(W, SessId, FuseRequest), element(2, rpc:call(W, worker_proxy, call,
    [fslogic_worker, {fuse_request, SessId, #fuse_request{fuse_request = FuseRequest}}]))).

-record(file_handle, {
    handle :: helpers_nif:file_handle(),
    timeout :: timeout()
}).


%%%===================================================================
%%% Test functions
%%%===================================================================


provider_connection_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    lists:foreach(fun({ProviderId, Nonce, ExpStatus}) ->
        ?assertMatch(ExpStatus, handshake_as_provider(Worker1, ProviderId, Nonce))
    end, [
        {?INCORRECT_PROVIDER_ID, ?CORRECT_NONCE, 'INVALID_PROVIDER'},
        {?INCORRECT_PROVIDER_ID, ?INCORRECT_NONCE, 'INVALID_PROVIDER'},
        {?CORRECT_PROVIDER_ID, ?INCORRECT_NONCE, 'INVALID_NONCE'},
        {?CORRECT_PROVIDER_ID, ?CORRECT_NONCE, 'OK'}
    ]).


client_connection_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    {ok, [CompatibleVersion | _]} = rpc:call(
        Worker1, application, get_env, [?APP_NAME, compatible_oc_versions]
    ),
    Macaroon = #'Macaroon'{
        macaroon = ?MACAROON,
        disch_macaroons = ?DISCH_MACAROONS
    },

    lists:foreach(fun({Macaroon, Version, ExpStatus}) ->
        ?assertMatch(ExpStatus, handshake_as_client(Worker1, Macaroon, Version))
    end, [
        {Macaroon, <<"16.07-rc2">>, 'INCOMPATIBLE_VERSION'},
        {Macaroon, CompatibleVersion, 'OK'}
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

    {ok, [Version | _]} = rpc:call(
        Worker1, application, get_env, [?APP_NAME, compatible_oc_versions]
    ),

    HandshakeMessage = #'ClientMessage'{message_body = {client_handshake_request,
        #'ClientHandshakeRequest'{
            session_id = <<"session_id">>,
            macaroon = #'Macaroon'{macaroon = ?MACAROON, disch_macaroons = ?DISCH_MACAROONS},
            version = list_to_binary(Version)
        }
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
        fuse_utils:connect_via_macaroon(Worker1, [])
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
    {ok, {Sock, _}} = fuse_utils:connect_via_macaroon(Worker1),
    {Major, Minor} = fuse_utils:get_protocol_version(Sock),
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

    initializer:remove_pending_messages(),

    % when
    {ok, {Sock, _}} = fuse_utils:connect_via_macaroon(Worker1),

    T1 = erlang:monotonic_time(milli_seconds),
    lists:foreach(fun({MsgId, Ping}) ->
        % send ping
        ok = ssl:send(Sock, Ping),

        % receive & validate pong
        ?assertMatch(#'ServerMessage'{
            message_id = MsgId,
            message_body = {pong, #'Pong'{}}
        }, fuse_utils:receive_server_message())
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

    initializer:remove_pending_messages(),
    Self = self(),

    T1 = erlang:monotonic_time(milli_seconds),
    [
        spawn_link(fun() ->
            % when
            {ok, {Sock, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}]),
            lists:foreach(fun(E) ->
                ok = ssl:send(Sock, E)
                          end, RawPings),
            Received = lists:map(fun(_) ->
                Pong = fuse_utils:receive_server_message(),
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
    {ok, {Sock, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}]),
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
    initializer:remove_pending_messages(),
    test_utils:mock_expect(Workers, router, route_message, fun
        (#client_message{message_body = #events{events = [#event{
            type = #file_read_event{counter = Counter}
        }]}}) ->
            Self ! Counter,
            ok
    end),

    % when
    {ok, {Sock, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}]),
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

    {ok, Sock} = fuse_utils:connect_as_provider(
        Worker1, ?CORRECT_PROVIDER_ID, ?CORRECT_NONCE
    ),
    ssl:setopts(Sock, [{active, once}, {packet, 4}]),

    Secret = fuse_utils:generate_rtransfer_conn_secret(Sock),
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

    {ok, Sock} = fuse_utils:connect_as_provider(
        Worker1, ?CORRECT_PROVIDER_ID, ?CORRECT_NONCE
    ),

    ssl:setopts(Sock, [{active, once}, {packet, 4}]),
    RespNodes = fuse_utils:get_rtransfer_nodes_ips(Sock),

    ?assertEqual(ExpectedNodes, RespNodes),
    ok = ssl:close(Sock).


% TODO fix
%%broken_connection_test(Config) ->
%%    % given
%%    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
%%    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
%%    RootGuid = get_guid(Worker1, SessionId, <<"/space_name1">>),
%%
%%    Mod = replica_synchronizer,
%%    Fun = cancel_transfers_of_session,
%%    test_utils:mock_new(Workers, Mod, [passthrough]),
%%    test_utils:mock_expect(Workers, Mod, Fun, fun(FileUuid, SessId) ->
%%        meck:passthrough([FileUuid, SessId])
%%    end),
%%
%%    % Create a couple of connections within the same session
%%    {ok, {Sock, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SessionId),
%%
%%    % when
%%    ok = ssl:send(Sock, fuse_utils:generate_create_file_message(RootGuid, <<"1">>, <<"f1">>)),
%%    ?assertMatch(#'ServerMessage'{message_body = {
%%        fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
%%    }, message_id = <<"1">>}, fuse_utils:receive_server_message()),
%%
%%    ct:pal("QWE2"),
%%    FileGuid = get_guid(Worker1, SessionId, <<"/space_name1/f1">>),
%%    FileUuid = rpc:call(Worker1, fslogic_uuid, guid_to_uuid, [FileGuid]),
%%
%%    ok = ssl:send(Sock, fuse_utils:generate_open_file_message(FileGuid, <<"2">>)),
%%    ?assertMatch(#'ServerMessage'{message_body = {
%%        fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
%%    }, message_id = <<"2">>}, fuse_utils:receive_server_message()),
%%
%%    ok = ssl:close(Sock),
%%
%%    % then
%%    timer:sleep(timer:seconds(60)),
%%
%%    CallsNum = lists:sum(meck_get_num_calls([Worker1], Mod, Fun, '_')),
%%    ?assertMatch(2, CallsNum),
%%
%%    lists:foreach(fun(Num) ->
%%        ?assertMatch(FileUuid,
%%            rpc:call(Worker1, meck, capture, [Num, Mod, Fun, '_', 1])
%%        ),
%%        ?assertMatch(SessionId,
%%            rpc:call(Worker1, meck, capture, [Num, Mod, Fun, '_', 2])
%%        )
%%    end, lists:seq(1, CallsNum)),
%%
%%    ok = test_utils:mock_unload(Workers, [Mod]).
%%
%%
%%client_send_test(Config) ->
%%    % given
%%    [Worker1 | _] = ?config(op_worker_nodes, Config),
%%    {ok, {Sock, SessionId}} = fuse_utils:connect_via_macaroon(Worker1),
%%    Code = ?OK,
%%    Description = <<"desc">>,
%%    ServerMsgInternal = #server_message{message_body = #status{
%%        code = Code,
%%        description = Description
%%    }},
%%    ServerMessageProtobuf = #'ServerMessage'{
%%        message_id = undefined,
%%        message_body = {status, #'Status'{
%%            code = Code,
%%            description = Description}
%%        }
%%    },
%%
%%    % when
%%    ?assertEqual(ok, rpc:call(Worker1, communicator, send_to_client,
%%        [ServerMsgInternal, SessionId])),
%%
%%    % then
%%    ?assertEqual(ServerMessageProtobuf, fuse_utils:receive_server_message()),
%%    ok = ssl:close(Sock).
%%
%%client_communicate_test(Config) ->
%%    % given
%%    [Worker1 | _] = ?config(op_worker_nodes, Config),
%%    Status = #status{code = ?OK, description = <<"desc">>},
%%    ServerMsgInternal = #server_message{message_body = Status},
%%
%%    % when
%%    {ok, {Sock, SessionId}} = spawn_ssl_echo_client(Worker1),
%%    CommunicateResult = rpc:call(Worker1, communicator, communicate,
%%        [ServerMsgInternal, SessionId, #{wait_for_ans => true}]),
%%
%%    % then
%%    ?assertMatch({ok, #client_message{message_body = Status}}, CommunicateResult),
%%    ok = ssl:close(Sock).
%%
%%client_communicate_async_test(Config) ->
%%    % given
%%    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
%%    Status = #status{
%%        code = ?OK,
%%        description = <<"desc">>
%%    },
%%    ServerMsgInternal = #server_message{message_body = Status},
%%    Self = self(),
%%    {ok, {Sock, SessionId}} = spawn_ssl_echo_client(Worker1),
%%
%%    % when
%%    {ok, MsgId} = rpc:call(Worker1, communicator, communicate,
%%        [ServerMsgInternal, SessionId, #{use_msg_id => {true, Self}}]),
%%
%%    % then
%%    ?assertReceivedMatch(#client_message{
%%        message_id = MsgId, message_body = Status
%%    }, ?TIMEOUT),
%%
%%    % given
%%    test_utils:mock_expect(Workers, router, route_message, fun
%%        (#client_message{message_id = Id = #message_id{issuer = Issuer,
%%            recipient = undefined}}) ->
%%            Issuer = oneprovider:get_id(),
%%            Self ! {router_message_called, Id},
%%            ok
%%    end),
%%
%%    % when
%%    {ok, MsgId2} = rpc:call(Worker1, communicator, communicate,
%%        [ServerMsgInternal, SessionId, #{use_msg_id => true}]),
%%
%%    % then
%%    ?assertReceivedMatch({router_message_called, MsgId2}, ?TIMEOUT),
%%    ok = ssl:close(Sock).
%%
%%socket_timeout_test(Config) ->
%%    [Worker1 | _] = ?config(op_worker_nodes, Config),
%%    SID = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
%%
%%    RootGuid = get_guid(Worker1, SID, <<"/space_name1">>),
%%    initializer:remove_pending_messages(),
%%    {ok, {Sock, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SID),
%%
%%    % send
%%    ok = ssl:send(Sock, fuse_utils:generate_create_file_message(RootGuid, <<"1">>, <<"f1">>)),
%%    % receive & validate
%%    ?assertMatch(#'ServerMessage'{message_body = {
%%        fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
%%    }, message_id = <<"1">>}, fuse_utils:receive_server_message()),
%%
%%    lists:foreach(fun(MainNum) ->
%%        timer:sleep(timer:seconds(20)),
%%        {ok, {Sock2, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SID),
%%
%%        lists:foreach(fun(Num) ->
%%            NumBin = integer_to_binary(Num),
%%            % send
%%            ok = ssl:send(Sock2, fuse_utils:generate_create_file_message(RootGuid, NumBin, NumBin))
%%        end, lists:seq(MainNum * 100 + 1, MainNum * 100 + 100)),
%%
%%        AnsNums = lists:foldl(fun(_Num, Acc) ->
%%            % receive & validate
%%            M = fuse_utils:receive_server_message(),
%%            ?assertMatch(#'ServerMessage'{message_body = {
%%                fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
%%            }}, M),
%%            #'ServerMessage'{message_id = NumBin} = M,
%%            [NumBin | Acc]
%%        end, [], lists:seq(MainNum * 100 + 1, MainNum * 100 + 100)),
%%
%%        lists:foreach(fun(Num) ->
%%            NumBin = integer_to_binary(Num),
%%            ?assert(lists:member(NumBin, AnsNums))
%%        end, lists:seq(MainNum * 100 + 1, MainNum * 100 + 100)),
%%
%%        lists:foreach(fun(Num) ->
%%            LSBin = list_to_binary(integer_to_list(Num) ++ "ls"),
%%            ok = ssl:send(Sock2, fuse_utils:generate_get_children_attrs_message(RootGuid, LSBin)),
%%            % receive & validate
%%            ?assertMatch(#'ServerMessage'{message_body = {
%%                fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
%%            }, message_id = LSBin}, fuse_utils:receive_server_message())
%%        end, lists:seq(MainNum * 100 + 1, MainNum * 100 + 100))
%%    end, lists:seq(1, 10)).
%%
%%timeouts_test(Config) ->
%%    [Worker1 | _] = ?config(op_worker_nodes, Config),
%%    SID = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
%%
%%    RootGuid = get_guid(Worker1, SID, <<"/space_name1">>),
%%    initializer:remove_pending_messages(),
%%    {ok, {Sock, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SID),
%%
%%    create_timeouts_test(Config, Sock, RootGuid),
%%    ls_timeouts_test(Config, Sock, RootGuid),
%%    fsync_timeouts_test(Config, Sock, RootGuid).
%%
%%client_keepalive_test(Config) ->
%%    [Worker1 | _] = ?config(op_worker_nodes, Config),
%%    SID = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
%%
%%    RootGuid = get_guid(Worker1, SID, <<"/space_name1">>),
%%    initializer:remove_pending_messages(),
%%    {ok, {Sock, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SID),
%%    % send keepalive msg and assert it will not end in decoding error
%%    % on provider side (following communication should succeed)
%%    ok = ssl:send(Sock, ?CLIENT_KEEPALIVE_MSG),
%%    create_timeouts_test(Config, Sock, RootGuid).
%%
%%create_timeouts_test(Config, Sock, RootGuid) ->
%%    % send
%%    ok = ssl:send(Sock, fuse_utils:generate_create_file_message(RootGuid, <<"1">>, <<"f1">>)),
%%    % receive & validate
%%    ?assertMatch(#'ServerMessage'{message_body = {
%%        fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
%%    }, message_id = <<"1">>}, fuse_utils:receive_server_message()),
%%
%%    configure_cp(Config, helper_timeout),
%%    % send
%%    ok = ssl:send(Sock, fuse_utils:generate_create_file_message(RootGuid, <<"2">>, <<"f2">>)),
%%    % receive & validate
%%    check_answer(fun() -> ?assertMatchTwo(
%%        #'ServerMessage'{message_body = {
%%            fuse_response, #'FuseResponse'{status = #'Status'{code = eagain}
%%            }
%%        }, message_id = <<"2">>},
%%        #'ServerMessage'{message_body = {
%%            processing_status, #'ProcessingStatus'{code = 'IN_PROGRESS'}
%%        }, message_id = <<"2">>},
%%        fuse_utils:receive_server_message()) end
%%    ),
%%
%%    configure_cp(Config, helper_delay),
%%    ok = ssl:send(Sock, fuse_utils:generate_create_file_message(RootGuid, <<"3">>, <<"f3">>)),
%%    % receive & validate
%%    check_answer(fun() -> ?assertMatchTwo(
%%        #'ServerMessage'{message_body = {
%%            fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
%%        }, message_id = <<"3">>},
%%        #'ServerMessage'{message_body = {
%%            processing_status, #'ProcessingStatus'{code = 'IN_PROGRESS'}
%%        }, message_id = <<"3">>},
%%        fuse_utils:receive_server_message()) end,
%%        3
%%    ).
%%
%%ls_timeouts_test(Config, Sock, RootGuid) ->
%%    % send
%%    configure_cp(Config, ok),
%%    ok = ssl:send(Sock, fuse_utils:generate_get_children_attrs_message(RootGuid, <<"ls1">>)),
%%    % receive & validate
%%    ?assertMatch(#'ServerMessage'{message_body = {
%%        fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
%%    }, message_id = <<"ls1">>}, fuse_utils:receive_server_message()),
%%
%%    configure_cp(Config, attr_delay),
%%    ok = ssl:send(Sock, fuse_utils:generate_get_children_attrs_message(RootGuid, <<"ls2">>)),
%%    % receive & validate
%%    check_answer(fun() -> ?assertMatchTwo(
%%        #'ServerMessage'{message_body = {
%%            fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
%%        }, message_id = <<"ls2">>},
%%        #'ServerMessage'{message_body = {
%%            processing_status, #'ProcessingStatus'{code = 'IN_PROGRESS'}
%%        }, message_id = <<"ls2">>},
%%        fuse_utils:receive_server_message()) end,
%%        2
%%    ).
%%
%%fsync_timeouts_test(Config, Sock, RootGuid) ->
%%    configure_cp(Config, ok),
%%    ok = ssl:send(Sock, fuse_utils:generate_fsync_message(RootGuid, <<"fs1">>)),
%%    % receive & validate
%%    ?assertMatch(#'ServerMessage'{message_body = {
%%        fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
%%    }, message_id = <<"fs1">>}, fuse_utils:receive_server_message()),
%%
%%    configure_cp(Config, events_delay),
%%    ok = ssl:send(Sock, fuse_utils:generate_fsync_message(RootGuid, <<"fs2">>)),
%%    % receive & validate
%%    check_answer(fun() -> ?assertMatchTwo(
%%        #'ServerMessage'{message_body = {
%%            fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
%%        }, message_id = <<"fs2">>},
%%        #'ServerMessage'{message_body = {
%%            processing_status, #'ProcessingStatus'{code = 'IN_PROGRESS'}
%%        }, message_id = <<"fs2">>},
%%        fuse_utils:receive_server_message()) end,
%%        2
%%    ).

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
    test_utils:mock_new(Workers, provider_logic, [passthrough]),

    test_utils:mock_expect(Workers, provider_logic, verify_provider_identity, fun(ProviderId) ->
        case ProviderId of
            ?CORRECT_PROVIDER_ID -> ok;
            ?INCORRECT_PROVIDER_ID -> ?ERROR_UNAUTHORIZED
        end
    end),

    test_utils:mock_expect(Workers, provider_logic, verify_provider_nonce, fun(_ProviderId, Nonce) ->
        case Nonce of
            ?CORRECT_NONCE -> ok;
            ?INCORRECT_NONCE -> ?ERROR_UNAUTHORIZED
        end
    end),

    test_utils:mock_expect(Workers, provider_logic, assert_zone_compatibility, fun() ->
        ok
    end),

    init_per_testcase(default, Config);

init_per_testcase(Case, Config) when
    Case =:= bandwidth_test;
    Case =:= bandwidth_test2;
%%    Case =:= client_communicate_async_test;
    Case =:= python_client_test
->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, router),
    init_per_testcase(default, Config);

%%init_per_testcase(timeouts_test, Config) ->
%%    Workers = ?config(op_worker_nodes, Config),
%%    initializer:remove_pending_messages(),
%%    ssl:start(),
%%
%%    test_utils:mock_new(Workers, user_identity),
%%    test_utils:mock_expect(Workers, user_identity, get_or_fetch,
%%        fun(#macaroon_auth{macaroon = ?MACAROON, disch_macaroons = ?DISCH_MACAROONS}) ->
%%            {ok, #document{value = #user_identity{user_id = <<"user1">>}}}
%%        end
%%    ),
%%
%%    CP_Pid = spawn_control_proc(),
%%
%%    test_utils:mock_new(Workers, helpers),
%%    test_utils:mock_expect(Workers, helpers, apply_helper_nif, fun
%%        (#helper_handle{handle = Handle, timeout = Timeout}, Function, Args) ->
%%            aplly_helper(Handle, Timeout, Function, Args, CP_Pid);
%%        (#file_handle{handle = Handle, timeout = Timeout}, Function, Args) ->
%%            aplly_helper(Handle, Timeout, Function, Args, CP_Pid)
%%    end),
%%
%%    test_utils:mock_new(Workers, attr_req),
%%    test_utils:mock_expect(Workers, attr_req, get_file_attr_insecure, fun
%%        (UserCtx, FileCtx) ->
%%            get_file_attr_insecure(UserCtx, FileCtx, CP_Pid)
%%    end),
%%
%%    test_utils:mock_new(Workers, fslogic_event_handler),
%%    test_utils:mock_expect(Workers, fslogic_event_handler, handle_file_written_events, fun
%%        (Evts, UserCtxMap) ->
%%            handle_file_written_events(CP_Pid),
%%            meck:passthrough([Evts, UserCtxMap])
%%    end),
%%
%%    [{control_proc, CP_Pid} |
%%        initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config)];
%%
%%init_per_testcase(broken_connection_test, Config) ->
%%    % Shorten ttl to force quicker client session removal
%%    init_per_testcase(timeouts_test, [{fuse_session_ttl_seconds, 10} | Config]);
%%
%%init_per_testcase(client_keepalive_test, Config) ->
%%    init_per_testcase(timeouts_test, Config);
%%
%%init_per_testcase(socket_timeout_test, Config) ->
%%    Workers = ?config(op_worker_nodes, Config),
%%    lists:foreach(fun(Worker) ->
%%        test_utils:set_env(Worker, ?APP_NAME,
%%            proto_connection_timeout, timer:seconds(10))
%%    end, Workers),
%%    init_per_testcase(timeouts_test, Config);

init_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ssl:start(),
    initializer:remove_pending_messages(),
    mock_identity(Workers),

    initializer:mock_provider_id(
        Workers, <<"providerId">>, <<"auth-macaroon">>, <<"identity-macaroon">>
    ),
    Config.

end_per_testcase(Case, Config) when
    Case =:= provider_connection_test;
    Case =:= rtransfer_connection_secret_test;
    Case =:= rtransfer_nodes_ips_test
->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, [provider_logic]),
    end_per_testcase(default, Config);

end_per_testcase(python_client_test, Config) ->
    file:delete(?TEST_FILE(Config, "handshake.arg")),
    file:delete(?TEST_FILE(Config, "message.arg")),

    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, [router]),
    end_per_testcase(default, Config);

end_per_testcase(Case, Config) when
    Case =:= bandwidth_test;
    Case =:= bandwidth_test2;
    Case =:= client_communicate_async_test
->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, [router]),
    end_per_testcase(default, Config);

%%end_per_testcase(timeouts_test, Config) ->
%%    Workers = ?config(op_worker_nodes, Config),
%%    CP_Pid = ?config(control_proc, Config),
%%    ssl:stop(),
%%    stop_control_proc(CP_Pid),
%%    test_utils:mock_validate_and_unload(Workers, [user_identity, helpers,
%%        attr_req, fslogic_event_handler]),
%%    initializer:clean_test_users_and_spaces_no_validate(Config);
%%
%%end_per_testcase(broken_connection_test, Config) ->
%%    end_per_testcase(timeouts_test, Config);
%%
%%end_per_testcase(client_keepalive_test, Config) ->
%%    end_per_testcase(timeouts_test, Config);
%%
%%end_per_testcase(socket_timeout_test, Config) ->
%%    Workers = ?config(op_worker_nodes, Config),
%%    lists:foreach(fun(Worker) ->
%%        test_utils:set_env(Worker, ?APP_NAME,
%%            proto_connection_timeout, timer:minutes(10))
%%    end, Workers),
%%    end_per_testcase(timeouts_test, Config);

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:unmock_provider_ids(Workers),
    test_utils:mock_validate_and_unload(Workers, [user_identity]),
    ssl:stop().

%%%===================================================================
%%% Internal functions
%%%===================================================================


handshake_as_provider(Node, ProviderId, Nonce) ->
    case fuse_utils:connect_as_provider(Node, ProviderId, Nonce) of
        {ok, Sock} ->
            ssl:close(Sock),
            'OK';
        {error, HandshakeResp} ->
            HandshakeResp
    end.


handshake_as_client(Node, Macaroon, Version) ->
    SessId = crypto:strong_rand_bytes(10),
    case fuse_utils:connect_as_client(Node, SessId, Macaroon, Version) of
        {ok, Sock} ->
            ssl:close(Sock),
            'OK';
        {error, HandshakeResp} ->
            HandshakeResp
    end.

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node, and send back each message received from the server
%% @end
%%--------------------------------------------------------------------
-spec spawn_ssl_echo_client(NodeToConnect :: node()) ->
    {ok, {Sock :: ssl:socket(), SessId :: session:id()}}.
spawn_ssl_echo_client(NodeToConnect) ->
    {ok, {Sock, SessionId}} = fuse_utils:connect_via_macaroon(NodeToConnect, []),
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
        fun(#macaroon_auth{macaroon = ?MACAROON, disch_macaroons = ?DISCH_MACAROONS}) ->
            {ok, #document{value = #user_identity{}}}
        end
    ).

%%%===================================================================
%%% Test helper functions
%%%===================================================================

check_answer(AsertFun) ->
    check_answer(AsertFun, 0).

check_answer(AsertFun, MinHeartbeatNum) ->
    timer:sleep(100), % wait for pending messages
    initializer:remove_pending_messages(),
    check_answer(AsertFun, 0, MinHeartbeatNum).

check_answer(AsertFun, HeartbeatNum, MinHeartbeatNum) ->
    AnsNum = AsertFun(),
    case AnsNum of
        1 ->
            ?assert(HeartbeatNum >= MinHeartbeatNum);
        _ ->
            check_answer(AsertFun, HeartbeatNum + 1, MinHeartbeatNum)
    end.

get_guid(Worker, SessId, Path) ->
    #fuse_response{fuse_response = #guid{guid = Guid}} =
        ?assertMatch(
            #fuse_response{status = #status{code = ?OK}},
            ?req(Worker, SessId, #resolve_guid{path = Path}),
            30
        ),
    Guid.

spawn_control_proc() ->
    spawn(fun() ->
        control_proc(ok)
          end).

stop_control_proc(Pid) ->
    Pid ! stop.

control_proc(Value) ->
    receive
        {get_value, Pid} ->
            Pid ! {value, Value},
            control_proc(Value);
        {set_value, NewValue, Pid} ->
            Pid ! value_changed,
            control_proc(NewValue);
        stop ->
            ok
    end.

configure_cp(CP_Pid, Value) when is_pid(CP_Pid) ->
    CP_Pid ! {set_value, Value, self()},
    receive
        value_changed -> ok
    end;
configure_cp(Config, Value) ->
    CP_Pid = ?config(control_proc, Config),
    configure_cp(CP_Pid, Value).

get_cp_settings(CP_Pid) ->
    CP_Pid ! {get_value, self()},
    receive
        {value, Value} -> Value
    end.

aplly_helper(Handle, Timeout, Function, Args, CP_Pid) ->
    case get_cp_settings(CP_Pid) of
        helper_timeout ->
            helpers:receive_loop(make_ref(), Timeout);
        helper_delay ->
            configure_cp(CP_Pid, ok),
            Master = self(),
            spawn(fun() ->
                {ok, ResponseRef} = apply(helpers_nif, Function, [Handle | Args]),
                Master ! {ref, ResponseRef},
                heartbeat(10, Master, ResponseRef),
                Ans = helpers:receive_loop(ResponseRef, Timeout),
                Master ! {ResponseRef, Ans}
                  end),
            receive
                {ref, ResponseRef} ->
                    helpers:receive_loop(ResponseRef, Timeout)
            end;
        _ ->
            {ok, ResponseRef} = apply(helpers_nif, Function, [Handle | Args]),
            helpers:receive_loop(ResponseRef, Timeout)
    end.

get_file_attr_insecure(UserCtx, FileCtx, CP_Pid) ->
    case get_cp_settings(CP_Pid) of
        attr_delay ->
            timer:sleep(timer:seconds(70)),
            attr_req:get_file_attr_insecure(UserCtx, FileCtx, false);
        _ ->
            attr_req:get_file_attr_insecure(UserCtx, FileCtx, false)
    end.

handle_file_written_events(CP_Pid) ->
    case get_cp_settings(CP_Pid) of
        events_delay ->
            timer:sleep(timer:seconds(70));
        _ ->
            ok
    end.

heartbeat(0, _Master, _ResponseRef) ->
    ok;
heartbeat(N, Master, ResponseRef) ->
    timer:sleep(timer:seconds(10)),
    Master ! {ResponseRef, heartbeat},
    heartbeat(N - 1, Master, ResponseRef).

meck_get_num_calls(Nodes, Module, Fun, Args) ->
    lists:map(fun(Node) ->
        rpc:call(Node, meck, num_calls, [Module, Fun, Args], timer:seconds(60))
    end, Nodes).
