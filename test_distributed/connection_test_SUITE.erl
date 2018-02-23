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
-export([all/0, init_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%%tests
-export([
    provider_connection_test/1,
    rtransfer_connection_secret_test/1,
    macaroon_connection_test/1,
    compatible_client_connection_test/1,
    incompatible_client_connection_test/1,
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
    proto_version_test/1,
    timeouts_test/1
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
    timeouts_test,
    provider_connection_test,
    rtransfer_connection_secret_test,
    macaroon_connection_test,
    compatible_client_connection_test,
    incompatible_client_connection_test,
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
-define(DISCH_MACAROONS, [<<"DM1">>, <<"DM2">>]).
-define(CORRECT_PROVIDER_ID, <<"correct-iden-mac">>).
-define(INCORRECT_PROVIDER_ID, <<"incorrect-iden-mac">>).
-define(CORRECT_NONCE, <<"correct-nonce">>).
-define(INCORRECT_NONCE, <<"incorrect-nonce">>).
-define(TIMEOUT, timer:minutes(1)).

-define(assertMatchTwo(Guard1, Guard2, Expr),
    begin
        ((fun () ->
            case (Expr) of
                Guard1 -> 1;
                Guard2 -> 2;
                __V ->
                    erlang:error({assertMatch,
                    [{module, ?MODULE},
                        {line, ?LINE},
                        {expression, (??Expr)},
                        {pattern, (??Guard1), (??Guard2)},
                        {value, __V}]})
            end
        end)())
    end).

-define(req(W, SessId, FuseRequest), element(2, rpc:call(W, worker_proxy, call,
    [fslogic_worker, {fuse_request, SessId, #fuse_request{fuse_request = FuseRequest}}]))).

-record(file_handle, {
    handle :: helpers_nif:file_handle(),
    timeout :: timeout()
}).

%%%===================================================================
%%% Test functions
%%%===================================================================

% to samo dla #fsync{} | #get_file_children_attrs{}
timeouts_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    SID = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),

    RootGuid = get_guid(Worker1, SID, <<"/space_name1">>),
    initializer:remove_pending_messages(),
    {ok, {Sock, _}} = connect_via_macaroon(Worker1, [{active, true}], SID),

    create_timeouts_test(Config, Sock, RootGuid),
    ls_timeouts_test(Config, Sock, RootGuid),
    fsync_timeouts_test(Config, Sock, RootGuid).

create_timeouts_test(Config, Sock, RootGuid) ->
    % send
    ok = ssl:send(Sock, generate_create_message(RootGuid, <<"1">>, <<"f1">>)),
    % receive & validate
    ?assertMatch(#'ServerMessage'{message_body = {
        fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
    }, message_id = <<"1">>}, receive_server_message()),

    configure_cp(Config, helper_timeout),
    % send
    ok = ssl:send(Sock, generate_create_message(RootGuid, <<"2">>, <<"f2">>)),
    % receive & validate
    check_answer(fun() -> ?assertMatchTwo(
        #'ServerMessage'{message_body = {
            fuse_response, #'FuseResponse'{status = #'Status'{code = eagain}
            }
        }, message_id = <<"2">>},
        #'ServerMessage'{message_body = {
            processing_status, #'ProcessingStatus'{code = 'IN_PROGRESS'}
        }, message_id = <<"2">>},
        receive_server_message()) end
        ),

    configure_cp(Config, helper_delay),
    ok = ssl:send(Sock, generate_create_message(RootGuid, <<"3">>, <<"f3">>)),
    % receive & validate
    check_answer(fun() -> ?assertMatchTwo(
        #'ServerMessage'{message_body = {
            fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
        }, message_id = <<"3">>},
        #'ServerMessage'{message_body = {
            processing_status, #'ProcessingStatus'{code = 'IN_PROGRESS'}
        }, message_id = <<"3">>},
        receive_server_message()) end,
        3
    ).

ls_timeouts_test(Config, Sock, RootGuid) ->
    % send
    configure_cp(Config, ok),
    ok = ssl:send(Sock, generate_get_children_message(RootGuid, <<"ls1">>)),
    % receive & validate
    ?assertMatch(#'ServerMessage'{message_body = {
        fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
    }, message_id = <<"ls1">>}, receive_server_message()),

    configure_cp(Config, attr_delay),
    ok = ssl:send(Sock, generate_get_children_message(RootGuid, <<"ls2">>)),
    % receive & validate
    check_answer(fun() -> ?assertMatchTwo(
        #'ServerMessage'{message_body = {
            fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
        }, message_id = <<"ls2">>},
        #'ServerMessage'{message_body = {
            processing_status, #'ProcessingStatus'{code = 'IN_PROGRESS'}
        }, message_id = <<"ls2">>},
        receive_server_message()) end,
        2
    ).

fsync_timeouts_test(Config, Sock, RootGuid) ->
    configure_cp(Config, ok),
    ok = ssl:send(Sock, generate_fsync_message(RootGuid, <<"fs1">>)),
    % receive & validate
    ?assertMatch(#'ServerMessage'{message_body = {
        fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
    }, message_id = <<"fs1">>}, receive_server_message()),

    configure_cp(Config, events_delay),
    ok = ssl:send(Sock, generate_fsync_message(RootGuid, <<"fs2">>)),
    % receive & validate
    check_answer(fun() -> ?assertMatchTwo(
        #'ServerMessage'{message_body = {
            fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
        }, message_id = <<"fs2">>},
        #'ServerMessage'{message_body = {
            processing_status, #'ProcessingStatus'{code = 'IN_PROGRESS'}
        }, message_id = <<"fs2">>},
        receive_server_message()) end,
        2
    ).

provider_connection_test(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    % then
    ?assertMatch(
        {ok, #'HandshakeResponse'{status = 'INVALID_PROVIDER'}},
        handshake_as_provider(Worker1, ?INCORRECT_PROVIDER_ID, ?CORRECT_NONCE)
    ),
    ?assertMatch(
        {ok, #'HandshakeResponse'{status = 'INVALID_PROVIDER'}},
        handshake_as_provider(Worker1, ?INCORRECT_PROVIDER_ID, ?INCORRECT_NONCE)
    ),
    ?assertMatch(
        {ok, #'HandshakeResponse'{status = 'INVALID_NONCE'}},
        handshake_as_provider(Worker1, ?CORRECT_PROVIDER_ID, ?INCORRECT_NONCE)
    ),
    ?assertMatch(
        {ok, #'HandshakeResponse'{status = 'OK'}},
        handshake_as_provider(Worker1, ?CORRECT_PROVIDER_ID, ?CORRECT_NONCE)
    ).

rtransfer_connection_secret_test(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    {ok, #'HandshakeResponse'{status = 'OK'}, Sock} = connect_as_provider(
        Worker1, ?CORRECT_PROVIDER_ID, ?CORRECT_NONCE
    ),

    {ok, MsgId} = message_id:generate(self(), <<"provId">>),
    {ok, EncodedId} = message_id:encode(MsgId),
    ClientMsg = #'ClientMessage'{
        message_id = EncodedId,
        message_body = {generate_rtransfer_conn_secret, #'GenerateRTransferConnSecret'{}}
    },
    RawMsg = messages:encode_msg(ClientMsg),
    ssl:send(Sock, RawMsg),
    ssl:setopts(Sock, [{active, once}, {packet, 4}]),

    #'ServerMessage'{message_body = Msg} = ?assertMatch(#'ServerMessage'{}, receive_server_message()),

    {rtransfer_conn_secret, #'RTransferConnSecret'{secret = Secret}} = ?assertMatch(
        {rtransfer_conn_secret, #'RTransferConnSecret'{}},
        Msg
    ),
    ?assert(is_binary(Secret)),
    ok = ssl:close(Sock).

macaroon_connection_test(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    % then
    {ok, {Sock, _}} = connect_via_macaroon(Worker1),
    ok = ssl:close(Sock).

compatible_client_connection_test(Config) ->
    % given
    [Node | _] = ?config(op_worker_nodes, Config),
    {ok, [Version | _]} = rpc:call(
        Node, application, get_env, [?APP_NAME, compatible_oc_versions]
    ),

    MacaroonAuthMessage = #'ClientMessage'{message_body = {client_handshake_request,
        #'ClientHandshakeRequest'{session_id = crypto:strong_rand_bytes(10),
            macaroon = #'Macaroon'{macaroon = ?MACAROON, disch_macaroons = ?DISCH_MACAROONS},
            version = list_to_binary(Version)
        }
    }},
    MacaroonAuthMessageRaw = messages:encode_msg(MacaroonAuthMessage),
    {ok, Port} = test_utils:get_env(Node, ?APP_NAME, gui_https_port),

    % when
    {ok, Sock} = connect_and_upgrade_proto(utils:get_host(Node), Port),
    ok = ssl:send(Sock, MacaroonAuthMessageRaw),

    % then
    #'ServerMessage'{message_body = {handshake_response, #'HandshakeResponse'{
        status = Status
    }}} = ?assertMatch(#'ServerMessage'{message_body = {handshake_response, _}},
        receive_server_message()
    ),
    ?assertMatch('OK', Status),
    ok.

incompatible_client_connection_test(Config) ->
    % given
    [Node | _] = ?config(op_worker_nodes, Config),

    MacaroonAuthMessage = #'ClientMessage'{message_body = {client_handshake_request,
        #'ClientHandshakeRequest'{session_id = crypto:strong_rand_bytes(10),
            macaroon = #'Macaroon'{macaroon = ?MACAROON, disch_macaroons = ?DISCH_MACAROONS},
            version = <<"16.07-rc2">>
        }
    }},
    MacaroonAuthMessageRaw = messages:encode_msg(MacaroonAuthMessage),
    {ok, Port} = test_utils:get_env(Node, ?APP_NAME, gui_https_port),

    % when
    {ok, Sock} = connect_and_upgrade_proto(utils:get_host(Node), Port),
    ok = ssl:send(Sock, MacaroonAuthMessageRaw),

    % then
    #'ServerMessage'{message_body = {handshake_response, #'HandshakeResponse'{
        status = Status
    }}} = ?assertMatch(#'ServerMessage'{message_body = {handshake_response, _}},
        receive_server_message()
    ),
    ?assertMatch('INCOMPATIBLE_VERSION', Status),
    ok.

protobuf_msg_test(Config) ->
    % given
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_expect(Workers, router, preroute_message, fun
        (#client_message{message_body = #events{events = [#event{
            type = #file_read_event{}
        }]}}, _, _, _) -> ok
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
    {ok, {Sock, _}} = connect_via_macaroon(Worker1),
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
        }]}}, _, _, _) ->
            Self ! Counter,
            ok
    end),

    % when
    {ok, {Sock, _}} = connect_via_macaroon(Worker1, [{active, true}]),
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
    {ok, {Sock, SessionId}} = connect_via_macaroon(Worker1),
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
            recipient = undefined}}, _, _, _) ->
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
            {ok, {Sock, _}} = connect_via_macaroon(Worker1, [{active, true}]),
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
    {ok, {Sock, _}} = connect_via_macaroon(Worker1),
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
        connect_via_macaroon(Worker1, [])
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
        (#client_message{message_body = #ping{}}, _, _, _) ->
            Self ! router_message_called,
            ok
    end),

    % when
    {ok, {Sock, _}} = connect_via_macaroon(Worker1, [{active, true}]),
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
        (#client_message{message_body = #ping{}}, _, _, _) ->
            Self ! router_message_called,
            ok;
        (_, _, _, _) ->
            ok
    end),

    ClientPath = ?TEST_FILE(Config, "ssl_client.py"),
    MessagePath = ?TEST_FILE(Config, "message.arg"),
    HandshakeMsgPath = ?TEST_FILE(Config, "handshake.arg"),
    file:write_file(MessagePath, PacketRaw),
    file:write_file(HandshakeMsgPath, HandshakeMessageRaw),
    Host = utils:get_host(Worker1),
    {ok, Port} = test_utils:get_env(Worker1, ?APP_NAME, gui_https_port),

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
    {ok, {Sock, _}} = connect_via_macaroon(Worker1),

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
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

init_per_testcase(Case, Config) when
    Case =:= provider_connection_test;
    Case =:= rtransfer_connection_secret_test ->
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
    Case =:= protobuf_msg_test;
    Case =:= multi_message_test;
    Case =:= client_communicate_async_test;
    Case =:= bandwidth_test;
    Case =:= python_client_test ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, router),
    init_per_testcase(default, Config);

init_per_testcase(timeouts_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:remove_pending_messages(),
    ssl:start(),

    test_utils:mock_new(Workers, user_identity),
    test_utils:mock_expect(Workers, user_identity, get_or_fetch,
        fun(#macaroon_auth{macaroon = ?MACAROON, disch_macaroons = ?DISCH_MACAROONS}) ->
            {ok, #document{value = #user_identity{user_id = <<"user1">>}}}
        end
    ),

    CP_Pid = spawn_control_proc(),

    test_utils:mock_new(Workers, helpers),
    test_utils:mock_expect(Workers, helpers, apply_helper_nif, fun
        (#helper_handle{handle = Handle, timeout = Timeout}, Function, Args) ->
            aplly_helper(Handle, Timeout, Function, Args, CP_Pid);
        (#file_handle{handle = Handle, timeout = Timeout}, Function, Args) ->
            aplly_helper(Handle, Timeout, Function, Args, CP_Pid)
    end),

    test_utils:mock_new(Workers, attr_req),
    test_utils:mock_expect(Workers, attr_req, get_file_attr_insecure, fun
        (UserCtx, FileCtx) ->
            get_file_attr_insecure(UserCtx, FileCtx, CP_Pid)
    end),

    test_utils:mock_new(Workers, fslogic_event_handler),
    test_utils:mock_expect(Workers, fslogic_event_handler, handle_file_written_events, fun
        (Evts, UserCtxMap) ->
            handle_file_written_events(CP_Pid),
            meck:passthrough([Evts, UserCtxMap])
    end),

    [{control_proc, CP_Pid} |
        initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config)];

init_per_testcase(default, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ssl:start(),
    initializer:remove_pending_messages(),
    mock_identity(Workers),

    initializer:mock_provider_id(
        Workers, <<"providerId">>, <<"auth-macaroon">>, <<"identity-macaroon">>
    ),
    Config;

init_per_testcase(_Case, Config) ->
    init_per_testcase(default, Config).


end_per_testcase(provider_connection_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, [provider_logic]),
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

end_per_testcase(timeouts_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    CP_Pid = ?config(control_proc, Config),
    ssl:stop(),
    stop_control_proc(CP_Pid),
    test_utils:mock_validate_and_unload(Workers, [user_identity, helpers,
        attr_req, fslogic_event_handler]),
    initializer:clean_test_users_and_spaces_no_validate(Config);

end_per_testcase(default, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:unmock_provider_ids(Workers),
    test_utils:mock_validate_and_unload(Workers, [user_identity]),
    ssl:stop();

end_per_testcase(_Case, Config) ->
    end_per_testcase(default, Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node using macaroon, with default socket_opts
%% @equiv connect_via_macaroon(Node, [{active, true}], ssl)
%% @end
%%--------------------------------------------------------------------
-spec connect_via_macaroon(Node :: node()) ->
    {ok, {Sock :: ssl:socket(), SessId :: session:id()}} | no_return().
connect_via_macaroon(Node) ->
    connect_via_macaroon(Node, [{active, true}]).

connect_via_macaroon(Node, SocketOpts) ->
    connect_via_macaroon(Node, SocketOpts, crypto:strong_rand_bytes(10)).

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node using a macaroon, with custom socket opts
%% @end
%%--------------------------------------------------------------------
-spec connect_via_macaroon(Node :: node(), SocketOpts :: list(), session:id()) ->
    {ok, {Sock :: term(), SessId :: session:id()}}.
connect_via_macaroon(Node, SocketOpts, SessId) ->
    % given
    {ok, [Version | _]} = rpc:call(
        Node, application, get_env, [?APP_NAME, compatible_oc_versions]
    ),

    MacaroonAuthMessage = #'ClientMessage'{message_body = {client_handshake_request,
        #'ClientHandshakeRequest'{
            session_id = SessId,
            macaroon = #'Macaroon'{macaroon = ?MACAROON, disch_macaroons = ?DISCH_MACAROONS},
            version = list_to_binary(Version)
        }
    }},
    MacaroonAuthMessageRaw = messages:encode_msg(MacaroonAuthMessage),
    ActiveOpt = case proplists:get_value(active, SocketOpts) of
        undefined -> [];
        Other -> [{active, Other}]
    end,
    {ok, Port} = test_utils:get_env(Node, ?APP_NAME, gui_https_port),

    % when
    {ok, Sock} = connect_and_upgrade_proto(utils:get_host(Node), Port),
    ok = ssl:send(Sock, MacaroonAuthMessageRaw),

    % then
    RM = receive_server_message(),
    #'ServerMessage'{message_body = {handshake_response, #'HandshakeResponse'{
        status = 'OK'
    }}} = ?assertMatch(#'ServerMessage'{message_body = {handshake_response, _}},
        RM
    ),
    ssl:setopts(Sock, ActiveOpt),
    {ok, {Sock, SessId}}.


connect_as_provider(Node, ProviderId, Nonce) ->
    MacaroonAuthMessage = #'ClientMessage'{message_body = {provider_handshake_request,
        #'ProviderHandshakeRequest'{provider_id = ProviderId, nonce = Nonce}
    }},
    MacaroonAuthMessageRaw = messages:encode_msg(MacaroonAuthMessage),
    {ok, Port} = test_utils:get_env(Node, ?APP_NAME, gui_https_port),

    % when
    {ok, Sock} = connect_and_upgrade_proto(utils:get_host(Node), Port),
    ok = ssl:send(Sock, MacaroonAuthMessageRaw),

    % then
    #'ServerMessage'{
        message_body = {handshake_response, HandshakeResp}
    } = receive_server_message(),
    {ok, HandshakeResp, Sock}.


handshake_as_provider(Node, ProviderId, Nonce) ->
    {ok, HandshakeResp, Sock} = connect_as_provider(Node, ProviderId, Nonce),
    ok = ssl:close(Sock),
    {ok, HandshakeResp}.


connect_and_upgrade_proto(Hostname, Port) ->
    {ok, Sock} = (catch ssl:connect(Hostname, Port, [binary,
        {active, once}, {reuse_sessions, false}
    ], timer:minutes(1))),
    ssl:send(Sock, connection:protocol_upgrade_request(list_to_binary(Hostname))),
    receive {ssl, Sock, Data} ->
        ?assert(connection:verify_protocol_upgrade_response(Data)),
        ssl:setopts(Sock, [{active, once}, {packet, 4}]),
        {ok, Sock}
    after timer:minutes(1) ->
        exit(timeout)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Connect to given node, and send back each message received from the server
%% @end
%%--------------------------------------------------------------------
-spec spawn_ssl_echo_client(NodeToConnect :: node()) ->
    {ok, {Sock :: ssl:socket(), SessId :: session:id()}}.
spawn_ssl_echo_client(NodeToConnect) ->
    {ok, {Sock, SessionId}} = connect_via_macaroon(NodeToConnect, []),
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

%%%===================================================================
%%% Timeouts test helper functions
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

generate_create_message(RootGuid, MsgId, File) ->
    Message = #'ClientMessage'{message_id = MsgId, message_body =
    {fuse_request, #'FuseRequest'{fuse_request = {file_request,
        #'FileRequest'{context_guid = RootGuid,
            file_request = {create_file, #'CreateFile'{name = File,
                mode = 8#644, flag = 'READ_WRITE'}}}
    }}}
    },
    messages:encode_msg(Message).

generate_get_children_message(RootGuid, MsgId) ->
    Message = #'ClientMessage'{message_id = MsgId, message_body =
    {fuse_request, #'FuseRequest'{fuse_request = {file_request,
        #'FileRequest'{context_guid = RootGuid,
            file_request = {get_file_children_attrs,
                #'GetFileChildrenAttrs'{offset = 0, size = 100}}}
    }}}
    },
    messages:encode_msg(Message).

generate_fsync_message(RootGuid, MsgId) ->
    Message = #'ClientMessage'{message_id = MsgId, message_body =
    {fuse_request, #'FuseRequest'{fuse_request = {file_request,
        #'FileRequest'{context_guid = RootGuid,
            file_request = {fsync,
                #'FSync'{data_only = false}}}
    }}}
    },
    messages:encode_msg(Message).