%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles communication using clproto binary protocol.
%%% Created connection can be one of the two possible types:
%%% - incoming - when it is initiated in response to peer (provider or client)
%%%              request. It awaits client_messages, handles them and
%%%              responds with server_messages,
%%% - outgoing - when this provider initiates it in order to connect to peer.
%%%              In order to do so, thirst http protocol upgrade request on
%%%              ?CLIENT_PROTOCOL_PATH is send to other provider.
%%%              If confirmation response is received, meaning that protocol
%%%              upgrade from http to clproto succeeded, then communication
%%%              using binary protocol can start.
%%%              This type of connection can send only client_messages and
%%%              receive server_messages.
%%%
%%% Beside type, connection has also one of the following statuses:
%%% - upgrading_protocol - valid only for outgoing connection. This status
%%%                        indicates that protocol upgrade request has been
%%%                        sent and response is awaited,
%%% - performing_handshake - indicates that connection either awaits
%%%                          authentication request (incoming connection)
%%%                          or response (outgoing connection),
%%% - ready - indicates that connection is in operational mode,
%%%           so that it can send and receive messages.
%%%
%%% More detailed transitions between statuses for each connection and
%%% message flow is depicted on below diagram.
%%%
%%%              INCOMING              |              OUTGOING
%%%                                    |
%%%                                    |             Provider B
%%%                                    |                 |
%%%                                    |        0: connect_to_provider
%%%                                    |                 |
%%%                                    |                 v
%%%           Provider A          1: protocol      +------------+
%%%          HTTP listener  <------ upgrade ------ |    init    |
%%%               |                 request        +------------+
%%%               |                    |                 |
%%%              1.5                   |                1.5
%%%               |                    |                 |
%%%               v                    |                 v
%%%          +----------+        2: protocol       +--------------------+
%%%          |   init   | -------- upgrade ------> | upgrading_protocol |
%%%          +----------+          response        +--------------------+
%%%               |                    |                 |      |
%%%              2.5                   |                 |      |
%%%               |                    |                 |      |
%%%               v                    |                 |      |
%%%    +----------------------+       3: handshake       |      |
%%%    | performing_handshake | <------ request ---------+      |
%%%    +----------------------+        |                       3.5
%%%           |        |               |                        |
%%%           |        |               |                        v
%%%           |        |       4: handshake        +----------------------+
%%%           |        +--------- response ------> | performing_handshake |
%%%           |                        |           +----------------------+
%%%           |                        |                      |
%%%          4.5                       |                     4.5
%%%           |                        |                      |
%%%           v                        |                      v
%%%       +-------+ <-------- n: client_message --------- +-------+
%%%       | ready |                    |                  | ready |
%%%       +-------+ -------- n+1: server_message -------> +-------+
%%%                                    |
%%%
%%% In case of errors during init, upgrading_protocol or performing_handshake
%%% connection is immediately terminated.
%%% On the other hand, When connection is in ready status, every kind of error
%%% is logged, but only socket errors terminates it.
%%% @end
%%%-------------------------------------------------------------------
-module(connection).
-author("Bartosz Walkowicz").

-behaviour(gen_server).
-behaviour(cowboy_sub_protocol).

-include("timeouts.hrl").
-include("http/gui_paths.hrl").
-include("global_definitions.hrl").
-include("proto/common/handshake_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

-record(state, {
    socket :: ssl:socket(),
    socket_mode = active_once :: active_always | active_once,
    transport :: module(),
    ip = undefined :: undefined | binary(),

    % transport messages
    ok :: atom(),
    closed :: atom(),
    error :: atom(),

    % connection state
    type :: incoming | outgoing,
    status :: upgrading_protocol | performing_handshake | ready,
    session_id = undefined :: undefined | session:id(),
    peer_id = undefined :: undefined | od_provider:id() | od_user:id(),
    verify_msg = true :: boolean()
}).

-type state() :: #state{}.
-type client_message() :: #client_message{}.
-type server_message() :: #server_message{}.
-type message() :: client_message() | server_message().

-define(PACKET_VALUE, 4).
-define(DEFAULT_SOCKET_MODE,
    application:get_env(?APP_NAME, default_socket_mode, active_once)
).
-define(DEFAULT_VERIFY_MSG_FLAG,
    application:get_env(?APP_NAME, verify_msg_before_encoding, true)
).

%% API
-export([
    connect_to_provider/7,
    send_sync/2, send_async/2
]).

%% Private API
-export([
    init/7
]).

%% cowboy_sub_protocol callbacks
-export([init/2, upgrade/4, upgrade/5, takeover/7]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Starts an connection to peer provider.
%% @end
%%--------------------------------------------------------------------
-spec connect_to_provider(od_provider:id(), session:id(), Domain :: binary(),
    IP :: binary(), Port :: non_neg_integer(), Transport :: atom(),
    Timeout :: non_neg_integer()) -> {ok, Pid :: pid()}.
connect_to_provider(ProviderId, SessionId, Domain, IP, Port, Transport, Timeout) ->
    proc_lib:start(?MODULE, init, [
        ProviderId, SessionId, Domain, IP, Port, Transport, Timeout
    ]).


%%-------------------------------------------------------------------
%% @doc
%% Tries to send a message and provides feedback about success or
%% eventual errors while serializing/sending.
%% @end
%%-------------------------------------------------------------------
-spec send_sync(pid(), message()) -> ok | {error, term()}.
send_sync(Pid, Msg) ->
    try
        gen_server2:call(Pid, {send, Msg})
    catch
        exit:{noproc, _} ->
            ?debug("Connection process ~p does not exist", [Pid]),
            {error, no_connection};
        exit:{normal, _} ->
            ?debug("Exit of connection process ~p for message ~p", [
                Pid, Msg
            ]),
            {error, no_connection};
        exit:{timeout, _} ->
            ?debug("Timeout of connection process ~p for message ~p", [
                Pid, Msg
            ]),
            {error, timeout};
        Type:Reason ->
            ?error("Connection ~p cannot send msg ~p due to: ~p", [
                Pid, Msg, {Type, Reason}
            ]),
            {error, Reason}
    end.


%%-------------------------------------------------------------------
%% @doc
%% Schedules message to be sent.
%% @end
%%-------------------------------------------------------------------
-spec send_async(pid(), message()) -> ok.
send_async(Pid, Msg) ->
    gen_server2:cast(Pid, {send, Msg}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is never called. We only define it so that
%% we can use the -behaviour(gen_server) attribute.
%% @end
%%--------------------------------------------------------------------
-spec init([]) -> {ok, undefined}.
init([]) -> {ok, undefined}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}.
handle_call({send, Msg}, _From, #state{status = ready} = State) ->
    case send_message(State, Msg) of
        {ok, NewState} ->
            {reply, ok, NewState, ?PROTO_CONNECTION_TIMEOUT};
        {error, serialization_failed} = SerializationError ->
            {reply, SerializationError, State, ?PROTO_CONNECTION_TIMEOUT};
        {error, sending_msg_via_wrong_connection} = WrongConnError ->
            {reply, WrongConnError, State, ?PROTO_CONNECTION_TIMEOUT};
        Error ->
            {stop, Error, Error, State}
    end;
handle_call({send, _Msg}, _From, #state{status = Status, socket = Socket} = State) ->
    ?warning("Attempt to send msg via not ready connection ~p", [Socket]),
    {reply, {error, Status}, State, ?PROTO_CONNECTION_TIMEOUT};
handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, {error, wrong_request}, State, ?PROTO_CONNECTION_TIMEOUT}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_cast({send, Msg}, #state{status = ready} = State) ->
    case send_message(State, Msg) of
        {ok, NewState} ->
            {noreply, NewState, ?PROTO_CONNECTION_TIMEOUT};
        {error, serialization_failed} ->
            {noreply, State, ?PROTO_CONNECTION_TIMEOUT};
        {error, sending_msg_via_wrong_connection} ->
            {noreply, State, ?PROTO_CONNECTION_TIMEOUT};
        Error ->
            {stop, Error, State}
    end;
handle_cast({send, _Msg}, #state{socket = Socket} = State) ->
    ?warning("Attempt to send msg via not ready connection ~p", [Socket]),
    {noreply, State, ?PROTO_CONNECTION_TIMEOUT};
handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State, ?PROTO_CONNECTION_TIMEOUT}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | {Ok :: atom(), Socket :: ssl:socket(),
    Data :: binary()} | term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_info(upgrade_protocol, #state{ip = Hostname} = State) ->
    case socket_send(State, protocol_utils:protocol_upgrade_request(Hostname)) of
        {ok, NewState} ->
            {noreply, NewState, ?PROTO_CONNECTION_TIMEOUT};
        {error, Reason} ->
            {stop, Reason, State}
    end;

handle_info({Ok, Socket, Data}, #state{status = upgrading_protocol, socket = Socket, ok = Ok} = State) ->
    case handle_protocol_upgrade_response(State, Data) of
        {ok, State1} ->
            State2 = State1#state{status = performing_handshake},
            activate_socket(State2, false),
            {noreply, State2, ?PROTO_CONNECTION_TIMEOUT};
        {error, Reason} ->
            {stop, Reason, State}
    end;

handle_info({Ok, Socket, Data}, #state{status = performing_handshake, socket = Socket, ok = Ok} = State) ->
    case handle_handshake(State, Data) of
        {ok, NewState} ->
            activate_socket(NewState, false),
            {noreply, NewState#state{status = ready}, ?PROTO_CONNECTION_TIMEOUT};
        {error, Reason} ->
            {stop, Reason, State}
    end;

handle_info({Ok, Socket, Data}, #state{status = ready, socket = Socket, ok = Ok} = State) ->
    case handle_message(State, Data) of
        {ok, NewState} ->
            activate_socket(NewState, false),
            {noreply, NewState, ?PROTO_CONNECTION_TIMEOUT};
        {error, Reason} ->
            {stop, Reason, State}
    end;

handle_info({Closed, _}, State = #state{closed = Closed}) ->
    {stop, normal, State};

handle_info({Error, Socket, Reason}, State = #state{error = Error}) ->
    ?warning("Connection ~p error: ~p", [Socket, Reason]),
    {stop, Reason, State};

handle_info(timeout, State = #state{socket = Socket}) ->
    ?warning("Connection ~p timeout", [Socket]),
    {stop, normal, State};

handle_info(disconnect, State) ->
    {stop, normal, State};

handle_info(Info, State) ->
    ?log_bad_request(Info),
    {noreply, State, ?PROTO_CONNECTION_TIMEOUT}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: state()) -> term().
terminate(Reason, #state{session_id = SessId, socket = Socket} = State) ->
    ?log_terminate(Reason, State),
    case SessId of
        undefined -> ok;
        _ -> session_connections:remove_connection(SessId, self())
    end,
    ssl:close(Socket),
    State.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: state(),
    Extra :: term()) -> {ok, NewState :: state()} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% cowboy_sub_protocol callbacks
%% ====================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Called by cowboy on receiving request on ?CLIENT_PROTOCOL_PATH path.
%% Causes the upgrade callback to be called.
%% @end
%%--------------------------------------------------------------------
-spec init(cowboy_req:req(), any()) -> {?MODULE, cowboy_req:req(), any()}.
init(Req, Opts) ->
    {?MODULE, Req, Opts}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initiates protocol switch from http to ?CLIENT_PROTOCOL_UPGRADE_NAME
%% if received request is proper upgrade request.
%% Otherwise responds with either 426 or 400.
%% @end
%%--------------------------------------------------------------------
-spec upgrade(cowboy_req:req(), cowboy_middleware:env(), module(), any()) ->
    {ok, cowboy_req:req(), cowboy_middleware:env()} | {stop, cowboy_req:req()}.
upgrade(Req, Env, Handler, HandlerState) ->
    upgrade(Req, Env, Handler, HandlerState, #{}).

-spec upgrade(cowboy_req:req(), cowboy_middleware:env(), module(), any(), any()) ->
    {ok, cowboy_req:req(), cowboy_middleware:env()} | {stop, cowboy_req:req()}.
upgrade(Req, Env, _Handler, HandlerOpts, _Opts) ->
    try protocol_utils:process_protocol_upgrade_request(Req) of
        ok ->
            Headers = cowboy_req:response_headers(#{
                <<"connection">> => <<"Upgrade">>,
                <<"upgrade">> => <<?CLIENT_PROTOCOL_UPGRADE_NAME>>
            }, Req),
            #{pid := Pid, streamid := StreamID} = Req,
            Pid ! {{Pid, StreamID}, {switch_protocol, Headers, ?MODULE, HandlerOpts}},
            {ok, Req, Env};
        {error, upgrade_required} ->
            NewReq = cowboy_req:reply(426, #{
                <<"connection">> => <<"Upgrade">>,
                <<"upgrade">> => <<?CLIENT_PROTOCOL_UPGRADE_NAME>>
            }, Req),
            {stop, NewReq}
    catch Type:Reason ->
        ?debug_stacktrace("Invalid protocol upgrade request - ~p:~p", [
            Type, Reason
        ]),
        cowboy_req:reply(400, Req),
        {stop, Req}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Called after successful upgrade.
%% Takes over connection process and changes it to gen_server.
%% @end
%%--------------------------------------------------------------------
-spec takeover(pid(), ranch:ref(), inet:socket(), module(), any(), binary(),
    any()) -> no_return().
takeover(_Parent, Ref, Socket, Transport, _Opts, _Buffer, _HandlerState) ->
    % Remove connection from the overall connections count so it will not be
    % included in limiting the number of e.g. REST connections.
    ranch:remove_connection(Ref),

    {Ok, Closed, Error} = Transport:messages(),

    State = #state{
        socket = Socket,
        socket_mode = ?DEFAULT_SOCKET_MODE,
        transport = Transport,
        ok = Ok,
        closed = Closed,
        error = Error,
        type = incoming,
        status = performing_handshake,
        verify_msg = ?DEFAULT_VERIFY_MSG_FLAG
    },

    ok = Transport:setopts(Socket, [binary, {packet, ?PACKET_VALUE}]),
    activate_socket(State, true),

    gen_server2:enter_loop(?MODULE, [], State, ?PROTO_CONNECTION_TIMEOUT).


%% ======================================================================
%% Functions used when initialising outgoing connection to peer provider
%% ======================================================================


%%--------------------------------------------------------------------
%% @doc
%% Initializes an outgoing connection to peer provider.
%% @end
%%--------------------------------------------------------------------
-spec init(od_provider:id(), session:id(), Domain :: binary(), Host :: binary(),
    Port :: non_neg_integer(), Transport :: atom(), Timeout :: non_neg_integer()) ->
    no_return().
init(ProviderId, SessionId, Domain, Host, Port, Transport, Timeout) ->
    % Map keeping reconnect interval and time between provider connection
    % retries; needed to implement backoff algorithm
    Intervals = application:get_env(?APP_NAME, providers_reconnect_intervals, #{}),
    ProviderId = session_utils:session_id_to_provider_id(SessionId),
    NextReconnect = protocol_utils:get_next_reconnect(ProviderId, Intervals),
    case time_utils:cluster_time_seconds() >= NextReconnect of
        false ->
            ?debug("Discarding connection request to provider(~p) as the "
                   "grace period has not passed yet.", [ProviderId]),
            exit(normal);
        true ->
            try
                State = connect_to_provider_internal(
                    SessionId, ProviderId, Domain, Host, Port,
                    Transport, Timeout
                ),
                ok = proc_lib:init_ack({ok, self()}),
                protocol_utils:reset_reconnect_interval(ProviderId, Intervals),
                gen_server2:enter_loop(?MODULE, [], State, ?PROTO_CONNECTION_TIMEOUT)
            catch
                throw:incompatible_peer_op_version ->
                    protocol_utils:postpone_next_reconnect(ProviderId, Intervals),
                    exit(normal);
                throw:cannot_check_peer_op_version ->
                    protocol_utils:postpone_next_reconnect(ProviderId, Intervals),
                    exit(normal);
                throw:cannot_verify_identity ->
                    protocol_utils:postpone_next_reconnect(ProviderId, Intervals),
                    exit(normal);
                exit:normal ->
                    ?info("Connection to peer provider(~p) closed", [ProviderId]),
                    exit(normal);
                Type:Reason ->
                    ?warning("Failed to connect to peer provider(~p) - ~p:~p. "
                             "Next retry not sooner than ~p.", [
                        ProviderId, Type, Reason, NextReconnect
                    ]),
                    protocol_utils:postpone_next_reconnect(ProviderId, Intervals),
                    exit(normal)
            end
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Attempts to connect to peer provider.
%% @end
%%--------------------------------------------------------------------
-spec connect_to_provider_internal(session:id(), od_provider:id(),
    Domain :: binary(), Host :: binary(),
    Port :: non_neg_integer(), Transport :: atom(),
    Timeout :: non_neg_integer()) -> state() | no_return().
connect_to_provider_internal(SessionId, ProviderId, Domain, Host, Port, Transport, Timeout) ->
    case provider_logic:verify_provider_identity(ProviderId) of
        ok ->
            ok;
        Err ->
            ?warning("Cannot verify identity of provider ~p, skipping connection - ~p", [
                ProviderId, Err
            ]),
            throw(cannot_verify_identity)
    end,

    SslOpts = [
        {cacerts, oneprovider:trusted_ca_certs()},
        {secure, application:get_env(?APP_NAME, interprovider_connections_security, true)},
        {hostname, Domain}
    ],
    provider_logic:assert_provider_compatibility(Host, ProviderId, SslOpts),

    DomainAndIpInfo = case Domain of
        Host -> str_utils:format("@ ~s:~b", [Host, Port]);
        _ -> str_utils:format("(~s) @ ~s:~b", [Domain, Host, Port])
    end,
    ?info("Connecting to provider '~s' ~s", [ProviderId, DomainAndIpInfo]),

    ConnectOpts = secure_ssl_opts:expand(Host, SslOpts),
    {ok, Socket} = Transport:connect(binary_to_list(Host), Port, ConnectOpts, Timeout),
    self() ! upgrade_protocol,

    session_manager:reuse_or_create_provider_session(
        SessionId, provider_outgoing, #user_identity{
            provider_id = session_utils:session_id_to_provider_id(SessionId)
        }, self()
    ),

    {Ok, Closed, Error} = Transport:messages(),
    State = #state{
        socket = Socket,
        socket_mode = ?DEFAULT_SOCKET_MODE,
        transport = Transport,
        ip = Host,
        ok = Ok,
        closed = Closed,
        error = Error,
        type = outgoing,
        status = upgrading_protocol,
        session_id = SessionId,
        peer_id = ProviderId,
        verify_msg = ?DEFAULT_VERIFY_MSG_FLAG
    },
    activate_socket(State, true),

    State.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Verifies protocol upgrade response and sends handshake request.
%% @end
%%--------------------------------------------------------------------
-spec handle_protocol_upgrade_response(state(), Data :: binary()) ->
    {ok, state()} | {error, Reason :: term()}.
handle_protocol_upgrade_response(State, Data) ->
    case protocol_utils:verify_protocol_upgrade_response(Data) of
        false ->
            ?error("Received invalid protocol upgrade response: ~p", [Data]),
            {error, invalid_protocol_upgrade_response};
        true ->
            {ok, MsgId} = message_id:generate(self()),
            {ok, Nonce} = authorization_nonce:create(),
            ClientMsg = #client_message{
                message_id = MsgId,
                message_body = #provider_handshake_request{
                    provider_id = oneprovider:get_id(),
                    nonce = Nonce
                }
            },
            #state{socket = Socket, transport = Transport} = State,
            ok = Transport:setopts(Socket, [binary, {packet, ?PACKET_VALUE}]),
            send_client_message(State, ClientMsg)
    end.


%% @private
-spec handle_handshake(state(), binary()) ->
    {ok, state()} | {error, Reason :: term()}.
handle_handshake(#state{type = incoming} = State, Data) ->
    handle_handshake_request(State, Data);
handle_handshake(#state{type = outgoing} = State, Data) ->
    handle_handshake_response(State, Data).


%% @private
-spec handle_handshake_request(state(), binary()) ->
    {ok, state()} | {error, Reason :: term()}.
handle_handshake_request(#state{socket = Socket} = State, Data) ->
    try
        {ok, {IpAddress, _Port}} = ssl:peername(Socket),
        {ok, Msg} = serializer:deserialize_client_message(Data, undefined),

        {PeerId, SessionId} = protocol_auth:handle_handshake(
            Msg#client_message.message_body, IpAddress
        ),
        NewState = State#state{peer_id = PeerId, session_id = SessionId},

        send_server_message(NewState, #server_message{
            message_body = #handshake_response{status = 'OK'}
        })
    catch Type:Reason ->
        ?debug("Invalid handshake request - ~p:~p", [Type, Reason]),
        send_server_message(State, protocol_auth:get_handshake_error(Reason)),
        {error, Reason}
    end.


%% @private
-spec handle_handshake_response(state(), binary()) ->
    {ok, state()} | {error, Reason :: term()}.
handle_handshake_response(#state{
    session_id = SessId,
    peer_id = ProviderId
} = State, Data) ->
    try serializer:deserialize_server_message(Data, SessId) of
        {ok, #server_message{message_body = #handshake_response{status = 'OK'}}} ->
            ?info("Successfully connected to provider '~s'", [ProviderId]),
            {ok, State};
        {ok, #server_message{message_body = #handshake_response{status = Error}}} ->
            ?error("Handshake refused by provider '~s' due to ~p, closing connection.", [
                ProviderId, Error
            ]),
            {error, {handshake_failed, Error}};
        _ ->
            ?error("Received invalid handshake response from provider '~s', "
                   "closing connection.", [
                ProviderId
            ]),
            {error, invalid_handshake_response}
    catch
        _:Error ->
            ?error_stacktrace("Client message decoding error: ~p", [Error]),
            {error, Error}
    end.


%% @private
-spec handle_message(state(), binary()) ->
    {ok, state()} | {error, Reason :: term()}.
handle_message(#state{type = incoming} = State, Data) ->
    handle_client_message(State, Data);
handle_message(#state{type = outgoing} = State, Data) ->
    handle_server_message(State, Data).


%% @private
-spec handle_client_message(state(), binary()) ->
    {ok, state()} | {error, Reason :: term()}.
handle_client_message(State, ?CLIENT_KEEPALIVE_MSG) ->
    {ok, State};
handle_client_message(#state{
    peer_id = PeerId,
    session_id = SessId
} = State, Data) ->
    try
        {ok, Msg} = serializer:deserialize_client_message(Data, SessId),
        protocol_utils:maybe_create_proxy_session(PeerId, Msg),
        route_message(State, Msg)
    catch Type:Reason ->
        ?error_stacktrace("Client message handling error - ~p:~p", [
            Type, Reason
        ]),
        {ok, State}
    end.


%% @private
-spec handle_server_message(state(), binary()) ->
    {ok, state()} | {error, Reason :: term()}.
handle_server_message(#state{session_id = SessId} = State, Data) ->
    try
        {ok, Msg} = serializer:deserialize_server_message(Data, SessId),
        route_message(State, Msg)
    catch Type:Error ->
        ?error_stacktrace("Server message handling error - ~p:~p", [
            Type, Error
        ]),
        {ok, State}
    end.


%% @private
-spec route_message(state(), message()) ->
    {ok, state()} | {error, Reason :: term()}.
route_message(State, Msg) ->
    case router:route_message(Msg) of
        ok ->
            {ok, State};
        {ok, ServerMsg} ->
            case send_server_message(State, ServerMsg) of
                % Serialization errors should not break ready connection
                {error, serialization_failed} ->
                    {ok, State};
                Result ->
                    Result
            end;
        {error, Reason} ->
            ?error_stacktrace("Message ~p handling error: ~p", [Msg, Reason]),
            {ok, State}
    end.


%% @private
-spec send_message(state(), message()) ->
    {ok, state()} | {error, Reason :: term()}.
send_message(#state{type = outgoing} = State, #client_message{} = Msg) ->
    send_client_message(State, Msg);
send_message(#state{type = incoming} = State, #server_message{} = Msg) ->
    send_server_message(State, Msg);
send_message(#state{type = ConnType}, Msg) ->
    ?warning_stacktrace("Attempt to send msg ~p via wrong connection ~p", [
        Msg, ConnType
    ]),
    {error, sending_msg_via_wrong_connection}.


%% @private
-spec send_client_message(state(), client_message()) ->
    {ok, state()} | {error, Reason :: term()}.
send_client_message(#state{verify_msg = VerifyMsg} = State, ClientMsg) ->
    try
        {ok, Data} = serializer:serialize_client_message(ClientMsg, VerifyMsg),
        socket_send(State, Data)
    catch
        _:Reason ->
            ?error_stacktrace("Unable to serialize client_message ~p due to: ~p", [
                ClientMsg, Reason
            ]),
            {error, serialization_failed}
    end.


%% @private
-spec send_server_message(state(), server_message()) ->
    {ok, state()} | {error, Reason :: term()}.
send_server_message(#state{verify_msg = VerifyMsg} = State, ServerMsg) ->
    try
        {ok, Data} = serializer:serialize_server_message(ServerMsg, VerifyMsg),
        socket_send(State, Data)
    catch
        _:Reason ->
            ?error_stacktrace("Unable to serialize server_message ~p due to: ~p", [
                ServerMsg, Reason
            ]),
            {error, serialization_failed}
    end.


%% @private
-spec socket_send(state(), Data :: binary()) ->
    {ok, state()} | {error, term()}.
socket_send(#state{transport = Transport, socket = Socket} = State, Data) ->
    case Transport:send(Socket, Data) of
        ok ->
            {ok, State};
        {error, Reason} = Error ->
            ?error_stacktrace("Unable to send message via socket ~p due to: ~p", [
                Socket, Reason
            ]),
            Error
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% If socket_mode is set to active_always then set it as such only during
%% first activation and do nothing for latter ones. It will cause every message
%% received on socket to be send to connection process as erlang message.
%% Otherwise (active_once) activates socket so that only next received packet
%% will be send to process as erlang message.
%% @end
%%--------------------------------------------------------------------
-spec activate_socket(state(), IsFirstActivation :: boolean()) -> ok.
activate_socket(#state{
    socket_mode = active_always,
    transport = Transport,
    socket = Socket
}, true) ->
    ok = Transport:setopts(Socket, [{active, true}]);
activate_socket(#state{socket_mode = active_always}, false) ->
    ok;
activate_socket(#state{transport = Transport, socket = Socket}, _) ->
    ok = Transport:setopts(Socket, [{active, once}]).
