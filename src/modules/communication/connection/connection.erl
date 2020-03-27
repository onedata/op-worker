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
%%%              In order to do so, first http protocol upgrade request on
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
%%% More detailed transitions between statuses for each connection type and
%%% message flow is depicted on diagram below.
%%%
%%%              INCOMING              |              OUTGOING
%%%                                    |
%%%                                    |             Provider B
%%%                                    |                 |
%%%                                    |           0: start_link
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
%%% On the other hand, when connection is in ready status, every kind of error
%%% is logged, but only socket errors terminates it.
%%% @end
%%%-------------------------------------------------------------------
-module(connection).
-author("Bartosz Walkowicz").

-behaviour(gen_server).
-behaviour(cowboy_sub_protocol).

-include("timeouts.hrl").
-include("http/gui_paths.hrl").
-include("http/rest.hrl").
-include("global_definitions.hrl").
-include("proto/common/handshake_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").

-record(state, {
    socket :: ssl:sslsocket(),
    socket_mode = active_once :: active_always | active_once,
    transport :: module(),

    % transport messages
    ok :: atom(),
    closed :: atom(),
    error :: atom(),

    % connection state
    type :: incoming | outgoing,
    status :: upgrading_protocol | performing_handshake | ready,
    session_id = undefined :: undefined | session:id(),
    peer_id = undefined :: undefined | od_provider:id() | od_user:id(),
    peer_ip :: inet:ip4_address(),
    verify_msg = true :: boolean(),
    connection_manager = undefined :: undefined | pid(),

    % routing information base - structure necessary for routing.
    rib :: router:rib() | undefined
}).

-type state() :: #state{}.
-type error() :: {error, Reason :: term()}.

% Default value for {packet, N} socket option. When specified, erlang first
% reads N bytes to get length of your data, allocates a buffer to hold it
% and reads data into buffer after getting each tcp packet.
% Then it sends the buffer as one msg to your process.
-define(PACKET_VALUE, 4).

-define(DEFAULT_SOCKET_MODE,
    application:get_env(?APP_NAME, default_socket_mode, active_once)
).
-define(DEFAULT_VERIFY_MSG_FLAG,
    application:get_env(?APP_NAME, verify_msg_before_encoding, true)
).

%% API
-export([
    start_link/5, start_link/7,
    close/1,

    send_msg/2,
    send_keepalive/1
]).

%% Private API
-export([connect_with_provider/8]).

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
%% @equiv start_link(ProviderId, SessionId, Domain, Host, Port, ranch_ssl,
%% timer:seconds(5)).
%% @end
%%--------------------------------------------------------------------
-spec start_link(od_provider:id(), session:id(), Domain :: binary(),
    Host :: binary(), Port :: non_neg_integer()) -> {ok, pid()} | error().
start_link(ProviderId, SessionId, Domain, Host, Port) ->
    start_link(ProviderId, SessionId, Domain, Host, Port,
        ranch_ssl, timer:seconds(5)
    ).


%%--------------------------------------------------------------------
%% @doc
%% Starts an outgoing connection to peer provider.
%% @end
%%--------------------------------------------------------------------
-spec start_link(od_provider:id(), session:id(), Domain :: binary(),
    Host :: binary(), Port :: non_neg_integer(), Transport :: atom(),
    Timeout :: non_neg_integer()) -> {ok, pid()} | error().
start_link(ProviderId, SessionId, Domain, Host, Port, Transport, Timeout) ->
    ConnManager = self(),
    proc_lib:start_link(?MODULE, connect_with_provider, [
        ProviderId, SessionId, Domain, Host, Port,
        Transport, Timeout, ConnManager
    ]).


%%--------------------------------------------------------------------
%% @doc
%% Sends msg for specified connection to shutdown itself.
%% @end
%%--------------------------------------------------------------------
close(Pid) ->
    gen_server2:cast(Pid, disconnect).


%%-------------------------------------------------------------------
%% @doc
%% Tries to send a message and provides feedback about success or
%% eventual errors while serializing/sending.
%% @end
%%-------------------------------------------------------------------
-spec send_msg(pid(), communicator:message()) -> ok | error().
send_msg(Pid, Msg) ->
    try
        gen_server2:call(Pid, {send_msg, Msg}, ?DEFAULT_REQUEST_TIMEOUT)
    catch
        exit:{noproc, _} ->
            ?debug("Connection process ~p does not exist", [Pid]),
            {error, no_connection};
        exit:{normal, _} ->
            ?debug("Exit of connection process ~p for message ~s", [
                Pid, clproto_utils:msg_to_string(Msg)
            ]),
            {error, no_connection};
        exit:{timeout, _} ->
            ?debug("Timeout of connection process ~p for message ~s", [
                Pid, clproto_utils:msg_to_string(Msg)
            ]),
            ?ERROR_TIMEOUT;
        Type:Reason ->
            ?error("Connection ~p cannot send msg ~s due to ~p:~p", [
                Pid, clproto_utils:msg_to_string(Msg), Type, Reason
            ]),
            {error, Reason}
    end.


%%-------------------------------------------------------------------
%% @doc
%% Schedules keepalive message to be sent.
%% @end
%%-------------------------------------------------------------------
-spec send_keepalive(pid()) -> ok.
send_keepalive(Pid) ->
    gen_server2:cast(Pid, send_keepalive).


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
handle_call({send_msg, Msg}, _From, #state{status = ready} = State) ->
    case send_message(State, Msg) of
        {ok, NewState} ->
            {reply, ok, NewState, ?PROTO_CONNECTION_TIMEOUT};
        {error, serialization_failed} = SerializationError ->
            {reply, SerializationError, State, ?PROTO_CONNECTION_TIMEOUT};
        {error, sending_msg_via_wrong_conn_type} = WrongConnError ->
            {reply, WrongConnError, State, ?PROTO_CONNECTION_TIMEOUT};
        Error ->
            {stop, Error, Error, State}
    end;
handle_call({send_msg, _Msg}, _From, #state{status = Status, socket = Socket} = State) ->
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
handle_cast(send_keepalive, State) ->
    case socket_send(State, ?CLIENT_KEEPALIVE_MSG) of
        {ok, NewState} ->
            {noreply, NewState, ?PROTO_CONNECTION_TIMEOUT};
        Error ->
            {stop, Error, State}
    end;
handle_cast(disconnect, State) ->
    {stop, normal, State};
handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State, ?PROTO_CONNECTION_TIMEOUT}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | {Ok :: atom(), Socket :: ssl:sslsocket(),
    Data :: binary()} | term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_info({upgrade_protocol, Hostname}, State) ->
    UpgradeReq = connection_utils:protocol_upgrade_request(Hostname),
    case socket_send(State, UpgradeReq) of
        {ok, NewState} ->
            {noreply, NewState, ?PROTO_CONNECTION_TIMEOUT};
        {error, _Reason} ->
            {stop, normal, State}
    end;

handle_info({Ok, Socket, Data}, #state{status = upgrading_protocol, socket = Socket, ok = Ok} = State) ->
    try handle_protocol_upgrade_response(State, Data) of
        {ok, State1} ->
            State2 = State1#state{status = performing_handshake},
            activate_socket(State2, false),
            {noreply, State2, ?PROTO_CONNECTION_TIMEOUT};
        {error, _Reason} ->
            % Concrete errors were already logged in 'handle_protocol_upgrade_response'
            % so terminate gracefully as to not spam more error logs
            {stop, normal, State}
    catch Type:Reason ->
        ?error_stacktrace("Unexpected error during protocol upgrade: ~p:~p", [
            Type, Reason
        ]),
        {stop, normal, State}
    end;

handle_info({Ok, Socket, Data}, #state{status = performing_handshake, socket = Socket, ok = Ok} = State) ->
    try handle_handshake(State, Data) of
        {ok, #state{session_id = SessionId} = State1} ->
            ok = session_connections:register(SessionId, self()),
            State2 = State1#state{status = ready},
            activate_socket(State2, false),
            {noreply, State2, ?PROTO_CONNECTION_TIMEOUT};
        {error, _Reason} ->
            % Concrete errors were already logged in 'handle_handshake' so
            % terminate gracefully as to not spam more error logs
            {stop, normal, State}
    catch Type:Reason ->
        ?error_stacktrace("Unexpected error while performing handshake: ~p:~p", [
            Type, Reason
        ]),
        {stop, normal, State}
    end;

handle_info({Ok, Socket, Data}, #state{status = ready, socket = Socket, ok = Ok} = State) ->
    case handle_message(State, Data) of
        {ok, NewState} ->
            activate_socket(NewState, false),
            {noreply, NewState, ?PROTO_CONNECTION_TIMEOUT};
        {error, _Reason} ->
            % Concrete errors were already logged in 'handle_message' so
            % terminate gracefully as to not spam more error logs
            {stop, normal, State}
    end;

handle_info({Error, Socket, Reason}, State = #state{error = Error}) ->
    ?warning("Connection ~p error: ~p", [Socket, Reason]),
    {stop, Reason, State};

handle_info({Closed, _}, State = #state{closed = Closed}) ->
    {stop, normal, State};

handle_info(timeout, State = #state{socket = Socket}) ->
    ?warning("Connection ~p timeout", [Socket]),
    {stop, timeout, State};

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
terminate(Reason, #state{session_id = SessionId, socket = Socket} = State) ->
    case Reason of
        handshake_failed ->
            % Do not log terminate here as concrete errors were logged already
            ok;
        _ ->
            ?log_terminate(Reason, State)
    end,
    case SessionId of
        undefined -> ok;
        _ -> session_connections:deregister(SessionId, self())
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
    Extra :: term()) -> {ok, NewState :: state()} | error().
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
    try connection_utils:process_protocol_upgrade_request(Req) of
        ok ->
            Headers = cowboy_req:response_headers(#{
                ?HDR_CONNECTION => <<"Upgrade">>,
                ?HDR_UPGRADE => <<?CLIENT_PROTOCOL_UPGRADE_NAME>>
            }, Req),
            #{pid := Pid, streamid := StreamID} = Req,
            Pid ! {{Pid, StreamID}, {switch_protocol, Headers, ?MODULE, HandlerOpts}},
            {ok, Req, Env};
        {error, upgrade_required} ->
            NewReq = cowboy_req:reply(?HTTP_426_UPGRADE_REQUIRED, #{
                ?HDR_CONNECTION => <<"Upgrade">>,
                ?HDR_UPGRADE => <<?CLIENT_PROTOCOL_UPGRADE_NAME>>
            }, Req),
            {stop, NewReq}
    catch Type:Reason ->
        ?debug("Invalid protocol upgrade request - ~p:~p", [Type, Reason]),
        cowboy_req:reply(?HTTP_400_BAD_REQUEST, Req),
        {stop, Req}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Called after successful upgrade.
%% Takes over connection process and changes it to gen_server.
%% @end
%%--------------------------------------------------------------------
-spec takeover(pid(), ranch:ref(), ssl:sslsocket(), module(), any(), binary(),
    any()) -> no_return().
takeover(_Parent, Ref, Socket, Transport, _Opts, _Buffer, _HandlerState) ->
    % Remove connection from the overall connections count so it will not be
    % included in limiting the number of e.g. REST connections.
    ranch:remove_connection(Ref),

    {ok, {IpAddress, _Port}} = ssl:peername(Socket),
    {Ok, Closed, Error} = Transport:messages(),

    State = #state{
        socket = Socket,
        socket_mode = ?DEFAULT_SOCKET_MODE,
        transport = Transport,
        ok = Ok,
        closed = Closed,
        error = Error,
        peer_ip = IpAddress,
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
-spec connect_with_provider(od_provider:id(), session:id(), Domain :: binary(),
    Host :: binary(), Port :: non_neg_integer(), Transport :: atom(),
    Timeout :: non_neg_integer(), ConnManager :: pid()
) ->
    no_return().
connect_with_provider(ProviderId, SessionId, Domain,
    Host, Port, Transport, Timeout, ConnManager
) ->
    DomainAndIpInfo = case Domain of
        Host -> str_utils:format("@ ~s:~b", [Host, Port]);
        _ -> str_utils:format("@ ~s:~b (~s)", [Host, Port, Domain])
    end,
    ?info("Connecting to provider ~ts ~s", [
        provider_logic:to_printable(ProviderId), DomainAndIpInfo
    ]),

    try
        State = open_socket_to_provider(
            SessionId, ProviderId, Domain, Host, Port,
            Transport, Timeout, ConnManager
        ),
        activate_socket(State, true),

        self() ! {upgrade_protocol, Host},
        ok = proc_lib:init_ack({ok, self()}),

        process_flag(trap_exit, true),
        gen_server2:enter_loop(?MODULE, [], State, ?PROTO_CONNECTION_TIMEOUT)
    catch _:Reason ->
        exit(Reason)
    end.


%% @private
-spec open_socket_to_provider(session:id(), od_provider:id(),
    Domain :: binary(), Host :: binary(),
    Port :: non_neg_integer(), Transport :: atom(),
    Timeout :: non_neg_integer(), ConnManager :: pid()
) ->
    state() | no_return().
open_socket_to_provider(SessionId, ProviderId, Domain,
    Host, Port, Transport, Timeout, ConnManager
) ->
    SslOpts = provider_logic:provider_connection_ssl_opts(Domain),
    ConnectOpts = secure_ssl_opts:expand(Host, SslOpts),
    {ok, Socket} = Transport:connect(
        binary_to_list(Host), Port, ConnectOpts, Timeout
    ),
    {ok, {IpAddress, _Port}} = ssl:peername(Socket),

    {Ok, Closed, Error} = Transport:messages(),
    #state{
        socket = Socket,
        socket_mode = ?DEFAULT_SOCKET_MODE,
        transport = Transport,
        ok = Ok,
        closed = Closed,
        error = Error,
        type = outgoing,
        status = upgrading_protocol,
        session_id = SessionId,
        peer_id = ProviderId,
        peer_ip = IpAddress,
        verify_msg = ?DEFAULT_VERIFY_MSG_FLAG,
        connection_manager = ConnManager,
        rib = router:build_rib(SessionId)
    }.


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
    {ok, state()} | error().
handle_protocol_upgrade_response(State, Data) ->
    case connection_utils:verify_protocol_upgrade_response(Data) of
        false ->
            ?error("Received invalid protocol upgrade response: ~p", [Data]),
            {error, invalid_protocol_upgrade_response};
        true ->
            #state{
                socket = Socket,
                transport = Transport,
                peer_id = ProviderId
            } = State,
            {ok, MsgId} = clproto_message_id:generate(self()),
            {ok, Token} = provider_auth:get_identity_token_for_consumer(
                ?SUB(?ONEPROVIDER, ProviderId)
            ),
            ClientMsg = #client_message{
                message_id = MsgId,
                message_body = #provider_handshake_request{
                    provider_id = oneprovider:get_id(),
                    token = Token
                }
            },
            ok = Transport:setopts(Socket, [binary, {packet, ?PACKET_VALUE}]),
            send_client_message(State, ClientMsg)
    end.


%% @private
-spec handle_handshake(state(), binary()) -> {ok, state()} | error().
handle_handshake(#state{type = incoming} = State, Data) ->
    handle_handshake_request(State, Data);
handle_handshake(#state{type = outgoing} = State, Data) ->
    handle_handshake_response(State, Data).


%% @private
-spec handle_handshake_request(state(), binary()) -> {ok, state()} | error().
handle_handshake_request(#state{peer_ip = IpAddress} = State, Data) ->
    try
        {ok, Msg} = clproto_serializer:deserialize_client_message(Data, undefined),

        {PeerId, SessionId} = connection_auth:handle_handshake(
            Msg#client_message.message_body, IpAddress
        ),

        NewState = State#state{
            peer_id = PeerId,
            session_id = SessionId,
            rib = router:build_rib(SessionId)
        },
        send_server_message(NewState, #server_message{
            message_body = #handshake_response{status = 'OK'}
        })
    catch Type:Reason ->
        ?debug("Invalid handshake request - ~p:~p", [Type, Reason]),
        ErrorMsg = connection_auth:get_handshake_error_msg(Reason),
        send_server_message(State, ErrorMsg),
        {error, handshake_failed}
    end.


%% @private
-spec handle_handshake_response(state(), binary()) -> {ok, state()} | error().
handle_handshake_response(#state{
    session_id = SessionId,
    peer_id = ProviderId,
    connection_manager = ConnManager
} = State, Data) ->
    try clproto_serializer:deserialize_server_message(Data, SessionId) of
        {ok, #server_message{message_body = #handshake_response{status = 'OK'}}} ->
            ?info("Successfully connected to provider ~ts", [
                provider_logic:to_printable(ProviderId)
            ]),
            outgoing_connection_manager:report_successful_handshake(ConnManager),
            {ok, State};
        {ok, #server_message{message_body = #handshake_response{status = Error}}} ->
            ?error("Handshake refused by provider ~ts due to ~p, closing connection.", [
                provider_logic:to_printable(ProviderId), Error
            ]),
            {error, handshake_failed};
        _ ->
            ?error("Received invalid handshake response from provider ~ts, closing connection.", [
                provider_logic:to_printable(ProviderId)
            ]),
            {error, handshake_failed}
    catch _:Error ->
        ?error("Client handshake message decoding error: ~p", [Error]),
        {error, handshake_failed}
    end.


%% @private
-spec handle_message(state(), binary()) -> {ok, state()} | error().
handle_message(#state{type = incoming} = State, Data) ->
    handle_client_message(State, Data);
handle_message(#state{type = outgoing} = State, Data) ->
    handle_server_message(State, Data).


%% @private
-spec handle_client_message(state(), binary()) -> {ok, state()} | error().
handle_client_message(State, ?CLIENT_KEEPALIVE_MSG) ->
    {ok, State};
handle_client_message(#state{
    peer_id = PeerId,
    peer_ip = PeerIp,
    session_id = SessId
} = State, Data) ->
    try
        {ok, Msg} = clproto_serializer:deserialize_client_message(Data, SessId),
        case connection_utils:maybe_create_proxied_session(PeerId, PeerIp, Msg) of
            ok ->
                route_message(State, Msg);
            Error ->
                ?error("Failed to create proxied session for ~p due to: ~p", [
                    clproto_utils:msg_to_string(Msg), Error
                ]),
                % Respond with eacces error if request has msg_id
                % (msg_id means that peer awaits answer)
                case Msg#client_message.message_id of
                    undefined ->
                        {ok, State};
                    _ ->
                        AccessErrorMsg = #server_message{
                            message_id = Msg#client_message.message_id,
                            message_body = #status{code = ?EACCES}
                        },
                        send_response(State, AccessErrorMsg)
                end
        end
    catch
        throw:{translation_failed, Reason, undefined} ->
            ?error("Client message decoding error - ~p", [Reason]),
            {ok, State};
        throw:{translation_failed, Reason, MsgId} ->
            ?error("Client message decoding error - ~p", [Reason]),
            InvalidArgErrorMsg = #server_message{
                message_id = MsgId,
                message_body = #status{code = ?EINVAL}
            },
            send_response(State, InvalidArgErrorMsg);
        Type:Reason ->
            ?error("Client message handling error - ~p:~p", [Type, Reason]),
            {ok, State}
    end.


%% @private
-spec handle_server_message(state(), binary()) -> {ok, state()} | error().
handle_server_message(#state{session_id = SessId} = State, Data) ->
    try
        {ok, Msg} = clproto_serializer:deserialize_server_message(Data, SessId),
        route_message(State, Msg)
    catch Type:Error ->
        ?error("Server message handling error - ~p:~p", [Type, Error]),
        {ok, State}
    end.


%% @private
-spec route_message(state(), communicator:message()) ->
    {ok, state()} | error().
route_message(#state{rib = RIB} = State, Msg) ->
    case router:route_message(Msg, RIB) of
        ok ->
            {ok, State};
        {ok, ServerMsg} ->
            send_response(State, ServerMsg);
        {error, Reason} ->
            ?error("Message ~s handling error: ~p", [
                clproto_utils:msg_to_string(Msg), Reason
            ]),
            {ok, State}
    end.


%% @private
-spec send_response(state(), communicator:server_message()) ->
    {ok, state()} | error().
send_response(#state{session_id = SessionId} = State, ServerMsg) ->
    case send_server_message(State, ServerMsg) of
        {ok, _NewState} = Ans ->
            Ans;
        % Serialization errors should not break ready connection
        {error, serialization_failed} ->
            {ok, State};
        Error ->
            % Remove this connection from the connections pool and try to send
            % msg via other connections of this session.
            % Removal from pool is necessary to avoid deadlock when some other
            % connection terminates as well and tries to send msg via this one
            % while this one tries to send via the other one.
            session_connections:deregister(SessionId, self()),
            connection_api:send(SessionId, ServerMsg, [self()], true),
            Error
    end.


%% @private
-spec send_message(state(), communicator:message()) -> {ok, state()} | error().
send_message(#state{type = outgoing} = State, #client_message{} = Msg) ->
    send_client_message(State, Msg);
send_message(#state{type = incoming} = State, #server_message{} = Msg) ->
    send_server_message(State, Msg);
send_message(#state{type = ConnType}, Msg) ->
    ?warning_stacktrace("Attempt to send msg ~s via wrong connection ~p", [
        clproto_utils:msg_to_string(Msg), ConnType
    ]),
    {error, sending_msg_via_wrong_conn_type}.


%% @private
-spec send_client_message(state(), communicator:client_message()) ->
    {ok, state()} | error().
send_client_message(#state{verify_msg = VerifyMsg} = State, ClientMsg) ->
    try
        {ok, Data} = clproto_serializer:serialize_client_message(ClientMsg, VerifyMsg),
        socket_send(State, Data)
    catch _:Reason ->
        ?error_stacktrace("Unable to serialize client_message ~s due to: ~p", [
            clproto_utils:msg_to_string(ClientMsg), Reason
        ]),
        {error, serialization_failed}
    end.


%% @private
-spec send_server_message(state(), communicator:server_message()) ->
    {ok, state()} | error().
send_server_message(#state{verify_msg = VerifyMsg} = State, ServerMsg) ->
    try
        {ok, Data} = clproto_serializer:serialize_server_message(ServerMsg, VerifyMsg),
        socket_send(State, Data)
    catch _:Reason ->
        ?error_stacktrace("Unable to serialize server_message ~s due to: ~p", [
            clproto_utils:msg_to_string(ServerMsg), Reason
        ]),
        {error, serialization_failed}
    end.


%% @private
-spec socket_send(state(), Data :: binary()) -> {ok, state()} | error().
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
