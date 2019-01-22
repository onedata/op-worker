%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @TODO WRITEME
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

    last_message_timestamp = {0, 0, 0} :: erlang:timestamp()
}).

-type state() :: #state{}.
-type client_message() :: #client_message{}.
-type server_message() :: #server_message{}.
-type message() :: client_message() | server_message().

-define(PACKET_VALUE, 4).
-define(DEFAULT_SOCKET_MODE,
    application:get_env(?APP_NAME, default_socket_mode, active_once)
).

% Definitions of reconnect intervals for provider connection.
-define(INITIAL_RECONNECT_INTERVAL_SEC, 2).
-define(RECONNECT_INTERVAL_INCREASE_RATE, 2).
-define(MAX_RECONNECT_INTERVAL, timer:minutes(15)).

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
%% @TODO WRITEME
%% @end
%%-------------------------------------------------------------------
-spec send_sync(pid(), message()) -> ok | {error, term()}.
send_sync(Pid, Msg) ->
    gen_server2:call(Pid, {send, Msg}).


%%-------------------------------------------------------------------
%% @doc
%% @TODO WRITEME
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
handle_call({send, #server_message{} = Msg}, _From, #state{type = outgoing} = State) ->
    send_message_sync(State, to_client_message(Msg));
handle_call({send, #client_message{} = Msg}, _From, #state{type = outgoing} = State) ->
    send_message_sync(State, Msg);
handle_call({send, #server_message{} = Msg}, _From, #state{type = incoming} = State) ->
    send_message_sync(State, Msg);
handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, wrong_request, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tries to send message to peer if connection is in `ready` state and
%% provides feedback about eventual failure (e.g. serialization error or
%% not ready connection). In case of socket errors it also stops
%% connection process.
%% @end
%%--------------------------------------------------------------------
-spec send_message_sync(state(), message()) ->
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: state()}.
send_message_sync(#state{status = ready} = State0, Msg) ->
    case send_message(State0, Msg) of
        {ok, State1} ->
            State2 = State1#state{last_message_timestamp = os:timestamp()},
            {reply, ok, State2, ?PROTO_CONNECTION_TIMEOUT};
        {error, serialization_failed} = SerializationError ->
            {reply, SerializationError, State0, ?PROTO_CONNECTION_TIMEOUT};
        Error ->
            {stop, Error, Error, State0}
    end;
send_message_sync(#state{status = Status} = State, _Msg) ->
    {reply, {error, Status}, State, ?PROTO_CONNECTION_TIMEOUT}.


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
handle_cast({send, #server_message{} = Msg}, #state{type = outgoing} = State) ->
    send_message_async(State, to_client_message(Msg));
handle_cast({send, #client_message{} = Msg}, #state{type = outgoing} = State) ->
    send_message_async(State, Msg);
handle_cast({send, #server_message{} = Msg}, #state{type = incoming} = State) ->
    send_message_async(State, Msg);
handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State, ?PROTO_CONNECTION_TIMEOUT}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tries to send message to peer if connection is in `ready`.
%% Stops connection process in case of socket errors.
%% @end
%%--------------------------------------------------------------------
-spec send_message_async(state(), message()) ->
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
send_message_async(#state{status = ready} = State0, Msg) ->
    case send_message(State0, Msg) of
        {ok, State1} ->
            State2 = State1#state{last_message_timestamp = os:timestamp()},
            {noreply, State2, ?PROTO_CONNECTION_TIMEOUT};
        {error, serialization_failed} ->
            {noreply, State0, ?PROTO_CONNECTION_TIMEOUT};
        Error ->
            {stop, Error, State0}
    end;
send_message_async(State, _Msg) ->
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
handle_info(upgrade_protocol, #state{ip = Hostname} = State0) ->
    case socket_send(State0, protocol_utils:protocol_upgrade_request(Hostname)) of
        {ok, State1} ->
            State2 = State1#state{last_message_timestamp = os:timestamp()},
            {noreply, State2, ?PROTO_CONNECTION_TIMEOUT};
        {error, Reason} ->
            {stop, Reason, State0}
    end;

handle_info({Ok, Socket, Data}, State = #state{status = upgrading_protocol, ok = Ok}) ->
    case protocol_utils:verify_protocol_upgrade_response(Data) of
        false ->
            ?error("Received invalid protocol upgrade response: ~p", [Data]),
            {stop, normal, State};
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
            case send_client_message(State, ClientMsg) of
                {ok, State1} ->
                    State2 = State1#state{
                        status = performing_handshake,
                        last_message_timestamp = os:timestamp()
                    },
                    activate_socket_once(State2),
                    {noreply, State2, ?PROTO_CONNECTION_TIMEOUT};
                {error, Reason} ->
                    {stop, Reason, State}
            end
    end;

handle_info({Ok, Socket, Data}, #state{status = performing_handshake, socket = Socket, ok = Ok} = State0) ->
    case handle_handshake(State0, Data) of
        {ok, State1} ->
            State2 = State1#state{
                status = ready,
                last_message_timestamp = os:timestamp()
            },
            activate_socket_once(State2),
            {noreply, State2, ?PROTO_CONNECTION_TIMEOUT};
        {error, Reason} ->
            {stop, Reason, State0}
    end;

handle_info({Ok, Socket, Data}, #state{status = ready, socket = Socket, ok = Ok} = State0) ->
    case handle_message(State0, Data) of
        {ok, State1} ->
            State2 = State1#state{last_message_timestamp = os:timestamp()},
            activate_socket_once(State2),
            {noreply, State2, ?PROTO_CONNECTION_TIMEOUT};
        {error, Reason} ->
            {stop, Reason, State0}
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

handle_info(heartbeat, #state{last_message_timestamp = LMT} = State) ->
    Interval = async_request_manager:get_processes_check_interval(),
    erlang:send_after(Interval, self(), heartbeat),

    Diff = timer:now_diff(os:timestamp(), LMT),
    case Diff > ?PROTO_CONNECTION_TIMEOUT * 1000 of
        true ->
            ?info("Connection ~p timeout", [State#state.socket]),
            {stop, normal, State};
        _ ->
            {noreply, State, ?PROTO_CONNECTION_TIMEOUT}
    end;

handle_info(Info, State) ->
    ?log_bad_request(Info),
    {stop, normal, State}.


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
    ranch:remove_connection(Ref),
    {Ok, Closed, Error} = Transport:messages(),
    SocketMode = ?DEFAULT_SOCKET_MODE,

    State = #state{
        socket = Socket,
        socket_mode = SocketMode,
        transport = Transport,
        ok = Ok,
        closed = Closed,
        error = Error,
        type = incoming,
        status = performing_handshake,
        last_message_timestamp = os:timestamp()
    },
    ok = Transport:setopts(Socket, [binary, {packet, ?PACKET_VALUE}]),

    case SocketMode of
        active_always ->
            Transport:setopts(Socket, [{active, true}]);
        active_once ->
            activate_socket_once(State)
    end,

    Interval = async_request_manager:get_processes_check_interval(),
    erlang:send_after(Interval, self(), heartbeat),

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
    Intervals = application:get_env(
        ?APP_NAME, providers_reconnect_intervals, #{}
    ),
    ProviderId = session_utils:session_id_to_provider_id(SessionId),
    {NextReconnect, Interval} = maps:get(ProviderId, Intervals,
        {time_utils:cluster_time_seconds(), ?INITIAL_RECONNECT_INTERVAL_SEC}
    ),
    case time_utils:cluster_time_seconds() >= NextReconnect of
        false ->
            ?debug("Discarding connection request to provider(~p) as the "
                   "grace period has not passed yet.", [ProviderId]),
            exit(normal);
        true ->
            try
                State = init_connection_to_provider(
                    SessionId, ProviderId, Domain, Host, Port,
                    Transport, Timeout
                ),
                reset_reconnect_interval(Intervals, ProviderId),
                gen_server2:enter_loop(?MODULE, [], State, ?PROTO_CONNECTION_TIMEOUT)
            catch
                throw:incompatible_peer_op_version ->
                    postpone_next_reconnect(Intervals, ProviderId, Interval),
                    exit(normal);
                throw:cannot_check_peer_op_version ->
                    postpone_next_reconnect(Intervals, ProviderId, Interval),
                    exit(normal);
                throw:cannot_verify_identity ->
                    postpone_next_reconnect(Intervals, ProviderId, Interval),
                    exit(normal);
                exit:normal ->
                    ?info("Connection to peer provider(~p) closed", [
                        ProviderId
                    ]),
                    exit(normal);
                Type:Reason ->
                    ?warning_stacktrace("Failed to connect to peer provider(~p) - ~p:~p. "
                    "Next retry not sooner than ~p seconds.", [
                        ProviderId, Type, Reason, Interval
                    ]),
                    postpone_next_reconnect(Intervals, ProviderId, Interval),
                    exit(normal)
            end
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Attempts to connect to peer provider.
%% @end
%%--------------------------------------------------------------------
-spec init_connection_to_provider(session:id(), od_provider:id(),
    Domain :: binary(), Host :: binary(),
    Port :: non_neg_integer(), Transport :: atom(),
    Timeout :: non_neg_integer()) -> state() | no_return().
init_connection_to_provider(SessionId, ProviderId, Domain, Host, Port,
    Transport, Timeout
) ->
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

    {Ok, Closed, Error} = Transport:messages(),

    session_manager:reuse_or_create_provider_session(
        SessionId, provider_outgoing, #user_identity{
            provider_id = session_utils:session_id_to_provider_id(SessionId)
        }, self()
    ),

    ok = proc_lib:init_ack({ok, self()}),
    self() ! upgrade_protocol,

    SocketMode = ?DEFAULT_SOCKET_MODE,
    State = #state{
        socket = Socket,
        socket_mode = SocketMode,
        transport = Transport,
        ip = Host,
        ok = Ok,
        closed = Closed,
        error = Error,
        type = outgoing,
        status = upgrading_protocol,
        session_id = SessionId,
        peer_id = ProviderId
    },

    case SocketMode of
        active_always ->
            Transport:setopts(Socket, [{active, true}]);
        active_once ->
            activate_socket_once(State)
    end,

    State.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Postpones the time of next reconnect in an increasing manner,
%% according to RECONNECT_INTERVAL_INCREASE_RATE.
%% @end
%%--------------------------------------------------------------------
-spec postpone_next_reconnect(Intervals :: #{},
    ProviderId :: od_provider:id(), Interval :: integer()) -> ok.
postpone_next_reconnect(Intervals, ProviderId, Interval) ->
    NewInterval = min(
        Interval * ?RECONNECT_INTERVAL_INCREASE_RATE,
        ?MAX_RECONNECT_INTERVAL
    ),
    NewIntervals = Intervals#{
        ProviderId => {time_utils:cluster_time_seconds() + Interval, NewInterval}
    },
    application:set_env(?APP_NAME, providers_reconnect_intervals, NewIntervals).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resets the reconnect interval to its initial value and next reconnect to
%% current time (which means next reconnect can be performed immediately).
%% @end
%%--------------------------------------------------------------------
-spec reset_reconnect_interval(Intervals :: #{},
    ProviderId :: od_provider:id()) -> ok.
reset_reconnect_interval(Intervals, ProviderId) ->
    NewIntervals = Intervals#{
        ProviderId => {
            time_utils:cluster_time_seconds(),
            ?INITIAL_RECONNECT_INTERVAL_SEC
        }
    },
    application:set_env(?APP_NAME, providers_reconnect_intervals, NewIntervals).


%%%===================================================================
%%% Internal functions
%%%===================================================================


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
handle_handshake_request(#state{
    socket = Socket,
    session_id = SessId
} = State, Data) ->
    try
        {ok, #client_message{
            message_body = HandshakeMsg
        }} = serializator:deserialize_client_message(Data, SessId),

        {ok, {IpAddress, _Port}} = ssl:peername(Socket),
        {PeerId, SessionId} = auth_manager:handle_handshake(
            HandshakeMsg, IpAddress
        ),
        NewState = State#state{peer_id = PeerId, session_id = SessionId},
        send_server_message(NewState, #server_message{
            message_body = #handshake_response{status = 'OK'}
        })
    catch Type:Reason ->
        ?debug_stacktrace("Invalid handshake request - ~p:~p", [
            Type, Reason
        ]),
        send_server_message(State, auth_manager:get_handshake_error(Reason)),
        {error, Reason}
    end.


%% @private
-spec handle_handshake_response(state(), binary()) ->
    {ok, state()} | {error, Reason :: term()}.
handle_handshake_response(#state{
    session_id = SessId,
    peer_id = ProviderId
} = State, Data) ->
    try serializator:deserialize_server_message(Data, SessId) of
        {ok, #server_message{message_body = #handshake_response{status = 'OK'}}} ->
            ?info("Successfully connected to provider '~s'", [ProviderId]),
            {ok, State};
        {ok, #server_message{message_body = #handshake_response{status = Error}}} ->
            ?error("Handshake refused by provider '~s' due to ~p, closing connection.", [
                ProviderId, Error
            ]),
            {error, {handshake_failed, Error}};
        _ ->
            ?error("Received invalid handshake response from provider '~s', closing connection.", [
                ProviderId
            ]),
            {error, invalid_handshake_response}
    catch
        _:Error ->
            ?warning_stacktrace("Client message decoding error: ~p", [Error]),
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
handle_client_message(State = #state{session_id = SessId}, Data) ->
    try
        {ok, Msg} = serializator:deserialize_client_message(Data, SessId),
        maybe_create_proxy_session(State, Msg),
        route_message(State, Msg)
    catch Type:Reason ->
        ?warning_stacktrace("Client message decoding error - ~p:~p", [
            Type, Reason
        ]),
        {ok, State}
    end.


%% @private
-spec handle_server_message(state(), binary()) ->
    {ok, state()} | {error, Reason :: term()}.
handle_server_message(State = #state{session_id = SessId}, Data) ->
    try
        {ok, Msg0} = serializator:deserialize_server_message(Data, SessId),
        Msg = fill_server_msg_proxy_info(State, Msg0),
        route_message(State, Msg)
    catch Type:Error ->
        ?warning_stacktrace("Server message decoding error - ~p:~p", [
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
                % Ignore serialization errors as they should not break connection
                {error, serialization_failed} ->
                    {ok, State};
                Result ->
                    Result
            end;
        {error, Reason} ->
            ?warning("Message ~p handling error: ~p", [Msg, Reason]),
            {ok, State}
    end.


%% @private
-spec send_message(state(), message()) ->
    {ok, state()} | {error, Reason :: term()}.
send_message(State, #client_message{} = ClientMessage) ->
    send_client_message(State, ClientMessage);
send_message(State, #server_message{} = ServerMessage) ->
    send_server_message(State, ServerMessage).


%% @private
-spec send_client_message(state(), client_message()) ->
    {ok, state()} | {error, Reason :: term()}.
send_client_message(State, #client_message{} = ClientMsg) ->
    try
        {ok, Data} = serializator:serialize_client_message(ClientMsg),
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
send_server_message(State, #server_message{} = ServerMsg) ->
    try
        {ok, Data} = serializator:serialize_server_message(ServerMsg),
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
        {error, _Reason} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% If message comes from provider and proxy session is requested - proceed
%% with authorization and switch context to the proxy session.
%% @end
%%--------------------------------------------------------------------
-spec maybe_create_proxy_session(state(), client_message()) -> ok.
maybe_create_proxy_session(#state{peer_id = ProviderId}, #client_message{
    proxy_session_id = ProxySessionId,
    proxy_session_auth = Auth
}) when ProxySessionId =/= undefined ->
    {ok, _} = session_manager:reuse_or_create_proxy_session(
        ProxySessionId, ProviderId, Auth, fuse
    ),
    ok;
maybe_create_proxy_session(_, _) ->
    ok.


%% @private
-spec fill_server_msg_proxy_info(state(), server_message()) -> server_message().
fill_server_msg_proxy_info(State, #server_message{proxy_session_id = undefined} = Msg) ->
    Msg#server_message{proxy_session_id = State#state.session_id};
fill_server_msg_proxy_info(_, Msg) ->
    Msg.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% If socket_mode is set to active_always does nothing because it was
%% already set during connection creation and every received packet will
%% be send to process as erlang message.
%% Otherwise (active_once) activates socket so that only next received packet
%% will be send to process as erlang message.
%% @end
%%--------------------------------------------------------------------
-spec activate_socket_once(state()) -> ok.
activate_socket_once(#state{socket_mode = active_always}) ->
    ok;
activate_socket_once(#state{transport = Transport, socket = Socket}) ->
    ok = Transport:setopts(Socket, [{active, once}]).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts given server_message to client_message if possible.
%% @end
%%--------------------------------------------------------------------
-spec to_client_message(Msg :: server_message()) -> client_message().
to_client_message(#server_message{
    message_id = Id,
    message_stream = Stream,
    message_body = Body,
    proxy_session_id = SessId
}) ->
    #client_message{
        message_id = Id,
        message_stream = Stream,
        message_body = Body,
        proxy_session_id = SessId
    }.
