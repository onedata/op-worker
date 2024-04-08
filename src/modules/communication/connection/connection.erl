%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2020 ACK CYFRONET AGH
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
-include("middleware/middleware.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").


%% API
-export([start_link/5, start_link/7, close/1]).
-export([send_msg/2, send_keepalive/1]).
-export([rebuild_rib/1]).
-export([find_outgoing_session/1]).

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
    op_worker:get_env(default_socket_mode, active_once)
).
-define(DEFAULT_VERIFY_MSG_FLAG,
    op_worker:get_env(verify_msg_before_encoding, true)
).

-define(REBUILD_RIB_MSG, rebuild_rib).

-define(CONNECTION_AWAIT_LOG_INTERVAL, 300). % 5 minutes

-define(THROTTLE_ERROR(__SESSION_ID, __FORMAT, __ARGS),
    ?THROTTLE_LOG(__SESSION_ID, __FORMAT, __ARGS, fun() -> ?error(__FORMAT, __ARGS) end)
).
-define(THROTTLE_WARNING(__SESSION_ID, __FORMAT, __ARGS),
    ?THROTTLE_LOG(__SESSION_ID, __FORMAT, __ARGS, fun() -> ?warning(__FORMAT, __ARGS) end)
).
-define(THROTTLE_LOG(__SESSION_ID, __FORMAT, __ARGS, __LOG_FUN),
    begin
        ?debug(__FORMAT, __ARGS),
        utils:throttle({?MODULE, __SESSION_ID, ?LINE}, ?CONNECTION_AWAIT_LOG_INTERVAL, __LOG_FUN)
    end
).
-define(THROTTLE_ERROR_EXCEPTION(__SESSION_ID, __FORMAT, __ARGS, __CLASS, __REASON, __STACKTRACE),
    begin
        ?debug_exception(__FORMAT, __ARGS, __CLASS, __REASON, __STACKTRACE),
        utils:throttle({?MODULE, __SESSION_ID, ?LINE}, ?CONNECTION_AWAIT_LOG_INTERVAL, fun() ->
            ?error_exception(__FORMAT, __ARGS, __CLASS, __REASON, __STACKTRACE)
        end)
    end
).

% NOTE: lager defaults to truncating messages at 4096 bytes
-define(MAX_LOGGED_DATA_SIZE, 1024).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% @equiv start_link(ProviderId, SessId, Domain, Host, Port, ranch_ssl,
%% timer:seconds(5)).
%% @end
%%--------------------------------------------------------------------
-spec start_link(od_provider:id(), session:id(), Domain :: binary(),
    Host :: binary(), Port :: non_neg_integer()) -> {ok, pid()} | error().
start_link(ProviderId, SessId, Domain, Host, Port) ->
    start_link(ProviderId, SessId, Domain, Host, Port,
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
start_link(ProviderId, SessId, Domain, Host, Port, Transport, Timeout) ->
    ConnManager = self(),
    proc_lib:start_link(?MODULE, connect_with_provider, [
        ProviderId, SessId, Domain, Host, Port,
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
    call_connection_process(Pid, {send_msg, Msg}).


%%-------------------------------------------------------------------
%% @doc
%% Schedules keepalive message to be sent.
%% @end
%%-------------------------------------------------------------------
-spec send_keepalive(pid()) -> ok.
send_keepalive(Pid) ->
    gen_server2:cast(Pid, send_keepalive).


%%-------------------------------------------------------------------
%% @doc
%% Informs connection process that it should rebuild it's rib as it
%% may be obsolete (e.g. after node death).
%% @end
%%-------------------------------------------------------------------
-spec rebuild_rib(pid()) -> ok | error().
rebuild_rib(Pid) ->
    call_connection_process(Pid, ?REBUILD_RIB_MSG).


-spec find_outgoing_session(oneprovider:id()) -> {ok, session:id()} | error.
find_outgoing_session(ProviderId) ->
    SessionId = session_utils:get_provider_session_id(outgoing, ProviderId),
    case session:exists(SessionId) of
        true -> {ok, SessionId};
        false -> error
    end.


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
    ?warning("Attempt to send msg via not ready connection ~tp", [Socket]),
    {reply, {error, Status}, State, ?PROTO_CONNECTION_TIMEOUT};
handle_call(?REBUILD_RIB_MSG, _From, #state{session_id = SessId} = State) ->
    {reply, ok, State#state{rib = router:build_rib(SessId)}, ?PROTO_CONNECTION_TIMEOUT};
handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
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
handle_cast(Request, State) ->
    ?log_bad_request(Request),
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

handle_info({Ok, Socket, Data}, #state{
    status = upgrading_protocol,
    session_id = SessId,
    socket = Socket,
    ok = Ok
} = State) ->
    try handle_protocol_upgrade_response(State, Data) of
        {ok, State1} ->
            State2 = State1#state{status = performing_handshake},
            activate_socket(State2, false),
            {noreply, State2, ?PROTO_CONNECTION_TIMEOUT};
        {error, _Reason} ->
            % Concrete errors were already logged in 'handle_protocol_upgrade_response'
            % so terminate gracefully as to not spam more error logs
            {stop, normal, State}
    catch
        throw:{error, _} = Error ->
            ?THROTTLE_ERROR(SessId, ?error(?autoformat_with_msg("Protocol upgrade failed", [SessId, Error]))),
            {stop, normal, State};
        Class:Reason:Stacktrace ->
            ?THROTTLE_ERROR_EXCEPTION(
                SessId,
                ?error(?autoformat_with_msg("Unexpected error during protocol upgrade", SessId)),
                Class, Reason, Stacktrace
            ),
            {stop, normal, State}
    end;

handle_info({Ok, Socket, Data}, #state{
    status = performing_handshake,
    % outgoing session id is set when starting connection while incoming only
    % after successful handshake
    session_id = OutgoingSessIdOrUndefined,
    socket = Socket,
    ok = Ok
} = State) ->
    try handle_handshake(State, Data) of
        {ok, #state{session_id = SessId} = State1} ->
            ok = session_connections:register(SessId, self()),
            State2 = State1#state{status = ready},
            activate_socket(State2, false),
            {noreply, State2, ?PROTO_CONNECTION_TIMEOUT};
        {error, _Reason} ->
            % Concrete errors were already logged in 'handle_handshake' so
            % terminate gracefully as to not spam more error logs
            {stop, normal, State}
    catch Class:Reason:Stacktrace ->
        ?THROTTLE_ERROR_EXCEPTION(
            OutgoingSessIdOrUndefined,
            "Unexpected error while performing handshake", [],
            Class, Reason, Stacktrace
        ),
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
    ?warning("Connection ~tp error: ~tp", [Socket, Reason]),
    {stop, Reason, State};

handle_info({Closed, _}, State = #state{closed = Closed}) ->
    {stop, normal, State};

handle_info(timeout, State = #state{socket = Socket}) ->
    ?warning("Connection ~tp timeout", [Socket]),
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
terminate(Reason, #state{session_id = SessId, socket = Socket} = State) ->
    ?log_terminate(Reason, State),

    case SessId of
        undefined -> ok;
        _ -> session_connections:deregister(SessId, self())
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
        ?debug("Invalid protocol upgrade request - ~tp:~tp", [Type, Reason]),
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
    {Ok, Closed, Error, _} = Transport:messages(),

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
connect_with_provider(ProviderId, SessId, Domain,
    Host, Port, Transport, Timeout, ConnManager
) ->
    DomainAndIpInfo = case Domain of
        Host -> str_utils:format("@ ~ts:~b", [Host, Port]);
        _ -> str_utils:format("@ ~ts:~b (~ts)", [Host, Port, Domain])
    end,
    ?debug("Connecting to provider ~ts ~ts", [
        provider_logic:to_printable(ProviderId), DomainAndIpInfo
    ]),

    try
        State = open_socket_to_provider(
            SessId, ProviderId, Domain, Host, Port,
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
open_socket_to_provider(SessId, ProviderId, Domain,
    Host, Port, Transport, Timeout, ConnManager
) ->
    SslOpts = provider_logic:provider_connection_ssl_opts(Domain),
    ConnectOpts = secure_ssl_opts:expand(SslOpts),
    {ok, Socket} = Transport:connect(
        binary_to_list(Host), Port, ConnectOpts, Timeout
    ),
    {ok, {IpAddress, _Port}} = ssl:peername(Socket),

    {Ok, Closed, Error, _} = Transport:messages(),
    #state{
        socket = Socket,
        socket_mode = ?DEFAULT_SOCKET_MODE,
        transport = Transport,
        ok = Ok,
        closed = Closed,
        error = Error,
        type = outgoing,
        status = upgrading_protocol,
        session_id = SessId,
        peer_id = ProviderId,
        peer_ip = IpAddress,
        verify_msg = ?DEFAULT_VERIFY_MSG_FLAG,
        connection_manager = ConnManager,
        rib = router:build_rib(SessId)
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
handle_protocol_upgrade_response(#state{session_id = SessId} = State, Data) ->
    case connection_utils:verify_protocol_upgrade_response(Data) of
        false ->
            ?THROTTLE_ERROR(SessId, ?error(?autoformat_with_msg(
                "Received invalid protocol upgrade response", [SessId, Data]))),
            {error, invalid_protocol_upgrade_response};
        true ->
            #state{
                socket = Socket,
                transport = Transport,
                peer_id = ProviderId
            } = State,
            {ok, MsgId} = clproto_message_id:generate(self()),
            Token = ?check(provider_auth:acquire_identity_token_for_consumer(
                ?SUB(?ONEPROVIDER, ProviderId)
            )),
            ClientMsg = #client_message{
                message_id = MsgId,
                message_body = #provider_handshake_request{
                    provider_id = oneprovider:get_id(),
                    token = Token
                }
            },
            ok = Transport:setopts(Socket, [binary, {packet, ?PACKET_VALUE}]),
            socket_send(State, ClientMsg)
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

        {PeerId, SessId} = connection_auth:handle_handshake(
            Msg#client_message.message_body, IpAddress
        ),

        NewState = State#state{
            peer_id = PeerId,
            session_id = SessId,
            rib = router:build_rib(SessId)
        },
        socket_send(NewState, #server_message{
            message_body = #handshake_response{status = 'OK'}
        })
    catch Type:Reason ->
        ?debug("Invalid handshake request - ~tp:~tp", [Type, Reason]),
        ErrorMsg = connection_auth:get_handshake_error_msg(Reason),
        socket_send(State, ErrorMsg),
        {error, handshake_failed}
    end.


%% @private
-spec handle_handshake_response(state(), binary()) -> {ok, state()} | error().
handle_handshake_response(#state{
    session_id = SessId,
    peer_id = ProviderId,
    connection_manager = ConnManager
} = State, Data) ->
    ProviderInfo = provider_logic:to_printable(ProviderId),
    try clproto_serializer:deserialize_server_message(Data, SessId) of
        {ok, #server_message{message_body = #handshake_response{status = 'OK'}}} ->
            ?info("Successfully connected to provider ~ts", [ProviderInfo]),
            outgoing_connection_manager:report_successful_handshake(ConnManager),
            {ok, State};
        {ok, #server_message{message_body = #handshake_response{status = Error}}} ->
            ?THROTTLE_ERROR(
                SessId,
                ?error(?autoformat_with_msg("Handshake refused by peer provider, closing connection",
                [ProviderInfo, Error]
            ))),
            {error, handshake_failed};
        Response ->
            ?THROTTLE_ERROR(
                SessId,
                ?error(?autoformat_with_msg(
                    "Received invalid handshake response from peer provider, closing connection",
                [ProviderInfo, Response]
            ))),
            {error, handshake_failed}
    catch Class:Reason:Stacktrace ->
        DataSample = str_utils:truncate_overflow(Data, ?MAX_LOGGED_DATA_SIZE),
        ?THROTTLE_ERROR_EXCEPTION(
            SessId,
            ?error(?autoformat_with_msg("Error decoding handshake response from peer provider",
            [ProviderInfo, DataSample])),
            Class, Reason, Stacktrace
        ),
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
                MsgStr = clproto_utils:msg_to_string(Msg),
                ?THROTTLE_ERROR(
                    SessId,
                    ?error(?autoformat_with_msg("Failed to create proxied session while handling message",
                        [Error, MsgStr]
                    ))),
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
            ?THROTTLE_ERROR(SessId, ?error(?autoformat_with_msg(
                "Client message deserialization error", [SessId, Reason]))),
            {ok, State};
        throw:{translation_failed, Reason, MsgId} ->
            Code = case Reason of
                {unrecognized_message, Message} ->
                    ?THROTTLE_WARNING(SessId, ?error(?autoformat_with_msg(
                        "Unrecognized client message", [SessId, Message]))),
                    ?EBADMSG;
                _ ->
                    ?THROTTLE_ERROR(SessId, ?error(?autoformat_with_msg(
                        "Client message deserialization error", [SessId, Reason]))),
                    ?EINVAL
            end,
            send_response(State, #server_message{
                message_id = MsgId,
                message_body = #status{code = Code}
            });
        Class:Reason:Stacktrace ->
            DataSample = str_utils:truncate_overflow(Data, 1000),
            ?THROTTLE_ERROR_EXCEPTION(
                SessId,
                ?error(?autoformat_with_msg("Client message handling error", [SessId, DataSample])),
                Class, Reason, Stacktrace
            ),
            {ok, State}
    end.


%% @private
-spec handle_server_message(state(), binary()) -> {ok, state()} | error().
handle_server_message(#state{session_id = SessId} = State, Data) ->
    try
        {ok, Msg} = clproto_serializer:deserialize_server_message(Data, SessId),
        route_message(State, Msg)
    catch Class:Reason:Stacktrace ->
        DataSample = str_utils:truncate_overflow(Data, ?MAX_LOGGED_DATA_SIZE),
        ?THROTTLE_ERROR_EXCEPTION(SessId, ?error(?autoformat(SessId, DataSample)), [], Class, Reason, Stacktrace),
        {ok, State}
    end.


%% @private
-spec route_message(state(), communicator:message()) ->
    {ok, state()} | error().
route_message(#state{session_id = SessId, rib = RIB} = State, Msg) ->
    case router:route_message(Msg, RIB) of
        ok ->
            {ok, State};
        {ok, ServerMsg} ->
            send_response(State, ServerMsg);
        {error, _} = Error ->
            MsgStr = clproto_utils:msg_to_string(Msg),
            ?THROTTLE_ERROR(SessId, ?error(?autoformat_with_msg("Message routing error", [Error, MsgStr]))),
            {ok, State}
    end.


%% @private
-spec send_response(state(), communicator:server_message()) ->
    {ok, state()} | error().
send_response(#state{session_id = SessId} = State, ServerMsg) ->
    case socket_send(State, ServerMsg) of
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
            session_connections:deregister(SessId, self()),
            connection_api:send(SessId, ServerMsg, [self()], true),
            Error
    end.


%% @private
-spec send_message(state(), communicator:message()) -> {ok, state()} | error().
send_message(#state{type = outgoing} = State, #client_message{} = Msg) ->
    socket_send(State, Msg);
send_message(#state{type = incoming} = State, #server_message{} = Msg) ->
    socket_send(State, Msg);
send_message(#state{type = ConnType}, Msg) ->
    MsgStr = clproto_utils:msg_to_string(Msg),
    ?error(?autoformat_with_msg("Attempt to send msg via wrong connection", [ConnType, MsgStr])),
    {error, sending_msg_via_wrong_conn_type}.


%% @private
-spec socket_send(state(), binary() | #client_message{} | #server_message{}) -> {ok, state()} | error().
socket_send(#state{
    session_id = SessId,
    transport = Transport,
    socket = Socket
} = State, DataOrMsg) ->
    case to_serialized_data(State, DataOrMsg) of
        {error, serialization_failed} ->
            {error, serialization_failed};
        {ok, Data} ->
            case Transport:send(Socket, Data) of
                ok ->
                    {ok, State};
                {error, closed} ->
                    % the connection may have closed in the meantime, no need to log
                    {error, closed};
                {error, _} = Error ->
                    MsgStr = case DataOrMsg of
                        Bin when is_binary(Bin) -> Bin;
                        _ -> clproto_utils:msg_to_string(DataOrMsg)
                    end,
                    ?THROTTLE_ERROR(SessId, ?error(?autoformat_with_msg("Unable to send message via socket", [
                        Socket, Error, MsgStr
                    ]))),
                    Error
            end
    end.


%% @private
-spec to_serialized_data(state(), binary() | #client_message{} | #server_message{}) -> {ok, binary()} | errors:error().
to_serialized_data(_State, Binary) when is_binary(Binary) ->
    {ok, Binary};
to_serialized_data(#state{session_id = SessId} = State, Msg) ->
    try
        serialize_message_unsafe(State, Msg)
    catch Class:Reason:Stacktrace ->
        MsgStr = clproto_utils:msg_to_string(Msg),
        ?THROTTLE_ERROR_EXCEPTION(SessId, "Unable to serialize message: ~ts", [MsgStr], Class, Reason, Stacktrace),
        {error, serialization_failed}
    end.


%% @private
-spec serialize_message_unsafe(state(), #client_message{} | #server_message{}) -> {ok, binary()} | errors:error().
serialize_message_unsafe(#state{verify_msg = VerifyMsg}, #client_message{} = Msg) ->
    clproto_serializer:serialize_client_message(Msg, VerifyMsg);
serialize_message_unsafe(#state{verify_msg = VerifyMsg}, #server_message{} = Msg) ->
    clproto_serializer:serialize_server_message(Msg, VerifyMsg).


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


%% @private
-spec call_connection_process(ConnPid :: pid(), term()) ->
    ok | error().
call_connection_process(ConnPid, Msg) ->
    try
        gen_server2:call(ConnPid, Msg, ?DEFAULT_REQUEST_TIMEOUT)
    catch
        exit:{noproc, _} ->
            ?debug("Connection process ~tp does not exist", [ConnPid]),
            {error, no_connection};
        exit:{{nodedown, Node}, _} ->
            ?debug("Node ~tp with connection process ~tp is down", [Node, ConnPid]),
            {error, no_connection};
        exit:{normal, _} ->
            ?debug("Exit of connection process ~tp for message ~ts", [
                ConnPid, maybe_stringify_msg(Msg)
            ]),
            {error, no_connection};
        exit:{timeout, _} ->
            ?debug("Timeout of connection process ~tp for message ~ts", [
                ConnPid, maybe_stringify_msg(Msg)
            ]),
            ?ERROR_TIMEOUT;
        Class:Reason:Stacktrace ->
            MsgStr = maybe_stringify_msg(Msg),
            ?error_exception(
                ?autoformat_with_msg("Cannot send message via the connection process", [ConnPid, MsgStr]),
                Class, Reason, Stacktrace
            ),
            {error, Reason}
    end.


%% @private
-spec maybe_stringify_msg(term()) -> term() | string().
maybe_stringify_msg({send_msg, Msg}) ->
    clproto_utils:msg_to_string(Msg);
maybe_stringify_msg(Msg) ->
    Msg.
