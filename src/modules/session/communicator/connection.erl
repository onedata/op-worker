%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles client and provider requests.
%%% @end
%%%-------------------------------------------------------------------
-module(connection).
-author("Tomasz Lichon").

-behaviour(ranch_protocol).
-behaviour(gen_server).

-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneclient/handshake_messages.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/4, init/4, send/2, send_async/2]).
-export([start_link/5, init/5]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-type ref() :: pid() | session:id().

-export_type([ref/0]).

-record(state, {
    certificate :: #'OTPCertificate'{},
    % handler responses
    ok :: atom(),
    closed :: atom(),
    error :: atom(),
    % actual connection state
    socket :: ssl2:socket(),
    transport :: module(),
    session_id :: session:id(),
    connection_type :: incoming | outgoing,
    peer_type = fuse_client :: fuse_client | provider
}).

-define(TIMEOUT, timer:minutes(10)).
-define(PACKET_VALUE, 4).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the incoming connection.
%% @end
%%--------------------------------------------------------------------
-spec start_link(Ref :: atom(), Socket :: ssl2:socket(), Transport :: atom(),
    Opts :: list()) -> {ok, Pid :: pid()}.
start_link(Ref, Socket, Transport, Opts) ->
    proc_lib:start_link(?MODULE, init, [Ref, Socket, Transport, Opts]).

start_link(SessionId, Hostname, Port, Transport, Timeout) ->
    proc_lib:start_link(?MODULE, init, [SessionId, Hostname, Port, Transport, Timeout]).

%%--------------------------------------------------------------------
%% @doc
%% Initializes the connection.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term(), Socket :: ssl2:socket(), Transport :: atom(), Opts :: list()) ->
    no_return().
init(Ref, Socket, Transport, _Opts) ->
    ok = proc_lib:init_ack({ok, self()}),
    ok = ranch:accept_ack(Ref),
    ok = Transport:setopts(Socket, [binary, {active, once}, {packet, ?PACKET_VALUE}]),
    {Ok, Closed, Error} = Transport:messages(),
    Certificate = get_cert(Socket),

    PeerType = case provider_auth_manager:is_provider(Certificate) of
        true -> provider;
        false -> fuse_client
    end,

    gen_server:enter_loop(?MODULE, [], #state{
        socket = Socket,
        transport = Transport,
        ok = Ok,
        closed = Closed,
        error = Error,
        certificate = Certificate,
        connection_type = incoming,
        peer_type = PeerType
    }, ?TIMEOUT).


%%--------------------------------------------------------------------
%% @doc
%% Initializes the outgoing connection.
%% @end
%%--------------------------------------------------------------------
-spec init(session:id(), Hostname :: binary(), Port :: non_neg_integer(), Transport :: atom(), Timeout :: non_neg_integer()) ->
    no_return().
init(SessionId, Hostname, Port, Transport, Timeout) ->
    TLSSettings = [{certfile, oz_plugin:get_cert_path()}, {keyfile, oz_plugin:get_key_path()}],
    ?info("Connecting to ~p ~p", [Hostname, Port]),
    {ok, Socket} = Transport:connect(Hostname, Port, TLSSettings, Timeout),
    ok = Transport:setopts(Socket, [binary, {active, once}, {packet, ?PACKET_VALUE}]),

    {Ok, Closed, Error} = Transport:messages(),
    Certificate = get_cert(Socket),

    session_manager:reuse_or_create_provider_session(SessionId, provider_outgoing, #identity{
        provider_id = session_manager:session_id_to_provider_id(SessionId)}, self()),

    ok = proc_lib:init_ack({ok, self()}),
    gen_server:enter_loop(?MODULE, [], #state{
        socket = Socket,
        transport = Transport,
        ok = Ok,
        closed = Closed,
        error = Error,
        certificate = Certificate,
        connection_type = outgoing,
        peer_type = provider
    }, ?TIMEOUT).

%%--------------------------------------------------------------------
%% @doc
%% Synchronously sends server message to client.
%% @end
%%--------------------------------------------------------------------
-spec send(Msg :: #server_message{} | #client_message{}, Ref :: ref()) ->
    ok | {error, Reason :: term()} | {exit, Reason :: term()}.
send(Msg, Ref) when is_pid(Ref) ->
    try
        gen_server:call(Ref, {send, Msg})
    catch
        _:Reason -> {error, Reason}
    end;
send(Msg, Ref) ->
    case session:get_random_connection(Ref) of
        {ok, Con} -> send(Msg, Con);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously sends server message to client.
%% @end
%%--------------------------------------------------------------------
-spec send_async(Msg :: #server_message{} | #client_message{}, Ref :: ref()) ->
    ok | {error, Reason :: term()}.
send_async(Msg, Ref) when is_pid(Ref) ->
    try
        gen_server:cast(Ref, {send, Msg})
    catch
        _:Reason -> {error, Reason}
    end;
send_async(Msg, Ref) ->
    case session:get_random_connection(Ref) of
        {ok, Con} -> send(Msg, Con);
        {error, Reason} -> {error, Reason}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is never called. We only define it so that
%% we can use the -behaviour(gen_server) attribute. Init is handled by ranch
%% init/4 function.
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
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_call({send, #server_message{} = ServerMsg}, _From, State = #state{socket = Socket, connection_type = incoming,
    transport = Transport}) ->
    send_server_message(Socket, Transport, ServerMsg),
    {reply, ok, State};
handle_call({send, #server_message{} = ServerMsg}, _From, State = #state{socket = Socket, connection_type = outgoing,
    transport = Transport}) ->
    send_client_message(Socket, Transport, to_client_message(ServerMsg)),
    {reply, ok, State};
handle_call({send, ClientMsg = #client_message{message_id = #message_id{recipient = Pid, id = MessageId} = MID}},
    _From, State = #state{socket = Socket, transport = Transport}) when is_pid(Pid) ->
    {ok, _} = message_id:save(#document{key = MessageId, value = MID}),
    send_client_message(Socket, Transport, ClientMsg),
    {reply, ok, State};
handle_call({send, ClientMsg = #client_message{}},
    _From, State = #state{socket = Socket, transport = Transport}) ->
    send_client_message(Socket, Transport, ClientMsg),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, wrong_request, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_cast({send, ServerMsg}, State = #state{socket = Socket, connection_type = incoming,
    transport = Transport}) ->
    send_server_message(Socket, Transport, ServerMsg),
    {noreply, State};
handle_cast({send, ServerMsg}, State = #state{socket = Socket, connection_type = outgoing,
    transport = Transport}) ->
    send_client_message(Socket, Transport, to_client_message(ServerMsg)),
    {noreply, State};
handle_cast({send, ClientMsg}, State = #state{socket = Socket, connection_type = outgoing,
    transport = Transport}) ->
    send_client_message(Socket, Transport, ClientMsg),
    {noreply, State};

handle_cast(disconnect, State) ->
    {stop, normal, State};

handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | {Ok :: atom(), Socket :: ssl2:socket(),
    Data :: binary()} | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_info({Ok, Socket, Data}, State = #state{socket = Socket, ok = Ok, connection_type = incoming,
    transport = Transport}) ->
    activate_socket_once(Socket, Transport),
    handle_client_message(State, Data);
handle_info({Ok, Socket, Data}, State = #state{socket = Socket, ok = Ok, connection_type = outgoing,
    transport = Transport}) ->
    activate_socket_once(Socket, Transport),
    handle_server_message(State, Data);

handle_info({Closed, _}, State = #state{closed = Closed}) ->
    {stop, normal, State};

handle_info({Error, Socket, Reason}, State = #state{error = Error}) ->
    ?warning("Connection ~p error: ~p", [Socket, Reason]),
    {stop, Reason, State};

handle_info(timeout, State = #state{socket = Socket}) ->
    ?warning("Connection ~p timeout", [Socket]),
    {stop, normal, State};

handle_info(_Info, State) ->
    ?log_bad_request(_Info),
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
    State :: #state{}) -> term().
terminate(Reason, #state{session_id = SessId, socket = Socket} = State) ->
    ?log_terminate(Reason, State),
    session:remove_connection(SessId, self()),
    ssl2:close(Socket),
    ssl2:close(State#state.socket),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) -> {ok, NewState :: #state{}} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle usual client data, it is decoded and passed to subsequent handler
%% functions
%% @end
%%--------------------------------------------------------------------
-spec handle_client_message(#state{}, binary()) ->
    {noreply, NewState :: #state{}, timeout()} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_client_message(State = #state{session_id = SessId, certificate = Cert}, Data) ->
    IsProvider = provider_auth_manager:is_provider(Cert),
    try serializator:deserialize_client_message(Data, SessId) of
        {ok, Msg} when SessId == undefined, IsProvider ->
            NewSessId = provider_auth_manager:handshake(Cert, self()),
            handle_normal_message(State#state{session_id = NewSessId}, Msg#client_message{session_id = NewSessId});
        {ok, Msg} when SessId == undefined ->
            handle_handshake(State, Msg);
        {ok, Msg} ->
            handle_normal_message(State, Msg#client_message{session_id = SessId})
    catch
        _:Error ->
            ?warning_stacktrace("Client message decoding error: ~p", [Error]),
            {stop, Error, State}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle usual server data, it is decoded and passed to subsequent handler
%% functions
%% @end
%%--------------------------------------------------------------------
-spec handle_server_message(#state{}, binary()) ->
    {noreply, NewState :: #state{}, timeout()} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_server_message(State = #state{session_id = SessId, certificate = Cert}, Data) ->
    try serializator:deserialize_server_message(Data, SessId) of
        {ok, Msg} ->
            handle_normal_message(State, Msg)
    catch
        _:Error ->
            ?warning_stacktrace("Client message decoding error: ~p", [Error]),
            {stop, Error, State}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle client handshake_request, it is necessary to authenticate
%% and obtain session
%% @end
%%--------------------------------------------------------------------
-spec handle_handshake(#state{}, #client_message{}) ->
    {noreply, NewState :: #state{}, timeout()} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_handshake(State = #state{certificate = Cert, socket = Sock,
    transport = Transp}, Msg) ->
    try fuse_auth_manager:handle_handshake(Msg, Cert) of
        {ok, Response = #server_message{message_body =
        #handshake_response{session_id = NewSessId}}} ->
            send_server_message(Sock, Transp, Response),
            {noreply, State#state{session_id = NewSessId}, ?TIMEOUT}
    catch
        _:Error ->
            ?warning_stacktrace("Handshake ~p, error ~p", [Msg, Error]),
            {stop, Error, State}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle nomal client_message
%% @end
%%--------------------------------------------------------------------
-spec handle_normal_message(#state{}, #client_message{} | #server_message{}) ->
    {noreply, NewState :: #state{}, timeout()} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_normal_message(State0 = #state{certificate = Cert, session_id = SessId, socket = Sock,
    transport = Transp}, Msg1) ->
    IsProvider = provider_auth_manager:is_provider(Cert),
    {State, Msg0} = update_message_id(State0, Msg1),

    {Msg, EffectiveSessionId} =
        case {IsProvider, Msg0} of
            {true, #client_message{proxy_session_id = ProxySessionId, proxy_session_auth = Auth = #auth{}}} when ProxySessionId =/= undefined ->
                ProviderId = provider_auth_manager:get_provider_id(Cert),
                {ok, _} = session_manager:reuse_or_create_proxy_session(ProxySessionId, ProviderId, Auth, fuse),
                {Msg0#client_message{session_id = ProxySessionId}, ProxySessionId};
            {true, #client_message{proxy_session_id = ProxySessionId}} when ProxySessionId =/= undefined ->
                {Msg0#client_message{session_id = ProxySessionId}, ProxySessionId};
            _ ->
                {Msg0, SessId}
        end,
    ?info("Handle message ~p ~p ~p ~p", [IsProvider, Msg, EffectiveSessionId, SessId]),

    case Msg of
        #server_message{proxy_session_id = TargetSessionId} when TargetSessionId =/= SessId ->
            ?info("REROUTE CONN ~p ~p ~p", [TargetSessionId, SessId, Msg]),
            connection:send(Msg, TargetSessionId),
            {noreply, State, ?TIMEOUT};
        _ ->
            case router:preroute_message(Msg, EffectiveSessionId) of
                ok ->
                    {noreply, State, ?TIMEOUT};
                {ok, ServerMsg} ->
                    ?info("Reply ~p", [ServerMsg]),
                    send_server_message(Sock, Transp, ServerMsg),
                    {noreply, State, ?TIMEOUT};
                {error, Reason} ->
                    ?warning("Message ~p handling error: ~p", [Msg, Reason]),
                    {stop, {error, Reason}, State}
            end
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% For message that is a response fo earlier request, update its recipient with stored, waiting pid.
%% @end
%%--------------------------------------------------------------------
-spec update_message_id(#state{}, #server_message{} | #client_message{}) ->
    {NewState :: #state{}, NewMsg :: #server_message{}}.
update_message_id(State = #state{connection_type = outgoing},
    Msg = #server_message{message_id = #message_id{id = ID} = MID}) ->

    NewMID = case message_id:get(ID) of
        {ok, #document{value = NewMID0}} ->
            message_id:delete(ID),
            NewMID0;
        _ ->
            MID
    end,

    NewMsg = Msg#server_message{message_id = NewMID},
    {State, NewMsg};
update_message_id(State, Msg) ->
    {State, Msg}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Activate socket for next message, so it will be send to handling process
%% via erlang message
%% @end
%%--------------------------------------------------------------------
-spec activate_socket_once(Socket :: ssl2:socket(), Transport :: module()) -> ok.
activate_socket_once(Socket, Transport) ->
    ok = Transport:setopts(Socket, [{active, once}]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends #server_message via given socket.
%% @end
%%--------------------------------------------------------------------
-spec send_server_message(Socket :: ssl2:socket(), Transport :: module(),
    ServerMessage :: #server_message{}) -> ok.
send_server_message(Socket, Transport, #server_message{} = ServerMsg) ->
    {ok, Data} = serializator:serialize_server_message(ServerMsg),
    ?info("Sending ~p", [ServerMsg]),
    ok = Transport:send(Socket, Data).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends #client_message via given socket.
%% @end
%%--------------------------------------------------------------------
-spec send_client_message(Socket :: ssl2:socket(), Transport :: module(),
    ServerMessage :: #client_message{}) -> ok.
send_client_message(Socket, Transport, #client_message{} = ClientMsg) ->
    {ok, Data} = serializator:serialize_client_message(ClientMsg),
    ?info("Sending ~p", [ClientMsg]),
    ok = Transport:send(Socket, Data).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns OTP certificate for given socket or 'undefined' if there isn't one.
%% @end
%%--------------------------------------------------------------------
-spec get_cert(Socket :: ssl2:socket()) ->
    undefined | #'OTPCertificate'{}.
get_cert(Socket) ->
    case ssl2:peercert(Socket) of
        {error, _} -> undefined;
        {ok, Der} -> public_key:pkix_decode_cert(Der, otp)
    end.


to_client_message(#server_message{message_body = Body, message_id = Id, message_stream = Stream, proxy_session_id = SessId}) ->
    #client_message{message_body = Body, message_id = Id, message_stream = Stream, proxy_session_id = SessId}.
