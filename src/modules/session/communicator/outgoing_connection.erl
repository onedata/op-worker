%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles outgoing connections where this provider serves as
%%% the client in communication with other providers.
%%% @end
%%%-------------------------------------------------------------------
-module(outgoing_connection).
-author("Lukasz Opiola").

-behaviour(gen_server).

-include("timeouts.hrl").
-include("global_definitions.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/handshake_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

-record(state, {
    socket :: ssl:socket(),
    transport :: module(),
    hostname :: binary(),
    % transport messages
    ok :: atom(),
    closed :: atom(),
    error :: atom(),
    % connection state
    status = upgrading_protocol :: upgrading_protocol | performing_handshake | ready,
    session_id :: undefined | session:id(),
    provider_id = undefined :: undefined | od_provider:id()
}).

-define(PACKET_VALUE, 4).

%% API
-export([start_link/6, init/6]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts an outgoing connection.
%% @end
%%--------------------------------------------------------------------
-spec start_link(od_provider:id(), session:id(), Hostname :: binary(),
    Port :: non_neg_integer(), Transport :: atom(), Timeout :: non_neg_integer()) ->
    {ok, Pid :: pid()}.
start_link(ProviderId, SessionId, Hostname, Port, Transport, Timeout) ->
    proc_lib:start_link(?MODULE, init, [ProviderId, SessionId, Hostname, Port, Transport, Timeout]).


%%--------------------------------------------------------------------
%% @doc
%% Initializes an outgoing connection.
%% @end
%%--------------------------------------------------------------------
-spec init(od_provider:id(), session:id(), Hostname :: binary(),
    Port :: non_neg_integer(), Transport :: atom(), Timeout :: non_neg_integer()) ->
    no_return().
init(ProviderId, SessionId, Hostname, Port, Transport, Timeout) ->
    CaCerts = oneprovider:get_ca_certs(),
    SecureFlag = application:get_env(?APP_NAME, interprovider_connections_security, true),
    Opts = secure_ssl_opts:expand(Hostname, [{cacerts, CaCerts}, {secure, SecureFlag}]),

    ?info("Connecting to provider '~s' - ~s:~p", [ProviderId, Hostname, Port]),
    {ok, Socket} = Transport:connect(binary_to_list(Hostname), Port, Opts, Timeout),

    {Ok, Closed, Error} = Transport:messages(),

    session_manager:reuse_or_create_provider_session(SessionId, provider_outgoing, #user_identity{
        provider_id = session_manager:session_id_to_provider_id(SessionId)}, self()),

    ok = proc_lib:init_ack({ok, self()}),
    self() ! upgrade_protocol,
    State = #state{
        socket = Socket,
        transport = Transport,
        hostname = Hostname,
        ok = Ok,
        closed = Closed,
        error = Error,
        session_id = SessionId,
        provider_id = ProviderId
    },
    activate_socket_once(State),
    gen_server2:enter_loop(?MODULE, [], State, ?PROTO_CONNECTION_TIMEOUT).


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
handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | {Ok :: atom(), Socket :: ssl:socket(),
    Data :: binary()} | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_info(upgrade_protocol, State = #state{hostname = Hostname}) ->
    socket_send(State, connection:protocol_upgrade_request(Hostname)),
    {noreply, State, ?PROTO_CONNECTION_TIMEOUT};

handle_info({Ok, Socket, Data}, State = #state{status = upgrading_protocol, ok = Ok}) ->
    case connection:verify_protocol_upgrade_response(Data) of
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
            send_client_message(State, ClientMsg),
            activate_socket_once(State),
            {noreply, State#state{status = performing_handshake}, ?PROTO_CONNECTION_TIMEOUT}
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

handle_info({Ok, Socket, Data}, State = #state{status = performing_handshake, socket = Socket, ok = Ok}) ->
    activate_socket_once(State),
    handle_handshake_response(State, Data);

% Defer any messages if the protocol upgrade and handshake haven't been made yet
handle_info(Msg, State = #state{status = Status}) when Status /= ready ->
    erlang:send_after(100, self(), Msg),
    {noreply, State, ?PROTO_CONNECTION_TIMEOUT};

handle_info({Ok, Socket, Data}, State = #state{socket = Socket, ok = Ok}) ->
    activate_socket_once(State),
    handle_server_message(State, Data);

handle_info({send_sync, From, ServerMsg = #server_message{}}, State) ->
    send_client_message(State, to_client_message(ServerMsg)),
    From ! {result, ok},
    {noreply, State, ?PROTO_CONNECTION_TIMEOUT};
handle_info({send_sync, From, ClientMsg = #client_message{}}, State) ->
    send_client_message(State, ClientMsg),
    From ! {result, ok},
    {noreply, State, ?PROTO_CONNECTION_TIMEOUT};

handle_info({send_async, ServerMsg = #server_message{}}, State) ->
    send_client_message(State, to_client_message(ServerMsg)),
    {noreply, State, ?PROTO_CONNECTION_TIMEOUT};
handle_info({send_async, ClientMsg = #client_message{}}, State) ->
    send_client_message(State, ClientMsg),
    {noreply, State, ?PROTO_CONNECTION_TIMEOUT};

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
    case SessId of
        undefined -> ok;
        _ -> session:remove_connection(SessId, self())
    end,
    ssl:close(Socket),
    State.


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
%% Handles handshake response received from server provider.
%% @end
%%--------------------------------------------------------------------
-spec handle_handshake_response(#state{}, binary()) ->
    {noreply, NewState :: #state{}, timeout()} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_handshake_response(State = #state{session_id = SessId, provider_id = ProviderId}, Data) ->
    try serializator:deserialize_server_message(Data, SessId) of
        {ok, #server_message{message_body = #handshake_response{status = 'OK'}}} ->
            ?info("Successfully connected to provider '~s'", [ProviderId]),
            {noreply, State#state{status = ready}, ?PROTO_CONNECTION_TIMEOUT};
        {ok, #server_message{message_body = #handshake_response{status = Error}}} ->
            ?error("Handshake refused by provider '~s' due to ~p, closing connection.", [
                ProviderId, Error
            ]),
            {stop, {shutdown, Error}, State};
        _ ->
            ?error("Received invalid handshake response from provider '~s', closing connection.", [
                ProviderId
            ]),
            {stop, {shutdown, invalid_handshake_response}, State}
    catch
        _:Error ->
            ?warning_stacktrace("Client message decoding error: ~p", [Error]),
            {stop, Error, State}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Safely handle incoming message data.
%% @end
%%--------------------------------------------------------------------
-spec handle_server_message(#state{}, binary()) ->
    {noreply, NewState :: #state{}, timeout()} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_server_message(State = #state{session_id = SessId}, Data) ->
    try serializator:deserialize_server_message(Data, SessId) of
        {ok, Msg} ->
            handle_server_message_unsafe(State, Msg)
    catch
        _:Error ->
            ?warning_stacktrace("Client message processing error: ~p", [Error]),
            {stop, Error, State}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle incoming message data.
%% @end
%%--------------------------------------------------------------------
-spec handle_server_message_unsafe(#state{}, #client_message{} | #server_message{}) ->
    {noreply, NewState :: #state{}, timeout()} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_server_message_unsafe(State = #state{session_id = SessId}, Msg) ->
    case router:preroute_message(Msg, SessId) of
        ok ->
            {noreply, State, ?PROTO_CONNECTION_TIMEOUT};
        {ok, ServerMsg} ->
            send_server_message(State, ServerMsg),
            {noreply, State, ?PROTO_CONNECTION_TIMEOUT};
        {error, Reason} ->
            ?warning("Message ~p handling error: ~p", [Msg, Reason]),
            {stop, {error, Reason}, State}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Activate socket for next message, so it will be sent to the handling process
%% via erlang message.
%% @end
%%--------------------------------------------------------------------
-spec activate_socket_once(#state{}) -> ok.
activate_socket_once(#state{transport = Transport, socket = Socket}) ->
    ok = Transport:setopts(Socket, [{active, once}]).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends #server_message via given socket.
%% @end
%%--------------------------------------------------------------------
-spec send_server_message(#state{}, #server_message{}) -> ok.
send_server_message(State, #server_message{} = ServerMsg) ->
    try serializator:serialize_server_message(ServerMsg) of
        {ok, Data} ->
            socket_send(State, Data)
    catch
        _:Reason ->
            ?error_stacktrace("Unable to serialize server_message ~p due to: ~p", [ServerMsg, Reason]),
            ok
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends #client_message via given socket.
%% @end
%%--------------------------------------------------------------------
-spec send_client_message(#state{}, #client_message{}) -> ok.
send_client_message(State, #client_message{} = ClientMsg) ->
    try serializator:serialize_client_message(ClientMsg) of
        {ok, Data} ->
            socket_send(State, Data)
    catch
        _:Reason ->
            ?error_stacktrace("Unable to serialize client_message ~p due to: ~p", [ClientMsg, Reason]),
            ok
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts given server_message to client_message if possible.
%% @end
%%--------------------------------------------------------------------
-spec to_client_message(Msg :: #server_message{}) ->
    #client_message{}.
to_client_message(#server_message{message_body = Body, message_id = Id, message_stream = Stream, proxy_session_id = SessId}) ->
    #client_message{message_body = Body, message_id = Id, message_stream = Stream, proxy_session_id = SessId}.


-spec socket_send(#state{}, Data :: binary()) -> ok.
socket_send(#state{transport = Transport, socket = Socket}, Data) ->
    ok = Transport:send(Socket, Data).