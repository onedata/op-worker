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
-include("http/http_common.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/common/handshake_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

-record(state, {
    socket :: ssl:socket(),
    transport :: module(),
    ip :: binary(),
    % transport messages
    ok :: atom(),
    closed :: atom(),
    error :: atom(),
    % connection state
    status = upgrading_protocol :: upgrading_protocol | performing_handshake | ready,
    session_id :: undefined | session:id(),
    provider_id = undefined :: undefined | od_provider:id(),
    wait_map = #{} :: map(),
    wait_pids = #{} :: map()
}).

-define(PACKET_VALUE, 4).

% Definitions of reconnect intervals for provider connection.
-define(INITIAL_RECONNECT_INTERVAL_SEC, 2).
-define(RECONNECT_INTERVAL_INCREASE_RATE, 2).
-define(MAX_RECONNECT_INTERVAL, timer:minutes(15)).

%% API
-export([start_link/7, init/7]).
-export([send_server_message/3]).

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
-spec start_link(od_provider:id(), session:id(), Domain :: binary(), IP :: binary(),
    Port :: non_neg_integer(), Transport :: atom(), Timeout :: non_neg_integer()) ->
    {ok, Pid :: pid()}.
start_link(ProviderId, SessionId, Domain, IP, Port, Transport, Timeout) ->
    proc_lib:start_link(?MODULE, init, [ProviderId, SessionId, Domain, IP, Port, Transport, Timeout]).


%%--------------------------------------------------------------------
%% @doc
%% Initializes an outgoing connection.
%% @end
%%--------------------------------------------------------------------
-spec init(od_provider:id(), session:id(), Domain :: binary(), IP :: binary(),
    Port :: non_neg_integer(), Transport :: atom(), Timeout :: non_neg_integer()) ->
    no_return().
init(ProviderId, SessionId, Domain, IP, Port, Transport, Timeout) ->
    % Map keeping reconnect interval and time between provider connection
    % retries; needed to implement backoff algorithm
    Intervals = application:get_env(
        ?APP_NAME, providers_reconnect_intervals, #{}
    ),
    ProviderId = session_manager:session_id_to_provider_id(SessionId),
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
                State = init_provider_conn(
                    SessionId, ProviderId, Domain, IP, Port, Transport, Timeout
                ),
                reset_reconnect_interval(Intervals, ProviderId),
                gen_server2:enter_loop(?MODULE, [], State, ?PROTO_CONNECTION_TIMEOUT)
            catch
                throw:incompatible_peer_op_version ->
                    postpone_next_reconnect(Intervals, ProviderId, Interval),
                    exit(normal);
                Type:Reason ->
                    ?warning("Failed to connect to peer provider(~p) - ~p:~p. "
                    "Next retry not sooner than ~p seconds.", [
                        ProviderId, Type, Reason, Interval
                    ]),
                    postpone_next_reconnect(Intervals, ProviderId, Interval),
                    exit(Reason)
            end
    end.


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
handle_info(upgrade_protocol, State = #state{ip = Hostname}) ->
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

handle_info(heartbeat, #state{wait_map = WaitMap, wait_pids = Pids} = State) ->
    TimeoutFun = fun(Id) ->
        send_server_message(State, router:get_heartbeat_msg(Id))
    end,
    ErrorFun = fun(Id) ->
        send_server_message(State, router:get_error_msg(Id))
    end,
    {Pids2, WaitMap2} = router:check_processes(Pids, WaitMap, TimeoutFun, ErrorFun),

    Interval = router:get_processes_check_interval(),
    erlang:send_after(Interval, self(), heartbeat),
    {noreply, State#state{wait_map = WaitMap2, wait_pids = Pids2},
        ?PROTO_CONNECTION_TIMEOUT};

handle_info(Info, #state{wait_map = WaitMap, wait_pids = Pids} = State) ->
    case router:process_ans(Info, WaitMap, Pids) of
        wrong_message ->
            ?log_bad_request(Info),
            {stop, normal, State};
        {Return, WaitMap2, Pids2} ->
            send_server_message(State, Return),
            {noreply, State#state{wait_map = WaitMap2, wait_pids = Pids2},
                ?PROTO_CONNECTION_TIMEOUT}
    end.


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
handle_server_message_unsafe(State = #state{session_id = SessId,
    wait_map = WaitMap, wait_pids = Pids}, Msg) ->
    case router:preroute_message(Msg, SessId) of
        ok ->
            {noreply, State, ?PROTO_CONNECTION_TIMEOUT};
        {ok, ServerMsg} ->
            send_server_message(State, ServerMsg),
            {noreply, State, ?PROTO_CONNECTION_TIMEOUT};
        {wait, Delegation} ->
            {WaitMap2, Pids2} = router:save_delegation(Delegation, WaitMap, Pids),
            {noreply, State#state{wait_map = WaitMap2, wait_pids = Pids2},
                ?PROTO_CONNECTION_TIMEOUT};
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
-spec send_server_message(Socket :: ssl:socket(), Transport :: module(),
    #server_message{}) -> ok.
send_server_message(Socket, Transport, #server_message{} = ServerMsg) ->
    try serializator:serialize_server_message(ServerMsg) of
        {ok, Data} ->
            Transport:send(Socket, Data)
    catch
        _:Reason ->
            ?error_stacktrace("Unable to serialize server_message ~p due to: ~p",
                [ServerMsg, Reason]),
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


%%--------------------------------------------------------------------
%% @doc
%% Attempt to connect to peer provider.
%% @end
%%--------------------------------------------------------------------
-spec init_provider_conn(session:id(), od_provider:id(), Domain :: binary(), IP :: binary(),
    Port :: non_neg_integer(), Transport :: atom(),
    Timeout :: non_neg_integer()) -> #state{} | no_return().
init_provider_conn(SessionId, ProviderId, Domain, IP, Port, Transport, Timeout) ->
    case provider_logic:verify_provider_identity(ProviderId) of
        ok ->
            ok;
        Err ->
            ?warning("Cannot verify identity of provider ~p, skipping connection - ~p", [
                ProviderId, Err
            ]),
            erlang:error({cannot_verify_identity, ProviderId})
    end,

    CaCerts = oneprovider:trusted_ca_certs(),
    SecureFlag = application:get_env(?APP_NAME, interprovider_connections_security, true),
    SslOpts = [{cacerts, CaCerts}, {secure, SecureFlag}, {hostname, Domain}],

    assert_compatibility(IP, ProviderId, SslOpts),

    DomainAndIpInfo = case Domain of
        IP -> str_utils:format("@ ~s:~b", [IP, Port]);
        _ -> str_utils:format("(~s) @ ~s:~b", [Domain, IP, Port])
    end,
    ?info("Connecting to provider '~s' ~s", [ProviderId, DomainAndIpInfo]),
    ConnectOpts = secure_ssl_opts:expand(IP, SslOpts),
    {ok, Socket} = Transport:connect(binary_to_list(IP), Port, ConnectOpts, Timeout),

    {Ok, Closed, Error} = Transport:messages(),

    session_manager:reuse_or_create_provider_session(SessionId, provider_outgoing, #user_identity{
        provider_id = session_manager:session_id_to_provider_id(SessionId)}, self()),

    ok = proc_lib:init_ack({ok, self()}),
    self() ! upgrade_protocol,
    State = #state{
        socket = Socket,
        transport = Transport,
        ip = IP,
        ok = Ok,
        closed = Closed,
        error = Error,
        session_id = SessionId,
        provider_id = ProviderId
    },
    activate_socket_once(State),
    State.


%%--------------------------------------------------------------------
%% @doc @private
%% Assert that peer provider is of compatible version.
%% @end
%%--------------------------------------------------------------------
-spec assert_compatibility(binary(), od_provider:id(), [http_client:ssl_opt()]) ->
    ok | no_return().
assert_compatibility(Hostname, ProviderId, SslOpts) ->
    URL = str_utils:format_bin("https://~s~s", [Hostname, ?provider_version_path]),
    {ok, CompatibleVersions} = application:get_env(?APP_NAME, compatible_op_versions),
    case http_client:get(URL, #{}, <<>>, [{ssl_options, SslOpts}]) of
        {ok, 200, _RespHeaders, ResponseBody} ->
            PeerProviderVersion = binary_to_list(ResponseBody),
            case lists:member(PeerProviderVersion, CompatibleVersions) of
                true ->
                    ok;
                false ->
                    ?error("Discarding connection to provider ~p because of "
                    "incompatible version (~s). Version must be one of: ~p",
                        [ProviderId, PeerProviderVersion, CompatibleVersions]
                    ),
                    throw(incompatible_peer_op_version)
            end;
        {ok, _Code, _RespHeaders, _ResponseBody} ->
            throw(cannot_check_peer_op_version);
        {error, Error} ->
            error(Error)
    end.


%%--------------------------------------------------------------------
%% @doc @private
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
%% @doc @private
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
