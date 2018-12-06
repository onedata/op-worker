%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles incoming connections where this provider serves as
%%% the server in communication with oneclients and other providers.
%%% @end
%%%-------------------------------------------------------------------
-module(incoming_connection).
-author("Lukasz Opiola").

-behaviour(cowboy_sub_protocol).

-include("timeouts.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("http/gui_paths.hrl").
-include_lib("ctool/include/logging.hrl").

-record(state, {
    socket :: ssl:socket(),
    transport :: module(),
    % transport messages
    ok :: atom(),
    closed :: atom(),
    error :: atom(),
    % connection state
    session_id :: undefined | session:id(),
    peer_id = undefined :: undefined | od_provider:id() | od_user:id(),
    continue = true,
    wait_map = #{} :: map(),
    wait_pids = #{} :: map(),
    last_message_timestamp :: erlang:timestamp()
}).
-type state() :: #state{}.

-define(PACKET_VALUE, 4).

%% API
-export([init/2, upgrade/4, upgrade/5, takeover/7]).

%% ====================================================================
%% Cowboy API functions
%% ====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Cowboy handler callback - causes the upgrade callback to be called.
%% @end
%%--------------------------------------------------------------------
-spec init(cowboy_req:req(), any()) -> {?MODULE, cowboy_req:req(), any()}.
init(Req, Opts) ->
    {?MODULE, Req, Opts}.


%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback - handle upgrade protocol request.
%% If correct initiate protocol switch, otherwise respond with either 426 or 400.
%% @end
%%--------------------------------------------------------------------
-spec upgrade(cowboy_req:req(), cowboy_middleware:env(), module(), any()) ->
    {ok, cowboy_req:req(), cowboy_middleware:env()} | {stop, cowboy_req:req()}.
upgrade(Req, Env, Handler, HandlerState) ->
    upgrade(Req, Env, Handler, HandlerState, #{}).

-spec upgrade(cowboy_req:req(), cowboy_middleware:env(), module(), any(), any()) ->
    {ok, cowboy_req:req(), cowboy_middleware:env()} | {stop, cowboy_req:req()}.
upgrade(Req, Env, _Handler, HandlerOpts, _Opts) ->
    try process_upgrade_request(Req) of
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
%% @doc
%% Cowboy callback - takeover connection process.
%% @end
%%--------------------------------------------------------------------
-spec takeover(pid(), ranch:ref(), inet:socket(), module(), any(), binary(),
    any()) -> no_return().
takeover(_Parent, Ref, Socket, Transport, _Opts, _Buffer, _HandlerState) ->
    ranch:remove_connection(Ref),
    {Ok, Closed, Error} = Transport:messages(),
    State = #state{
        socket = Socket,
        transport = Transport,
        ok = Ok,
        closed = Closed,
        error = Error,
        last_message_timestamp = os:timestamp()
    },
    ok = Transport:setopts(Socket, [binary, {packet, ?PACKET_VALUE}]),
    activate_socket_once(State),

    Interval = async_request_manager:get_processes_check_interval(),
    erlang:send_after(Interval, self(), heartbeat),

    handler_loop(State),
    exit(normal).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes protocol upgrade request, responds with 101 on success or 400 on
%% failure.
%% @end
%%--------------------------------------------------------------------
-spec process_upgrade_request(cowboy_req:req()) -> ok | {error, update_required}.
process_upgrade_request(Req) ->
    ConnTokens = cowboy_req:parse_header(<<"connection">>, Req, []),
    case lists:member(<<"upgrade">>, ConnTokens) of
        false ->
            {error, upgrade_required};
        true ->
            case cowboy_req:parse_header(<<"upgrade">>, Req, []) of
                [<<?CLIENT_PROTOCOL_UPGRADE_NAME>>] -> ok;
                _ -> {error, upgrade_required}
            end
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Loop that receives data from socket and processes it until it is stopped.
%% @end
%%--------------------------------------------------------------------
-spec handler_loop(state()) -> state().
handler_loop(State) ->
    NewState = receive Msg ->
        try
            handle_info(Msg, State)
        catch Type:Reason ->
            ?error_stacktrace("Unexpected error in protocol server - ~p:~p", [
                Type, Reason
            ]),
            State#state{continue = false}
        end
    after
        ?PROTO_CONNECTION_TIMEOUT ->
            % Should not appear (heartbeats)
            ?error("Connection ~p timeout", [State#state.socket]),
            State#state{continue = false}
    end,
    case NewState#state.continue of
        true ->
            handler_loop(NewState);
        false ->
            case NewState#state.session_id of
                undefined -> ok;
                SessId -> session_connections:remove_connection(SessId, self())
            end,
            % Fulfill remaining promises before shutdown
            fulfill_pending_promises(NewState#state{continue = true})
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles messages sent to this process.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(term(), state()) -> state().
handle_info({send_sync, From, #server_message{} = ServerMsg}, State) ->
    {Result, NewState} = send_server_message(State, ServerMsg),
    From ! {result, Result},
    NewState#state{last_message_timestamp = os:timestamp()};
handle_info({send_async, #server_message{} = ServerMsg}, State) ->
    {_, NewState} = send_server_message(State, ServerMsg),
    NewState#state{last_message_timestamp = os:timestamp()};
handle_info({Ok, Socket, Data}, State = #state{socket = Socket, ok = Ok}) ->
    activate_socket_once(State),
    State2 = handle_client_message(State, Data),
    State2#state{last_message_timestamp = os:timestamp()};

handle_info({Closed, _}, State = #state{closed = Closed}) ->
    State#state{continue = false};

handle_info({Error, Socket, Reason}, State = #state{error = Error}) ->
    ?warning("Connection ~p error: ~p", [Socket, Reason]),
    State#state{continue = false};

handle_info(disconnect, State) ->
    State#state{continue = false};

handle_info(heartbeat, #state{wait_map = WaitMap, wait_pids = Pids,
    last_message_timestamp = LMT} = State) ->
    TimeoutFun = fun(Id) ->
        send_server_message(State, async_request_manager:get_heartbeat_msg(Id))
    end,
    ErrorFun = fun(Id) ->
        send_server_message(State, async_request_manager:get_error_msg(Id))
    end,
    {Pids2, WaitMap2} = async_request_manager:check_processes(Pids, WaitMap, TimeoutFun, ErrorFun),

    Interval = async_request_manager:get_processes_check_interval(),
    erlang:send_after(Interval, self(), heartbeat),

    Continue = case maps:size(WaitMap2) of
        0 ->
            Diff = timer:now_diff(os:timestamp(), LMT),
            case Diff > ?PROTO_CONNECTION_TIMEOUT * 1000 of
                true ->
                    ?info("Connection ~p timeout", [State#state.socket]),
                    false;
                _ ->
                    true
            end;
        _ ->
            true
    end,
    State#state{continue = Continue, wait_map = WaitMap2, wait_pids = Pids2};

handle_info(Info, State = #state{wait_map = WaitMap, wait_pids = Pids}) ->
    case async_request_manager:process_ans(Info, WaitMap, Pids) of
        wrong_message ->
            ?log_bad_request(Info),
            State#state{continue = false};
        {Return, WaitMap2, Pids2} ->
            % Result is ignored as there is no possible fallback if
            % send_server_message fails (the connection will close then).
            {_, NewState} = send_server_message(State, Return),
            NewState#state{last_message_timestamp = os:timestamp(),
                wait_map = WaitMap2, wait_pids = Pids2}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends #server_message via given socket.
%% @end
%%--------------------------------------------------------------------
-spec send_server_message(state(), #server_message{}) -> {Result :: ok | {error, term()}, state()}.
send_server_message(State = #state{session_id = SessionId, transport = Transport, socket = Socket}, ServerMsg) ->
    try serializator:serialize_server_message(ServerMsg) of
        {ok, Data} ->
            case Transport:send(Socket, Data) of
                ok ->
                    {ok, State};
                {error, _} ->
                    session_connections:remove_connection(SessionId, self()),
                    Result = send_via_other_connection(State, ServerMsg),
                    % Terminate as the connection was closed
                    {Result, State#state{continue = false}}
            end
    catch
        _:Reason ->
            ?error_stacktrace("Unable to serialize server_message ~p due to: ~p", [
                ServerMsg, Reason
            ]),
            {{error, cannot_send_message}, State#state{continue = false}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle usual client data, it is decoded and passed to subsequent handler
%% functions
%% @end
%%--------------------------------------------------------------------
-spec handle_client_message(state(), binary()) -> state().
handle_client_message(State, ?CLIENT_KEEPALIVE_MSG) ->
    State;
handle_client_message(State = #state{session_id = SessId}, Data) ->
    try serializator:deserialize_client_message(Data, SessId) of
        {ok, Msg} when SessId == undefined ->
            handle_handshake(State, Msg);
        {ok, Msg} ->
            handle_normal_message(State, Msg#client_message{session_id = SessId})
    catch Type:Reason ->
        ?warning_stacktrace("Client message decoding error - ~p:~p", [
            Type, Reason
        ]),
        State#state{continue = false}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle client handshake_request, it is necessary to authenticate
%% and obtain session
%% @end
%%--------------------------------------------------------------------
-spec handle_handshake(state(), #client_message{}) -> state().
handle_handshake(#state{socket = Socket} = State, ClientMsg) ->
    try
        #client_message{message_body = HandshakeMsg} = ClientMsg,
        {ok, {IpAddress, _Port}} = ssl:peername(Socket),
        {PeerId, SessionId} = auth_manager:handle_handshake(
            HandshakeMsg, IpAddress
        ),
        NewState = State#state{peer_id = PeerId, session_id = SessionId},
        % Result is ignored as there is no possible fallback if
        % send_server_message fails (the connection will close then).
        {_, EndState} = send_server_message(NewState, #server_message{
            message_body = #handshake_response{status = 'OK'}
        }),
        EndState
    catch Type:Reason ->
        ?debug_stacktrace("Invalid handshake request - ~p:~p", [
            Type, Reason
        ]),
        send_server_message(State,
            auth_manager:get_handshake_error(Reason)),
        State#state{continue = false}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle normal incoming message.
%% @end
%%--------------------------------------------------------------------
-spec handle_normal_message(state(), #client_message{}) -> state().
handle_normal_message(State = #state{peer_id = ProviderId,
    wait_map = WaitMap, wait_pids = Pids}, Msg) ->
    case Msg of
        %% If message comes from provider and proxy session is requested - proceed
        %% with authorization and switch context to the proxy session.
        #client_message{proxy_session_id = ProxySessionId, proxy_session_auth = Auth}
            when ProxySessionId =/= undefined ->
            {ok, _} = session_manager:reuse_or_create_proxy_session(ProxySessionId, ProviderId, Auth, fuse);
        _ ->
            ok
    end,

    case router:route_message(Msg) of
        ok ->
            State;
        {ok, ServerMsg} ->
            % Result is ignored as there is no possible fallback if
            % send_server_message fails (the connection will close then).
            {_, NewState} = send_server_message(State, ServerMsg),
            NewState;
        {wait, Delegation} ->
            {WaitMap2, Pids2} = async_request_manager:save_delegation(Delegation, WaitMap, Pids),
            State#state{wait_map = WaitMap2, wait_pids = Pids2};
        {error, Reason} ->
            ?warning("Message ~p handling error: ~p", [Msg, Reason]),
            State#state{continue = false}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tries to fulfill all pending promises - requests that are still being
%% processed by the cluster.
%% @end
%%--------------------------------------------------------------------
-spec fulfill_pending_promises(state()) -> state().
fulfill_pending_promises(#state{wait_map = WaitMap} = State) when map_size(WaitMap) =:= 0 ->
    % No more promises, flush all waiting messages and finish
    receive
        {send_sync, From, _} ->
            From ! {result, {error, closed}},
            fulfill_pending_promises(State);
        _ ->
            fulfill_pending_promises(State)
    after 5000 ->
        State
    end;
fulfill_pending_promises(#state{wait_map = WaitMap, wait_pids = Pids} = State) ->
    NewState = try
        receive
            {send_sync, From, _} ->
                From ! {result, {error, closed}},
                State;
            {send_async, _} ->
                % Ignore when shutting down
                State;
            heartbeat ->
                handle_info(heartbeat, State);
            Info ->
                case async_request_manager:process_ans(Info, WaitMap, Pids) of
                    wrong_message ->
                        ?warning("Discarding message received on connection shutdown: ~p", [Info]),
                        State;
                    {Return, WaitMap2, Pids2} ->
                        % Send and ignore result - if it fails, there is no further fallback here
                        send_via_other_connection(State, Return),
                        State#state{last_message_timestamp = os:timestamp(),
                            wait_map = WaitMap2, wait_pids = Pids2}
                end
        after
            ?PROTO_CONNECTION_TIMEOUT ->
                % Should not appear (heartbeats)
                ?error("Connection ~p timeout", [State#state.socket]),
                State#state{continue = false}
        end
    catch Type:Reason ->
        ?error_stacktrace("Unexpected error in protocol server - ~p:~p", [
            Type, Reason
        ]),
        State#state{continue = false}
    end,
    case NewState#state.continue of
        true -> fulfill_pending_promises(NewState);
        false -> NewState
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tries to send a message using another connection within current session.
%% @end
%%--------------------------------------------------------------------
-spec send_via_other_connection(state(), #server_message{}) -> ok | {error, term()}.
send_via_other_connection(#state{session_id = SessionId}, ServerMsg) ->
    {ok, Connections} = session_connections:get_connections(SessionId),
    % Try to send the message via another connection of this session. This must
    % be delegated to an asynchronous process so the connection process can
    % still receive messages in the meantime. Otherwise, a deadlock is possible
    % if two connections try to send a message via each other.
    Parent = self(),
    Ref = make_ref(),
    spawn(fun() ->
        Result = lists:foldl(fun(Pid, ResultAcc) ->
            case ResultAcc of
                ok ->
                    ok;
                _ ->
                    try
                        case session_utils:is_provider_session_id(SessionId) of
                            true ->
                                provider_communicator:send(ServerMsg, Pid);
                            false ->
                                communicator:send_to_client(ServerMsg, Pid)
                        end
                    catch _:_ ->
                        {error, closed}
                    end
            end
        end, {error, closed}, Connections -- [self()]),
        Parent ! {Ref, Result}
    end),
    WaitForResult = fun Fun() ->
        receive
            {Ref, Result} ->
                Result;
            {send_sync, From, _} ->
                From ! {result, {error, closed}},
                Fun();
            Other ->
                % Defer other messages while waiting for the async process
                erlang:send_after(500, self(), Other),
                Fun()
        after
            ?PROTO_CONNECTION_TIMEOUT ->
                {error, timeout}
        end
    end,
    WaitForResult().


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Activate socket for next message, so it will be sent to the handling process
%% via erlang message.
%% @end
%%--------------------------------------------------------------------
-spec activate_socket_once(state()) -> ok.
activate_socket_once(#state{transport = Transport, socket = Socket}) ->
    ok = Transport:setopts(Socket, [{active, once}]).