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
-include("http/http_common.hrl").
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
    peer_type = unknown :: unknown | fuse_client | provider,
    peer_id = undefined :: undefined | od_user:id() | od_provider:id(),
    continue = true
}).
-type state() :: #state{}.

-define(PACKET_VALUE, 4).

%% API
-export([init/3, upgrade/4]).

%% ====================================================================
%% Cowboy API functions
%% ====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Cowboy handler callback - causes the upgrade callback to be called.
%% @end
%%--------------------------------------------------------------------
-spec init({tcp | ssl | atom(), http | atom()}, cowboy_req:req(), any()) ->
    {upgrade, protocol, ?MODULE}.
init(_Protocol, _Req, _Opts) ->
    {upgrade, protocol, ?MODULE}.


%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback - upgrades protocol and sends a HTTP 101 Switching Protocol
%% response to the client.
%% Initialized from client_protocol_handler module.
%% @end
%%--------------------------------------------------------------------
-spec upgrade(cowboy_req:req(), cowboy_middleware:env(), module(), any()) ->
    {halt, cowboy_req:req()} | no_return().
upgrade(Req, Env, _Handler, _HandlerOpts) ->
    case process_upgrade_request(Req, Env) of
        error -> {halt, Req};
        {ok, Req2} -> init_handler_loop(Req2)
    end.

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
-spec process_upgrade_request(cowboy_req:req(), cowboy_middleware:env()) ->
    {ok, cowboy_req:req()} | error.
process_upgrade_request(Req, Env) ->
    try
        {_, Ref} = lists:keyfind(listener, 1, Env),
        ranch:remove_connection(Ref),
        {ok, ConnTokens, Req2} = cowboy_req:parse_header(<<"connection">>, Req),
        true = lists:member(<<"upgrade">>, ConnTokens),
        {ok, [<<?client_protocol_upgrade_name>>], _} = cowboy_req:parse_header(<<"upgrade">>, Req2),
        {ok, Req3} = cowboy_req:upgrade_reply(101, [
            {<<"Upgrade">>, <<?client_protocol_upgrade_name>>}
        ], Req2),
        %% Flush the resp_sent message before moving on.
        receive {cowboy_req, resp_sent} -> ok after 0 -> ok end,
        {ok, Req3}
    catch Type:Reason ->
        ?debug_stacktrace("Invalid protocol upgrade request - ~p:~p", [
            Type, Reason
        ]),
        receive
            {cowboy_req, resp_sent} -> ok
        after 0 ->
            _ = cowboy_req:reply(400, Req)
        end,
        error
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the handler loop by configuring socket opts and enters the loop.
%% @end
%%--------------------------------------------------------------------
-spec init_handler_loop(cowboy_req:req()) -> {halt, cowboy_req:req()}.
init_handler_loop(Req) ->
    [Socket, Transport] = cowboy_req:get([socket, transport], Req),
    {Ok, Closed, Error} = Transport:messages(),
    State = #state{
        socket = Socket,
        transport = Transport,
        ok = Ok,
        closed = Closed,
        error = Error,
        peer_type = unknown
    },
    ok = Transport:setopts(Socket, [binary, {packet, ?PACKET_VALUE}]),
    activate_socket_once(State),
    NewState = handler_loop(State),
    case NewState#state.session_id of
        undefined -> ok;
        SessId -> session:remove_connection(SessId, self())
    end,
    {halt, Req}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Loop that receives data from socket and processes it until it is stopped.
%% @end
%%--------------------------------------------------------------------
-spec handler_loop(state()) -> state().
handler_loop(State) ->
    NewState = try
        receive
            Msg ->
                handle_info(Msg, State)
        after
            ?PROTO_CONNECTION_TIMEOUT ->
                ?warning("Connection ~p timeout", [State#state.socket]),
                State#state{continue = false}
        end
    catch Type:Reason ->
        ?error_stacktrace("Unxpected error in protocol server - ~p:~p", [
            Type, Reason
        ]),
        State
    end,
    case NewState#state.continue of
        true -> handler_loop(NewState);
        false -> NewState
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles messages sent to this process.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(term(), state()) -> state().
handle_info({send_sync, From, #server_message{} = ServerMsg}, State) ->
    send_server_message(State, ServerMsg),
    From ! {result, ok},
    State;
handle_info({send_async, #server_message{} = ServerMsg}, State) ->
    send_server_message(State, ServerMsg),
    State;
handle_info({Ok, Socket, Data}, State = #state{socket = Socket, ok = Ok}) ->
    activate_socket_once(State),
    handle_client_message(State, Data);

handle_info({Closed, _}, State = #state{closed = Closed}) ->
    State#state{continue = false};

handle_info({Error, Socket, Reason}, State = #state{error = Error}) ->
    ?warning("Connection ~p error: ~p", [Socket, Reason]),
    State#state{continue = false};

handle_info(disconnect, State) ->
    State#state{continue = false};

handle_info(_Info, State) ->
    ?log_bad_request(_Info),
    State#state{continue = false}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends #server_message via given socket.
%% @end
%%--------------------------------------------------------------------
-spec send_server_message(state(), #server_message{}) -> ok.
send_server_message(#state{transport = Transport, socket = Socket}, ServerMsg) ->
    try serializator:serialize_server_message(ServerMsg) of
        {ok, Data} ->
            ok = Transport:send(Socket, Data)
    catch
        _:Reason ->
            ?error_stacktrace("Unable to serialize server_message ~p due to: ~p", [
                ServerMsg, Reason
            ]),
            ok
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle usual client data, it is decoded and passed to subsequent handler
%% functions
%% @end
%%--------------------------------------------------------------------
-spec handle_client_message(state(), binary()) -> state().
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
handle_handshake(State, ClientMsg) ->
    try
        #client_message{message_body = HandshakeMsg} = ClientMsg,
        NewState = case HandshakeMsg of
            #client_handshake_request{} ->
                {UserId, SessionId} = fuse_auth_manager:handle_handshake(HandshakeMsg),
                put(session_id, SessionId),
                State#state{peer_type = fuse_client, peer_id = UserId, session_id = SessionId};
            #provider_handshake_request{} ->
                {ProviderId, SessionId} = provider_auth_manager:handle_handshake(HandshakeMsg),
                put(session_id, SessionId),
                State#state{peer_type = provider, peer_id = ProviderId, session_id = SessionId}
        end,
        send_server_message(State, #server_message{
            message_body = #handshake_response{status = 'OK'}
        }),
        NewState
    catch Type:Reason ->
        ?debug_stacktrace("Invalid handshake request - ~p:~p", [
            Type, Reason
        ]),
        report_handshake_error(State, Reason),
        State#state{continue = false}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends a server message with the handshake error details.
%% @end
%%--------------------------------------------------------------------
-spec report_handshake_error(state(), Error :: term()) -> ok.
report_handshake_error(State, incompatible_client_version) ->
    send_server_message(State, #server_message{
        message_body = #handshake_response{
            status = 'INCOMPATIBLE_VERSION'
        }
    });
report_handshake_error(State, invalid_token) ->
    send_server_message(State, #server_message{
        message_body = #handshake_response{
            status = 'INVALID_TOKEN'
        }
    });
report_handshake_error(State, invalid_provider) ->
    send_server_message(State, #server_message{
        message_body = #handshake_response{
            status = 'INVALID_PROVIDER'
        }
    });
report_handshake_error(State, invalid_nonce) ->
    send_server_message(State, #server_message{
        message_body = #handshake_response{
            status = 'INVALID_NONCE'
        }
    });
report_handshake_error(State, {badmatch, {error, Error}}) ->
    report_handshake_error(State, Error);
report_handshake_error(State, {Code, Error, _Description}) when is_integer(Code) ->
    send_server_message(State, #server_message{
        message_body = #handshake_response{
            status = translator:translate_handshake_error(Error)
        }
    });
report_handshake_error(State, _) ->
    send_server_message(State, #server_message{
        message_body = #handshake_response{
            status = 'INTERNAL_SERVER_ERROR'
        }
    }).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handle normal incoming message.
%% @end
%%--------------------------------------------------------------------
-spec handle_normal_message(state(), #client_message{} | #server_message{}) -> state().
handle_normal_message(State = #state{session_id = SessId, peer_type = PeerType, peer_id = ProviderId}, Msg0) ->
    {Msg, EffectiveSessionId} = case {PeerType, Msg0} of
        %% If message comes from provider and proxy session is requested - proceed
        %% with authorization and switch context to the proxy session.
        {provider, #client_message{proxy_session_id = ProxySessionId, proxy_session_auth = Auth}}
            when ProxySessionId =/= undefined, Auth =/= undefined ->
            {ok, _} = session_manager:reuse_or_create_proxy_session(ProxySessionId, ProviderId, Auth, fuse),
            {Msg0, ProxySessionId};
        _ ->
            {Msg0, SessId}
    end,

    case Msg of
        %% Remote proxy session has received message which is now to be routed as proxy message.
        #client_message{proxy_session_id = TargetSessionId} = Msg when TargetSessionId =/= EffectiveSessionId, is_binary(TargetSessionId) ->
            router:route_proxy_message(Msg, TargetSessionId),
            State;
        _ -> %% Non-proxy case
            case router:preroute_message(Msg, EffectiveSessionId) of
                ok ->
                    State;
                {ok, ServerMsg} ->
                    send_server_message(State, ServerMsg),
                    State;
                {error, Reason} ->
                    ?warning("Message ~p handling error: ~p", [Msg, Reason]),
                    State#state{continue = false}
            end
    end.


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
