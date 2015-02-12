%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles client and provider requests
%%% @end
%%%-------------------------------------------------------------------
-module(protocol_handler).
-author("Tomasz Lichon").

-behaviour(ranch_protocol).
-behaviour(gen_server).

-include("workers/datastore/models/session.hrl").
-include_lib("proto_internal/oneclient/server_messages.hrl").
-include_lib("proto_internal/oneclient/handshake_messages.hrl").
-include_lib("proto_internal/oneproxy/oneproxy_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/4, init/4, call/2, cast/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(sock_state, {
    % handler responses
    ok :: atom(),
    closed :: atom(),
    error :: atom(),
    % actual connection state
    socket :: port(),
    transport :: module(),
    certificate_info :: #certificate_info{},
    session_id :: session_id(),
    sequencer_manager :: pid()
}).

-define(TIMEOUT, timer:minutes(1)).
-define(PACKET_VALUE, 4).
%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv gen_server:call(Pid, Req).
%% @end
%%--------------------------------------------------------------------
-spec call(Pid :: pid(), Req :: term()) -> ok | {error, term()}.
call(Pid, Req) ->
    gen_server:call(Pid, Req).

%%--------------------------------------------------------------------
%% @doc
%% @equiv gen_server:cast(Pid, Req).
%% @end
%%--------------------------------------------------------------------
-spec cast(Pid :: pid(), Req :: term()) -> ok.
cast(Pid, Req) ->
    gen_server:cast(Pid, Req).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server.
%% @end
%%--------------------------------------------------------------------
-spec start_link(Ref :: atom(), Socket :: term(), Transport :: atom(), Opts :: list()) ->
    {ok, Pid :: pid()}.
start_link(Ref, Socket, Transport, Opts) ->
    proc_lib:start_link(?MODULE, init, [Ref, Socket, Transport, Opts]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term(), Socket :: port(), Transport :: atom(), Opts :: list()) ->
    no_return().
init(Ref, Socket, Transport, _Opts = []) ->
    ok = proc_lib:init_ack({ok, self()}),
    ok = ranch:accept_ack(Ref),
    ok = Transport:setopts(Socket, [{active, once}, {packet, ?PACKET_VALUE}]),
    {Ok, Closed, Error} = Transport:messages(),
    gen_server:enter_loop(?MODULE, [], #sock_state{
        socket = Socket,
        transport = Transport,
        ok = Ok,
        closed = Closed,
        error = Error
    }, ?TIMEOUT).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is never called. We only define it so that
%% we can use the -behaviour(gen_server) attribute. Init is handled by ranch
%% init/4 function
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
    State :: #sock_state{}) ->
    {reply, Reply :: term(), NewState :: #sock_state{}} |
    {reply, Reply :: term(), NewState :: #sock_state{}, timeout() | hibernate} |
    {noreply, NewState :: #sock_state{}} |
    {noreply, NewState :: #sock_state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #sock_state{}} |
    {stop, Reason :: term(), NewState :: #sock_state{}}.
handle_call({send, ServerMsg}, _From, State = #sock_state{socket = Socket,
    transport = Transport}) ->
    Ans = send_server_message(Socket, Transport, ServerMsg),
    {reply, Ans, State};

handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, wrong_request, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: #sock_state{}) ->
    {noreply, NewState :: #sock_state{}} |
    {noreply, NewState :: #sock_state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #sock_state{}}.
handle_cast({send, ServerMsg}, State = #sock_state{socket = Socket,
    transport = Transport}) ->
    send_server_message(Socket, Transport, ServerMsg),
    {noreply, State};

handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | {Ok :: atom(), Socket :: port(),
    Data :: binary()} | term(), State :: #sock_state{}) ->
    {noreply, NewState :: #sock_state{}} |
    {noreply, NewState :: #sock_state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #sock_state{}}.
handle_info({Ok, Socket, Data}, State = #sock_state{socket = Socket, ok = Ok,
    transport = Transport, certificate_info = undefined}) ->
    activate_socket_once(Socket, Transport),
    handle_oneproxy_certificate_info_message(State, Data);

handle_info({Ok, Socket, Data}, State = #sock_state{socket = Socket, ok = Ok,
    transport = Transport}) ->
    activate_socket_once(Socket, Transport),
    handle_client_message(State, Data);

handle_info({Closed, _}, State = #sock_state{closed = Closed}) ->
    {stop, normal, State};

handle_info({Error, Socket, Reason}, State = #sock_state{error = Error}) ->
    ?warning("Connection ~p error: ~p", [Socket, Reason]),
    {stop, Reason, State};

handle_info(timeout, State = #sock_state{socket = Socket}) ->
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
    State :: #sock_state{}) -> term().
terminate(_Reason, #sock_state{session_id = SessionId}) ->
    session:remove_connection(SessionId, self()),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: #sock_state{},
    Extra :: term()) -> {ok, NewState :: #sock_state{}} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handle first message after opening connection - information from
%% oneproxy about peer certificate
%% @end
%%--------------------------------------------------------------------
-spec handle_oneproxy_certificate_info_message(#sock_state{}, binary()) ->
    {noreply, NewState :: #sock_state{}, timeout()} |
    {stop, Reason :: term(), NewState :: #sock_state{}}.
handle_oneproxy_certificate_info_message(State, Data) ->
    try serializator:deserialize_oneproxy_certificate_info_message(Data) of
        {ok, #certificate_info{} = Info} ->
            {noreply, State#sock_state{certificate_info = Info}, ?TIMEOUT}
    catch
        _:Error ->
            ?error_stacktrace("Cannot decode oneproxy CertificateInfo message, error: ~p", [Error]),
            {stop, Error, State}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Handle usual client data, it is decoded and passed to subsequent handler
%% functions
%% @end
%%--------------------------------------------------------------------
-spec handle_client_message(#sock_state{}, binary()) ->
    {noreply, NewState :: #sock_state{}, timeout()} |
    {stop, Reason :: term(), NewState :: #sock_state{}}.
handle_client_message(State = #sock_state{session_id = SessionId}, Data) ->
    try serializator:deserialize_client_message(Data, SessionId) of
        {ok, Msg} when SessionId == undefined ->
            handle_handshake(State, Msg);
        {ok, Msg} ->
            handle_normal_message(State, Msg)
    catch
        _:Error ->
            ?warning_stacktrace("Client message decoding error: ~p", [Error]),
            {stop, Error, State}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Handle client handshake_request, it is necessary to authenticate
%% and obtain session
%% @end
%%--------------------------------------------------------------------
-spec handle_handshake(#sock_state{}, #certificate_info{}) ->
    {noreply, NewState :: #sock_state{}, timeout()} |
    {stop, Reason :: term(), NewState :: #sock_state{}}.
handle_handshake(State =
    #sock_state{certificate_info = Cert, socket = Sock, transport = Transp}, Msg) ->
    try client_auth:handle_handshake(Msg, Cert) of
        {ok, Response = #server_message{server_message =
        #handshake_response{session_id = NewSessionId}}} ->
            send_server_message(Sock, Transp, Response),
            {noreply, State#sock_state{session_id = NewSessionId}, ?TIMEOUT}
    catch
        _:Error ->
            ?warning_stacktrace("Handshake ~p, error ~p", [Msg, Error]),
            {stop, Error, State}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Handle nomal client_message
%% @end
%%--------------------------------------------------------------------
-spec handle_normal_message(#sock_state{}, tuple()) ->
    {noreply, NewState :: #sock_state{}, timeout()} |
    {stop, Reason :: term(), NewState :: #sock_state{}}.
handle_normal_message(State =
    #sock_state{sequencer_manager = SeqMan, socket = Sock, transport = Transp}, Msg) ->
    case router:preroute_message(SeqMan, Msg) of
        ok ->
            {noreply, State, ?TIMEOUT};
        {_, Response = #server_message{}} ->
            send_server_message(Sock, Transp, Response),
            {noreply, State, ?TIMEOUT};
        {error, Reason} ->
            ?warning("Message ~p handling error: ~p", [Msg, Reason]),
            {stop, {error, Reason}, State}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Activate socket for next message, so it will be send to handling process
%% via erlang message
%% @end
%%--------------------------------------------------------------------
-spec activate_socket_once(Socket :: port(), Transport :: module()) -> ok.
activate_socket_once(Socket, Transport) ->
    ok = Transport:setopts(Socket, [{active, once}]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Activate socket for next message, so it will be send to handling process
%% via erlang message
%% @end
%%--------------------------------------------------------------------
-spec send_server_message(Socket :: port(), Transport :: module(),
    ServerMessage :: tuple()) -> ok | {error, term()}.
send_server_message(Socket, Transport, ServerMsg) ->
    try serializator:serialize_server_message(ServerMsg) of
        {ok, Data} ->
            ok = Transport:send(Socket, Data)
    catch
        _:Error ->
            ?error_stacktrace("Connection ~p, message ~p encoding error: ~p",
                [Socket, ServerMsg, Error]),
            Error
    end.
