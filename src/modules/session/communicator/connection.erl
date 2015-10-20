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
-include("proto/oneclient/handshake_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/4, init/4, send/2, send_async/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(sock_state, {
    certificate :: #'OTPCertificate'{},
    % handler responses
    ok :: atom(),
    closed :: atom(),
    error :: atom(),
    % actual connection state
    socket :: ssl2:socket(),
    transport :: module(),
    session_id :: session:id()
}).

-define(TIMEOUT, timer:minutes(1)).
-define(PACKET_VALUE, 4).
%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Send server_message to client, returns sending result info.
%% @equiv gen_server:call(Pid, {send, Req}).
%% @end
%%--------------------------------------------------------------------
-spec send(Pid :: pid(), Req :: #server_message{}) -> ok | {error, term()}.
send(Pid, Req) ->
    gen_server:call(Pid, {send, Req}).

%%--------------------------------------------------------------------
%% @doc
%% Send server_message to client, returns 'ok'.
%% @equiv gen_server:cast(Pid, {send, Req}).
%% @end
%%--------------------------------------------------------------------
-spec send_async(Pid :: pid(), Req :: #server_message{}) -> ok.
send_async(Pid, Req) ->
    gen_server:cast(Pid, {send, Req}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server.
%% @end
%%--------------------------------------------------------------------
-spec start_link(Ref :: atom(), Socket :: ssl2:socket(), Transport :: atom(), Opts :: list()) ->
    {ok, Pid :: pid()}.
start_link(Ref, Socket, Transport, Opts) ->
    proc_lib:start_link(?MODULE, init, [Ref, Socket, Transport, Opts]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term(), Socket :: ssl2:socket(), Transport :: atom(), Opts :: list()) ->
    no_return().
init(Ref, Socket, Transport, _Opts = []) ->
    ok = proc_lib:init_ack({ok, self()}),
    ok = ranch:accept_ack(Ref),
    ok = Transport:setopts(Socket, [binary, {active, once}, {packet, ?PACKET_VALUE}]),
    {Ok, Closed, Error} = Transport:messages(),
    Certificate =
        case ssl2:peercert(Socket) of
            {error, _} -> undefined;
            {ok, Der} -> public_key:pkix_decode_cert(Der, otp)
        end,

    gen_server:enter_loop(?MODULE, [], #sock_state{
        socket = Socket,
        transport = Transport,
        ok = Ok,
        closed = Closed,
        error = Error,
        certificate = Certificate
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
    send_server_message(Socket, Transport, ServerMsg),
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
-spec handle_info(Info :: timeout() | {Ok :: atom(), Socket :: ssl2:socket(),
    Data :: binary()} | term(), State :: #sock_state{}) ->
    {noreply, NewState :: #sock_state{}} |
    {noreply, NewState :: #sock_state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #sock_state{}}.
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
terminate(Reason, #sock_state{session_id = Id, socket = Socket} = State) ->
    ?log_terminate(Reason, State),
    ssl2:close(Socket),
    catch communicator:remove_connection(Id, self()),
    ssl2:close(State#sock_state.socket),
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
%% @private
%% @doc
%% Handle usual client data, it is decoded and passed to subsequent handler
%% functions
%% @end
%%--------------------------------------------------------------------
-spec handle_client_message(#sock_state{}, binary()) ->
    {noreply, NewState :: #sock_state{}, timeout()} |
    {stop, Reason :: term(), NewState :: #sock_state{}}.
handle_client_message(State = #sock_state{session_id = SessId}, Data) ->
    try serializator:deserialize_client_message(Data, SessId) of
        {ok, Msg} when SessId == undefined ->
            handle_handshake(State, Msg);
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
-spec handle_handshake(#sock_state{}, #client_message{}) ->
    {noreply, NewState :: #sock_state{}, timeout()} |
    {stop, Reason :: term(), NewState :: #sock_state{}}.
handle_handshake(State = #sock_state{certificate = Cert, socket = Sock,
    transport = Transp}, Msg) ->
    try fuse_auth_manager:handle_handshake(Msg, Cert) of
        {ok, Response = #server_message{message_body =
        #handshake_response{session_id = NewSessId}}} ->
            send_server_message(Sock, Transp, Response),
            {noreply, State#sock_state{session_id = NewSessId}, ?TIMEOUT}
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
-spec handle_normal_message(#sock_state{}, #client_message{}) ->
    {noreply, NewState :: #sock_state{}, timeout()} |
    {stop, Reason :: term(), NewState :: #sock_state{}}.
handle_normal_message(State = #sock_state{session_id = SessId,
    socket = _Sock, transport = _Transp}, Msg) ->
    case router:preroute_message(Msg, SessId) of
        ok ->
            {noreply, State, ?TIMEOUT}
%%         {error, Reason} ->
%%             ?warning("Message ~p handling error: ~p", [Msg, Reason]),
%%             {stop, {error, Reason}, State}
    end.

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
%% Activate socket for next message, so it will be send to handling process
%% via erlang message
%% @end
%%--------------------------------------------------------------------
-spec send_server_message(Socket :: ssl2:socket(), Transport :: module(),
    ServerMessage :: #server_message{}) -> ok.
send_server_message(Socket, Transport, ServerMsg) ->
    {ok, Data} = serializator:serialize_server_message(ServerMsg),
    ok = Transport:send(Socket, Data).

