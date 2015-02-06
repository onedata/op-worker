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

-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/4]).
-export([init/4]).

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
    credentials = undefined :: binary()
}).

-define(TIMEOUT, timer:minutes(1)).
-define(PACKET_VALUE, 4).
%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server.
%% @end
%%--------------------------------------------------------------------
-spec start_link(Ref :: atom(), Socket :: term(), Transport :: atom(), Opts :: list()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
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
handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: #sock_state{}) ->
    {noreply, NewState :: #sock_state{}} |
    {noreply, NewState :: #sock_state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #sock_state{}}.
handle_info({Ok, Socket, Data}, State = #sock_state{socket = Socket, transport = Transport, ok = Ok, credentials = undefined}) ->
    activate_socket_once(Socket, Transport),
    case client_auth:handle_auth_info(Data) of
        {ok, ClientId} ->
            {noreply, State#sock_state{credentials = ClientId}, ?TIMEOUT};
        Error ->
            ?warning_stacktrace("Handling auth_info for connection ~p error: ~p", [Socket, Error]),
            {stop, Error, State}
    end;

handle_info({Ok, Socket, Data}, State = #sock_state{socket = Socket, transport = Transport, ok = Ok}) ->
    activate_socket_once(Socket, Transport),
    ok = Transport:send(Socket, Data),
    {noreply, State, ?TIMEOUT};

handle_info({Closed, Socket}, State = #sock_state{closed = Closed}) ->
    ?info("Connection ~p closed", [Socket]),
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
terminate(_Reason, _State) ->
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
%% Activate socket for next message, so it will be send to handling process
%% via erlang message
%% @end
%%--------------------------------------------------------------------
-spec activate_socket_once(Socket :: port(), Transport :: module()) -> ok.
activate_socket_once(Socket, Transport) ->
    ok = Transport:setopts(Socket, [{active, once}]).