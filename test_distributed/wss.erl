%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides simple WebSocket client that allows connecting
%%       to veilcluster with given host
%% @end
%% ===================================================================
-module(wss).

-export([
    init/2,
    websocket_handle/3,
    websocket_info/3,
    websocket_terminate/3,
    connect/3, send/2, recv/2, close/1
]).

%% ====================================================================
%% Behaviour callback functions
%% ====================================================================

init([Pid], _Req) ->
    Pid ! {connected, self()},
    {ok, Pid}.

websocket_handle({binary, Data}, _ConnState, State) ->
    State ! {self(), {recv, Data}},
    {ok, State};
websocket_handle(_, _ConnState, State) ->
    {ok, State}.

websocket_info({send, Data}, _ConnState, State) ->
    {reply, {binary, Data}, State};
websocket_info({close, Payload}, _ConnState, State) ->
    {close, Payload, State}.

websocket_terminate({close, Code, _Payload}, _ConnState, State) ->
    State ! {self(), {closed, Code}},
    ok.


%% ====================================================================
%% API functions
%% ====================================================================

%% Connects to cluster with given host, port and transport options.
%% Note that some options may conflict with websocket_client's options so don't pass any options but certificate configuration.
%% Returns {ok, SocketRef :: pid()} on succes, {error, Reason :: term()} on failure.
connect(Host, Port, Opts) when is_atom(Host) ->
    connect(atom_to_list(Host), Port, Opts);
connect(Host, Port, Opts) ->
    erlang:process_flag(trap_exit, true),
    flush_errors(),
    crypto:start(),
    ssl:start(),
    Monitored =
        case websocket_client:start_link("wss://" ++ Host ++ ":" ++ integer_to_list(Port) ++ "/veilclient" , ?MODULE, [self()], Opts ++ [{reuse_sessions, false}]) of
            {ok, Proc}      -> erlang:monitor(process, Proc), Proc;
            {error, Error}  -> self() ! {error, Error}, ok;
            Error1          -> self() ! {error, Error1}, ok
        end,
    Return =
        receive
            {connected, Monitored}              -> {ok, Monitored};
            {error, Other1}                     -> {error, Other1};
            {'DOWN', _, _, Monitored, Info}     -> {error, Info};
            {'EXIT', Monitored, Reason}         -> {error, Reason}
        after 5000 ->
            {error, timeout}
        end,
    Return.


%% Receives WebSocket frame from given SocketRef (Pid). Timeouts after Timeout.
%% Returns {ok, Data} on success, {error, Reason :: term()} otherwise.
recv(Pid, Timeout) ->
    receive
        {Pid, {recv, Data}} -> {ok, Data};
        {Pid, Other} -> {error, Other}
    after Timeout ->
        {error, timeout}
    end.

%% Sends asynchronously Data over WebSocket. Returns 'ok'.
send(Pid, Data) ->
    Pid ! {send, Data},
    ok.

%% Closes WebSocket connection. Returns 'ok'.
close(Pid) ->
    Pid ! {close, ""},
    ok.


%% ====================================================================
%% Internal functions
%% ====================================================================

flush_errors() ->
    receive
        {error, _} -> flush_errors()
    after 0 ->
        ok
    end.