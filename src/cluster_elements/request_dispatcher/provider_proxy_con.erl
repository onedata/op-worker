%%%-------------------------------------------------------------------
%%% @author RoXeon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Jul 2014 00:12
%%%-------------------------------------------------------------------
-module(provider_proxy_con).
-author("RoXeon").


-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("remote_file_management_pb.hrl").
-include_lib("ctool/include/logging.hrl").

-export([
    init/2,
    websocket_handle/3,
    websocket_info/3,
    websocket_terminate/3,
    connect/3
]).

-record(ppcon_state, {msg_id = 0, connections = #{}, inbox = #{}}).

%% API
-export([ensure_running/0, get_msg_id/0, send/3]).


ensure_running() ->
    case whereis(ppcon) of
        undefined ->
            Pid = spawn(fun main_loop/0),
            register(ppcon, Pid),
            ok;
        _ ->
            ok
    end.

get_msg_id() ->
    ensure_running(),
    exec(get_msg_id).

send(HostName, MsgId, Data) ->
    ensure_running(),
    exec({send, HostName, MsgId, Data}).


main_loop() ->
    main_loop(#ppcon_state{}).
main_loop(#ppcon_state{msg_id = CurrentMsgId, connections = Connections, inbox = Inbox} = State) ->
    NewState =

        receive
            {From, get_msg_id} ->
                From ! {self(), CurrentMsgId},
                State#ppcon_state{msg_id = CurrentMsgId + 1};
            {From, {send, HostName, MsgId, Data} = _Req} ->
                NState1 = case maps:find(HostName, Connections) of
                    error ->
                        case connect(HostName, 5555, [{certfile, gr_plugin:get_cert_path()}, {keyfile, gr_plugin:get_key_path()}]) of
                            {ok, Socket} ->
                                ?info("Connected to ~p", [HostName]),
                                State#ppcon_state{connections = maps:put(HostName, Socket, Connections)};
                            {error, Reason} ->
                                ?error("Cannot connect to ~p due to ~p", [HostName, Reason]),
                                State
                        end;
                    {ok, _} ->
                        State
                end,
                case maps:find(HostName, NState1#ppcon_state.connections) of
                    {ok, Socket1} ->
                        Socket1 ! {send, Data},
                        From ! {self(), ok},
                        NState1#ppcon_state{inbox = maps:put(MsgId, From, Inbox)};
                    error ->
                        From ! {self(), error},
                        NState1
                end;
            {Socket, {closed, Code}} ->
                ValueMap = lists:map(fun({Key, Value}) -> {Value, Key} end, maps:to_list(Connections)),
                HostName = maps:get(Socket, ValueMap, undefined),
                ?info("Connection to ~p closed due to ~p.", [HostName, Code]),
                State#ppcon_state{connections = maps:remove(HostName, Connections)};
            {_Socket, {recv, Data}} ->
                #answer{answer_status = AnswerStatus, worker_answer = WorkerAnswer, message_id = MsgId} = communication_protocol_pb:decode_answer(Data),
                SendTo = maps:get(MsgId, Inbox),
                SendTo ! {response, MsgId, AnswerStatus, WorkerAnswer},
                State

        after 10000 ->
            State
        end,
    main_loop(NewState).


exec(Command) ->
    PPCon = whereis(ppcon),
    PPCon ! {self(), Command},
    receive
        {PPCon, Response} -> Response
    after 10000 ->
        throw(ppcon_timeout)
    end.


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
    ok;
websocket_terminate({_Code, _Payload}, _ConnState, _State) ->
    ok.


%% ====================================================================
%% API functions
%% ====================================================================


%% connect/3
%% ====================================================================
%% @doc Connects to cluster with given host, port and transport options.
%%      Note that some options may conflict with websocket_client's options so don't pass any options but certificate configuration.
%%      Additionally if you pass 'auto_handshake' atom in Opts, handshakeInit/3 and handshakeAck/2 will be called with generic arguments before returning SocketRef.
-spec connect(Host :: string(), Port :: non_neg_integer(), Opts :: [term()]) -> {ok, Socket :: pid()} | {error, timout} | {error, Reason :: any()}.
%% ====================================================================
connect(Host, Port, Opts) when is_atom(Host) ->
    connect(atom_to_list(Host), Port, Opts);
connect(Host, Port, Opts) ->
    erlang:process_flag(trap_exit, true),
    flush_errors(),
    crypto:start(),
    ssl:start(),
    Opts1 = Opts -- [auto_handshake],
    Monitored =
        case websocket_client:start_link("wss://" ++ vcn_utils:ensure_list(Host) ++ ":" ++ integer_to_list(Port) ++ "/veilclient" , ?MODULE, [self()], Opts1 ++ [{reuse_sessions, false}]) of
            {ok, Proc}      -> erlang:monitor(process, Proc), Proc;
            {error, Error}  -> self() ! {error, Error}, ok;
            Error1          -> self() ! {error, Error1}, ok
        end,
    Return =
        receive
            {connected, Monitored}              ->

                {ok, Monitored};
            {error, Other1}                     -> {error, Other1};
            {'DOWN', _, _, Monitored, Info}     -> {error, Info};
            {'EXIT', Monitored, Reason}         -> {error, Reason}
        after 3000 ->
            {error, timeout}
        end,
    Return.


%% ====================================================================
%% Internal functions
%% ====================================================================

flush_errors() ->
    receive
        {error, _} -> flush_errors()
    after 0 ->
        ok
    end.
