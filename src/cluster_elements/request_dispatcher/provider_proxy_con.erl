%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides raw inter-provider communication layer (based on WebSocket Secure)
%% @end
%% ===================================================================
-module(provider_proxy_con).
-author("Rafal Slota").


-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("rtcore_pb.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("remote_file_management_pb.hrl").
-include_lib("ctool/include/logging.hrl").

%% websocket_client's callbacks
-export([
    init/2,
    websocket_handle/3,
    websocket_info/3,
    websocket_terminate/3,
    connect/4
]).

%% API
-export([ensure_running/0, get_msg_id/0, send/3, main_loop/1, reset_connection/1, report_ack/1, report_timeout/1]).

%% Connection master's state record
-record(ppcon_state, {msg_id = 0, connections = #{}, inbox = #{}, error_counter = #{}, socket_endpoint = #{}}).

%% ====================================================================
%% API functions
%% ====================================================================


%% ensure_running/0
%% ====================================================================
%% @doc Ensures that inter-provider connection manager is running.
%% @end
-spec ensure_running() -> ok.
%% ====================================================================
ensure_running() ->
    case whereis(ppcon) of
        undefined ->
            Pid = spawn(fun main_loop/0),
            register(ppcon, Pid),
            ok;
        _ ->
            ok
    end.


%% get_msg_id/0
%% ====================================================================
%% @doc Returns first free to use MsgId for inter-provider message.
%% @end
-spec get_msg_id() -> MsgId :: integer().
%% ====================================================================
get_msg_id() ->
    ensure_running(),
    exec(get_msg_id).


%% reset_connection/1
%% ====================================================================
%% @doc Resets connection to selected HostName.
%% @end
-spec reset_connection(HostName :: string()) -> ok.
%% ====================================================================
reset_connection({HostName, Endpoint}) ->
    ensure_running(),
    exec({reset_connection, {HostName, utils:ensure_binary(Endpoint)}});
reset_connection(HostName) ->
    reset_connection({HostName, <<"oneclient">>}).


%% report_timeout/1
%% ====================================================================
%% @doc Report that answer message wasn't successfully delivered. Connection manager will increment its error counter for this HostName.
%% @end
-spec report_timeout(HostName :: string()) -> ok.
%% ====================================================================
report_timeout({HostName, Endpoint}) ->
    ensure_running(),
    exec({report_timeout, {HostName, utils:ensure_binary(Endpoint)}});
report_timeout(HostName) ->
    report_timeout({HostName, <<"oneclient">>}).


%% report_ack/1
%% ====================================================================
%% @doc Report that answer message was successfully delivered. Connection manager will reset its error counter for this HostName.
%% @end
-spec report_ack(HostName :: string()) -> ok.
%% ====================================================================
report_ack({HostName, Endpoint}) ->
    ensure_running(),
    exec({report_ack, {HostName, utils:ensure_binary(Endpoint)}});
report_ack(HostName) ->
    report_ack({HostName, <<"oneclient">>}).


%% send/3
%% ====================================================================
%% @doc Sends given binary data to selected hostname using wss protocol.
%%      In case of pull messages, response will be delivered as message to self() with fallowing structure: <br/>
%%      {response, MsgId :: integer(), AnswerStatus :: string(), WorkerAnswer :: iolist()}
%% @end
-spec send(HostName :: string() | binary(), MsgId :: integer(), Data :: iolist()) -> ok | error.
%% ====================================================================
send({HostName, Endpoint}, MsgId, Data) ->
    ensure_running(),
    exec({send, {HostName, utils:ensure_binary(Endpoint)}, MsgId, Data});
send(HostName, MsgId, Data) ->
    send({HostName, <<"oneclient">>}, MsgId, Data).


%% ====================================================================
%% Behaviour callback functions
%% For docs see websocket_client library
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
websocket_terminate({Code, _Payload}, _ConnState, State) ->
    State ! {self(), {closed, Code}},
    ok.


%% ====================================================================
%% Internal functions
%% ====================================================================

%% main_loop/0
%% ====================================================================
%% @doc Enter main_loop/1 with newly created state.
%% @end
-spec main_loop() -> no_return().
%% ====================================================================
main_loop() ->
    main_loop(#ppcon_state{}).


%% main_loop/1
%% ====================================================================
%% @doc Inter-provider communicator service process main loop.
%%      This process executes commands issued by exec/1 and handles communication with
%%      websocket_client library. Also, communicator manages open connections to other providers.
%% @end
-spec main_loop(State :: #ppcon_state{}) -> Result :: any().
%% ====================================================================
main_loop(#ppcon_state{msg_id = CurrentMsgId, connections = Connections, inbox = Inbox,
                        error_counter = Errors, socket_endpoint = Endpoints} = State) ->
    NewState =

        receive
            {From, {report_timeout, {HostName, Endpoint}}} ->
                From ! {self(), ok},
                case maps:get({HostName, Endpoint}, Errors, 0) of
                    Counter when Counter > 3 ->
                        ?error("There were over 3 timeouts in row for connection with ~p. Reseting the connection...", [{HostName, Endpoint}]),
                        State#ppcon_state{connections = maps:remove({HostName, Endpoint}, Connections), error_counter = maps:remove({HostName, Endpoint}, Errors)};
                    _ ->
                        State#ppcon_state{error_counter = maps:put({HostName, Endpoint}, maps:get({HostName, Endpoint}, Errors, 0) + 1, Errors)}
                end;
            {From, {report_ack, {HostName, Endpoint}}} ->
                From ! {self(), ok},
                State#ppcon_state{error_counter = maps:remove({HostName, Endpoint}, Errors)};
            {From, {reset_connection, {HostName, Endpoint}}} ->
                From ! {self(), ok},
                State#ppcon_state{connections = maps:remove({HostName, Endpoint}, Connections)};
            {From, get_msg_id} ->
                From ! {self(), CurrentMsgId},
                State#ppcon_state{msg_id = CurrentMsgId + 1};
            {From, {send, {HostName, Endpoint}, MsgId, Data} = _Req} ->
                NState1 = case maps:find({HostName, Endpoint}, Connections) of
                              error ->
                                  case connect(HostName, Endpoint, 5555, [{certfile, gr_plugin:get_cert_path()}, {keyfile, gr_plugin:get_key_path()}]) of
                                      {ok, Socket} ->
                                          ?info("Connected to ~p", [{HostName, Endpoint}]),
                                          State#ppcon_state{connections = maps:put({HostName, Endpoint}, Socket, Connections),
                                                            socket_endpoint = maps:put(Socket, {HostName, utils:ensure_binary(Endpoint)}, Endpoints)};
                                      {error, Reason} ->
                                          ?error("Cannot connect to ~p due to ~p", [{HostName, Endpoint}, Reason]),
                                          State
                                  end;
                              {ok, _} ->
                                  State
                          end,
                case maps:find({HostName, Endpoint}, NState1#ppcon_state.connections) of
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
                HostName = proplists:get_value(Socket, ValueMap, undefined),
                ?info("Connection to ~p closed due to ~p.", [HostName, Code]),
                State#ppcon_state{connections = maps:remove(HostName, Connections), socket_endpoint = maps:remove(Socket, Connections)};
            {Socket, {recv, Data}} ->
                {HostName, Endpoint} = maps:get(Socket, Endpoints),
                {AnswerStatus, WorkerAnswer, MsgId} =
                    case Endpoint of
                        <<"oneclient">> ->
                            #answer{answer_status = AnswerStatus0, worker_answer = WorkerAnswer0,
                                    message_id = MsgId0} = communication_protocol_pb:decode_answer(Data),
                            {AnswerStatus0, WorkerAnswer0, MsgId0};
                        <<"oneprovider">> ->
                            #rtresponse{answer_status = AnswerStatus0, worker_answer = WorkerAnswer0,
                                        message_id = MsgId0} = rtcore_pb:decode_rtresponse(Data),
                            {AnswerStatus0, WorkerAnswer0, MsgId0}
                    end,
                SendTo = maps:get(MsgId, Inbox),
                SendTo ! {response, MsgId, AnswerStatus, WorkerAnswer},
                State#ppcon_state{inbox = maps:remove(MsgId, Inbox)}
        after 10000 ->
            State
        end,
    ?MODULE:main_loop(NewState).


%% exec/1
%% ====================================================================
%% @doc Synchronously executes given command on inter-cluster communicator service (main_loop/1).
%%      Returns response or fails with exception.
%% @end
-spec exec(Command :: term()) -> Result :: term() | no_return().
%% ====================================================================
exec(Command) ->
    PPCon = whereis(ppcon),
    PPCon ! {self(), Command},
    receive
        {PPCon, Response} -> Response
    after 10000 ->
        throw(ppcon_timeout)
    end.


%% connect/4
%% ====================================================================
%% @doc Connects to cluster with given host, port and transport options. Returns socket's handle.
%%      Note that some options may conflict with websocket_client's options so don't pass any options but certificate configuration.
-spec connect(Host :: string(), Endpoint :: string() | binary(), Port :: non_neg_integer(), Opts :: [term()]) -> {ok, Socket :: pid()} | {error, timout} | {error, Reason :: any()}.
%% ====================================================================
connect(Host, Endpoint, Port, Opts) when is_atom(Host) ->
    connect(atom_to_list(Host), Endpoint, Port, Opts);
connect(Host, Endpoint, Port, Opts) ->
    erlang:process_flag(trap_exit, true),
    flush_errors(),
    Opts1 = Opts -- [auto_handshake],
    Monitored =
        case websocket_client:start_link("wss://" ++ utils:ensure_list(Host) ++ ":" ++ integer_to_list(Port) ++ "/" ++ utils:ensure_list(Endpoint), ?MODULE, [self()], Opts1 ++ [{reuse_sessions, false}]) of
            {ok, Proc}      -> erlang:monitor(process, Proc), Proc;
            {error, Error}  -> self() ! {error, Error}, undefined;
            Error1          -> self() ! {error, Error1}, undefined
        end,
    Return =
        receive
            {connected, Monitored}              -> {ok, Monitored};
            {error, Other1}                     -> {error, Other1};
            {'DOWN', _, _, Monitored, Info}     -> {error, Info};
            {'EXIT', Monitored, Reason}         -> {error, Reason}
        after 3000 ->
            {error, timeout}
        end,
    Return.


%% flush_errors/0
%% ====================================================================
%% @doc Removes all {error, _} messages from inbox.
%% @end
-spec flush_errors() -> ok.
%% ====================================================================
flush_errors() ->
    receive
        {error, _} -> flush_errors()
    after 0 ->
        ok
    end.
