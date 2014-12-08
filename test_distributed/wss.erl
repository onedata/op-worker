%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides simple WebSocket client that allows connecting
%%       to oneprovider with given host
%% @end
%% ===================================================================
-module(wss).

-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").

-export([
    init/2,
    websocket_handle/3,
    websocket_info/3,
    websocket_terminate/3,
    connect/3, send/2, recv/2, close/1,
    handshakeInit/3, handshakeAck/2
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
        case websocket_client:start_link("wss://" ++ Host ++ ":" ++ integer_to_list(Port) ++ "/oneclient" , ?MODULE, [self()], Opts1 ++ [{reuse_sessions, false}]) of
            {ok, Proc}      -> erlang:monitor(process, Proc), Proc;
            {error, Error}  -> self() ! {error, Error}, ok;
            Error1          -> self() ! {error, Error1}, ok
        end,
    Return =
        receive
            {connected, Monitored}              ->
                %% If auto_handshake is enabled, set FuseId for this connection
                case lists:member(auto_handshake, Opts) of
                    true ->
                        FuseId = handshakeInit(Monitored, "hostname", []),
                        handshakeAck(Monitored, FuseId);
                    false -> ok
                end,
                {ok, Monitored};
            {error, Other1}                     -> {error, Other1};
            {'DOWN', _, _, Monitored, Info}     -> {error, Info};
            {'EXIT', Monitored, Reason}         -> {error, Reason}
        after 5000 ->
            {error, timeout}
        end,
    Return.


%% recv/2
%% ====================================================================
%% @doc Receives WebSocket frame from given SocketRef (Pid). Timeouts after Timeout.
-spec recv(SocketRef :: pid(), Timeout :: non_neg_integer()) -> {ok, Data :: binary()} | {error, timout} | {error, Reason :: any()}.
%% ====================================================================
recv(Pid, Timeout) ->
    receive
        {Pid, {recv, Data}} -> {ok, Data};
        {Pid, Other} -> {error, Other}
    after Timeout ->
        {error, timeout}
    end.

%% send/2
%% ====================================================================
%% @doc Sends asynchronously Data over WebSocket.
-spec send(SocketRef :: pid(), Data :: binary()) -> ok.
%% ====================================================================
send(Pid, Data) ->
    Pid ! {send, Data},
    ok.


%% close/1
%% ====================================================================
%% @doc Closes WebSocket connection.
-spec close(SocketRef :: pid()) -> ok.
%% ====================================================================
close(Pid) ->
    Pid ! {close, <<>>},
    ok.


%% handshakeInit/3
%% ====================================================================
%% @doc Negotiates FuseId with cluster. The function returns newly negotiated FuseId as string.
-spec handshakeInit(SocketRef :: pid(), Hostname :: string(), EnvList :: [{EnvName :: atom(), EnvValue :: string()}]) -> string() | no_return().
%% ====================================================================
handshakeInit(Pid, Hostname, EnvList) ->
    HEnv = [#handshakerequest_envvariable{name = atom_to_list(Name), value = Value} || {Name, Value} <- EnvList],
    Req = #handshakerequest{hostname = Hostname, variable = HEnv},
    HBin = erlang:iolist_to_binary(fuse_messages_pb:encode_handshakerequest(Req)),
    MsgId = 1000,
    CMsg = #clustermsg{protocol_version = 1, module_name = "", message_type = "handshakerequest", message_id = MsgId, message_decoder_name = "fuse_messages", answer_type = "handshakeresponse", answer_decoder_name = "fuse_messages", input = HBin},
    CMsgBin = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(CMsg)),
    send(Pid, CMsgBin),
    {ok, Ans} = recv(Pid, 5000),
    case communication_protocol_pb:decode_answer(Ans) of
        #answer{answer_status = "ok", worker_answer = Bytes} ->
            #handshakeresponse{fuse_id = FuseId} = fuse_messages_pb:decode_handshakeresponse(Bytes),
            FuseId;
        #answer{answer_status = NonOK} ->
            throw(list_to_atom(NonOK))
    end.


%% handshakeAck/2
%% ====================================================================
%% @doc Inform cluster that FuseId shall be used with this connection.
-spec handshakeAck(SocketRef :: pid(), FuseId :: string()) -> ok | atom() | no_return().
%% ====================================================================
handshakeAck(Pid, FuseId) ->
    Ack = #handshakeack{fuse_id = FuseId},
    AckBin = erlang:iolist_to_binary(fuse_messages_pb:encode_handshakeack(Ack)),
    MsgId = 1001,
    CMsg = #clustermsg{protocol_version = 1, module_name = "", message_type = "handshakeack", message_id = MsgId, message_decoder_name = "fuse_messages", answer_type = "atom", answer_decoder_name = "communication_protocol", input = AckBin},
    CMsgBin = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(CMsg)),
    send(Pid, CMsgBin),
    {ok, Ans} = recv(Pid, 5000),
    #answer{answer_status = Status, worker_answer = _Bytes} = communication_protocol_pb:decode_answer(Ans),
    list_to_atom(Status).


%% ====================================================================
%% Internal functions
%% ====================================================================

flush_errors() ->
    receive
        {error, _} -> flush_errors()
    after 0 ->
        ok
    end.
