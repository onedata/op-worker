%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module forwards requests from socket to dispatcher.
%% @end
%% ===================================================================

-module(ranch_handler).
-behaviour(ranch_protocol).
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([start_link/4]).
-export([init/4]).

-ifdef(TEST).
-export([decode_protocol_buffer/1, encode_answer/3]).
-endif.

%% ====================================================================
%% API functions
%% ====================================================================

%% start_link/4
%% ====================================================================
%% @doc Starts handler
-spec start_link(Ref :: term(), Socket :: term(), Transport :: term(), Opts :: term()) -> Result when
  Result ::  {ok,Pid},
  Pid :: pid().
%% ====================================================================
start_link(Ref, Socket, Transport, Opts) ->
  Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
  {ok, Pid}.

%% init/4
%% ====================================================================
%% @doc Initializes handler loop
-spec init(Ref :: term(), Socket :: term(), Transport :: term(), Opts :: term()) -> Result when
  Result ::  ok.
%% ====================================================================
init(Ref, Socket, Transport, _Opts = []) ->
  {ok, RanchTimeout} = application:get_env(veil_cluster_node, ranch_timeout),
  {ok, DispatcherTimeout} = application:get_env(veil_cluster_node, dispatcher_timeout),
  ok = ranch:accept_ack(Ref),
  loop(Socket, Transport, RanchTimeout, DispatcherTimeout).

%% ====================================================================
%% Internal functions
%% ====================================================================

%% loop/4
%% ====================================================================
%% @doc Main handler loop. It receives clients messages and forwards them to dispatcher
-spec loop(Socket :: term(), Transport :: term(), RanchTimeout :: integer(), DispatcherTimeout :: integer()) -> Result when
  Result ::  ok.
%% ====================================================================
loop(Socket, Transport, RanchTimeout, DispatcherTimeout) ->
  case Transport:recv(Socket, 0, RanchTimeout) of
    {ok, Data} ->
      try
        {Synch, Task, ProtocolVersion, Msg, Answer_type} = decode_protocol_buffer(Data),
        case Synch of
          true ->
            try
              Pid = self(),
              Ans = gen_server:call(?Dispatcher_Name, {Task, ProtocolVersion, Pid, Msg}),
              case Ans of
                ok ->
                  receive
                    Ans2 -> Transport:send(Socket, encode_answer(Ans, Answer_type, Ans2))
                  after DispatcherTimeout ->
                    Transport:send(Socket, encode_answer(dispatcher_timeout, non, []))
                  end;
                Other -> Transport:send(Socket, encode_answer(Other, non, []))
              end
            catch
              _:_ -> Transport:send(Socket, encode_answer(dispatcher_error, non, []))
            end;
          false ->
            try
              Ans = gen_server:call(?Dispatcher_Name, {Task, ProtocolVersion, Msg}),
              Transport:send(Socket, encode_answer(Ans, non, []))
            catch
                _:_ -> Transport:send(Socket, encode_answer(dispatcher_error, non, []))
            end
         end,
         loop(Socket, Transport, RanchTimeout, DispatcherTimeout)
    catch
      _:_ -> Transport:send(Socket, encode_answer(wrong_message_format, non, []))
    end;
    _ ->
      ok = Transport:close(Socket)
  end.

%% decode_protocol_buffer/1
%% ====================================================================
%% @doc Decodes the message using protocol buffers records_translator.
-spec decode_protocol_buffer(MsgBytes :: binary()) -> Result when
  Result ::  {Synch, ModuleName, Msg, Answer_type},
  Synch :: boolean(),
  ModuleName :: atom(),
  Msg :: term(),
  Answer_type :: string().
%% ====================================================================
decode_protocol_buffer(MsgBytes) ->
  #clustermsg{module_name = ModuleName, message_type = Message_type, answer_type = Answer_type, synch = Synch, protocol_version = Prot_version, input = Bytes} = communication_protocol_pb:decode_clustermsg(MsgBytes),
  Msg = erlang:apply(communication_protocol_pb, list_to_atom("decode_" ++ Message_type), [Bytes]),
  {Synch, list_to_atom(ModuleName), Prot_version, records_translator:translate(Msg), Answer_type}.

%% encode_answer/3
%% ====================================================================
%% @doc Encodes answer using protocol buffers records_translator.
-spec encode_answer(Main_Answer :: atom(), AnswerType :: string(), Worker_Answer :: term()) -> Result when
  Result ::  binary().
%% ====================================================================
encode_answer(Main_Answer, AnswerType, Worker_Answer) ->
  Message = case Main_Answer of
    ok -> case AnswerType of
      non -> #answer{answer_status = atom_to_list(Main_Answer)};
      _Type ->
        WAns = erlang:apply(communication_protocol_pb, list_to_atom("encode_" ++ AnswerType), [records_translator:translate_to_record(Worker_Answer)]),
        #answer{answer_status = atom_to_list(Main_Answer), worker_answer = WAns}
    end;
    _Other -> #answer{answer_status = atom_to_list(Main_Answer)}
  end,
  erlang:iolist_to_binary(communication_protocol_pb:encode_answer(Message)).