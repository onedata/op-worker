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
-export([decode_protocol_buffer/1, encode_answer/1, encode_answer/4]).
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
  Transport:setopts(Socket, [{packet, 4}]),
  case Transport:recv(Socket, 0, RanchTimeout) of
    {ok, Data} ->
      try
        {Synch, Task, Answer_decoder_name, ProtocolVersion, Msg, Answer_type} = decode_protocol_buffer(Data),
        case Synch of
          true ->
            try
              Pid = self(),
              Ans = gen_server:call(?Dispatcher_Name, {Task, ProtocolVersion, Pid, Msg}),
              case Ans of
                ok ->
                  receive
                    Ans2 -> Transport:send(Socket, encode_answer(Ans, Answer_type, Answer_decoder_name, Ans2))
                  after DispatcherTimeout ->
                    Transport:send(Socket, encode_answer(dispatcher_timeout))
                  end;
                Other -> Transport:send(Socket, encode_answer(Other))
              end
            catch
              _:_ -> Transport:send(Socket, encode_answer(dispatcher_error))
            end;
          false ->
            try
              Ans = gen_server:call(?Dispatcher_Name, {Task, ProtocolVersion, Msg}),
              Transport:send(Socket, encode_answer(Ans))
            catch
                _:_ -> Transport:send(Socket, encode_answer(dispatcher_error))
            end
         end,
         loop(Socket, Transport, RanchTimeout, DispatcherTimeout)
    catch
      _:_ -> Transport:send(Socket, encode_answer(wrong_message_format))
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
  #clustermsg{module_name = ModuleName, message_type = Message_type, message_decoder_name = Message_decoder_name, answer_type = Answer_type,
  answer_decoder_name = Answer_decoder_name, synch = Synch, protocol_version = Prot_version, input = Bytes}
    = communication_protocol_pb:decode_clustermsg(MsgBytes),
  Msg = erlang:apply(list_to_atom(Message_decoder_name ++ "_pb"), list_to_atom("decode_" ++ Message_type), [Bytes]),
  {Synch, list_to_atom(ModuleName), Answer_decoder_name, Prot_version, records_translator:translate(Msg, Message_decoder_name), Answer_type}.

%% encode_answer/1
%% ====================================================================
%% @doc Encodes answer using protocol buffers records_translator.
-spec encode_answer(Main_Answer :: atom()) -> Result when
  Result ::  binary().
%% ====================================================================
encode_answer(Main_Answer) ->
  encode_answer(Main_Answer, non, "non", []).

%% encode_answer/4
%% ====================================================================
%% @doc Encodes answer using protocol buffers records_translator.
-spec encode_answer(Main_Answer :: atom(), AnswerType :: string(), Answer_decoder_name :: string(), Worker_Answer :: term()) -> Result when
  Result ::  binary().
%% ====================================================================
encode_answer(Main_Answer, AnswerType, Answer_decoder_name, Worker_Answer) ->
  Check = ((Main_Answer =:= ok) and is_atom(Worker_Answer) and (Worker_Answer =:= worker_plug_in_error)),
  Main_Answer2 = case Check of
     true -> Worker_Answer;
     false -> Main_Answer
  end,
  Message = case Main_Answer2 of
    ok -> case AnswerType of
      non -> #answer{answer_status = atom_to_list(Main_Answer2)};
      _Type ->
        try
          WAns = erlang:apply(list_to_atom(Answer_decoder_name ++ "_pb"), list_to_atom("encode_" ++ AnswerType), [records_translator:translate_to_record(Worker_Answer)]),
          #answer{answer_status = atom_to_list(Main_Answer2), worker_answer = WAns}
        catch
          Type:Error ->
            lager:error("Ranch handler error during encoding answer: ~p:~p, answer type: ~s, decoder ~s, worker answer ~p", [Type, Error, AnswerType, Answer_decoder_name, Worker_Answer]),
            #answer{answer_status = "answer_encoding_error"}
        end
    end;
    _Other -> #answer{answer_status = atom_to_list(Main_Answer2)}
  end,
  erlang:iolist_to_binary(communication_protocol_pb:encode_answer(Message)).