%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module forwards requests from ranch to dispatcher.
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
-export([decode_protocol_buffer/1]).
-endif.

%% ====================================================================
%% API functions
%% ====================================================================
start_link(Ref, Socket, Transport, Opts) ->
  Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
  {ok, Pid}.

init(Ref, Socket, Transport, _Opts = []) ->
  {ok, RanchTimeout} = application:get_env(veil_cluster_node, ranch_timeout),
  {ok, DispatcherTimeout} = application:get_env(veil_cluster_node, dispatcher_timeout),
  ok = ranch:accept_ack(Ref),
  loop(Socket, Transport, RanchTimeout, DispatcherTimeout).

%% ====================================================================
%% Internal functions
%% ====================================================================
loop(Socket, Transport, RanchTimeout, DispatcherTimeout) ->
  case Transport:recv(Socket, 0, RanchTimeout) of
    {ok, Data} ->
      Request = binary_to_term(Data),
      case Request of
        {synch, Task, ProtocolVersion, Msg} ->
          try
            Pid = self(),
            Ans = gen_server:call(?Dispatcher_Name, {Task, ProtocolVersion, Pid, Msg}),
            case Ans of
              ok ->
                receive
                  Ans2 -> Transport:send(Socket, atom_to_binary(Ans2, utf8))
                after DispatcherTimeout ->
                  Transport:send(Socket, <<"dispatcher timeout">>)
                end;
              Other -> Transport:send(Socket, atom_to_binary(Other, utf8))
            end
          catch
            _:_ -> Transport:send(Socket, <<"dispatcher error">>)
          end;
        {asynch, Task, ProtocolVersion, Msg} ->
          try
            Ans = gen_server:call(?Dispatcher_Name, {Task, ProtocolVersion, Msg}),
            Transport:send(Socket, atom_to_binary(Ans, utf8))
            catch
              _:_ -> Transport:send(Socket, <<"dispatcher error">>)
          end;
        _Other -> Transport:send(Socket, <<"wrong message format">>)
       end,
       loop(Socket, Transport, RanchTimeout, DispatcherTimeout);
    _ ->
      ok = Transport:close(Socket)
  end.

decode_protocol_buffer(MsgBytes) ->
  #clustermsg{module_name = ModuleName, message_type = InputType, input = Bytes} = communication_protocol_pb:decode_clustermsg(MsgBytes),
  Msg = erlang:apply(communication_protocol_pb, list_to_atom("decode_" ++ InputType), [Bytes]),
  {list_to_atom(ModuleName), records_translator:translate(Msg)}.