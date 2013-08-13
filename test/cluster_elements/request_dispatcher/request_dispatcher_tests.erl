%% ==================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of request_dispatcher.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================

%% TODO sprawdzić zachowanie funkcji kodującej i dekudującej (encode_answer i decode_protocol_buffer) w ranch_handler
%% w przypadku błędnych argumentów (rekordów/protoclo_bufferów)

%% TODO sprawdzić metodę handle_call pod kątem forwardowania różnych typów zapytań do workerów

-module(request_dispatcher_tests).
-include("communication_protocol_pb.hrl").
-include("registered_names.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

%% ====================================================================
%% Test functions
%% ====================================================================

%% This test checks if dispatcher uses protocol buffer correctly.
protocol_buffers_test() ->
  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Message = #clustermsg{module_name = "module", message_type = "atom",
  message_decoder_name = "communication_protocol", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = PingBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {Synch, Task, Answer_decoder_name, ProtocolVersion, Msg, Answer_type} = ranch_handler:decode_protocol_buffer(MessageBytes),
  ?assert(Synch),
  ?assert(Msg =:= ping),
  ?assert(Task =:= module),
  ?assert(Answer_decoder_name =:= "communication_protocol"),
  ?assert(ProtocolVersion == 1),
  ?assert(Answer_type =:= "atom"),

  Pong = #atom{value = "pong"},
  PongBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Pong)),

  Message2 = #answer{answer_status = "ok", worker_answer = PongBytes},
  MessageBytes2 = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(Message2)),

  EncodedPong = ranch_handler:encode_answer(ok, "atom", "communication_protocol", pong),
  ?assert(EncodedPong =:= MessageBytes2).

%% This test checks if dispatcher returns nodes where modules are running correctly
get_worker_node_test() ->
  request_dispatcher:start_link(),

  N1 = node(),
  WorkersList = [{N1, fslogic}, {N1, dao}, {n2, fslogic}, {n3, fslogic}, {n3, dao}, {n4, gateway}, {N1, dns_worker}],
  gen_server:cast(?Dispatcher_Name, {update_workers, WorkersList, 1, 1, 1}),
  Requests = [fslogic, fslogic, fslogic, fslogic, fslogic, fslogic, fslogic, dao, rtransfer, dao, dns_worker, dns_worker, gateway, gateway],
  ExpectedAns = [n3, n2, N1, N1, n2, n3, n3, n3, non, N1, N1, N1, n4, n4],

  FullAns = lists:foldl(fun(R, TmpAns) ->
    Ans = gen_server:call(?Dispatcher_Name, {get_worker_node, R}),
    [Ans | TmpAns]
  end, [], Requests),
  ?assertEqual(ExpectedAns, lists:reverse(FullAns)),

  request_dispatcher:stop().

%% This test checks if dispatcher returns nodes where modules are running correctly
%% when it is expected to process request on chosen node and node load is low
check_worker_node_ok_test() ->
  request_dispatcher:start_link(),

  N1 = node(),
  WorkersList = [{N1, fslogic}, {N1, dao}, {n2, fslogic}, {n3, fslogic}, {n3, dao},{n3, gateway}, {n4, gateway}, {N1, dns_worker}],
  gen_server:cast(?Dispatcher_Name, {update_workers, WorkersList, 1, 1, 1}),
  Requests = [fslogic, fslogic, fslogic, fslogic, dao, rtransfer, dao, dns_worker, dns_worker, gateway, gateway, gateway],
  ExpectedAns = [N1, N1, N1, N1, N1, non, N1, N1, N1, n4, n3, n3],

  FullAns = lists:foldl(fun(R, TmpAns) ->
    Ans = gen_server:call(?Dispatcher_Name, {check_worker_node, R}),
    [Ans | TmpAns]
  end, [], Requests),
  ?assertEqual(ExpectedAns, lists:reverse(FullAns)),

  request_dispatcher:stop().

%% This test checks if dispatcher returns nodes where modules are running correctly
%% when it is expected to process request on chosen node and node load is high
check_worker_node_high_load1_test() ->
  check_worker_node_high_load_helper(3.5, 3.5).

%% This test checks if dispatcher returns nodes where modules are running correctly
%% when it is expected to process request on chosen node and node load is high
check_worker_node_high_load2_test() ->
  check_worker_node_high_load_helper(2, 0.9).

%% ====================================================================
%% Helper functions
%% ====================================================================

%% This function checks if dispatcher returns nodes where modules are running correctly
%% when it is expected to process request on chosen node and node load is high
check_worker_node_high_load_helper(Current, Avg) ->
  request_dispatcher:start_link(),

  N1 = node(),
  WorkersList = [{N1, fslogic}, {N1, dao}, {n2, fslogic}, {n3, fslogic}, {n3, dao}, {n4, gateway}, {N1, dns_worker}],
  gen_server:cast(?Dispatcher_Name, {update_workers, WorkersList, 1, Current, Avg}),
  Requests = [fslogic, fslogic, fslogic, fslogic, fslogic, fslogic, fslogic, dao, rtransfer, dao, dns_worker, dns_worker, gateway, gateway],
  ExpectedAns = [n3, n2, N1, N1, n2, n3, n3, n3, non, N1, N1, N1, n4, n4],

  FullAns = lists:foldl(fun(R, TmpAns) ->
    Ans = gen_server:call(?Dispatcher_Name, {check_worker_node, R}),
    [Ans | TmpAns]
  end, [], Requests),
  ?assertEqual(ExpectedAns, lists:reverse(FullAns)),

  request_dispatcher:stop().

-endif.
