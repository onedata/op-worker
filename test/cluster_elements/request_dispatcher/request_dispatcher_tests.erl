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

%% TODO sprawdzić zachowanie funkcji kodującej i dekudującej (encode_answer i decode_protocol_buffer) w ws_handler
%% w przypadku błędnych argumentów (rekordów/protoclo_bufferów)

%% TODO sprawdzić metodę handle_call pod kątem forwardowania różnych typów zapytań do workerów

-module(request_dispatcher_tests).
-include("communication_protocol_pb.hrl").
-include("registered_names.hrl").
-include("modules_and_args.hrl").
-include("fuse_messages_pb.hrl").
-include("remote_file_management_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

-record(test_record, {xyz = [], abc = ""}).

%% ====================================================================
%% Test functions
%% ====================================================================

%% This test checks if dispatcher uses protocol buffer correctly.
protocol_buffers_test() ->
  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Message = #clustermsg{module_name = "module", message_type = "atom",
  message_decoder_name = "communication_protocol", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, message_id = 22, input = PingBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {Synch, Task, Answer_decoder_name, ProtocolVersion, Msg, MsgId, Answer_type, {_, _}} = ws_handler:decode_clustermsg_pb(MessageBytes, standard_user),
  ?assert(Synch),
  ?assert(Msg =:= ping),
  ?assert(Task =:= module),
  ?assert(Answer_decoder_name =:= "communication_protocol"),
  ?assert(ProtocolVersion == 1),
  ?assert(MsgId == 22),
  ?assert(Answer_type =:= "atom"),

  Pong = #atom{value = "pong"},
  PongBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Pong)),

  Message2 = #answer{answer_status = "ok", worker_answer = PongBytes},
  MessageBytes2 = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(Message2)),

  EncodedPong = ws_handler:encode_answer(ok, 0, "atom", "communication_protocol", pong),
  ?assert(EncodedPong =:= MessageBytes2).

%% This test checks what happens when wrong request appears
protocol_buffers_wrong_request_test() ->
  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),
  Pong = #atom{value = "pong"},
  PongBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Pong)),

  Ans = try
    ws_handler:decode_clustermsg_pb(some_atom, standard_user),
    ok
  catch
    wrong_message_format -> wrong_message_format;
    _:_ -> unknown_error
  end,
  ?assertEqual(Ans, wrong_message_format),

  Message = #clustermsg{module_name = "module", message_type = "strange_message",
  message_decoder_name = "communication_protocol", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, message_id = 33, input = PingBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),
  Ans2 = try
    ws_handler:decode_clustermsg_pb(MessageBytes, standard_user),
    ok
  catch
    {wrong_internal_message_type, 33} -> wrong_internal_message_type;
    _:_ -> unknown_error
  end,
  ?assertEqual(Ans2, wrong_internal_message_type),

  Message2 = #clustermsg{module_name = "module", message_type = "atom",
  message_decoder_name = "communication_protocol", answer_type = "atom",
  answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, message_id = 44, input = PongBytes},
  MessageBytes2 = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message2)),
  Ans3 = try
    ws_handler:decode_clustermsg_pb(MessageBytes2, standard_user),
    ok
         catch
           {message_not_supported, 44} -> message_not_supported;
           _:_ -> unknown_error
         end,
  ?assertEqual(Ans3, message_not_supported).

%% This test checks what happens when wrong request appears
protocol_buffers_wrong_answer_test() ->
  EncodedPong = ws_handler:encode_answer("wrong_main_answer", 0, "atom", "communication_protocol", pong),
  Pong = communication_protocol_pb:decode_answer(EncodedPong),
  ?assertEqual(Pong#answer.answer_status, "main_answer_encoding_error"),

  EncodedPong2 = ws_handler:encode_answer(ok, 0, "atom", "communication_protocol", "wrong_worker_answer"),
  Pong2 = communication_protocol_pb:decode_answer(EncodedPong2),
  ?assertEqual(Pong2#answer.answer_status, "worker_answer_encoding_error").

%% This test checks if dispatcher returns nodes where modules are running correctly
get_worker_node_test() ->
  {ok, _} = request_dispatcher:start_link(),

  N1 = node(),
  WorkersList = [{N1, fslogic}, {N1, dao_worker}, {n2, fslogic}, {n3, fslogic}, {n3, dao_worker}, {n4, gateway}, {N1, dns_worker}],
  gen_server:cast(?Dispatcher_Name, {update_workers, WorkersList, [], 1, 1, 1}),
  Requests = [fslogic, fslogic, fslogic, fslogic, fslogic, fslogic, fslogic, dao_worker, rtransfer, dao_worker, dns_worker, dns_worker, gateway, gateway],
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
  {ok, _} = request_dispatcher:start_link(),

  N1 = node(),
  WorkersList = [{N1, fslogic}, {N1, dao_worker}, {n2, fslogic}, {n3, fslogic}, {n3, dao_worker},{n3, gateway}, {n4, gateway}, {N1, dns_worker}],
  gen_server:cast(?Dispatcher_Name, {update_workers, WorkersList, [], 1, 1, 1}),
  Requests = [fslogic, fslogic, fslogic, fslogic, dao_worker, rtransfer, dao_worker, dns_worker, dns_worker, gateway, gateway, gateway],
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

%% This test checks if dispatcher is able to forward messages to module
request_forward_test() ->
  Module = sample_plug_in,
  {ok, _} = request_dispatcher:start_link([Module]),
  worker_host:start_link(Module, [], 10),

  N1 = node(),
  WorkersList = [{N1, Module}],
  gen_server:cast(?Dispatcher_Name, {update_workers, WorkersList, [], 1, 1, 1, [Module | ?Modules]}),

  ok = gen_server:call(?Dispatcher_Name, {Module, 1, self(), 11, {ok_request, 1}}),
  First =
    receive
      {worker_answer, 11, 1} -> ok
    after 1000 ->
      error
    end,
  ?assertEqual(First, ok),

  ok = gen_server:call(?Dispatcher_Name, {Module, 1, self(), {ok_request, 2}}),
  Second =
    receive
      2 -> ok
    after 1000 ->
      error
    end,
  ?assertEqual(Second, ok),

  ok = gen_server:call(?Dispatcher_Name, {node_chosen, {Module, 1, self(), {ok_request, 3}}}),
  Third =
    receive
      3 -> ok
    after 1000 ->
      error
    end,
  ?assertEqual(Third, ok),

  worker_host:stop(Module),
  request_dispatcher:stop().

%% This test checks if dispatcher is able to check which messages should be discarded
white_list_test() ->
  ?assert(ws_handler:checkMessage(#fusemessage{message_type = "type", input = <<>>}, "User")),
  ?assert(ws_handler:checkMessage(#remotefilemangement{message_type = "type", input = <<>>}, "User")),
  ?assertEqual(false, ws_handler:checkMessage(#test_record{xyz = [x], abc = "a"}, "User")),

  ?assert(ws_handler:checkMessage(ping, "User")),
  ?assertEqual(false, ws_handler:checkMessage(pong, "User")),

  ?assertEqual(ws_handler:checkMessage("string", "User"), false).


%% ====================================================================
%% Helper functions
%% ====================================================================

%% This function checks if dispatcher returns nodes where modules are running correctly
%% when it is expected to process request on chosen node and node load is high
check_worker_node_high_load_helper(Current, Avg) ->
  {ok, _} = request_dispatcher:start_link(),

  N1 = node(),
  WorkersList = [{N1, fslogic}, {N1, dao_worker}, {n2, fslogic}, {n3, fslogic}, {n3, dao_worker}, {n4, gateway}, {N1, dns_worker}],
  gen_server:cast(?Dispatcher_Name, {update_workers, WorkersList, [], 1, Current, Avg}),
  Requests = [fslogic, fslogic, fslogic, fslogic, fslogic, fslogic, fslogic, dao_worker, rtransfer, dao_worker, dns_worker, dns_worker, gateway, gateway],
  ExpectedAns = [n3, n2, N1, N1, n2, n3, n3, n3, non, N1, N1, N1, n4, n4],

  FullAns = lists:foldl(fun(R, TmpAns) ->
    Ans = gen_server:call(?Dispatcher_Name, {check_worker_node, R}),
    [Ans | TmpAns]
  end, [], Requests),
  ?assertEqual(ExpectedAns, lists:reverse(FullAns)),

  request_dispatcher:stop().

-endif.
