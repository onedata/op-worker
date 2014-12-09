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

%% TODO sprawdzić zachowanie funkcji kodującej i dekudującej (encode_answer i decode_clustermsg_pb) w ws_handler
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

%% ====================================================================
%% Test functions
%% ====================================================================

%% This test checks if dispatcher uses protocol buffer correctly.
protocol_buffers_test() ->
  oneprovider_node_app:activate_white_lists(),

  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Message = #clustermsg{module_name = "dao", message_type = "atom",
  message_decoder_name = "communication_protocol", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, message_id = 22, input = PingBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {Task, Answer_decoder_name, ProtocolVersion, Msg, MsgId, Answer_type, {_, _}} = ws_handler:decode_clustermsg_pb(MessageBytes),
  ?assert(Msg =:= ping),
  ?assert(Task =:= dao),
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

%% This test checks if decoding of nested message works
recursive_decoding_test() ->
  InternalMessage1 = #getfilelocation{file_logic_name = "filename"},
  InternalMessage1Bytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getfilelocation(InternalMessage1)),

  InternalMessage2 = #fusemessage{message_type = "getfilelocation", input = InternalMessage1Bytes},
  InternalMessage2Bytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(InternalMessage2)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, message_id = 22, input = InternalMessage2Bytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {Task, Answer_decoder_name, ProtocolVersion, Msg, MsgId, Answer_type, _} = ws_handler:decode_clustermsg_pb(MessageBytes),
  ?assert(Task =:= fslogic),
  ?assert(Answer_decoder_name =:= "communication_protocol"),
  ?assert(ProtocolVersion == 1),
  ?assert(MsgId == 22),
  ?assert(Answer_type =:= "atom"),

  ?assert(is_record(Msg, fusemessage)),
  ?assertEqual(getfilelocation, Msg#fusemessage.message_type),
  IntMessage = Msg#fusemessage.input,
  ?assert(is_record(IntMessage, getfilelocation)),
  ?assertEqual("filename", IntMessage#getfilelocation.file_logic_name).

%% This test checks if decoding of nested message works when an error in internal message appear
recursive_decoding_error_test() ->
  WrongInput = erlang:iolist_to_binary("wrong_input"),
  InternalMessage = #fusemessage{message_type = "getfilelocation", input = WrongInput},
  InternalMessageBytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(InternalMessage)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, message_id = 22, input = InternalMessageBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {Task, Answer_decoder_name, ProtocolVersion, Msg, MsgId, Answer_type, _} = ws_handler:decode_clustermsg_pb(MessageBytes),
  ?assert(Task =:= fslogic),
  ?assert(Answer_decoder_name =:= "communication_protocol"),
  ?assert(ProtocolVersion == 1),
  ?assert(MsgId == 22),
  ?assert(Answer_type =:= "atom"),

  ?assert(is_record(Msg, fusemessage)),
  ?assertEqual("getfilelocation", Msg#fusemessage.message_type),
  ?assertEqual(WrongInput, Msg#fusemessage.input).

%% This test checks handling of errors during decoding of nested message
recursive_not_supported_message_decoding_test() ->
  InternalMessage1 = #getfilelocation{file_logic_name = "filename"},
  InternalMessage1Bytes = erlang:iolist_to_binary(fuse_messages_pb:encode_getfilelocation(InternalMessage1)),

  InternalMessage2 = #fusemessage{message_type = "wrong_type", input = InternalMessage1Bytes},
  InternalMessage2Bytes = erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(InternalMessage2)),

  Message = #clustermsg{module_name = "fslogic", message_type = "fusemessage",
  message_decoder_name = "fuse_messages", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, message_id = 22, input = InternalMessage2Bytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  {Task, Answer_decoder_name, ProtocolVersion, Msg, MsgId, Answer_type, _} = ws_handler:decode_clustermsg_pb(MessageBytes),

  ?assert(Task =:= fslogic),
  ?assert(Answer_decoder_name =:= "communication_protocol"),
  ?assert(ProtocolVersion == 1),
  ?assert(MsgId == 22),
  ?assert(Answer_type =:= "atom"),

  ?assert(is_record(Msg, fusemessage)),
  ?assertEqual("wrong_type", Msg#fusemessage.message_type),
  ?assertEqual(InternalMessage1Bytes, Msg#fusemessage.input).

%% This test checks what happens when wrong request appears
protocol_buffers_wrong_request_structure_test() ->
  oneprovider_node_app:activate_white_lists(),

  Ans = try
    ws_handler:decode_clustermsg_pb(some_atom),
    ok
  catch
    wrong_message_format -> wrong_message_format;
    E1:E2 -> {unknown_error, E1, E2}
  end,
  ?assertEqual(Ans, wrong_message_format).

%% This test checks what happens when wrong request appears
protocol_buffers_wrong_request_message_test() ->
  oneprovider_node_app:activate_white_lists(),

  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),
  NotExistingAtom = #atom{value = "not_existing_atom"},
  NotExistingAtomBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(NotExistingAtom)),

  Message = #clustermsg{module_name = "dao", message_type = "strange_message",
  message_decoder_name = "communication_protocol", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, message_id = 33, input = PingBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),
  Ans2 = try
    ws_handler:decode_clustermsg_pb(MessageBytes),
    ok
  catch
    {message_not_supported, 33} -> message_not_supported;
    E3:E4  -> {unknown_error, E3, E4}
  end,
  ?assertEqual(Ans2, message_not_supported),

  Message2 = #clustermsg{module_name = "dao", message_type = "atom",
  message_decoder_name = "communication_protocol", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, message_id = 44, input = NotExistingAtomBytes},
  MessageBytes2 = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message2)),
  Ans3 = try
    ws_handler:decode_clustermsg_pb(MessageBytes2),
    ok
         catch
           {message_not_supported, 44} -> message_not_supported;
           E5:E6 -> {unknown_error, E5, E6}
         end,
  ?assertEqual(Ans3, message_not_supported),

  Message3 = #clustermsg{module_name = "dao", message_type = "atom",
  message_decoder_name = "communication_protocol", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, message_id = 55, input = "wrong_input"},
  MessageBytes3 = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message3)),
  Ans4 = try
    ws_handler:decode_clustermsg_pb(MessageBytes3),
    ok
         catch
           {wrong_internal_message_type, 55} -> wrong_internal_message_type;
           E7:E8  -> {unknown_error, E7, E8}
         end,
  ?assertEqual(Ans4, wrong_internal_message_type).

%% This test checks what happens when wrong request appears
protocol_buffers_wrong_request_decoder_test() ->
  oneprovider_node_app:activate_white_lists(),

  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Message = #clustermsg{module_name = "dao", message_type = "atom",
  message_decoder_name = "not_existing_decoder", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, message_id = 66, input = PingBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  Ans = try
    ws_handler:decode_clustermsg_pb(MessageBytes),
    ok
         catch
           {message_not_supported, 66} -> message_not_supported;
           E1:E2  -> {unknown_error, E1, E2}
         end,
  ?assertEqual(Ans, message_not_supported).

%% This test checks what happens when wrong request appears
protocol_buffers_wrong_request_module_test() ->
  oneprovider_node_app:activate_white_lists(),

  Ping = #atom{value = "ping"},
  PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

  Message = #clustermsg{module_name = "strange_module", message_type = "atom",
  message_decoder_name = "communication_protocol", answer_type = "atom",
  answer_decoder_name = "communication_protocol", protocol_version = 1, message_id = 77, input = PingBytes},
  MessageBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

  Ans = try
    ws_handler:decode_clustermsg_pb(MessageBytes),
    ok
        catch
          {message_not_supported, 77} -> message_not_supported;
          E1:E2  -> {unknown_error, E1, E2}
        end,
  ?assertEqual(Ans, message_not_supported).

%% This test checks what happens when wrong request appears
protocol_buffers_wrong_answer_test() ->
  oneprovider_node_app:activate_white_lists(),

  EncodedPong = ws_handler:encode_answer("wrong_main_answer", 0, "atom", "communication_protocol", pong),
  Pong = communication_protocol_pb:decode_answer(EncodedPong),
  ?assertEqual(Pong#answer.answer_status, "main_answer_encoding_error"),

  EncodedPong2 = ws_handler:encode_answer(ok, 0, "atom", "communication_protocol", "wrong_worker_answer"),
  Pong2 = communication_protocol_pb:decode_answer(EncodedPong2),
  ?assertEqual(Pong2#answer.answer_status, "worker_answer_encoding_error").

%% This test checks what happens when wrong request appears
protocol_buffers_answer_encoding_error_test() ->
  oneprovider_node_app:activate_white_lists(),

  EncodedAnswer = ws_handler:encode_answer(ok, 0, "atom", "not_existing_decoder", pong),
  Answer = communication_protocol_pb:decode_answer(EncodedAnswer),
  ?assertEqual(Answer#answer.answer_status, "not_supported_answer_decoder"),

  EncodedAnswer2 = ws_handler:encode_answer(ok, 0, "strange_message_type", "communication_protocol", pong),
  Answer2 = communication_protocol_pb:decode_answer(EncodedAnswer2),
  ?assertEqual(Answer2#answer.answer_status, "not_supported_answer_decoder").

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
  gen_server:cast(?Dispatcher_Name, {update_workers, WorkersList, [], 1, 1, 1, [Module | ?MODULES]}),

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
