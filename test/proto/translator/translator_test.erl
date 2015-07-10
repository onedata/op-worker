%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for translator module.
%%% @end
%%%--------------------------------------------------------------------
-module(translator_test).
-author("Jakub Kudzia").

-ifdef(TEST).

-include("global_definitions.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/stream_messages.hrl").
-include("proto/oneclient/handshake_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test functions
%%%===================================================================

%% Testing translate_from_protobuf function

translate_status_from_protobuf_test() ->
  {Internal, Protobuf} = get_status('VOK', <<1,2,3>>),
  [?_assertEqual(Internal, translator:translate_from_protobuf(Protobuf))].

translate_file_block_from_protobuf_test() ->
  {Internal, Protobuf} = get_file_block(0,0),
  {Internal2, Protobuf2} = get_file_block(32,2048),
  [?_assertEqual(Internal, translator:translate_from_protobuf(Protobuf)),
  [?_assertEqual(Internal2, translator:translate_from_protobuf(Protobuf2))]].

translate_read_event_from_protobuf_test() ->
  {Internal, Protobuf} = get_read_event(1, 2, 512, 100, 1024),
  [?_assertEqual(Internal, translator:translate_from_protobuf(Protobuf))].

translate_write_event_from_protobuf_test() ->
  {Internal, Protobuf} = get_write_event(1, 2, 512, 2048, 100, 1024),
  [?_assertEqual(Internal, translator:translate_from_protobuf(Protobuf))].

translate_handshake_request_from_protobuf_test() ->
  {Internal, Protobuf} = get_handshake_request(1, 1),
  [?_assertEqual(Internal, translator:translate_from_protobuf(Protobuf))].

translate_message_stream_from_protobuf_test() ->
  {Internal, Protobuf} = get_message_stream(1, 1),
  [?_assertEqual(Internal, translator:translate_from_protobuf(Protobuf))].

translate_end_of_message_stream_from_protobuf_test() ->
  {Internal, Protobuf} = get_end_of_message_stream(),
  [?_assertEqual(Internal, translator:translate_from_protobuf(Protobuf))].

translate_token_from_protobuf_test() ->
  {Internal, Protobuf} = get_token(1),
  [?_assertEqual(Internal, translator:translate_from_protobuf(Protobuf))].

translate_ping_from_protobuf_test() ->
  {Internal, Protobuf} = get_ping(1),
  [?_assertEqual(Internal, translator:translate_from_protobuf(Protobuf))].

translate_get_protocol_version_from_protobuf_test() ->
  {Internal, Protobuf} = get_get_protocol_version(),
  [?_assertEqual(Internal, translator:translate_from_protobuf(Protobuf))].

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_status(Code, Description) ->
  {#status{code = Code, description = Description},
   #'Status'{code = Code, description = Description}}.

get_file_block(0, 0) ->
  {#file_block{}, #'FileBlock'{}};
get_file_block(Off, S) ->
  {#file_block{offset = Off, size = S}, #'FileBlock'{offset = Off, size = S}}.

get_file_block_list(Num, MaxS) ->
  lists:unzip([ get_file_block(random:uniform(MaxS), random:uniform(MaxS)) || _ <- lists:seq(1,Num)]).

get_read_event(Counter, File_id, Size, Num, MaxS) ->
  {InternalBlocks, ProtobufBlocks} = get_file_block_list(Num, MaxS),
  {
    #read_event{counter = Counter, file_id = File_id, size = Size,blocks = InternalBlocks },
    #'ReadEvent'{counter = Counter, file_id = File_id, size = Size,blocks = ProtobufBlocks }
  }.

get_write_event(Counter, File_id, Size, FileSize, Num, MaxS) ->
  {InternalBlocks, ProtobufBlocks} = get_file_block_list(Num, MaxS),
  {
    #write_event{counter = Counter, file_id = File_id, size = Size, file_size = FileSize, blocks = InternalBlocks },
    #'WriteEvent'{counter = Counter, file_id = File_id, size = Size, file_size = FileSize, blocks = ProtobufBlocks }
  }.

get_token(Val)->
  {#token{value = Val}, #'Token'{value = Val}}.

get_handshake_request(TokenVal, SessionId) ->
  {IntToken, PBToken} = get_token(TokenVal),
  {
    #handshake_request{token = IntToken, session_id = SessionId},
    #'HandshakeRequest'{token = PBToken, session_id = SessionId}
  }.

get_message_stream(StmId, SeqNum) ->
  { 
    #message_stream{stream_id = StmId, sequence_number = SeqNum},
    #'MessageStream'{stream_id = StmId, sequence_number = SeqNum}
  }.
  
get_end_of_message_stream() ->
  {#end_of_message_stream{},#'EndOfMessageStream'{}}.

get_ping(Data) ->
  {#ping{data = Data},#'Ping'{data = Data} }.
  
get_get_protocol_version() ->
  {#get_protocol_version{}, #'GetProtocolVersion'{} }.

-endif.