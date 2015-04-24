%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Translations between protobuff and internal protocol
%%% @end
%%%-------------------------------------------------------------------
-module(translator).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/stream_messages.hrl").
-include("proto/oneclient/handshake_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("clproto/include/messages.hrl").

%% API
-export([translate_from_protobuf/1, translate_to_protobuf/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% traslate protobuf record to internal record
%% @end
%%--------------------------------------------------------------------
-spec translate_from_protobuf(tuple() | undefined) -> tuple() | undefined.
translate_from_protobuf(#'FileBlock'{offset = Off, size = S}) ->
    #file_block{offset = Off, size = S};
translate_from_protobuf(#'Status'{code = Code, description = Desc}) ->
    #status{code = Code, description = Desc};
translate_from_protobuf(#'Event'{event = {_, Record}}) ->
    translate_from_protobuf(Record);
translate_from_protobuf(#'ReadEvent'{} = Record) ->
    #read_event{
        counter = Record#'ReadEvent'.counter,
        file_id = Record#'ReadEvent'.file_id,
        size = Record#'ReadEvent'.size,
        blocks = [translate_to_protobuf(B) || B <- Record#'ReadEvent'.blocks]
    };
translate_from_protobuf(#'WriteEvent'{} = Record) ->
    #write_event{
        counter = Record#'WriteEvent'.counter,
        file_id = Record#'WriteEvent'.file_id,
        size = Record#'WriteEvent'.size,
        blocks = [translate_to_protobuf(B) || B <- Record#'WriteEvent'.blocks]
    };
translate_from_protobuf(#'HandshakeRequest'{token = Token, session_id = SessionId}) ->
    #handshake_request{token = translate_from_protobuf(Token), session_id = SessionId};
translate_from_protobuf(#'MessageStream'{stream_id = StmId, sequence_number = SeqNum}) ->
    #message_stream{stream_id = StmId, sequence_number = SeqNum};
translate_from_protobuf(#'EndOfMessageStream'{}) ->
    #end_of_message_stream{};
translate_from_protobuf(#'Token'{value = Val}) ->
    #token{value = Val};
translate_from_protobuf(#'Ping'{}) ->
    #ping{};
translate_from_protobuf(#'GetProtocolVersion'{}) ->
    #get_protocol_version{};
translate_from_protobuf(#'Data'{data = Data}) ->
    #data{data = Data};
translate_from_protobuf(undefined) ->
    undefined.

%%--------------------------------------------------------------------
%% @doc
%% translate internal record to protobuf record
%% @end
%%--------------------------------------------------------------------
-spec translate_to_protobuf(tuple() | undefined) -> tuple() | undefined.
translate_to_protobuf(#file_block{offset = Off, size = S}) ->
    #'FileBlock'{offset = Off, size = S};
translate_to_protobuf(#status{code = Code, description = Desc}) ->
    {status, #'Status'{code = Code, description = Desc}};
translate_to_protobuf(#event_subscription_cancellation{id = Id}) ->
    {event_subscription, #'EventSubscription'{
        event_subscription = {event_subscription_cancellation,
            #'EventSubscriptionCancellation'{
                id = Id
            }
        }
    }};
translate_to_protobuf(#read_event_subscription{} = Sub) ->
    {event_subscription, #'EventSubscription'{
        event_subscription = {read_event_subscription,
            #'ReadEventSubscription'{
                id = Sub#read_event_subscription.id,
                counter_threshold = Sub#read_event_subscription.producer_counter_threshold,
                time_threshold = Sub#read_event_subscription.producer_time_threshold,
                size_threshold = Sub#read_event_subscription.producer_size_threshold
            }
        }
    }};
translate_to_protobuf(#write_event_subscription{} = Sub) ->
    {event_subscription, #'EventSubscription'{
        event_subscription = {write_event_subscription,
            #'WriteEventSubscription'{
                id = Sub#write_event_subscription.id,
                counter_threshold = Sub#write_event_subscription.producer_counter_threshold,
                time_threshold = Sub#write_event_subscription.producer_time_threshold,
                size_threshold = Sub#write_event_subscription.producer_size_threshold
            }
        }}};
translate_to_protobuf(#handshake_response{session_id = Id}) ->
    {handshake_response, #'HandshakeResponse'{session_id = Id}};
translate_to_protobuf(#message_stream{stream_id = StmId, sequence_number = SeqNum}) ->
    #'MessageStream'{stream_id = StmId, sequence_number = SeqNum};
translate_to_protobuf(#message_stream_reset{}) ->
    {message_stream_reset, #'MessageStreamReset'{}};
translate_to_protobuf(#message_request{stream_id = StmId,
    lower_sequence_number = LowerSeqNum, upper_sequence_number = UpperSeqNum}) ->
    {message_request, #'MessageRequest'{stream_id = StmId,
        lower_sequence_number = LowerSeqNum, upper_sequence_number = UpperSeqNum}};
translate_to_protobuf(#message_acknowledgement{stream_id = StmId, sequence_number = SeqNum}) ->
    {message_acknowledgement,
        #'MessageAcknowledgement'{stream_id = StmId, sequence_number = SeqNum}};
translate_to_protobuf(#pong{}) ->
    {pong, #'Pong'{}};
translate_to_protobuf(#protocol_version{major = Major, minor = Minor}) ->
    {protocol_version, #'ProtocolVersion'{major = Major, minor = Minor}};
translate_to_protobuf(undefined) ->
    undefined.

%%%===================================================================
%%% Internal functions
%%%===================================================================
