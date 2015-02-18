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
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto_internal/oneclient/common_messages.hrl").
-include("proto_internal/oneclient/stream_messages.hrl").
-include("proto_internal/oneclient/handshake_messages.hrl").
-include("proto_internal/oneclient/event_messages.hrl").
-include("proto_internal/oneclient/ping_messages.hrl").
-include_lib("ctool/include/logging.hrl").

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
-spec translate_from_protobuf(tuple()) -> tuple().
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
        blocks = Record#'ReadEvent'.blocks
    };
translate_from_protobuf(#'WriteEvent'{} = Record) ->
    #write_event{
        counter = Record#'WriteEvent'.counter,
        file_id = Record#'WriteEvent'.file_id,
        size = Record#'WriteEvent'.size,
        blocks = Record#'WriteEvent'.blocks
    };
translate_from_protobuf(#'HandshakeRequest'{token = Token, session_id = SessionId}) ->
    #handshake_request{token = translate_from_protobuf(Token), session_id = SessionId};
translate_from_protobuf(#'MessageStream'{stm_id = StmId, seq_num = SeqNum, eos = Eos}) ->
    #message_stream{stm_id = StmId, seq_num = SeqNum, eos = Eos};
translate_from_protobuf(#'Token'{value = Val}) ->
    #token{value = Val};
translate_from_protobuf(#'Ping'{}) ->
    #ping{};
translate_from_protobuf(undefined) ->
    undefined.

%%--------------------------------------------------------------------
%% @doc
%% translate internal record to protobuf record
%% @end
%%--------------------------------------------------------------------
-spec translate_to_protobuf(tuple()) -> tuple().
translate_to_protobuf(#file_block{offset = Off, size = S}) ->
    #'FileBlock'{offset = Off, size = S};
translate_to_protobuf(#status{code = Code, description = Desc}) ->
    #'Status'{code = Code, description = Desc};
translate_to_protobuf(#event_subscription_cancellation{id = Id}) ->
    #'EventSubscription'{
        event_subscription = {event_subscription_cancellation,
            #'EventSubscriptionCancellation'{
                id = Id
            }
        }
    };
translate_to_protobuf(#read_event_subscription{} = Sub) ->
    #'EventSubscription'{
        event_subscription = {read_event_subscription,
            #'ReadEventSubscription'{
                id = Sub#read_event_subscription.id,
                counter_threshold = Sub#read_event_subscription.producer_counter_threshold,
                time_threshold = Sub#read_event_subscription.producer_time_threshold,
                size_threshold = Sub#read_event_subscription.producer_size_threshold
            }
        }
    };
translate_to_protobuf(#write_event_subscription{} = Sub) ->
    #'EventSubscription'{
        event_subscription = {write_event_subscription,
            #'WriteEventSubscription'{
                id = Sub#write_event_subscription.id,
                counter_threshold = Sub#write_event_subscription.producer_counter_threshold,
                time_threshold = Sub#write_event_subscription.producer_time_threshold,
                size_threshold = Sub#write_event_subscription.producer_size_threshold
            }
        }};
translate_to_protobuf(#handshake_response{session_id = Id}) ->
    #'HandshakeResponse'{session_id = Id};
translate_to_protobuf(#message_stream{stm_id = StmId, seq_num = SeqNum, eos = Eos}) ->
    #'MessageStream'{stm_id = StmId, seq_num = SeqNum, eos = Eos};
translate_to_protobuf(#message_stream_reset{}) ->
    #'MessageStreamReset'{};
translate_to_protobuf(#message_request{stm_id = StmId, lower_seq_num = LoSeqNum,
    upper_seq_num = UpSeqNum}) ->
    #'MessageRequest'{stm_id = StmId, lower_seq_num = LoSeqNum,
        upper_seq_num = UpSeqNum};
translate_to_protobuf(#message_acknowledgement{stm_id = StmId, seq_num = SeqNum}) ->
    #'MessageAcknowledgement'{stm_id = StmId, seq_num = SeqNum};
translate_to_protobuf(#pong{}) ->
    #'Pong'{};
translate_to_protobuf(undefined) ->
    undefined.

%%%===================================================================
%%% Internal functions
%%%===================================================================
