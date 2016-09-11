%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal versions of common protocol messages.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(COMMUNICATION_MESSAGES_HRL).
-define(COMMUNICATION_MESSAGES_HRL, 1).

-record(message_stream, {
    stream_id :: sequencer:stream_id(),
    sequence_number :: undefined | sequencer:sequence_number()
}).

-record(message_stream_reset, {
    stream_id :: undefined | sequencer:stream_id()
}).

-record(end_of_message_stream, {
}).

-record(message_request, {
    stream_id :: sequencer:stream_id(),
    lower_sequence_number :: sequencer:sequence_number(),
    upper_sequence_number :: sequencer:sequence_number()
}).

-record(message_acknowledgement, {
    stream_id :: sequencer:stream_id(),
    sequence_number :: sequencer:sequence_number()
}).

-endif.
