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
    stream_id :: non_neg_integer(),
    sequence_number :: non_neg_integer()
}).

-record(message_stream_reset, {
}).

-record(end_of_message_stream, {
}).

-record(message_request, {
    stream_id :: non_neg_integer(),
    lower_sequence_number :: non_neg_integer(),
    upper_sequence_number :: non_neg_integer()
}).

-record(message_acknowledgement, {
    stream_id :: non_neg_integer(),
    sequence_number :: non_neg_integer()
}).

-endif.
