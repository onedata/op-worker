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

% POSIX error code
-type code() :: 'VOK' |
                'VENOENT' |
                'VEACCES' |
                'VEEXIST' |
                'VEIO' |
                'VENOTSUP' |
                'VENOTEMPTY' |
                'VEREMOTEIO' |
                'VEPERM' |
                'VEINVAL' |
                'VEDQUOT' |
                'VENOATTR' |
                'VECOMM'.

-record(status, {
    code :: code(),
    description :: binary()
}).

-record(message_stream, {
    stm_id :: non_neg_integer(),
    seq_num :: non_neg_integer(),
    eos :: boolean()
}).

-record(message_stream_reset, {
    seq_num :: non_neg_integer()
}).

-record(message_request, {
    stm_id :: non_neg_integer(),
    lower_seq_num :: non_neg_integer(),
    upper_seq_num :: non_neg_integer()
}).

-record(message_acknowledgement, {
    stm_id :: non_neg_integer(),
    seq_num :: non_neg_integer()
}).

-endif.
