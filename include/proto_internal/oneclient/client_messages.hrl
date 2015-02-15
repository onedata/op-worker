%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal version of client protocol message.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(CLIENT_MESSAGES_HRL).
-define(CLIENT_MESSAGES_HRL, 1).

-record(client_message, {
    message_id :: message_id:message_id(),
    stream_id :: non_neg_integer(),
    seq_num :: non_neg_integer(),
    last_message :: boolean(),
    session_id :: session:session_id(),
    client_message :: tuple()
}).

-endif.
