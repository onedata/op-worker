%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal version of server protocol message.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(SERVER_MESSAGES_HRL).
-define(SERVER_MESSAGES_HRL, 1).

-record(server_message, {
    message_id = undefined :: non_neg_integer(),
    stream_id = undefined :: non_neg_integer(),
    seq_num = undefined :: non_neg_integer(),
    last_message = undefined :: boolean(),
    server_message :: tuple()
}).

-endif.
