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

-include("cluster_elements/protocol_handler/message_id.hrl").

-record(server_message, {
    message_id :: #message_id{} | undefined,
    stream_id :: non_neg_integer() | undefined,
    seq_num :: non_neg_integer() | undefined,
    last_message :: boolean() | undefined,
    server_message :: tuple()
}).

-endif.
