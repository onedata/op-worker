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

-include("stream_messages.hrl").
-include("handshake_messages.hrl").

-record(client_message, {
    message_id :: message_id:id(),
    session_id :: session:id(),
    proxy_session_id :: session:id(),
    proxy_session_auth :: #token_auth{},
    message_stream :: #message_stream{},
    message_body :: tuple()
}).

-endif.
