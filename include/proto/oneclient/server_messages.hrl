%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal version of server protocol and processing status messages.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(SERVER_MESSAGES_HRL).
-define(SERVER_MESSAGES_HRL, 1).

-include("stream_messages.hrl").
-include("message_id.hrl").

-record(server_message, {
    message_id :: undefined | message_id:id(),
    message_stream :: undefined | #message_stream{},
    message_body :: tuple(),
    proxy_session_id :: undefined | session:id()
}).

-record(processing_status, {
    code :: 'IN_PROGRESS' | 'ERROR'
}).

-endif.
