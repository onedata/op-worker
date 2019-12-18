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
-include("proto/common/handshake_messages.hrl").

-record(client_message, {
    message_id :: undefined | clproto_message_id:id(),
    session_id :: undefined | session:id(),
    % session id on behalf which operations should be carried
    effective_session_id :: undefined | session:id(),
    effective_session_credentials :: undefined | auth_manager:credentials(),
    message_stream :: undefined | #message_stream{},
    message_body :: tuple()
}).

-define(CLIENT_KEEPALIVE_MSG, <<"KA">>).

-endif.
