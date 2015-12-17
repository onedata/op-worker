%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal version of protocol handshake messages.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(HANDSHAKE_MESSAGES_HRL).
-define(HANDSHAKE_MESSAGES_HRL, 1).

-include("proto/common/credentials.hrl").
-include("modules/events/subscriptions.hrl").

-record(handshake_request, {
    auth :: #auth{},
    session_id :: session:id()
}).

-record(handshake_response, {
    session_id :: session:id(),
    subscriptions = [] :: [#subscription{}]
}).

-endif.
