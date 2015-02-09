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

-include("common_messages.hrl").

-record(handshake_request, {
    environment_variables = [] :: [#environment_variable{}]
}).

-record(handshake_acknowledgement, {
    fuse_id :: binary()
}).

-record(handshake_response, {
    fuse_id :: binary()
}).

-endif.
