%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal server message_id (to client it is opaque as it goes as binary)
%%% @end
%%%-------------------------------------------------------------------

-ifndef(MESSAGE_ID_HRL).
-define(MESSAGE_ID_HRL, 1).

%% Message ID containing recipient for remote response.
-record(message_id, {
    issuer :: undefined | message_id:issuer(),
    id :: undefined | binary(),
    recipient :: binary() | undefined % pid encoded with term_to_binary,
    % decode only if pid is local
}).

-endif.
