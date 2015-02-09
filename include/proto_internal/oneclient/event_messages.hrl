%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% internal protocol event messages
%%% @end
%%%-------------------------------------------------------------------

-ifndef(EVENT_MESSAGES_HRL).
-define(EVENT_MESSAGES_HRL, 1).

-record(event_subscription_cancellation, {
    id :: binary()
}).

-endif.
