%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal protocol event messages.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(EVENT_MESSAGES_HRL).
-define(EVENT_MESSAGES_HRL, 1).

-include("workers/session/event_manager/read_event.hrl").
-include("workers/session/event_manager/write_event.hrl").

-record(event_subscription_cancellation, {
    id :: non_neg_integer()
}).

-endif.
