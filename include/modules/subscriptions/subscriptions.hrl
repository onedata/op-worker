%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C): 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Definitions of names used by subscriptions related functions.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(SUBSCRIPTIONS_HRL).
-define(SUBSCRIPTIONS_HRL, 1).

-define(SUBSCRIPTIONS_WORKER_NAME, subscriptions_worker).
-define(SUBSCRIPTIONS_STATE_KEY, <<"current_state">>).

-record(sub_update, {
    delete = false :: boolean(),
    ignore = false :: boolean(),

    seq :: subscriptions:seq(),
    doc :: undefined | datastore:document(),
    id :: datastore:ext_key(),
    model :: undefined | subscriptions:model(),
    revs :: undefined | [subscriptions:rev()]
}).

-endif.