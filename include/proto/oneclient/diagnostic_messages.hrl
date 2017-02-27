%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal version of diagnostic protocol messages
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DIAGNOSTIC_MESSAGES_HRL).
-define(DIAGNOSTIC_MESSAGES_HRL, 1).

-include("modules/events/subscriptions.hrl").

-record(ping, {
    data :: binary()
}).

-record(pong, {
    data :: binary()
}).

-record(get_protocol_version, {}).

-record(protocol_version, {
    major = 3 :: non_neg_integer(),
    minor = 0 :: non_neg_integer()
}).

-record(get_configuration, {}).

-record(configuration, {
    root_guid :: binary(),
    subscriptions = [] :: [#subscription{}],
    disabled_spaces = [] :: [od_space:id()]
}).

-endif.
