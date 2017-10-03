%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Protocol messages for remote driver.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(REMOTE_DRIVER_MESSAGES_HRL).
-define(REMOTE_DRIVER_MESSAGES_HRL, 1).

-include("proto/oneclient/common_messages.hrl").

-record(get_remote_document, {
    model :: datastore_model:model(),
    key :: datastore:key(),
    routing_key :: undefined | datastore:key()
}).

-record(remote_document, {
    status :: #status{},
    compressed_data :: undefined | binary()
}).

-endif.