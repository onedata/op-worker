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
    routing_key :: datastore:key() | undefined
}).

-record(remote_driver_request, {
    request :: #get_remote_document{}
}).

-record(remote_document, {
    compressed_data :: binary()
}).

-record(remote_driver_response, {
    status :: #status{},
    response :: undefined | #remote_document{}
}).

-endif.