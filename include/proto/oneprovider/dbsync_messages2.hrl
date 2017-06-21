%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal protocol for DBSync messages.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DBSYNC_MESSAGES2_HRL).
-define(DBSYNC_MESSAGES2_HRL, 1).

-record(dbsync_message, {
    message_body :: dbsync_communicator:msg()
}).

-record(tree_broadcast2, {
    src_provider_id :: od_provider:id(),
    low_provider_id :: od_provider:id(),
    high_provider_id :: od_provider:id(),
    message_id :: undefined | dbsync_communicator:msg_id(),
    message_body :: dbsync_communicator:changes_batch()
}).

-record(changes_batch, {
    space_id :: od_space:id(),
    since :: couchbase_changes:since(),
    until :: couchbase_changes:until(),
    compressed_docs :: binary()
}).

-record(changes_request2, {
    space_id :: od_space:id(),
    since :: couchbase_changes:since(),
    until :: couchbase_changes:until()
}).

-endif.