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

-include("modules/dbsync/dbsync.hrl").

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
    timestamp :: dbsync_changes:timestamp(),
    compressed_docs :: binary()
}).

-record(changes_request2, {
    space_id :: od_space:id(),
    since :: couchbase_changes:since(),
    until :: couchbase_changes:until()
}).

% TODO VFS-7031 - use in standard dbsync flow
-record(custom_changes_request, {
    space_id :: od_space:id(),
    since :: couchbase_changes:since(),
    until :: couchbase_changes:until(),
    reference_provider_id :: od_provider:id(), % id of provider which sequence numbers are used to determine
                                               % first and last document to be synchronized
                                               % (each provider has own sequence numbers)
    trim_skipped = false :: boolean() % allows reduction of sequences range when only changes of single
                                      % provider are needed (see dbsync_seqs_correlations_history.erl)
}).

-endif.