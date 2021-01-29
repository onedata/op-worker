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
    compressed_docs :: binary(),
    % TODO VFS-7262 - rename field when requesting changes of multiple providers is integrated with dbsync
    mutator_id = <<>> :: oneprovider:id(),
    custom_request_extension = <<>> :: dbsync_worker:custom_request_extension()
}).

% Record used to represent changes batch internally.
% It is similar to #changes_batch{} but stores uncompressed documents.
% It does not contain space id as each stream is connected with single space.
% For same reason it contains distributor_id instead of mutator_id
% (each stream processes changes produced by a single mutator that can be
% provided by multiple distributors).
-record(internal_changes_batch, {
    since :: couchbase_changes:since(),
    until :: couchbase_changes:until(),
    timestamp :: dbsync_changes:timestamp(),
    docs :: dbsync_worker:batch_docs(),
    distributor_id :: oneprovider:id(),
    custom_request_extension = <<>> :: dbsync_worker:custom_request_extension()
}).

-record(changes_request2, {
    space_id :: od_space:id(),
    since :: couchbase_changes:since(),
    until :: couchbase_changes:until()
}).

-record(custom_changes_request, {
    space_id :: od_space:id(),
    since :: couchbase_changes:since(),
    until :: couchbase_changes:until(),
    mutator_id :: od_provider:id(), % id of provider which sequence numbers are used to determine
                                    % first and last document to be synchronized
                                    % (each provider has own sequence numbers)
    include_mutators = all_providers :: dbsync_worker:mutators_to_include() % allows reduction of documents amount
                                                                            % to be sent when only changes of
                                                                            % single provider are needed
}).

-endif.