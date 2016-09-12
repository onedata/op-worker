%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Protocol messages for dbsync
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DBSYNC_MESSAGES_HRL).
-define(DBSYNC_MESSAGES_HRL, 1).

-record(dbsync_request, {
    message_body
}).

-record(dbsync_response, {
    status
}).

-record(tree_broadcast, {
    depth :: non_neg_integer(),
    l_edge :: oneprovider:id(),
    r_edge :: oneprovider:id(),
    space_id :: binary(),
    request_id,
    excluded_providers :: [oneprovider:id()],
    message_body :: undefined | term()
}).

-record(batch_update, {
    space_id,
    since_seq,
    until_seq,
    changes_encoded
}).

-record(status_report, {
    space_id,
    seq
}).

-record(status_request, {
}).

-record(changes_request, {
    since_seq,
    until_seq
}).

-endif.