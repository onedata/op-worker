%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc @todo: Write me!
%%% @end
%%%-------------------------------------------------------------------
-author("Rafal Slota").

-record(dbsync_request, {
    message_body
}).

-record(dbsync_response, {
    status
}).

-record(tree_broadcast, {
    depth,
    l_edge,
    r_edge,
    space_id, %% ??
    request_id,
    excluded_providers,
    message_body
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
