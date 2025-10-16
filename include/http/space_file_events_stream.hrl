%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions for space file events stream - a mechanism for observing
%%% changes happening to files in a space.
%%%-------------------------------------------------------------------

-ifndef(SPACE_FILE_EVENTS_STREAM_HRL).
-define(SPACE_FILE_EVENTS_STREAM_HRL, 1).


-include("http/http_auth.hrl").
-include("http/rest.hrl").
-include("modules/fslogic/file_attr.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").


% A heartbeat event sent periodically to clients to keep them informed about the current
% sequence number. This is necessary because a client may not receive any events for an
% extended period if no changes occur in their observed directories. However, changes may
% be happening elsewhere in the space, causing the database document sequences to advance.
% If the client disconnects and reconnects, it will send the last sequence it observed to 
% replay from, which may be significantly outdated. Without this heartbeat, the client 
% would unnecessarily replay and process (then discard) all intermediate documents from 
% that old sequence, even though none of them are relevant to its observed directories.
-record(heartbeat_event, {
    id :: binary()
}).

-record(file_deleted_event, {
    id :: binary(),
    file_guid :: file_id:file_guid(),
    parent_file_guid :: file_id:file_guid()
}).

-record(file_changed_or_created_event, {
    id :: binary(),
    file_guid :: file_id:file_guid(),
    parent_file_guid :: file_id:file_guid(),
    doc_type :: space_files_monitor_common:doc_type(),
    file_attr :: file_attr:record()
}).

% ?attr_name belongs to ?LINK_TREE_FILE_ATTRS attr group BUT the link docs are only checked
% when either:
% - other attrs from that group (file name conflicts related ones) are specified
% - `resolve_name_conflicts` attr resolution policy is specified when resolving attrs
%   (see file_attr.erl for details)
% In case of space file events this is not happening and getting name attr just gets it
% from file_meta doc
-define(OBSERVABLE_FILE_META_ATTRS, [?attr_name | ?FILE_META_ATTRS]).

-define(OBSERVABLE_FILE_ATTRS,
    lists:flatten([
        ?OBSERVABLE_FILE_META_ATTRS, ?TIMES_FILE_ATTRS, ?LOCATION_FILE_ATTRS
    ]) -- ?INTERNAL_FILE_ATTRS
).

-record(space_files_monitoring_spec, {
    observed_dirs :: [file_id:file_guid()],
    observed_attrs_per_doc :: space_files_monitor_common:observed_attrs_per_doc()
}).

-record(subscribe_req, {
    observer_pid :: pid(),
    session_id :: session:id(),
    files_monitoring_spec :: space_files_monitoring_spec:t(),
    since_seq :: undefined | couchbase_changes:seq(),
    until_seq = undefined :: undefined | couchbase_changes:seq()
}).

-record(docs_change_notification, {
    docs :: [datastore:doc()]
}).

-record(seq_advancement_notification, {
    seq :: couchbase_changes:seq()
}).

-record(observer, {
    session_id :: session:id(),
    files_monitoring_spec :: space_files_monitoring_spec:t(),
    last_seen_seq :: couchbase_changes:seq()
}).

-record(dir_monitoring_spec, {
    observers :: [pid()],
    observed_attrs_per_doc :: space_files_monitor_common:observed_attrs_per_doc()
}).

-record(monitoring, {
    current_seq :: couchbase_changes:seq(),
    observers = #{} :: #{pid() => space_files_monitor_common:observer()},
    dir_monitoring_specs = #{} :: #{file_id:file_guid() => space_files_monitor_common:dir_monitoring_spec()}
}).


-endif.
