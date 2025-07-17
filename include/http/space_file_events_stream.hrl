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


-define(OBSERVABLE_FILE_ATTRS,
    lists:flatten([
        % Name belongs to ?LINK_TREE_FILE_ATTRS attr group BUT it is only relevant for other
        % attrs in this group (file name conflicts related attrs) - if only name is no link
        % docs are consulted (and as such we do not need to monitor them - file_meta change
        % trigger name change event)
        ?attr_name,
        ?FILE_META_ATTRS, ?TIMES_FILE_ATTRS, ?LOCATION_FILE_ATTRS
    ]) -- ?INTERNAL_FILE_ATTRS
).


-record(space_files_monitoring_spec, {
    observed_dirs :: [file_id:file_guid()],
    observed_attrs_per_doc :: space_files_monitor:observed_attrs_per_doc()
}).

-record(file_changed_or_created_event, {
    id :: binary(),
    file_guid :: file_id:file_guid(),
    parent_file_guid :: file_id:file_guid(),
    doc_type :: file_meta | times | file_location,
    file_attr :: file_attr:record()
}).


-endif.
