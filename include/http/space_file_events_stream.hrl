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


-define(OBSERVABLE_FILE_ATTRS, lists:flatten([
    ?FILE_META_ATTRS, ?TIMES_FILE_ATTRS, ?LOCATION_FILE_ATTRS, ?METADATA_FILE_ATTRS
]) -- ?INTERNAL_FILE_ATTRS).


-record(space_file_events_monitoring_spec, {
    observed_dirs :: [file_id:file_guid()],
    observed_attrs_per_doc :: #{
        file_meta => [onedata_file:attr_name()],
        times => [onedata_file:attr_name()],
        file_location => [onedata_file:attr_name()],
        custom_metadata => [onedata_file:attr_name()]
    }
}).


-endif.
