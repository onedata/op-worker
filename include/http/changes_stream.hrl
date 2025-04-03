%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions for changes stream.
%%% @end
%%%-------------------------------------------------------------------

-include("http/http_auth.hrl").
-include("http/rest.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").

-ifndef(CHANGES_STREAM_HRL).
-define(CHANGES_STREAM_HRL, 1).


-define(DEFAULT_TIMEOUT, <<"infinity">>).
-define(DEFAULT_LAST_SEQ, <<"now">>).
-define(DEFAULT_ALWAYS, false).

-define(OBSERVABLE_DOCUMENTS, [
    <<"fileMeta">>, <<"fileLocation">>, <<"times">>, <<"customMetadata">>
]).

-define(OBSERVABLE_FILE_META_FIELDS, [
    <<"name">>, <<"type">>, <<"mode">>, <<"owner">>,
    <<"provider_id">>, <<"shares">>, <<"deleted">>
]).

-define(OBSERVABLE_FILE_LOCATION_FIELDS, [
    <<"provider_id">>, <<"storage_id">>, <<"size">>, <<"space_id">>,
    <<"storage_file_created">>
]).

-define(OBSERVABLE_TIME_FIELDS, [
    <<"atime">>, <<"mtime">>, <<"ctime">>
]).

-define(ONEDATA_SPECIAL_XATTRS, [<<"onedata_json">>, <<"onedata_rdf">>]).


-record(doc_monitoring_spec, {
    doc_type :: changes_stream_processor:observable_doc_type(),
    always_include_in_other_docs_changes = false :: boolean(),
    observed_fields_for_values = [] :: [binary() | {binary(), integer()}],
    observed_fields_for_existence = [] :: [binary()]
}).

-record(changes_monitoring_spec, {
    space_id :: od_space:id(),

    timeout :: infinity | integer(),
    start_after_seq :: null | integer(),
    %% TODO implement
%%    finish_at_seq :: null | integer(),

    triggers :: changes_stream_processor:triggers(),
    %% TODO implement
%%    observed_directories :: [file_id:file_guid()],
    doc_monitoring_specs :: [changes_stream_processor:doc_monitoring_spec()]
}).


-endif.
