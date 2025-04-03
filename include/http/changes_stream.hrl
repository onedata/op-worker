%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions for changes stream - a mechanism for observing
%%% changes happening to files in a space.
%%%
%%% Possible documents and their fields to observe:
%%%
%%% fileMeta (file_meta)
%%%     - name
%%%     - type
%%%     - mode
%%%     - owner
%%%     - group_owner
%%%     - provider_id
%%%     - shares
%%%     - deleted
%%%
%%% fileLocation (file_location)
%%%     - provider_id
%%%     - storage_id
%%%     - size
%%%     - space_id
%%%     - storage_file_created
%%%
%%% times (times)
%%%     - atime
%%%     - mtime
%%%     - ctime
%%%
%%% customMetadata (custom_metadata)
%%%     - onedata_json
%%%     - onedata_rdf
%%%     - onedata_keyvalue
%%%     ...
%%%
%%% Important notes:
%%% 1. For file_meta, file_location and times, fields correspond one-to-one
%%%    to those held in records
%%% 2. For custom_metadata, elements from value map field in record can be 
%%%    requested individually or special metakey 'onedata_keyvalue' can be used
%%%    to observe all elements beside onedata_json and onedata_rdf
%%% 
%%% 
%%% ## Changes monitoring specification and events
%%% 
%%% To open a changes stream, user must specify:
%%% 1. Space ID to observe changes in
%%% 2. Timeout for the stream (optional, default: infinity)
%%% 3. Start after sequence number (optional, default: now)
%%% 4. Triggers - list of document types whose changes should trigger sending events (optional, default: all)
%%% 5. Document monitoring specifications - for each document type:
%%%    - fields to return (`fields`)
%%%    - fields for which to check existence (`exists` - only for customMetadata)
%%%    - whether to always include this document's info (`always` flag, default: false)
%%%      When true, document info is included even when other documents change.
%%%      When false, document info is only included when this document changes.
%%%
%%% Document monitoring specification format:
%%% <record>:
%%%     [fields: <fields>]
%%%     [exists: <fields>]
%%%     [always: boolean()]
%%%
%%% The response will include:
%%% - fileId - CDMI object ID
%%% - filePath - canonical path
%%% - seq - change sequence number
%%% - For each monitored document type:
%%%   - rev - document revision
%%%   - mutators - list of changing providers
%%%   - changed - whether this document triggered the event
%%%   - deleted - whether document was deleted
%%%   - fields - requested field values
%%%   - exists - existence status of requested fields (customMetadata only)
%%%
%%% 
%%% ## Example:
%%% 
%%% Request:
%%% ```json
%%% {
%%%     "timeout": 10000,
%%%     "startAfterSeq": 99,
%%%     "triggers": ["fileMeta", "times"],
%%%     "fileMeta": {
%%%         "fields": ["owner"]
%%%     },
%%%     "customMetadata": {
%%%         "fields": ["onedata_rdf"],
%%%         "exists": ["onedata_json"],
%%%         "always": true
%%%     }
%%% }
%%% ```
%%%
%%% Response:
%%% ```json
%%% {
%%%     "fileId": "00000000002C66ED677569642361626562383736303665323765313",
%%%     "filePath": "space1/my/file", 
%%%     "seq": 100,
%%%     "fileMeta": {
%%%         "rev": "2-c500a5eb026d9474429903d47841f9c5",
%%%         "mutators": ["p1.1542789098.test"],
%%%         "changed": true,
%%%         "deleted": false,
%%%         "fields": {
%%%             "owner": "john"
%%%         }
%%%     },
%%%     "customMetadata": {
%%%         "rev": "1-09f941b4e8452ef6a244c5181d894814",
%%%         "mutators": ["p1.1542789098.test"],
%%%         "changed": false,
%%%         "deleted": false,
%%%         "exists": {
%%%             "onedata_rdf": true
%%%         },
%%%         "fields": {
%%%             "onedata_json": {
%%%                 "name1": "value1",
%%%                 "name2": "value2"
%%%             }
%%%         }
%%%     }
%%% }
%%% ```
%%%-------------------------------------------------------------------

-ifndef(CHANGES_STREAM_HRL).
-define(CHANGES_STREAM_HRL, 1).


-include("http/http_auth.hrl").
-include("http/rest.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").


-define(DEFAULT_TIMEOUT, <<"infinity">>).
-define(DEFAULT_LAST_SEQ, <<"now">>).
-define(DEFAULT_ALWAYS, false).

-define(OBSERVABLE_DOCUMENTS, [
    <<"fileMeta">>, <<"fileLocation">>, <<"times">>, <<"customMetadata">>
]).

%%% NOTE: Adding new fields here is not sufficient for them to work.
%%% To enable new fields, changes_stream_parser must also be modified to handle them.
%%% Do not forget to update the documentation at the top of this file!
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

    triggers :: changes_stream_processor:triggers(),
    doc_monitoring_specs :: [changes_stream_processor:doc_monitoring_spec()]
}).


-endif.
