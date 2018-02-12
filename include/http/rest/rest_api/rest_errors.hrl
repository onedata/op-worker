%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This file defines errors thrown while handling rest request.
%%% @end
%%%--------------------------------------------------------------------

-include("http/rest/http_status.hrl").

-ifndef(REST_ERRORS_HRL).
-define(REST_ERRORS_HRL, 1).

-define(ERROR_REPLY(Code, Error, ErrorDescription), {Code, #{
    <<"error">> => Error,
    <<"error_description">> => ErrorDescription
}}).

%% HTTP 400 errors
-define(ERROR_INVALID_ATTRIBUTE, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_attribute">>,
    <<"Given attribute is not valid">>)
).
-define(ERROR_UNDEFINED_ATTRIBUTE, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"undefined_attribute">>,
    <<"You must define attribute name when requesting for extended attribute.">>)
).
-define(ERROR_INVALID_ATTRIBUTE_BODY, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_attribute_body">>,
    <<"Request's body is malformed, provide one valid attribute with its value.">>)
).
-define(ERROR_INVALID_ATTRIBUTE_NAME, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_attribute_name">>,
    <<"Request's attribute name is invalid.">>)
).
-define(ERROR_INVALID_EXTENDED_FLAG, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_extended_flag">>,
    <<"Given extended flag is not a valid boolean.">>)
).
-define(ERROR_INVALID_INHERITED_FLAG, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_inherited_flag">>,
    <<"Given inherited flag is not a valid boolean.">>)
).
-define(ERROR_INVALID_MODE, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_mode">>,
    <<"Given mode is invalid, it should be provided in octal form.">>)
).
-define(ERROR_TOO_MANY_ENTRIES, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"too_many_entries">>,
    <<"The directory contains too many entries to list them all, ask for specific range.">>)
).
-define(ERROR_INVALID_METRIC, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_metric">>,
    <<"Requested metric is invalid.">>)
).
-define(ERROR_INVALID_STEP, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_step">>,
    <<"Requested step is invalid.">>)
).
-define(ERROR_INVALID_TIMEOUT, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_timeout">>,
    <<"Requested timeout is invalid, it must be of integer type.">>)
).
-define(ERROR_INVALID_LAST_SEQ, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_last_seq">>,
    <<"Requested last_seq is invalid, it must be of integer type.">>)
).
-define(ERROR_INVALID_OFFSET, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_offset">>,
    <<"Requested offset is invalid, it must be of integer type.">>)
).
-define(ERROR_INVALID_LIMIT, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_limit">>,
    <<"Requested limit is invalid, it must be of integer type.">>)
).

-define(ERROR_LIMIT_TOO_LARGE(Max), ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"limit_too_large">>,
    <<"Requested limit exceeds maximal value of ", (integer_to_binary(Max))/binary, ".">>)
).
-define(ERROR_INVALID_STATUS, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_status">>,
    <<"Requested transfer status is invalid.">>)
).
-define(ERROR_INVALID_METADATA_TYPE, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_metadata_type">>,
    <<"Given metadatadata type is invalid for selected contentent type.">>)
).
-define(ERROR_SPACE_NOT_PROVIDED, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"space_not_provided">>,
    <<"Required space_id was not provided.">>)
).
-define(ERROR_INVALID_DESCENDING, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_descending">>,
    <<"The descending parameter is invalid, it must be of boolean type.">>)
).
-define(ERROR_INVALID_INCLUSIVE_END, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_inclusive_end">>,
    <<"The inclusive_end parameter is invalid, it must be of boolean type.">>)
).
-define(ERROR_INVALID_STALE, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_stale">>,
    <<"The stale parameter is invalid, it must be one of: 'ok', 'update_after', 'false'.">>)
).
-define(ERROR_INVALID_SKIP, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_skip">>,
    <<"The skip parameter is invalid, it must be of integer type.">>)
).
-define(ERROR_INVALID_FILTER_TYPE, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_filter_type">>,
    <<"The filter_type parameter is invalid.">>)
).
-define(ERROR_INVALID_FILTER, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_filter">>,
    <<"The filter parameter is invalid.">>)
).
-define(ERROR_MISSING_FILTER, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"missing_filter">>,
    <<"The filter parameter is missing.">>)
).
-define(ERROR_INVALID_OBJECTID, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_objectid">>,
    <<"Given id is invalid.">>)
).
-define(ERROR_INVALID_ENDKEY, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_endkey">>,
    <<"The endkey parameter is invalid, it must be a valid json.">>)
).
-define(ERROR_INVALID_STARTKEY, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_startkey">>,
    <<"The startkey parameter is invalid, it must be a valid json.">>)
).
-define(ERROR_INVALID_END_RANGE, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_end_range">>,
    <<"The end_range parameter is invalid, it must be a valid json.">>)
).
-define(ERROR_INVALID_START_RANGE, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_start_range">>,
    <<"The start_range parameter is invalid, it must be a valid json.">>)
).
-define(ERROR_INVALID_KEY, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_key">>,
    <<"The key parameter is invalid, it must be a valid json.">>)
).
-define(ERROR_INVALID_KEYS, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_keys">>,
    <<"The keys parameter is invalid, it must be a valid json list.">>)
).
-define(ERROR_INVALID_SPATIAL_FLAG, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_spatial_flag">>,
    <<"Given spatial flag is not a valid boolean.">>)
).
-define(ERROR_INVALID_BBOX, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"error_invalid_bbox">>,
    <<"Bounding box is invalid, it needs to be bbox=W,S,E,N where each direction is a number.">>)
).
-define(ERROR_SPACE_NOT_SUPPORTED, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"error_space_not_supported">>,
    <<"The space of requested file is not locally supported.">>)
).
-define(ERROR_PROVIDER_NOT_FOUND, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"error_provider_not_found">>,
    <<"Given provider could not be found.">>)
).
-define(ERROR_PROVIDER_NOT_SUPPORTING_SPACE, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"error_provider_not_supporting_space">>,
    <<"Given provider does not support requested space.">>)
).
-define(ERROR_NON_LOCAL_PROVIDER, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"error_non_local_provider">>,
    <<"The given provider is not the local one. Currently invalidation is only supported on provider handling the request.">>)
).
-define(ERROR_NOT_TARGET_PROVIDER, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"error_not_target_provider">>,
    <<"Local provider is not target provider for given replication transfer.
    Replication transfers can only be restarted by target providers">>)
).
-define(ERROR_NOT_SOURCE_PROVIDER, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"error_not_source_provider">>,
    <<"Local provider is not source provider for given migration/invalidation transfer.
    Replication and invalidation transfers can only be restarted by source providers">>)
).
-define(ERROR_TRANSFER_NOT_FOUND, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"error_transfer_not_found">>,
    <<"Given transfer could not be found.">>)
).

%% HTTP 401 errors
-define(ERROR_UNAUTHORIZED, ?ERROR_REPLY(
    ?NOT_AUTHORIZED,
    <<"unauthorized">>,
    <<"Error unauthorized.">>)
).

%% HTTP 403 errors
-define(ERROR_PERMISSION_DENIED, ?ERROR_REPLY(
    ?FORBIDDEN,
    <<"permission_denied">>,
    <<"Permission denied.">>)
).
-define(ERROR_FORBIDDEN, ?ERROR_REPLY(
    ?FORBIDDEN,
    <<"forbidden">>,
    <<"Operation not permitted.">>)
).

%% HTTP 404 errors
-define(ERROR_NOT_FOUND, ?ERROR_REPLY(
    ?NOT_FOUND,
    <<"not_found">>,
    <<"The resource could not be found.">>)
).
-define(ERROR_SPACE_NOT_FOUND, ?ERROR_REPLY(
    ?NOT_FOUND,
    <<"space_not_found">>,
    <<"The space could not be found.">>)
).

-endif.