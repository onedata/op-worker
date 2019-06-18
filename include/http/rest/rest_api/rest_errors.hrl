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

-include("http/rest/rest.hrl").

-ifndef(REST_ERRORS_HRL).
-define(REST_ERRORS_HRL, 1).

-define(ERROR_ALREADY_EXISTS, {error, already_exists}).

-define(ERROR_BAD_VALUE_DIRECTORY, {error, bad_directory}).
-define(ERROR_BAD_VALUE_AMBIGUOUS_ID(__Key), {error, {ambiguous_id, __Key}}).

-define(ERROR_SPACE_NOT_SUPPORTED, {error, space_not_supported}).
-define(ERROR_SPACE_NOT_SUPPORTED_BY(__ProviderId), {error, {space_not_supported_by, __ProviderId}}).

-define(ERROR_INDEX_NOT_SUPPORTED_BY(__ProviderId), {error, {index_not_supported_by, __ProviderId}}).

-define(ERROR_TRANSFER_ALREADY_ENDED, {error, transfer_already_ended}).
-define(ERROR_TRANSFER_NOT_ENDED, {error, transfer_not_ended}).


-define(ERROR_REPLY(Code, Error, ErrorDescription), {Code, #{
    <<"error">> => Error,
    <<"error_description">> => ErrorDescription
}}).

%% HTTP 400 errors
-define(ERROR_INVALID_INHERITED_FLAG, ?ERROR_REPLY(
    ?HTTP_400_BAD_REQUEST,
    <<"invalid_inherited_flag">>,
    <<"Given inherited flag is not a valid boolean.">>)
).
-define(ERROR_INVALID_METRIC, ?ERROR_REPLY(
    ?HTTP_400_BAD_REQUEST,
    <<"invalid_metric">>,
    <<"Requested metric is invalid.">>)
).
-define(ERROR_INVALID_STEP, ?ERROR_REPLY(
    ?HTTP_400_BAD_REQUEST,
    <<"invalid_step">>,
    <<"Requested step is invalid.">>)
).
-define(ERROR_INVALID_TIMEOUT, ?ERROR_REPLY(
    ?HTTP_400_BAD_REQUEST,
    <<"invalid_timeout">>,
    <<"Requested timeout is invalid, it must be of integer type.">>)
).
-define(ERROR_INVALID_LAST_SEQ, ?ERROR_REPLY(
    ?HTTP_400_BAD_REQUEST,
    <<"invalid_last_seq">>,
    <<"Requested last_seq is invalid, it must be of integer type.">>)
).
-define(ERROR_INVALID_METADATA_TYPE, ?ERROR_REPLY(
    ?HTTP_400_BAD_REQUEST,
    <<"invalid_metadata_type">>,
    <<"Given metadatadata type is invalid for selected contentent type.">>)
).
-define(ERROR_INVALID_FILTER_TYPE, ?ERROR_REPLY(
    ?HTTP_400_BAD_REQUEST,
    <<"invalid_filter_type">>,
    <<"The filter_type parameter is invalid.">>)
).
-define(ERROR_INVALID_FILTER, ?ERROR_REPLY(
    ?HTTP_400_BAD_REQUEST,
    <<"invalid_filter">>,
    <<"The filter parameter is invalid.">>)
).
-define(ERROR_MISSING_FILTER, ?ERROR_REPLY(
    ?HTTP_400_BAD_REQUEST,
    <<"missing_filter">>,
    <<"The filter parameter is missing.">>)
).
-define(ERROR_INVALID_OBJECTID, ?ERROR_REPLY(
    ?HTTP_400_BAD_REQUEST,
    <<"invalid_objectid">>,
    <<"Given id is invalid.">>)
).
-define(ERROR_INVALID_BBOX, ?ERROR_REPLY(
    ?HTTP_400_BAD_REQUEST,
    <<"error_invalid_bbox">>,
    <<"Bounding box is invalid, it needs to be bbox=W,S,E,N where each direction is a number.">>)
).

% Changes errors
-define(ERROR_INVALID_CHANGES_REQ, ?ERROR_REPLY(
    ?HTTP_400_BAD_REQUEST,
    <<"error_invalid_changes_req">>,
    <<"Given metadata changes request has invalid format.">>
)).
-define(ERROR_INVALID_FIELD(__Record, __Field), ?ERROR_REPLY(
    ?HTTP_400_BAD_REQUEST,
    <<"error_invalid_field">>,
    <<
        "Given field(s) \"", __Field/binary, "\"
        is/are invalidly specified for ", __Record/binary, " record."
    >>
)).
-define(ERROR_INVALID_FORMAT(__Record), ?ERROR_REPLY(
    ?HTTP_400_BAD_REQUEST,
    <<"error_invalid_format">>,
    <<"Invalid record \"", __Record/binary, "\" or it's specification.">>
)).

%% HTTP 401 errors
-define(ERROR_UNAUTHORIZED_REST, ?ERROR_REPLY(
    ?HTTP_401_UNAUTHORIZED,
    <<"unauthorized">>,
    <<"Error unauthorized.">>)
).

%% HTTP 403 errors
-define(ERROR_PERMISSION_DENIED, ?ERROR_REPLY(
    ?HTTP_403_FORBIDDEN,
    <<"permission_denied">>,
    <<"Permission denied.">>)
).
-define(ERROR_FORBIDDEN_REST, ?ERROR_REPLY(
    ?HTTP_403_FORBIDDEN,
    <<"forbidden">>,
    <<"Operation not permitted.">>)
).

%% HTTP 404 errors
-define(ERROR_NOT_FOUND_REST, ?ERROR_REPLY(
    ?HTTP_404_NOT_FOUND,
    <<"not_found">>,
    <<"The resource could not be found.">>)
).
-define(ERROR_SPACE_NOT_FOUND, ?ERROR_REPLY(
    ?HTTP_404_NOT_FOUND,
    <<"space_not_found">>,
    <<"The space could not be found.">>)
).

-endif.