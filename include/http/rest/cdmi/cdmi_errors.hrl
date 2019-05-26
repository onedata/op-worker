%%%--------------------------------------------------------------------
%%% @author Piotr Ociepka
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This file defines errors thrown while handling CDMI request.
%%% @end
%%%--------------------------------------------------------------------

-include("http/rest/http_status.hrl").
-include("http/rest/rest_api/rest_errors.hrl").

-ifndef(CDMI_ERRORS_HRL).
-define(CDMI_ERRORS_HRL, 1).

%% HTTP 400 errors
-define(ERROR_UNSUPPORTED_VERSION, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"unsupported_version">>,
    <<"Given CDMI version is not supported. Use 1.1.1 instead.">>)
).
-define(ERROR_NO_VERSION_GIVEN, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"no_version_given">>,
    <<"No CDMI version given. Add valid 'X-CDMI-Specification-Version' header.">>)
).
-define(ERROR_CONFLICTING_BODY_FIELDS, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"conflicting_body_fields">>,
    <<"Request body contains fields that are in conflict with each other.">>)
).
-define(ERROR_DUPLICATED_BODY_FIELDS, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"duplicated_body_fields">>,
    <<"Request body contains duplicates.">>)
).
-define(ERROR_INVALID_CHILDRENRANGE, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_childrenrange">>,
    <<"Requested childrenrange is invalid.">>)
).
-define(ERROR_TOO_LARGE_CHILDRENRANGE(MaxChildren), ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"too_large_childrenrange">>,
    <<"Requested childrenrange exceeds the limit of ",
        (integer_to_binary(MaxChildren))/binary, " entries.">>)
).
-define(ERROR_INVALID_RANGE, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_range">>,
    <<"Given range is invalid.">>)
).
-define(ERROR_INVALID_BASE64, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_base64">>,
    <<"Given base64 value could not be docoded.">>)
).
-define(ERROR_WRONG_PATH, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"wrong_path">>,
    <<"Requested uri is invalid, check the trailing slash.">>)
).
-define(ERROR_INVALID_ACL, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_acl">>,
    <<"Given access control list is invalid.">>)
).
-define(ERROR_INVALID_METADATA, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_metadata">>,
    <<"Given metadata is invalid.">>)
).
-define(ERROR_MALFORMED_QS, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"malformed_qs">>,
    <<"Request's query string is malformed.">>)
).
-define(ERROR_INVALID_JSON, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"invalid_json">>,
    <<"Could not parse json.">>)
).
-define(ERROR_EXISTS, ?ERROR_REPLY(
    ?BAD_REQUEST,
    <<"already_exists">>,
    <<"The resource already exists.">>)
).

%% HTTP 404 errors
-define(ERROR_ATTRIBUTE_NOT_FOUND, ?ERROR_REPLY(
    ?NOT_FOUND,
    <<"attribute_not_found">>,
    <<"The attribute could not be found.">>)
).

-endif.