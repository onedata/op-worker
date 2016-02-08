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

-ifndef(CDMI_ERRORS_HRL).
-define(CDMI_ERRORS_HRL, 1).

%% HTTP 400 errors
-define(ERROR_UNSUPPORTED_VERSION,
  {?BAD_REQUEST, [{<<"error_unsupported_version">>, <<"Given CDMI version is not supported. Use 1.1.1 instead.">>}]}).
-define(ERROR_NO_VERSION_GIVEN,
  {?BAD_REQUEST, [{<<"error_no_version_given">>, <<"No CDMI version given. Add valid 'X-CDMI-Specification-Version' header.">>}]}).
-define(ERROR_CONFLICTING_BODY_FIELDS,
  {?BAD_REQUEST, [{<<"error_conflicting_body_fields">>, <<"Request body contains fields that are in conflict with each other.">>}]}).
-define(ERROR_DUPLICATED_BODY_FIELDS,
  {?BAD_REQUEST, [{<<"error_duplicated_body_fields">>, <<"Request body contains duplicates.">>}]}).
-define(ERROR_INVALID_CHILDRENRANGE,
  {?BAD_REQUEST, [{<<"error_invalid_childrenrange">>, <<"Requested childrenrange is invalid.">>}]}).
-define(ERROR_TOO_LARGE_CHILDRENRANGE(MaxChildren),
  {?BAD_REQUEST, [{<<"error_too_large_childrenrange">>, <<"Requested childrenrange exceeds the limit of ", MaxChildren/integer, " entries.">>}]}).
-define(ERROR_INVALID_RANGE,
  {?BAD_REQUEST, [{<<"error_invalid_range">>, <<"Given range is invalid.">>}]}).
-define(ERROR_INVALID_BASE64,
  {?BAD_REQUEST, [{<<"error_invalid_base64">>, <<"Given base64 value could not be docoded.">>}]}).
-define(ERROR_INVALID_OBJECTID,
  {?BAD_REQUEST, [{<<"error_invalid_objectid">>, <<"Given objectid is invalid.">>}]}).
-define(ERROR_WRONG_PATH,
  {?BAD_REQUEST, [{<<"error_wrong_path">>, <<"Requested uri is invalid, check the trailing slash.">>}]}).
-define(ERROR_INVALID_ACL,
  {?BAD_REQUEST, [{<<"error_invalid_acl">>, <<"Given access control list is invalid.">>}]}).
-define(ERROR_INVALID_METADATA,
  {?BAD_REQUEST, [{<<"error_invalid_metadata">>, <<"Given metadata is invalid.">>}]}).
-define(ERROR_MALFORMED_QS,
  {?BAD_REQUEST, [{<<"error_malformed_qs">>, <<"Request's query string is malformed.">>}]}).

%% HTTP 401 errors
-define(ERROR_UNAUTHORIZED,
  {?NOT_AUTHORIZED, [{<<"error_unauthorized">>, <<"Error unauthorized.">>}]}).

%% HTTP 403 errors
-define(ERROR_PERMISSION_DENIED,
  {?FORBIDDEN, [{<<"error_permission_denied">>, <<"Permission denied.">>}]}).
-define(ERROR_FORBIDDEN,
  {?FORBIDDEN, [{<<"error_forbidden">>, <<"Operation not permitted.">>}]}).

%% HTTP 404 errors
-define(ERROR_NOT_FOUND,
  {?NOT_FOUND, [{<<"error_not_found">>, <<"The resource could not be found.">>}]}).

-endif.