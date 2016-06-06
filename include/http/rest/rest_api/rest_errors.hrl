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

%% HTTP 400 errors
-define(ERROR_INVALID_ATTRIBUTE,
  {?BAD_REQUEST, [{<<"error_invalid_attribute">>, <<"Given attribute is not valid">>}]}).
-define(ERROR_INVALID_ATTRIBUTE_BODY,
  {?BAD_REQUEST, [{<<"error_invalid_attribute_body">>, <<"Request's body is malformed, provide one valid attribute with its value.">>}]}).
-define(ERROR_INVALID_MODE,
  {?BAD_REQUEST, [{<<"error_invalid_mode">>, <<"Given mode is invalid, it should be provided in octal form.">>}]}).
-define(ERROR_TOO_MANY_ENTRIES,
  {?BAD_REQUEST, [{<<"error_too_many_entries">>, <<"The directory contains too many entries to list them all, ask for specific range.">>}]}).
-define(ERROR_INVALID_METRIC,
  {?BAD_REQUEST, [{<<"error_invalid_metric">>, <<"Requested metric is invalid.">>}]}).
-define(ERROR_INVALID_STEP,
  {?BAD_REQUEST, [{<<"error_invalid_step">>, <<"Requested step is invalid.">>}]}).
-define(ERROR_INVALID_TIMEOUT,
  {?BAD_REQUEST, [{<<"error_invalid_timeout">>, <<"Requested timeout is invalid, it must be of integer type.">>}]}).
-define(ERROR_INVALID_LAST_SEQ,
  {?BAD_REQUEST, [{<<"error_invalid_last_seq">>, <<"Requested last_seq is invalid, it must be of integer type.">>}]}).

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