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