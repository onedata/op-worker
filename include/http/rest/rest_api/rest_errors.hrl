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
-define(ERROR_INVALID_ATTRIBUTE, {?BAD_REQUEST, [{<<"error">>, <<"invalid_attribute">>},
    {<<"error_description">>, <<"Given attribute is not valid">>}]}).
-define(ERROR_INVALID_ATTRIBUTE_BODY, {?BAD_REQUEST, [{<<"error">>, <<"invalid_attribute_body">>},
    {<<"error_description">>, <<"Request's body is malformed, provide one valid attribute with its value.">>}]}).
-define(ERROR_INVALID_MODE, {?BAD_REQUEST, [{<<"error">>, <<"invalid_mode">>},
    {<<"error_description">>, <<"Given mode is invalid, it should be provided in octal form.">>}]}).
-define(ERROR_TOO_MANY_ENTRIES, {?BAD_REQUEST, [{<<"error">>, <<"too_many_entries">>},
    {<<"error_description">>, <<"The directory contains too many entries to list them all, ask for specific range.">>}]}).
-define(ERROR_INVALID_METRIC, {?BAD_REQUEST, [{<<"error">>, <<"invalid_metric">>},
    {<<"error_description">>, <<"Requested metric is invalid.">>}]}).
-define(ERROR_INVALID_STEP, {?BAD_REQUEST, [{<<"error">>, <<"invalid_step">>},
    {<<"error_description">>, <<"Requested step is invalid.">>}]}).
-define(ERROR_INVALID_TIMEOUT, {?BAD_REQUEST, [{<<"error">>, <<"invalid_timeout">>},
    {<<"error_description">>, <<"Requested timeout is invalid, it must be of integer type.">>}]}).
-define(ERROR_INVALID_LAST_SEQ, {?BAD_REQUEST, [{<<"error">>, <<"invalid_last_seq">>},
    {<<"error_description">>, <<"Requested last_seq is invalid, it must be of integer type.">>}]}).
-define(ERROR_INVALID_OFFSET, {?BAD_REQUEST, [{<<"error">>, <<"invalid_offset">>},
    {<<"error_description">>, <<"Requested offset is invalid, it must be of integer type.">>}]}).
-define(ERROR_INVALID_LIMIT, {?BAD_REQUEST, [{<<"error">>, <<"invalid_limit">>},
    {<<"error_description">>, <<"Requested limit is invalid, it must be of integer type.">>}]}).
-define(ERROR_LIMIT_TOO_LARGE(Max), {?BAD_REQUEST, [{<<"error">>, <<"limit_too_large">>},
    {<<"error_description">>, <<"Requested limit exceeds maximal value of ", (integer_to_binary(Max))/binary, ".">>}]}).

%% HTTP 401 errors
-define(ERROR_UNAUTHORIZED, {?NOT_AUTHORIZED, [{<<"error">>, <<"unauthorized">>},
    {<<"error_description">>, <<"Error unauthorized.">>}]}).

%% HTTP 403 errors
-define(ERROR_PERMISSION_DENIED, {?FORBIDDEN, [{<<"error">>, <<"permission_denied">>},
    {<<"error_description">>, <<"Permission denied.">>}]}).
-define(ERROR_FORBIDDEN, {?FORBIDDEN, [{<<"error">>, <<"forbidden">>},
    {<<"error_description">>, <<"Operation not permitted.">>}]}).

%% HTTP 404 errors
-define(ERROR_NOT_FOUND, {?NOT_FOUND, [{<<"error">>, <<"not_found">>},
    {<<"error_description">>, <<"The resource could not be found.">>}]}).
-define(ERROR_SPACE_NOT_FOUND, {?NOT_FOUND, [{<<"error">>, <<"space_not_found">>},
    {<<"error_description">>, <<"The space could not be found.">>}]}).

-endif.