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

%% HTTP error code 400 and request sent to client
-define(unsupported_version,
  {?BAD_REQUEST, [{<<"error_unsupported_version">>, <<"Given CDMI version is not supported. Use 1.1.1 instead.">>}]}).
-define(no_version_given,
  {?BAD_REQUEST, [{<<"error_no_version_given">>, <<"No CDMI version given. Add valid 'X-CDMI-Specification-Version' header.">>}]}).
-define(conflicting_body_fields,
  {?BAD_REQUEST, [{<<"error_conflicting_body_fields">>, <<"Request body contains fields that are in conflict with each other.">>}]}).
-define(duplicated_body_fields,
  {?BAD_REQUEST, [{<<"error_duplicated_body_fields">>, <<"Request body contains duplicates.">>}]}).
-define(invalid_childrenrange,
  {?BAD_REQUEST, [{<<"error_invalid_childrenrange">>, <<"Requested childrenrange is invalid.">>}]}).
-define(too_large_childrenrange(MaxChildren),
  {?BAD_REQUEST, [{<<"error_too_large_childrenrange">>, <<"Requested childrenrange exceeds the limit of ", MaxChildren/integer, " entries.">>}]}).
-define(invalid_range,
  {?BAD_REQUEST, [{<<"error_invalid_range">>, <<"Given range is invalid.">>}]}).
-define(invalid_base64,
  {?BAD_REQUEST, [{<<"error_invalid_base64">>, <<"Given base64 value could not be docoded.">>}]}).
-define(invalid_objectid,
  {?BAD_REQUEST, [{<<"error_invalid_objectid">>, <<"Given objectid is invalid.">>}]}).
-define(wrong_path,
  {?BAD_REQUEST, [{<<"error_wrong_path">>, <<"Requested uri is invalid, check the trailing slash.">>}]}).

%% HTTP error code 403 and request sent to client
-define(forbidden,
  {?FORBIDDEN, [{<<"forbidden">>,
    <<"The client lacks the proper authorization to perform this request.">>}]}).

%% HTTP error code 500 and request sent to client
-define(write_object_unknown_error,
  {?INTERNAL_SERVER_ERROR, [{<<"write_object_unknown_error">>, <<"Uknown error while writing to object">>}]}).
-define(put_object_unknown_error,
  {?INTERNAL_SERVER_ERROR, [{<<"put_object_unknown_error">>, <<"Uknown error when putting object">>}]}).

-endif.