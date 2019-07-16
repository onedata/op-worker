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

-define(ERROR_REPLY(Code, Error, ErrorDescription), {Code, #{
    <<"error">> => Error,
    <<"error_description">> => ErrorDescription
}}).

%% HTTP 400 errors
-define(ERROR_INVALID_OBJECTID, ?ERROR_REPLY(
    ?HTTP_400_BAD_REQUEST,
    <<"invalid_objectid">>,
    <<"Given id is invalid.">>)
).

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

-endif.