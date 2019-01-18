%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% HTTP status codes
%%% @end
%%%--------------------------------------------------------------------
-ifndef(HTTP_STATUS_HRL).
-define(HTTP_STATUS_HRL, 1).

-define(HTTP_OK, 200).
-define(PARTIAL_CONTENT, 206).

-define(MOVED_PERMANENTLY, 301).
-define(BAD_REQUEST, 400).
-define(NOT_AUTHORIZED, 401).
-define(FORBIDDEN, 403).
-define(NOT_FOUND, 404).
-define(CONFLICT, 409).
-define(UNSUPPORTED_MEDIA_TYPE, 415).
-define(INTERNAL_SERVER_ERROR, 500).

-endif.
