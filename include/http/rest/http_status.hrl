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

-define(HTTP_200_OK, 200).
-define(HTTP_206_PARTIAL_CONTENT, 206).

-define(HTTP_301_MOVED_PERMANENTLY, 301).
-define(HTTP_400_BAD_REQUEST, 400).
-define(HTTP_401_NOT_AUTHORIZED, 401).
-define(HTTP_403_FORBIDDEN, 403).
-define(HTTP_404_NOT_FOUND, 404).
-define(HTTP_409_CONFLICT, 409).
-define(HTTP_415_UNSUPPORTED_MEDIA_TYPE, 415).
-define(HTTP_426_UPGRADE_REQUIRED, 426).
-define(HTTP_500_INTERNAL_SERVER_ERROR, 500).

-endif.
