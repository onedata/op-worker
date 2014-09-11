%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains values returned by connection_check urls, used by
%% globalregistry to check connection
%% @end
%% ===================================================================

-ifndef(GLOBAL_REGISTRY_INTERFACING_HRL).
-define(GLOBAL_REGISTRY_INTERFACING_HRL, 1).

% Relative path to connection check endpoint (used by Global Registry)
-define(connection_check_path, <<"connection_check">>).

% Returned values on connection test endpoints
-define(gui_connection_check_value, <<"gui">>).
-define(rest_connection_check_value, <<"rest">>).

% Request param sent when redirecting to Global Registry. Its value is provider hostname
-define(referer_request_param, "referer").

% Global registry login endpoint
-define(gr_login_endpoint, "/login").

-endif.
