%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Definitions common to listeners started by node_manager.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(CLUSTER_MANAGER_LISTENERS_HRL).
-define(CLUSTER_MANAGER_LISTENERS_HRL, 1).

%% Path (relative to domain) on which cowboy expects incomming websocket connections with client and provider
-define(ONECLIENT_URI_PATH, "/oneclient").
-define(ONEPROVIDER_URI_PATH, "/oneprovider").

% Paths in gui static directory
-define(STATIC_PATHS, ["/common/", "/css/", "/flatui/", "/fonts/", "/images/", "/js/", "/n2o/"]).

% Session logic module
-define(SESSION_LOGIC_MODULE, session_logic).

% GUI routing module
-define(GUI_ROUTING_MODULE, gui_routes).

% Custom cowboy bridge module
-define(COWBOY_BRIDGE_MODULE, n2o_handler).

% Cowboy listener references
-define(DISPATCHER_LISTENER, dispatcher).
-define(HTTPS_LISTENER, https).
-define(REST_LISTENER, rest).
-define(HTTP_REDIRECTOR_LISTENER, http).

-endif.