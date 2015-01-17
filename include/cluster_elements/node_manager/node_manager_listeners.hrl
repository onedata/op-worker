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
-define(static_paths, ["/common/", "/css/", "/flatui/", "/fonts/", "/images/", "/js/", "/n2o/"]).

% Session logic module
-define(session_logic_module, session_logic).

% GUI routing module
-define(gui_routing_module, gui_routes).

% Custom cowboy bridge module
-define(cowboy_bridge_module, n2o_handler).

% Cowboy listener references
-define(dispatcher_listener, dispatcher).
-define(https_listener, https).
-define(rest_listener, rest).
-define(http_redirector_listener, http).

-endif.