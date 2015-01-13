%%%-------------------------------------------------------------------
%%% @author lichon
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Jan 2015 17:02
%%%-------------------------------------------------------------------
-author("lichon").

%% Path (relative to domain) on which cowboy expects client's requests
-define(ONECLIENT_URI_PATH, "/oneclient").
-define(ONEPROVIDER_URI_PATH, "/oneprovider").

%% ------------
% GUI and cowboy related defines

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