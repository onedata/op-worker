%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common defines used in gui modules.
%%% @end
%%%-------------------------------------------------------------------
-author("Lukasz Opiola").

%% Types of session requirements - it is used in gui_route record to
%% denote what type of session user has to have to be allowed to visit the page.
%% SESSION_ANY - user can visit the page regardless of his session type
-define(SESSION_ANY, any).
%% SESSION_LOGGED_IN - user can only visit the page if he is logged in
-define(SESSION_LOGGED_IN, logged_in).
%% SESSION_LOGGED_IN - user can only visit the page if he is NOT logged in
-define(SESSION_NOT_LOGGED_IN, not_logged_in).
%% WEBSOCKET_DISABLED - only for websocket field, completely disables websocket
%% on given route.
-define(WEBSOCKET_DISABLED, ws_disabled).

%% Prefix of paths requested by websocket clients.
-define(WEBSOCKET_PREFIX_PATH, "/ws/").

% Graph Sync websocket endpoint
-define(GUI_GRAPH_SYNC_WS_PATH, "/graph_sync/gui").

%% GUI routing plugin - module of such name is expected in
%% the application including gui.
%% @todo this could be configurable from env - do we need so?
-define(GUI_ROUTE_PLUGIN, gui_route_plugin).
%% GUI session plugin - module of such name is expected in
%% the application including gui.
%% @todo this could be configurable from env - do we need so?
-define(GUI_SESSION_PLUGIN, gui_session_plugin).

%% Record used to define GUI routes, their requirements and logic.
-record(gui_route, {
    % Does this resource require being logged in?
    requires_session = ?SESSION_ANY :: ?SESSION_ANY | ?SESSION_LOGGED_IN |
    ?SESSION_NOT_LOGGED_IN,
    % Is websocket enabled on this page? If so, what session must client have to
    % connect?
    websocket = ?WEBSOCKET_DISABLED :: ?SESSION_ANY | ?SESSION_LOGGED_IN |
    ?SESSION_NOT_LOGGED_IN | ?WEBSOCKET_DISABLED,
    % HTML file connected to this resource
    % (just the name, not file path).
    % `undefined` value can be used if the page does not have a html file.
    html_file = undefined :: binary() | undefined,
    % Erlang handler module for the page server logic (if exists),
    % implementing page_backend_behaviour.
    % `undefined` value can be used if the page does not have a backend logic,
    % then simply HTML file will be served.
    page_backend = undefined :: module()
}).
