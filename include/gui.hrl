-author("lopiola").

-define(LOGGED_IN, logged_in).
-define(NOT_LOGGED_IN, not_logged_in).
-define(ANY, any).

% Preix of paths requested by webscoket clients.
-define(WEBSOCKET_PREFIX_PATH, "/ws/").


% Session cookie id
-define(SESSION_COOKIE_KEY, <<"session_id">>).
% Value of cookie when there is no session
-define(NO_SESSION_COOKIE, <<"no_session">>).


% GUI routing plugin
-define(GUI_ROUTE_PLUGIN, gui_route_plugin).
-define(GUI_SESSION_PLUGIN, gui_session_plugin).

-record(gui_route, {
    % Does this resource require being logged in?
    requires_session = true ::  boolean(),
    % HTML file connected to this resource, if exists
    % (just the name, not file path).
    html_file = undefined :: binary() | undefined,
    % Erlang handler module for the page server logic (if exists),
    % implementing page_backend_behaviour.
    handler_module = undefined :: module()
}).