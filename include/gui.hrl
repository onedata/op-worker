-author("lopiola").

-define(LOGGED_IN, logged_in).
-define(NOT_LOGGED_IN, not_logged_in).
-define(ANY, any).

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