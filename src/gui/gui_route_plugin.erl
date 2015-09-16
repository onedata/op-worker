-module(gui_route_plugin).
-author("Lukasz Opiola").
-behaviour(gui_route_plugin_behaviour).

-include_lib("gui/include/gui.hrl").


-export([route/1]).
-export([login_page_path/0, default_page_path/0]).
-export([error_404_html_file/0, error_500_html_file/0]).


-define(LOGIN, #gui_route{
    requires_session = ?SESSION_NOT_LOGGED_IN,
    html_file = <<"login.html">>,
    page_backend = login_backend
}).
-define(VER_LOGIN, #gui_route{
    requires_session = ?SESSION_NOT_LOGGED_IN,
    html_file = undefined,
    page_backend = ver_login_backend
}).
-define(LOGOUT, #gui_route{
    requires_session = ?SESSION_LOGGED_IN,
    html_file = undefined,
    page_backend = logout_backend
}).

-define(VALIDATE_LOGIN, #gui_route{
    requires_session = ?SESSION_NOT_LOGGED_IN,
    html_file = undefined,
    page_backend = validate_login_backend
}).

-define(FILE_MANAGER, #gui_route{
    requires_session = ?SESSION_LOGGED_IN,
    html_file = <<"file_manager.html">>,
    page_backend = file_manager_backend
}).




login_page_path() ->
    <<"/login.html">>.


default_page_path() ->
    <<"/">>.


error_404_html_file() ->
    <<"page404.html">>.


error_500_html_file() ->
    <<"page500.html">>.


route(<<"/login.html">>) -> ?LOGIN;
route(<<"/logout.html">>) -> ?LOGOUT;
route(<<"/ver_login.html">>) -> ?VER_LOGIN;
route(<<"/validate_login.html">>) -> ?VALIDATE_LOGIN;
route(<<"/">>) -> ?FILE_MANAGER;
route(<<"/file_manager.html">>) -> ?FILE_MANAGER.