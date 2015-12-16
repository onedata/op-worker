-module(gui_route_plugin).
-author("Lukasz Opiola").
-behaviour(gui_route_plugin_behaviour).

-include_lib("gui/include/gui.hrl").


-export([route/1, data_backend/1, callback_backend/1]).
-export([login_page_path/0, default_page_path/0]).
-export([error_404_html_file/0, error_500_html_file/0]).


-define(LOGIN, #gui_route{
    requires_session = ?SESSION_NOT_LOGGED_IN,
    html_file = undefined,
    page_backend = login_backend
}).

-define(LOGOUT, #gui_route{
    requires_session = ?SESSION_LOGGED_IN,
    html_file = undefined,
    page_backend = logout_backend
}).

-define(VALIDATE_LOGIN, #gui_route{
    requires_session = ?SESSION_ANY,
    html_file = undefined,
    page_backend = validate_login_backend
}).

-define(INDEX, #gui_route{
    requires_session = ?SESSION_LOGGED_IN,
    html_file = <<"index.html">>,
    page_backend = undefined
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
route(<<"/validate_login.html">>) -> ?VALIDATE_LOGIN;
route(<<"/">>) -> ?INDEX;
route(<<"/index.html">>) -> ?INDEX.


data_backend(<<"file">>) -> file_data_backend;
data_backend(<<"fileContent">>) -> file_data_backend.

callback_backend(<<"global">>) -> global_callback_backend.