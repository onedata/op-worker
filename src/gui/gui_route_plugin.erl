-module(gui_route_plugin).
-author("Lukasz Opiola").
-behaviour(gui_route_plugin_behaviour).

-include_lib("gui/include/gui.hrl").


-export([route/1]).
-export([login_page_path/0, default_page_path/0]).
-export([error_404_html_file/0, error_500_html_file/0]).


-define(LOGIN, #gui_route{
    requires_session = ?NOT_LOGGED_IN,
    html_file = <<"login.html">>,
    page_backend = login_backend
}).

-define(VERIFY_LOGIN, #gui_route{
    requires_session = ?LOGGED_IN,
    html_file = undefined,
    page_backend = verify_login_backend
}).

-define(FILE_MANAGER, #gui_route{
    requires_session = ?LOGGED_IN,
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


route(<<"/logintest.html">>) -> ?LOGIN;
route(<<"/verify_login.html">>) -> ?VERIFY_LOGIN;
route(<<"/">>) -> ?FILE_MANAGER;
route(<<"/file_manager.html">>) -> ?FILE_MANAGER.