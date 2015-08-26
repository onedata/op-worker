-module(gui_session_plugin).
-author("Lukasz Opiola").

-include("gui.hrl").


-export([route/1, login_page/0, default_page/0]).


-define(LOGIN, #gui_route{
    requires_session = ?NOT_LOGGED_IN,
    html_file = <<"login.html">>,
    handler_module = login_backend
}).

-define(VERIFY_LOGIN, #gui_route{
    requires_session = ?LOGGED_IN,
    html_file = undefined,
    handler_module = verify_login_backend
}).

-define(FILE_MANAGER, #gui_route{
    requires_session = ?LOGGED_IN,
    html_file = <<"file_manager.html">>,
    handler_module = file_manager_backend
}).

-define(PAGE_404, #gui_route{
    requires_session = ?ANY,
    html_file = <<"page404.html">>
}).


login_page() ->
    <<"/login.html">>.


default_page() ->
    <<"/">>.


route(<<"/logintest.html">>) -> ?LOGIN;
route(<<"/verify_login.html">>) -> ?VERIFY_LOGIN;
route(<<"/">>) -> ?FILE_MANAGER;
route(<<"/file_manager.html">>) -> ?FILE_MANAGER;
route(_) -> ?PAGE_404.