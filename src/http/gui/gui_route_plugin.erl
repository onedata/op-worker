%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gui_route_plugin_behaviour. It decides on:
%%%   - mapping of URLs to pages (routes)
%%%   - logic and requirements on different routes
%%%   - what pages are used for login, logout, displaying errors
%%%   - what modules handles server logic of WebSocket connection with
%%%         the client (data and callback backends)
%%% @end
%%%-------------------------------------------------------------------
-module(gui_route_plugin).
-author("Lukasz Opiola").
-behaviour(gui_route_plugin_behaviour).

-include_lib("gui/include/gui.hrl").

-export([route/1, data_backend/1, callback_backend/1]).
-export([login_page_path/0, default_page_path/0]).
-export([error_404_html_file/0, error_500_html_file/0]).

%% Convenience macros for defining routes.
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

%% ====================================================================
%% API
%% ====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Should return a gui_route record per every page that a user can visit.
%% If the Path is not valid, error_404_html_file/0 function will be used
%% to retrieve .html file to serve that will display the error.
%% @end
%%--------------------------------------------------------------------
-spec route(Path :: binary()) -> #gui_route{}.
route(<<"/login.html">>) -> ?LOGIN;
route(<<"/logout.html">>) -> ?LOGOUT;
route(<<"/validate_login.html">>) -> ?VALIDATE_LOGIN;
route(<<"/">>) -> ?INDEX;
route(<<"/index.html">>) -> ?INDEX.


%%--------------------------------------------------------------------
%% @doc
%% Should return a module that implements data_backend_behaviour and
%% will be called for models synchronization over websocket.
%% @end
%%--------------------------------------------------------------------
-spec data_backend(Identifier :: binary()) -> HandlerModule :: module().
data_backend(<<"file">>) -> file_data_backend;
data_backend(<<"fileContent">>) -> file_data_backend.


%%--------------------------------------------------------------------
%% @doc
%% Should return a module that implements callback_backend_behaviour and
%% will be called to handle calls from the GUI that do not regard models.
%% @end
%%--------------------------------------------------------------------
-spec callback_backend(Identifier :: binary()) -> HandlerModule :: module().
callback_backend(<<"global">>) -> global_callback_backend.


%%--------------------------------------------------------------------
%% @doc
%% Should return login page where the user will be redirected if he requests
%% a page that can only be visited when logged in.
%% @end
%%--------------------------------------------------------------------
-spec login_page_path() -> Path :: binary().
login_page_path() ->
    <<"/login.html">>.


%%--------------------------------------------------------------------
%% @doc
%% Should return a default page where the user will be redirected if
%% he requests a page that he cannot currently visit (for example login page
%% when the user is already logged in).
%% @end
%%--------------------------------------------------------------------
-spec default_page_path() -> Path :: binary().
default_page_path() ->
    <<"/">>.


%%--------------------------------------------------------------------
%% @doc
%% Should return a file name of the HTML file that displays error 404 page.
%% @end
%%--------------------------------------------------------------------
-spec error_404_html_file() -> FileName :: binary().
error_404_html_file() ->
    <<"page404.html">>.


%%--------------------------------------------------------------------
%% @doc
%% Should return a file name of the HTML file that displays error 500 page.
%% @end
%%--------------------------------------------------------------------
-spec error_500_html_file() -> FileName :: binary().
error_500_html_file() ->
    <<"page500.html">>.
