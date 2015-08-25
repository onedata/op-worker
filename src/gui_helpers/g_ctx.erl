%%%-------------------------------------------------------------------
%%% @author lopiola
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Aug 2015 11:33
%%%-------------------------------------------------------------------
-module(g_ctx).
-author("lopiola").


-include("gui.hrl").
-include_lib("ctool/include/logging.hrl").


-record(context, {
    req = undefined :: cowboy_req:req() | undefined,
    path = undefined :: binary() | undefined,
    gui_route = undefined :: #gui_route{} | undefined,
    session = <<"">> :: binary()
}).


-define(CTX_KEY, ctx).

%% API
-export([init_context/1]).
-export([req/0, path/0, html_file/0, page_module/0]).
-export([session_requirements/0, user_logged_in/0]).


init_context(Req) ->
    {Path, _} = cowboy_req:path(Req),
    NewCtx = #context{
        req = Req,
        path = Path,
        gui_route = gui_routes:route(Path)
    },
    put(?CTX_KEY, NewCtx).


req() ->
    #context{req = Req} = get(?CTX_KEY),
    Req.


path() ->
    #context{path = Path} = get(?CTX_KEY),
    Path.


html_file() ->
    #context{gui_route = #gui_route{html_file = File}} = get(?CTX_KEY),
    File.


page_module() ->
    #context{gui_route = #gui_route{handler_module = Mod}} = get(?CTX_KEY),
    Mod.


session_requirements() ->
    #context{gui_route = #gui_route{requires_session = Reqs}} = get(?CTX_KEY),
    Reqs.


user_logged_in() ->
    #context{session = _Session} = get(?CTX_KEY),
    true.