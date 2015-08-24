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


-include_lib("ctool/include/logging.hrl").

-record(context, {
    req = undefined :: cowboy_req:req() | undefined,
    path = <<"">> :: binary(),
    page_module = undefined :: module(),
    session = <<"">> :: binary()
}).


-define(CTX_KEY, ctx).

%% API
-export([init_context/1, req/0, path/0, page_module/0]).


init_context(Req) ->
    {Path, _} = cowboy_req:path(Req),
    NewCtx = #context{
        req = Req,
        path = Path,
        page_module = get_page_module(Path)
    },
    put(?CTX_KEY, NewCtx).


req() ->
    #context{req = Req} = get(?CTX_KEY),
    Req.


path() ->
    #context{path = Path} = get(?CTX_KEY),
    Path.


page_module() ->
    #context{page_module = PageModule} = get(?CTX_KEY),
    PageModule.



get_page_module(<<"/ws/", Path/binary>>) ->
    get_page_module(Path);
get_page_module(<<"/", Path/binary>>) ->
    get_page_module(Path);
get_page_module(Path) ->
    case binary:split(Path, <<"/">>) of
        [Path] ->
            PathNoExt = filename:rootname(Path),
            binary_to_atom(<<PathNoExt/binary, "_backend">>, latin1);
        _ ->
            undefined
    end.