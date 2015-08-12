%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module provides mapping of gui paths to modules that will
%%% render the pages.
%%% @end
%%% @todo function headers
%%%--------------------------------------------------------------------
-module(gui_routes).
-author("Lukasz Opiola").

-include_lib("n2o/include/wf.hrl").
-export([init/2, finish/2]).

finish(State, Ctx) -> {ok, State, Ctx}.
init(State, Ctx) ->
<<<<<<< HEAD
    Path = wf:path(Ctx#cx.req),
    {ok, State, Ctx#cx{path = Path, module = route(Path)}}.
=======
    Path = wf:path(Ctx#context.req),
    {ok, State, Ctx#context{path = Path, module = route(Path)}}.

route(<<"/openid/login">>) -> page_openid_login;
>>>>>>> 53e59ad7a4bdf1b6e1967470292063241c515077
route(_) -> page_404.