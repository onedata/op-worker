%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides mapping of gui paths to modules that will
%% render the pages. Note that pages using any actions or events
%% must be available both under /page_name and /ws/page_name.
%% @end
%% ===================================================================

-module(gui_routes).
-behaviour(route_handler).
-include_lib("n2o/include/wf.hrl").
-export([init/2, finish/2]).

finish(State, Ctx) -> {ok, State, Ctx}.
init(State, Ctx) ->
    Path = wf:path(Ctx#context.req),
    {ok, State, Ctx#context{path = Path, module = route(Path)}}.

route(<<"/strona">>) -> strona;
route(<<"/ws/strona">>) -> strona;
route(_) -> page_404.
