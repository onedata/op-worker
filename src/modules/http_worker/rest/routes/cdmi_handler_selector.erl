%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Some rest handlers must be selected in runtime basing on request type.
%%% This module contains implementation of selectors that can do it during
%%% pre-handling request.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_handler_selector).
-author("Tomasz Lichon").

-include("modules/http_worker/rest/handler_description.hrl").

%% API
-export([choose_object_or_container_handler/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Chooses handler basing on request, and returns handler description
%% @end
%%--------------------------------------------------------------------
-spec choose_object_or_container_handler(cowboy_req:req()) ->
    {ok, #handler_description{}}.
choose_object_or_container_handler(Req) ->
    {Path, _} = cowboy_req:path(Req),
    Handler = choose_object_or_container_handler_module(Path),
    {ok, #handler_description{handler = Handler}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Chooses handler basing on presence of '/' at the end of request path
%% @end
%%--------------------------------------------------------------------
-spec choose_object_or_container_handler_module(binary()) ->
    cdmi_container_handler | cdmi_object_handler.
choose_object_or_container_handler_module(Path) ->
    case ends_with_slash(Path) of
        true -> cdmi_container_handler;
        false -> cdmi_object_handler
    end.


%%--------------------------------------------------------------------
%% @equiv binary:last(Path) =:= $/
%%--------------------------------------------------------------------
-spec ends_with_slash(binary()) -> boolean().
ends_with_slash(Path) ->
    binary:last(Path) =:= $/.