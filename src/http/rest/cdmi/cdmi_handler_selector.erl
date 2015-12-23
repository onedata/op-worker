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
    {#{handler => module(),
       exception_handler => protocol_plugin_behaviour:exception_handler()
    }, cowboy_req:req()}.
choose_object_or_container_handler(Req) ->
    {Path, Req2} = cowboy_req:path(Req),
    Handler = choose_object_or_container_handler_module(Path),
    {#{handler => Handler,
        exception_handler => fun cdmi_exception_handler:handle/4
    }, Req2}.

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
    case filepath_utils:ends_with_slash(Path) of
        true -> cdmi_container_handler;
        false -> cdmi_object_handler
    end.