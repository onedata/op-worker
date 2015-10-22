%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Routes used by cowboy to route requests to adequate handlers.
%%% @end
%%%--------------------------------------------------------------------
-module(rest_router).
-author("Tomasz Lichon").

-include("modules/http_worker/rest/handler_description.hrl").

%% API
-export([top_level_routing/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns top level routes of rest endpoint.
%% @end
%%--------------------------------------------------------------------
-spec top_level_routing() -> list().
top_level_routing() ->
    custom_api_routes() ++ cdmi_routes().

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns routes to onedata's custom rest api.
%% @end
%%--------------------------------------------------------------------
-spec custom_api_routes() -> list().
custom_api_routes() ->
    [
        {"/rest/:version/[...]", pre_handler,
            #handler_description{handler = rest_handler}}
    ].

%%--------------------------------------------------------------------
%% @doc
%% Returns routes to cdmi protocol.
%% @end
%%--------------------------------------------------------------------
-spec cdmi_routes() -> list().
cdmi_routes() ->
    [
        {"/cdmi/cdmi_capabilities/[...]", pre_handler,
            #handler_description{handler = cdmi_capabilities_handler}},
        {"/cdmi/cdmi_objectid/:id/[...]", pre_handler,
            #handler_description{handler = cdmi_objectid_handler}},
        {"/cdmi/[...]", pre_handler,
            fun cdmi_handler_selector:choose_object_or_container_handler/1}
    ].
