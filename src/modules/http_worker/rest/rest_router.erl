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

%% API
-export([top_level_routing/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Finds all protocol plugins and returns routes to them with preceding
%% pre_handler.
%% @end
%%--------------------------------------------------------------------
-spec top_level_routing() -> list().
top_level_routing() ->
    {ok, Plugins} = application:get_env(protocol_plugins),
    PluginRoutes = [Plugin:routes() || Plugin <- Plugins],
    FlattenPluginRoutes = lists:flatten(PluginRoutes),
    [{Path, pre_handler, PluginDescription} || {Path, PluginDescription} <- FlattenPluginRoutes].

%%%===================================================================
%%% Internal functions
%%%===================================================================


